// Package chain provides a gRPC client for querying CosmWasm events from the Cosmos Hub.
package chain

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/Cordtus/libyaci"
)

const (
	methodGetTxsEvent  = "cosmos.tx.v1beta1.Service.GetTxsEvent"
	methodGetNodeInfo  = "cosmos.base.tendermint.v1beta1.Service.GetNodeInfo"
	methodLatestBlock  = "cosmos.base.tendermint.v1beta1.Service.GetLatestBlock"

	defaultDialTimeout = 30 * time.Second
)

// Client wraps libyaci for gRPC queries to a Cosmos SDK node.
type Client struct {
	yaci   *libyaci.Client
	logger *slog.Logger
}

// NewClient creates a gRPC client connected to the given endpoint.
func NewClient(endpoint string, logger *slog.Logger) (*Client, error) {
	opts := []libyaci.Option{
		libyaci.WithMaxRecvMsgSize(50 * 1024 * 1024),
		libyaci.WithMaxRetries(3),
		libyaci.WithDialTimeout(defaultDialTimeout),
	}

	if !strings.Contains(endpoint, ":443") {
		opts = append(opts, libyaci.WithInsecure())
	}

	ctx := context.Background()
	client, err := libyaci.Dial(ctx, endpoint, opts...)
	if err != nil {
		return nil, fmt.Errorf("connecting to %s: %w", endpoint, err)
	}

	logger.Info("gRPC connected", "endpoint", endpoint)

	return &Client{yaci: client, logger: logger}, nil
}

// Close closes the gRPC connection.
func (c *Client) Close() error {
	if c.yaci != nil {
		return c.yaci.Close()
	}
	return nil
}

// WasmEvent represents a parsed wasm contract event from on-chain data.
type WasmEvent struct {
	TxHash    string
	Height    int64
	Sender    string
	Contract  string
	Action    string
	Attrs     map[string]string
	Timestamp time.Time
}

// txsEventResponse represents the decoded GetTxsEvent gRPC response.
type txsEventResponse struct {
	TxResponses []txResponse `json:"txResponses"`
	Pagination  *pagination  `json:"pagination,omitempty"`
}

type txResponse struct {
	Height string `json:"height"`
	TxHash string `json:"txhash"`
	Code   uint32 `json:"code"`
	Logs   []txLog `json:"logs"`
	Timestamp string `json:"timestamp"`
}

type txLog struct {
	MsgIndex int       `json:"msgIndex"`
	Events   []txEvent `json:"events"`
}

type txEvent struct {
	Type       string        `json:"type"`
	Attributes []txAttribute `json:"attributes"`
}

type txAttribute struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type pagination struct {
	NextKey string `json:"nextKey,omitempty"`
	Total   string `json:"total,omitempty"`
}

// QueryWasmEvents queries the chain for wasm events matching the contract address
// after the given height. Returns events sorted ascending by height.
func (c *Client) QueryWasmEvents(ctx context.Context, contractAddr string, afterHeight int64, limit int) ([]WasmEvent, error) {
	if limit <= 0 {
		limit = 100
	}

	var allEvents []WasmEvent
	var pageKey string

	for {
		query := fmt.Sprintf("wasm._contract_address='%s' AND tx.height>%d", contractAddr, afterHeight)

		params := map[string]any{
			"query":    query,
			"order_by": "ORDER_BY_ASC",
			"pagination": map[string]any{
				"limit": limit,
			},
		}
		if pageKey != "" {
			params["pagination"].(map[string]any)["key"] = pageKey
		}

		paramsJSON, _ := json.Marshal(params)

		resp, err := c.yaci.Invoke(methodGetTxsEvent, paramsJSON)
		if err != nil {
			return nil, fmt.Errorf("GetTxsEvent: %w", err)
		}

		var result txsEventResponse
		if err := json.Unmarshal(resp, &result); err != nil {
			return nil, fmt.Errorf("parsing response: %w", err)
		}

		events := c.extractWasmEvents(result.TxResponses, contractAddr)
		allEvents = append(allEvents, events...)

		// Stop if no more pages or we've collected enough
		if result.Pagination == nil || result.Pagination.NextKey == "" {
			break
		}
		pageKey = result.Pagination.NextKey
	}

	return allEvents, nil
}

// extractWasmEvents pulls wasm events from transaction responses for the target contract.
func (c *Client) extractWasmEvents(txResponses []txResponse, contractAddr string) []WasmEvent {
	var events []WasmEvent

	for _, txResp := range txResponses {
		if txResp.Code != 0 {
			continue // Skip failed transactions
		}

		var height int64
		fmt.Sscanf(txResp.Height, "%d", &height)

		ts, _ := time.Parse(time.RFC3339, txResp.Timestamp)

		for _, log := range txResp.Logs {
			for _, event := range log.Events {
				if event.Type != "wasm" {
					continue
				}

				we := WasmEvent{
					TxHash:    txResp.TxHash,
					Height:    height,
					Timestamp: ts,
					Attrs:     make(map[string]string),
				}

				for _, attr := range event.Attributes {
					we.Attrs[attr.Key] = attr.Value

					switch attr.Key {
					case "_contract_address":
						we.Contract = attr.Value
					case "action":
						we.Action = attr.Value
					case "sender":
						we.Sender = attr.Value
					}
				}

				// Only include events for our target contract
				if we.Contract != contractAddr {
					continue
				}

				events = append(events, we)
			}
		}
	}

	return events
}

// GetChainID fetches the chain ID from the node.
func (c *Client) GetChainID(ctx context.Context) (string, error) {
	resp, err := c.yaci.Invoke(methodGetNodeInfo, nil)
	if err != nil {
		return "", fmt.Errorf("GetNodeInfo: %w", err)
	}

	var result struct {
		DefaultNodeInfo struct {
			Network string `json:"network"`
		} `json:"defaultNodeInfo"`
	}
	if err := json.Unmarshal(resp, &result); err != nil {
		return "", fmt.Errorf("parsing node info: %w", err)
	}

	return result.DefaultNodeInfo.Network, nil
}

// GetLatestHeight returns the latest block height.
func (c *Client) GetLatestHeight(ctx context.Context) (int64, error) {
	resp, err := c.yaci.Invoke(methodLatestBlock, nil)
	if err != nil {
		return 0, fmt.Errorf("GetLatestBlock: %w", err)
	}

	var result struct {
		SdkBlock *struct {
			Header struct {
				Height string `json:"height"`
			} `json:"header"`
		} `json:"sdkBlock"`
		Block *struct {
			Header struct {
				Height string `json:"height"`
			} `json:"header"`
		} `json:"block"`
	}
	if err := json.Unmarshal(resp, &result); err != nil {
		return 0, fmt.Errorf("parsing block: %w", err)
	}

	var heightStr string
	if result.SdkBlock != nil {
		heightStr = result.SdkBlock.Header.Height
	} else if result.Block != nil {
		heightStr = result.Block.Header.Height
	}

	var height int64
	fmt.Sscanf(heightStr, "%d", &height)
	return height, nil
}
