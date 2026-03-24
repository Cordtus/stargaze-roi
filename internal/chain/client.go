// Package chain provides a gRPC client for querying CosmWasm transaction data from the Cosmos Hub.
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
	methodGetTxsEvent = "cosmos.tx.v1beta1.Service.GetTxsEvent"
	methodGetNodeInfo = "cosmos.base.tendermint.v1beta1.Service.GetNodeInfo"
	methodLatestBlock = "cosmos.base.tendermint.v1beta1.Service.GetLatestBlock"

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

// ContractTx represents a transaction that interacted with a tracked contract.
type ContractTx struct {
	TxHash    string
	Height    int64
	Sender    string
	FeeUatom  int64 // Gas fee paid in uatom
	Action    string
	Contract  string
	Timestamp time.Time
}

// txResponse represents a decoded transaction response.
// Events are at the top level (not nested in logs) in newer SDK versions.
type txResponse struct {
	Height    string    `json:"height"`
	TxHash    string    `json:"txhash"`
	Code      uint32    `json:"code"`
	Events    []txEvent `json:"events"`
	Timestamp string    `json:"timestamp"`
	GasWanted string    `json:"gasWanted"`
	GasUsed   string    `json:"gasUsed"`
}

type txEvent struct {
	Type       string        `json:"type"`
	Attributes []txAttribute `json:"attributes"`
}

type txAttribute struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type txsResponse struct {
	TxResponses []txResponse `json:"txResponses"`
	Pagination  *pagination  `json:"pagination,omitempty"`
}

type pagination struct {
	NextKey string `json:"nextKey,omitempty"`
	Total   string `json:"total,omitempty"`
}

// QueryContractTxs queries the chain for transactions involving the given contract
// after the given height. Returns transactions sorted ascending by height.
func (c *Client) QueryContractTxs(ctx context.Context, contractAddr string, afterHeight int64, limit int) ([]ContractTx, error) {
	if limit <= 0 {
		limit = 100
	}

	var allTxs []ContractTx
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

		var result txsResponse
		if err := json.Unmarshal(resp, &result); err != nil {
			return nil, fmt.Errorf("parsing response: %w", err)
		}

		txs := c.extractContractTxs(result.TxResponses, contractAddr)
		allTxs = append(allTxs, txs...)

		if result.Pagination == nil || result.Pagination.NextKey == "" {
			break
		}
		pageKey = result.Pagination.NextKey
	}

	return allTxs, nil
}

// extractContractTxs parses transaction responses into ContractTx structs.
// Events are at txResponse.events[] (top-level), not in logs.
func (c *Client) extractContractTxs(txResponses []txResponse, contractAddr string) []ContractTx {
	var txs []ContractTx
	seen := map[string]bool{} // Dedup by tx hash (multiple wasm events per tx)

	for _, txResp := range txResponses {
		if txResp.Code != 0 || seen[txResp.TxHash] {
			continue
		}
		seen[txResp.TxHash] = true

		var height int64
		fmt.Sscanf(txResp.Height, "%d", &height)

		ts, _ := time.Parse(time.RFC3339, txResp.Timestamp)

		ct := ContractTx{
			TxHash:    txResp.TxHash,
			Height:    height,
			Contract:  contractAddr,
			Timestamp: ts,
		}

		// Extract fee from the first coin_spent event (tx fee payment)
		// Extract wasm action and sender from wasm events
		feeFound := false
		for _, evt := range txResp.Events {
			switch evt.Type {
			case "coin_spent":
				if !feeFound {
					for _, a := range evt.Attributes {
						if a.Key == "amount" {
							ct.FeeUatom = parseUatomAmount(a.Value)
							feeFound = true
						}
						if a.Key == "spender" {
							ct.Sender = a.Value
						}
					}
				}
			case "wasm":
				for _, a := range evt.Attributes {
					if a.Key == "action" && ct.Action == "" {
						ct.Action = a.Value
					}
				}
			}
		}

		txs = append(txs, ct)
	}

	return txs
}

// parseUatomAmount extracts uatom amount from a string like "6417uatom" or "100000uatom".
// Returns 0 if the denom is not uatom or parsing fails.
func parseUatomAmount(s string) int64 {
	s = strings.TrimSpace(s)
	if !strings.HasSuffix(s, "uatom") {
		return 0
	}
	numStr := strings.TrimSuffix(s, "uatom")
	var amount int64
	fmt.Sscanf(numStr, "%d", &amount)
	return amount
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
