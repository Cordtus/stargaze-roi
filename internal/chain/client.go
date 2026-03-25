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

	methodContractInfo     = "cosmwasm.wasm.v1.Query.ContractInfo"
	methodContractsByCode  = "cosmwasm.wasm.v1.Query.ContractsByCode"
	methodContractsByCreat = "cosmwasm.wasm.v1.Query.ContractsByCreator"

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
// Queries both wasm._contract_address and execute._contract_address to catch all event prefixes.
func (c *Client) QueryContractTxs(ctx context.Context, contractAddr string, afterHeight int64, limit int) ([]ContractTx, error) {
	if limit <= 0 {
		limit = 100
	}

	// Query both event key prefixes -- some chains index under wasm.*, others under execute.*
	prefixes := []string{"wasm._contract_address", "execute._contract_address"}

	var allTxs []ContractTx
	seen := map[string]bool{}

	for _, prefix := range prefixes {
		txs, err := c.queryByPrefix(ctx, prefix, contractAddr, afterHeight, limit)
		if err != nil {
			c.logger.Warn("query failed for prefix", "prefix", prefix, "error", err)
			continue
		}
		for _, tx := range txs {
			if !seen[tx.TxHash] {
				seen[tx.TxHash] = true
				allTxs = append(allTxs, tx)
			}
		}
	}

	return allTxs, nil
}

// queryByPrefix runs a paginated GetTxsEvent query for a single event prefix.
func (c *Client) queryByPrefix(ctx context.Context, prefix, contractAddr string, afterHeight int64, limit int) ([]ContractTx, error) {
	var txs []ContractTx
	seen := map[string]bool{}
	var pageKey string
	cursor := afterHeight

	for {
		query := fmt.Sprintf("%s='%s' AND tx.height>%d", prefix, contractAddr, cursor)

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

		parsed := c.extractContractTxs(result.TxResponses, contractAddr)
		var maxHeight int64
		for _, tx := range parsed {
			if !seen[tx.TxHash] {
				seen[tx.TxHash] = true
				txs = append(txs, tx)
			}
			if tx.Height > maxHeight {
				maxHeight = tx.Height
			}
		}

		// If server provides a nextKey, use it
		if result.Pagination != nil && result.Pagination.NextKey != "" {
			pageKey = result.Pagination.NextKey
			continue
		}

		// nextKey is empty. If we got a full page, the node likely capped results.
		// Re-query using height cursor to get the next batch.
		if len(result.TxResponses) >= limit && maxHeight > cursor {
			c.logger.Debug("page full with no nextKey, advancing height cursor",
				"contract", contractAddr[:20]+"...",
				"cursor", cursor, "new_cursor", maxHeight,
				"collected", len(txs),
			)
			cursor = maxHeight
			pageKey = ""
			continue
		}

		break
	}

	return txs, nil
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
			default:
				// Custom wasm event types: wasm-set-ask, wasm-finalize-sale, etc.
				if ct.Action == "" && strings.HasPrefix(evt.Type, "wasm-") {
					ct.Action = strings.TrimPrefix(evt.Type, "wasm-")
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

// ContractInfo holds metadata about a CosmWasm contract.
type ContractInfo struct {
	Address string
	CodeID  int64
	Creator string
	Admin   string
	Label   string
}

// GetContractInfo queries the chain for a contract's metadata (code_id, creator, admin, label).
func (c *Client) GetContractInfo(ctx context.Context, addr string) (*ContractInfo, error) {
	params, _ := json.Marshal(map[string]any{"address": addr})
	resp, err := c.yaci.Invoke(methodContractInfo, params)
	if err != nil {
		return nil, fmt.Errorf("ContractInfo(%s): %w", addr, err)
	}

	var result struct {
		Address      string `json:"address"`
		ContractInfo struct {
			CodeID  string `json:"codeId"`
			Creator string `json:"creator"`
			Admin   string `json:"admin"`
			Label   string `json:"label"`
		} `json:"contractInfo"`
	}
	if err := json.Unmarshal(resp, &result); err != nil {
		return nil, fmt.Errorf("parsing ContractInfo: %w", err)
	}

	var codeID int64
	fmt.Sscanf(result.ContractInfo.CodeID, "%d", &codeID)

	return &ContractInfo{
		Address: result.Address,
		CodeID:  codeID,
		Creator: result.ContractInfo.Creator,
		Admin:   result.ContractInfo.Admin,
		Label:   result.ContractInfo.Label,
	}, nil
}

// GetContractsByCode returns all contract addresses instantiated from a given code_id.
func (c *Client) GetContractsByCode(ctx context.Context, codeID int64) ([]string, error) {
	var all []string
	var pageKey string

	for {
		req := map[string]any{
			"codeId": fmt.Sprintf("%d", codeID),
			"pagination": map[string]any{
				"limit": 100,
			},
		}
		if pageKey != "" {
			req["pagination"].(map[string]any)["key"] = pageKey
		}

		params, _ := json.Marshal(req)
		resp, err := c.yaci.Invoke(methodContractsByCode, params)
		if err != nil {
			return nil, fmt.Errorf("ContractsByCode(%d): %w", codeID, err)
		}

		var result struct {
			Contracts  []string    `json:"contracts"`
			Pagination *pagination `json:"pagination,omitempty"`
		}
		if err := json.Unmarshal(resp, &result); err != nil {
			return nil, fmt.Errorf("parsing ContractsByCode: %w", err)
		}

		all = append(all, result.Contracts...)

		if result.Pagination == nil || result.Pagination.NextKey == "" {
			break
		}
		pageKey = result.Pagination.NextKey
	}

	return all, nil
}

// GetContractsByCreator returns all contract addresses deployed by a given creator.
func (c *Client) GetContractsByCreator(ctx context.Context, creator string) ([]string, error) {
	var all []string
	var pageKey string

	for {
		req := map[string]any{
			"creatorAddress": creator,
			"pagination": map[string]any{
				"limit": 100,
			},
		}
		if pageKey != "" {
			req["pagination"].(map[string]any)["key"] = pageKey
		}

		params, _ := json.Marshal(req)
		resp, err := c.yaci.Invoke(methodContractsByCreat, params)
		if err != nil {
			return nil, fmt.Errorf("ContractsByCreator(%s): %w", creator, err)
		}

		var result struct {
			ContractAddresses []string    `json:"contractAddresses"`
			Pagination        *pagination `json:"pagination,omitempty"`
		}
		if err := json.Unmarshal(resp, &result); err != nil {
			return nil, fmt.Errorf("parsing ContractsByCreator: %w", err)
		}

		all = append(all, result.ContractAddresses...)

		if result.Pagination == nil || result.Pagination.NextKey == "" {
			break
		}
		pageKey = result.Pagination.NextKey
	}

	return all, nil
}

// QueryCreatorTxs queries for non-contract transactions by a creator address,
// such as MsgStoreCode, MsgUpdateAdmin, etc. that don't emit wasm._contract_address.
func (c *Client) QueryCreatorTxs(ctx context.Context, creator string, afterHeight int64, limit int) ([]ContractTx, error) {
	if limit <= 0 {
		limit = 100
	}

	var allTxs []ContractTx
	seen := map[string]bool{}
	cursor := afterHeight

	// Query for each message type that doesn't produce wasm._contract_address events
	actions := []string{
		"/cosmwasm.wasm.v1.MsgStoreCode",
		"/cosmwasm.wasm.v1.MsgUpdateAdmin",
		"/cosmwasm.wasm.v1.MsgClearAdmin",
	}

	for _, action := range actions {
		pageCursor := cursor
		var pageKey string

		for {
			query := fmt.Sprintf("message.sender='%s' AND message.action='%s' AND tx.height>%d", creator, action, pageCursor)

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
				c.logger.Warn("failed to query creator txs", "action", action, "error", err)
				break
			}

			var result txsResponse
			if err := json.Unmarshal(resp, &result); err != nil {
				break
			}

			txs := c.extractContractTxs(result.TxResponses, "store_code:"+creator)
			var maxHeight int64
			for _, tx := range txs {
				if !seen[tx.TxHash] {
					seen[tx.TxHash] = true
					// Override action with the message type
					tx.Action = strings.TrimPrefix(action, "/cosmwasm.wasm.v1.")
					allTxs = append(allTxs, tx)
				}
				if tx.Height > maxHeight {
					maxHeight = tx.Height
				}
			}

			if result.Pagination != nil && result.Pagination.NextKey != "" {
				pageKey = result.Pagination.NextKey
				continue
			}

			if len(result.TxResponses) >= limit && maxHeight > pageCursor {
				pageCursor = maxHeight
				pageKey = ""
				continue
			}

			break
		}
	}

	return allTxs, nil
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
