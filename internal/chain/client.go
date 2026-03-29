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
	TxHash           string
	Height           int64
	Sender           string
	FeeUatom         int64 // Gas fee paid in uatom
	Action           string
	Contract         string
	CodeID           int64 // Code ID from instantiate/migrate events (0 if not applicable)
	Timestamp        time.Time
	ProtocolFeeUatom int64 // 2% marketplace protocol fee from finalize-sale
	ListingFeeUatom  int64 // Listing deposit from set-ask, forwarded to protocol
	CreationFeeUatom int64 // Minter creation fee from create_minter
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

	// Query all event key prefixes that reference a contract address:
	// - wasm.* / execute.* for MsgExecuteContract (varies by SDK/wasmd version)
	// - instantiate.* for MsgInstantiateContract / MsgInstantiateContract2
	// - migrate.* for MsgMigrateContract
	prefixes := []string{
		"wasm._contract_address",
		"execute._contract_address",
		"instantiate._contract_address",
		"migrate._contract_address",
	}

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
// Extracts gas fees, protocol fees, listing fees, and creation fees.
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

		// Track whether specific revenue-generating events were seen.
		var hasSetAsk bool
		var hasFinalizeSale bool

		// Track the marketplace contract address from wasm-set-ask for listing fee detection.
		var marketplaceAddr string

		feeFound := false
		for _, evt := range txResp.Events {
			switch evt.Type {
			case "coin_spent":
				if !feeFound {
					// First coin_spent event = gas fee to fee collector (no msg_index)
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

			case "instantiate", "migrate":
				for _, a := range evt.Attributes {
					if a.Key == "code_id" && ct.CodeID == 0 {
						fmt.Sscanf(a.Value, "%d", &ct.CodeID)
					}
					if a.Key == "_contract_address" && ct.Contract == contractAddr {
						ct.Contract = a.Value
					}
				}

			case "wasm-finalize-sale":
				hasFinalizeSale = true
				if ct.Action == "" {
					ct.Action = "finalize-sale"
				}
				// Accumulate protocol fees across all sales in this tx
				for _, a := range evt.Attributes {
					if a.Key == "protocol" {
						var amt int64
						fmt.Sscanf(a.Value, "%d", &amt)
						ct.ProtocolFeeUatom += amt
					}
				}

			case "wasm-set-ask":
				hasSetAsk = true
				for _, a := range evt.Attributes {
					if a.Key == "_contract_address" {
						marketplaceAddr = a.Value
					}
				}
				if ct.Action == "" {
					ct.Action = "set-ask"
				}

			default:
				if ct.Action == "" && strings.HasPrefix(evt.Type, "wasm-") {
					ct.Action = strings.TrimPrefix(evt.Type, "wasm-")
				}
			}
		}

		// Listing fees: when set-ask is present (even in multi-msg txs like approve+set-ask),
		// look for coin_spent from the marketplace contract (forwarding listing deposit to fee collector).
		// Skip if finalize-sale is also present (sale disbursements are not listing fees).
		if hasSetAsk && !hasFinalizeSale && marketplaceAddr != "" {
			for _, evt := range txResp.Events {
				if evt.Type != "coin_spent" {
					continue
				}
				var spender, amount string
				for _, a := range evt.Attributes {
					switch a.Key {
					case "spender":
						spender = a.Value
					case "amount":
						amount = a.Value
					}
				}
				if spender == marketplaceAddr {
					ct.ListingFeeUatom += parseUatomAmount(amount)
				}
			}
		}

		// Creation fees: funds sent by the tx sender with create_minter action.
		// These are non-gas coin_spent events (have msg_index) from the sender.
		if ct.Action == "create_minter" {
			for _, evt := range txResp.Events {
				if evt.Type != "coin_spent" {
					continue
				}
				var spender, amount string
				var hasMsgIndex bool
				for _, a := range evt.Attributes {
					switch a.Key {
					case "spender":
						spender = a.Value
					case "amount":
						amount = a.Value
					case "msg_index":
						hasMsgIndex = true
					}
				}
				if hasMsgIndex && spender == ct.Sender {
					ct.CreationFeeUatom += parseUatomAmount(amount)
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
// such as MsgStoreCode, MsgUpdateAdmin, MsgInstantiateContract, MsgMigrateContract, etc.
// that don't reliably emit wasm._contract_address events.
// knownCodeIDs filters instantiate/migrate txs to only include Stargaze-related contracts.
func (c *Client) QueryCreatorTxs(ctx context.Context, creator string, afterHeight int64, limit int, knownCodeIDs map[int64]bool) ([]ContractTx, error) {
	if limit <= 0 {
		limit = 100
	}

	var allTxs []ContractTx
	seen := map[string]bool{}
	cursor := afterHeight

	actions := []string{
		"/cosmwasm.wasm.v1.MsgStoreCode",
		"/cosmwasm.wasm.v1.MsgUpdateAdmin",
		"/cosmwasm.wasm.v1.MsgClearAdmin",
		"/cosmwasm.wasm.v1.MsgInstantiateContract",
		"/cosmwasm.wasm.v1.MsgInstantiateContract2",
		"/cosmwasm.wasm.v1.MsgMigrateContract",
	}

	// Actions that require code_id filtering
	codeIDFiltered := map[string]bool{
		"/cosmwasm.wasm.v1.MsgInstantiateContract":  true,
		"/cosmwasm.wasm.v1.MsgInstantiateContract2": true,
		"/cosmwasm.wasm.v1.MsgMigrateContract":      true,
	}

	for _, action := range actions {
		pageCursor := cursor
		var pageKey string
		needsCodeFilter := codeIDFiltered[action] && len(knownCodeIDs) > 0

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

			txs := c.extractContractTxs(result.TxResponses, "creator:"+creator)
			var maxHeight int64
			for _, tx := range txs {
				if tx.Height > maxHeight {
					maxHeight = tx.Height
				}
				if seen[tx.TxHash] {
					continue
				}
				// Filter instantiate/migrate by known Stargaze code IDs
				if needsCodeFilter && !knownCodeIDs[tx.CodeID] {
					continue
				}
				seen[tx.TxHash] = true
				tx.Action = strings.TrimPrefix(action, "/cosmwasm.wasm.v1.")
				allTxs = append(allTxs, tx)
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
