// Package chain provides a gRPC client for querying CosmWasm transaction data from the Cosmos Hub.
package chain

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Discovery finds and caches all contracts related to a set of seed addresses.
// It expands seeds by querying code_id siblings and creator-deployed contracts,
// then caches results in PostgreSQL to avoid redundant node queries.
type Discovery struct {
	client       *Client
	pool         *pgxpool.Pool
	logger       *slog.Logger
	refreshEvery time.Duration

	mu        sync.RWMutex
	contracts map[string]*ContractInfo // address -> info
}

// NewDiscovery creates a contract discovery instance.
// refreshEvery controls how often the discovery re-queries the chain for new contracts.
// A zero duration disables periodic refresh (discovery runs once on startup).
func NewDiscovery(client *Client, pool *pgxpool.Pool, logger *slog.Logger, refreshEvery time.Duration) *Discovery {
	return &Discovery{
		client:       client,
		pool:         pool,
		logger:       logger,
		refreshEvery: refreshEvery,
		contracts:    make(map[string]*ContractInfo),
	}
}

// Run performs initial discovery from seed addresses and optionally refreshes periodically.
// It loads cached contracts from the DB first, then expands from seeds.
// extraCodeIDs are additional code_ids to discover via ContractsByCode (for sub-message-instantiated contracts).
func (d *Discovery) Run(ctx context.Context, seeds []string, extraCodeIDs ...[]int64) error {
	// Load existing cache from DB
	cached, err := d.loadCached(ctx)
	if err != nil {
		d.logger.Warn("failed to load cached contracts, starting fresh", "error", err)
	} else if len(cached) > 0 {
		d.mu.Lock()
		for _, c := range cached {
			d.contracts[c.Address] = c
		}
		d.mu.Unlock()
		d.logger.Info("loaded cached contracts", "count", len(cached))
	}

	// Run initial discovery
	if err := d.expand(ctx, seeds); err != nil {
		return fmt.Errorf("initial discovery: %w", err)
	}

	// Discover contracts from extra code IDs (sub-message-instantiated contracts like cw721)
	var extras []int64
	if len(extraCodeIDs) > 0 {
		extras = extraCodeIDs[0]
	}
	if len(extras) > 0 {
		if err := d.expandByCodeIDs(ctx, extras); err != nil {
			d.logger.Warn("extra code_id discovery failed", "error", err)
		}
	}

	// Periodic refresh if configured
	if d.refreshEvery > 0 {
		go d.refreshLoop(ctx, seeds, extras)
	}

	return nil
}

// Contracts returns a snapshot of all discovered contract addresses.
func (d *Discovery) Contracts() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	addrs := make([]string, 0, len(d.contracts))
	for addr := range d.contracts {
		addrs = append(addrs, addr)
	}
	return addrs
}

// QueryableContracts returns contracts that should be actively queried for transactions.
// Contracts from high-volume code_ids (e.g., WL Merkle whitelists) are excluded because
// their instantiation txs are already captured by querying their parent factory contracts.
// skipCodeIDs specifies code_ids to exclude from tx querying.
func (d *Discovery) QueryableContracts(skipCodeIDs map[int64]bool) []string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	addrs := make([]string, 0)
	for addr, info := range d.contracts {
		if skipCodeIDs != nil && skipCodeIDs[info.CodeID] {
			continue
		}
		addrs = append(addrs, addr)
	}
	return addrs
}

// ContractCount returns the number of discovered contracts.
func (d *Discovery) ContractCount() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.contracts)
}

// KnownCodeIDs returns the set of all code IDs from discovered contracts.
func (d *Discovery) KnownCodeIDs() map[int64]bool {
	d.mu.RLock()
	defer d.mu.RUnlock()

	ids := map[int64]bool{}
	for _, info := range d.contracts {
		ids[info.CodeID] = true
	}
	return ids
}

// Creators returns the unique creator addresses from all discovered contracts.
func (d *Discovery) Creators() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	seen := map[string]bool{}
	var creators []string
	for _, info := range d.contracts {
		if !seen[info.Creator] {
			seen[info.Creator] = true
			creators = append(creators, info.Creator)
		}
	}
	return creators
}

// Admins returns unique admin addresses from all discovered contracts (excluding empty admins
// and addresses already covered by Creators).
func (d *Discovery) Admins() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	creators := map[string]bool{}
	for _, info := range d.contracts {
		creators[info.Creator] = true
	}

	seen := map[string]bool{}
	var admins []string
	for _, info := range d.contracts {
		if info.Admin == "" || creators[info.Admin] || seen[info.Admin] {
			continue
		}
		seen[info.Admin] = true
		admins = append(admins, info.Admin)
	}
	return admins
}

// expand discovers all contracts related to seeds via code_id and creator lookups.
func (d *Discovery) expand(ctx context.Context, seeds []string) error {
	// Track which code_ids and creators we've already expanded
	expandedCodes := map[int64]bool{}
	expandedCreators := map[string]bool{}

	// Pre-populate from existing state
	d.mu.RLock()
	for _, info := range d.contracts {
		expandedCodes[info.CodeID] = true
		expandedCreators[info.Creator] = true
	}
	d.mu.RUnlock()

	// Queue: start with seeds that we haven't seen yet
	queue := make([]string, 0, len(seeds))
	d.mu.RLock()
	for _, addr := range seeds {
		if _, known := d.contracts[addr]; !known {
			queue = append(queue, addr)
		}
	}
	d.mu.RUnlock()

	// If seeds are already known and expanded, check for new contracts from known codes/creators
	if len(queue) == 0 {
		// Re-expand known codes and creators to catch newly deployed contracts
		d.mu.RLock()
		codes := make([]int64, 0)
		creators := make([]string, 0)
		for _, info := range d.contracts {
			if !expandedCodes[info.CodeID] {
				codes = append(codes, info.CodeID)
			}
			if !expandedCreators[info.Creator] {
				creators = append(creators, info.Creator)
			}
		}
		d.mu.RUnlock()

		// Re-query all known codes and creators for new contracts
		d.mu.RLock()
		for _, info := range d.contracts {
			codes = append(codes, info.CodeID)
			creators = append(creators, info.Creator)
		}
		d.mu.RUnlock()

		return d.expandFromCodesAndCreators(ctx, codes, creators)
	}

	var newContracts []*ContractInfo

	for len(queue) > 0 {
		addr := queue[0]
		queue = queue[1:]

		d.mu.RLock()
		_, known := d.contracts[addr]
		d.mu.RUnlock()
		if known {
			continue
		}

		info, err := d.client.GetContractInfo(ctx, addr)
		if err != nil {
			d.logger.Warn("failed to get contract info, skipping", "address", addr, "error", err)
			continue
		}

		d.mu.Lock()
		d.contracts[addr] = info
		d.mu.Unlock()
		newContracts = append(newContracts, info)

		d.logger.Info("discovered contract",
			"address", addr,
			"code_id", info.CodeID,
			"creator", info.Creator,
			"label", info.Label,
		)

		// Expand by code_id if not already done
		if !expandedCodes[info.CodeID] {
			expandedCodes[info.CodeID] = true
			siblings, err := d.client.GetContractsByCode(ctx, info.CodeID)
			if err != nil {
				d.logger.Warn("failed to query contracts by code", "code_id", info.CodeID, "error", err)
			} else {
				d.logger.Info("found contracts by code_id", "code_id", info.CodeID, "count", len(siblings))
				queue = append(queue, siblings...)
			}
		}

		// Expand by creator if not already done
		if !expandedCreators[info.Creator] {
			expandedCreators[info.Creator] = true
			deployed, err := d.client.GetContractsByCreator(ctx, info.Creator)
			if err != nil {
				d.logger.Warn("failed to query contracts by creator", "creator", info.Creator, "error", err)
			} else {
				d.logger.Info("found contracts by creator", "creator", info.Creator, "count", len(deployed))
				queue = append(queue, deployed...)
			}
		}
	}

	// Persist newly discovered contracts
	if len(newContracts) > 0 {
		if err := d.persistContracts(ctx, newContracts); err != nil {
			d.logger.Error("failed to persist discovered contracts", "error", err)
		}
	}

	d.logger.Info("discovery complete", "total_contracts", d.ContractCount())
	return nil
}

// expandFromCodesAndCreators re-queries known codes and creators for new contracts.
func (d *Discovery) expandFromCodesAndCreators(ctx context.Context, codes []int64, creators []string) error {
	seen := map[int64]bool{}
	for _, codeID := range codes {
		if seen[codeID] {
			continue
		}
		seen[codeID] = true

		siblings, err := d.client.GetContractsByCode(ctx, codeID)
		if err != nil {
			d.logger.Warn("failed to query contracts by code", "code_id", codeID, "error", err)
			continue
		}

		for _, addr := range siblings {
			d.mu.RLock()
			_, known := d.contracts[addr]
			d.mu.RUnlock()
			if known {
				continue
			}

			info, err := d.client.GetContractInfo(ctx, addr)
			if err != nil {
				d.logger.Warn("skipping contract", "address", addr, "error", err)
				continue
			}

			d.mu.Lock()
			d.contracts[addr] = info
			d.mu.Unlock()

			d.logger.Info("discovered new contract via refresh",
				"address", addr, "code_id", info.CodeID, "label", info.Label,
			)

			if err := d.persistContracts(ctx, []*ContractInfo{info}); err != nil {
				d.logger.Error("failed to persist contract", "address", addr, "error", err)
			}
		}
	}

	seenCreator := map[string]bool{}
	for _, creator := range creators {
		if seenCreator[creator] {
			continue
		}
		seenCreator[creator] = true

		deployed, err := d.client.GetContractsByCreator(ctx, creator)
		if err != nil {
			d.logger.Warn("failed to query contracts by creator", "creator", creator, "error", err)
			continue
		}

		for _, addr := range deployed {
			d.mu.RLock()
			_, known := d.contracts[addr]
			d.mu.RUnlock()
			if known {
				continue
			}

			info, err := d.client.GetContractInfo(ctx, addr)
			if err != nil {
				d.logger.Warn("skipping contract", "address", addr, "error", err)
				continue
			}

			d.mu.Lock()
			d.contracts[addr] = info
			d.mu.Unlock()

			d.logger.Info("discovered new contract via creator refresh",
				"address", addr, "creator", creator, "label", info.Label,
			)

			if err := d.persistContracts(ctx, []*ContractInfo{info}); err != nil {
				d.logger.Error("failed to persist contract", "address", addr, "error", err)
			}
		}
	}

	return nil
}

// expandByCodeIDs discovers all contracts for the given code IDs via ContractsByCode.
// Used for contracts instantiated by factory sub-messages that aren't reachable via seed expansion.
func (d *Discovery) expandByCodeIDs(ctx context.Context, codeIDs []int64) error {
	var newContracts []*ContractInfo

	for _, codeID := range codeIDs {
		addrs, err := d.client.GetContractsByCode(ctx, codeID)
		if err != nil {
			d.logger.Warn("failed to query contracts by extra code_id", "code_id", codeID, "error", err)
			continue
		}

		added := 0
		for _, addr := range addrs {
			d.mu.RLock()
			_, known := d.contracts[addr]
			d.mu.RUnlock()
			if known {
				continue
			}

			info, err := d.client.GetContractInfo(ctx, addr)
			if err != nil {
				d.logger.Warn("skipping extra contract", "address", addr, "error", err)
				continue
			}

			d.mu.Lock()
			d.contracts[addr] = info
			d.mu.Unlock()
			newContracts = append(newContracts, info)
			added++
		}

		d.logger.Info("discovered contracts from extra code_id",
			"code_id", codeID, "total", len(addrs), "new", added)
	}

	if len(newContracts) > 0 {
		if err := d.persistContracts(ctx, newContracts); err != nil {
			d.logger.Error("failed to persist extra contracts", "error", err)
		}
	}

	return nil
}

// refreshLoop periodically re-runs discovery to catch newly deployed contracts.
func (d *Discovery) refreshLoop(ctx context.Context, seeds []string, extraCodeIDs []int64) {
	ticker := time.NewTicker(d.refreshEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.logger.Debug("refreshing contract discovery")
			if err := d.expand(ctx, seeds); err != nil {
				d.logger.Warn("discovery refresh failed", "error", err)
			}
			if len(extraCodeIDs) > 0 {
				if err := d.expandByCodeIDs(ctx, extraCodeIDs); err != nil {
					d.logger.Warn("extra code_id refresh failed", "error", err)
				}
			}
		}
	}
}

// loadCached loads previously discovered contracts from the database.
func (d *Discovery) loadCached(ctx context.Context) ([]*ContractInfo, error) {
	rows, err := d.pool.Query(ctx, `
		SELECT address, code_id, creator, admin, label
		FROM roi_tracker.discovered_contracts
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var contracts []*ContractInfo
	for rows.Next() {
		var c ContractInfo
		var admin *string
		if err := rows.Scan(&c.Address, &c.CodeID, &c.Creator, &admin, &c.Label); err != nil {
			return nil, err
		}
		if admin != nil {
			c.Admin = *admin
		}
		contracts = append(contracts, &c)
	}
	return contracts, rows.Err()
}

// persistContracts upserts discovered contracts into the database cache.
func (d *Discovery) persistContracts(ctx context.Context, contracts []*ContractInfo) error {
	batch := &pgx.Batch{}
	for _, c := range contracts {
		batch.Queue(`
			INSERT INTO roi_tracker.discovered_contracts
				(address, code_id, creator, admin, label, discovered_at)
			VALUES ($1, $2, $3, $4, $5, NOW())
			ON CONFLICT (address) DO UPDATE SET
				code_id = EXCLUDED.code_id,
				creator = EXCLUDED.creator,
				admin = EXCLUDED.admin,
				label = EXCLUDED.label
		`, c.Address, c.CodeID, c.Creator, c.Admin, c.Label)
	}

	br := d.pool.SendBatch(ctx, batch)
	defer br.Close()

	for range contracts {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("persisting contract: %w", err)
		}
	}
	return nil
}
