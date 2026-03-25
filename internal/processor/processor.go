// Package processor handles processing contract transactions from the chain into fee revenue records.
package processor

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/cordt-sei/starmos-roi-tracker/internal/chain"
	"github.com/cordt-sei/starmos-roi-tracker/internal/config"
	"github.com/cordt-sei/starmos-roi-tracker/internal/price"
)

// Processor queries the chain for Stargaze contract transactions and records fee revenue.
type Processor struct {
	pool         *pgxpool.Pool
	chainClient  *chain.Client
	discovery    *chain.Discovery
	priceFetcher *price.Fetcher
	logger       *slog.Logger
	chainID      string
	startHeight  int64
	skipCodeIDs  map[int64]bool
	pollInterval time.Duration
	batchSize    int
	stopCh       chan struct{}
}

// New creates a new event processor.
func New(pool *pgxpool.Pool, chainClient *chain.Client, discovery *chain.Discovery, priceFetcher *price.Fetcher, cfg *config.Config, logger *slog.Logger) *Processor {
	interval := 10 * time.Second
	if cfg.Processor.PollSeconds > 0 {
		interval = time.Duration(cfg.Processor.PollSeconds) * time.Second
	}
	skipCodes := map[int64]bool{}
	for _, id := range cfg.Contract.SkipCodeIDs {
		skipCodes[id] = true
	}
	return &Processor{
		pool:         pool,
		chainClient:  chainClient,
		discovery:    discovery,
		priceFetcher: priceFetcher,
		logger:       logger,
		chainID:      cfg.Chain.ChainID,
		startHeight:  cfg.Contract.StartHeight,
		skipCodeIDs:  skipCodes,
		pollInterval: interval,
		batchSize:    100,
		stopCh:       make(chan struct{}),
	}
}

// Start begins processing contract transactions from the chain via gRPC.
func (p *Processor) Start(ctx context.Context) error {
	queryable := p.discovery.QueryableContracts(p.skipCodeIDs)
	if len(queryable) == 0 {
		p.logger.Warn("no queryable contracts discovered, processor will idle")
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.stopCh:
			return nil
		}
	}

	// Initialize sync state to configured start height if it's still at 0
	if p.startHeight > 0 {
		_, err := p.pool.Exec(ctx, `
			UPDATE roi_tracker.sync_state
			SET last_processed_event_id = $1
			WHERE id = 1 AND last_processed_event_id = 0
		`, p.startHeight)
		if err != nil {
			p.logger.Warn("failed to set start height", "error", err)
		}
	}

	p.logger.Info("starting fee revenue processor",
		"queryable_contracts", len(queryable),
		"total_contracts", p.discovery.ContractCount(),
		"skipped_code_ids", len(p.skipCodeIDs),
		"chain_id", p.chainID,
	)

	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.stopCh:
			return nil
		case <-ticker.C:
			if err := p.processNewTxs(ctx); err != nil {
				p.logger.Warn("failed to process transactions", "error", err)
			}
		}
	}
}

// Stop gracefully stops the processor.
func (p *Processor) Stop() {
	close(p.stopCh)
}

// processNewTxs queries the chain for new contract transactions and records their fees.
// Transactions are deduped by tx_hash across all contracts.
func (p *Processor) processNewTxs(ctx context.Context) error {
	var lastHeight int64
	err := p.pool.QueryRow(ctx, `
		SELECT last_processed_event_id FROM roi_tracker.sync_state WHERE id = 1
	`).Scan(&lastHeight)
	if err != nil {
		return err
	}

	contracts := p.discovery.QueryableContracts(p.skipCodeIDs)
	if len(contracts) == 0 {
		return nil
	}

	// Collect all txs across all contracts, dedup by tx_hash
	seen := map[string]*chain.ContractTx{}
	var maxHeight int64 = lastHeight

	for _, contractAddr := range contracts {
		txs, err := p.chainClient.QueryContractTxs(ctx, contractAddr, lastHeight, p.batchSize)
		if err != nil {
			p.logger.Warn("failed to query txs", "contract", contractAddr, "error", err)
			continue
		}

		for i := range txs {
			tx := &txs[i]
			if tx.Height > maxHeight {
				maxHeight = tx.Height
			}
			// Keep first occurrence per tx_hash (avoid double-counting multi-contract txs)
			if _, exists := seen[tx.TxHash]; !exists {
				seen[tx.TxHash] = tx
			}
		}
	}

	var processedCount int
	var dbErrCount int

	for _, tx := range seen {
		if tx.FeeUatom <= 0 {
			continue
		}

		feeUatom := decimal.NewFromInt(tx.FeeUatom)

		timestamp := tx.Timestamp
		if timestamp.IsZero() {
			timestamp = time.Now()
		}

		// Try to get price from cache; if unavailable, insert with zero (backfill later)
		atomPrice, err := p.priceFetcher.GetPrice(ctx, timestamp)
		if err != nil {
			atomPrice = decimal.Zero
		}

		usdValue := price.CalculateUSD(feeUatom, atomPrice)

		_, err = p.pool.Exec(ctx, `
			INSERT INTO roi_tracker.processed_burns
			(tx_hash, height, timestamp, uatom_amount, atom_price_usd, usd_value, sender, action, contract)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT (tx_hash) DO NOTHING
		`, tx.TxHash, tx.Height, timestamp, feeUatom.String(), atomPrice.String(), usdValue.String(),
			tx.Sender, tx.Action, tx.Contract)

		if err != nil {
			p.logger.Error("failed to insert fee record", "tx_hash", tx.TxHash, "error", err)
			dbErrCount++
			continue
		}

		processedCount++
	}

	// Only block cursor on DB errors (not price failures)
	if dbErrCount > 0 {
		p.logger.Warn("DB insert errors, not advancing cursor",
			"db_errors", dbErrCount, "processed", processedCount)
		return nil
	}

	if maxHeight > lastHeight {
		_, err = p.pool.Exec(ctx, `
			UPDATE roi_tracker.sync_state
			SET last_processed_event_id = $1,
			    contract_address = $2,
			    chain_id = $3,
			    updated_at = NOW()
			WHERE id = 1
		`, maxHeight, fmt.Sprintf("%d contracts", p.discovery.ContractCount()), p.chainID)
		if err != nil {
			p.logger.Error("failed to update sync state", "error", err)
		}

		if processedCount > 0 {
			p.logger.Info("processing complete",
				"fees_recorded", processedCount,
				"last_height", maxHeight,
				"unique_txs", len(seen),
			)
		}
	}

	return nil
}
