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
	pool              *pgxpool.Pool
	chainClient       *chain.Client
	priceFetcher      *price.Fetcher
	logger            *slog.Logger
	contractAddresses []string
	chainID           string
	stopCh            chan struct{}
}

// New creates a new event processor.
func New(pool *pgxpool.Pool, chainClient *chain.Client, priceFetcher *price.Fetcher, cfg *config.Config, logger *slog.Logger) *Processor {
	return &Processor{
		pool:              pool,
		chainClient:       chainClient,
		priceFetcher:      priceFetcher,
		logger:            logger,
		contractAddresses: cfg.Contract.Addresses,
		chainID:           cfg.Chain.ChainID,
		stopCh:            make(chan struct{}),
	}
}

// Start begins processing contract transactions from the chain via gRPC.
func (p *Processor) Start(ctx context.Context) error {
	if len(p.contractAddresses) == 0 {
		p.logger.Warn("no contract addresses configured, processor will idle until configured")
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.stopCh:
			return nil
		}
	}

	p.logger.Info("starting fee revenue processor",
		"contracts", len(p.contractAddresses),
		"chain_id", p.chainID,
	)

	ticker := time.NewTicker(10 * time.Second)
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
func (p *Processor) processNewTxs(ctx context.Context) error {
	var lastHeight int64
	err := p.pool.QueryRow(ctx, `
		SELECT last_processed_event_id FROM roi_tracker.sync_state WHERE id = 1
	`).Scan(&lastHeight)
	if err != nil {
		return err
	}

	var maxHeight int64 = lastHeight
	var processedCount int

	for _, contractAddr := range p.contractAddresses {
		txs, err := p.chainClient.QueryContractTxs(ctx, contractAddr, lastHeight, 100)
		if err != nil {
			p.logger.Warn("failed to query txs", "contract", contractAddr, "error", err)
			continue
		}

		for _, tx := range txs {
			if tx.Height > maxHeight {
				maxHeight = tx.Height
			}

			if tx.FeeUatom <= 0 {
				continue
			}

			feeUatom := decimal.NewFromInt(tx.FeeUatom)

			timestamp := tx.Timestamp
			if timestamp.IsZero() {
				timestamp = time.Now()
			}

			atomPrice, err := p.priceFetcher.GetPrice(ctx, timestamp)
			if err != nil {
				p.logger.Warn("failed to get historical price, using current",
					"tx_hash", tx.TxHash, "error", err)
				atomPrice, err = p.priceFetcher.GetCurrentPrice(ctx)
				if err != nil {
					p.logger.Error("failed to get any price", "error", err)
					continue
				}
			}

			usdValue := price.CalculateUSD(feeUatom, atomPrice)

			// Use tx_hash for dedup (wasm_event_id column repurposed as unique key)
			_, err = p.pool.Exec(ctx, `
				INSERT INTO roi_tracker.processed_burns
				(wasm_event_id, tx_hash, height, timestamp, uatom_amount, atom_price_usd, usd_value, sender)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
				ON CONFLICT (wasm_event_id) DO NOTHING
			`, tx.Height, tx.TxHash, tx.Height, timestamp, feeUatom.String(), atomPrice.String(), usdValue.String(), tx.Sender)

			if err != nil {
				p.logger.Error("failed to insert fee record", "tx_hash", tx.TxHash, "error", err)
				continue
			}

			processedCount++
			p.logger.Info("recorded fee revenue",
				"tx_hash", tx.TxHash,
				"height", tx.Height,
				"action", tx.Action,
				"fee_uatom", tx.FeeUatom,
				"atom_price", atomPrice.StringFixed(4),
				"usd", usdValue.StringFixed(6),
			)
		}
	}

	if maxHeight > lastHeight {
		_, err = p.pool.Exec(ctx, `
			UPDATE roi_tracker.sync_state
			SET last_processed_event_id = $1, contract_address = $2, chain_id = $3, updated_at = NOW()
			WHERE id = 1
		`, maxHeight, fmt.Sprintf("%d contracts", len(p.contractAddresses)), p.chainID)
		if err != nil {
			p.logger.Error("failed to update sync state", "error", err)
		}

		if processedCount > 0 {
			p.logger.Info("processing complete",
				"fees_recorded", processedCount,
				"last_height", maxHeight,
			)
		}
	}

	return nil
}
