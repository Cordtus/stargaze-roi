// Package processor handles processing wasm events from the chain into burn records.
package processor

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/cordt-sei/starmos-roi-tracker/internal/chain"
	"github.com/cordt-sei/starmos-roi-tracker/internal/config"
	"github.com/cordt-sei/starmos-roi-tracker/internal/price"
)

// Processor queries the chain for wasm events and creates burn records with USD values.
type Processor struct {
	pool              *pgxpool.Pool
	chainClient       *chain.Client
	priceFetcher      *price.Fetcher
	logger            *slog.Logger
	contractAddresses []string
	burnAction        string
	burnAttribute     string
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
		burnAction:        cfg.Contract.BurnAction,
		burnAttribute:     cfg.Contract.BurnAttribute,
		chainID:           cfg.Chain.ChainID,
		stopCh:            make(chan struct{}),
	}
}

// Start begins processing wasm events from the chain via gRPC.
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

	p.logger.Info("starting event processor",
		"contracts", len(p.contractAddresses),
		"burn_action", p.burnAction,
		"chain_id", p.chainID,
	)

	// Process loop
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.stopCh:
			return nil
		case <-ticker.C:
			if err := p.processNewEvents(ctx); err != nil {
				p.logger.Warn("failed to process events", "error", err)
			}
		}
	}
}

// Stop gracefully stops the processor.
func (p *Processor) Stop() {
	close(p.stopCh)
}

// processNewEvents queries the chain for new wasm events and processes burns.
func (p *Processor) processNewEvents(ctx context.Context) error {
	// Get last processed height from sync state
	var lastHeight int64
	err := p.pool.QueryRow(ctx, `
		SELECT last_processed_event_id FROM roi_tracker.sync_state WHERE id = 1
	`).Scan(&lastHeight)
	if err != nil {
		return err
	}

	var maxHeight int64 = lastHeight
	var processedCount int

	// Query each contract
	for _, contractAddr := range p.contractAddresses {
		events, err := p.chainClient.QueryWasmEvents(ctx, contractAddr, lastHeight, 100)
		if err != nil {
			p.logger.Warn("failed to query events", "contract", contractAddr, "error", err)
			continue
		}

		for _, evt := range events {
			if evt.Height > maxHeight {
				maxHeight = evt.Height
			}

			if !p.isBurnAction(evt.Action) {
				continue
			}

			burnAmount, ok := p.extractBurnAmount(evt.Attrs)
			if !ok {
				continue
			}

			timestamp := evt.Timestamp
			if timestamp.IsZero() {
				timestamp = time.Now()
			}

			atomPrice, err := p.priceFetcher.GetPrice(ctx, timestamp)
			if err != nil {
				p.logger.Warn("failed to get historical price, using current",
					"tx_hash", evt.TxHash,
					"error", err,
				)
				atomPrice, err = p.priceFetcher.GetCurrentPrice(ctx)
				if err != nil {
					p.logger.Error("failed to get any price", "error", err)
					continue
				}
			}

			usdValue := price.CalculateUSD(burnAmount, atomPrice)

			_, err = p.pool.Exec(ctx, `
				INSERT INTO roi_tracker.processed_burns
				(wasm_event_id, tx_hash, height, timestamp, uatom_amount, atom_price_usd, usd_value, sender)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
				ON CONFLICT (wasm_event_id) DO NOTHING
			`, evt.Height, evt.TxHash, evt.Height, timestamp, burnAmount.String(), atomPrice.String(), usdValue.String(), evt.Sender)

			if err != nil {
				p.logger.Error("failed to insert burn", "tx_hash", evt.TxHash, "error", err)
				continue
			}

			processedCount++
			p.logger.Info("processed burn",
				"tx_hash", evt.TxHash,
				"height", evt.Height,
				"contract", contractAddr,
				"uatom", burnAmount.String(),
				"atom_price", atomPrice.StringFixed(4),
				"usd", usdValue.StringFixed(6),
			)
		}
	}

	// Update sync state with the highest height we've seen
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
				"burns_processed", processedCount,
				"last_height", maxHeight,
			)
		}
	}

	return nil
}

// isBurnAction checks if the action indicates a burn event.
func (p *Processor) isBurnAction(action string) bool {
	return action == p.burnAction || action == "burn" || action == "burn_tokens" || action == "burn_from"
}

// extractBurnAmount extracts the burn amount from event attributes.
func (p *Processor) extractBurnAmount(attrs map[string]string) (decimal.Decimal, bool) {
	// Try configured attribute first
	amountStr := attrs[p.burnAttribute]
	if amountStr == "" {
		amountStr = attrs["amount"]
	}
	if amountStr == "" {
		amountStr = attrs["burn_amount"]
	}
	if amountStr == "" {
		return decimal.Zero, false
	}

	// Clean the amount string - remove denom suffix
	amountStr = strings.TrimSuffix(amountStr, "uatom")
	amountStr = strings.TrimSuffix(amountStr, "atom")
	amountStr = strings.TrimSpace(amountStr)

	// Parse as decimal
	amount, err := decimal.NewFromString(amountStr)
	if err != nil {
		return decimal.Zero, false
	}

	// Must be positive
	if amount.LessThanOrEqual(decimal.Zero) {
		return decimal.Zero, false
	}

	return amount, true
}
