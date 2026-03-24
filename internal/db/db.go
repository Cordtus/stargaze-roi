package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
)

// BurnTransaction represents a processed burn event with USD value.
type BurnTransaction struct {
	ID           int64
	WasmEventID  int64
	TxHash       string
	Height       int64
	Timestamp    time.Time
	UatomAmount  decimal.Decimal
	AtomPriceUSD decimal.Decimal
	USDValue     decimal.Decimal
	Sender       string
	CreatedAt    time.Time
}

// Stats represents aggregated burn statistics.
type Stats struct {
	TotalUatomBurned   decimal.Decimal
	TotalUSDBurned     decimal.Decimal
	TransactionCount   int64
	AvgAtomPriceUSD    decimal.Decimal // Volume-weighted average price across all burns
	FirstBurnTimestamp *time.Time
	LastBurnTimestamp  *time.Time
	LastUpdated        time.Time
	LastProcessedID    int64
	ContractAddress    string
	ChainID            string
}

// InitSchema initializes the ROI tracker schema in the shared database.
func InitSchema(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, Schema)
	if err != nil {
		return fmt.Errorf("initializing schema: %w", err)
	}
	return nil
}

// GetStats returns the current aggregate statistics from the cache.
func GetStats(ctx context.Context, pool *pgxpool.Pool) (Stats, error) {
	var stats Stats
	var totalUatom, totalUSD string
	var firstBurn, lastBurn *time.Time

	row := pool.QueryRow(ctx, `
		SELECT
			s.total_uatom_burned::TEXT,
			s.total_usd_burned::TEXT,
			s.transaction_count,
			s.first_burn_timestamp,
			s.last_burn_timestamp,
			s.last_updated,
			ss.last_processed_event_id,
			ss.contract_address,
			ss.chain_id
		FROM roi_tracker.stats_cache s, roi_tracker.sync_state ss
		WHERE s.id = 1 AND ss.id = 1
	`)

	err := row.Scan(
		&totalUatom,
		&totalUSD,
		&stats.TransactionCount,
		&firstBurn,
		&lastBurn,
		&stats.LastUpdated,
		&stats.LastProcessedID,
		&stats.ContractAddress,
		&stats.ChainID,
	)
	if err != nil {
		return stats, fmt.Errorf("querying stats: %w", err)
	}

	stats.TotalUatomBurned, _ = decimal.NewFromString(totalUatom)
	stats.TotalUSDBurned, _ = decimal.NewFromString(totalUSD)
	stats.FirstBurnTimestamp = firstBurn
	stats.LastBurnTimestamp = lastBurn

	// Calculate volume-weighted average ATOM price from processed burns
	// VWAP = SUM(price * amount) / SUM(amount)
	var avgPriceStr *string
	err = pool.QueryRow(ctx, `
		SELECT (SUM(atom_price_usd * uatom_amount) / NULLIF(SUM(uatom_amount), 0))::TEXT
		FROM roi_tracker.processed_burns
	`).Scan(&avgPriceStr)
	if err == nil && avgPriceStr != nil {
		stats.AvgAtomPriceUSD, _ = decimal.NewFromString(*avgPriceStr)
	}

	return stats, nil
}

// GetRecentBurns returns the most recent burn transactions.
func GetRecentBurns(ctx context.Context, pool *pgxpool.Pool, limit, offset int) ([]BurnTransaction, int64, error) {
	var total int64
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM roi_tracker.processed_burns`).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("counting burns: %w", err)
	}

	rows, err := pool.Query(ctx, `
		SELECT id, wasm_event_id, tx_hash, height, timestamp,
		       uatom_amount::TEXT, atom_price_usd::TEXT, usd_value::TEXT,
		       sender, created_at
		FROM roi_tracker.processed_burns
		ORDER BY timestamp DESC
		LIMIT $1 OFFSET $2
	`, limit, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("querying burns: %w", err)
	}
	defer rows.Close()

	var burns []BurnTransaction
	for rows.Next() {
		var b BurnTransaction
		var uatom, priceUSD, usdValue string

		if err := rows.Scan(&b.ID, &b.WasmEventID, &b.TxHash, &b.Height, &b.Timestamp,
			&uatom, &priceUSD, &usdValue, &b.Sender, &b.CreatedAt); err != nil {
			return nil, 0, fmt.Errorf("scanning burn row: %w", err)
		}

		b.UatomAmount, _ = decimal.NewFromString(uatom)
		b.AtomPriceUSD, _ = decimal.NewFromString(priceUSD)
		b.USDValue, _ = decimal.NewFromString(usdValue)

		burns = append(burns, b)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterating burns: %w", err)
	}

	return burns, total, nil
}

// CachePrice stores a price in the cache.
func CachePrice(ctx context.Context, pool *pgxpool.Pool, minute time.Time, price decimal.Decimal) error {
	_, err := pool.Exec(ctx, `
		INSERT INTO roi_tracker.price_cache (timestamp_minute, atom_price_usd, fetched_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (timestamp_minute) DO UPDATE SET atom_price_usd = EXCLUDED.atom_price_usd
	`, minute, price.String())
	return err
}

// GetCachedPrice retrieves a cached price for the given minute.
func GetCachedPrice(ctx context.Context, pool *pgxpool.Pool, minute time.Time) (decimal.Decimal, bool, error) {
	var priceStr string
	err := pool.QueryRow(ctx, `
		SELECT atom_price_usd::TEXT FROM roi_tracker.price_cache WHERE timestamp_minute = $1
	`, minute).Scan(&priceStr)

	if err == pgx.ErrNoRows {
		return decimal.Zero, false, nil
	}
	if err != nil {
		return decimal.Zero, false, err
	}

	price, err := decimal.NewFromString(priceStr)
	return price, true, err
}
