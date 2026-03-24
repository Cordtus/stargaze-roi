// Package db handles database operations for the burn tracker.
package db

// Schema contains the SQL statements to initialize the ROI tracker schema.
// This schema lives alongside CosmoFlow-Maps tables in the same database.
const Schema = `
-- Create schema for ROI tracker (separate from CosmoFlow-Maps public schema)
CREATE SCHEMA IF NOT EXISTS roi_tracker;

-- Sync state: tracks last processed wasm_event from CosmoFlow-Maps
CREATE TABLE IF NOT EXISTS roi_tracker.sync_state (
    id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    last_processed_event_id BIGINT NOT NULL DEFAULT 0,
    contract_address TEXT NOT NULL DEFAULT '',
    chain_id TEXT NOT NULL DEFAULT 'cosmoshub-4',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Initialize sync_state if empty
INSERT INTO roi_tracker.sync_state (id, last_processed_event_id)
VALUES (1, 0)
ON CONFLICT (id) DO NOTHING;

-- Processed burns with USD values
-- Derived from wasm_events in CosmoFlow-Maps with price data added
CREATE TABLE IF NOT EXISTS roi_tracker.processed_burns (
    id BIGSERIAL PRIMARY KEY,
    wasm_event_id BIGINT NOT NULL UNIQUE,
    tx_hash TEXT NOT NULL,
    height BIGINT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    uatom_amount NUMERIC NOT NULL,
    atom_price_usd NUMERIC NOT NULL,
    usd_value NUMERIC NOT NULL,
    sender TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Price cache with 1-minute granularity
CREATE TABLE IF NOT EXISTS roi_tracker.price_cache (
    timestamp_minute TIMESTAMPTZ PRIMARY KEY,
    atom_price_usd NUMERIC NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Cached aggregate statistics
CREATE TABLE IF NOT EXISTS roi_tracker.stats_cache (
    id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    total_uatom_burned NUMERIC NOT NULL DEFAULT 0,
    total_usd_burned NUMERIC NOT NULL DEFAULT 0,
    transaction_count BIGINT NOT NULL DEFAULT 0,
    first_burn_timestamp TIMESTAMPTZ,
    last_burn_timestamp TIMESTAMPTZ,
    last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Initialize stats_cache if empty
INSERT INTO roi_tracker.stats_cache (id, total_uatom_burned, total_usd_burned, transaction_count)
VALUES (1, 0, 0, 0)
ON CONFLICT (id) DO NOTHING;

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_processed_burns_timestamp ON roi_tracker.processed_burns(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_processed_burns_height ON roi_tracker.processed_burns(height DESC);
CREATE INDEX IF NOT EXISTS idx_processed_burns_tx ON roi_tracker.processed_burns(tx_hash);

-- Function to update stats cache after burn insert
CREATE OR REPLACE FUNCTION roi_tracker.update_stats_cache()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE roi_tracker.stats_cache SET
        total_uatom_burned = (SELECT COALESCE(SUM(uatom_amount), 0) FROM roi_tracker.processed_burns),
        total_usd_burned = (SELECT COALESCE(SUM(usd_value), 0) FROM roi_tracker.processed_burns),
        transaction_count = (SELECT COUNT(*) FROM roi_tracker.processed_burns),
        first_burn_timestamp = (SELECT MIN(timestamp) FROM roi_tracker.processed_burns),
        last_burn_timestamp = (SELECT MAX(timestamp) FROM roi_tracker.processed_burns),
        last_updated = NOW()
    WHERE id = 1;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to auto-update stats on insert (per statement for efficiency)
DROP TRIGGER IF EXISTS trg_update_stats ON roi_tracker.processed_burns;
CREATE TRIGGER trg_update_stats
    AFTER INSERT ON roi_tracker.processed_burns
    FOR EACH STATEMENT
    EXECUTE FUNCTION roi_tracker.update_stats_cache();
`
