// Package db handles database operations for the burn tracker.
package db

// Schema contains the SQL statements to initialize the ROI tracker schema.
// This schema lives alongside CosmoFlow-Maps tables in the same database.
const Schema = `
-- Create schema for ROI tracker (separate from CosmoFlow-Maps public schema)
CREATE SCHEMA IF NOT EXISTS roi_tracker;

-- Sync state: tracks last processed height per contract
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

-- Discovered contracts: cached results of contract discovery (code_id + creator expansion)
CREATE TABLE IF NOT EXISTS roi_tracker.discovered_contracts (
    address TEXT PRIMARY KEY,
    code_id BIGINT NOT NULL,
    creator TEXT NOT NULL,
    admin TEXT,
    label TEXT,
    discovered_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Processed fee transactions with USD values, deduped by tx_hash
CREATE TABLE IF NOT EXISTS roi_tracker.processed_burns (
    id BIGSERIAL PRIMARY KEY,
    wasm_event_id BIGINT,
    tx_hash TEXT NOT NULL UNIQUE,
    height BIGINT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    uatom_amount NUMERIC NOT NULL,
    atom_price_usd NUMERIC NOT NULL,
    usd_value NUMERIC NOT NULL,
    sender TEXT,
    action TEXT,
    contract TEXT,
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
CREATE INDEX IF NOT EXISTS idx_processed_burns_action ON roi_tracker.processed_burns(action);
CREATE INDEX IF NOT EXISTS idx_processed_burns_contract ON roi_tracker.processed_burns(contract);
CREATE INDEX IF NOT EXISTS idx_discovered_contracts_code ON roi_tracker.discovered_contracts(code_id);
CREATE INDEX IF NOT EXISTS idx_discovered_contracts_creator ON roi_tracker.discovered_contracts(creator);

-- Function to update stats cache after insert
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

// Migration contains SQL to migrate from the old schema (wasm_event_id unique)
// to the new schema (tx_hash unique, action/contract columns).
// Safe to run repeatedly -- all operations are idempotent.
const Migration = `
-- Add new columns if missing
ALTER TABLE roi_tracker.processed_burns ADD COLUMN IF NOT EXISTS action TEXT;
ALTER TABLE roi_tracker.processed_burns ADD COLUMN IF NOT EXISTS contract TEXT;

-- Migrate unique constraint from wasm_event_id to tx_hash.
-- Drop old constraint if it exists, add new one if missing.
DO $$
BEGIN
    -- Drop old wasm_event_id unique constraint
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'processed_burns_wasm_event_id_key'
        AND conrelid = 'roi_tracker.processed_burns'::regclass
    ) THEN
        ALTER TABLE roi_tracker.processed_burns DROP CONSTRAINT processed_burns_wasm_event_id_key;
    END IF;

    -- Make wasm_event_id nullable (no longer required)
    ALTER TABLE roi_tracker.processed_burns ALTER COLUMN wasm_event_id DROP NOT NULL;

    -- Add tx_hash unique constraint if missing
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'processed_burns_tx_hash_key'
        AND conrelid = 'roi_tracker.processed_burns'::regclass
    ) THEN
        ALTER TABLE roi_tracker.processed_burns ADD CONSTRAINT processed_burns_tx_hash_key UNIQUE (tx_hash);
    END IF;
END
$$;
`
