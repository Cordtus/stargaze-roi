# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Stargaze ROI Tracker measures the return on a $1.5M ATOM grant given to Stargaze for migrating their NFT marketplace to Cosmos Hub. It tracks ATOM burns from a CosmWasm contract, converts them to USD using CoinGecko prices, and displays progress toward the $1.5M target.

This service shares a PostgreSQL database with CosmoFlow-Maps. It reads from a `wasm_events` table (populated by CosmoFlow-Maps) and writes to its own `roi_tracker` schema.

## Build and Run Commands

```bash
make build          # Build binary to bin/tracker
make run            # Build and run with processor + server
make server         # Run server-only mode (no event processing)
make test           # Run tests
make fmt            # Format code
make lint           # Run golangci-lint
make deploy         # Deploy to Fly.io
```

## Architecture

```
cmd/tracker/main.go     Entry point, wires components together
internal/
  config/               TOML config parsing, DATABASE_URL env override
  processor/            Polls wasm_events table, processes burns, fetches prices
  price/                CoinGecko API client with rate limiting and caching
  server/               HTTP API (/api/stats, /api/transactions, /health)
  db/                   PostgreSQL schema and queries (roi_tracker schema)
static/                 Frontend HTML/CSS/JS
```

## Database Design

The tracker creates a `roi_tracker` schema in the shared PostgreSQL database:

- `sync_state` - Cursor tracking (last_processed_event_id)
- `processed_burns` - Burn records with USD values
- `price_cache` - CoinGecko price cache (1-minute granularity)
- `stats_cache` - Aggregated stats (auto-updated via trigger)

## External Dependency

Requires `wasm_events` table in the public schema (from CosmoFlow-Maps):

```sql
SELECT id, tx_hash, height, sender, action, attributes, block_time
FROM wasm_events
WHERE contract_address = $1 AND id > $2
ORDER BY id ASC
```

The processor filters for `action = 'burn'` and extracts `amount` from the JSONB `attributes` column.

## Configuration

Edit `config.toml` or set `DATABASE_URL` environment variable. Key settings:

- `contract.address` - Stargaze marketplace contract (empty = processor idles)
- `contract.burn_action` - Event action to match (default: "burn")
- `target.usd_amount` - Grant target for progress calculation
- `coingecko.rate_limit_per_minute` - API rate limit (free tier: 10-30/min)

## Deployment

Deploys to Fly.io. Set database secret:
```bash
fly secrets set DATABASE_URL='postgres://...'
```
