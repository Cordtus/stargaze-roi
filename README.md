# Stargaze ROI Tracker

Tracks burn revenue from the Stargaze NFT marketplace on Cosmos Hub, measuring ROI against the 699,626 ATOM community pool grant ([Proposal #1017](https://www.mintscan.io/cosmos/proposals/1017)).

The service reads indexed CosmWasm events from a shared PostgreSQL database (populated by [CosmoFlow-Maps](https://github.com/cordt-sei/cosmoflow-maps)), fetches historical ATOM prices from CoinGecko, and serves a dashboard showing progress toward the $1.5M target.

## Prerequisites

- Go 1.25+
- PostgreSQL with the CosmoFlow-Maps `wasm_events` table
- CoinGecko API access (free tier works)

## Quick Start

```bash
# Clone and build
git clone https://github.com/cordt-sei/starmos-roi-tracker.git
cd starmos-roi-tracker
make build

# Set database connection (same DB as CosmoFlow-Maps)
export DATABASE_URL='postgres://user:pass@localhost:5432/cosmoflow?sslmode=disable'

# Run (processor + server)
./bin/tracker -config config.toml

# Or server-only mode (no event processing)
./bin/tracker -config config.toml --server-only
```

The dashboard is served at `http://localhost:8080`.

## Environment Variables

| Variable | Description | Required |
|---|---|---|
| `DATABASE_URL` | PostgreSQL connection string (overrides config.toml) | Yes |
| `CONTRACT_ADDRESS` | Stargaze marketplace contract address (overrides config.toml) | No |
| `COINGECKO_API_KEY` | CoinGecko demo API key for higher rate limits | No |

## Configuration

Edit `config.toml`:

```toml
[chain]
chain_id = "cosmoshub-4"

[contract]
address = ""              # Set after Stargaze deploys on Hub
burn_action = "burn"
burn_attribute = "amount"

[coingecko]
rate_limit_per_minute = 10  # Free tier: 10-30/min
# api_key = ""              # Optional: demo key for higher limits

[target]
usd_amount = "1500000.00"
grant_date = "2025-11-21"
proposal_id = 1017
grant_uatom_amount = "699626000000"
multisig_address = "cosmos1vdqfavw0cu0fpvlcl52ku3qztt38szktlpfsuz"

[server]
port = 8080

[database]
conn_string = "postgres://localhost:5432/cosmoflow?sslmode=disable"
```

## Shared Database

This service shares a PostgreSQL database with CosmoFlow-Maps. It reads from the `wasm_events` table in the public schema and writes to its own `roi_tracker` schema.

### Required: `wasm_events` table (from CosmoFlow-Maps)

```sql
CREATE TABLE wasm_events (
    id BIGSERIAL PRIMARY KEY,
    tx_hash TEXT NOT NULL,
    height BIGINT NOT NULL,
    msg_index INTEGER NOT NULL DEFAULT 0,
    contract_address TEXT NOT NULL,
    sender TEXT NOT NULL,
    action TEXT,
    attributes JSONB NOT NULL DEFAULT '{}',
    block_time TIMESTAMPTZ
);

CREATE INDEX idx_wasm_events_contract ON wasm_events(contract_address, id);
```

### Auto-created: `roi_tracker` schema

The tracker creates these tables on startup:
- `roi_tracker.sync_state` - cursor tracking
- `roi_tracker.processed_burns` - burn records with USD values
- `roi_tracker.price_cache` - CoinGecko price cache (1-min granularity)
- `roi_tracker.stats_cache` - aggregated stats (trigger-updated)

## API Endpoints

| Endpoint | Description |
|---|---|
| `GET /api/stats` | Aggregated burn statistics, progress, grant info |
| `GET /api/transactions?limit=100&offset=0` | Paginated burn transactions |
| `GET /health` | Service health check |

## Development

```bash
make build    # Build binary
make test     # Run tests with race detector
make fmt      # Format code
make lint     # Run golangci-lint
make run      # Build and run
make server   # Server-only mode
```

## Docker

```bash
docker build -t starmos-roi-tracker .
docker run -e DATABASE_URL='postgres://...' -p 8080:8080 starmos-roi-tracker
```

## Fly.io Deployment

```bash
fly secrets set DATABASE_URL='postgres://...'
fly deploy
```

## Architecture

```
cmd/tracker/main.go           Entry point
internal/
  config/                     TOML config + env overrides
  db/                         Schema, queries (roi_tracker schema)
  price/                      CoinGecko client (rate limiting, retry, caching)
  processor/                  Polls wasm_events, processes burns
  server/                     HTTP API + static file serving
static/                       Frontend (HTML/CSS/JS)
```

## License

Apache License 2.0
