## How the indexer works (step by step)

---

### 1. Finding the contracts *(Discovery)*

**Process**

* Start with 4 seed contract addresses

* For each seed, call:

  ```txt
  cosmwasm.wasm.v1.Query.ContractInfo
  ```

  via gRPC (libyaci server reflection -- proto descriptors cached for connection lifetime)

* Extract:

  * `code_id`
  * `creator`

* Expand via:

  ```txt
  ContractsByCode(code_id)
  ContractsByCreator(creator)
  ```

**Outcome**

* 10 unique `code_id`s discovered
* 5058 total contracts from one creator (`cosmos1s8qx0zvz8yd6e...`)
* Results cached in `roi_tracker.discovered_contracts` table (no re-discovery on restart)

**Coverage added**

* VMF v1 contracts (code 260, Aug 2025)
* Migration factories (codes 411, 412)
* 5043 WL Merkle whitelist contracts
* Minter Querier (code 428)

---

### 2. Finding transactions

#### 2a. Contract transactions *(wasm._contract_address)*

For each queryable contract (~15 non-WL-Merkle contracts):

```txt
cosmos.tx.v1beta1.Service.GetTxsEvent
query: "wasm._contract_address='<addr>' AND tx.height > <cursor>"
order_by: ORDER_BY_ASC
```

**Covers:**

* `MsgExecuteContract` -- listings, offers, claims, transfers, burns, mints
* `MsgInstantiateContract` -- factory creating child contracts
* `MsgMigrateContract` -- contract upgrades

**Pagination:**

* Uses `nextKey` when provided by the node
* Falls back to height-based re-querying when the node returns a full page with empty `nextKey` (archive node pagination cap workaround)

**Code 417 (WL Merkle) exclusion:**

* 5043 contracts instantiated by Migration VMF (code 410)
* Instantiation emits events for both factory and child contract
* Querying the factory captures these transactions
* Skipping individual WL contracts avoids 5043 redundant queries

#### 2b. Creator transactions *(message.sender)*

For each unique creator address:

```txt
query: "message.sender='<creator>' AND message.action='<action>' AND tx.height > <cursor>"
```

**Actions queried:**

* `/cosmwasm.wasm.v1.MsgStoreCode` -- contract code uploads (no `wasm._contract_address` event)
* `/cosmwasm.wasm.v1.MsgUpdateAdmin` -- admin updates
* `/cosmwasm.wasm.v1.MsgClearAdmin` -- admin removal

**Why needed:**

* `MsgStoreCode` txs don't emit `wasm._contract_address` events
* 48 code upload txs totaling ~44 ATOM in gas fees were missed without this
* Same pagination logic (nextKey + height fallback) as contract queries

---

### 3. Data extracted per transaction

Each `txResponse`:

```json
{
  "height": "30232803",
  "txhash": "2038F829...",
  "code": 0,
  "events": [...],
  "timestamp": "2026-03-17T...",
  "gasWanted": "...",
  "gasUsed": "..."
}
```

**Filter**

* Only `code == 0` (successful transactions)
* Deduped by `tx_hash` (a single tx can appear in queries for multiple contracts)

---

**Extracted fields**

* **Gas fee**

  * From `coin_spent` event -> `amount` attribute
  * Verified: matches `tx.authInfo.fee.amount` in all sampled txs (no mismatches)
  * Only `uatom` denom parsed (all fees on Cosmos Hub are uatom)

* **Sender**

  * From `coin_spent` event -> `spender` attribute

* **Action**

  * From `wasm` event -> `action` attribute (for contract txs)
  * From `message.action` (for creator txs like MsgStoreCode)

* **Contract**

  * Known from query input

---

### 4. Price conversion

**Two valuations displayed:**

1. **Current value** -- total ATOM fees * today's ATOM price
2. **Historical value** -- sum of each tx's fee * ATOM price on the day of that tx

**Historical price source:**

* CoinGecko `/coins/cosmos/history` API (daily granularity)
* Backfill runs on startup, paced within CoinGecko free-tier rate limit (10 req/min)
* Cached in `price_cache` table at day-level granularity
* `GetPrice()` checks day-level cache first, falls back to API

**Formula**

```txt
usd_value = (fee_uatom / 1_000_000) * atom_price_usd
```

**Price-independent indexing:**

* Transactions are inserted with `atom_price_usd = 0` when price is unavailable
* The UI shows how many txs have prices ("X of Y txs priced")
* Prices can be backfilled separately without re-indexing txs

---

### 5. Deduplication

**Scenario**

* A single transaction can involve multiple contracts (e.g., factory instantiating a child)
* Same tx appears in queries for different contract addresses
* Creator txs (MsgStoreCode) could theoretically overlap with contract txs

**Process**

* In-memory dedup:

  ```go
  map[tx_hash]*ContractTx
  ```

* First occurrence retained

**Database constraint**

```sql
UNIQUE(tx_hash)
ON CONFLICT DO NOTHING
```

---

### 6. Cursor management

**State**

* `sync_state.last_processed_event_id` = highest processed block height

**Query**

```txt
tx.height > cursor
```

**Behavior**

* Cursor advances only when no DB insert errors occur
* Price lookup failures are non-blocking (tx inserted with zero price)
* On DB error: cursor stays, entire batch retries next cycle

---

### 7. Verified assumptions

From on-chain audit:

* `coin_spent.amount` matches `tx.authInfo.fee.amount` -- no mismatches found
* All fees are in `uatom` -- no multi-denom fees observed
* `wasm._contract_address` and `execute._contract_address` return identical results on this chain
* All contract interaction txs use `MsgExecuteContract` -- no exotic message types
* WL Merkle contracts have exactly 1 tx each (instantiation only, no post-deploy activity)
