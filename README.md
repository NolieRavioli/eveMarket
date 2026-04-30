# eveMarket API

eveMarket runs a market snapshot collector and an HTTP API server.

Default bind: `0.0.0.0:13307`

## Prerequisites

```bash
pip install -r requirements.txt
```

## Start

```bash
python eveMarket.py
```

Optional flags:

| Flag | Default | Description |
|------|---------|-------------|
| `--bind` | `0.0.0.0` | HTTP bind address |
| `--port` | `13307` | HTTP port |
| `--data` | `./data` | Data directory |
| `--sde` | `./sde` | SDE directory (auto-downloaded if missing) |
| `--interval` | `1200` | Collection interval in seconds (20 min) |
| `--no-inference` | | Disable inferred-trade processing |
| `--log-level` | `INFO` | Python logging level |

## Response formats

| Format | Content-Type | When used |
|--------|-------------|-----------|
| JSON | `application/json` | All single-object responses |
| NDJSON | `application/x-ndjson` | Streaming order/trade/history/contract endpoints |

NDJSON: one JSON object per line, chunked transfer encoding. Read line-by-line
without buffering the full response.

## Location IDs

All `{location_id}` path parameters accept any of:

| Kind | ID range | Example |
|------|----------|---------|
| Region | 10 000 000 – 10 999 999 | `10000002` (The Forge) |
| Constellation | 20 000 000 – 20 999 999 | `20000020` (Kimotoro) |
| Solar system | 30 000 000 – 30 999 999 | `30000142` (Jita) |
| NPC station | 60 000 000 – 63 999 999 | `60003760` (Jita 4-4) |
| Player structure | ≥ 1 000 000 000 000 | `1035466617946` |

## Endpoints

---

### `GET /`

Service overview. Returns an HTML page documenting all available routes (this README, rendered).

---

### `GET /health`

```
GET /health
```

**Response** `200 application/json`

```json
{
  "ok": true,
  "snapshots": 18,
  "latest_unix": 1777272798
}
```

| Field | Type | Description |
|-------|------|-------------|
| `ok` | bool | Always `true` |
| `snapshots` | int | Number of snapshots on disk |
| `latest_unix` | int\|null | Unix timestamp of the latest snapshot |

---

### `GET /currentOrders/{location_id}`

Current live orders from the latest snapshot, filtered to the given location.

```
GET /currentOrders/10000002
GET /currentOrders/60003760
```

**Response** `200 application/x-ndjson` — stream of order objects:

```json
{"order_id":7315521535,"type_id":3452,"region_id":10000001,"system_id":30000087,
 "location_id":60012142,"is_buy_order":false,"price":1010.0,"volume_remain":1,
 "volume_total":1,"min_volume":1,"range":"region","duration":7,
 "issued":"2026-04-22T14:35:48Z"}
```

| Field | Type | Description |
|-------|------|-------------|
| `order_id` | int | ESI order ID |
| `type_id` | int | Item type |
| `region_id` | int | Region the order was collected from |
| `system_id` | int | Solar system of the order |
| `location_id` | int | Station or structure ID |
| `is_buy_order` | bool | `true` = buy order |
| `price` | float | ISK per unit |
| `volume_remain` | int | Units remaining |
| `volume_total` | int | Original quantity |
| `min_volume` | int | Minimum fill size |
| `range` | string | Buy-order fill range: `"station"`, `"solarsystem"`, `"constellation"`, `"region"`, or `"1"`–`"40"` (jumps). Always `"region"` for sell orders. |
| `duration` | int | Order lifetime in days |
| `issued` | string | ISO 8601 UTC timestamp of issuance |

**Errors**

| Code | Reason |
|------|--------|
| `400` | Invalid `location_id` |
| `404` | Unknown location |
| `503` | No snapshots yet |

---

### `GET /orders`

The entire latest snapshot as a flat NDJSON stream — every region, every order.

```
GET /orders
```

**Response** `200 application/x-ndjson`

Response header `X-Snapshot-Unix` carries the snapshot unix timestamp.
Each row has the same fields as [`GET /currentOrders`](#get-currentorderslocation_id).

**Errors**

| Code | Reason |
|------|--------|
| `503` | No snapshots yet |

---

### `GET /orders/{unix_time}`

Orders from the snapshot **nearest** to `unix_time` (any direction).

```
GET /orders/1777247000
GET /orders/1777247000?location=30000142
```

**Query parameters**

| Param | Description |
|-------|-------------|
| `location` | Optional — filter by any location ID |

**Response** `200 application/x-ndjson`

Response header `X-Snapshot-Unix` carries the timestamp of the snapshot actually used.
Each row has the same fields as [`GET /currentOrders`](#get-currentorderslocation_id).

**Errors**

| Code | Reason |
|------|--------|
| `400` | Invalid `unix_time` or invalid `location` param |
| `404` | Unknown location |
| `503` | No snapshots yet |

---

### `GET /inferred/{location_id}`

Inferred trades derived from the latest snapshot pair, filtered to the given location.

Reads from the precomputed inferred cache (`data/inferred/infer-<unix>.jsonl`) when
available; falls back to live computation otherwise.

```
GET /inferred/10000002
GET /inferred/60003760
```

**Response** `200 application/x-ndjson` — stream of inferred-trade objects:

```json
{"actual":true,"order_id":7319060832,"type_id":13249,"region_id":10000002,
 "system_id":30000142,"is_buy_order":false,"price":18900000.0,"volume":1.0,
 "location_id":60003760,"buyer_range":null,"buyer_station_id":null}
```

| Field | Type | Description |
|-------|------|-------------|
| `actual` | bool | `true` = fill observed as volume delta; `false` = inferred from order disappearance |
| `order_id` | int | ESI order ID of the filled order |
| `type_id` | int | Item type |
| `region_id` | int | Region |
| `system_id` | int\|null | Solar system (null for some older records) |
| `is_buy_order` | bool | Whether the filled order was a buy |
| `price` | float | ISK per unit at time of fill |
| `volume` | float | Estimated units traded |
| `location_id` | int\|null | Concrete fill station, or `null` for non-station-range buys where the exact fill destination is unknown |
| `buyer_range` | string\|null | ESI range of the buy order (`"station"`, `"solarsystem"`, `"constellation"`, `"region"`, or jump count as string). `null` for sell-side fills. |
| `buyer_station_id` | int\|null | Station ID of the buyer's order. `null` for sell-side fills. |

Response header `X-Snapshot-Unix` carries the unix timestamp of the snapshot used.

**Errors**

| Code | Reason |
|------|--------|
| `400` | Invalid `location_id` |
| `404` | Unknown location |
| `503` | No snapshots yet, or fewer than 2 snapshots available |

---

### `GET /history/{range}`

Universe-wide precomputed history — one NDJSON row per precomputed region or NPC
station. Used by eveHauler to bulk-load fill caps without per-station POST requests.

Precomputed ranges: `7d`, `14d`, `30d`.

```
GET /history/7d
GET /history/30d
```

**Response** `200 application/x-ndjson`

```json
{"scope":"station","location_id":60003760,"range":"30d","types":{"34":{"buy":{...},"sell":{...}},...}}
```

| Field | Type | Description |
|-------|------|-------------|
| `scope` | string | `"region"` or `"station"` |
| `location_id` | int | Region or NPC station ID |
| `range` | string | Range label matching the request (e.g. `"30d"`) |
| `types` | object | Map of `type_id` string → `{"buy":{…},"sell":{…}}` — same shape as [`POST /stats`](#post-statslocation_id) response values |

Response headers:

| Header | Description |
|--------|-------------|
| `X-History-Range` | Range label used |
| `X-Snapshot-Unix` | Snapshot unix timestamp from precompute metadata (if available) |

**Errors**

| Code | Reason |
|------|--------|
| `400` | Invalid or non-precomputed range |

---

### `GET /contracts/courier`

Every currently-active public courier contract, universe-wide, from the latest
contracts snapshot.

```
GET /contracts/courier
```

**Response** `200 application/x-ndjson`

```json
{"contract_id":12345678,"region_id":10000002,"start_location_id":60003760,
 "end_location_id":60008494,"reward":5000000.0,"collateral":500000000.0,
 "volume":10000.0,"days_to_complete":3,"date_issued":"2026-04-22T14:00:00Z",
 "date_expired":"2026-04-29T14:00:00Z","title":""}
```

| Field | Type | Description |
|-------|------|-------------|
| `contract_id` | int | ESI contract ID |
| `region_id` | int | Region the contract was collected from |
| `start_location_id` | int | Pickup station or structure |
| `end_location_id` | int | Dropoff station or structure |
| `reward` | float | ISK reward offered |
| `collateral` | float | ISK collateral required |
| `volume` | float | Cargo volume in m³ |
| `days_to_complete` | int | Days to complete after acceptance |
| `date_issued` | string | ISO 8601 UTC |
| `date_expired` | string | ISO 8601 UTC — only non-expired contracts are returned |
| `title` | string | Contract title (may be empty) |

Response header `X-Contracts-Snapshot-Unix` carries the unix timestamp of the
snapshot used.

**Errors**

| Code | Reason |
|------|--------|
| `503` | No contracts snapshot yet |

---

### `POST /stats/{location_id}`

Order-book statistics from the latest snapshot scoped to the given location.
Served from precomputed files for regions and NPC stations (fast); falls back
to live snapshot scan for constellations, systems, and structures.

```
POST /stats/10000002
POST /stats/60003760
Content-Type: application/json

[34, 35, 36]
```

**Request body** — JSON array of `type_id` integers (max 1 MiB).

**Response** `200 application/json` — object keyed by stringified `type_id`:

```json
{
  "34": {
    "buy": {
      "weightedAverage": "4.07",
      "max": "4.18",
      "min": "0.56",
      "stddev": "0.83",
      "median": "4.09",
      "volume": "8192983945.0",
      "orderCount": "32",
      "percentile": "4.07"
    },
    "sell": {
      "weightedAverage": "4.54",
      "max": "999",
      "min": "4.18",
      "stddev": "2.33",
      "median": "4.75",
      "volume": "5078497821.0",
      "orderCount": "46",
      "percentile": "4.20"
    }
  }
}
```

All numeric fields are returned as strings to preserve precision.

| Field | Description |
|-------|-------------|
| `weightedAverage` | Volume-weighted mean price |
| `max` | Highest price in the order book |
| `min` | Lowest price in the order book |
| `stddev` | Volume-weighted standard deviation |
| `median` | Median price |
| `volume` | Total units on offer |
| `orderCount` | Number of orders |
| `percentile` | 5th percentile for sell side; 95th percentile for buy side (best realistic fill price) |

**Errors**

| Code | Reason |
|------|--------|
| `400` | Invalid `location_id`; malformed or empty body |
| `404` | Unknown location |
| `413` | Body exceeds 1 MiB |
| `503` | No snapshots yet |

---

### `POST /history/{location_id}/{range}`

Historic market statistics aggregated from this server's inferred-trade tape.
Identical shape to `/stats` so callers can use the same parser.

Served from precomputed files for the three fixed ranges (fast); for stations
the per-station file is used, otherwise the parent region file. Falls back to
live aggregation for custom ranges, system/constellation scopes, or before the
first precompute cycle finishes.

```
POST /history/10000002/30d
POST /history/60003760/7d
Content-Type: application/json

[34, 35, 36]
```

**Path parameters**

| Param | Format | Examples |
|-------|--------|---------|
| `{range}` | `<int><h\|d\|w\|m\|y>` | `1h`, `7d`, `14d`, `30d`, `644h` |

Unit meanings: `h` = hours · `d` = days · `w` = weeks · `m` = 30-day months · `y` = 365-day years.

Precomputed ranges (fast): `7d`, `14d`, `30d`. All other valid expressions are computed live from inferred trades.

**Request body** — JSON array of `type_id` integers (max 1 MiB).

**Response** `200 application/json` — object keyed by stringified `type_id`. Same shape as `/stats`:

```json
{
  "34": {
    "buy":  {"weightedAverage":"4.80","max":"5","min":"2.83","stddev":"0.62","median":"3.91","volume":"1092089.0","orderCount":"2","percentile":"4.89"},
    "sell": {"weightedAverage":"4.95","max":"5.10","min":"4.80","stddev":"0.05","median":"4.95","volume":"982314.0","orderCount":"5","percentile":"4.81"}
  }
}
```

See `/stats` for full field descriptions. Volumes/order counts here are the
total observed *trade* volume and trade-event count over the window (not
standing-order book counts).

Notes:

- Region scope aggregates every inferred trade in the region over the window.
- Station scope aggregates inferred trades whose `location_id` exactly matches
  the station (sells always have a concrete location; buy orders with a wider
  range have no concrete station and are excluded).
- Types with no trades in the window return zeroed records.

**Errors**

| Code | Reason |
|------|--------|
| `400` | Invalid `location_id`; invalid range string; malformed or empty body; player structure scope |
| `404` | Unknown location |
| `413` | Body exceeds 1 MiB |

---

## Data layout

Default under `./data`:

| Path | Description |
|------|-------------|
| `orders/orders-<unix>.jsonl` | Raw snapshot — one order per line (latest 2 uncompressed) |
| `orders/orders-<unix>.jsonl.gz` | Older snapshots, gzipped in place (~12× smaller) |
| `inferred/infer-<unix>.jsonl` | Inferred trades for the latest snapshot pair |
| `inferred/infer-<unix>.jsonl.gz` | Older inferred-trade files, gzipped in place |
| `inferred/infer-<unix>.done` | Completion marker |
| `contracts/contracts-<unix>.jsonl` | Raw public contracts snapshot |
| `history/region-<id>.json` | Cached ESI per-region daily history |
| `precomputed/meta.json` | Generation metadata (snapshot_unix, elapsed_s, counts) |
| `precomputed/stats/<location_id>.json` | Order-book stats (regions + NPC stations) |
| `precomputed/history/<region_id>_<range>.json` | Per-region inferred-trade stats (ranges: 7d, 14d, 30d) |
| `precomputed/history_station/<station_id>_<range>.json` | Per-NPC-station inferred-trade stats (ranges: 7d, 14d, 30d) |

Historical `orders/` and `inferred/` files older than the latest 2 / 1 are
gzipped automatically after every collection cycle (and once on startup as a
backfill). Reads through the API are transparent — `iter_snapshot` and the
inferred-cache loader open `.jsonl` and `.jsonl.gz` interchangeably, so older
snapshots requested via `GET /orders/{unix_time}` simply stream a bit slower.
The `precomputed/` tree itself is left uncompressed because it's already small
and is read on every `POST /stats` / `POST /history` request.

After each 20-minute collection cycle the scheduler regenerates everything
under `precomputed/` from the new snapshot. `POST /stats` and `POST /history`
serve those files directly (filtered by the request's `type_ids` body) and
respond in under 100 ms. Custom ranges and non-region/station scopes compute
live on the fly.
