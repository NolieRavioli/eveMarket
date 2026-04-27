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
| `--sde` | `./sde` | SDE directory |
| `--interval` | `1200` | Collection interval in seconds (20 min) |
| `--no-inference` | | Disable inferred-trade processing |

## Response formats

| Format | Content-Type | When used |
|--------|-------------|-----------|
| JSON | `application/json` | All single-object responses |
| NDJSON | `application/x-ndjson` | Streaming order/trade endpoints |

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

Service discovery. Returns a JSON object listing available routes.

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
GET /inferred/60003760?attribution=jump_weighted
```

**Query parameters**

| Param | Values | Description |
|-------|--------|-------------|
| `attribution` | `jump_weighted` | Station scope only — see [Jump-weighted attribution](#jump-weighted-attribution) |

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

When `?attribution=jump_weighted` is used, estimated trades are also emitted with
these additional fields:

| Field | Type | Description |
|-------|------|-------------|
| `estimated` | bool | Always `true` |
| `attribution` | string | `"jump_weighted"` |
| `attribution_share` | float | Fraction of the original trade volume attributed here (0–1) |

Response headers:

| Header | Description |
|--------|-------------|
| `X-Snapshot-Unix` | Unix timestamp of the snapshot used |
| `X-Attribution` | `"jump_weighted"` when attribution mode is active |

**Errors**

| Code | Reason |
|------|--------|
| `400` | Invalid `location_id`; or `attribution=jump_weighted` on a non-station scope; or unknown attribution value |
| `404` | Unknown location |
| `503` | No snapshots yet, or fewer than 2 snapshots available |

---

### `POST /stats/{location_id}`

Order-book statistics from the latest snapshot scoped to the given location.
Served from precomputed files for regions and NPC stations (fast); falls back
to live snapshot scan for constellations, systems, and structures.

```
POST /stats/10000002
POST /stats/60003760
POST /stats/60003760?attribution=jump_weighted
Content-Type: application/json

[34, 35, 36]
```

**Request body** — JSON array of `type_id` integers (max 1 MiB).

**Query parameters**

| Param | Values | Description |
|-------|--------|-------------|
| `attribution` | `jump_weighted` | Station scope only — augments buy side with fractionally-attributed non-station-range buys |

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

With `?attribution=jump_weighted` the buy side gains extra fields and a top-level `attribution` key:

```json
{
  "34": {
    "buy": {
      "...",
      "exact_volume": "8192983945.0",
      "estimated_volume": "2319817176.4",
      "estimated_order_count": "0",
      "volume": "10512801121.4",
      "orderCount": "32"
    },
    "sell": { "..." },
    "attribution": "jump_weighted"
  }
}
```

| Extra field | Description |
|-------------|-------------|
| `exact_volume` | Volume from station-range buy orders at this station |
| `estimated_volume` | Fractionally-attributed volume from non-station-range buys |
| `estimated_order_count` | Count of estimated (fractional) buy orders |

**Errors**

| Code | Reason |
|------|--------|
| `400` | Invalid `location_id`; malformed or empty body; `attribution=jump_weighted` on a non-station scope; unknown attribution value |
| `404` | Unknown location |
| `413` | Body exceeds 1 MiB |
| `503` | No snapshots yet |

---

### `POST /history/{location_id}/{range}`

Historic market statistics aggregated from ESI's per-region daily history feed.

Served from precomputed files for the five fixed ranges and single-region scopes (fast);
falls back to live aggregation for custom ranges or multi-region scopes.

```
POST /history/10000002/30d
POST /history/60003760/7d
Content-Type: application/json

[34, 35, 36]
```

**Path parameters**

| Param | Format | Examples |
|-------|--------|---------|
| `{range}` | `<int><h\|d\|w\|m\|y>` | `1h`, `7d`, `30d`, `90d`, `1y`, `644h` |

Unit meanings: `h` = hours · `d` = days · `w` = weeks · `m` = 30-day months · `y` = 365-day years.

Precomputed ranges (fast): `1d`, `7d`, `30d`, `90d`, `1y`.
All other valid expressions are computed live.

**Request body** — JSON array of `type_id` integers (max 1 MiB).

**Response** `200 application/json` — object keyed by stringified `type_id`:

```json
{
  "34": {
    "average": 4.05,
    "date": "2026-04-25",
    "range": 2592000,
    "highest": 4.48,
    "lowest": 3.50,
    "order_count": 61689,
    "volume": 177833511249
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `average` | float | Volume-weighted mean price over the period |
| `date` | string | Date of the most recent included daily bucket (`YYYY-MM-DD`) |
| `range` | int | Actual covered span in seconds (may exceed the requested range if the window extends to a daily boundary, or be shorter if history doesn't go that far back) |
| `highest` | float | Highest daily high over the period |
| `lowest` | float | Lowest daily low over the period |
| `order_count` | int | Total order count over the period |
| `volume` | int | Total units traded over the period |

Notes:

- ESI history is region-level only. For `station`, `system`, or `constellation`
  scopes the results reflect the parent region.
- Types with no history in the requested window return zeroed records.

**Errors**

| Code | Reason |
|------|--------|
| `400` | Invalid `location_id`; invalid range string; malformed or empty body; player structure scope |
| `404` | Unknown location |
| `413` | Body exceeds 1 MiB |

---

## Jump-weighted attribution

Non-station-range buy orders (range = `solarsystem`, `constellation`, `region`,
or a jump count) fill from multiple potential stations. The exact fill station
cannot be determined from order snapshots alone. By default such trades carry
`location_id: null` and are excluded from station-scope queries.

With `?attribution=jump_weighted`, station-scope `/inferred` and `/stats`
requests synthesize estimated contributions from those unresolved trades using:

```
weight(s) = (1 / (1 + jumps(buyer_system, system(s)))) × sell_liquidity(type, s)
share(s)  = weight(s) / Σ weight
```

The sell-side liquidity term biases shares toward stations that actually hold
current sell offers for that type. Stations with zero sell liquidity are excluded
entirely (the remaining candidates absorb their weight).

Estimated `/inferred` trades are emitted alongside exact trades with:

- `estimated: true`
- `attribution: "jump_weighted"`
- `attribution_share: <float 0–1>`

Estimated `/stats` buy-side volumes are merged into the standard buy fields
with `exact_volume`, `estimated_volume`, and `estimated_order_count` breakdowns.

This mode is opt-in. Default behavior is unchanged.

---

## Data layout

Default under `./data`:

| Path | Description |
|------|-------------|
| `orders/orders-<unix>.jsonl` | Raw snapshot — one order per line (latest 2 only) |
| `orders/orders-<unix>.jsonl.gz` | Older snapshots, gzipped in place (~12× smaller) |
| `inferred/infer-<unix>.jsonl` | Inferred trades for the latest snapshot pair |
| `inferred/infer-<unix>.jsonl.gz` | Older inferred-trade files, gzipped in place |
| `inferred/infer-<unix>.done` | Completion marker |
| `history/region-<id>.json` | Cached ESI per-region daily history |
| `precomputed/meta.json` | Generation metadata (snapshot_unix, elapsed_s, counts) |
| `precomputed/stats/<location_id>.json` | Strict order-book stats (regions + NPC stations) |
| `precomputed/stats_attr/<station_id>.json` | Jump-weighted stats (NPC stations) |
| `precomputed/history/<region_id>_<range>.json` | Aggregated history (ranges: 1d, 7d, 30d, 90d, 1y) |

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
