# eveMarket API

eveMarket runs a market snapshot collector and an HTTP API server.

Default bind: `0.0.0.0:13307`

## Start

```bash
python eveMarket.py
```

Optional flags:

- `--bind` HTTP bind address
- `--port` HTTP port
- `--data` data directory (default `./data`)
- `--sde` SDE directory (default `./sde`)
- `--interval` collection interval in seconds (default `1200` = 20 minutes)
- `--no-inference` disable inferred-trade processing

## Response format

Most data endpoints stream NDJSON (`application/x-ndjson`):

- One JSON object per line
- Chunked transfer encoding

## Endpoints

### GET /

Returns service info and route hints.

### GET /health

Returns basic service status:

- `ok`
- `snapshots` (count of saved snapshots)
- `latest_unix` (latest snapshot timestamp, or null)

### GET /currentOrders/{location_id}

Returns current orders from the latest snapshot filtered by location.

`location_id` can be:

- Region ID
- Constellation ID
- Solar system ID
- NPC station ID
- Structure ID

Errors:

- `400` invalid location id
- `404` unknown location
- `503` no snapshots yet

### GET /orders

Returns the **entire latest snapshot** as NDJSON (every region, every order).
Response header `X-Snapshot-Unix` carries the snapshot timestamp.

Errors:

- `503` no snapshots yet

### GET /orders/{unix_time}

Returns orders from the snapshot nearest to `unix_time`.

- If no `location` query param is provided, returns the full snapshot stream.
- Includes response header `X-Snapshot-Unix` with the actual snapshot timestamp used.

Optional query:

- `?location={location_id}` to filter by location

Errors:

- `400` invalid unix time or invalid location
- `404` unknown location
- `503` no snapshots yet

Examples:

```text
/orders/1777247000
/orders/1777247000?location=30000142
```

### GET /inferred/{location_id}

Returns inferred trades for a location, based on the latest snapshot pair.

Behavior:

- If cached inferred file exists for the latest snapshot, it streams that.
- Otherwise, it computes inferred trades from the previous and latest snapshots.

Response header `X-Snapshot-Unix` carries the snapshot timestamp.

Optional query:

- `?attribution=jump_weighted` (station scope only) — see
  [Jump-weighted attribution](#jump-weighted-attribution) below.

Errors:

- `400` invalid location id, or `attribution=jump_weighted` requested for a
  non-station scope, or unknown attribution mode
- `404` unknown location
- `503` no snapshots yet, or fewer than 2 snapshots available

#### Jump-weighted attribution

Non-station-range buy orders fill from multiple stations; the exact fill
station is unknowable from snapshots. By default such trades carry
`location_id: null` and are excluded from station-scope queries.

With `?attribution=jump_weighted`, station-scope `/inferred` and `/stats`
requests synthesize estimated contributions from those trades using:

```
weight(s) = (1 / (1 + jumps(buyer_system, system_of(s)))) * sell_volume(type, s)
share(s)  = weight(s) / sum(weight)
```

The sell-side liquidity term biases shares toward stations that actually have
offers for the type (where buyers realistically fill). Estimated trades are
emitted alongside exact trades with the additional fields:

- `estimated: true`
- `attribution: "jump_weighted"`
- `attribution_share: <float>`

Response headers also include `X-Attribution: jump_weighted`.

This mode is opt-in only; default behavior is unchanged.

### POST /stats/{location_id}

Live order-book statistics from the latest snapshot, scoped to the given
location (region/constellation/system/station/structure).

Request body: a JSON array of `int64` `type_id`s.

```
POST /stats/30000142
Content-Type: application/json

[34, 35, 36]
```

Response (`application/json`): an object keyed by stringified `type_id`, each
value carrying `buy` and `sell` summaries:

```json
{
  "34": {
    "buy":  { "weightedAverage": "...", "max": "...", "min": "...",
              "stddev": "...", "median": "...", "volume": "...",
              "orderCount": "...", "percentile": "..." },
    "sell": { ... }
  }
}
```

Notes:

- `percentile` is the 5th percentile of sell prices and the 95th percentile of
  buy prices (i.e., the "best realistic" prices on each side).
- All numeric fields are returned as strings to preserve precision.
- Optional query `?attribution=jump_weighted` (station scope only) adds
  fractionally-attributed buy-side volume from non-station-range buys reachable
  to the station. The response gains an `attribution: "jump_weighted"` field
  per type and the buy side adds `exact_volume`, `estimated_volume`, and
  `estimated_order_count` alongside the (now-combined) `volume` / `orderCount`.

Errors:

- `400` invalid `location_id`, malformed body, or empty/invalid `type_id` list
- `404` unknown location
- `413` body larger than 1 MiB
- `503` no snapshots yet

### POST /history/{location_id}/{range}

Historic statistics aggregated from ESI's per-region market history.

`{range}` is `<int><h|d|w|m|y>` (hours, days, weeks, 30-day months, 365-day years).
Examples: `1h`, `644h`, `7d`, `1w`, `3m`, `1y`.

Request body: a JSON array of `int64` `type_id`s.

```
POST /history/30000142/7d
Content-Type: application/json

[34, 35]
```

Response (`application/json`): an object keyed by stringified `type_id`:

```json
{
  "34": {
    "average": 5.42,
    "date": "2024-12-31",
    "range": 604800,
    "highest": 5.99,
    "lowest": 5.01,
    "order_count": 12345,
    "volume": 678901234
  }
}
```

If the available history is shorter than the requested range, `range` reflects
the actual covered span. Empty types return zeroed records.

Notes:

- ESI history is region-only. For `system`, `station`, or `constellation`
  scopes, results reflect the parent region(s).
- Player structures have no ESI history; the endpoint returns `400`.

Errors:

- `400` invalid `location_id`, invalid range string, malformed body, or
  structure scope
- `404` unknown location
- `413` body larger than 1 MiB

## Data layout

Default under `./data`:

- `orders/orders-<unix>.jsonl` snapshot files
- `inferred/infer-<unix>.jsonl` inferred trade cache
- `history/region-<id>.json` market history cache
- `precomputed/meta.json` snapshot_unix + generation timestamp + counts
- `precomputed/stats/<location_id>.json` strict stats per region / NPC station
- `precomputed/stats_attr/<station_id>.json` jump-weighted stats per NPC station
- `precomputed/history/<region_id>_<range>.json` aggregated history (1d, 7d, 30d, 90d, 1y)

A completion marker is written after inferred processing:

- `inferred/infer-<unix>.done`

After every collection cycle the scheduler regenerates everything under
`precomputed/` from the new snapshot, so `POST /stats` and `POST /history`
become near-instant file reads (filtered by the request's `type_ids` body).
Custom history ranges and constellation/system scopes still fall back to
live computation.
