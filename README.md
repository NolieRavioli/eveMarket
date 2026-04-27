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

Errors:

- `400` invalid location id
- `404` unknown location
- `503` no snapshots yet, or fewer than 2 snapshots available

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

A completion marker is written after inferred processing:

- `inferred/infer-<unix>.done`
