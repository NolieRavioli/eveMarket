"""HTTP API for eveMarket.

Endpoints:
  GET /                             -> HTML service overview + documentation
  GET /health                       -> {ok, snapshots, latest_unix}
  GET /currentOrders/{location_id}  -> NDJSON of orders matching the location
  GET /orders                       -> NDJSON of entire latest snapshot
  GET /orders/{unix_time}           -> NDJSON of the snapshot nearest unix_time
                                        Optional ?location=<id> filters within.
  GET /inferred/{location_id}       -> NDJSON of inferred trades from the latest
                                        snapshot pair, filtered to the location.
  GET /history/{range}              -> NDJSON of universe-wide precomputed history
  GET /contracts/courier            -> NDJSON of active public courier contracts

NDJSON: one JSON object per line; Content-Type: application/x-ndjson.
"""
from __future__ import annotations

import json
import logging
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Iterable, Optional
from urllib.parse import parse_qs, urlsplit

from .esi import EsiClient
from .contracts import iter_courier_contracts
from .index import filter_snapshot, iter_snapshot, matches
from .inferred import diff_snapshots, load_inferred
from .location import LocationResolver
from .precompute import (
    history_file as precomputed_history_file,
    history_station_file as precomputed_history_station_file,
    precomputed_dir,
    read_meta as read_precomputed_meta,
    stats_file as precomputed_stats_file,
)
from .snapshot import (
    contracts_path,
    find_nearest_snapshot,
    latest_contracts_snapshot,
    latest_snapshot,
    list_snapshots,
    orders_path,
    previous_snapshot,
)
from .stats import (
    compute_history_stats,
    compute_live_stats,
    latest_snapshot_path,
    parse_range,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Root HTML page — served by GET /
# ---------------------------------------------------------------------------
_ROOT_HTML = b"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>eveMarket API</title>
<style>
:root{--bg:#0d1117;--bg2:#161b22;--border:#30363d;--text:#c9d1d9;--accent:#58a6ff;--green:#3fb950;--orange:#d29922;--head:#e6edf3;--code-bg:#1f2428}
*{box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif;background:var(--bg);color:var(--text);margin:0;padding:1.5rem 2.5rem;line-height:1.6;max-width:980px}
h1{color:var(--head);font-size:1.9rem;border-bottom:1px solid var(--border);padding-bottom:.5rem}
h2{color:var(--head);font-size:1.25rem;border-bottom:1px solid var(--border);padding-bottom:.3rem;margin-top:2.5rem}
h3{color:var(--head);font-size:1rem;margin-top:1.8rem}
a{color:var(--accent);text-decoration:none}a:hover{text-decoration:underline}
code{font-family:"SFMono-Regular",Consolas,"Liberation Mono",Menlo,monospace;background:var(--code-bg);border-radius:4px;padding:.15em .4em;font-size:.875em}
pre{background:var(--code-bg);border:1px solid var(--border);border-radius:6px;padding:1em;overflow-x:auto;font-family:"SFMono-Regular",Consolas,"Liberation Mono",Menlo,monospace;font-size:.875rem;white-space:pre-wrap}
pre code{background:none;padding:0;font-size:inherit}
table{border-collapse:collapse;width:100%;margin:.8em 0;font-size:.9rem}
th,td{border:1px solid var(--border);padding:.45em .75em;text-align:left}
th{background:var(--bg2);color:var(--head)}
.get{color:var(--green);font-weight:700}.post{color:var(--orange);font-weight:700}
hr{border:none;border-top:1px solid var(--border);margin:2.5rem 0}
ul{padding-left:1.4em}li{margin:.2em 0}
.toc{background:var(--bg2);border:1px solid var(--border);border-radius:6px;padding:1rem 1.5rem;display:inline-block;margin-bottom:1.5rem;min-width:300px}
.toc ul{margin:.2em 0}
</style>
</head>
<body>
<h1>eveMarket API</h1>
<p>Market snapshot collector and HTTP API server for EVE Online market data. Default bind: <code>0.0.0.0:13307</code></p>

<div class="toc"><strong>Contents</strong>
<ul>
<li><a href="#start">Start</a></li>
<li><a href="#formats">Response Formats</a></li>
<li><a href="#locations">Location IDs</a></li>
<li><a href="#endpoints">Endpoints</a>
  <ul>
  <li><a href="#ep-health"><span class="get">GET</span> /health</a></li>
  <li><a href="#ep-currentorders"><span class="get">GET</span> /currentOrders/{location_id}</a></li>
  <li><a href="#ep-orders"><span class="get">GET</span> /orders</a></li>
  <li><a href="#ep-orders-unix"><span class="get">GET</span> /orders/{unix_time}</a></li>
  <li><a href="#ep-inferred"><span class="get">GET</span> /inferred/{location_id}</a></li>
  <li><a href="#ep-history-range"><span class="get">GET</span> /history/{range}</a></li>
  <li><a href="#ep-contracts-courier"><span class="get">GET</span> /contracts/courier</a></li>
  <li><a href="#ep-stats"><span class="post">POST</span> /stats/{location_id}</a></li>
  <li><a href="#ep-history-post"><span class="post">POST</span> /history/{location_id}/{range}</a></li>
  </ul>
</li>
<li><a href="#data">Data Layout</a></li>
</ul>
</div>

<h2 id="start">Start</h2>
<pre><code>pip install -r requirements.txt
python eveMarket.py</code></pre>
<table>
<tr><th>Flag</th><th>Default</th><th>Description</th></tr>
<tr><td><code>--bind</code></td><td><code>0.0.0.0</code></td><td>HTTP bind address</td></tr>
<tr><td><code>--port</code></td><td><code>13307</code></td><td>HTTP port</td></tr>
<tr><td><code>--data</code></td><td><code>./data</code></td><td>Data directory</td></tr>
<tr><td><code>--sde</code></td><td><code>./sde</code></td><td>SDE directory (auto-downloaded if missing)</td></tr>
<tr><td><code>--interval</code></td><td><code>1200</code></td><td>Collection interval in seconds (20&thinsp;min)</td></tr>
<tr><td><code>--no-inference</code></td><td></td><td>Disable inferred-trade processing</td></tr>
<tr><td><code>--log-level</code></td><td><code>INFO</code></td><td>Python logging level</td></tr>
</table>

<h2 id="formats">Response Formats</h2>
<table>
<tr><th>Format</th><th>Content-Type</th><th>When used</th></tr>
<tr><td>JSON</td><td><code>application/json</code></td><td>All single-object responses</td></tr>
<tr><td>NDJSON</td><td><code>application/x-ndjson</code></td><td>Streaming order/trade/history/contract endpoints</td></tr>
</table>
<p>NDJSON: one JSON object per line, chunked transfer encoding. Read line-by-line without buffering the full response.</p>

<h2 id="locations">Location IDs</h2>
<p>All <code>{location_id}</code> path parameters accept:</p>
<table>
<tr><th>Kind</th><th>ID range</th><th>Example</th></tr>
<tr><td>Region</td><td>10&thinsp;000&thinsp;000 &ndash; 10&thinsp;999&thinsp;999</td><td><code>10000002</code> (The Forge)</td></tr>
<tr><td>Constellation</td><td>20&thinsp;000&thinsp;000 &ndash; 20&thinsp;999&thinsp;999</td><td><code>20000020</code> (Kimotoro)</td></tr>
<tr><td>Solar system</td><td>30&thinsp;000&thinsp;000 &ndash; 30&thinsp;999&thinsp;999</td><td><code>30000142</code> (Jita)</td></tr>
<tr><td>NPC station</td><td>60&thinsp;000&thinsp;000 &ndash; 63&thinsp;999&thinsp;999</td><td><code>60003760</code> (Jita&nbsp;4-4)</td></tr>
<tr><td>Player structure</td><td>&ge;&thinsp;1&thinsp;000&thinsp;000&thinsp;000&thinsp;000</td><td><code>1035466617946</code></td></tr>
</table>

<h2 id="endpoints">Endpoints</h2>
<hr>
<h3 id="ep-health"><span class="get">GET</span> /health</h3>
<p>Server health and snapshot count.</p>
<pre><code>GET /health</code></pre>
<p><strong>Response</strong> <code>200 application/json</code></p>
<pre><code>{"ok": true, "snapshots": 18, "latest_unix": 1777272798}</code></pre>
<table>
<tr><th>Field</th><th>Type</th><th>Description</th></tr>
<tr><td><code>ok</code></td><td>bool</td><td>Always <code>true</code></td></tr>
<tr><td><code>snapshots</code></td><td>int</td><td>Number of snapshots on disk</td></tr>
<tr><td><code>latest_unix</code></td><td>int|null</td><td>Unix timestamp of the latest snapshot</td></tr>
</table>
<hr>

<h3 id="ep-currentorders"><span class="get">GET</span> /currentOrders/{location_id}</h3>
<p>Current live orders from the latest snapshot, filtered to the given location.</p>
<pre><code>GET /currentOrders/10000002
GET /currentOrders/60003760</code></pre>
<p><strong>Response</strong> <code>200 application/x-ndjson</code> &mdash; stream of order objects:</p>
<pre><code>{"order_id":7315521535,"type_id":3452,"region_id":10000001,"system_id":30000087,
 "location_id":60012142,"is_buy_order":false,"price":1010.0,"volume_remain":1,
 "volume_total":1,"min_volume":1,"range":"region","duration":7,
 "issued":"2026-04-22T14:35:48Z"}</code></pre>
<table>
<tr><th>Field</th><th>Type</th><th>Description</th></tr>
<tr><td><code>order_id</code></td><td>int</td><td>ESI order ID</td></tr>
<tr><td><code>type_id</code></td><td>int</td><td>Item type</td></tr>
<tr><td><code>region_id</code></td><td>int</td><td>Region the order was collected from</td></tr>
<tr><td><code>system_id</code></td><td>int</td><td>Solar system of the order</td></tr>
<tr><td><code>location_id</code></td><td>int</td><td>Station or structure ID</td></tr>
<tr><td><code>is_buy_order</code></td><td>bool</td><td><code>true</code> = buy order</td></tr>
<tr><td><code>price</code></td><td>float</td><td>ISK per unit</td></tr>
<tr><td><code>volume_remain</code></td><td>int</td><td>Units remaining</td></tr>
<tr><td><code>volume_total</code></td><td>int</td><td>Original quantity</td></tr>
<tr><td><code>min_volume</code></td><td>int</td><td>Minimum fill size</td></tr>
<tr><td><code>range</code></td><td>string</td><td>Buy-order fill range: <code>"station"</code>, <code>"solarsystem"</code>, <code>"constellation"</code>, <code>"region"</code>, or <code>"1"</code>&ndash;<code>"40"</code> jumps. Always <code>"region"</code> for sell orders.</td></tr>
<tr><td><code>duration</code></td><td>int</td><td>Order lifetime in days</td></tr>
<tr><td><code>issued</code></td><td>string</td><td>ISO&nbsp;8601 UTC timestamp of issuance</td></tr>
</table>
<p><strong>Errors</strong></p>
<table>
<tr><th>Code</th><th>Reason</th></tr>
<tr><td><code>400</code></td><td>Invalid <code>location_id</code></td></tr>
<tr><td><code>404</code></td><td>Unknown location</td></tr>
<tr><td><code>503</code></td><td>No snapshots yet</td></tr>
</table>
<hr>

<h3 id="ep-orders"><span class="get">GET</span> /orders</h3>
<p>Entire latest snapshot as a flat NDJSON stream &mdash; every region, every order.</p>
<pre><code>GET /orders</code></pre>
<p><strong>Response</strong> <code>200 application/x-ndjson</code></p>
<p>Response header <code>X-Snapshot-Unix</code> carries the snapshot unix timestamp. Each row has the same fields as <a href="#ep-currentorders">GET /currentOrders</a>.</p>
<p><strong>Errors</strong></p>
<table>
<tr><th>Code</th><th>Reason</th></tr>
<tr><td><code>503</code></td><td>No snapshots yet</td></tr>
</table>
<hr>

<h3 id="ep-orders-unix"><span class="get">GET</span> /orders/{unix_time}</h3>
<p>Orders from the snapshot <strong>nearest</strong> to <code>unix_time</code> (any direction).</p>
<pre><code>GET /orders/1777247000
GET /orders/1777247000?location=30000142</code></pre>
<table>
<tr><th>Param</th><th>Description</th></tr>
<tr><td><code>location</code></td><td>Optional &mdash; filter by any location ID</td></tr>
</table>
<p><strong>Response</strong> <code>200 application/x-ndjson</code></p>
<p>Response header <code>X-Snapshot-Unix</code> carries the timestamp of the snapshot actually used. Each row has the same fields as <a href="#ep-currentorders">GET /currentOrders</a>.</p>
<p><strong>Errors</strong></p>
<table>
<tr><th>Code</th><th>Reason</th></tr>
<tr><td><code>400</code></td><td>Invalid <code>unix_time</code> or invalid <code>location</code> param</td></tr>
<tr><td><code>404</code></td><td>Unknown location</td></tr>
<tr><td><code>503</code></td><td>No snapshots yet</td></tr>
</table>
<hr>

<h3 id="ep-inferred"><span class="get">GET</span> /inferred/{location_id}</h3>
<p>Inferred trades derived from the latest snapshot pair, filtered to the given location. Reads from the precomputed inferred cache when available; falls back to live computation otherwise.</p>
<pre><code>GET /inferred/10000002
GET /inferred/60003760</code></pre>
<p><strong>Response</strong> <code>200 application/x-ndjson</code> &mdash; stream of inferred-trade objects:</p>
<pre><code>{"actual":true,"order_id":7319060832,"type_id":13249,"region_id":10000002,
 "system_id":30000142,"is_buy_order":false,"price":18900000.0,"volume":1.0,
 "location_id":60003760,"buyer_range":null,"buyer_station_id":null}</code></pre>
<table>
<tr><th>Field</th><th>Type</th><th>Description</th></tr>
<tr><td><code>actual</code></td><td>bool</td><td><code>true</code> = fill observed as volume delta; <code>false</code> = inferred from order disappearance</td></tr>
<tr><td><code>order_id</code></td><td>int</td><td>ESI order ID of the filled order</td></tr>
<tr><td><code>type_id</code></td><td>int</td><td>Item type</td></tr>
<tr><td><code>region_id</code></td><td>int</td><td>Region</td></tr>
<tr><td><code>system_id</code></td><td>int|null</td><td>Solar system (null for some older records)</td></tr>
<tr><td><code>is_buy_order</code></td><td>bool</td><td>Whether the filled order was a buy</td></tr>
<tr><td><code>price</code></td><td>float</td><td>ISK per unit at time of fill</td></tr>
<tr><td><code>volume</code></td><td>float</td><td>Estimated units traded</td></tr>
<tr><td><code>location_id</code></td><td>int|null</td><td>Concrete fill station, or <code>null</code> for non-station-range buys where the exact fill destination is unknown</td></tr>
<tr><td><code>buyer_range</code></td><td>string|null</td><td>ESI range of the buy order (<code>"station"</code>, <code>"solarsystem"</code>, <code>"constellation"</code>, <code>"region"</code>, or jump count as string). <code>null</code> for sell-side fills.</td></tr>
<tr><td><code>buyer_station_id</code></td><td>int|null</td><td>Station ID of the buyer&rsquo;s order. <code>null</code> for sell-side fills.</td></tr>
</table>
<p>Response header <code>X-Snapshot-Unix</code> carries the unix timestamp of the snapshot used.</p>
<p><strong>Errors</strong></p>
<table>
<tr><th>Code</th><th>Reason</th></tr>
<tr><td><code>400</code></td><td>Invalid <code>location_id</code></td></tr>
<tr><td><code>404</code></td><td>Unknown location</td></tr>
<tr><td><code>503</code></td><td>No snapshots yet, or fewer than 2 snapshots available</td></tr>
</table>
<hr>

<h3 id="ep-history-range"><span class="get">GET</span> /history/{range}</h3>
<p>Universe-wide precomputed history &mdash; one NDJSON row per precomputed region or NPC station. Used by eveHauler to bulk-load fill caps without per-station POST requests.</p>
<p>Precomputed ranges: <code>7d</code>, <code>14d</code>, <code>30d</code>.</p>
<pre><code>GET /history/7d
GET /history/30d</code></pre>
<p><strong>Response</strong> <code>200 application/x-ndjson</code></p>
<pre><code>{"scope":"station","location_id":60003760,"range":"30d",
 "types":{"34":{"buy":{...},"sell":{...}},...}}</code></pre>
<table>
<tr><th>Field</th><th>Type</th><th>Description</th></tr>
<tr><td><code>scope</code></td><td>string</td><td><code>"region"</code> or <code>"station"</code></td></tr>
<tr><td><code>location_id</code></td><td>int</td><td>Region or NPC station ID</td></tr>
<tr><td><code>range</code></td><td>string</td><td>Range label matching the request (e.g. <code>"30d"</code>)</td></tr>
<tr><td><code>types</code></td><td>object</td><td>Map of <code>type_id</code> string &rarr; <code>{"buy":{&hellip;},"sell":{&hellip;}}</code> &mdash; same shape as <a href="#ep-stats">POST /stats</a> response values</td></tr>
</table>
<p>Response headers: <code>X-History-Range</code> (range label), <code>X-Snapshot-Unix</code> (precompute metadata unix, if available).</p>
<p><strong>Errors</strong></p>
<table>
<tr><th>Code</th><th>Reason</th></tr>
<tr><td><code>400</code></td><td>Invalid or non-precomputed range</td></tr>
</table>
<hr>

<h3 id="ep-contracts-courier"><span class="get">GET</span> /contracts/courier</h3>
<p>Every currently-active public courier contract, universe-wide, from the latest contracts snapshot.</p>
<pre><code>GET /contracts/courier</code></pre>
<p><strong>Response</strong> <code>200 application/x-ndjson</code></p>
<pre><code>{"contract_id":12345678,"region_id":10000002,
 "start_location_id":60003760,"end_location_id":60008494,
 "reward":5000000.0,"collateral":500000000.0,"volume":10000.0,
 "days_to_complete":3,"date_issued":"2026-04-22T14:00:00Z",
 "date_expired":"2026-04-29T14:00:00Z","title":""}</code></pre>
<table>
<tr><th>Field</th><th>Type</th><th>Description</th></tr>
<tr><td><code>contract_id</code></td><td>int</td><td>ESI contract ID</td></tr>
<tr><td><code>region_id</code></td><td>int</td><td>Region the contract was collected from</td></tr>
<tr><td><code>start_location_id</code></td><td>int</td><td>Pickup station or structure</td></tr>
<tr><td><code>end_location_id</code></td><td>int</td><td>Dropoff station or structure</td></tr>
<tr><td><code>reward</code></td><td>float</td><td>ISK reward offered</td></tr>
<tr><td><code>collateral</code></td><td>float</td><td>ISK collateral required</td></tr>
<tr><td><code>volume</code></td><td>float</td><td>Cargo volume in m&sup3;</td></tr>
<tr><td><code>days_to_complete</code></td><td>int</td><td>Days to complete after acceptance</td></tr>
<tr><td><code>date_issued</code></td><td>string</td><td>ISO&nbsp;8601 UTC</td></tr>
<tr><td><code>date_expired</code></td><td>string</td><td>ISO&nbsp;8601 UTC &mdash; only non-expired contracts are returned</td></tr>
<tr><td><code>title</code></td><td>string</td><td>Contract title (may be empty)</td></tr>
</table>
<p>Response header <code>X-Contracts-Snapshot-Unix</code> carries the unix timestamp of the snapshot used.</p>
<p><strong>Errors</strong></p>
<table>
<tr><th>Code</th><th>Reason</th></tr>
<tr><td><code>503</code></td><td>No contracts snapshot yet</td></tr>
</table>
<hr>

<h3 id="ep-stats"><span class="post">POST</span> /stats/{location_id}</h3>
<p>Order-book statistics from the latest snapshot scoped to the given location. Served from precomputed files for regions and NPC stations (fast); falls back to live snapshot scan for constellations, systems, and structures.</p>
<pre><code>POST /stats/10000002
POST /stats/60003760
Content-Type: application/json

[34, 35, 36]</code></pre>
<p><strong>Request body</strong> &mdash; JSON array of <code>type_id</code> integers (max 1&nbsp;MiB).</p>
<p><strong>Response</strong> <code>200 application/json</code> &mdash; object keyed by stringified <code>type_id</code>:</p>
<pre><code>{
  "34": {
    "buy":  {"weightedAverage":"4.07","max":"4.18","min":"0.56","stddev":"0.83",
             "median":"4.09","volume":"8192983945.0","orderCount":"32","percentile":"4.07"},
    "sell": {"weightedAverage":"4.54","max":"999","min":"4.18","stddev":"2.33",
             "median":"4.75","volume":"5078497821.0","orderCount":"46","percentile":"4.20"}
  }
}</code></pre>
<p>All numeric fields are returned as strings to preserve precision.</p>
<table>
<tr><th>Field</th><th>Description</th></tr>
<tr><td><code>weightedAverage</code></td><td>Volume-weighted mean price</td></tr>
<tr><td><code>max</code></td><td>Highest price in the order book</td></tr>
<tr><td><code>min</code></td><td>Lowest price in the order book</td></tr>
<tr><td><code>stddev</code></td><td>Volume-weighted standard deviation</td></tr>
<tr><td><code>median</code></td><td>Median price</td></tr>
<tr><td><code>volume</code></td><td>Total units on offer</td></tr>
<tr><td><code>orderCount</code></td><td>Number of orders</td></tr>
<tr><td><code>percentile</code></td><td>5th percentile for sell; 95th percentile for buy (best realistic fill price)</td></tr>
</table>
<p><strong>Errors</strong></p>
<table>
<tr><th>Code</th><th>Reason</th></tr>
<tr><td><code>400</code></td><td>Invalid <code>location_id</code>; malformed or empty body</td></tr>
<tr><td><code>404</code></td><td>Unknown location</td></tr>
<tr><td><code>413</code></td><td>Body exceeds 1&nbsp;MiB</td></tr>
<tr><td><code>503</code></td><td>No snapshots yet</td></tr>
</table>
<hr>

<h3 id="ep-history-post"><span class="post">POST</span> /history/{location_id}/{range}</h3>
<p>Historic market statistics aggregated from the server&rsquo;s inferred-trade tape. Same response shape as <a href="#ep-stats">POST /stats</a> (volumes are trade volume over the window, not standing-order counts). Precomputed ranges <code>7d</code>, <code>14d</code>, <code>30d</code> are fast; other valid expressions are computed live.</p>
<pre><code>POST /history/10000002/30d
POST /history/60003760/7d
Content-Type: application/json

[34, 35, 36]</code></pre>
<table>
<tr><th>Param</th><th>Format</th><th>Examples</th></tr>
<tr><td><code>{range}</code></td><td><code>&lt;int&gt;&lt;h|d|w|m|y&gt;</code></td><td><code>1h</code>, <code>7d</code>, <code>14d</code>, <code>30d</code>, <code>644h</code></td></tr>
</table>
<p>Unit meanings: <code>h</code>&nbsp;= hours &middot; <code>d</code>&nbsp;= days &middot; <code>w</code>&nbsp;= weeks &middot; <code>m</code>&nbsp;= 30-day months &middot; <code>y</code>&nbsp;= 365-day years.</p>
<p>Notes:</p>
<ul>
<li>Region scope aggregates every inferred trade in the region over the window.</li>
<li>Station scope aggregates inferred trades whose <code>location_id</code> exactly matches the station (buy orders with a wider range are excluded).</li>
<li>Types with no trades in the window return zeroed records.</li>
</ul>
<p><strong>Errors</strong></p>
<table>
<tr><th>Code</th><th>Reason</th></tr>
<tr><td><code>400</code></td><td>Invalid <code>location_id</code>; invalid range string; malformed or empty body; player structure scope</td></tr>
<tr><td><code>404</code></td><td>Unknown location</td></tr>
<tr><td><code>413</code></td><td>Body exceeds 1&nbsp;MiB</td></tr>
</table>

<h2 id="data">Data Layout</h2>
<p>Default under <code>./data</code>:</p>
<table>
<tr><th>Path</th><th>Description</th></tr>
<tr><td><code>orders/orders-&lt;unix&gt;.jsonl</code></td><td>Raw snapshot &mdash; one order per line (latest 2 kept uncompressed)</td></tr>
<tr><td><code>orders/orders-&lt;unix&gt;.jsonl.gz</code></td><td>Older snapshots, gzipped in place (~12&times; smaller)</td></tr>
<tr><td><code>inferred/infer-&lt;unix&gt;.jsonl</code></td><td>Inferred trades for the latest snapshot pair</td></tr>
<tr><td><code>inferred/infer-&lt;unix&gt;.jsonl.gz</code></td><td>Older inferred-trade files, gzipped in place</td></tr>
<tr><td><code>inferred/infer-&lt;unix&gt;.done</code></td><td>Completion marker</td></tr>
<tr><td><code>contracts/contracts-&lt;unix&gt;.jsonl</code></td><td>Raw public contracts snapshot</td></tr>
<tr><td><code>history/region-&lt;id&gt;.json</code></td><td>Cached ESI per-region daily history</td></tr>
<tr><td><code>precomputed/meta.json</code></td><td>Generation metadata (snapshot_unix, elapsed_s, counts)</td></tr>
<tr><td><code>precomputed/stats/&lt;location_id&gt;.json</code></td><td>Order-book stats (regions + NPC stations)</td></tr>
<tr><td><code>precomputed/history/&lt;region_id&gt;_&lt;range&gt;.json</code></td><td>Per-region inferred-trade stats (ranges: 7d, 14d, 30d)</td></tr>
<tr><td><code>precomputed/history_station/&lt;station_id&gt;_&lt;range&gt;.json</code></td><td>Per-NPC-station inferred-trade stats (ranges: 7d, 14d, 30d)</td></tr>
</table>
<p>Historical <code>orders/</code> and <code>inferred/</code> files older than the latest 2&nbsp;/&nbsp;1 are gzipped automatically after every collection cycle. Reads are transparent &mdash; older snapshots via <code>GET /orders/{unix_time}</code> simply stream a bit slower. The <code>precomputed/</code> tree is left uncompressed as it is small and read on every <code>POST /stats</code> / <code>POST /history</code> request.</p>
</body>
</html>"""


class MarketState:
    def __init__(
        self,
        sde_dir: Path,
        data_dir: Path,
        *,
        client: Optional[EsiClient] = None,
    ) -> None:
        self.sde_dir = Path(sde_dir)
        self.data_dir = Path(data_dir)
        self.client = client or EsiClient()
        self.resolver = LocationResolver(self.sde_dir)


def _make_handler(state: MarketState):

    class Handler(BaseHTTPRequestHandler):
        server_version = "eveMarket/1.0"

        def log_message(self, fmt, *args):
            logger.info("%s - " + fmt, self.address_string(), *args)

        # ---------- helpers ----------
        def _send_json(self, code: int, payload: dict) -> None:
            body = json.dumps(payload).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _send_html(self, body: bytes) -> None:
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _send_ndjson(self, rows: Iterable[dict],
                         extra_headers: Optional[dict] = None) -> None:
            self.send_response(200)
            self.send_header("Content-Type", "application/x-ndjson")
            self.send_header("Transfer-Encoding", "chunked")
            for k, v in (extra_headers or {}).items():
                self.send_header(k, str(v))
            self.end_headers()
            try:
                count = 0
                for r in rows:
                    line = (json.dumps(r, separators=(",", ":")) + "\n").encode("utf-8")
                    chunk = (f"{len(line):X}\r\n").encode("ascii") + line + b"\r\n"
                    self.wfile.write(chunk)
                    count += 1
                self.wfile.write(b"0\r\n\r\n")
                logger.info("ndjson: %s rows", count)
            except (BrokenPipeError, ConnectionResetError):
                logger.info("client disconnected during stream")

        def _parse_int(self, raw: str) -> Optional[int]:
            try:
                return int(raw)
            except (TypeError, ValueError):
                return None

        # ---------- routes ----------
        def do_GET(self):  # noqa: N802
            url = urlsplit(self.path)
            parts = [p for p in url.path.split("/") if p]
            qs = parse_qs(url.query)

            if not parts:
                return self._send_html(_ROOT_HTML)

            head = parts[0]

            if head == "health" and len(parts) == 1:
                snaps = list_snapshots(state.data_dir)
                return self._send_json(200, {
                    "ok": True,
                    "snapshots": len(snaps),
                    "latest_unix": snaps[-1] if snaps else None,
                })

            if head == "currentOrders" and len(parts) == 2:
                lid = self._parse_int(parts[1])
                if lid is None:
                    return self._send_json(400, {"error": "invalid location_id"})
                latest = latest_snapshot(state.data_dir)
                if latest is None:
                    return self._send_json(503, {"error": "no snapshots yet"})
                info = state.resolver.classify(lid)
                if info.kind == "unknown":
                    return self._send_json(404, {"error": "unknown location"})
                return self._send_ndjson(filter_snapshot(orders_path(state.data_dir, latest), info))

            if head == "orders" and len(parts) == 1:
                latest = latest_snapshot(state.data_dir)
                if latest is None:
                    return self._send_json(503, {"error": "no snapshots yet"})
                snap_path = orders_path(state.data_dir, latest)
                return self._send_ndjson(iter_snapshot(snap_path),
                                         extra_headers={"X-Snapshot-Unix": latest})

            if head == "orders" and len(parts) == 2:
                t = self._parse_int(parts[1])
                if t is None:
                    return self._send_json(400, {"error": "invalid unix_time"})
                snap = find_nearest_snapshot(state.data_dir, t)
                if snap is None:
                    return self._send_json(503, {"error": "no snapshots yet"})
                snap_path = orders_path(state.data_dir, snap)
                loc_q = qs.get("location", [None])[0]
                if loc_q is None:
                    return self._send_ndjson(iter_snapshot(snap_path),
                                             extra_headers={"X-Snapshot-Unix": snap})
                lid = self._parse_int(loc_q)
                if lid is None:
                    return self._send_json(400, {"error": "invalid location"})
                info = state.resolver.classify(lid)
                if info.kind == "unknown":
                    return self._send_json(404, {"error": "unknown location"})
                return self._send_ndjson(filter_snapshot(snap_path, info))

            if head == "inferred" and len(parts) == 2:
                lid = self._parse_int(parts[1])
                if lid is None:
                    return self._send_json(400, {"error": "invalid location_id"})
                latest = latest_snapshot(state.data_dir)
                if latest is None:
                    return self._send_json(503, {"error": "no snapshots yet"})
                info = state.resolver.classify(lid)
                if info.kind == "unknown":
                    return self._send_json(404, {"error": "unknown location"})

                # Build the base inferred-trade source (cached file or live diff).
                cached = load_inferred(state.data_dir, latest)
                if cached is not None:
                    source_iter = iter(cached)
                else:
                    prev = previous_snapshot(state.data_dir, latest)
                    if prev is None:
                        return self._send_json(503, {"error": "need at least 2 snapshots"})
                    source_iter = diff_snapshots(
                        orders_path(state.data_dir, prev),
                        orders_path(state.data_dir, latest),
                        data_dir=state.data_dir,
                        client=state.client,
                    )

                rows = (t for t in source_iter if _trade_matches(t, info, state.resolver))
                return self._send_ndjson(rows, extra_headers={"X-Snapshot-Unix": latest})

            if head == "history" and len(parts) == 2:
                rng = parse_range(parts[1])
                if rng is None:
                    return self._send_json(400, {
                        "error": "invalid range",
                        "hint": "use one of " + ", ".join(
                            sorted(_PRECOMPUTED_HISTORY_RANGES.values())
                        ),
                    })
                range_label = _PRECOMPUTED_HISTORY_RANGES.get(rng)
                if range_label is None:
                    return self._send_json(400, {
                        "error": "range not precomputed",
                        "hint": "use one of " + ", ".join(
                            sorted(_PRECOMPUTED_HISTORY_RANGES.values())
                        ),
                    })
                meta = read_precomputed_meta(state.data_dir)
                snap_unix = (meta or {}).get("snapshot_unix")
                rows = _iter_universe_history(state.data_dir, range_label)
                extra = {"X-History-Range": range_label}
                if snap_unix is not None:
                    extra["X-Snapshot-Unix"] = snap_unix
                return self._send_ndjson(rows, extra_headers=extra)

            if head == "contracts" and len(parts) == 2 and parts[1] == "courier":
                snap = latest_contracts_snapshot(state.data_dir)
                if snap is None:
                    return self._send_json(503, {"error": "no contracts snapshot yet"})
                snap_path = contracts_path(state.data_dir, snap)
                if not snap_path.exists():
                    return self._send_json(503, {"error": "contracts snapshot missing on disk"})
                rows = iter_courier_contracts(snap_path)
                return self._send_ndjson(
                    rows,
                    extra_headers={"X-Contracts-Snapshot-Unix": snap},
                )

            return self._send_json(404, {"error": "not found", "path": self.path})

        # ---------- POST routes ----------
        MAX_POST_BYTES = 1 << 20  # 1 MiB

        def _read_body(self):
            """Return parsed JSON body or send a 4xx response and return None."""
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except (TypeError, ValueError):
                self._send_json(400, {"error": "missing or invalid Content-Length"})
                return None
            if length <= 0:
                self._send_json(400, {"error": "empty body"})
                return None
            if length > self.MAX_POST_BYTES:
                self._send_json(413, {"error": "body too large",
                                       "max_bytes": self.MAX_POST_BYTES})
                return None
            raw = self.rfile.read(length)
            try:
                return json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                self._send_json(400, {"error": "invalid JSON body"})
                return None

        def _parse_type_ids(self, body):
            """Validate body is a list of int64-ish ids; returns list[int] or None."""
            if not isinstance(body, list):
                self._send_json(400, {"error": "body must be an array of type_ids"})
                return None
            out: list[int] = []
            for item in body:
                if isinstance(item, bool) or not isinstance(item, int):
                    self._send_json(400, {"error": "all items must be integer type_ids"})
                    return None
                if item < 0 or item > (1 << 63) - 1:
                    self._send_json(400, {"error": "type_id out of int64 range"})
                    return None
                out.append(item)
            if not out:
                self._send_json(400, {"error": "empty type_id list"})
                return None
            return out

        def do_POST(self):  # noqa: N802
            url = urlsplit(self.path)
            parts = [p for p in url.path.split("/") if p]
            if not parts:
                return self._send_json(404, {"error": "not found", "path": self.path})
            head = parts[0]

            if head == "stats" and len(parts) == 2:
                lid = self._parse_int(parts[1])
                if lid is None:
                    return self._send_json(400, {"error": "invalid location_id"})
                info = state.resolver.classify(lid)
                if info.kind == "unknown":
                    return self._send_json(404, {"error": "unknown location"})
                body = self._read_body()
                if body is None:
                    return
                type_ids = self._parse_type_ids(body)
                if type_ids is None:
                    return

                # Try to serve from precomputed files first. Regions and NPC
                # stations are precomputed; constellation/system/structure
                # fall back to live compute.
                precomputed_path = None
                empty_template = _empty_strict_record
                if info.kind in ("region", "station"):
                    precomputed_path = precomputed_stats_file(state.data_dir, lid)

                file_payload = _load_precomputed_json(precomputed_path) if precomputed_path else None
                if file_payload is not None:
                    out = {}
                    for tid in type_ids:
                        key = str(tid)
                        out[key] = file_payload.get(key) or empty_template()
                    return self._send_json(200, out)

                # Fallback: live compute (covers constellations, systems, and
                # the gap before the first precompute cycle finishes).
                snap_path = latest_snapshot_path(state.data_dir)
                if snap_path is None:
                    return self._send_json(503, {"error": "no snapshots yet"})
                payload = compute_live_stats(snap_path, info, type_ids)
                return self._send_json(200, payload)

            if head == "history" and len(parts) == 3:
                lid = self._parse_int(parts[1])
                if lid is None:
                    return self._send_json(400, {"error": "invalid location_id"})
                rng = parse_range(parts[2])
                if rng is None:
                    return self._send_json(400, {
                        "error": "invalid range",
                        "hint": "use <int><h|d|w|m|y>, e.g. 1h, 7d, 1w, 3m, 1y",
                    })
                info = state.resolver.classify(lid)
                if info.kind == "unknown":
                    return self._send_json(404, {"error": "unknown location"})
                if info.kind == "structure":
                    return self._send_json(
                        400,
                        {"error": "history not available for player structures (ESI is region-only)"},
                    )
                body = self._read_body()
                if body is None:
                    return
                type_ids = self._parse_type_ids(body)
                if type_ids is None:
                    return

                # Precomputed history files exist per (region, fixed range)
                # and per (NPC station, fixed range). Prefer station scope when
                # the request targets one; otherwise use region scope.
                range_label = _PRECOMPUTED_HISTORY_RANGES.get(rng)
                region_ids = sorted(info.region_ids)
                file_payload = None
                if range_label:
                    if info.kind == "station":
                        file_payload = _load_precomputed_json(
                            precomputed_history_station_file(
                                state.data_dir, lid, range_label,
                            )
                        )
                    if file_payload is None and len(region_ids) == 1:
                        rid = region_ids[0]
                        file_payload = _load_precomputed_json(
                            precomputed_history_file(state.data_dir, rid, range_label)
                        )
                if file_payload is not None:
                    out = {}
                    for tid in type_ids:
                        key = str(tid)
                        out[key] = file_payload.get(key) or _empty_history_record(rng)
                    return self._send_json(200, out)

                # Fallback: live compute (custom ranges, multi-region scopes,
                # or precompute hasn't run yet).
                payload = compute_history_stats(
                    state.data_dir, info, type_ids, rng,
                    client=state.client,
                )
                return self._send_json(200, payload)

            return self._send_json(404, {"error": "not found", "path": self.path})

    return Handler


# ---------------------------------------------------------------------------
# Precomputed-file helpers (used by POST /stats and POST /history)
# ---------------------------------------------------------------------------
_EMPTY_SIDE_STRICT = {
    "weightedAverage": "0", "max": "0", "min": "0", "stddev": "0",
    "median": "0", "volume": "0", "orderCount": "0", "percentile": "0",
}


def _empty_strict_record() -> dict:
    return {"buy": dict(_EMPTY_SIDE_STRICT), "sell": dict(_EMPTY_SIDE_STRICT)}


def _empty_history_record(range_seconds: int) -> dict:
    return {"buy": dict(_EMPTY_SIDE_STRICT), "sell": dict(_EMPTY_SIDE_STRICT)}


def _load_precomputed_json(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def _iter_universe_history(data_dir: Path, range_label: str):
    """Yield one NDJSON row per precomputed location for the given range.

    Walks ``precomputed/history/`` (regions) and ``precomputed/history_station/``
    (NPC stations) for files matching ``*_{range_label}.json``. Each row::

        {"scope": "region"|"station",
         "location_id": <int>,
         "range": "<label>",
         "types": {"<type_id>": {"buy": {...}, "sell": {...}}, ...}}
    """
    suffix = f"_{range_label}.json"
    base = precomputed_dir(data_dir)
    for scope, sub in (("region", "history"), ("station", "history_station")):
        d = base / sub
        if not d.is_dir():
            continue
        # Sorted for stable streaming order; not required for correctness.
        for entry in sorted(d.iterdir()):
            name = entry.name
            if not name.endswith(suffix) or not entry.is_file():
                continue
            stem = name[:-len(suffix)]
            try:
                lid = int(stem)
            except ValueError:
                continue
            payload = _load_precomputed_json(entry)
            if not payload:
                continue
            yield {
                "scope": scope,
                "location_id": lid,
                "range": range_label,
                "types": payload,
            }


# Fixed history ranges that match precompute.HISTORY_RANGES.
_PRECOMPUTED_HISTORY_RANGES: dict[int, str] = {
    604_800: "7d",
    1_209_600: "14d",
    2_592_000: "30d",
}


def _trade_matches(trade: dict, info, resolver: LocationResolver) -> bool:
    """Reuse the order matcher: trade rows carry region_id + location_id.

    For buy-side trades whose ``location_id`` was nulled (range > station), we
    can still attribute exactly when the requested scope FULLY CONTAINS the
    buyer's reachable system set: e.g. a ``solarsystem`` buy at Jita matches
    a Jita-system query, a Forge-region query, and a Kimotoro-constellation
    query, but NOT a station query (use ``attribution=jump_weighted`` for
    that).
    """
    # Fabricate an order-shaped dict so index.matches() can be reused.
    pseudo = {
        "region_id": trade.get("region_id"),
        "location_id": trade.get("location_id"),
        "system_id": trade.get("system_id"),
    }
    loc = trade.get("location_id")
    if pseudo["system_id"] is None and loc is not None \
            and 60_000_000 <= int(loc) <= 63_999_999:
        pseudo["system_id"] = resolver.system_of_station.get(int(loc))
    if matches(pseudo, info):
        return True

    # Fallback: null location_id buy-side trade with known buyer scope.
    if loc is not None or info.kind in ("station", "structure", "unknown"):
        return False
    rng = trade.get("buyer_range")
    bsid = trade.get("buyer_station_id")
    if rng is None or bsid is None:
        return False
    buyer_sys = resolver.system_of_station.get(int(bsid))
    if buyer_sys is None:
        return False
    buyer_region = resolver.region_for_system(buyer_sys)

    rng_str = str(rng).lower()
    if rng_str == "solarsystem":
        # Buyer fills only in their own system.
        if info.kind == "system":
            return buyer_sys in info.system_ids
        if info.kind == "constellation":
            return buyer_sys in info.system_ids
        if info.kind == "region":
            return buyer_region in info.region_ids
        return False
    if rng_str == "region":
        # Buyer fills anywhere in their region.
        if info.kind in ("region", "constellation", "system"):
            return buyer_region is not None and buyer_region in info.region_ids
        return False
    # constellation / numeric ranges: only safe when the requested scope is
    # the whole region (a strict superset of any sub-region buy reach).
    if info.kind == "region":
        return buyer_region is not None and buyer_region in info.region_ids
    return False


def serve(
    sde_dir: Path,
    data_dir: Path,
    *,
    bind: str = "0.0.0.0",
    port: int = 13307,
    client: Optional[EsiClient] = None,
) -> ThreadingHTTPServer:
    state = MarketState(sde_dir, data_dir, client=client)
    handler_cls = _make_handler(state)
    httpd = ThreadingHTTPServer((bind, port), handler_cls)
    logger.info("eveMarket HTTP listening on http://%s:%s", bind, port)
    return httpd
