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
  GET /contracts/itemExchange       -> NDJSON of active item-exchange contracts with items
  GET /contracts/bpc?type_id=       -> per-run prices of single-BPC contracts
  GET /esi/adjustedPrices           -> cached ESI /markets/prices (1 h)
  GET /esi/costIndices              -> cached ESI /industry/systems (1 h)
  POST /quote/{location_id}         -> order-book fills for real quantities
  POST /esiHistory/{region_id}      -> cached ESI daily history + 7/30-day summaries

NDJSON: one JSON object per line; Content-Type: application/x-ndjson.
Responses are gzip-encoded when the client sends ``Accept-Encoding: gzip``
(which ``requests`` does by default and decompresses transparently).
"""
from __future__ import annotations

import collections
import gzip
import html
import json
import logging
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Iterable, Optional
from urllib.parse import parse_qs, urlsplit

from .esi import EsiClient
from .scheduler import CollectionStatus
from .contracts import (
    bpc_summary,
    contract_items_stats,
    iter_auction_contracts,
    iter_courier_contracts,
    iter_item_exchange_contracts,
    iter_item_exchange_feed,
)
from .esi_history import MAX_TYPES_PER_REQUEST, EsiHistoryStore
from .jumps import JumpGraph
from .quote import BookIndex, QuoteError, parse_lines
from .quote import quote as quote_book
from .refdata import RefData
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
from ._browser import make_browser_html as _make_browser_html
from ._appraisal import APPRAISAL_HTML, appraise, resolve_location_names
from ._skillbook import SKILLBOOK_HTML, skillbook_maxx

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared CSS + HTML page builders
# ---------------------------------------------------------------------------

_CSS = """\
:root{--bg:#0d1117;--bg2:#161b22;--border:#30363d;--text:#c9d1d9;--accent:#58a6ff;\
--green:#3fb950;--orange:#d29922;--red:#f85149;--head:#e6edf3;--code-bg:#1f2428}
*{box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif;\
background:var(--bg);color:var(--text);margin:0;padding:1.5rem 2rem;line-height:1.6;max-width:1100px}
h1{color:var(--head);font-size:1.8rem;border-bottom:1px solid var(--border);padding-bottom:.4rem;margin-bottom:.75rem}
h2{color:var(--head);font-size:1.1rem;border-bottom:1px solid var(--border);padding-bottom:.25rem;margin-top:1.8rem}
a{color:var(--accent);text-decoration:none}a:hover{text-decoration:underline}
code{font-family:"SFMono-Regular",Consolas,"Liberation Mono",Menlo,monospace;\
background:var(--code-bg);border-radius:4px;padding:.15em .4em;font-size:.875em}
pre{background:var(--code-bg);border:1px solid var(--border);border-radius:6px;\
padding:.75em 1em;overflow-x:auto;font-size:.85rem;white-space:pre-wrap}
table{border-collapse:collapse;width:100%;margin:.6em 0;font-size:.9rem}
th,td{border:1px solid var(--border);padding:.4em .7em;text-align:left}
th{background:var(--bg2);color:var(--head)}
.get{color:var(--green);font-weight:700}.post{color:var(--orange);font-weight:700}
.tools{display:flex;gap:.5rem;flex-wrap:wrap;margin:.6rem 0 1.5rem}
.tools a{background:var(--bg2);border:1px solid var(--border);border-radius:5px;\
padding:.3rem .75rem;font-size:.9rem;color:var(--accent)}
.tools a:hover{border-color:var(--accent);text-decoration:none}
.cards{display:flex;flex-wrap:wrap;gap:.65rem;margin:.5rem 0 1rem}
.card{background:var(--bg2);border:1px solid var(--border);border-radius:6px;\
padding:.65rem 1rem;min-width:160px;flex:1}
.card h3{margin:0 0 .3rem;color:var(--head);font-size:.9rem;font-weight:600}
.card .val{font-size:1.45rem;font-weight:700;color:var(--accent);line-height:1.2}
.card .sub{font-size:.77rem;color:#8b949e;margin-top:.2rem}
.log-table{width:100%;border-collapse:collapse;font-size:.78rem;font-family:monospace}
.log-table td{padding:.12em .4em;border-bottom:1px solid #21262d;vertical-align:top;word-break:break-all}
.log-INFO{color:var(--text)}.log-WARNING{color:var(--orange)}.log-ERROR,.log-CRITICAL{color:var(--red)}.log-DEBUG{color:#6e7681}
"""


class _RingHandler(logging.Handler):
    """In-memory ring buffer of the last N log records for the dashboard."""

    def __init__(self, maxlen: int = 300) -> None:
        super().__init__()
        self._buf: collections.deque = collections.deque(maxlen=maxlen)
        self.setFormatter(logging.Formatter(
            "%(asctime)s %(levelname)-8s %(name)s: %(message)s",
            datefmt="%H:%M:%S",
        ))

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._buf.append((record.levelno, self.format(record)))
        except Exception:
            self.handleError(record)

    def get_records(self) -> list:
        return list(self._buf)


# ---------------------------------------------------------------------------
# HTML page builders
# ---------------------------------------------------------------------------

def _ago(now_s: float, ts: Optional[float]) -> str:
    if ts is None:
        return "never"
    diff = int(now_s - ts)
    if diff < 0:
        return "just now"
    if diff < 60:
        return f"{diff}s ago"
    if diff < 3600:
        return f"{diff // 60}m {diff % 60:02d}s ago"
    h, m = divmod(diff // 60, 60)
    return f"{h}h {m:02d}m ago"


def _fmt_ts(ts: Optional[float]) -> str:
    if ts is None:
        return "&mdash;"
    return time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(ts))


def _fmt_eta(eta_s: Optional[float]) -> str:
    if eta_s is None or eta_s < 0:
        return ""
    s = int(eta_s)
    m, s2 = divmod(s, 60)
    h, m2 = divmod(m, 60)
    if h:
        return f"{h}h {m2:02d}m"
    if m2:
        return f"{m2}m {s2:02d}s"
    return f"{s2}s"


def _render_status(st: dict, now: float) -> str:
    """Return an HTML status line for a CollectionStatus snapshot dict."""
    if not st.get("active"):
        next_at = st.get("next_at")
        if next_at and next_at > now:
            wait = _fmt_eta(next_at - now)
            return (
                f'<span style="color:#8b949e">○ idle</span>'
                f' &mdash; next run in <b>{wait}</b>'
            )
        return '<span style="color:#8b949e">○ idle</span>'

    parts = ['<span style="color:var(--green)">● COLLECTING</span>']
    phase = st.get("phase") or ""
    if phase:
        parts.append(f'<b>{html.escape(phase)}</b>')
    done = st.get("done", 0)
    total = st.get("total", 0)
    if total:
        pct = int(100 * done / total)
        parts.append(f'{done:,}/{total:,} ({pct}%)')
    eta_str = _fmt_eta(st.get("eta_s"))
    if eta_str:
        parts.append(f'ETA <b>{eta_str}</b>')
    started_at = st.get("started_at")
    if started_at:
        parts.append(f'running {_fmt_eta(now - started_at)}')
    return " &middot; ".join(parts)


def _build_root_html(state: "MarketState") -> bytes:
    run_inf = state.run_inference
    run_con = state.run_contracts

    inf_rows = ""
    if run_inf:
        inf_rows = """\
<tr><td><span class="get">GET</span></td><td><code>/inferred/{location_id}</code></td>\
<td>Inferred trades at a location from the latest snapshot pair</td></tr>
<tr><td><span class="get">GET</span></td><td><code>/history/{range}</code></td>\
<td>Universe-wide precomputed history &mdash; <code>7d</code>, <code>14d</code>, <code>30d</code></td></tr>
<tr><td><span class="post">POST</span></td><td><code>/history/{location_id}/{range}</code></td>\
<td>Inferred-trade history for a JSON array of type IDs</td></tr>"""

    con_rows = ""
    if run_con:
        con_rows = """\
<tr><td><span class="get">GET</span></td><td><code>/contracts/courier</code></td>\
<td>Active courier contracts universe-wide (NDJSON)</td></tr>
<tr><td><span class="get">GET</span></td><td><code>/contracts/sell</code></td>\
<td>Active item-exchange sell contracts &mdash; WTS (NDJSON)</td></tr>
<tr><td><span class="get">GET</span></td><td><code>/contracts/buy</code></td>\
<td>Active item-exchange buy contracts &mdash; WTB, <code>reward&gt;0</code> (NDJSON)</td></tr>
<tr><td><span class="get">GET</span></td><td><code>/contracts/auction</code></td>\
<td>Active auction contracts (NDJSON)</td></tr>"""

    body = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>eveMarket API</title>
<style>{_CSS}</style>
</head>
<body>
<h1>eveMarket API</h1>
<p>Market-data collector and HTTP API for EVE Online. &nbsp;<a href="/dash">Dashboard &rarr;</a></p>

<div class="tools">
<a href="/browser">Market Browser</a>
<a href="/appraisal-maxxer">Appraisal Maxxer</a>
<a href="/skillbook-maxxer">Skillbook Maxxer</a>
<a href="/dash">Dashboard</a>
</div>

<h2>Endpoints</h2>
<table>
<tr><th>Method</th><th>Path</th><th>Description</th></tr>
<tr><td><span class="get">GET</span></td><td><code>/health</code></td>\
<td>Service health, snapshot count and latest timestamp</td></tr>
<tr><td><span class="get">GET</span></td><td><code>/orders</code></td>\
<td>Latest full market snapshot (NDJSON, gzip)</td></tr>
<tr><td><span class="get">GET</span></td><td><code>/orders/{{unix}}</code></td>\
<td>Snapshot nearest to unix timestamp &mdash; optional <code>?location=</code> filter</td></tr>
<tr><td><span class="get">GET</span></td><td><code>/currentOrders/{{location_id}}</code></td>\
<td>Live orders at a region, constellation, system, or station</td></tr>
<tr><td><span class="post">POST</span></td><td><code>/stats/{{location_id}}</code></td>\
<td>Order-book statistics for a JSON array of type IDs</td></tr>
{inf_rows}
{con_rows}
</table>

<h2>Location IDs</h2>
<table>
<tr><th>Kind</th><th>Range</th><th>Example</th></tr>
<tr><td>Region</td><td>10&thinsp;000&thinsp;000&ndash;10&thinsp;999&thinsp;999</td>\
<td><code>10000002</code> (The Forge)</td></tr>
<tr><td>Constellation</td><td>20&thinsp;000&thinsp;000&ndash;20&thinsp;999&thinsp;999</td>\
<td><code>20000020</code> (Kimotoro)</td></tr>
<tr><td>Solar system</td><td>30&thinsp;000&thinsp;000&ndash;30&thinsp;999&thinsp;999</td>\
<td><code>30000142</code> (Jita)</td></tr>
<tr><td>NPC station</td><td>60&thinsp;000&thinsp;000&ndash;63&thinsp;999&thinsp;999</td>\
<td><code>60003760</code> (Jita 4-4)</td></tr>
<tr><td>Player structure</td><td>&ge;&thinsp;1&thinsp;000&thinsp;000&thinsp;000&thinsp;000</td>\
<td><code>1035466617946</code></td></tr>
</table>

<p>All streaming endpoints return <code>application/x-ndjson</code> with \
<code>Content-Encoding: gzip</code>. Default bind: <code>0.0.0.0:13307</code>.</p>
</body>
</html>"""
    return body.encode("utf-8")


def _build_rl_cards(by_group: dict, err_remain, err_reset_at, now: float) -> str:
    """Build HTML card markup for the ESI rate-limit section of the dashboard."""
    cards = []

    if by_group:
        for grp, state in sorted(by_group.items()):
            rem = state.get("remaining")
            lim = state.get("limit")
            win = state.get("window_s")
            if rem is not None and lim:
                pct = rem / lim
                color = "var(--green)" if pct > 0.33 else ("var(--orange)" if pct > 0.1 else "var(--red)")
                val = f'<span style="color:{color}">{rem:,}/{lim:,}</span>'
            elif rem is not None:
                val = str(rem)
            else:
                val = "&mdash;"
            sub = f"window {win}s" if win else "no window seen"
            cards.append(
                f'<div class="card"><h3>{html.escape(grp)}</h3>'
                f'<div class="val">{val}</div>'
                f'<div class="sub">{sub}</div></div>'
            )
    else:
        cards.append(
            '<div class="card"><h3>Request limit</h3>'
            '<div class="val">&mdash;</div>'
            '<div class="sub">no headers seen yet</div></div>'
        )

    # Global error budget (shared across all groups / endpoints).
    if err_remain is not None:
        if err_remain > 50:
            err_color = "var(--green)"
        elif err_remain > 10:
            err_color = "var(--orange)"
        else:
            err_color = "var(--red)"
        err_val = f'<span style="color:{err_color}">{err_remain}</span>'
        if err_reset_at is not None:
            reset_s = max(0, int(err_reset_at - now))
            err_sub = f"resets in {reset_s}s"
        else:
            err_sub = "errors / 60 s window"
    else:
        err_val = "&mdash;"
        err_sub = "no error-limit headers seen"
    cards.append(
        '<div class="card"><h3>Error Budget</h3>'
        f'<div class="val">{err_val}</div>'
        f'<div class="sub">{err_sub}</div></div>'
    )

    return "\n".join(cards)


def _build_dash_html(state: "MarketState") -> bytes:
    from .snapshot import list_snapshots, list_contracts_snapshots
    from .precompute import read_meta as _read_pc_meta

    now = time.time()
    now_str = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(now))

    # --- collector status ---
    orders_st = state.orders_status.snapshot() if state.orders_status else {}
    headers_st = state.headers_status.snapshot() if state.headers_status else {}
    items_st = state.items_status.snapshot() if state.items_status else {}

    # --- market orders ---
    try:
        snaps = list_snapshots(state.data_dir)
    except Exception:
        snaps = []
    latest_order = snaps[-1] if snaps else None

    # --- contracts ---
    try:
        c_snaps = list_contracts_snapshots(state.data_dir)
    except Exception:
        c_snaps = []
    latest_contract = c_snaps[-1] if c_snaps else None

    # --- contract items ---
    items_fetched = 0
    items_mb = "0 B"
    if state.run_contracts:
        try:
            ist = contract_items_stats(state.data_dir)
            items_fetched = ist.get("fetched_count", 0)
            b = ist.get("items_file_bytes", 0)
            items_mb = f"{b / 1_048_576:.1f} MB" if b >= 1024 else f"{b} B"
        except Exception:
            pass

    # --- precompute ---
    try:
        pc = _read_pc_meta(state.data_dir) or {}
    except Exception:
        pc = {}
    pc_snap_unix = pc.get("snapshot_unix")
    pc_gen_unix  = pc.get("generated_unix")
    pc_elapsed   = pc.get("elapsed_s")
    pc_types     = pc.get("type_count")
    pc_strict    = pc.get("strict_files", 0) or 0
    pc_hist_r    = pc.get("history_region_files", 0) or 0
    pc_hist_s    = pc.get("history_station_files", 0) or 0

    # --- rate limits ---
    rl_snap = state.client.rl_snapshot()
    rl_by_group = rl_snap.get("by_group") or {}
    err_remain = rl_snap.get("err_remain")
    err_reset_at = rl_snap.get("err_reset_at")

    # --- log buffer ---
    log_html = "<p style='color:#8b949e;font-size:.85rem'>No log records captured yet.</p>"
    if state.log_ring is not None:
        records = state.log_ring.get_records()
        if records:
            rows = []
            for levelno, text in reversed(records[-200:]):
                lvl = logging.getLevelName(levelno)
                rows.append(
                    f'<tr class="log-{lvl}"><td>{html.escape(text)}</td></tr>'
                )
            log_html = f'<table class="log-table">{"".join(rows)}</table>'

    # --- contracts section ---
    con_section = ""
    if state.run_contracts:
        con_section = f"""
<h2>Contracts</h2>
<div style="margin:.3rem 0 .35rem;padding:.35rem .75rem;background:var(--bg2);border:1px solid var(--border);border-radius:5px;font-size:.88rem"><span style="color:#8b949e;font-size:.8rem;margin-right:.5rem">headers</span>{_render_status(headers_st, now)}</div>
<div style="margin:.3rem 0 .7rem;padding:.35rem .75rem;background:var(--bg2);border:1px solid var(--border);border-radius:5px;font-size:.88rem"><span style="color:#8b949e;font-size:.8rem;margin-right:.5rem">items</span>{_render_status(items_st, now)}</div>
<div class="cards">
<div class="card"><h3>Snapshots</h3><div class="val">{len(c_snaps)}</div>\
<div class="sub">on disk</div></div>
<div class="card"><h3>Latest</h3><div class="val">{_ago(now, latest_contract)}</div>\
<div class="sub">{_fmt_ts(latest_contract)}</div></div>
<div class="card"><h3>Items Cached</h3><div class="val">{items_fetched:,}</div>\
<div class="sub">contracts with items &mdash; {items_mb}</div></div>
</div>"""

    # --- precompute section ---
    pc_section = ""
    if pc:
        pc_section = f"""
<h2>Precompute</h2>
<div class="cards">
<div class="card"><h3>Snapshot</h3><div class="val">{_ago(now, pc_snap_unix)}</div>\
<div class="sub">{_fmt_ts(pc_snap_unix)}</div></div>
<div class="card"><h3>Generated</h3><div class="val">{_ago(now, pc_gen_unix)}</div>\
<div class="sub">elapsed: {pc_elapsed}s</div></div>
<div class="card"><h3>Types</h3><div class="val">{pc_types if pc_types is not None else "&mdash;"}</div>\
<div class="sub">unique types tracked</div></div>
<div class="card"><h3>Files</h3><div class="val">{pc_strict + pc_hist_r + pc_hist_s}</div>\
<div class="sub">{pc_strict} stats &middot; {pc_hist_r} hist-region \
&middot; {pc_hist_s} hist-station</div></div>
</div>"""

    body = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="15">
<title>eveMarket Dashboard</title>
<style>{_CSS}</style>
</head>
<body>
<h1><a href="/" style="color:inherit;text-decoration:none">eveMarket</a> &mdash; Dashboard</h1>
<p style="color:#8b949e;font-size:.85rem">Auto-refreshes every 15 s &mdash; {now_str}</p>

<h2>Market Orders</h2>
<div style="margin:.3rem 0 .7rem;padding:.35rem .75rem;background:var(--bg2);border:1px solid var(--border);border-radius:5px;font-size:.88rem">{_render_status(orders_st, now)}</div>
<div class="cards">
<div class="card"><h3>Snapshots</h3><div class="val">{len(snaps)}</div>\
<div class="sub">on disk</div></div>
<div class="card"><h3>Latest</h3><div class="val">{_ago(now, latest_order)}</div>\
<div class="sub">{_fmt_ts(latest_order)}</div></div>
</div>

{con_section}

<h2>ESI Rate Limits</h2>
<div class="cards">
{_build_rl_cards(rl_by_group, err_remain, err_reset_at, now)}
</div>

{pc_section}

<h2>Recent Logs</h2>
{log_html}
</body>
</html>"""
    return body.encode("utf-8")


# keep this marker so the old constant name doesn't confuse grep searches
class MarketState:
    def __init__(
        self,
        sde_dir: Path,
        data_dir: Path,
        *,
        client: Optional[EsiClient] = None,
        run_inference: bool = True,
        run_contracts: bool = True,
        log_ring: Optional[_RingHandler] = None,
        orders_status: Optional[CollectionStatus] = None,
        headers_status: Optional[CollectionStatus] = None,
        items_status: Optional[CollectionStatus] = None,
        history: Optional[EsiHistoryStore] = None,
        history_warmer=None,
    ) -> None:
        self.sde_dir = Path(sde_dir)
        self.data_dir = Path(data_dir)
        self.client = client or EsiClient()
        self.resolver = LocationResolver(self.sde_dir)
        self.run_inference = run_inference
        self.run_contracts = run_contracts
        self.log_ring = log_ring
        self.orders_status = orders_status
        self.headers_status = headers_status
        self.items_status = items_status
        self.history = history
        self.history_warmer = history_warmer
        self.books = BookIndex(self.resolver, JumpGraph(self.sde_dir))
        self.refdata = RefData(self.data_dir, self.client)


def _make_handler(state: MarketState):

    class Handler(BaseHTTPRequestHandler):
        server_version = "eveMarket/1.0"

        def log_message(self, fmt, *args):
            logger.info("%s - " + fmt, self.address_string(), *args)

        # ---------- helpers ----------
        def _send_json(self, code: int, payload: dict, extra_headers: Optional[dict] = None) -> None:
            body = json.dumps(payload).encode("utf-8")
            self._send_json_bytes(code, body, extra_headers)

        def _send_json_bytes(self, code: int, body: bytes, extra_headers: Optional[dict] = None) -> None:
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            for k, v in (extra_headers or {}).items():
                self.send_header(k, str(v))
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
            """Stream NDJSON rows, always gzip-encoded.

            At ~12x compression on universe-scale payloads, identity-encoded
            responses aren't worth supporting — every standard HTTP client
            (``requests``, ``curl --compressed``, browsers, ``wget``) handles
            ``Content-Encoding: gzip`` transparently. Bare ``curl`` callers
            can pipe through ``gunzip`` (or pass ``--compressed``).

            Body is close-delimited rather than chunked: gzip framing inside
            HTTP/1.1 chunked encoding requires extra wrapping for no benefit
            here.
            """
            self.send_response(200)
            self.send_header("Content-Type", "application/x-ndjson")
            self.send_header("Content-Encoding", "gzip")
            self.send_header("Connection", "close")
            self.close_connection = True
            for k, v in (extra_headers or {}).items():
                self.send_header(k, str(v))
            self.end_headers()
            count = 0
            try:
                gz = gzip.GzipFile(fileobj=self.wfile, mode="wb",
                                   compresslevel=6)
                try:
                    for r in rows:
                        gz.write((json.dumps(r, separators=(",", ":"))
                                  + "\n").encode("utf-8"))
                        count += 1
                finally:
                    gz.close()
                logger.info("ndjson: %s rows (gzip)", count)
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                logger.info("client disconnected after %s rows", count)

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
                return self._send_html(_build_root_html(state))

            head = parts[0]

            if head == "health" and len(parts) == 1:
                snaps = list_snapshots(state.data_dir)
                payload = {
                    "ok": True,
                    "snapshots": len(snaps),
                    "latest_unix": snaps[-1] if snaps else None,
                    "quote_books": state.books.stats(),
                }
                if state.history is not None:
                    payload["history"] = state.history.stats()
                    if state.history_warmer is not None:
                        payload["history"]["warm"] = dict(state.history_warmer.status)
                return self._send_json(200, payload)

            if head == "esi" and len(parts) == 2:
                try:
                    body, fetched_at = state.refdata.get(parts[1])
                except KeyError:
                    return self._send_json(404, {"error": "unknown dataset",
                                                 "hint": "adjustedPrices or costIndices"})
                except RuntimeError as exc:
                    return self._send_json(503, {"error": str(exc)})
                return self._send_json_bytes(200, body, {"X-Fetched-Unix": int(fetched_at)})

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

            if head == "contracts" and len(parts) == 2:
                sub = parts[1]
                snap = latest_contracts_snapshot(state.data_dir)
                if snap is None:
                    return self._send_json(503, {"error": "no contracts snapshot yet"})
                snap_path = contracts_path(state.data_dir, snap)
                if not snap_path.exists():
                    return self._send_json(503, {"error": "contracts snapshot missing on disk"})
                hdrs = {"X-Contracts-Snapshot-Unix": snap}
                if sub == "itemExchange":
                    rid = self._parse_int(qs.get("region_id", [""])[0]) if qs.get("region_id") else None
                    tid = self._parse_int(qs.get("type_id", [""])[0]) if qs.get("type_id") else None
                    return self._send_ndjson(
                        iter_item_exchange_feed(snap_path, data_dir=state.data_dir, region_id=rid, type_id=tid),
                        extra_headers=hdrs,
                    )
                if sub == "bpc":
                    tid = self._parse_int(qs.get("type_id", [""])[0])
                    if tid is None:
                        return self._send_json(400, {"error": "type_id (the blueprint type) is required"})
                    return self._send_json(200, bpc_summary(snap_path, data_dir=state.data_dir, type_id=tid),
                                           extra_headers=hdrs)
                if sub == "courier":
                    return self._send_ndjson(iter_courier_contracts(snap_path), extra_headers=hdrs)
                if sub == "sell":
                    return self._send_ndjson(
                        iter_item_exchange_contracts(
                            snap_path, data_dir=state.data_dir, side="sell"
                        ),
                        extra_headers=hdrs,
                    )
                if sub == "buy":
                    return self._send_ndjson(
                        iter_item_exchange_contracts(
                            snap_path, data_dir=state.data_dir, side="buy"
                        ),
                        extra_headers=hdrs,
                    )
                if sub == "auction":
                    return self._send_ndjson(
                        iter_auction_contracts(snap_path, data_dir=state.data_dir),
                        extra_headers=hdrs,
                    )

            if head == "dash" and len(parts) == 1:
                return self._send_html(_build_dash_html(state))

            # ----- browser UI -----
            if head == "browser" and len(parts) == 1:
                from . import sde_market as _sdm
                _browser_html = _make_browser_html(
                    _sdm.market_group_tree(state.sde_dir),
                    _sdm.regions_list(state.sde_dir),
                )
                return self._send_html(_browser_html)

            # ----- appraisal-maxxer UI -----
            if head == "appraisal-maxxer" and len(parts) == 1:
                return self._send_html(APPRAISAL_HTML)

            # ----- skillbook-maxxer UI -----
            if head == "skillbook-maxxer" and len(parts) == 1:
                return self._send_html(SKILLBOOK_HTML)

            # ----- browser JSON API -----
            if head == "api" and len(parts) >= 2:
                from . import sde_market as _sdm
                sub = parts[1]

                if sub == "market_groups" and len(parts) == 2:
                    data = _sdm.market_group_tree(state.sde_dir)
                    return self._send_json(200, data)

                if sub == "types" and len(parts) == 3:
                    gid = self._parse_int(parts[2])
                    if gid is None:
                        return self._send_json(400, {"error": "invalid group_id"})
                    data = _sdm.types_in_group(state.sde_dir, gid)
                    return self._send_json(200, data)

                if sub == "regions" and len(parts) == 2:
                    data = _sdm.regions_list(state.sde_dir)
                    return self._send_json(200, data)

                if sub == "search_types" and len(parts) == 2:
                    q = qs.get("q", [""])[0].strip().lower()
                    if len(q) < 2:
                        return self._send_json(200, [])
                    cache = _sdm.get_cache(state.sde_dir)
                    groups = cache["groups"]
                    results = []
                    for tid, t in cache["types"].items():
                        if q in t["name"].lower():
                            gid = t["group"]
                            gname = groups.get(gid, {}).get("name", "") if gid else ""
                            results.append({"id": t["id"], "name": t["name"], "groupName": gname})
                    results.sort(key=lambda x: (x["name"].lower().find(q), x["name"].lower()))
                    return self._send_json(200, results[:100])

                if sub == "orders" and len(parts) == 3:
                    tid = self._parse_int(parts[2])
                    if tid is None:
                        return self._send_json(400, {"error": "invalid type_id"})
                    latest = latest_snapshot(state.data_dir)
                    if latest is None:
                        return self._send_json(503, {"error": "no snapshots yet"})
                    snap_path = orders_path(state.data_dir, latest)
                    cache = _sdm.get_cache(state.sde_dir)
                    sec_map = cache["system_security"]
                    rgn_map = cache["region_names"]
                    out = []
                    for o in iter_snapshot(snap_path):
                        if o.get("type_id") != tid:
                            continue
                        sys_id = o.get("system_id")
                        rgn_id = o.get("region_id")
                        o["security_status"] = round(sec_map.get(sys_id, 0.0), 1) if sys_id else None
                        o["region_name"] = rgn_map.get(rgn_id, str(rgn_id)) if rgn_id else None
                        out.append(o)
                    return self._send_json(200, out)

                if sub == "esi_history" and len(parts) in (3, 4):
                    tid = self._parse_int(parts[2])
                    if tid is None:
                        return self._send_json(400, {"error": "invalid type_id"})
                    from .history import load_history as _load_hist
                    # all regions or a specific region
                    if len(parts) == 3 or parts[3] == "all":
                        # aggregate daily history across all cached region files
                        from .snapshot import history_dir as _hist_dir
                        import os as _os
                        hdir = _hist_dir(state.data_dir)
                        agg: dict[str, dict] = {}  # date -> {volume, total_avg, count}
                        for fname in _os.listdir(hdir):
                            if not fname.startswith("region-") or not fname.endswith(".json"):
                                continue
                            try:
                                rid2 = int(fname[len("region-"):-len(".json")])
                            except ValueError:
                                continue
                            hist = _load_hist(state.data_dir, rid2) or {}
                            type_data = hist.get("types", {}).get(str(tid), {})
                            for row in type_data.get("rows", []):
                                d = row.get("date")
                                if not d:
                                    continue
                                vol = row.get("volume", 0) or 0
                                avg = row.get("average", 0) or 0
                                high = row.get("highest", 0) or 0
                                low = row.get("lowest", 0) or 0
                                if d not in agg:
                                    agg[d] = {"volume": 0, "wsum": 0.0,
                                               "highest": 0.0, "lowest": float("inf")}
                                agg[d]["volume"] += vol
                                agg[d]["wsum"] += avg * vol
                                agg[d]["highest"] = max(agg[d]["highest"], high)
                                if low:
                                    agg[d]["lowest"] = min(agg[d]["lowest"], low)
                        rows_out = []
                        for d, v in sorted(agg.items()):
                            vol = v["volume"]
                            avg = (v["wsum"] / vol) if vol else 0.0
                            low = v["lowest"] if v["lowest"] != float("inf") else 0.0
                            rows_out.append({
                                "date": d,
                                "average": avg,
                                "highest": v["highest"],
                                "lowest": low,
                                "volume": vol,
                            })
                        return self._send_json(200, {"rows": rows_out})
                    else:
                        rid = self._parse_int(parts[3])
                        if rid is None:
                            return self._send_json(400, {"error": "invalid region_id"})
                        hist = _load_hist(state.data_dir, rid) or {}
                        type_data = hist.get("types", {}).get(str(tid), {})
                        return self._send_json(200, {"rows": type_data.get("rows", [])})

                if sub == "names" and len(parts) == 2:
                    ids_raw = qs.get("ids", [""])[0]
                    try:
                        ids_list = [int(x) for x in ids_raw.split(",") if x.strip()]
                    except ValueError:
                        return self._send_json(400, {"error": "invalid ids"})
                    if not ids_list:
                        return self._send_json(200, {})
                    import requests as _req
                    try:
                        resp = _req.post(
                            "https://esi.evetech.net/latest/universe/names/",
                            json=ids_list,
                            headers={"User-Agent": "eveMarket/1.0"},
                            timeout=10,
                        )
                        resp.raise_for_status()
                        names = {item["id"]: item["name"] for item in resp.json()}
                        return self._send_json(200, names)
                    except Exception as exc:
                        return self._send_json(502, {"error": str(exc)})

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
                    meta = read_precomputed_meta(state.data_dir) or {}
                    hdrs = {"X-Snapshot-Unix": meta["snapshot_unix"]} if meta.get("snapshot_unix") else None
                    return self._send_json(200, out, hdrs)

                # Fallback: live compute (covers constellations, systems, and
                # the gap before the first precompute cycle finishes).
                snap_path = latest_snapshot_path(state.data_dir)
                if snap_path is None:
                    return self._send_json(503, {"error": "no snapshots yet"})
                payload = compute_live_stats(snap_path, info, type_ids)
                return self._send_json(200, payload, {"X-Snapshot-Unix": latest_snapshot(state.data_dir)})

            if head == "quote" and len(parts) == 2:
                lid = self._parse_int(parts[1])
                if lid is None:
                    return self._send_json(400, {"error": "invalid location_id"})
                info = state.resolver.classify(lid)
                if info.kind == "unknown":
                    return self._send_json(404, {"error": "unknown location"})
                body = self._read_body()
                if body is None:
                    return
                try:
                    lines = parse_lines(body)
                except QuoteError as exc:
                    return self._send_json(400, {"error": str(exc)})
                snap = latest_snapshot(state.data_dir)
                if snap is None:
                    return self._send_json(503, {"error": "no snapshots yet"})
                book = state.books.get(orders_path(state.data_dir, snap), snap, info)
                return self._send_json(200, quote_book(book, lines), {"X-Snapshot-Unix": snap})

            if head == "esiHistory" and len(parts) == 2:
                rid = self._parse_int(parts[1])
                if rid is None or not 10_000_000 <= rid <= 10_999_999:
                    return self._send_json(400, {"error": "region_id must be a region (10000001-10999999)"})
                if state.history is None:
                    return self._send_json(503, {"error": "ESI history cache is disabled"})
                qs = parse_qs(url.query)
                days = self._parse_int(qs.get("days", ["30"])[0]) or 30
                body = self._read_body()
                if body is None:
                    return
                type_ids = self._parse_type_ids(body)
                if type_ids is None:
                    return
                if len(type_ids) > MAX_TYPES_PER_REQUEST:
                    return self._send_json(400, {"error": f"at most {MAX_TYPES_PER_REQUEST} type_ids per request"})
                return self._send_json(200, state.history.get(rid, type_ids, days))

            if head == "appraisal-maxxer" and len(parts) == 1:
                body = self._read_body()
                if body is None:
                    return
                if not isinstance(body, dict):
                    return self._send_json(400, {"error": "body must be a JSON object"})
                text = body.get("items") or ""
                if not isinstance(text, str):
                    return self._send_json(400, {"error": "items must be a string"})
                try:
                    num_orders = int(body.get("num_orders") or 1)
                except (TypeError, ValueError):
                    return self._send_json(400, {"error": "invalid num_orders"})
                try:
                    top_n = int(body.get("top_n") or 10)
                except (TypeError, ValueError):
                    return self._send_json(400, {"error": "invalid top_n"})
                top_n = max(1, min(top_n, 50))
                mode = str(body.get("mode") or "buy").lower()
                if mode not in ("buy", "sell"):
                    return self._send_json(400, {"error": "mode must be 'buy' or 'sell'"})

                result = appraise(
                    state.sde_dir, state.data_dir, state.resolver,
                    text, num_orders, mode, top_n=top_n,
                )
                if "results" in result and result["results"]:
                    name_map = resolve_location_names(
                        r["location_id"] for r in result["results"]
                    )
                    for r in result["results"]:
                        r["location_name"] = name_map.get(r["location_id"])
                code = 400 if result.get("error") else 200
                return self._send_json(code, result)

            if head == "skillbook-maxxer" and len(parts) == 1:
                body = self._read_body()
                if body is None:
                    return
                if not isinstance(body, dict):
                    return self._send_json(400, {"error": "body must be a JSON object"})
                text = body.get("items") or ""
                if not isinstance(text, str):
                    return self._send_json(400, {"error": "items must be a string"})
                hub = str(body.get("hub") or "jita").lower()
                try:
                    markup = float(body.get("markup") or 1.2)
                except (TypeError, ValueError):
                    return self._send_json(400, {"error": "invalid markup"})
                result = skillbook_maxx(
                    state.sde_dir, state.data_dir, text, hub, markup=markup,
                )
                code = 400 if result.get("error") else 200
                return self._send_json(code, result)

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


class _QuietThreadingHTTPServer(ThreadingHTTPServer):
    """ThreadingHTTPServer that doesn't dump tracebacks for client-side resets.

    The default ``socketserver.handle_error`` prints a full traceback whenever
    a client closes the TCP connection before sending a complete request line
    (browsers, ``requests`` connection-pool reuse, port scanners, etc.). Those
    are normal and produce extreme log noise; downgrade to a single debug line.
    """

    _QUIET_EXC = (
        ConnectionResetError,
        ConnectionAbortedError,
        BrokenPipeError,
        TimeoutError,
    )

    def handle_error(self, request, client_address):  # type: ignore[override]
        exc = sys.exc_info()[1]
        if isinstance(exc, self._QUIET_EXC):
            logger.debug("client %s disconnected: %s", client_address, exc)
            return
        super().handle_error(request, client_address)


def serve(
    sde_dir: Path,
    data_dir: Path,
    *,
    bind: str = "0.0.0.0",
    port: int = 13307,
    client: Optional[EsiClient] = None,
    run_inference: bool = True,
    run_contracts: bool = True,
    orders_status: Optional[CollectionStatus] = None,
    headers_status: Optional[CollectionStatus] = None,
    items_status: Optional[CollectionStatus] = None,
    history: Optional[EsiHistoryStore] = None,
    history_warmer=None,
) -> ThreadingHTTPServer:
    ring = _RingHandler()
    logging.getLogger().addHandler(ring)
    state = MarketState(
        sde_dir, data_dir,
        client=client,
        run_inference=run_inference,
        run_contracts=run_contracts,
        log_ring=ring,
        orders_status=orders_status,
        headers_status=headers_status,
        items_status=items_status,
        history=history,
        history_warmer=history_warmer,
    )
    handler_cls = _make_handler(state)
    httpd = _QuietThreadingHTTPServer((bind, port), handler_cls)
    logger.info("eveMarket HTTP listening on http://%s:%s", bind, port)
    return httpd
