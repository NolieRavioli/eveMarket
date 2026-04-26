"""HTTP API for eveMarket.

Endpoints:
  GET /health                       -> {ok, snapshots, latest_unix}
  GET /currentOrders/{location_id}  -> NDJSON of orders matching the location
  GET /orders/{unix_time}           -> NDJSON of the snapshot nearest unix_time
                                        Optional ?location=<id> filters within.
  GET /inferred/{location_id}       -> NDJSON of inferred trades from the latest
                                        snapshot pair, filtered to the location.

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
from .index import filter_snapshot, iter_snapshot, matches
from .inferred import diff_snapshots, load_inferred
from .location import LocationResolver
from .snapshot import (
    find_nearest_snapshot,
    latest_snapshot,
    list_snapshots,
    orders_path,
    previous_snapshot,
)

logger = logging.getLogger(__name__)


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
                return self._send_json(200, {"service": "eveMarket", "see": [
                    "/health",
                    "/currentOrders/{location_id}",
                    "/orders/{unix_time}[?location=<id>]",
                    "/inferred/{location_id}",
                ]})

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
                cached = load_inferred(state.data_dir, latest)
                if cached is not None:
                    rows = (t for t in cached if _trade_matches(t, info, state.resolver))
                    return self._send_ndjson(rows)
                prev = previous_snapshot(state.data_dir, latest)
                if prev is None:
                    return self._send_json(503, {"error": "need at least 2 snapshots"})
                trades = diff_snapshots(orders_path(state.data_dir, prev),
                                        orders_path(state.data_dir, latest),
                                        data_dir=state.data_dir,
                                        client=state.client)
                rows = (t for t in trades if _trade_matches(t, info, state.resolver))
                return self._send_ndjson(rows)

            return self._send_json(404, {"error": "not found", "path": self.path})

    return Handler


def _trade_matches(trade: dict, info, resolver: LocationResolver) -> bool:
    """Reuse the order matcher: trade rows carry region_id + location_id."""
    # Fabricate an order-shaped dict so index.matches() can be reused.
    pseudo = {
        "region_id": trade.get("region_id"),
        "location_id": trade.get("location_id"),
        "system_id": None,
    }
    loc = trade.get("location_id")
    if loc is not None and 60_000_000 <= int(loc) <= 63_999_999:
        pseudo["system_id"] = resolver.system_of_station.get(int(loc))
    return matches(pseudo, info)


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
