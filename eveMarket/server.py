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
from .jumps import JumpGraph
from .location import LocationResolver
from .precompute import (
    history_file as precomputed_history_file,
    read_meta as read_precomputed_meta,
    stats_attr_file as precomputed_stats_attr_file,
    stats_file as precomputed_stats_file,
)
from .snapshot import (
    find_nearest_snapshot,
    latest_snapshot,
    list_snapshots,
    orders_path,
    previous_snapshot,
)
from .stats import (
    compute_history_stats,
    compute_live_stats,
    compute_live_stats_with_attribution,
    latest_snapshot_path,
    parse_range,
)
from .attribution import estimated_inferred_for_station

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
        self.jumps = JumpGraph(self.sde_dir)


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
                    "/orders                       (entire latest snapshot)",
                    "/orders/{unix_time}[?location=<id>]",
                    "/inferred/{location_id}[?attribution=jump_weighted]",
                    "POST /stats/{location_id}[?attribution=jump_weighted]  body: [type_id, ...]",
                    "POST /history/{location_id}/{range}  body: [type_id, ...]",
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
                attribution = (qs.get("attribution", [None])[0] or "").lower() or None
                if attribution and attribution != "jump_weighted":
                    return self._send_json(400, {"error": "unknown attribution mode",
                                                  "hint": "use attribution=jump_weighted"})
                if attribution == "jump_weighted" and info.kind != "station":
                    return self._send_json(400, {
                        "error": "attribution=jump_weighted requires a station scope",
                    })

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

                if attribution == "jump_weighted":
                    rows = estimated_inferred_for_station(
                        source_iter,
                        target_station_id=lid,
                        resolver=state.resolver,
                        jumps=state.jumps,
                        liquidity_index=None,
                    )
                    return self._send_ndjson(rows, extra_headers={
                        "X-Attribution": "jump_weighted",
                        "X-Snapshot-Unix": latest,
                    })

                rows = (t for t in source_iter if _trade_matches(t, info, state.resolver))
                return self._send_ndjson(rows, extra_headers={"X-Snapshot-Unix": latest})

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
                attribution = (urlsplit(self.path).query and
                               parse_qs(urlsplit(self.path).query).get(
                                   "attribution", [None])[0]) or None
                if attribution:
                    attribution = attribution.lower()
                if attribution and attribution != "jump_weighted":
                    return self._send_json(400, {"error": "unknown attribution mode",
                                                  "hint": "use attribution=jump_weighted"})
                if attribution == "jump_weighted" and info.kind != "station":
                    return self._send_json(400, {
                        "error": "attribution=jump_weighted requires a station scope",
                    })
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
                if attribution == "jump_weighted" and info.kind == "station":
                    precomputed_path = precomputed_stats_attr_file(state.data_dir, lid)
                    empty_template = _empty_attr_record
                elif info.kind in ("region", "station"):
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
                if attribution == "jump_weighted":
                    payload = compute_live_stats_with_attribution(
                        snap_path, lid, type_ids,
                        resolver=state.resolver, jumps=state.jumps,
                    )
                else:
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

                # Precomputed history files exist per (region, fixed range).
                # Resolve scope to its single region (multi-region scopes are
                # rare; only kind="region"/"constellation"/"system"/"station"
                # are valid here, and each maps to exactly one region for the
                # purposes of /history).
                range_label = _PRECOMPUTED_HISTORY_RANGES.get(rng)
                region_ids = sorted(info.region_ids)
                if range_label and len(region_ids) == 1:
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


def _empty_attr_record() -> dict:
    buy = dict(_EMPTY_SIDE_STRICT)
    buy["exact_volume"] = "0.0"
    buy["estimated_volume"] = "0.0"
    buy["estimated_order_count"] = "0"
    return {"buy": buy, "sell": dict(_EMPTY_SIDE_STRICT),
            "attribution": "jump_weighted"}


def _empty_history_record(range_seconds: int) -> dict:
    return {
        "average": 0.0, "date": "", "range": int(range_seconds),
        "highest": 0.0, "lowest": 0.0, "order_count": 0, "volume": 0,
    }


def _load_precomputed_json(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


# Fixed history ranges that match precompute.HISTORY_RANGES.
_PRECOMPUTED_HISTORY_RANGES: dict[int, str] = {
    86_400: "1d",
    604_800: "7d",
    2_592_000: "30d",
    7_776_000: "90d",
    31_536_000: "1y",
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
