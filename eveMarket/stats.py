"""Live + historical market-stats aggregation for the POST endpoints.

`/stats/{location_id}`   -> per-type buy/sell statistics from the latest snapshot
`/history/{location_id}/{range}` -> per-type aggregated history over a time range
"""
from __future__ import annotations

import logging
import math
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

from .esi import EsiClient
from .history import HistoryStore
from .index import iter_snapshot, matches
from .location import LocationInfo, LocationResolver
from .snapshot import latest_snapshot, orders_path

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Range parser: "1h", "644h", "1d", "1w", "1m", "1y", etc.
# ---------------------------------------------------------------------------
_RANGE_RE = re.compile(r"^\s*(\d+)\s*([hdwmy])\s*$", re.IGNORECASE)
_RANGE_MULT = {
    "h": 3600,
    "d": 86_400,
    "w": 604_800,
    "m": 2_592_000,   # 30 days
    "y": 31_536_000,  # 365 days
}


def parse_range(spec: str) -> Optional[int]:
    """Return seconds for ``<int><h|d|w|m|y>``, or None if invalid."""
    if not isinstance(spec, str):
        return None
    m = _RANGE_RE.match(spec)
    if not m:
        return None
    n = int(m.group(1))
    unit = m.group(2).lower()
    if n <= 0:
        return None
    return n * _RANGE_MULT[unit]


# ---------------------------------------------------------------------------
# Live stats from the latest snapshot
# ---------------------------------------------------------------------------
def _percentile(sorted_vals: list[float], q: float) -> float:
    """Linear-interpolation percentile. ``q`` in [0,100]. Empty -> 0.0."""
    if not sorted_vals:
        return 0.0
    if len(sorted_vals) == 1:
        return float(sorted_vals[0])
    pos = (q / 100.0) * (len(sorted_vals) - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return float(sorted_vals[lo])
    frac = pos - lo
    return float(sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * frac)


def _median(sorted_vals: list[float]) -> float:
    if not sorted_vals:
        return 0.0
    n = len(sorted_vals)
    mid = n // 2
    if n % 2:
        return float(sorted_vals[mid])
    return float((sorted_vals[mid - 1] + sorted_vals[mid]) / 2.0)


def _weighted_stats(rows: list[tuple[float, float]]) -> dict:
    """Compute summary stats for a side. ``rows`` = list of (price, volume)."""
    if not rows:
        return {
            "weightedAverage": "0",
            "max": "0",
            "min": "0",
            "stddev": "0",
            "median": "0",
            "volume": "0",
            "orderCount": "0",
            "percentile": "0",
        }
    prices = [p for p, _ in rows]
    volumes = [v for _, v in rows]
    total_vol = sum(volumes)
    if total_vol > 0:
        wmean = sum(p * v for p, v in rows) / total_vol
        # Volume-weighted variance.
        var = sum(((p - wmean) ** 2) * v for p, v in rows) / total_vol
        stddev = math.sqrt(var) if var > 0 else 0.0
    else:
        wmean = sum(prices) / len(prices)
        stddev = 0.0
    sorted_prices = sorted(prices)
    return {
        "weightedAverage": f"{wmean:.11g}",
        "max": f"{max(prices):.11g}",
        "min": f"{min(prices):.11g}",
        "stddev": f"{stddev:.11g}",
        "median": f"{_median(sorted_prices):.11g}",
        "volume": f"{total_vol:.1f}",
        "orderCount": str(len(rows)),
        # Eve community: 5th percentile of sells / 95th of buys (low end of asks,
        # high end of bids). Caller picks the side; we just compute both consistently.
        "percentile": f"{_percentile(sorted_prices, 5.0):.11g}",
    }


def _weighted_stats_buy_side(rows: list[tuple[float, float]]) -> dict:
    """Like _weighted_stats but uses 95th percentile for the buy side."""
    base = _weighted_stats(rows)
    if rows:
        sorted_prices = sorted(p for p, _ in rows)
        base["percentile"] = f"{_percentile(sorted_prices, 95.0):.11g}"
    return base


def compute_live_stats(
    snapshot_path: Path,
    info: LocationInfo,
    type_ids: Iterable[int],
) -> dict[str, dict]:
    """Single streaming pass over the snapshot; per-type buy/sell summary."""
    wanted = {int(t) for t in type_ids}
    if not wanted:
        return {}
    # (type_id, is_buy) -> list of (price, volume_remain)
    by_side: dict[tuple[int, bool], list[tuple[float, float]]] = {
        (t, False): [] for t in wanted
    }
    by_side.update({(t, True): [] for t in wanted})

    for order in iter_snapshot(snapshot_path):
        try:
            tid = int(order.get("type_id"))
        except (TypeError, ValueError):
            continue
        if tid not in wanted:
            continue
        if not matches(order, info):
            continue
        try:
            price = float(order.get("price", 0))
            vol = float(order.get("volume_remain", 0))
        except (TypeError, ValueError):
            continue
        if vol <= 0:
            continue
        is_buy = bool(order.get("is_buy_order"))
        by_side[(tid, is_buy)].append((price, vol))

    out: dict[str, dict] = {}
    for tid in sorted(wanted):
        out[str(tid)] = {
            "buy": _weighted_stats_buy_side(by_side[(tid, True)]),
            "sell": _weighted_stats(by_side[(tid, False)]),
        }
    return out


def latest_snapshot_path(data_dir: Path) -> Optional[Path]:
    snap = latest_snapshot(data_dir)
    if snap is None:
        return None
    return orders_path(data_dir, snap)


def compute_live_stats_with_attribution(
    snapshot_path: Path,
    target_station_id: int,
    type_ids: Iterable[int],
    *,
    resolver: LocationResolver,
    jumps,
) -> dict[str, dict]:
    """Live stats for a station with jump-weighted buy-side attribution.

    Sell side: exact-only (sell orders always carry concrete location_id).
    Buy side: includes exact station-range buys at the target plus
    fractionally-attributed shares of non-station-range buys reachable to
    the target station. The response carries an ``attribution`` annotation.
    """
    # Local import to avoid a circular import at module load time.
    from .attribution import estimated_buy_orders_for_station

    wanted = sorted({int(t) for t in type_ids})
    info = resolver.classify(int(target_station_id))
    base = compute_live_stats(snapshot_path, info, wanted)

    estimated = estimated_buy_orders_for_station(
        snapshot_path,
        target_station_id=int(target_station_id),
        type_ids=wanted,
        resolver=resolver,
        jumps=jumps,
        use_liquidity_bias=True,
    )

    out: dict[str, dict] = {}
    for tid in wanted:
        key = str(tid)
        rec = base.get(key, {"buy": _weighted_stats_buy_side([]),
                              "sell": _weighted_stats([])})
        est_rows = estimated.get(int(tid), [])
        est_volume = sum(v for _, v in est_rows)
        # Re-blend buy side with estimated rows for an attributed view.
        try:
            exact_volume = float(rec["buy"]["volume"])
        except (TypeError, ValueError, KeyError):
            exact_volume = 0.0
        try:
            exact_orders = int(rec["buy"]["orderCount"])
        except (TypeError, ValueError, KeyError):
            exact_orders = 0
        rec["buy"]["exact_volume"] = f"{exact_volume:.1f}"
        rec["buy"]["estimated_volume"] = f"{est_volume:.1f}"
        rec["buy"]["volume"] = f"{exact_volume + est_volume:.1f}"
        rec["buy"]["estimated_order_count"] = str(len(est_rows))
        rec["buy"]["orderCount"] = str(exact_orders + len(est_rows))
        rec["attribution"] = "jump_weighted"
        out[key] = rec
    return out


# ---------------------------------------------------------------------------
# Historic stats via /markets/{region_id}/history/
# ---------------------------------------------------------------------------
def _regions_for(info: LocationInfo) -> list[int]:
    """Regions to query history for, given a LocationInfo."""
    if info.kind in ("region", "constellation", "system", "station"):
        return sorted(info.region_ids)
    # structures and unknowns: no history available via ESI
    return []


def _date_to_unix(date_str: str) -> Optional[int]:
    """Parse ESI's daily ``YYYY-MM-DD`` date to a UTC unix timestamp."""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None
    return int(dt.timestamp())


def _aggregate_history(
    rows: list[dict],
    range_seconds: int,
    *,
    now_unix: Optional[int] = None,
) -> Optional[dict]:
    """Aggregate ESI daily history rows into a single record over ``range_seconds``.

    Returns None if no rows fall within the window AND no rows exist at all.
    If rows exist but the window is too small (< 1 day), uses lifetime instead.
    """
    if not rows:
        return None
    if now_unix is None:
        now_unix = int(time.time())
    cutoff = now_unix - int(range_seconds)
    in_window = []
    for r in rows:
        ts = _date_to_unix(r.get("date", ""))
        if ts is None:
            continue
        if ts >= cutoff:
            in_window.append((ts, r))

    used = in_window
    if not used:
        # Fall back to lifetime so the caller still gets data, but the
        # response will reflect the actual covered range.
        used = [(ts, r) for ts, r in
                ((_date_to_unix(r.get("date", "")), r) for r in rows)
                if ts is not None]
        if not used:
            return None

    used.sort(key=lambda x: x[0])
    total_vol = 0.0
    total_orders = 0
    weighted_sum = 0.0
    highest = -math.inf
    lowest = math.inf
    for _, r in used:
        try:
            v = float(r.get("volume", 0))
            avg = float(r.get("average", 0))
            oc = int(r.get("order_count", 0))
            hi = float(r.get("highest", 0))
            lo = float(r.get("lowest", 0))
        except (TypeError, ValueError):
            continue
        total_vol += v
        total_orders += oc
        weighted_sum += avg * v
        if hi > highest:
            highest = hi
        if lo < lowest:
            lowest = lo
    avg_price = (weighted_sum / total_vol) if total_vol > 0 else 0.0

    last_ts, _ = used[-1]
    first_ts, _ = used[0]
    actual_range = max(int(range_seconds), last_ts - first_ts + 86_400)
    # Cap actual_range to the requested range when fully satisfied.
    if last_ts - first_ts + 86_400 < int(range_seconds):
        actual_range = last_ts - first_ts + 86_400

    return {
        "average": float(avg_price),
        "date": datetime.fromtimestamp(last_ts, tz=timezone.utc).strftime("%Y-%m-%d"),
        "range": int(actual_range),
        "highest": float(highest if highest != -math.inf else 0.0),
        "lowest": float(lowest if lowest != math.inf else 0.0),
        "order_count": int(total_orders),
        "volume": int(total_vol),
    }


def compute_history_stats(
    data_dir: Path,
    info: LocationInfo,
    type_ids: Iterable[int],
    range_seconds: int,
    *,
    client: Optional[EsiClient] = None,
) -> dict[str, dict]:
    """Per-type aggregated history (live fallback) using inferred trades.

    Returns the same ``{"buy": {...}, "sell": {...}}`` shape as ``/stats``.
    Used when the requested range/scope isn't covered by the precomputed
    files. ``info`` selects which trades are included:

    - ``region`` / ``constellation`` / ``system`` / ``station``: bucket trades
      whose ``region_id`` (or ``location_id`` for station scope) matches.
    - ``structure`` / ``unknown``: empty payload.
    """
    # Local imports to avoid circulars at module load.
    from .compression import _inferred_jsonl_path, _list_inferred_unix, open_jsonl

    wanted = sorted({int(t) for t in type_ids})
    if not wanted:
        return {}
    if info.kind in ("structure", "unknown"):
        return {str(t): {"buy": _weighted_stats_buy_side([]),
                         "sell": _weighted_stats([])} for t in wanted}

    region_ids = set(info.region_ids)
    station_id = int(info.location_id) if info.kind == "station" else None
    # constellation/system: any trade in the region whose location is inside
    # the configured system set.
    system_set: Optional[set[int]] = None
    if info.kind in ("constellation", "system"):
        system_set = {int(s) for s in (info.system_ids or set())}

    wanted_set = set(wanted)
    now = int(time.time())
    cutoff = now - int(range_seconds)
    inferred_unix = [u for u in _list_inferred_unix(data_dir) if u >= cutoff]

    # type_id -> (buy_rows, sell_rows)
    buckets: dict[int, tuple[list[tuple[float, float]], list[tuple[float, float]]]] = {
        t: ([], []) for t in wanted
    }

    import json as _json
    for unix in inferred_unix:
        path = _inferred_jsonl_path(data_dir, unix)
        try:
            with open_jsonl(path) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = _json.loads(line)
                        tid = int(row.get("type_id"))
                    except (TypeError, ValueError, _json.JSONDecodeError):
                        continue
                    if tid not in wanted_set:
                        continue
                    rid = row.get("region_id")
                    try:
                        rid_i = int(rid) if rid is not None else None
                    except (TypeError, ValueError):
                        rid_i = None
                    if rid_i is None or rid_i not in region_ids:
                        continue
                    if station_id is not None:
                        loc = row.get("location_id")
                        try:
                            loc_i = int(loc) if loc is not None else None
                        except (TypeError, ValueError):
                            loc_i = None
                        if loc_i != station_id:
                            continue
                    elif system_set is not None:
                        sys_i = row.get("system_id")
                        try:
                            sys_v = int(sys_i) if sys_i is not None else None
                        except (TypeError, ValueError):
                            sys_v = None
                        if sys_v is None or sys_v not in system_set:
                            continue
                    try:
                        price = float(row.get("price", 0))
                        vol = float(row.get("volume", 0))
                    except (TypeError, ValueError):
                        continue
                    if price <= 0 or vol <= 0:
                        continue
                    pair = buckets[tid]
                    (pair[0] if row.get("is_buy_order") else pair[1]).append((price, vol))
        except FileNotFoundError:
            continue

    out: dict[str, dict] = {}
    for tid in wanted:
        buy_rows, sell_rows = buckets[tid]
        out[str(tid)] = {
            "buy": _weighted_stats_buy_side(buy_rows),
            "sell": _weighted_stats(sell_rows),
        }
    return out
