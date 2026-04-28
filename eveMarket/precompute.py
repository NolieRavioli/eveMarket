"""Precompute /stats and /history payloads after every snapshot.

Output layout under ``data/precomputed/``::

    meta.json                                   global state (snapshot_unix, gen_unix, counts)
    stats/{location_id}.json                    strict stats for regions + NPC stations
    stats_attr/{station_id}.json                jump-weighted stats for NPC stations
    history/{region_id}_{range}.json            inferred-trade stats by region
    history_station/{station_id}_{range}.json   inferred-trade stats by NPC station

Endpoints become file-reads with a per-request type_id filter.

Pipeline (called from the scheduler after `inferred`)::

    one snapshot pass populates region/station buy/sell buckets AND
    sell-liquidity AND a list of unresolved (non-station-range) buys

    then strict stats are written for every non-empty bucket; jump-weighted
    attribution distributes the unresolved buys across reachable NPC stations
    using ``(1/(1+jumps)) * sell_liquidity`` weights.

    history is built by streaming all inferred-trade files within the
    largest range and bucketing by region + station for each fixed range.
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
import time
from pathlib import Path
from typing import Optional

from .esi import EsiClient
from .compression import open_jsonl, _list_inferred_unix, _inferred_jsonl_path
from .index import iter_snapshot
from .jumps import JumpGraph
from .location import LocationResolver
from .snapshot import latest_snapshot, orders_path
from .stats import (
    _weighted_stats,
    _weighted_stats_buy_side,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------------
HISTORY_RANGES: tuple[tuple[str, int], ...] = (
    ("7d", 604_800),
    ("14d", 1_209_600),
    ("30d", 2_592_000),
)


def precomputed_dir(data_dir: Path) -> Path:
    p = Path(data_dir) / "precomputed"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _stats_dir(data_dir: Path) -> Path:
    p = precomputed_dir(data_dir) / "stats"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _stats_attr_dir(data_dir: Path) -> Path:
    p = precomputed_dir(data_dir) / "stats_attr"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _history_dir(data_dir: Path) -> Path:
    p = precomputed_dir(data_dir) / "history"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _history_station_dir(data_dir: Path) -> Path:
    p = precomputed_dir(data_dir) / "history_station"
    p.mkdir(parents=True, exist_ok=True)
    return p


def stats_file(data_dir: Path, location_id: int) -> Path:
    return _stats_dir(data_dir) / f"{int(location_id)}.json"


def stats_attr_file(data_dir: Path, station_id: int) -> Path:
    return _stats_attr_dir(data_dir) / f"{int(station_id)}.json"


def history_file(data_dir: Path, region_id: int, range_label: str) -> Path:
    return _history_dir(data_dir) / f"{int(region_id)}_{range_label}.json"


def history_station_file(data_dir: Path, station_id: int, range_label: str) -> Path:
    return _history_station_dir(data_dir) / f"{int(station_id)}_{range_label}.json"


def meta_file(data_dir: Path) -> Path:
    return precomputed_dir(data_dir) / "meta.json"


def read_meta(data_dir: Path) -> Optional[dict]:
    try:
        with meta_file(data_dir).open("r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def _atomic_write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=path.name + ".", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, separators=(",", ":"))
        os.replace(tmp_name, path)
    except Exception:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


# ---------------------------------------------------------------------------
# Single-pass aggregation
# ---------------------------------------------------------------------------
class _Aggregator:
    """In-memory buckets built from one snapshot pass."""

    __slots__ = (
        "region_buy", "region_sell",
        "station_buy", "station_sell",
        "sell_liquidity",
        "unresolved_buys",
        "all_types",
    )

    def __init__(self) -> None:
        # Per-(location_id, type_id) lists of (price, volume_remain).
        self.region_buy: dict[int, dict[int, list[tuple[float, float]]]] = {}
        self.region_sell: dict[int, dict[int, list[tuple[float, float]]]] = {}
        self.station_buy: dict[int, dict[int, list[tuple[float, float]]]] = {}
        self.station_sell: dict[int, dict[int, list[tuple[float, float]]]] = {}
        # type_id -> station_id -> remaining sell volume (for liquidity bias).
        self.sell_liquidity: dict[int, dict[int, float]] = {}
        # Non-station-range buys to attribute later. Each row:
        #   (type_id, buyer_station_id, range_str, volume_remain)
        self.unresolved_buys: list[tuple[int, int, str, float]] = []
        self.all_types: set[int] = set()

    def add_order(self, order: dict) -> None:
        try:
            tid = int(order.get("type_id"))
            price = float(order.get("price", 0))
            vol = float(order.get("volume_remain", 0))
        except (TypeError, ValueError):
            return
        if vol <= 0:
            return
        rid = order.get("region_id")
        if rid is None:
            return
        try:
            rid_i = int(rid)
        except (TypeError, ValueError):
            return
        loc = order.get("location_id")
        try:
            loc_i: Optional[int] = int(loc) if loc is not None else None
        except (TypeError, ValueError):
            loc_i = None
        is_npc_station = (
            loc_i is not None and 60_000_000 <= loc_i <= 63_999_999
        )
        is_buy = bool(order.get("is_buy_order"))

        self.all_types.add(tid)

        # --- region side ---
        side_map = self.region_buy if is_buy else self.region_sell
        side_map.setdefault(rid_i, {}).setdefault(tid, []).append((price, vol))

        if not is_buy:
            # Sell side has a concrete location.
            if is_npc_station:
                self.station_sell.setdefault(loc_i, {}).setdefault(tid, []).append((price, vol))
                # liquidity index (NPC stations only)
                lt = self.sell_liquidity.setdefault(tid, {})
                lt[loc_i] = lt.get(loc_i, 0.0) + vol
            return

        # --- buy side ---
        rng = order.get("range")
        rng_str = str(rng).lower() if rng is not None else "station"
        if rng_str == "station":
            if is_npc_station:
                self.station_buy.setdefault(loc_i, {}).setdefault(tid, []).append((price, vol))
            return
        # Non-station-range buy: needs attribution. Only useful if we know
        # the buyer's station (NPC) so we can locate its system.
        if is_npc_station:
            self.unresolved_buys.append((tid, loc_i, rng_str, vol))


def _scan_snapshot(snap_path: Path) -> _Aggregator:
    agg = _Aggregator()
    n = 0
    t0 = time.monotonic()
    for order in iter_snapshot(snap_path):
        agg.add_order(order)
        n += 1
    logger.info(
        "precompute: scanned %s orders in %.2fs (%s types, %s region buckets, "
        "%s station-buy, %s station-sell, %s unresolved buys)",
        f"{n:,}", time.monotonic() - t0, len(agg.all_types),
        len(agg.region_buy), len(agg.station_buy), len(agg.station_sell),
        len(agg.unresolved_buys),
    )
    return agg


# ---------------------------------------------------------------------------
# Stats writers
# ---------------------------------------------------------------------------
def _make_stats_payload(
    buy_by_type: dict[int, list[tuple[float, float]]],
    sell_by_type: dict[int, list[tuple[float, float]]],
) -> dict[str, dict]:
    out: dict[str, dict] = {}
    types = sorted(set(buy_by_type) | set(sell_by_type))
    for tid in types:
        out[str(tid)] = {
            "buy": _weighted_stats_buy_side(buy_by_type.get(tid, [])),
            "sell": _weighted_stats(sell_by_type.get(tid, [])),
        }
    return out


def _write_strict_stats(data_dir: Path, agg: _Aggregator) -> int:
    """Write stats/{location_id}.json for every non-empty region + NPC station."""
    written = 0
    locations = (
        set(agg.region_buy) | set(agg.region_sell)
        | set(agg.station_buy) | set(agg.station_sell)
    )
    for lid in sorted(locations):
        if 10_000_000 <= lid <= 10_999_999:
            payload = _make_stats_payload(
                agg.region_buy.get(lid, {}),
                agg.region_sell.get(lid, {}),
            )
        elif 60_000_000 <= lid <= 63_999_999:
            payload = _make_stats_payload(
                agg.station_buy.get(lid, {}),
                agg.station_sell.get(lid, {}),
            )
        else:
            continue
        if not payload:
            continue
        _atomic_write_json(stats_file(data_dir, lid), payload)
        written += 1
    return written


# ---------------------------------------------------------------------------
# Jump-weighted attribution (NPC stations)
# ---------------------------------------------------------------------------
_NUMERIC_RANGES = {"1", "2", "3", "4", "5", "10", "20", "30", "40"}


def _candidates_for_scope(
    buyer_station_id: int,
    range_str: str,
    resolver: LocationResolver,
    jumps: JumpGraph,
) -> list[tuple[int, int]]:
    """Return ``[(candidate_station_id, jumps), ...]`` for a buyer's reach.

    Candidates are NPC stations only. ``jumps`` is the buyer-system→station-system
    distance, or 0 when the scope (region/constellation/solarsystem) doesn't
    distinguish per-system distance.
    """
    buyer_sys = resolver.system_of_station.get(int(buyer_station_id))
    if buyer_sys is None:
        return []
    sys_distance: dict[int, int]
    if range_str == "solarsystem":
        sys_distance = {buyer_sys: 0}
    elif range_str == "constellation":
        chosen: set[int] = {buyer_sys}
        for cid, sysset in resolver.systems_in_constellation.items():
            if buyer_sys in sysset:
                chosen = sysset
                break
        sys_distance = {s: 0 for s in chosen}
    elif range_str == "region":
        rid = resolver.region_for_system(buyer_sys)
        if rid is None:
            sys_distance = {buyer_sys: 0}
        else:
            sys_distance = {s: 0 for s in resolver.systems_in_region.get(rid, set())}
    elif range_str in _NUMERIC_RANGES:
        sys_distance = jumps.bfs_within(buyer_sys, int(range_str))
    else:
        return []
    out: list[tuple[int, int]] = []
    for stid, sid in resolver.system_of_station.items():
        d = sys_distance.get(sid)
        if d is None:
            continue
        out.append((stid, d))
    return out


def _attribute_unresolved(
    agg: _Aggregator,
    resolver: LocationResolver,
    jumps: JumpGraph,
) -> dict[int, dict[int, float]]:
    """Fold ``unresolved_buys`` into ``{station_id: {type_id: estimated_volume}}``.

    Also tracks per-(station, type) estimated order count.
    """
    # Cache the candidate set per (buyer_station_id, range_str) tuple.
    cand_cache: dict[tuple[int, str], list[tuple[int, int]]] = {}

    est_volume: dict[int, dict[int, float]] = {}
    if not agg.unresolved_buys:
        return est_volume

    n = len(agg.unresolved_buys)
    log_every = max(1, n // 20)
    t0 = time.monotonic()
    for i, (tid, bsid, rng_str, vol) in enumerate(agg.unresolved_buys, 1):
        key = (bsid, rng_str)
        cands = cand_cache.get(key)
        if cands is None:
            cands = _candidates_for_scope(bsid, rng_str, resolver, jumps)
            cand_cache[key] = cands
        if not cands:
            continue
        type_liq = agg.sell_liquidity.get(tid, {})
        # Weight = (1/(1+jumps)) * sell_liquidity[type, station]; drop dead candidates.
        weights: list[tuple[int, float]] = []
        total = 0.0
        for stid, d in cands:
            liq = type_liq.get(stid, 0.0)
            if liq <= 0.0:
                continue
            w = liq / (1.0 + float(d))
            weights.append((stid, w))
            total += w
        if total <= 0.0:
            continue
        for stid, w in weights:
            share = w / total
            if share <= 0.0:
                continue
            slot = est_volume.setdefault(stid, {})
            slot[tid] = slot.get(tid, 0.0) + share * vol
        if i % log_every == 0:
            logger.info("precompute: attribution %d/%d (%.1f%%)",
                        i, n, 100.0 * i / n)
    logger.info("precompute: attribution done in %.2fs (cache size=%d)",
                time.monotonic() - t0, len(cand_cache))
    return est_volume


def _write_attributed_stats(
    data_dir: Path,
    agg: _Aggregator,
    estimated: dict[int, dict[int, float]],
) -> int:
    """For every NPC station that has either strict orders or attributed buys,
    write stats_attr/{station_id}.json mirroring compute_live_stats_with_attribution.
    """
    written = 0
    stations = (
        set(agg.station_buy) | set(agg.station_sell) | set(estimated)
    )
    for stid in sorted(stations):
        buy_strict = agg.station_buy.get(stid, {})
        sell_strict = agg.station_sell.get(stid, {})
        est = estimated.get(stid, {})
        types = sorted(set(buy_strict) | set(sell_strict) | set(est))
        if not types:
            continue
        payload: dict[str, dict] = {}
        for tid in types:
            buy_rows = buy_strict.get(tid, [])
            sell_rows = sell_strict.get(tid, [])
            est_vol = float(est.get(tid, 0.0))
            buy = _weighted_stats_buy_side(buy_rows)
            sell = _weighted_stats(sell_rows)
            try:
                exact_volume = float(buy["volume"])
            except (TypeError, ValueError, KeyError):
                exact_volume = 0.0
            try:
                exact_orders = int(buy["orderCount"])
            except (TypeError, ValueError, KeyError):
                exact_orders = 0
            buy["exact_volume"] = f"{exact_volume:.1f}"
            buy["estimated_volume"] = f"{est_vol:.1f}"
            buy["volume"] = f"{exact_volume + est_vol:.1f}"
            # We don't track per-buy order counts in attribution (a single
            # source order is split across many candidates); report 0.
            buy["estimated_order_count"] = "0"
            buy["orderCount"] = str(exact_orders)
            payload[str(tid)] = {
                "buy": buy,
                "sell": sell,
                "attribution": "jump_weighted",
            }
        _atomic_write_json(stats_attr_file(data_dir, stid), payload)
        written += 1
    return written


# ---------------------------------------------------------------------------
# History precompute (from inferred trades)
# ---------------------------------------------------------------------------
# Bucket type:  scope_id -> type_id -> (buy_rows, sell_rows)
# Each "row" is (price, volume).
_BucketSide = list[tuple[float, float]]
_TypeBucket = dict[int, tuple[_BucketSide, _BucketSide]]
_ScopeBucket = dict[int, _TypeBucket]


def _new_type_bucket() -> _TypeBucket:
    return {}


def _bucket_add(bucket: _ScopeBucket, scope_id: int, tid: int,
                price: float, volume: float, is_buy: bool) -> None:
    types = bucket.setdefault(scope_id, {})
    pair = types.get(tid)
    if pair is None:
        pair = ([], [])
        types[tid] = pair
    (pair[0] if is_buy else pair[1]).append((price, volume))


def _scope_payload(types: _TypeBucket) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for tid in sorted(types):
        buy_rows, sell_rows = types[tid]
        out[str(tid)] = {
            "buy": _weighted_stats_buy_side(buy_rows),
            "sell": _weighted_stats(sell_rows),
        }
    return out


def _precompute_history(data_dir: Path) -> tuple[int, int]:
    """Aggregate inferred trades into per-(region|station, range) stat files.

    Returns ``(region_files_written, station_files_written)``.

    For each fixed range in ``HISTORY_RANGES`` we build two scope buckets:
      * region scope keyed on ``trade.region_id`` (always present)
      * station scope keyed on ``trade.location_id`` (only NPC stations;
        wider-range buys with ``location_id is None`` are skipped here)
    Every trade is added to all matching ranges in a single pass.
    """
    if not HISTORY_RANGES:
        return (0, 0)
    valid_labels = {label for label, _ in HISTORY_RANGES}
    # Remove any stale files from previous range configurations.
    for d in (_history_dir(data_dir), _history_station_dir(data_dir)):
        for entry in d.iterdir():
            if not entry.is_file() or not entry.name.endswith(".json"):
                continue
            stem = entry.name[:-len(".json")]
            _, _, label = stem.rpartition("_")
            if label and label not in valid_labels:
                try:
                    entry.unlink()
                except OSError:
                    pass
    max_window = max(secs for _, secs in HISTORY_RANGES)
    now = int(time.time())
    cutoff = now - max_window

    inferred_unix = [u for u in _list_inferred_unix(data_dir) if u >= cutoff]
    if not inferred_unix:
        return (0, 0)

    # Per-range buckets.
    region_buckets: dict[str, _ScopeBucket] = {label: {} for label, _ in HISTORY_RANGES}
    station_buckets: dict[str, _ScopeBucket] = {label: {} for label, _ in HISTORY_RANGES}

    n_trades = 0
    n_files = 0
    t0 = time.monotonic()
    for unix in inferred_unix:
        # Which ranges include this file?
        in_ranges = [label for label, secs in HISTORY_RANGES if unix >= now - secs]
        if not in_ranges:
            continue
        path = _inferred_jsonl_path(data_dir, unix)
        try:
            with open_jsonl(path) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    try:
                        tid = int(row.get("type_id"))
                        price = float(row.get("price", 0))
                        volume = float(row.get("volume", 0))
                    except (TypeError, ValueError):
                        continue
                    if volume <= 0 or price <= 0:
                        continue
                    rid = row.get("region_id")
                    sid = row.get("location_id")
                    is_buy = bool(row.get("is_buy_order"))
                    try:
                        rid_i = int(rid) if rid is not None else None
                    except (TypeError, ValueError):
                        rid_i = None
                    try:
                        sid_i = int(sid) if sid is not None else None
                    except (TypeError, ValueError):
                        sid_i = None
                    is_npc_station = (
                        sid_i is not None and 60_000_000 <= sid_i <= 63_999_999
                    )
                    for label in in_ranges:
                        if rid_i is not None:
                            _bucket_add(region_buckets[label], rid_i, tid,
                                        price, volume, is_buy)
                        if is_npc_station:
                            _bucket_add(station_buckets[label], sid_i, tid,
                                        price, volume, is_buy)
                    n_trades += 1
        except FileNotFoundError:
            continue
        n_files += 1

    region_written = 0
    station_written = 0
    for label, _ in HISTORY_RANGES:
        for rid, types in region_buckets[label].items():
            payload = _scope_payload(types)
            if payload:
                _atomic_write_json(history_file(data_dir, rid, label), payload)
                region_written += 1
        for sid, types in station_buckets[label].items():
            payload = _scope_payload(types)
            if payload:
                _atomic_write_json(history_station_file(data_dir, sid, label), payload)
                station_written += 1

    logger.info(
        "precompute history: %s trades from %s inferred files in %.2fs "
        "-> %s region files, %s station files (ranges=%s)",
        f"{n_trades:,}", n_files, time.monotonic() - t0,
        region_written, station_written,
        ",".join(label for label, _ in HISTORY_RANGES),
    )
    return (region_written, station_written)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------
def needs_precompute(data_dir: Path, snapshot_unix: int) -> bool:
    """True if no precomputed meta exists or it's stale relative to ``snapshot_unix``."""
    meta = read_meta(data_dir)
    if not meta:
        return True
    try:
        return int(meta.get("snapshot_unix", 0)) != int(snapshot_unix)
    except (TypeError, ValueError):
        return True


def run_precompute(
    sde_dir: Path,
    data_dir: Path,
    *,
    snapshot_unix: Optional[int] = None,
    resolver: Optional[LocationResolver] = None,
    jumps: Optional[JumpGraph] = None,
    client: Optional[EsiClient] = None,
    do_history: bool = True,
) -> dict:
    """Generate all precomputed datasets for the latest (or given) snapshot."""
    sde_dir = Path(sde_dir)
    data_dir = Path(data_dir)
    if snapshot_unix is None:
        snapshot_unix = latest_snapshot(data_dir)
    if snapshot_unix is None:
        logger.warning("precompute: no snapshots on disk, skipping")
        return {"skipped": True, "reason": "no_snapshots"}
    snap_path = orders_path(data_dir, snapshot_unix)
    if not snap_path.exists():
        logger.warning("precompute: snapshot file missing: %s", snap_path)
        return {"skipped": True, "reason": "snapshot_missing"}

    resolver = resolver or LocationResolver(sde_dir)
    jumps = jumps or JumpGraph(sde_dir)

    t_total = time.monotonic()
    logger.info("precompute: starting for snapshot_unix=%s", snapshot_unix)
    agg = _scan_snapshot(snap_path)

    n_strict = _write_strict_stats(data_dir, agg)
    logger.info("precompute: wrote %s strict stats files", n_strict)

    estimated = _attribute_unresolved(agg, resolver, jumps)
    n_attr = _write_attributed_stats(data_dir, agg, estimated)
    logger.info("precompute: wrote %s jump-weighted stats files", n_attr)

    n_hist_region = 0
    n_hist_station = 0
    if do_history:
        n_hist_region, n_hist_station = _precompute_history(data_dir)
        logger.info(
            "precompute: wrote %s region history files, %s station history files",
            n_hist_region, n_hist_station,
        )

    elapsed = time.monotonic() - t_total
    meta = {
        "snapshot_unix": int(snapshot_unix),
        "generated_unix": int(time.time()),
        "elapsed_s": round(elapsed, 2),
        "strict_files": n_strict,
        "attr_files": n_attr,
        "history_region_files": n_hist_region,
        "history_station_files": n_hist_station,
        "ranges": [label for label, _ in HISTORY_RANGES],
        "type_count": len(agg.all_types),
    }
    _atomic_write_json(meta_file(data_dir), meta)
    logger.info("precompute: complete in %.2fs (%s)", elapsed, meta)
    return meta
