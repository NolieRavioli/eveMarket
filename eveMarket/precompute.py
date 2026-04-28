"""Precompute /stats and /history payloads after every snapshot.

Output layout under ``data/precomputed/``::

    meta.json                                   global state (snapshot_unix, gen_unix, counts)
    stats/{location_id}.json                    strict stats for regions + NPC stations
    history/{region_id}_{range}.json            inferred-trade stats by region
    history_station/{station_id}_{range}.json   inferred-trade stats by NPC station

Endpoints become file-reads with a per-request type_id filter.

Strict mode only: every value reflects orders / inferred trades whose
location is KNOWN exactly. Non-station-range buy orders contribute to their
region only; they are never fractionally attributed to nearby stations.
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
        "all_types",
    )

    def __init__(self) -> None:
        # Per-(location_id, type_id) lists of (price, volume_remain).
        self.region_buy: dict[int, dict[int, list[tuple[float, float]]]] = {}
        self.region_sell: dict[int, dict[int, list[tuple[float, float]]]] = {}
        self.station_buy: dict[int, dict[int, list[tuple[float, float]]]] = {}
        self.station_sell: dict[int, dict[int, list[tuple[float, float]]]] = {}
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
        # All orders contribute to their region (region is always known).
        side_map = self.region_buy if is_buy else self.region_sell
        side_map.setdefault(rid_i, {}).setdefault(tid, []).append((price, vol))

        # --- station side ---
        # Sells are always station-bound. Buys count for the station ONLY if
        # range=="station"; wider-range buys are not attributed to any station.
        if not is_npc_station:
            return
        if not is_buy:
            self.station_sell.setdefault(loc_i, {}).setdefault(tid, []).append((price, vol))
            return
        rng = order.get("range")
        rng_str = str(rng).lower() if rng is not None else "station"
        if rng_str == "station":
            self.station_buy.setdefault(loc_i, {}).setdefault(tid, []).append((price, vol))


def _scan_snapshot(snap_path: Path) -> _Aggregator:
    agg = _Aggregator()
    n = 0
    t0 = time.monotonic()
    for order in iter_snapshot(snap_path):
        agg.add_order(order)
        n += 1
    logger.info(
        "precompute: scanned %s orders in %.2fs (%s types, %s region buckets, "
        "%s station-buy, %s station-sell)",
        f"{n:,}", time.monotonic() - t0, len(agg.all_types),
        len(agg.region_buy), len(agg.station_buy), len(agg.station_sell),
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
# (Jump-weighted attribution removed: strict known-data only.)
# ---------------------------------------------------------------------------


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
    jumps=None,  # accepted for back-compat; ignored (jump-weighted attribution removed)
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

    t_total = time.monotonic()
    logger.info("precompute: starting for snapshot_unix=%s", snapshot_unix)
    agg = _scan_snapshot(snap_path)

    n_strict = _write_strict_stats(data_dir, agg)
    logger.info("precompute: wrote %s strict stats files", n_strict)

    # Clean up any legacy jump-weighted stats from previous versions.
    legacy_attr = precomputed_dir(data_dir) / "stats_attr"
    if legacy_attr.is_dir():
        for entry in list(legacy_attr.iterdir()):
            try:
                entry.unlink()
            except OSError:
                pass
        try:
            legacy_attr.rmdir()
        except OSError:
            pass

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
        "history_region_files": n_hist_region,
        "history_station_files": n_hist_station,
        "ranges": [label for label, _ in HISTORY_RANGES],
        "type_count": len(agg.all_types),
    }
    _atomic_write_json(meta_file(data_dir), meta)
    logger.info("precompute: complete in %.2fs (%s)", elapsed, meta)
    return meta
