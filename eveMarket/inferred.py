"""Inferred trades via snapshot-pair diff.

Algorithm (after EveKit's extract_trades.py):
  Given two market-order snapshots A (older) and B (newer), and the per-region
  rolling-5-day mean daily volume per type from /markets/{region}/history/:

  - For an order present in BOTH A and B with volume_remaining_A > volume_remaining_B:
        emit a trade with actual=True, volume = (A - B), price = order.price.
  - For an order present in A but ABSENT from B with volume_remaining_A > 0
    AND volume_remaining_A <= 0.04 * mean_daily_volume(region, type):
        emit a trade with actual=False (likely traded; could also be cancelled),
        volume = volume_remaining_A.
  - For BUY orders whose ``range`` is not 'station', the matched location is
    unknown to the buyer side, so ``location_id`` is set to None.

Caches per-snapshot to ``data/inferred/<curr_unix>.jsonl`` for re-use.
"""
from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Iterable, Iterator, Optional

from ._term import rprint
from .esi import EsiClient
from .history import HistoryStore, rolling_mean_volume
from .snapshot import inferred_dir

logger = logging.getLogger(__name__)

REMOVED_ORDER_THRESHOLD = 0.04  # 4% of 5-day mean daily volume


def _rprint(msg: str, *, end: bool = False, client: Optional[EsiClient] = None) -> None:
    rprint("eveMarket.inferred", msg, end=end, client=client)


def _index_orders(orders_jsonl: Path) -> dict[int, dict]:
    """Return ``{order_id: order_row}`` for a snapshot file."""
    out: dict[int, dict] = {}
    with Path(orders_jsonl).open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            oid = row.get("order_id")
            if oid is None:
                continue
            out[int(oid)] = row
    return out


def _trade_location(order: dict) -> Optional[int]:
    """Apply the buy-order range rule. Returns None when buyer's range is wider than 'station'."""
    if order.get("is_buy_order"):
        rng = order.get("range")
        if rng is not None and str(rng) != "station":
            return None
    loc = order.get("location_id")
    return None if loc is None else int(loc)


def diff_snapshots(
    prev_jsonl: Path,
    curr_jsonl: Path,
    *,
    data_dir: Path,
    client: Optional[EsiClient] = None,
    history_refresh_s: float = 12 * 3600,
) -> Iterator[dict]:
    """Yield inferred-trade dicts. Lazy: history is fetched only when needed."""
    if client is None:
        client = EsiClient()

    store = HistoryStore(data_dir, client=client, refresh_after_s=history_refresh_s)

    prev = _index_orders(prev_jsonl)
    _rprint(f"[infer] indexed prev snapshot: {len(prev):,} orders", end=True, client=client)
    curr_ids: set[int] = set()
    history_cache: dict[tuple[int, int], float] = {}

    def _mean_for(region_id: int, type_id: int) -> float:
        key = (int(region_id), int(type_id))
        v = history_cache.get(key)
        if v is not None:
            return v
        # Print BEFORE the (potentially slow) network call so the user sees
        # what's happening even on the very first lookup.
        _rprint(
            f"[history] region={region_id} type={type_id}",
            client=client,
        )
        try:
            rows = store.get(int(region_id), int(type_id), progress=False)
        except Exception as exc:
            logger.warning("history fetch failed region=%s type=%s: %s",
                           region_id, type_id, exc)
            rows = []
        v = rolling_mean_volume(rows, days=5)
        history_cache[key] = v
        return v

    try:
        # Pass 1: stream curr, detect partial fills against prev.
        trades_found = 0
        orders_seen = 0
        t0 = time.monotonic()
        with Path(curr_jsonl).open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                oid = row.get("order_id")
                if oid is None:
                    continue
                oid = int(oid)
                curr_ids.add(oid)
                orders_seen += 1
                if orders_seen % 5000 == 0:
                    _rprint(
                        f"[pass1] partial-fill scan. orders: {orders_seen:,}. trades: {trades_found:,}",
                        client=client,
                    )
                prev_row = prev.get(oid)
                if prev_row is None:
                    continue
                try:
                    vp = float(prev_row.get("volume_remain", 0))
                    vc = float(row.get("volume_remain", 0))
                except (TypeError, ValueError):
                    continue
                delta = vp - vc
                if delta > 0:
                    trades_found += 1
                    yield {
                        "actual": True,
                        "order_id": oid,
                        "type_id": row.get("type_id"),
                        "region_id": row.get("region_id"),
                        "is_buy_order": bool(row.get("is_buy_order")),
                        "price": row.get("price"),
                        "volume": delta,
                        "location_id": _trade_location(row),
                    }
        _rprint(
            f"[pass1] partial-fill scan. orders: {orders_seen:,}. trades: {trades_found:,}",
            end=True,
            client=client,
        )

        # Pass 2: removed orders -> probable trades (subject to threshold).
        removed = [(oid, prev_row) for oid, prev_row in prev.items() if oid not in curr_ids]
        total_removed = len(removed)
        t_p2 = time.monotonic()
        for i, (oid, prev_row) in enumerate(removed, 1):
            if i == 1 or i % 100 == 0 or i == total_removed:
                elapsed = time.monotonic() - t_p2
                if i > 0:
                    eta_s = elapsed / i * (total_removed - i)
                    eta_m, eta_s2 = divmod(int(eta_s), 60)
                    eta_str = f"{eta_m}m {eta_s2:02d}s"
                else:
                    eta_str = "--"
                _rprint(
                    f"[pass2] removed-order check. {i}/{total_removed}. ETA {eta_str}. trades: {trades_found:,}",
                    client=client,
                )
            try:
                vp = float(prev_row.get("volume_remain", 0))
            except (TypeError, ValueError):
                continue
            if vp <= 0:
                continue
            rid = prev_row.get("region_id")
            tid = prev_row.get("type_id")
            if rid is None or tid is None:
                continue
            threshold = REMOVED_ORDER_THRESHOLD * _mean_for(int(rid), int(tid))
            if threshold <= 0 or vp > threshold:
                continue
            trades_found += 1
            yield {
                "actual": False,
                "order_id": oid,
                "type_id": tid,
                "region_id": rid,
                "is_buy_order": bool(prev_row.get("is_buy_order")),
                "price": prev_row.get("price"),
                "volume": vp,
                "location_id": _trade_location(prev_row),
            }
        _rprint(
            f"[pass2] removed-order check. {total_removed}/{total_removed}. ETA 0m 00s. trades: {trades_found:,}",
            end=True,
            client=client,
        )
        _rprint(
            f"[infer] complete in {time.monotonic() - t0:.1f}s. {trades_found:,} trades total",
            end=True,
            client=client,
        )
    finally:
        # Persist any newly fetched history rows once per touched region
        # (avoids the multi-MB-per-fetch rewrites that dominated pass 2).
        try:
            n_flushed = store.flush_all()
            if n_flushed:
                logger.info("history: flushed %s region cache file(s)", n_flushed)
        except Exception:
            logger.exception("history flush_all failed")


def cache_path(data_dir: Path, curr_unix: int) -> Path:
    return inferred_dir(data_dir) / f"infer-{int(curr_unix)}.jsonl"


def done_marker(data_dir: Path, curr_unix: int) -> Path:
    return inferred_dir(data_dir) / f"infer-{int(curr_unix)}.done"


def is_complete(data_dir: Path, curr_unix: int) -> bool:
    return done_marker(data_dir, curr_unix).exists()


def write_inferred(
    data_dir: Path,
    curr_unix: int,
    trades: Iterable[dict],
) -> tuple[Path, int]:
    """Write inferred trades progressively so partial results are visible.

    Drops a ``.done`` sentinel beside the file once the iterator is exhausted.
    """
    out = cache_path(data_dir, curr_unix)
    marker = done_marker(data_dir, curr_unix)
    # Remove any stale marker from a previous incomplete attempt.
    try:
        marker.unlink()
    except FileNotFoundError:
        pass
    n = 0
    with out.open("w", encoding="utf-8") as f:
        for t in trades:
            f.write(json.dumps(t, separators=(",", ":")))
            f.write("\n")
            n += 1
            # Flush every 1k rows so /inferred/* sees progress quickly.
            if n % 1000 == 0:
                f.flush()
        f.flush()
        try:
            os.fsync(f.fileno())
        except OSError:
            pass
    marker.write_text(str(int(curr_unix)), encoding="utf-8")
    return out, n


def load_inferred(data_dir: Path, curr_unix: int) -> Optional[list[dict]]:
    p = cache_path(data_dir, curr_unix)
    if not p.exists():
        return None
    out: list[dict] = []
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return out
