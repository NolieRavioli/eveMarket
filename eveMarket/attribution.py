"""Jump-weighted attribution for unresolved buy-side trades / order-book.

Default: estimation is opt-in via ``attribution=jump_weighted`` on the
relevant endpoints. This module is otherwise unused.

Weighting:
    base weight = 1 / (1 + jumps(buyer_system, candidate_system))
Optional liquidity bias (on by default):
    weight *= max(epsilon, sell_volume_remain[type, candidate_station])
Then shares are normalised across candidate stations.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable, Iterator, Optional

from .index import iter_snapshot
from .jumps import JumpGraph
from .location import LocationResolver

logger = logging.getLogger(__name__)

# ESI buy-order ranges that map directly to jump counts.
_NUMERIC_RANGES = {"1", "2", "3", "4", "5", "10", "20", "30", "40"}

# Below which a sell-side candidate is treated as effectively absent for the
# liquidity bias. Pure jump-weight is still applied.
_LIQUIDITY_EPSILON = 1e-9


def parse_buyer_range(rng) -> Optional[object]:
    """Normalise an ESI ``range`` value into one of:
    ``"station"`` | ``"solarsystem"`` | ``"constellation"`` | ``"region"`` | int(jumps).
    Returns None for unknown.
    """
    if rng is None:
        return None
    s = str(rng).lower()
    if s in ("station", "solarsystem", "constellation", "region"):
        return s
    if s in _NUMERIC_RANGES:
        return int(s)
    # Some old ESI dumps store ints directly.
    try:
        n = int(s)
        if 0 <= n <= 40:
            return n
    except ValueError:
        pass
    return None


def reachable_systems(
    buyer_station_id: int,
    buyer_range,
    resolver: LocationResolver,
    jumps: JumpGraph,
) -> dict[int, int]:
    """Return ``{system_id: jump_distance}`` of systems where this buy can fill.

    For ``solarsystem`` returns just the buyer's system at distance 0.
    For ``constellation`` / ``region`` returns the membership set with
    distance 0 (no per-system jump distance is meaningful for these scopes,
    and downstream weighting is by jumps).
    """
    bsid = int(buyer_station_id)
    buyer_sys = resolver.system_of_station.get(bsid)
    if buyer_sys is None:
        return {}
    rng = parse_buyer_range(buyer_range)
    if rng is None or rng == "station":
        return {buyer_sys: 0}
    if rng == "solarsystem":
        return {buyer_sys: 0}
    if rng == "constellation":
        # All systems in the buyer's constellation. We don't have system->cons
        # directly, so derive it via the resolver's reverse maps.
        for cid, sysset in resolver.systems_in_constellation.items():
            if buyer_sys in sysset:
                return {s: 0 for s in sysset}
        return {buyer_sys: 0}
    if rng == "region":
        rid = resolver.region_for_system(buyer_sys)
        if rid is None:
            return {buyer_sys: 0}
        return {s: 0 for s in resolver.systems_in_region.get(rid, set())}
    # numeric jumps
    return jumps.bfs_within(buyer_sys, int(rng))


def stations_in_systems(
    system_ids: Iterable[int],
    resolver: LocationResolver,
) -> dict[int, set[int]]:
    """Return ``{system_id: {station_id, ...}}`` for the requested systems."""
    by_system: dict[int, set[int]] = {sid: set() for sid in system_ids}
    for stid, sid in resolver.system_of_station.items():
        if sid in by_system:
            by_system[sid].add(stid)
    return by_system


def build_sell_liquidity_index(
    snapshot_path: Path,
    type_ids: Iterable[int],
) -> dict[int, dict[int, float]]:
    """Single streaming pass: ``{type_id: {station_id: sell_volume_remain}}``.

    Only NPC stations (60M..63.99M) are included. Player structures are
    excluded because we cannot solve buy-fill destination at structures.
    """
    wanted = {int(t) for t in type_ids}
    out: dict[int, dict[int, float]] = {t: {} for t in wanted}
    if not wanted:
        return out
    for order in iter_snapshot(snapshot_path):
        try:
            tid = int(order.get("type_id"))
        except (TypeError, ValueError):
            continue
        if tid not in wanted:
            continue
        if order.get("is_buy_order"):
            continue
        loc = order.get("location_id")
        if loc is None:
            continue
        try:
            loc_i = int(loc)
        except (TypeError, ValueError):
            continue
        if not (60_000_000 <= loc_i <= 63_999_999):
            continue
        try:
            vol = float(order.get("volume_remain", 0))
        except (TypeError, ValueError):
            continue
        if vol <= 0:
            continue
        out[tid][loc_i] = out[tid].get(loc_i, 0.0) + vol
    return out


def share_for_station(
    *,
    type_id: int,
    buyer_station_id: int,
    buyer_range,
    target_station_id: int,
    resolver: LocationResolver,
    jumps: JumpGraph,
    liquidity_index: Optional[dict[int, dict[int, float]]] = None,
) -> float:
    """Return the fraction of this trade attributed to ``target_station_id``.

    0.0 if the target station is outside the buyer's reachable set.
    """
    target_sys = resolver.system_of_station.get(int(target_station_id))
    if target_sys is None:
        return 0.0
    reach = reachable_systems(buyer_station_id, buyer_range, resolver, jumps)
    if target_sys not in reach:
        return 0.0

    # Candidate stations = NPC stations across all reachable systems.
    by_sys = stations_in_systems(reach.keys(), resolver)

    # For numeric ranges we already have jump distances; for named ranges all
    # candidate systems get distance 0, which collapses the formula to pure
    # liquidity weighting (or uniform if liquidity is disabled).
    weights: dict[int, float] = {}
    type_liquidity = (liquidity_index or {}).get(int(type_id), {})
    for sid, dist in reach.items():
        for stid in by_sys.get(sid, ()):
            base = 1.0 / (1.0 + float(dist))
            if liquidity_index is not None:
                liq = type_liquidity.get(stid, 0.0)
                if liq <= _LIQUIDITY_EPSILON:
                    # Drop dead candidates entirely so live ones get a real
                    # share rather than being diluted by zeros.
                    continue
                w = base * liq
            else:
                w = base
            weights[stid] = w

    total = sum(weights.values())
    if total <= 0:
        return 0.0
    return weights.get(int(target_station_id), 0.0) / total


def estimated_inferred_for_station(
    inferred_iter: Iterable[dict],
    *,
    target_station_id: int,
    resolver: LocationResolver,
    jumps: JumpGraph,
    liquidity_index: Optional[dict[int, dict[int, float]]] = None,
) -> Iterator[dict]:
    """Stream inferred trades for a station, including jump-weighted estimates.

    - Trades with concrete ``location_id == target`` pass through.
    - Trades with concrete ``location_id != target`` are dropped.
    - Trades with ``location_id is None`` and a known buyer scope are
      partially attributed to the target station and emitted with
      ``estimated: True`` and ``attribution_share`` annotations.
    """
    target = int(target_station_id)
    target_sys = resolver.system_of_station.get(target)
    if target_sys is None:
        return  # unknown station; nothing to do
    target_region = resolver.region_for_system(target_sys)

    for trade in inferred_iter:
        loc = trade.get("location_id")
        if loc is not None:
            try:
                if int(loc) == target:
                    yield trade
            except (TypeError, ValueError):
                continue
            continue

        bsid = trade.get("buyer_station_id")
        rng = trade.get("buyer_range")
        if bsid is None or rng is None:
            continue
        # Cheap pre-filter: if the trade's region cannot reach our region,
        # skip the BFS. (Region-range buys cross within a region only.)
        rid = trade.get("region_id")
        if target_region is not None and rid is not None and int(rid) != target_region:
            continue

        try:
            tid = int(trade.get("type_id"))
        except (TypeError, ValueError):
            continue
        share = share_for_station(
            type_id=tid,
            buyer_station_id=int(bsid),
            buyer_range=rng,
            target_station_id=target,
            resolver=resolver,
            jumps=jumps,
            liquidity_index=liquidity_index,
        )
        if share <= 0:
            continue
        try:
            vol = float(trade.get("volume", 0)) * share
        except (TypeError, ValueError):
            continue
        if vol <= 0:
            continue
        out = dict(trade)
        out["volume"] = vol
        out["location_id"] = target
        out["estimated"] = True
        out["attribution"] = "jump_weighted"
        out["attribution_share"] = share
        yield out


def estimated_buy_orders_for_station(
    snapshot_path: Path,
    *,
    target_station_id: int,
    type_ids: Iterable[int],
    resolver: LocationResolver,
    jumps: JumpGraph,
    use_liquidity_bias: bool = True,
) -> dict[int, list[tuple[float, float]]]:
    """For a station+types, return per-type list of (price, est_volume) buy
    rows representing fractionally-attributed non-station-range buy orders.

    Only NON-station-range buy orders are processed; station-range buys at
    the target station are returned as-is by the caller's normal pipeline.
    """
    target = int(target_station_id)
    wanted = {int(t) for t in type_ids}
    if not wanted:
        return {}

    liquidity_index: Optional[dict[int, dict[int, float]]] = None
    if use_liquidity_bias:
        liquidity_index = build_sell_liquidity_index(snapshot_path, wanted)

    out: dict[int, list[tuple[float, float]]] = {t: [] for t in wanted}
    for order in iter_snapshot(snapshot_path):
        try:
            tid = int(order.get("type_id"))
        except (TypeError, ValueError):
            continue
        if tid not in wanted:
            continue
        if not order.get("is_buy_order"):
            continue
        rng_raw = order.get("range")
        rng = parse_buyer_range(rng_raw)
        if rng is None or rng == "station":
            # station-range buys are exact; caller handles them already.
            continue
        bsid = order.get("location_id")
        if bsid is None:
            continue
        try:
            bsid_i = int(bsid)
        except (TypeError, ValueError):
            continue
        share = share_for_station(
            type_id=tid,
            buyer_station_id=bsid_i,
            buyer_range=rng_raw,
            target_station_id=target,
            resolver=resolver,
            jumps=jumps,
            liquidity_index=liquidity_index,
        )
        if share <= 0:
            continue
        try:
            price = float(order.get("price", 0))
            vol = float(order.get("volume_remain", 0)) * share
        except (TypeError, ValueError):
            continue
        if vol <= 0:
            continue
        out[tid].append((price, vol))
    return out
