"""Order-book fills for real quantities (``POST /quote/{location_id}``).

A quote walks the latest snapshot's book for one location scope:

* ``side="sell"``: *we sell into buy orders*, walking bids from high to low.
* ``side="buy"``: *we buy from sell orders*, walking asks from low to high.

Scope rules (same location ids as ``/stats``):

* region / constellation / system: every order in that scope, both sides.
* NPC station: sell orders located in the station; buy orders located in the
  station **plus** buy orders elsewhere whose ``range`` reaches the station
  (``solarsystem``, ``constellation``, ``region`` or ``N`` jumps).
* structure: orders located in that structure only.

Bids are walked with EVE's minimum-volume rule: an order whose (effective)
``min_volume`` is larger than what is still to be filled is skipped.

Books are built lazily per (snapshot, location) by one streaming pass over the
snapshot (lines are pre-filtered by region before JSON parsing) and kept in a
small LRU, so the first quote after a new snapshot pays the scan and the rest
are dictionary lookups.
"""
from __future__ import annotations

import json
import logging
import threading
from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Optional

from .compression import open_jsonl
from .location import LocationInfo, LocationResolver

logger = logging.getLogger(__name__)

MAX_LINES = 2000          # quote lines per request
MAX_LEVELS = 500          # price levels returned per line when ``levels`` is asked for


@dataclass(frozen=True)
class Order:
    price: float
    volume: int
    min_volume: int
    order_id: int
    location_id: int


@dataclass
class Book:
    """All orders of one location scope in one snapshot, per (type_id, is_buy), best first."""

    snapshot_unix: int
    location_id: int
    sides: dict[tuple[int, bool], list[Order]] = field(default_factory=dict)

    def orders(self, type_id: int, is_buy: bool) -> list[Order]:
        return self.sides.get((int(type_id), bool(is_buy)), [])


@dataclass
class Fill:
    requested: int
    filled: int
    cost: float
    best: Optional[float]
    worst: Optional[float]
    orders_used: int
    depth_total: int
    levels: list[tuple[float, int, int]]

    @property
    def vwap(self) -> Optional[float]:
        return self.cost / self.filled if self.filled else None


def walk(orders: list[Order], qty: int, *, respect_min_volume: bool) -> Fill:
    """Fill ``qty`` units from ``orders`` (already sorted best first).

    ``respect_min_volume`` applies EVE's rule for selling into buy orders: an order
    can't be used when its minimum volume exceeds what's still to be filled.
    """
    qty = int(qty)
    remaining = qty
    cost = 0.0
    used = 0
    best = worst = None
    levels: list[tuple[float, int, int]] = []
    depth = 0
    for o in orders:
        depth += o.volume
    for o in orders:
        if remaining <= 0:
            break
        eff_min = min(o.min_volume, o.volume) if respect_min_volume else 1
        if eff_min > remaining:
            continue
        take = min(o.volume, remaining)
        if take <= 0:
            continue
        cost += take * o.price
        remaining -= take
        used += 1
        best = o.price if best is None else best
        worst = o.price
        if len(levels) < MAX_LEVELS:
            levels.append((o.price, o.volume, o.min_volume))
    return Fill(qty, qty - remaining, cost, best, worst, used, depth, levels)


def _fmt(x: Optional[float]) -> str:
    if x is None:
        return "0"
    return f"{x:.11g}"


def fill_payload(fill: Fill, snapshot_unix: int, *, with_levels: bool = False) -> dict:
    out = {
        "requested": str(fill.requested),
        "filled": str(fill.filled),
        "vwap": _fmt(fill.vwap),
        "best": _fmt(fill.best),
        "worst": _fmt(fill.worst),
        "orders_used": str(fill.orders_used),
        "depth_total": str(fill.depth_total),
        "snapshot_unix": int(snapshot_unix),
    }
    if with_levels:
        out["levels"] = [[_fmt(p), str(v), str(m)] for p, v, m in fill.levels]
    return out


# ---------------------------------------------------------------------------
# Buy-order range coverage (station scope)
# ---------------------------------------------------------------------------

class RangeCoverage:
    """Does a buy order placed at (system, location) with ``range`` reach a given station?"""

    def __init__(self, resolver: LocationResolver, jumps=None) -> None:
        self.resolver = resolver
        self.jumps = jumps
        self._constellation_of_system: Optional[dict[int, int]] = None

    def _constellation(self, system_id: int) -> Optional[int]:
        if self._constellation_of_system is None:
            m: dict[int, int] = {}
            for cid, systems in self.resolver.systems_in_constellation.items():
                for s in systems:
                    m[s] = cid
            self._constellation_of_system = m
        return self._constellation_of_system.get(int(system_id))

    def reaches(self, order_range: str, order_system: Optional[int], order_location: int,
                station_id: int, station_system: Optional[int], jump_dist: Optional[dict[int, int]]) -> bool:
        r = str(order_range or "station").lower()
        if order_location == station_id:
            return True
        if r == "station" or order_system is None or station_system is None:
            return False
        if r == "solarsystem":
            return order_system == station_system
        if r == "constellation":
            c = self._constellation(order_system)
            return c is not None and c == self._constellation(station_system)
        if r == "region":
            return self.resolver.region_for_system(order_system) == self.resolver.region_for_system(station_system)
        if r.isdigit():
            if jump_dist is None:
                return order_system == station_system
            d = jump_dist.get(order_system)
            return d is not None and d <= int(r)
        return False


# ---------------------------------------------------------------------------
# Book index (lazy, LRU per (snapshot, location))
# ---------------------------------------------------------------------------

class BookIndex:
    def __init__(self, resolver: LocationResolver, jumps=None, *, max_books: int = 3) -> None:
        self.resolver = resolver
        self.coverage = RangeCoverage(resolver, jumps)
        self.jumps = jumps
        self.max_books = max(1, int(max_books))
        self._books: "OrderedDict[tuple[int, int], Book]" = OrderedDict()
        self._lock = threading.Lock()
        self._building: dict[tuple[int, int], threading.Event] = {}
        self.builds = 0

    def get(self, snapshot_path: Path, snapshot_unix: int, info: LocationInfo) -> Book:
        key = (int(snapshot_unix), int(info.location_id))
        while True:
            with self._lock:
                book = self._books.get(key)
                if book is not None:
                    self._books.move_to_end(key)
                    return book
                ev = self._building.get(key)
                if ev is None:
                    ev = threading.Event()
                    self._building[key] = ev
                    mine = True
                else:
                    mine = False
            if not mine:
                ev.wait(timeout=300)
                continue
            try:
                book = build_book(snapshot_path, snapshot_unix, info, self.coverage, self.jumps)
                with self._lock:
                    self._books[key] = book
                    self._books.move_to_end(key)
                    while len(self._books) > self.max_books:
                        self._books.popitem(last=False)
                    self.builds += 1
                return book
            finally:
                with self._lock:
                    self._building.pop(key, None)
                ev.set()

    def stats(self) -> dict:
        with self._lock:
            return {"books": [{"snapshot_unix": s, "location_id": loc} for s, loc in self._books],
                    "builds": self.builds}


def _region_needles(info: LocationInfo) -> Optional[list[str]]:
    if not info.region_ids:
        return None
    # Terminated, so 10000002 doesn't also match 10000020. Only a prefilter: the region is checked again.
    return [f'"region_id":{rid}{end}' for rid in sorted(info.region_ids) for end in ("}", ",")]


def build_book(snapshot_path: Path, snapshot_unix: int, info: LocationInfo,
               coverage: RangeCoverage, jumps=None) -> Book:
    """One streaming pass over the snapshot, keeping the orders the scope can trade with."""
    book = Book(int(snapshot_unix), int(info.location_id))
    needles = _region_needles(info)
    station_id = next(iter(info.station_ids)) if info.kind == "station" and info.station_ids else None
    station_system = next(iter(info.system_ids)) if station_id and info.system_ids else None
    jump_dist = jumps.bfs_within(station_system, 40) if (jumps is not None and station_system) else None
    structure_needle = None
    if info.kind == "structure":
        sid = next(iter(info.structure_ids))
        structure_needle = f'"location_id":{sid},'
    sides: dict[tuple[int, bool], list[Order]] = {}
    with open_jsonl(snapshot_path) as f:
        for line in f:
            if structure_needle is not None:
                if structure_needle not in line:
                    continue
            elif needles is not None and not any(n in line for n in needles):
                continue
            try:
                o = json.loads(line)
            except json.JSONDecodeError:
                continue
            try:
                tid = int(o["type_id"])
                is_buy = bool(o.get("is_buy_order"))
                loc = int(o.get("location_id") or 0)
                vol = int(o.get("volume_remain") or 0)
                price = float(o["price"])
            except (KeyError, TypeError, ValueError):
                continue
            if vol <= 0:
                continue
            sys_id = o.get("system_id")
            sys_id = int(sys_id) if sys_id is not None else None
            if info.region_ids and info.kind != "structure" and int(o.get("region_id") or 0) not in info.region_ids:
                continue
            if info.kind == "structure":
                keep = loc in info.structure_ids
            elif info.kind == "station":
                if is_buy:
                    keep = coverage.reaches(o.get("range"), sys_id, loc, station_id, station_system, jump_dist)
                else:
                    keep = loc == station_id
            elif info.kind == "region":
                keep = True
            else:  # constellation / system
                keep = sys_id is not None and sys_id in info.system_ids
            if not keep:
                continue
            sides.setdefault((tid, is_buy), []).append(
                Order(price, vol, int(o.get("min_volume") or 1), int(o.get("order_id") or 0), loc))
    for (tid, is_buy), orders in sides.items():
        orders.sort(key=(lambda x: (-x.price, x.order_id)) if is_buy else (lambda x: (x.price, x.order_id)))
    book.sides = sides
    logger.info("quote book built: snapshot=%s location=%s types=%s orders=%s", snapshot_unix,
                info.location_id, len({t for t, _ in sides}), sum(len(v) for v in sides.values()))
    return book


# ---------------------------------------------------------------------------
# Request handling
# ---------------------------------------------------------------------------

class QuoteError(ValueError):
    pass


def parse_lines(body) -> list[dict]:
    if not isinstance(body, list) or not body:
        raise QuoteError("body must be a non-empty array of {type_id, qty, side}")
    if len(body) > MAX_LINES:
        raise QuoteError(f"at most {MAX_LINES} lines per request")
    out = []
    for item in body:
        if not isinstance(item, dict):
            raise QuoteError("each line must be an object {type_id, qty, side}")
        tid, qty, side = item.get("type_id"), item.get("qty"), item.get("side")
        if isinstance(tid, bool) or not isinstance(tid, int) or tid <= 0:
            raise QuoteError("type_id must be a positive integer")
        if isinstance(qty, bool) or not isinstance(qty, int) or qty <= 0 or qty > 10**13:
            raise QuoteError("qty must be a positive integer")
        if side not in ("sell", "buy"):
            raise QuoteError('side must be "sell" (into buy orders) or "buy" (from sell orders)')
        out.append({"type_id": tid, "qty": qty, "side": side, "levels": bool(item.get("levels"))})
    return out


def quote(book: Book, lines: Iterable[dict]) -> dict:
    out: dict[str, dict] = {}
    for ln in lines:
        is_buy_orders = ln["side"] == "sell"  # selling walks the buy orders
        fill = walk(book.orders(ln["type_id"], is_buy_orders), ln["qty"], respect_min_volume=is_buy_orders)
        out[f'{ln["type_id"]}:{ln["side"]}'] = fill_payload(fill, book.snapshot_unix, with_levels=ln["levels"])
    return out
