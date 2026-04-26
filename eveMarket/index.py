"""Filter snapshot orders by a LocationInfo and stream them out."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, Iterator

from .location import LocationInfo


def iter_snapshot(orders_jsonl: Path) -> Iterator[dict]:
    with Path(orders_jsonl).open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def matches(order: dict, info: LocationInfo) -> bool:
    """True if ``order`` falls within the LocationInfo scope."""
    if info.kind == "unknown":
        return False
    if info.kind == "structure":
        loc = order.get("location_id")
        return loc is not None and int(loc) in info.structure_ids
    if info.kind == "station":
        loc = order.get("location_id")
        return loc is not None and int(loc) in info.station_ids
    # region / constellation / system: filter by region first (cheapest),
    # then by system_id when narrower than a whole region.
    rid = order.get("region_id")
    if info.region_ids and (rid is None or int(rid) not in info.region_ids):
        return False
    if info.kind == "region":
        return True
    sid = order.get("system_id")
    if sid is None:
        return False
    return int(sid) in info.system_ids


def filter_snapshot(orders_jsonl: Path, info: LocationInfo) -> Iterable[dict]:
    for order in iter_snapshot(orders_jsonl):
        if matches(order, info):
            yield order
