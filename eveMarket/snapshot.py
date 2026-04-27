"""Path conventions for the eveMarket data directory.

Layout under ``data_dir`` (default: ./eveMarket/data):
    orders/orders-<unix>.jsonl     - one snapshot per collection trigger
    inferred/<unix>.jsonl          - inferred trades produced from snapshot pair
    history/region-<id>.json       - cached ESI /markets/{region}/history/
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

_ORDERS_RE = re.compile(r"^orders-(\d+)\.jsonl(?:\.gz)?$")


def orders_dir(data_dir: Path) -> Path:
    p = Path(data_dir) / "orders"
    p.mkdir(parents=True, exist_ok=True)
    return p


def inferred_dir(data_dir: Path) -> Path:
    p = Path(data_dir) / "inferred"
    p.mkdir(parents=True, exist_ok=True)
    return p


def history_dir(data_dir: Path) -> Path:
    p = Path(data_dir) / "history"
    p.mkdir(parents=True, exist_ok=True)
    return p


def orders_path(data_dir: Path, unix_time: int) -> Path:
    return orders_dir(data_dir) / f"orders-{int(unix_time)}.jsonl"


def list_snapshots(data_dir: Path) -> list[int]:
    """Return sorted list of snapshot unix-times present on disk."""
    out: list[int] = []
    d = orders_dir(data_dir)
    for entry in d.iterdir():
        if not entry.is_file():
            continue
        m = _ORDERS_RE.match(entry.name)
        if m:
            try:
                out.append(int(m.group(1)))
            except ValueError:
                pass
    out.sort()
    return out


def latest_snapshot(data_dir: Path) -> Optional[int]:
    snaps = list_snapshots(data_dir)
    return snaps[-1] if snaps else None


def find_nearest_snapshot(data_dir: Path, target_unix: int) -> Optional[int]:
    """Return the snapshot unix-time closest to ``target_unix`` (any direction)."""
    snaps = list_snapshots(data_dir)
    if not snaps:
        return None
    return min(snaps, key=lambda u: abs(u - int(target_unix)))


def previous_snapshot(data_dir: Path, before_unix: int) -> Optional[int]:
    snaps = [s for s in list_snapshots(data_dir) if s < int(before_unix)]
    return snaps[-1] if snaps else None
