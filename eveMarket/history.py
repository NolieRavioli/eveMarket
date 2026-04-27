"""Cached ESI /markets/{region_id}/history/ access.

Returns the rolling 5-day mean daily volume per type for use as the
inferred-trade `actual=False` (removed-order) volume threshold.

`HistoryStore` keeps each region's history dict in memory and writes the
``region-<id>.json`` file only when ``flush(region_id)`` is called explicitly
(typically once per region at the end of an inference pass). This avoids
rewriting a multi-megabyte JSON file after every single (region, type)
fetch, which was the dominant cost in pass 2 of inference.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Optional

from ._term import rprint
from .esi import EsiClient
from .snapshot import history_dir

logger = logging.getLogger(__name__)

ESI_BASE = "https://esi.evetech.net/latest"
DEFAULT_REFRESH_S = 12 * 3600


def _history_path(data_dir: Path, region_id: int) -> Path:
    return history_dir(data_dir) / f"region-{int(region_id)}.json"


def load_history(data_dir: Path, region_id: int) -> Optional[dict]:
    """Read the on-disk cache file for one region, if it exists."""
    p = _history_path(data_dir, region_id)
    if not p.exists():
        return None
    try:
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


class HistoryStore:
    """In-memory cache of region history; flushes to disk only on demand."""

    def __init__(
        self,
        data_dir: Path,
        *,
        client: Optional[EsiClient] = None,
        refresh_after_s: float = DEFAULT_REFRESH_S,
    ) -> None:
        self.data_dir = Path(data_dir)
        self.client = client or EsiClient()
        self.refresh_after_s = float(refresh_after_s)
        self._lock = threading.Lock()
        # region_id -> {"types": {tid_str: {"fetched_at": float, "rows": list}}}
        self._regions: dict[int, dict] = {}
        # Track regions that have been mutated since last flush, so callers
        # can skip the disk write for read-only regions.
        self._dirty: set[int] = set()
        self._fetch_count = 0  # for status-line accounting

    def _ensure_loaded(self, region_id: int) -> dict:
        """Load region cache from disk on first touch; return the dict."""
        rid = int(region_id)
        with self._lock:
            cached = self._regions.get(rid)
            if cached is not None:
                return cached
        # Disk read outside the lock to avoid blocking other regions.
        on_disk = load_history(self.data_dir, rid) or {"types": {}}
        if "types" not in on_disk or not isinstance(on_disk.get("types"), dict):
            on_disk["types"] = {}
        with self._lock:
            existing = self._regions.get(rid)
            if existing is not None:
                # Another thread won the race; use their copy.
                return existing
            self._regions[rid] = on_disk
            return on_disk

    def get(
        self,
        region_id: int,
        type_id: int,
        *,
        progress: bool = False,
    ) -> list[dict]:
        """Return daily history rows for (region, type), fetching if stale."""
        rid = int(region_id)
        tid = int(type_id)
        cached = self._ensure_loaded(rid)
        types_map: dict = cached["types"]
        key = str(tid)

        with self._lock:
            entry = types_map.get(key)
            if entry and (time.time() - float(entry.get("fetched_at", 0))) < self.refresh_after_s:
                return list(entry.get("rows", []))

        # Network fetch (no lock — multiple regions/types can fetch in parallel).
        url = f"{ESI_BASE}/markets/{rid}/history/"
        if progress:
            rprint(
                "eveMarket.history",
                f"[history] region={rid} type={tid}. fetched={self._fetch_count + 1}",
                client=self.client,
            )
        resp = self.client.get(url, params={"type_id": tid, "datasource": "tranquility"})
        if not resp.ok:
            logger.warning("history region=%s type=%s HTTP %s", rid, tid, resp.status_code)
            with self._lock:
                entry = types_map.get(key)
            return list(entry.get("rows", [])) if entry else []
        try:
            rows = resp.json()
        except ValueError:
            logger.warning("history region=%s type=%s bad JSON", rid, tid)
            with self._lock:
                entry = types_map.get(key)
            return list(entry.get("rows", [])) if entry else []
        if not isinstance(rows, list):
            rows = []

        with self._lock:
            types_map[key] = {"fetched_at": time.time(), "rows": rows}
            self._dirty.add(rid)
            self._fetch_count += 1
        return rows

    def touched_regions(self) -> set[int]:
        """Return regions that have had at least one fetch since construction."""
        with self._lock:
            return set(self._dirty)

    def flush(self, region_id: int) -> None:
        """Atomically write one region's cache to disk."""
        rid = int(region_id)
        with self._lock:
            cached = self._regions.get(rid)
            if cached is None or rid not in self._dirty:
                return
            payload = json.dumps(cached, separators=(",", ":"))
            self._dirty.discard(rid)
        p = _history_path(self.data_dir, rid)
        tmp = p.with_suffix(p.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            f.write(payload)
        os.replace(tmp, p)

    def flush_all(self) -> int:
        """Flush every dirty region. Returns number of files written."""
        with self._lock:
            dirty = list(self._dirty)
        for rid in dirty:
            self.flush(rid)
        return len(dirty)


# ---------------------------------------------------------------------------
# Backwards-compatible single-shot helper used by callers that don't want to
# manage a HistoryStore lifecycle (e.g. one-off /history queries on the API).
# Still pays the file-rewrite cost — prefer HistoryStore for bulk usage.
# ---------------------------------------------------------------------------
def fetch_history(
    data_dir: Path,
    region_id: int,
    type_id: int,
    *,
    client: Optional[EsiClient] = None,
    refresh_after_s: float = DEFAULT_REFRESH_S,
) -> list[dict]:
    store = HistoryStore(data_dir, client=client, refresh_after_s=refresh_after_s)
    rows = store.get(region_id, type_id)
    store.flush(int(region_id))
    return rows


def rolling_mean_volume(rows: list[dict], days: int = 5) -> float:
    """Mean daily ``volume`` over the last ``days`` history rows."""
    if not rows:
        return 0.0
    tail = rows[-days:]
    if not tail:
        return 0.0
    s = 0.0
    n = 0
    for r in tail:
        v = r.get("volume")
        if v is None:
            continue
        try:
            s += float(v)
            n += 1
        except (TypeError, ValueError):
            continue
    return (s / n) if n else 0.0
