"""Cached ESI /markets/{region_id}/history/ access.

Returns the rolling 5-day mean daily volume per type for use as the
inferred-trade `actual=False` (removed-order) volume threshold.
"""
from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Optional

from .esi import EsiClient
from .snapshot import history_dir

logger = logging.getLogger(__name__)

ESI_BASE = "https://esi.evetech.net/latest"
DEFAULT_REFRESH_S = 12 * 3600


def _history_path(data_dir: Path, region_id: int) -> Path:
    return history_dir(data_dir) / f"region-{int(region_id)}.json"


def load_history(data_dir: Path, region_id: int) -> Optional[dict]:
    p = _history_path(data_dir, region_id)
    if not p.exists():
        return None
    try:
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def fetch_history(
    data_dir: Path,
    region_id: int,
    type_id: int,
    *,
    client: Optional[EsiClient] = None,
    refresh_after_s: float = DEFAULT_REFRESH_S,
) -> list[dict]:
    """Fetch (and cache) the daily history for a single (region, type)."""
    if client is None:
        client = EsiClient()

    cached = load_history(data_dir, region_id) or {}
    types_map: dict = cached.setdefault("types", {})
    key = str(int(type_id))
    entry = types_map.get(key)
    now = time.time()
    if entry and (now - float(entry.get("fetched_at", 0))) < refresh_after_s:
        return list(entry.get("rows", []))

    url = f"{ESI_BASE}/markets/{int(region_id)}/history/"
    resp = client.get(url, params={"type_id": int(type_id), "datasource": "tranquility"})
    if not resp.ok:
        logger.warning("history region=%s type=%s HTTP %s",
                       region_id, type_id, resp.status_code)
        return list(entry.get("rows", [])) if entry else []
    try:
        rows = resp.json()
    except ValueError:
        logger.warning("history region=%s type=%s bad JSON", region_id, type_id)
        return list(entry.get("rows", [])) if entry else []
    if not isinstance(rows, list):
        rows = []

    types_map[key] = {"fetched_at": now, "rows": rows}
    cached["types"] = types_map
    p = _history_path(data_dir, region_id)
    tmp = p.with_suffix(p.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(cached, f, separators=(",", ":"))
    os.replace(tmp, p)
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
