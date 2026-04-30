"""Universe-wide market order collector (incremental).

Walks every NPC region's ``GET /markets/{region_id}/orders/`` endpoint and
maintains a per-region cache on disk so subsequent ticks only re-pull the
regions whose order book has actually changed (driven by ESI's
``Last-Modified`` / ``If-Modified-Since`` semantics, ~5-minute cache).

Storage layout under ``data_dir``:

    orders/state/_meta.json           - per-region Last-Modified + counts
    orders/state/region-<id>.jsonl    - cached raw rows for that region
    orders/orders-<unix>.jsonl        - aggregate snapshot for the rest
                                        of the pipeline (inference, etc.)

Per-region request flow:
    1. GET page 1 with ``If-Modified-Since: <stored Last-Modified>``.
    2. ``304 Not Modified`` -> nothing changed; reuse the cached jsonl rows.
    3. ``200 OK``           -> read ``Last-Modified`` + ``X-Pages``, fetch
                               pages 2..N, atomically replace
                               ``region-<id>.jsonl``, update meta.
    4. ``204`` / error      -> drop the cached rows for that region.

After all regions are visited the cached per-region rows are concatenated
into ``orders-<unix>.jsonl`` so downstream consumers (inferred trades,
precompute) keep using a single snapshot file.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from email.utils import format_datetime, parsedate_to_datetime
from datetime import timezone
from pathlib import Path
from typing import Iterator, Optional

from ._term import rprint
from .esi import EsiClient
from .snapshot import orders_dir, orders_path

logger = logging.getLogger(__name__)

ESI_BASE = "https://esi.evetech.net/latest"

STATE_SCHEMA_VERSION = "v1"


def _rprint(msg: str, *, end: bool = False, client: Optional[EsiClient] = None) -> None:
    rprint("eveMarket.collector", msg, end=end, client=client)


def iter_market_region_ids(sde_dir: Path) -> list[int]:
    """Return all known-space region IDs (k-space + Pochven) from SDE."""
    regions_path = Path(sde_dir) / "mapRegions.jsonl"
    out: list[int] = []
    with regions_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            rid = row.get("_key") or row.get("regionID")
            if rid is None:
                continue
            rid = int(rid)
            if rid < 11000000:
                out.append(rid)
    out.sort()
    return out


# --- per-region cache layout --------------------------------------------

def _state_dir(data_dir: Path) -> Path:
    p = orders_dir(data_dir) / "state"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _meta_path(data_dir: Path) -> Path:
    return _state_dir(data_dir) / "_meta.json"


def _region_jsonl_path(data_dir: Path, region_id: int) -> Path:
    return _state_dir(data_dir) / f"region-{int(region_id)}.jsonl"


def _load_meta(data_dir: Path) -> dict:
    path = _meta_path(data_dir)
    if not path.exists():
        return {"schema": STATE_SCHEMA_VERSION, "regions": {}}
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        logger.warning("orders meta corrupt -- starting fresh")
        return {"schema": STATE_SCHEMA_VERSION, "regions": {}}
    if data.get("schema") != STATE_SCHEMA_VERSION:
        return {"schema": STATE_SCHEMA_VERSION, "regions": {}}
    if not isinstance(data.get("regions"), dict):
        data["regions"] = {}
    return data


def _save_meta(data_dir: Path, meta: dict) -> None:
    path = _meta_path(data_dir)
    tmp = path.with_suffix(".json.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(meta, f, separators=(",", ":"))
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _drop_region_cache(data_dir: Path, region_id: int, meta: dict) -> None:
    path = _region_jsonl_path(data_dir, region_id)
    try:
        if path.exists():
            path.unlink()
    except OSError:
        logger.warning("could not unlink stale region cache %s", path)
    meta["regions"].pop(str(region_id), None)


def _write_region_cache(
    data_dir: Path,
    region_id: int,
    rows: list[dict],
    last_modified: Optional[str],
    meta: dict,
) -> int:
    """Atomically replace ``region-<id>.jsonl`` and update ``meta``."""
    path = _region_jsonl_path(data_dir, region_id)
    tmp = path.with_suffix(".jsonl.tmp")
    n = 0
    with tmp.open("w", encoding="utf-8") as f:
        for o in rows:
            o["region_id"] = region_id
            f.write(json.dumps(o, separators=(",", ":")))
            f.write("\n")
            n += 1
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    meta["regions"][str(region_id)] = {
        "last_modified": last_modified or "",
        "fetched_at": int(time.time()),
        "row_count": n,
    }
    return n


def _iter_region_jsonl(path: Path) -> Iterator[str]:
    """Yield raw lines (already JSON-encoded) from a cached region file."""
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as f:
        for raw in f:
            if raw.strip():
                yield raw.rstrip("\n")


def _normalize_last_modified(value: Optional[str]) -> Optional[str]:
    """Round-trip through email.utils so the header is RFC 7231 conformant."""
    if not value:
        return None
    try:
        dt = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return format_datetime(dt.astimezone(timezone.utc), usegmt=True)


def _meta_max_lm_unix(meta: dict) -> Optional[float]:
    """Return the newest Last-Modified unix time across all cached regions.

    Used by the scheduler to derive the next collection time as
    ``max_lm + interval`` rather than ``time.time() + interval``, so we
    fire exactly when ESI's cache window expires instead of drifting.
    """
    best: Optional[float] = None
    for info in meta.get("regions", {}).values():
        lm = info.get("last_modified") or ""
        if not lm:
            continue
        try:
            dt = parsedate_to_datetime(lm)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            ts = dt.timestamp()
            if best is None or ts > best:
                best = ts
        except (TypeError, ValueError):
            pass
    return best


# --- collection ---------------------------------------------------------

def collect_snapshot(
    sde_dir: Path,
    data_dir: Path,
    *,
    trigger_unix: Optional[int] = None,
    client: Optional[EsiClient] = None,
    stop_event: Optional[threading.Event] = None,
) -> tuple[Path, int, int, Optional[float]]:
    """Refresh per-region order-book cache and emit an aggregate snapshot.

    Returns ``(out_path, orders_total, trigger_unix, esi_last_modified_unix)``.
    ``esi_last_modified_unix`` is the newest ``Last-Modified`` unix time seen
    across all cached regions (``None`` if no region returned that header).
    The scheduler adds ``interval_s`` to this value to derive the next tick
    time so collections fire exactly when ESI's cache window expires.
    """
    sde_dir = Path(sde_dir)
    data_dir = Path(data_dir)
    if trigger_unix is None:
        trigger_unix = int(time.time())
    out_path = orders_path(data_dir, trigger_unix)
    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")

    if client is None:
        client = EsiClient()

    region_ids = iter_market_region_ids(sde_dir)
    if not region_ids:
        raise RuntimeError(f"No regions found in {sde_dir / 'mapRegions.jsonl'}")

    meta = _load_meta(data_dir)
    region_meta: dict = meta["regions"]

    t0 = time.monotonic()
    refreshed = 0
    not_modified = 0
    failed = 0
    n_regions = len(region_ids)
    logger.info("collect: %s regions, trigger_unix=%s -> %s (incremental)",
                n_regions, trigger_unix, out_path)

    # Phase A: visit every region's page 1 with If-Modified-Since.
    pages_per_region: dict[int, int] = {}
    last_modified_per_region: dict[int, Optional[str]] = {}
    new_page1_rows: dict[int, list[dict]] = {}
    for idx, region_id in enumerate(region_ids, start=1):
        if stop_event is not None and stop_event.is_set():
            raise InterruptedError("collect stopped")
        prev = region_meta.get(str(region_id)) or {}
        ims = prev.get("last_modified") or ""
        url = f"{ESI_BASE}/markets/{region_id}/orders/"
        params = {"order_type": "all", "datasource": "tranquility", "page": 1}
        headers = {"If-Modified-Since": ims} if ims else None
        _rprint(
            f"[orders] {idx}/{n_regions} region {region_id} "
            f"(refreshed={refreshed} 304={not_modified} fail={failed})",
            client=client,
        )
        try:
            resp = client.get(url, params=params, headers=headers)
        except Exception as exc:
            logger.warning("region %s page 1: %s", region_id, exc)
            failed += 1
            pages_per_region[region_id] = 0
            continue

        if resp.status_code == 304:
            not_modified += 1
            pages_per_region[region_id] = 0
            continue

        if resp.status_code == 204:
            _drop_region_cache(data_dir, region_id, meta)
            refreshed += 1
            pages_per_region[region_id] = 0
            continue

        if not resp.ok:
            logger.warning("region %s page 1: HTTP %s", region_id, resp.status_code)
            failed += 1
            pages_per_region[region_id] = 0
            continue

        try:
            page1 = resp.json()
        except ValueError:
            logger.warning("region %s page 1: bad JSON", region_id)
            failed += 1
            pages_per_region[region_id] = 0
            continue
        if not isinstance(page1, list):
            failed += 1
            pages_per_region[region_id] = 0
            continue

        try:
            total_pages = int(resp.headers.get("X-Pages", 1))
        except (TypeError, ValueError):
            total_pages = 1

        last_modified_per_region[region_id] = _normalize_last_modified(
            resp.headers.get("Last-Modified")
        )
        new_page1_rows[region_id] = page1
        pages_per_region[region_id] = total_pages
    _rprint(
        f"[orders] page-1 sweep done: refreshed={refreshed} 304={not_modified} fail={failed}",
        end=True, client=client,
    )

    # Phase B: pages 2..N for regions that changed and have multiple pages.
    pages_total_b = sum(max(0, p - 1) for p in pages_per_region.values())
    pages_done_b = 0
    t_b = time.monotonic()
    for region_id, total_pages in pages_per_region.items():
        if total_pages <= 1:
            continue
        url = f"{ESI_BASE}/markets/{region_id}/orders/"
        all_rows = list(new_page1_rows.get(region_id, []))
        broken = False
        for page in range(2, total_pages + 1):
            if stop_event is not None and stop_event.is_set():
                raise InterruptedError("collect stopped")
            elapsed_b = time.monotonic() - t_b
            if pages_done_b > 0:
                eta_s = elapsed_b / pages_done_b * (pages_total_b - pages_done_b)
                eta_m, eta_s2 = divmod(int(eta_s), 60)
                eta_str = f"{eta_m}m {eta_s2:02d}s"
            else:
                eta_str = "--"
            _rprint(
                f"[orders pages] {pages_done_b}/{pages_total_b} "
                f"region={region_id} ETA {eta_str}",
                client=client,
            )
            params = {"order_type": "all", "datasource": "tranquility", "page": page}
            try:
                resp = client.get(url, params=params)
            except Exception as exc:
                logger.warning("region %s page %s: %s", region_id, page, exc)
                broken = True
                pages_done_b += 1
                break
            if not resp.ok:
                if resp.status_code != 204:
                    logger.warning("region %s page %s: HTTP %s",
                                   region_id, page, resp.status_code)
                pages_done_b += 1
                continue
            try:
                rows = resp.json()
            except ValueError:
                logger.warning("region %s page %s: bad JSON", region_id, page)
                pages_done_b += 1
                continue
            if isinstance(rows, list):
                all_rows.extend(rows)
            pages_done_b += 1

        if broken:
            failed += 1
            continue

        n = _write_region_cache(
            data_dir, region_id, all_rows,
            last_modified_per_region.get(region_id),
            meta,
        )
        refreshed += 1
        logger.debug("region %s refreshed: %s rows", region_id, n)
    _rprint(
        f"[orders pages] {pages_done_b}/{pages_total_b} done",
        end=True, client=client,
    )

    # Single-page regions that returned 200 also need to be persisted.
    for region_id, total_pages in pages_per_region.items():
        if total_pages != 1:
            continue
        rows = new_page1_rows.get(region_id)
        if rows is None:
            continue
        n = _write_region_cache(
            data_dir, region_id, rows,
            last_modified_per_region.get(region_id),
            meta,
        )
        refreshed += 1

    _save_meta(data_dir, meta)

    # Concatenate all per-region caches into the aggregate snapshot.
    orders_total = 0
    try:
        with tmp_path.open("w", encoding="utf-8") as fout:
            for region_id in region_ids:
                rpath = _region_jsonl_path(data_dir, region_id)
                for line in _iter_region_jsonl(rpath):
                    fout.write(line)
                    fout.write("\n")
                    orders_total += 1
            fout.flush()
            os.fsync(fout.fileno())
        os.replace(tmp_path, out_path)
    except BaseException:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError:
            pass
        raise

    logger.info(
        "collect complete: refreshed=%s 304=%s failed=%s total=%s -> %s (%.1fs)",
        refreshed, not_modified, failed, orders_total, out_path,
        time.monotonic() - t0,
    )
    return out_path, orders_total, trigger_unix, _meta_max_lm_unix(meta)
