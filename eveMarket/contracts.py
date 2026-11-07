"""Universe-wide public contracts collector (incremental).

Walks every NPC region's ``GET /contracts/public/{region_id}/`` endpoint and
maintains a per-region cache on disk so subsequent ticks only re-pull the
regions whose contracts have actually changed.

Storage layout under ``data_dir``:

    contracts/state/_meta.json          - per-region Last-Modified + counts
    contracts/state/region-<id>.jsonl   - cached raw rows for that region
    contracts/contracts-<unix>.jsonl    - aggregate snapshot for HTTP API

Per-region request flow:
    1. GET page 1 with ``If-Modified-Since: <stored Last-Modified>``.
    2. ``304 Not Modified`` -> nothing changed; reuse the cached jsonl rows.
    3. ``200 OK``           -> read ``Last-Modified`` + ``X-Pages``, fetch
                               pages 2..N, atomically replace
                               ``region-<id>.jsonl``, update meta.
    4. ``204`` / error      -> drop the cached rows for that region (it has
                               no public contracts right now or ESI broke).

After all regions are visited the cached per-region rows are concatenated
into ``contracts-<unix>.jsonl`` so ``iter_courier_contracts`` (and the HTTP
``/contracts/courier`` endpoint) keep using a single snapshot file.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from email.utils import format_datetime, parsedate_to_datetime
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator, Optional

from ._term import rprint
from .collector import iter_market_region_ids
from .esi import EsiClient
from .snapshot import contracts_dir, contracts_path

logger = logging.getLogger(__name__)

ESI_BASE = "https://esi.evetech.net/latest"

STATE_SCHEMA_VERSION = "v1"


def _rprint(msg: str, *, end: bool = False, client: Optional[EsiClient] = None) -> None:
    rprint("eveMarket.contracts", msg, end=end, client=client)


# --- per-region cache layout --------------------------------------------

def _state_dir(data_dir: Path) -> Path:
    p = contracts_dir(data_dir) / "state"
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
        logger.warning("contracts meta corrupt -- starting fresh")
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
        for c in rows:
            c["region_id"] = region_id
            f.write(json.dumps(c, separators=(",", ":")))
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
    """Yield the raw lines (already JSON-encoded) of a cached region file."""
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
    ``max_lm + interval`` rather than ``time.time() + interval``.
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

def collect_contracts(
    sde_dir: Path,
    data_dir: Path,
    *,
    trigger_unix: Optional[int] = None,
    client: Optional[EsiClient] = None,
    stop_event: Optional[threading.Event] = None,
    status=None,
) -> tuple[Path, int, int, Optional[float]]:
    """Refresh per-region public-contracts cache and emit an aggregate snapshot.

    Returns ``(snapshot_path, total_rows, trigger_unix, esi_last_modified_unix)``.
    ``esi_last_modified_unix`` is the newest ``Last-Modified`` unix time seen
    across all cached regions (``None`` if no region returned that header).
    """
    sde_dir = Path(sde_dir)
    data_dir = Path(data_dir)
    if trigger_unix is None:
        trigger_unix = int(time.time())
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
    logger.info("contracts: %s regions, trigger_unix=%s (incremental)",
                n_regions, trigger_unix)

    # Phase A: visit every region's page 1 with If-Modified-Since.
    pages_per_region: dict[int, int] = {}
    last_modified_per_region: dict[int, Optional[str]] = {}
    new_page1_rows: dict[int, list[dict]] = {}
    for idx, region_id in enumerate(region_ids, start=1):
        if stop_event is not None and stop_event.is_set():
            raise InterruptedError("contracts stopped")
        prev = region_meta.get(str(region_id)) or {}
        ims = prev.get("last_modified") or ""
        url = f"{ESI_BASE}/contracts/public/{region_id}/"
        params = {"datasource": "tranquility", "page": 1}
        headers = {"If-Modified-Since": ims} if ims else None
        _rprint(
            f"[contracts] {idx}/{n_regions} region {region_id} "
            f"(refreshed={refreshed} 304={not_modified} fail={failed})",
            client=client,
        )
        if status is not None:
            elapsed_a = time.monotonic() - t0
            eta_a = elapsed_a / idx * (n_regions - idx) if idx > 0 else None
            status.update(phase="page-1 sweep", done=idx, total=n_regions, eta_s=eta_a)
        try:
            resp = client.get(url, params=params, headers=headers)
        except Exception as exc:
            logger.warning("contracts region %s page 1: %s", region_id, exc)
            failed += 1
            pages_per_region[region_id] = 0
            continue

        if resp.status_code == 304:
            not_modified += 1
            pages_per_region[region_id] = 0  # don't fetch more pages
            continue

        if resp.status_code == 204:
            # Region has zero public contracts now -- drop the cached rows.
            _drop_region_cache(data_dir, region_id, meta)
            refreshed += 1
            pages_per_region[region_id] = 0
            continue

        if not resp.ok:
            logger.warning("contracts region %s page 1: HTTP %s",
                           region_id, resp.status_code)
            failed += 1
            pages_per_region[region_id] = 0
            continue

        try:
            page1 = resp.json()
        except ValueError:
            logger.warning("contracts region %s page 1: bad JSON", region_id)
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

    # Phase B: pages 2..N for regions that changed and have multiple pages.
    pages_total_b = sum(max(0, p - 1) for p in pages_per_region.values())
    pages_done_b = 0
    t_b = time.monotonic()
    if status is not None:
        status.update(phase="pages 2…N", done=0, total=pages_total_b, eta_s=None)
    for region_id, total_pages in pages_per_region.items():
        if total_pages <= 1:
            continue
        url = f"{ESI_BASE}/contracts/public/{region_id}/"
        all_rows = list(new_page1_rows.get(region_id, []))
        broken = False
        for page in range(2, total_pages + 1):
            if stop_event is not None and stop_event.is_set():
                raise InterruptedError("contracts stopped")
            elapsed_b = time.monotonic() - t_b
            if pages_done_b > 0:
                eta_s = elapsed_b / pages_done_b * (pages_total_b - pages_done_b)
                eta_m, eta_s2 = divmod(int(eta_s), 60)
                eta_str = f"{eta_m}m {eta_s2:02d}s"
            else:
                eta_s = None
                eta_str = "--"
            if status is not None:
                status.update(phase="pages 2…N", done=pages_done_b, total=pages_total_b, eta_s=eta_s)
            _rprint(
                f"[contracts pages] {pages_done_b}/{pages_total_b} "
                f"region={region_id} ETA {eta_str}",
                client=client,
            )
            params = {"datasource": "tranquility", "page": page}
            try:
                resp = client.get(url, params=params)
            except Exception as exc:
                logger.warning("contracts region %s page %s: %s",
                               region_id, page, exc)
                broken = True
                pages_done_b += 1
                break
            if not resp.ok:
                if resp.status_code != 204:
                    logger.warning("contracts region %s page %s: HTTP %s",
                                   region_id, page, resp.status_code)
                pages_done_b += 1
                continue
            try:
                rows = resp.json()
            except ValueError:
                logger.warning("contracts region %s page %s: bad JSON",
                               region_id, page)
                pages_done_b += 1
                continue
            if isinstance(rows, list):
                all_rows.extend(rows)
            pages_done_b += 1

        if broken:
            failed += 1
            continue

        # Atomic per-region cache replace + meta update.
        n = _write_region_cache(
            data_dir, region_id, all_rows,
            last_modified_per_region.get(region_id),
            meta,
        )
        refreshed += 1
        logger.debug("contracts region %s refreshed: %s rows", region_id, n)

    # Phase B follow-up: single-page regions that returned 200 also need to
    # be persisted (their loop above never ran).
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
        logger.debug("contracts region %s refreshed: %s rows", region_id, n)

    _save_meta(data_dir, meta)

    # Concatenate all per-region caches into the aggregate snapshot.
    out_path = contracts_path(data_dir, trigger_unix)
    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    contracts_total = 0
    try:
        with tmp_path.open("w", encoding="utf-8") as fout:
            for region_id in region_ids:
                rpath = _region_jsonl_path(data_dir, region_id)
                for line in _iter_region_jsonl(rpath):
                    fout.write(line)
                    fout.write("\n")
                    contracts_total += 1
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
        "contracts complete: refreshed=%s 304=%s failed=%s "
        "total=%s -> %s (%.1fs)",
        refreshed, not_modified, failed,
        contracts_total, out_path, time.monotonic() - t0,
    )
    return out_path, contracts_total, trigger_unix, _meta_max_lm_unix(meta)



# --- contract items collection -------------------------------------------

def _items_dir(data_dir: Path) -> Path:
    p = contracts_dir(data_dir) / "items"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _items_jsonl_path(data_dir: Path) -> Path:
    return _items_dir(data_dir) / "items.jsonl"


def _fetched_ids_path(data_dir: Path) -> Path:
    return _items_dir(data_dir) / "_fetched.json"


def _load_fetched_ids(data_dir: Path) -> set:
    path = _fetched_ids_path(data_dir)
    if not path.exists():
        return set()
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return {int(x) for x in data}
    except (OSError, json.JSONDecodeError, ValueError):
        logger.warning("contract items: fetched-ids index unreadable, rebuilding from items.jsonl")
    return _rebuild_fetched_ids(data_dir)


def _rebuild_fetched_ids(data_dir: Path) -> set:
    items_path = _items_jsonl_path(data_dir)
    if not items_path.exists():
        return set()
    out: set = set()
    try:
        with items_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                    cid = row.get("contract_id")
                    if cid is not None:
                        out.add(int(cid))
                except (json.JSONDecodeError, ValueError, TypeError):
                    pass
    except OSError:
        pass
    return out


def _save_fetched_ids(data_dir: Path, fetched: set) -> None:
    path = _fetched_ids_path(data_dir)
    tmp = path.with_suffix(".json.tmp")
    try:
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(sorted(fetched), f, separators=(",", ":"))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except OSError:
        logger.warning("could not save contract-items fetched index")


def _read_non_courier_ids(snapshot_path: Path) -> set:
    """Return the set of all non-courier contract_ids in a snapshot file."""
    ids: set = set()
    try:
        for row in _iter_jsonl(snapshot_path):
            if row.get("type") == "courier":
                continue
            cid = row.get("contract_id")
            if cid is not None:
                ids.add(int(cid))
    except OSError:
        pass
    return ids


class ContractItemsQueue:
    """Thread-safe queue of non-courier contract IDs awaiting items collection.

    The items-collection thread consumes from this queue via ``pop()`` /
    ``done()``.  The contracts-header scheduler updates it via
    ``sync_snapshot()`` after every header collection — whether or not the
    items thread is currently running — so the live queue always reflects the
    current universe of active contracts:

    * IDs that disappeared from the latest snapshot are removed (unless
      currently in-flight, in which case the fetch is allowed to finish).
    * IDs that just appeared (and have not been fetched before) are added.
    """

    __slots__ = ("_data_dir", "_lock", "_pending", "_active")

    def __init__(self, data_dir: Path) -> None:
        self._data_dir = Path(data_dir)
        self._lock = threading.Lock()
        self._pending: set = set()
        self._active: Optional[int] = None  # contract_id currently being fetched

    def sync_snapshot(self, snapshot_path: Path) -> tuple:
        """Diff a new snapshot against the fetched-index and update the queue.

        Returns ``(added, removed)`` counts.
        """
        all_ids = _read_non_courier_ids(snapshot_path)
        fetched = _load_fetched_ids(self._data_dir)
        desired = all_ids - fetched

        with self._lock:
            # Remove IDs no longer active, but skip the one currently in-flight.
            to_remove = self._pending - desired
            if self._active is not None:
                to_remove.discard(self._active)
            self._pending -= to_remove

            # Add IDs that are new and not already queued or in-flight.
            to_add = desired - self._pending
            if self._active is not None:
                to_add.discard(self._active)
            self._pending |= to_add

            return len(to_add), len(to_remove)

    def pop(self) -> Optional[int]:
        """Return the next pending ID and mark it active, or ``None`` if empty."""
        with self._lock:
            if not self._pending:
                return None
            cid = next(iter(self._pending))
            self._pending.discard(cid)
            self._active = cid
            return cid

    def done(self, cid: Optional[int]) -> None:
        """Clear the active state for ``cid`` (call after every pop(), success or not)."""
        with self._lock:
            if self._active == cid:
                self._active = None

    def __len__(self) -> int:
        """Number of IDs pending (not counting the one currently in-flight)."""
        with self._lock:
            return len(self._pending)


def collect_contract_items(
    data_dir: Path,
    queue: ContractItemsQueue,
    *,
    client: Optional[EsiClient] = None,
    stop_event: Optional[threading.Event] = None,
    status=None,
) -> int:
    """Consume ``queue`` and fetch items for each contract ID.

    Runs until the queue is empty.  The queue may grow or shrink concurrently
    as the contracts-header scheduler calls ``queue.sync_snapshot()``; this
    function reacts to those changes naturally because it re-evaluates the
    queue on every iteration.

    Progress is checkpointed to disk every 20 stored contracts so that a
    restart does not re-fetch more than 20 contracts' worth of work.

    Returns the number of newly stored contracts.
    """
    data_dir = Path(data_dir)
    if client is None:
        client = EsiClient()

    fetched = _load_fetched_ids(data_dir)
    items_path = _items_jsonl_path(data_dir)
    fetched_count = 0
    attempts = 0
    now_unix = int(time.time())
    t_items = time.monotonic()

    logger.info("collect_contract_items: starting (queue=%d)", len(queue))

    with items_path.open("a", encoding="utf-8") as fout:
        while True:
            if stop_event is not None and stop_event.is_set():
                raise InterruptedError("contract items stopped")

            contract_id = queue.pop()
            if contract_id is None:
                break

            try:
                url = f"{ESI_BASE}/contracts/public/items/{contract_id}/"
                try:
                    resp = client.get(url, params={"datasource": "tranquility"})
                except Exception as exc:
                    logger.warning("contract items %d: %s", contract_id, exc)
                    continue  # transient error — not stored; retry next sync

                if resp.status_code == 404:
                    items_list: list = []  # accepted/expired — store empty
                elif not resp.ok:
                    logger.warning("contract items %d: HTTP %d", contract_id, resp.status_code)
                    continue  # not stored; retry next sync
                else:
                    try:
                        items_list = resp.json()
                        if not isinstance(items_list, list):
                            items_list = []
                    except ValueError:
                        logger.warning("contract items %d: bad JSON", contract_id)
                        continue

                fout.write(json.dumps(
                    {"contract_id": contract_id, "items": items_list, "fetched_at": now_unix},
                    separators=(",", ":"),
                ))
                fout.write("\n")
                fetched.add(contract_id)
                fetched_count += 1

                if fetched_count % 20 == 0:
                    fout.flush()
                    _save_fetched_ids(data_dir, fetched)

            finally:
                # always clear active state, even on continue / unexpected error
                queue.done(contract_id)
                attempts += 1
                n_remaining = len(queue)
                if attempts % 200 == 0:
                    _rprint(
                        f"[contract items] {attempts} done"
                        f" ({fetched_count} stored, {n_remaining} remaining)",
                        client=client,
                    )
                if status is not None and (attempts % 50 == 0 or attempts <= 5):
                    elapsed_i = time.monotonic() - t_items
                    eta_i = (elapsed_i / attempts * n_remaining) if attempts > 0 else None
                    status.update(
                        phase="items",
                        done=attempts,
                        total=attempts + n_remaining,
                        eta_s=eta_i,
                    )

    _save_fetched_ids(data_dir, fetched)
    _rprint(
        f"[contract items] done: {fetched_count} stored in {attempts} attempts",
        end=True, client=client,
    )
    logger.info("collect_contract_items: %d stored / %d attempts", fetched_count, attempts)
    return fetched_count


# ---------- streaming readers and contract iterators ----------------------

def _iter_jsonl(path: Path) -> Iterator[dict]:
    with path.open("r", encoding="utf-8") as f:
        for raw in f:
            raw = raw.strip()
            if not raw:
                continue
            try:
                yield json.loads(raw)
            except json.JSONDecodeError:
                continue


def _project_contract(row: dict) -> dict:
    """Return a rich projection of any public contract row.

    Fields present for all types: contract_id, region_id, type, issuer_id,
    issuer_corporation_id, for_corporation, start_location_id, price, reward,
    collateral, volume, date_issued, date_expired, title.
    Type-specific: end_location_id + days_to_complete (courier); buyout (auction).
    """
    buyout = row.get("buyout")
    return {
        "contract_id":           row.get("contract_id"),
        "region_id":             row.get("region_id"),
        "type":                  row.get("type"),
        "issuer_id":             row.get("issuer_id"),
        "issuer_corporation_id": row.get("issuer_corporation_id"),
        "for_corporation":       bool(row.get("for_corporation")),
        "start_location_id":     row.get("start_location_id"),
        "end_location_id":       row.get("end_location_id"),
        "price":                 float(row.get("price") or 0.0),
        "reward":                float(row.get("reward") or 0.0),
        "collateral":            float(row.get("collateral") or 0.0),
        "volume":                float(row.get("volume") or 0.0),
        "buyout":                float(buyout) if buyout is not None else None,
        "days_to_complete":      int(row.get("days_to_complete") or 0),
        "date_issued":           row.get("date_issued"),
        "date_expired":          row.get("date_expired"),
        "title":                 row.get("title") or "",
    }


# ---------------------------------------------------------------------------
# In-memory items cache — incrementally loads items.jsonl on demand
# ---------------------------------------------------------------------------

class _ContractItemsCache:
    """Thread-safe, incrementally-loaded cache of ``{contract_id: [items]}``.

    On the first call (or whenever ``items.jsonl`` has grown) the cache reads
    only the *new* bytes appended since its last load, keeping subsequent
    requests O(new lines) instead of O(full file).  The file is opened in
    binary mode so byte-level seeking is exact.  Partial last lines (written
    but not yet flushed, or the result of a mid-write crash) are detected by
    the absence of a trailing ``\\n`` and left for the next refresh cycle.
    """

    __slots__ = ("_data", "_offset", "_lock")

    def __init__(self) -> None:
        self._data: dict[int, list] = {}
        self._offset: int = 0
        self._lock = threading.Lock()

    def get(self, data_dir: Path, contract_ids) -> dict:
        """Return ``{contract_id: items_list}`` for all requested IDs that are cached."""
        ids = {int(c) for c in contract_ids}
        with self._lock:
            self._refresh(_items_jsonl_path(data_dir))
            return {cid: self._data[cid] for cid in ids if cid in self._data}

    def _refresh(self, items_path: Path) -> None:
        try:
            current_size = items_path.stat().st_size
        except OSError:
            return
        if current_size <= self._offset:
            return
        try:
            with items_path.open("rb") as fh:
                fh.seek(self._offset)
                while True:
                    raw = fh.readline()
                    if not raw:
                        break
                    if not raw.endswith(b"\n"):
                        # Partial line at EOF — leave for next cycle.
                        break
                    stripped = raw.strip()
                    if stripped:
                        try:
                            row = json.loads(stripped)
                            cid = row.get("contract_id")
                            items = row.get("items")
                            if cid is not None and items is not None:
                                self._data[int(cid)] = items
                        except (json.JSONDecodeError, ValueError, TypeError):
                            pass
                    self._offset = fh.tell()
        except OSError:
            pass


# Module-level singleton — shared across all requests in the process.
_items_cache = _ContractItemsCache()


def get_contract_items(data_dir: Path, contract_ids) -> dict:
    """Return ``{contract_id: items_list}`` for the given IDs, loading from disk as needed."""
    return _items_cache.get(data_dir, contract_ids)


# ---------------------------------------------------------------------------
# Contract iterators
# ---------------------------------------------------------------------------

def iter_item_exchange_contracts(
    snapshot_path: Path,
    *,
    data_dir: Optional[Path] = None,
    now_iso: Optional[str] = None,
    side: Optional[str] = None,
) -> Iterator[dict]:
    """Yield active item-exchange contracts, enriched with their items list.

    side='sell': exclude pure buy-side contracts (reward > 0 and price == 0)
    side='buy':  only contracts where reward > 0 (WTB — issuer pays ISK for items)
    side=None:   all item_exchange

    Each yielded dict includes an ``"items"`` key (list from
    ``GET /contracts/public/items/{contract_id}``, or ``None`` if not yet
    collected).  Pass ``data_dir`` to enable item enrichment; omit to get
    header-only rows.
    """
    if now_iso is None:
        now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    # Phase 1: collect all matching header rows from snapshot.
    matching: list[dict] = []
    for row in _iter_jsonl(snapshot_path):
        if row.get("type") != "item_exchange":
            continue
        expired = row.get("date_expired") or ""
        if expired and expired <= now_iso:
            continue
        price  = float(row.get("price")  or 0.0)
        reward = float(row.get("reward") or 0.0)
        if side == "sell" and reward > 0 and price == 0:
            continue
        if side == "buy" and reward <= 0:
            continue
        matching.append(row)

    # Phase 2: bulk-load items for all matched contract_ids in one cache pass.
    items_map: dict[int, list] = {}
    if data_dir is not None and matching:
        ids = {int(r["contract_id"]) for r in matching if r.get("contract_id") is not None}
        items_map = get_contract_items(data_dir, ids)

    # Phase 3: yield enriched rows.
    for row in matching:
        out = _project_contract(row)
        cid = row.get("contract_id")
        out["items"] = items_map.get(int(cid)) if cid is not None else None
        yield out


def iter_auction_contracts(
    snapshot_path: Path,
    *,
    data_dir: Optional[Path] = None,
    now_iso: Optional[str] = None,
) -> Iterator[dict]:
    """Yield active auction contracts, enriched with their items list.

    Each yielded dict includes an ``"items"`` key (list from
    ``GET /contracts/public/items/{contract_id}``, or ``None`` if not yet
    collected).  Pass ``data_dir`` to enable item enrichment.
    """
    if now_iso is None:
        now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    matching: list[dict] = []
    for row in _iter_jsonl(snapshot_path):
        if row.get("type") != "auction":
            continue
        expired = row.get("date_expired") or ""
        if expired and expired <= now_iso:
            continue
        matching.append(row)

    items_map: dict[int, list] = {}
    if data_dir is not None and matching:
        ids = {int(r["contract_id"]) for r in matching if r.get("contract_id") is not None}
        items_map = get_contract_items(data_dir, ids)

    for row in matching:
        out = _project_contract(row)
        cid = row.get("contract_id")
        out["items"] = items_map.get(int(cid)) if cid is not None else None
        yield out


def contract_items_stats(data_dir: Path) -> dict:
    """Return summary stats about the contract-items cache."""
    fetched = _load_fetched_ids(data_dir)
    items_path = _items_jsonl_path(data_dir)
    try:
        items_bytes = items_path.stat().st_size
    except OSError:
        items_bytes = 0
    return {
        "fetched_count": len(fetched),
        "items_file_bytes": items_bytes,
    }


def iter_courier_contracts(
    snapshot_path: Path,
    *,
    now_iso: Optional[str] = None,
) -> Iterator[dict]:
    """Yield active courier contracts from a contracts snapshot.

    Each yielded row is a slim projection containing the fields the public
    courier endpoint exposes:
      contract_id, region_id, start_location_id, end_location_id,
      reward, collateral, volume, days_to_complete,
      date_issued, date_expired, title.

    "Active" = not expired (``date_expired > now``). The public ESI endpoint
    only returns outstanding contracts, so no further status check is needed.
    """
    if now_iso is None:
        now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    for row in _iter_jsonl(snapshot_path):
        if row.get("type") != "courier":
            continue
        # Skip rows missing the location pair (defensive — courier rows
        # should always have both, but ESI has surprised us before).
        start_loc = row.get("start_location_id")
        end_loc = row.get("end_location_id")
        if start_loc is None or end_loc is None:
            continue
        # Active filter: date_expired strings are RFC3339 / ISO8601 with 'Z'.
        # Lex compare works because they're zero-padded and same TZ.
        expired = row.get("date_expired") or ""
        if expired and expired <= now_iso:
            continue
        yield {
            "contract_id":      row.get("contract_id"),
            "region_id":        row.get("region_id"),
            "start_location_id": int(start_loc),
            "end_location_id":   int(end_loc),
            "reward":           float(row.get("reward") or 0.0),
            "collateral":       float(row.get("collateral") or 0.0),
            "volume":           float(row.get("volume") or 0.0),
            "days_to_complete": int(row.get("days_to_complete") or 0),
            "date_issued":      row.get("date_issued"),
            "date_expired":     row.get("date_expired"),
            "title":            row.get("title") or "",
        }


# ---------------------------------------------------------------------------
# Item-exchange feed for sibling tools (GET /contracts/itemExchange) and a
# blueprint-copy price summary (GET /contracts/bpc?type_id=)
# ---------------------------------------------------------------------------

_FEED_ITEM_KEYS = ("type_id", "quantity", "is_included", "is_blueprint_copy",
                   "material_efficiency", "time_efficiency", "runs")


def iter_item_exchange_feed(
    snapshot_path: Path,
    *,
    data_dir: Optional[Path] = None,
    region_id: Optional[int] = None,
    type_id: Optional[int] = None,
    now_iso: Optional[str] = None,
) -> Iterator[dict]:
    """Active public item-exchange contracts in the shape eveBuyback and eveInvention read.

    ``{contract_id, region_id, location_id, price, reward, date_issued, date_expired, title, items}`` where
    ``items`` lists ``{type_id, quantity, is_included, is_blueprint_copy, material_efficiency,
    time_efficiency, runs}`` (``null`` while the contract's items haven't been collected yet).
    Optional filters: ``region_id``, and ``type_id`` (contracts containing that type; needs items).
    """
    for row in iter_item_exchange_contracts(snapshot_path, data_dir=data_dir, now_iso=now_iso):
        if region_id is not None and row.get("region_id") != region_id:
            continue
        items = row.get("items")
        feed_items = None
        if items is not None:
            feed_items = [{k: it.get(k) for k in _FEED_ITEM_KEYS} for it in items]
            for it in feed_items:
                it["is_blueprint_copy"] = bool(it["is_blueprint_copy"])
        if type_id is not None and not any((it.get("type_id") == type_id) for it in (feed_items or [])):
            continue
        yield {
            "contract_id": row.get("contract_id"),
            "region_id": row.get("region_id"),
            "location_id": row.get("start_location_id"),
            "price": row.get("price"),
            "reward": row.get("reward"),
            "date_issued": row.get("date_issued"),
            "date_expired": row.get("date_expired"),
            "title": row.get("title"),
            "items": feed_items,
        }


def _percentile(sorted_vals: list, q: float) -> Optional[float]:
    if not sorted_vals:
        return None
    k = (len(sorted_vals) - 1) * q / 100.0
    lo, hi = int(k), min(int(k) + 1, len(sorted_vals) - 1)
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (k - lo)


def bpc_summary(snapshot_path: Path, *, data_dir: Path, type_id: int) -> dict:
    """Per-run prices of public contracts selling exactly one BPC of ``type_id`` and nothing else.

    Raw ``price / runs`` with each contract's ME/TE; the ME0 normalisation (ECONOMICS §5.5 in eveBuyback,
    the eveInvention method) needs material prices and is done by the caller.
    """
    rows = []
    for c in iter_item_exchange_feed(snapshot_path, data_dir=data_dir, type_id=int(type_id)):
        items = [it for it in (c.get("items") or []) if it.get("is_included", True)]
        if len(items) != 1:
            continue
        it = items[0]
        if not it.get("is_blueprint_copy") or it.get("type_id") != int(type_id) or int(it.get("quantity") or 1) != 1:
            continue
        runs = int(it.get("runs") or 0)
        price = float(c.get("price") or 0)
        if runs <= 0 or price <= 0:
            continue
        rows.append({"contract_id": c["contract_id"], "region_id": c["region_id"], "location_id": c["location_id"],
                     "price": price, "runs": runs, "material_efficiency": int(it.get("material_efficiency") or 0),
                     "time_efficiency": int(it.get("time_efficiency") or 0), "per_run": price / runs,
                     "date_issued": c.get("date_issued")})
    per_run = sorted(r["per_run"] for r in rows)
    return {
        "type_id": int(type_id),
        "contracts": len(rows),
        "per_run": {"min": per_run[0] if per_run else None, "p10": _percentile(per_run, 10),
                    "p25": _percentile(per_run, 25), "median": _percentile(per_run, 50)},
        "rows": sorted(rows, key=lambda r: r["per_run"])[:200],
    }
