"""Universe-wide public contracts collector.

Mirrors :mod:`collector` but for ``GET /contracts/public/{region_id}``.
Walks every NPC region, paginates via ``X-Pages``, and writes every public
contract to ``contracts/contracts-<unix>.jsonl`` with one JSON object per
line. Each row gets ``region_id`` injected so downstream filtering (e.g. the
``/contracts/courier`` endpoint) doesn't need to re-derive it.

Public contracts are unauthenticated and include:
  - item_exchange / auction / courier
  - only currently-listed (outstanding) contracts; expired/closed ones are
    omitted server-side, so "active" downstream is just
    ``date_expired > now``.

Courier-specific fields ``start_location_id`` and ``end_location_id`` are
already in the response — no enrichment pass needed.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Iterable, Iterator, Optional

from ._term import rprint
from .collector import iter_market_region_ids
from .esi import EsiClient
from .snapshot import contracts_path

logger = logging.getLogger(__name__)

ESI_BASE = "https://esi.evetech.net/latest"


def _rprint(msg: str, *, end: bool = False, client: Optional[EsiClient] = None) -> None:
    rprint("eveMarket.contracts", msg, end=end, client=client)


def _write_rows(fout, rows: list[dict], region_id: int) -> int:
    n = 0
    for c in rows:
        c["region_id"] = region_id
        fout.write(json.dumps(c, separators=(",", ":")))
        fout.write("\n")
        n += 1
    return n


def collect_contracts(
    sde_dir: Path,
    data_dir: Path,
    *,
    trigger_unix: Optional[int] = None,
    client: Optional[EsiClient] = None,
    stop_event: Optional[threading.Event] = None,
) -> tuple[Path, int, int]:
    """Fetch every NPC region's public contracts.

    Writes ``contracts-<trigger_unix>.jsonl`` and returns
    ``(out_path, contracts_total, trigger_unix)``.
    """
    sde_dir = Path(sde_dir)
    data_dir = Path(data_dir)
    if trigger_unix is None:
        trigger_unix = int(time.time())
    out_path = contracts_path(data_dir, trigger_unix)
    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")

    if client is None:
        client = EsiClient()

    region_ids = iter_market_region_ids(sde_dir)
    if not region_ids:
        raise RuntimeError(f"No regions found in {sde_dir / 'mapRegions.jsonl'}")

    t0 = time.monotonic()
    contracts_total = 0
    pages_per_region: dict[int, int] = {}
    logger.info("contracts: %s regions, trigger_unix=%s -> %s",
                len(region_ids), trigger_unix, out_path)

    try:
        with tmp_path.open("w", encoding="utf-8") as fout:
            # Phase A: page 1 of every region (learn X-Pages).
            total_pages_so_far = 0
            for region_id in region_ids:
                if stop_event is not None and stop_event.is_set():
                    raise InterruptedError("contracts stopped")
                _rprint(
                    f"[pages] collecting contract pages for {region_id}. total pages so far: {total_pages_so_far}",
                    client=client,
                )
                url = f"{ESI_BASE}/contracts/public/{region_id}/"
                params = {"datasource": "tranquility", "page": 1}
                resp = client.get(url, params=params)
                if not resp.ok:
                    # 204 No Content is expected for regions with zero public
                    # contracts — quietly drop those without warning spam.
                    if resp.status_code != 204:
                        logger.warning("contracts region %s page 1: HTTP %s",
                                       region_id, resp.status_code)
                    pages_per_region[region_id] = 0
                    continue
                try:
                    page1 = resp.json()
                except ValueError:
                    logger.warning("contracts region %s page 1: bad JSON", region_id)
                    pages_per_region[region_id] = 0
                    continue
                if not isinstance(page1, list):
                    pages_per_region[region_id] = 0
                    continue
                try:
                    total_pages = int(resp.headers.get("X-Pages", 1))
                except (TypeError, ValueError):
                    total_pages = 1
                pages_per_region[region_id] = total_pages
                total_pages_so_far += total_pages
                contracts_total += _write_rows(fout, page1, region_id)
            _rprint(
                f"[pages] collecting contract pages. total pages: {total_pages_so_far}",
                end=True,
                client=client,
            )

            # Phase B: pages 2..N for regions that have more.
            pages_total_b = sum(max(0, p - 1) for p in pages_per_region.values())
            pages_done_b = 0
            t_b = time.monotonic()
            for region_id, total_pages in pages_per_region.items():
                if total_pages <= 1:
                    continue
                url = f"{ESI_BASE}/contracts/public/{region_id}/"
                for page in range(2, total_pages + 1):
                    if stop_event is not None and stop_event.is_set():
                        raise InterruptedError("contracts stopped")
                    elapsed_b = time.monotonic() - t_b
                    if pages_done_b > 0:
                        eta_s = elapsed_b / pages_done_b * (pages_total_b - pages_done_b)
                        eta_m, eta_s2 = divmod(int(eta_s), 60)
                        eta_str = f"{eta_m}m {eta_s2:02d}s"
                    else:
                        eta_str = "--"
                    _rprint(
                        f"[contracts] {pages_done_b}/{pages_total_b}. ETA {eta_str}. total contracts: {contracts_total}",
                        client=client,
                    )
                    params = {"datasource": "tranquility", "page": page}
                    resp = client.get(url, params=params)
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
                        contracts_total += _write_rows(fout, rows, region_id)
                    pages_done_b += 1
            _rprint(
                f"[contracts] {pages_done_b}/{pages_total_b}. ETA 0m 00s. total contracts: {contracts_total}",
                end=True,
                client=client,
            )

            fout.flush()
            os.fsync(fout.fileno())

        os.replace(tmp_path, out_path)
        logger.info("contracts complete: %s contracts -> %s (%.1fs)",
                    contracts_total, out_path, time.monotonic() - t0)
        return out_path, contracts_total, trigger_unix

    except BaseException:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError:
            pass
        raise


# ---------- streaming readers ----------------------------------------------

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
