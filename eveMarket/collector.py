"""Universe-wide market order collector.

Adapted from eveHauler/market.py. Drops Tk progress reporting; stamps the
snapshot's ``trigger_unix`` BEFORE the fetch begins so all snapshot files are
labeled by trigger time, not completion time.
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
from .snapshot import orders_path

logger = logging.getLogger(__name__)

ESI_BASE = "https://esi.evetech.net/latest"


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


def _write_rows(fout, rows: list[dict], region_id: int) -> int:
    n = 0
    for o in rows:
        o["region_id"] = region_id
        fout.write(json.dumps(o, separators=(",", ":")))
        fout.write("\n")
        n += 1
    return n


def collect_snapshot(
    sde_dir: Path,
    data_dir: Path,
    *,
    trigger_unix: Optional[int] = None,
    client: Optional[EsiClient] = None,
    stop_event: Optional[threading.Event] = None,
) -> tuple[Path, int, int]:
    """Fetch every NPC region's orders and write to ``orders-<trigger_unix>.jsonl``.

    Returns ``(out_path, orders_total, trigger_unix)``.
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

    t0 = time.monotonic()
    orders_total = 0
    pages_per_region: dict[int, int] = {}
    logger.info("collect: %s regions, trigger_unix=%s -> %s",
                len(region_ids), trigger_unix, out_path)

    try:
        with tmp_path.open("w", encoding="utf-8") as fout:
            # Phase A: page 1 of every region (learn X-Pages).
            total_pages_so_far = 0
            for region_id in region_ids:
                if stop_event is not None and stop_event.is_set():
                    raise InterruptedError("collect stopped")
                _rprint(
                    f"[pages] collecting pages for {region_id}. total pages so far: {total_pages_so_far}",
                    client=client,
                )
                url = f"{ESI_BASE}/markets/{region_id}/orders/"
                params = {"order_type": "all", "datasource": "tranquility", "page": 1}
                resp = client.get(url, params=params)
                if not resp.ok:
                    logger.warning("region %s page 1: HTTP %s", region_id, resp.status_code)
                    pages_per_region[region_id] = 0
                    continue
                try:
                    page1 = resp.json()
                except ValueError:
                    logger.warning("region %s page 1: bad JSON", region_id)
                    pages_per_region[region_id] = 0
                    continue
                try:
                    total_pages = int(resp.headers.get("X-Pages", 1))
                except (TypeError, ValueError):
                    total_pages = 1
                pages_per_region[region_id] = total_pages
                total_pages_so_far += total_pages
                orders_total += _write_rows(fout, page1, region_id)
            _rprint(
                f"[pages] collecting pages for {region_id}. total pages so far: {total_pages_so_far}",
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
                url = f"{ESI_BASE}/markets/{region_id}/orders/"
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
                        f"[orders] collecting orders. {pages_done_b}/{pages_total_b}. ETA {eta_str}. total orders: {orders_total}",
                        client=client,
                    )
                    params = {"order_type": "all", "datasource": "tranquility", "page": page}
                    resp = client.get(url, params=params)
                    if not resp.ok:
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
                    orders_total += _write_rows(fout, rows, region_id)
                    pages_done_b += 1
            _rprint(
                f"[orders] collecting orders. {pages_done_b}/{pages_total_b}. ETA 0m 00s. total orders: {orders_total}",
                end=True,
                client=client,
            )

            fout.flush()
            os.fsync(fout.fileno())

        os.replace(tmp_path, out_path)
        logger.info("collect complete: %s orders -> %s (%.1fs)",
                    orders_total, out_path, time.monotonic() - t0)
        return out_path, orders_total, trigger_unix

    except BaseException:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError:
            pass
        raise
