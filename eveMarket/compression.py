"""Transparent gzip support for historical JSONL snapshots.

Strategy
--------
- Latest N order snapshots stay as plain ``.jsonl`` so the live endpoints,
  the inferred-diff step, and precompute can mmap-stream them without paying
  a decompression cost.
- Everything older is rewritten in place as ``<name>.jsonl.gz``.
- ``open_jsonl(path)`` resolves a logical ``foo.jsonl`` path to whichever of
  ``foo.jsonl`` / ``foo.jsonl.gz`` exists and returns a text-mode handle.
  Readers therefore need no other changes.
"""
from __future__ import annotations

import gzip
import logging
import os
import shutil
import tempfile
import time
from pathlib import Path
from typing import IO, Iterable, Optional

from .snapshot import (
    contracts_dir,
    contracts_path,
    inferred_dir,
    list_contracts_snapshots,
    list_snapshots,
    orders_dir,
    orders_path,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Path resolution / open helpers
# ---------------------------------------------------------------------------
def gz_sibling(path: Path) -> Path:
    """Return the ``.gz`` sibling for a ``.jsonl`` path."""
    p = Path(path)
    return p.with_name(p.name + ".gz")


def resolve_jsonl(path: Path) -> Path:
    """Return whichever of ``path`` or ``path + .gz`` exists.

    If neither exists the original ``path`` is returned so callers see a
    natural ``FileNotFoundError`` from the subsequent open.
    """
    p = Path(path)
    if p.exists():
        return p
    gz = gz_sibling(p)
    if gz.exists():
        return gz
    return p


def open_jsonl(path: Path) -> IO[str]:
    """Open a JSONL file in text mode, transparently handling ``.gz``."""
    p = resolve_jsonl(path)
    if p.suffix == ".gz":
        return gzip.open(p, "rt", encoding="utf-8")
    return p.open("r", encoding="utf-8")


# ---------------------------------------------------------------------------
# Sweep: compress everything older than the latest N
# ---------------------------------------------------------------------------
def _gzip_in_place(src: Path) -> Path:
    """Atomically rewrite ``src`` as ``src + .gz`` and unlink the original."""
    dst = gz_sibling(src)
    tmp_fd, tmp_name = tempfile.mkstemp(
        prefix=dst.name + ".", suffix=".tmp", dir=str(dst.parent)
    )
    os.close(tmp_fd)
    tmp_path = Path(tmp_name)
    try:
        with src.open("rb") as fin, gzip.open(tmp_path, "wb", compresslevel=6) as fout:
            shutil.copyfileobj(fin, fout, length=1 << 20)
        os.replace(tmp_path, dst)
    except BaseException:
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass
        raise
    try:
        src.unlink()
    except FileNotFoundError:
        pass
    return dst


def _list_inferred_unix(data_dir: Path) -> list[int]:
    """Return sorted unix-times of every ``infer-<unix>.jsonl[.gz]`` present."""
    out: list[int] = []
    d = inferred_dir(data_dir)
    for entry in d.iterdir():
        if not entry.is_file():
            continue
        name = entry.name
        if not name.startswith("infer-"):
            continue
        if name.endswith(".jsonl"):
            stem = name[len("infer-"):-len(".jsonl")]
        elif name.endswith(".jsonl.gz"):
            stem = name[len("infer-"):-len(".jsonl.gz")]
        else:
            continue
        try:
            out.append(int(stem))
        except ValueError:
            continue
    out.sort()
    return out


def _inferred_jsonl_path(data_dir: Path, unix: int) -> Path:
    return inferred_dir(data_dir) / f"infer-{int(unix)}.jsonl"


def _delete_file(path: Path) -> bool:
    """Delete path, returning True if a file was actually removed."""
    try:
        path.unlink()
        return True
    except FileNotFoundError:
        return False
    except OSError:
        logger.exception("failed to delete %s", path)
        return False


def sweep_old_data(
    data_dir: Path,
    *,
    keep_orders: int = 1,
    keep_inferred: int = 0,
    keep_contracts: int = 1,
) -> dict[str, int]:
    """Delete old order/inferred/contracts files, keeping only the newest few.

    Both plain .jsonl and already-gzipped .jsonl.gz variants are removed so
    that files accumulated before this change are also cleaned up. Disk usage
    is therefore bounded: at most keep_orders complete snapshots on disk at
    any time.

    Returns a small report dict. Safe to call repeatedly.
    """
    report = {
        "orders_deleted": 0,
        "inferred_deleted": 0,
        "contracts_deleted": 0,
        "elapsed_s": 0,
    }
    t0 = time.monotonic()

    # --- orders --------------------------------------------------------
    snaps = list_snapshots(data_dir)
    if len(snaps) > keep_orders:
        old = snaps[: len(snaps) - keep_orders]
        for unix in old:
            src = orders_path(data_dir, unix)
            if _delete_file(src):
                report["orders_deleted"] += 1
                logger.info("deleted orders-%s.jsonl", unix)
            if _delete_file(gz_sibling(src)):
                report["orders_deleted"] += 1
                logger.info("deleted orders-%s.jsonl.gz", unix)

    # --- inferred ------------------------------------------------------
    inf_unix = _list_inferred_unix(data_dir)
    if len(inf_unix) > keep_inferred:
        old = inf_unix[: len(inf_unix) - keep_inferred]
        for unix in old:
            src = _inferred_jsonl_path(data_dir, unix)
            if _delete_file(src):
                report["inferred_deleted"] += 1
                logger.info("deleted infer-%s.jsonl", unix)
            if _delete_file(gz_sibling(src)):
                report["inferred_deleted"] += 1
                logger.info("deleted infer-%s.jsonl.gz", unix)
            _delete_file(inferred_dir(data_dir) / f"infer-{int(unix)}.done")

    # --- contracts snapshots ----------------------------------------
    contract_unix = list_contracts_snapshots(data_dir)
    if len(contract_unix) > keep_contracts:
        old = contract_unix[: len(contract_unix) - keep_contracts]
        for unix in old:
            src = contracts_path(data_dir, unix)
            if _delete_file(src):
                report["contracts_deleted"] += 1
                logger.info("deleted contracts-%s.jsonl", unix)
            if _delete_file(gz_sibling(src)):
                report["contracts_deleted"] += 1
                logger.info("deleted contracts-%s.jsonl.gz", unix)

    report["elapsed_s"] = round(time.monotonic() - t0, 2)
    if report["orders_deleted"] or report["inferred_deleted"] or report["contracts_deleted"]:
        logger.info(
            "sweep: deleted %s orders + %s inferred + %s contracts in %ss",
            report["orders_deleted"],
            report["inferred_deleted"],
            report["contracts_deleted"],
            report["elapsed_s"],
        )
    return report
