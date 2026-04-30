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


def sweep_old_data(
    data_dir: Path,
    *,
    keep_orders: int = 2,
    keep_inferred: int = 1,
    keep_contracts: int = 1,
) -> dict[str, int]:
    """Gzip historical order/inferred/contracts files, leaving the newest few alone.

    Returns a small report dict with counts and bytes saved. Safe to call
    repeatedly; already-compressed files are skipped.
    """
    report = {
        "orders_compressed": 0,
        "inferred_compressed": 0,
        "contracts_compressed": 0,
        "bytes_before": 0,
        "bytes_after": 0,
        "elapsed_s": 0,
    }
    t0 = time.monotonic()

    # --- orders --------------------------------------------------------
    snaps = list_snapshots(data_dir)
    if len(snaps) > keep_orders:
        old = snaps[: len(snaps) - keep_orders]
        for unix in old:
            src = orders_path(data_dir, unix)
            if not src.exists():
                continue  # already gzipped
            try:
                size_before = src.stat().st_size
                dst = _gzip_in_place(src)
                size_after = dst.stat().st_size
                report["orders_compressed"] += 1
                report["bytes_before"] += size_before
                report["bytes_after"] += size_after
                logger.info(
                    "compressed orders-%s.jsonl: %.1f MiB -> %.1f MiB (%.1fx)",
                    unix,
                    size_before / 1024 / 1024,
                    size_after / 1024 / 1024,
                    size_before / max(size_after, 1),
                )
            except Exception:
                logger.exception("failed to gzip %s", src)

    # --- inferred ------------------------------------------------------
    inf_unix = _list_inferred_unix(data_dir)
    if len(inf_unix) > keep_inferred:
        old = inf_unix[: len(inf_unix) - keep_inferred]
        for unix in old:
            src = _inferred_jsonl_path(data_dir, unix)
            if not src.exists():
                continue  # already gzipped (or never written)
            try:
                size_before = src.stat().st_size
                dst = _gzip_in_place(src)
                size_after = dst.stat().st_size
                report["inferred_compressed"] += 1
                report["bytes_before"] += size_before
                report["bytes_after"] += size_after
                logger.info(
                    "compressed infer-%s.jsonl: %.1f MiB -> %.1f MiB (%.1fx)",
                    unix,
                    size_before / 1024 / 1024,
                    size_after / 1024 / 1024,
                    size_before / max(size_after, 1),
                )
            except Exception:
                logger.exception("failed to gzip %s", src)

    # --- contracts snapshots ----------------------------------------
    contract_unix = list_contracts_snapshots(data_dir)
    if len(contract_unix) > keep_contracts:
        old = contract_unix[: len(contract_unix) - keep_contracts]
        for unix in old:
            src = contracts_path(data_dir, unix)
            if not src.exists():
                continue  # already gzipped
            try:
                size_before = src.stat().st_size
                dst = _gzip_in_place(src)
                size_after = dst.stat().st_size
                report["contracts_compressed"] += 1
                report["bytes_before"] += size_before
                report["bytes_after"] += size_after
                logger.info(
                    "compressed contracts-%s.jsonl: %.1f MiB -> %.1f MiB (%.1fx)",
                    unix,
                    size_before / 1024 / 1024,
                    size_after / 1024 / 1024,
                    size_before / max(size_after, 1),
                )
            except Exception:
                logger.exception("failed to gzip %s", src)

    report["elapsed_s"] = round(time.monotonic() - t0, 2)
    if report["orders_compressed"] or report["inferred_compressed"] or report["contracts_compressed"]:
        saved = report["bytes_before"] - report["bytes_after"]
        logger.info(
            "sweep: compressed %s orders + %s inferred + %s contracts in %ss, saved %.1f MiB",
            report["orders_compressed"],
            report["inferred_compressed"],
            report["contracts_compressed"],
            report["elapsed_s"],
            saved / 1024 / 1024,
        )
    return report
