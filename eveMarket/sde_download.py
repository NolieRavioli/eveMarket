"""Download and update the EVE Online Static Data Export (SDE).

Uses the official endpoints documented at
https://developers.eveonline.com/static-data/.

Local build tracking: we write the current build number to
``<sde_dir>/_build.txt`` so subsequent runs can skip the download when the
upstream build hasn't changed.
"""
from __future__ import annotations

import json
import logging
import shutil
import tempfile
import urllib.request
import zipfile
from pathlib import Path
from typing import Callable, Optional

logger = logging.getLogger(__name__)

LATEST_URL = "https://developers.eveonline.com/static-data/tranquility/latest.jsonl"
ZIP_URL_TEMPLATE = (
    "https://developers.eveonline.com/static-data/tranquility/"
    "eve-online-static-data-{build}-jsonl.zip"
)
LATEST_ZIP_URL = (
    "https://developers.eveonline.com/static-data/"
    "eve-online-static-data-latest-jsonl.zip"
)
USER_AGENT = "eveMarket/1.0 (+https://github.com/NolieRavioli/eveMarket)"
BUILD_FILE = "_build.txt"


ProgressFn = Callable[[str, float], None]
"""``(message, fraction)`` — fraction in [0,1] or -1 for indeterminate."""


def _request(url: str) -> urllib.request.Request:
    return urllib.request.Request(url, headers={"User-Agent": USER_AGENT})


def fetch_latest_build_number(timeout: float = 15.0) -> int:
    """Return the upstream latest SDE build number."""
    with urllib.request.urlopen(_request(LATEST_URL), timeout=timeout) as resp:
        for raw in resp:
            line = raw.decode("utf-8").strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if obj.get("_key") == "sde":
                val = obj.get("_value", obj)
                if isinstance(val, dict) and "buildNumber" in val:
                    return int(val["buildNumber"])
                if isinstance(val, int):
                    return val
            if "buildNumber" in obj and obj.get("_key") in (None, "sde"):
                return int(obj["buildNumber"])
    raise RuntimeError("latest.jsonl did not contain an 'sde' build number")


def read_local_build(sde_dir: Path) -> Optional[int]:
    """Return the locally recorded build number, or None if unknown."""
    p = sde_dir / BUILD_FILE
    if not p.exists():
        return None
    try:
        return int(p.read_text(encoding="utf-8").strip())
    except (ValueError, OSError):
        return None


def write_local_build(sde_dir: Path, build: int) -> None:
    sde_dir.mkdir(parents=True, exist_ok=True)
    (sde_dir / BUILD_FILE).write_text(str(build), encoding="utf-8")


def download_sde(
    sde_dir: Path,
    build: Optional[int] = None,
    progress: Optional[ProgressFn] = None,
    timeout: float = 60.0,
) -> int:
    """Download the SDE zip and extract its ``.jsonl`` files into ``sde_dir``.

    Returns the build number that was installed. If ``build`` is None, the
    'latest' shorthand URL is used.
    """
    sde_dir.mkdir(parents=True, exist_ok=True)
    url = ZIP_URL_TEMPLATE.format(build=build) if build else LATEST_ZIP_URL
    if progress:
        progress(f"Downloading SDE from {url}", -1.0)
    logger.info("Downloading SDE: %s", url)

    with tempfile.NamedTemporaryFile(
        delete=False, suffix=".zip", dir=sde_dir
    ) as tmp:
        tmp_path = Path(tmp.name)
    try:
        with urllib.request.urlopen(_request(url), timeout=timeout) as resp:
            total = int(resp.headers.get("Content-Length", "0") or 0)
            chunk = 1 << 20  # 1 MiB
            written = 0
            with tmp_path.open("wb") as f:
                while True:
                    buf = resp.read(chunk)
                    if not buf:
                        break
                    f.write(buf)
                    written += len(buf)
                    if progress:
                        frac = (written / total) if total > 0 else -1.0
                        progress(
                            f"Downloading SDE: {written / (1 << 20):.1f} MiB"
                            + (f" / {total / (1 << 20):.1f} MiB" if total else ""),
                            frac,
                        )

        if progress:
            progress("Extracting SDE...", -1.0)
        installed_build = build
        with zipfile.ZipFile(tmp_path) as zf:
            members = [
                m for m in zf.namelist()
                if m.lower().endswith(".jsonl") and not m.endswith("/")
            ]
            for i, m in enumerate(members, 1):
                target = sde_dir / Path(m).name
                with zf.open(m) as src, target.open("wb") as dst:
                    shutil.copyfileobj(src, dst, length=1 << 20)
                if progress and (i % 8 == 0 or i == len(members)):
                    progress(
                        f"Extracting SDE: {i}/{len(members)} files",
                        i / max(len(members), 1),
                    )

            if installed_build is None:
                manifest = sde_dir / "_sde.jsonl"
                if manifest.exists():
                    try:
                        for line in manifest.read_text(encoding="utf-8").splitlines():
                            obj = json.loads(line)
                            if obj.get("_key") == "sde":
                                val = obj.get("_value", obj)
                                if isinstance(val, dict) and "buildNumber" in val:
                                    installed_build = int(val["buildNumber"])
                                    break
                    except (OSError, json.JSONDecodeError):
                        pass
        if installed_build is not None:
            write_local_build(sde_dir, installed_build)
        if progress:
            progress(f"SDE updated to build {installed_build or '?'}", 1.0)
        return installed_build or 0
    finally:
        try:
            tmp_path.unlink()
        except OSError:
            pass


def ensure_sde_current(
    sde_dir: Path,
    progress: Optional[ProgressFn] = None,
    auto_download: bool = True,
    timeout: float = 30.0,
) -> tuple[bool, Optional[int], Optional[int]]:
    """Ensure the SDE in ``sde_dir`` matches the upstream latest build.

    Returns ``(updated, local_build, remote_build)``. ``remote_build`` may be
    None if the network check failed.
    """
    local = read_local_build(sde_dir)
    try:
        if progress:
            progress("Checking latest SDE build...", -1.0)
        remote = fetch_latest_build_number(timeout=timeout)
    except Exception as exc:  # noqa: BLE001
        logger.warning("SDE freshness check failed: %s", exc)
        if progress:
            progress(f"SDE freshness check failed: {exc}", 1.0)
        return (False, local, None)

    if local is not None and local >= remote:
        if progress:
            progress(f"SDE up to date (build {local}).", 1.0)
        return (False, local, remote)

    if not auto_download:
        if progress:
            progress(
                f"SDE outdated (local {local}, remote {remote}); skipping download.",
                1.0,
            )
        return (False, local, remote)

    download_sde(sde_dir, build=remote, progress=progress, timeout=max(timeout, 120.0))
    return (True, local, remote)
