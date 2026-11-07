"""Cached ESI reference data shared by the sibling tools: adjusted prices and industry cost indices.

``GET /esi/adjustedPrices`` proxies ESI ``/markets/prices/`` and ``GET /esi/costIndices`` proxies
``/industry/systems/``. Each is cached for an hour in memory and on disk (``data/refdata/``), so a restart
or an ESI hiccup serves the last good copy.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Optional

from .esi import EsiClient

logger = logging.getLogger(__name__)

ESI_BASE = "https://esi.evetech.net/latest"
TTL_S = 3600
SOURCES = {"adjustedPrices": "/markets/prices/", "costIndices": "/industry/systems/"}


class RefData:
    def __init__(self, data_dir: Path, client: Optional[EsiClient] = None, ttl_s: int = TTL_S) -> None:
        self.dir = Path(data_dir) / "refdata"
        self.dir.mkdir(parents=True, exist_ok=True)
        self.client = client or EsiClient()
        self.ttl_s = ttl_s
        self._lock = threading.Lock()
        self._mem: dict[str, tuple[float, bytes]] = {}

    def _path(self, name: str) -> Path:
        return self.dir / f"{name}.json"

    def get(self, name: str) -> tuple[bytes, float]:
        """(JSON body, fetched_at unix). Raises KeyError for unknown names, RuntimeError with no data at all."""
        if name not in SOURCES:
            raise KeyError(name)
        with self._lock:
            hit = self._mem.get(name)
            if hit is None and self._path(name).exists():
                p = self._path(name)
                hit = (p.stat().st_mtime, p.read_bytes())
                self._mem[name] = hit
            if hit is not None and time.time() - hit[0] < self.ttl_s:
                return hit[1], hit[0]
            try:
                resp = self.client.get(ESI_BASE + SOURCES[name], params={"datasource": "tranquility"})
                if resp.status_code == 200:
                    body = json.dumps(resp.json(), separators=(",", ":")).encode("utf-8")
                    now = time.time()
                    tmp = self._path(name).with_suffix(".tmp")
                    tmp.write_bytes(body)
                    os.replace(tmp, self._path(name))
                    self._mem[name] = (now, body)
                    return body, now
                logger.warning("refdata %s: ESI HTTP %s", name, resp.status_code)
            except Exception as exc:  # noqa: BLE001 - serve the stale copy
                logger.warning("refdata %s: %s", name, exc)
            if hit is not None:
                return hit[1], hit[0]
            raise RuntimeError(f"no {name} data yet")
