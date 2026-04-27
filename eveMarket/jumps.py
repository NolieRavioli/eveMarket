"""SDE-derived solar-system jump graph + cached BFS within N jumps.

Built from ``mapStargates.jsonl`` (each row carries ``solarSystemID`` and a
``destination.solarSystemID``). The graph is undirected for jump-distance
purposes (every stargate has its mate).
"""
from __future__ import annotations

import json
import logging
import threading
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class JumpGraph:
    """In-memory adjacency + memoised BFS-within-N-jumps."""

    def __init__(self, sde_dir: Path) -> None:
        self.sde_dir = Path(sde_dir)
        self._adj: Optional[dict[int, set[int]]] = None
        self._bfs_cache: dict[tuple[int, int], dict[int, int]] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ load
    def _ensure_loaded(self) -> dict[int, set[int]]:
        if self._adj is not None:
            return self._adj
        with self._lock:
            if self._adj is not None:
                return self._adj
            adj: dict[int, set[int]] = {}
            path = self.sde_dir / "mapStargates.jsonl"
            with path.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    src = row.get("solarSystemID")
                    dst = (row.get("destination") or {}).get("solarSystemID")
                    if src is None or dst is None:
                        continue
                    src = int(src); dst = int(dst)
                    adj.setdefault(src, set()).add(dst)
                    adj.setdefault(dst, set()).add(src)  # idempotent; safety net
            logger.info("JumpGraph loaded: %s connected systems", len(adj))
            self._adj = adj
            return adj

    # ------------------------------------------------------------------ bfs
    def bfs_within(self, start_system_id: int, max_jumps: int) -> dict[int, int]:
        """Return ``{system_id: jump_distance}`` for systems within ``max_jumps``.

        Always includes ``start_system_id`` at distance 0 (even if isolated).
        Cached per ``(start, max_jumps)`` for the lifetime of this graph.
        """
        start = int(start_system_id)
        max_jumps = max(0, int(max_jumps))
        key = (start, max_jumps)
        cached = self._bfs_cache.get(key)
        if cached is not None:
            return cached

        adj = self._ensure_loaded()
        dist: dict[int, int] = {start: 0}
        if max_jumps == 0 or start not in adj:
            self._bfs_cache[key] = dist
            return dist

        frontier = [start]
        for d in range(1, max_jumps + 1):
            next_frontier: list[int] = []
            for u in frontier:
                for v in adj.get(u, ()):
                    if v in dist:
                        continue
                    dist[v] = d
                    next_frontier.append(v)
            if not next_frontier:
                break
            frontier = next_frontier
        self._bfs_cache[key] = dist
        return dist
