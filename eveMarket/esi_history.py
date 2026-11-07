"""ESI daily market history cache for ``POST /esiHistory/{region_id}`` (inference stays off).

Proxies ESI ``GET /markets/{region_id}/history/?type_id=``:

* fetched lazily per (region, type) and cached until ESI's next daily rollover (about 11:05 UTC);
* stored in SQLite at ``data/history/esi_history.sqlite`` (one row per day, 400 days kept), so a refresh
  writes only the new days instead of rewriting a per-region file;
* optionally warmed once a day for the busiest types of the main trade hubs' regions (``HistoryWarmer``).

Summaries per type (over the requested window, default 30 days):
``avg_7d`` mean daily average over the last 7 days with trades, ``vwavg_30d`` volume-weighted average,
``median_30d`` median daily average, ``adv_30d`` average daily volume over the window's calendar days
(ESI omits days without trades, so those count as zero), ``last_date``.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import statistics
import threading
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Optional

from .esi import EsiClient
from .snapshot import history_dir

logger = logging.getLogger(__name__)

ESI_BASE = "https://esi.evetech.net/latest"
ROLLOVER_UTC = (11, 5)          # ESI publishes the previous day's history after downtime
NOT_FOUND_BACKOFF_S = 1800      # ESI 404s for a while after downtime and for never-traded types
KEEP_DAYS = 400
MAX_TYPES_PER_REQUEST = 500
HUB_REGIONS = (10000002, 10000043, 10000032, 10000030, 10000042)  # Forge, Domain, Sinq Laison, Heimatar, Metropolis


def last_rollover(now: Optional[float] = None) -> float:
    now = time.time() if now is None else now
    dt = datetime.fromtimestamp(now, timezone.utc)
    roll = dt.replace(hour=ROLLOVER_UTC[0], minute=ROLLOVER_UTC[1], second=0, microsecond=0)
    if dt < roll:
        roll -= timedelta(days=1)
    return roll.timestamp()


def _f(x) -> str:
    if x is None:
        return "0"
    return f"{float(x):.11g}"


def summarize(rows: list[tuple], days: int, today: date) -> dict:
    """rows: (date, average, highest, lowest, volume, order_count) sorted by date, already within ``days``."""
    out_days = [{"date": d, "average": _f(a), "highest": _f(h), "lowest": _f(lo), "volume": str(int(v or 0)),
                 "order_count": str(int(n or 0))} for d, a, h, lo, v, n in rows]
    vols = [int(v or 0) for _, _, _, _, v, _ in rows]
    avgs = [float(a or 0) for _, a, _, _, _, _ in rows]
    tot_v = sum(vols)
    vwavg = sum(a * v for a, v in zip(avgs, vols)) / tot_v if tot_v else None
    last7 = avgs[-7:]
    return {
        "days": out_days,
        "avg_7d": _f(sum(last7) / len(last7)) if last7 else "0",
        "vwavg_30d": _f(vwavg),
        "median_30d": _f(statistics.median(avgs)) if avgs else "0",
        "adv_30d": _f(tot_v / days) if days else "0",
        "last_date": rows[-1][0] if rows else None,
    }


class EsiHistoryStore:
    def __init__(self, data_dir: Path, *, client: Optional[EsiClient] = None, keep_days: int = KEEP_DAYS,
                 path: Optional[Path] = None) -> None:
        self.client = client or EsiClient()
        self.keep_days = int(keep_days)
        self.path = Path(path) if path else history_dir(Path(data_dir)) / "esi_history.sqlite"
        self._lock = threading.RLock()
        self._db = sqlite3.connect(str(self.path), check_same_thread=False, timeout=30)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=NORMAL")
        self._db.executescript("""
            CREATE TABLE IF NOT EXISTS days (
                region_id INTEGER NOT NULL, type_id INTEGER NOT NULL, date TEXT NOT NULL,
                average REAL, highest REAL, lowest REAL, volume INTEGER, order_count INTEGER,
                PRIMARY KEY (region_id, type_id, date)) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS fetched (
                region_id INTEGER NOT NULL, type_id INTEGER NOT NULL, fetched_at REAL NOT NULL,
                status INTEGER NOT NULL, PRIMARY KEY (region_id, type_id)) WITHOUT ROWID;
        """)
        self._db.commit()
        self.fetches = 0
        self.last_fetch_at: Optional[float] = None
        self.last_error: Optional[str] = None

    # ------------------------------------------------------------------ freshness
    def is_fresh(self, region_id: int, type_id: int, now: Optional[float] = None) -> bool:
        now = time.time() if now is None else now
        with self._lock:
            row = self._db.execute("SELECT fetched_at, status FROM fetched WHERE region_id=? AND type_id=?",
                                   (region_id, type_id)).fetchone()
        if row is None:
            return False
        fetched_at, status = row
        if status == 404:
            return now - fetched_at < NOT_FOUND_BACKOFF_S
        if status != 200:
            return False
        return fetched_at >= last_rollover(now)

    # ------------------------------------------------------------------ fetch
    def fetch(self, region_id: int, type_id: int) -> int:
        """Fetch one (region, type) from ESI and store it. Returns the HTTP status."""
        url = f"{ESI_BASE}/markets/{int(region_id)}/history/"
        try:
            resp = self.client.get(url, params={"type_id": int(type_id), "datasource": "tranquility"})
        except Exception as exc:  # network: keep what we have
            self.last_error = f"{type(exc).__name__}: {exc}"
            logger.warning("esiHistory region=%s type=%s: %s", region_id, type_id, exc)
            return 599
        status = resp.status_code
        now = time.time()
        rows: list = []
        if status == 200:
            try:
                rows = resp.json() or []
            except ValueError:
                status = 502
        cutoff = (datetime.now(timezone.utc).date() - timedelta(days=self.keep_days)).isoformat()
        with self._lock:
            if status == 200:
                self._db.executemany(
                    "INSERT OR REPLACE INTO days VALUES (?,?,?,?,?,?,?,?)",
                    [(region_id, type_id, r["date"], r.get("average"), r.get("highest"), r.get("lowest"),
                      r.get("volume"), r.get("order_count")) for r in rows
                     if isinstance(r, dict) and r.get("date", "") >= cutoff])
                self._db.execute("DELETE FROM days WHERE region_id=? AND type_id=? AND date<?",
                                 (region_id, type_id, cutoff))
            if status in (200, 404):
                self._db.execute("INSERT OR REPLACE INTO fetched VALUES (?,?,?,?)", (region_id, type_id, now, status))
            self._db.commit()
            self.fetches += 1
            self.last_fetch_at = now
        if status not in (200, 404):
            self.last_error = f"HTTP {status} for region {region_id} type {type_id}"
        return status

    # ------------------------------------------------------------------ read
    def get(self, region_id: int, type_ids: Iterable[int], days: int = 30) -> dict[str, dict]:
        region_id, days = int(region_id), max(1, min(int(days), self.keep_days))
        today = datetime.now(timezone.utc).date()
        cutoff = (today - timedelta(days=days)).isoformat()
        out: dict[str, dict] = {}
        for tid in dict.fromkeys(int(t) for t in type_ids):
            if not self.is_fresh(region_id, tid):
                self.fetch(region_id, tid)
            with self._lock:
                rows = self._db.execute(
                    "SELECT date, average, highest, lowest, volume, order_count FROM days "
                    "WHERE region_id=? AND type_id=? AND date>=? ORDER BY date", (region_id, tid, cutoff)).fetchall()
            out[str(tid)] = summarize(rows, days, today)
        return out

    def stats(self) -> dict:
        with self._lock:
            pairs, regions = self._db.execute(
                "SELECT COUNT(*), COUNT(DISTINCT region_id) FROM fetched").fetchone()
            fresh_after = last_rollover()
            fresh = self._db.execute("SELECT COUNT(*) FROM fetched WHERE status=200 AND fetched_at>=?",
                                     (fresh_after,)).fetchone()[0]
            nrows = self._db.execute("SELECT COUNT(*) FROM days").fetchone()[0]
        return {"pairs": pairs, "regions": regions, "fresh_pairs": fresh, "rows": nrows,
                "fetches_since_start": self.fetches, "last_fetch_unix": int(self.last_fetch_at) if self.last_fetch_at else None,
                "last_error": self.last_error, "db_bytes": self.path.stat().st_size if self.path.exists() else 0}

    def close(self) -> None:
        with self._lock:
            self._db.close()


def top_types(data_dir: Path, region_id: int, n: int) -> list[int]:
    """The ``n`` types with the most orders in a region, from the precomputed region stats file."""
    from .precompute import stats_file

    try:
        with open(stats_file(Path(data_dir), int(region_id)), "r", encoding="utf-8") as f:
            payload = json.load(f)
    except (OSError, json.JSONDecodeError):
        return []
    counts = []
    for tid, rec in payload.items():
        try:
            c = sum(float((rec.get(side) or {}).get("orderCount") or 0) for side in ("buy", "sell"))
        except (TypeError, ValueError, AttributeError):
            continue
        if c > 0:
            counts.append((c, int(tid)))
    counts.sort(reverse=True)
    return [t for _, t in counts[:max(0, int(n))]]


class HistoryWarmer:
    """Once a day after the rollover, refresh the busiest ``n_types`` per hub region (0 disables)."""

    def __init__(self, store: EsiHistoryStore, data_dir: Path, *, n_types: int = 3000,
                 regions: Iterable[int] = HUB_REGIONS, pause_s: float = 0.25) -> None:
        self.store, self.data_dir, self.n_types = store, Path(data_dir), int(n_types)
        self.regions = tuple(int(r) for r in regions)
        self.pause_s = pause_s
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.status = {"last_run_unix": None, "running": False, "done": 0, "total": 0}

    def start(self) -> None:
        if self.n_types <= 0 or self._thread is not None:
            return
        self._thread = threading.Thread(target=self._loop, name="esi-history-warm", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def run_once(self) -> int:
        self.status.update(running=True, done=0, total=0)
        n = 0
        try:
            work = [(r, t) for r in self.regions for t in top_types(self.data_dir, r, self.n_types)]
            self.status["total"] = len(work)
            for r, t in work:
                if self._stop.is_set():
                    break
                if not self.store.is_fresh(r, t):
                    self.store.fetch(r, t)
                    n += 1
                    self._stop.wait(self.pause_s)
                self.status["done"] += 1
        finally:
            self.status.update(running=False, last_run_unix=int(time.time()))
        logger.info("esiHistory warm-up: %s fetched, %s pairs checked", n, self.status["done"])
        return n

    def _loop(self) -> None:
        self._stop.wait(120)  # let the collector start first
        while not self._stop.is_set():
            last = self.status["last_run_unix"]
            if last is None or last < last_rollover() + 600:
                if time.time() >= last_rollover() + 600:  # 10 min after the rollover
                    try:
                        self.run_once()
                    except Exception:  # noqa: BLE001 - keep the thread alive
                        logger.exception("esiHistory warm-up failed")
            self._stop.wait(300)
