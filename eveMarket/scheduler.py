"""5-minute order-book collection scheduler.

Runs forever in a daemon thread. Skips a tick if the previous collection is
still running (no overlapping snapshots). The default interval (301 s) is
one second past ESI's 5-minute Last-Modified cache so the very first tick
after the cache flips picks up the new data immediately.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, List, Optional

from .collector import collect_snapshot
from .compression import sweep_old_data
from .contracts import collect_contracts
from .esi import EsiClient
from .inferred import diff_snapshots, is_complete, write_inferred
from .location import LocationResolver
from .precompute import needs_precompute, run_precompute
from .snapshot import latest_snapshot, previous_snapshot, orders_path

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Downtime support helpers
# ---------------------------------------------------------------------------

# EVE server downtime starts at 11:00 UTC every day.
_DOWNTIME_HOUR_UTC = 11

# ESI health endpoint used to detect recovery.
_ESI_PING_URL = "https://esi.evetech.net/ping"

# Interval between ESI ping attempts during downtime (seconds).
_DOWNTIME_POLL_INTERVAL = 120


class _CancelToken:
    """Pseudo-Event whose ``is_set()`` returns True if *any* backing event is set.

    Passed to ``collect_snapshot`` / ``collect_contracts`` as the ``stop_event``
    argument so that a downtime cancel pulse (``_cancel``) or a full shutdown
    (``stop_event``) both abort in-flight collection without conflating the two
    signals.
    """

    __slots__ = ("_events",)

    def __init__(self, *events: threading.Event) -> None:
        self._events = events

    def is_set(self) -> bool:  # noqa: D102
        return any(e.is_set() for e in self._events)


class CollectorScheduler:
    def __init__(
        self,
        sde_dir: Path,
        data_dir: Path,
        *,
        interval_s: int = 301,
        client: Optional[EsiClient] = None,
        run_inference: bool = True,
        on_snapshot: Optional[Callable[[Path, int, int], None]] = None,
    ) -> None:
        self.sde_dir = Path(sde_dir)
        self.data_dir = Path(data_dir)
        self.interval_s = int(interval_s)
        self.client = client or EsiClient()
        self.run_inference = run_inference
        self.on_snapshot = on_snapshot
        self.stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._running = threading.Lock()
        # Downtime support: _downtime is SET while ESI is suspended; _cancel
        # is a momentary pulse used by DowntimeGuard to abort an in-flight
        # collection without triggering a full scheduler shutdown.
        self._downtime = threading.Event()
        self._cancel = threading.Event()
        # Precompute artefacts share the resolver with the HTTP server when
        # possible (set via `attach_precompute_resources`); otherwise it is
        # lazy-loaded on first use.
        self._resolver: Optional[LocationResolver] = None

    def attach_precompute_resources(
        self,
        resolver: Optional[LocationResolver],
        jumps=None,  # accepted for back-compat; ignored
    ) -> None:
        """Reuse already-loaded SDE caches to avoid double-loading them."""
        self._resolver = resolver

    def _do_precompute(self, snapshot_unix: int) -> None:
        try:
            run_precompute(
                self.sde_dir, self.data_dir,
                snapshot_unix=snapshot_unix,
                resolver=self._resolver,
                client=self.client,
            )
        except Exception:
            logger.exception("precompute failed for snapshot %s", snapshot_unix)
        # Always try the compression sweep after precompute, so disk usage
        # stays bounded even if the precompute step itself failed.
        try:
            sweep_old_data(self.data_dir)
        except Exception:
            logger.exception("compression sweep failed")

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        # Catch up on inferred trades for any pre-existing snapshot pair before
        # the periodic loop starts (runs in a daemon thread so we don't block).
        if self.run_inference:
            threading.Thread(
                target=self.catch_up_inferred,
                name="eveMarket-infer-catchup",
                daemon=True,
            ).start()
        # Catch up on precomputed datasets for the latest snapshot if stale.
        threading.Thread(
            target=self.catch_up_precompute,
            name="eveMarket-precompute-catchup",
            daemon=True,
        ).start()
        # One-shot backfill: gzip any historical snapshot/inferred files that
        # accumulated before compression existed.
        threading.Thread(
            target=self._startup_sweep,
            name="eveMarket-compression-backfill",
            daemon=True,
        ).start()
        self._thread = threading.Thread(target=self._loop, name="eveMarket-scheduler",
                                        daemon=True)
        self._thread.start()

    def catch_up_inferred(self) -> None:
        """If snapshots already exist on disk, infer trades for the latest pair.

        No-op when there are <2 snapshots or the inferred file is already done.
        """
        latest = latest_snapshot(self.data_dir)
        if latest is None:
            return
        if is_complete(self.data_dir, latest):
            logger.info("catch_up_inferred: infer-%s already complete", latest)
            return
        prev = previous_snapshot(self.data_dir, latest)
        if prev is None:
            logger.info("catch_up_inferred: only one snapshot (%s); waiting for next collection",
                        latest)
            return
        logger.info("catch_up_inferred: processing inferred trades for snapshot %s (vs %s)",
                    latest, prev)
        try:
            trades = diff_snapshots(
                orders_path(self.data_dir, prev),
                orders_path(self.data_dir, latest),
                data_dir=self.data_dir,
                client=self.client,
            )
            inf_path, n = write_inferred(self.data_dir, latest, trades)
            logger.info("catch_up_inferred: %s trades -> %s", n, inf_path)
        except Exception:
            logger.exception("catch_up_inferred failed")

    def stop(self) -> None:
        self.stop_event.set()

    def trigger_now(self) -> None:
        """Run one collection synchronously (used for the initial tick)."""
        self._run_one()

    def _loop(self) -> None:
        # If we already have a recent snapshot, wait until <latest> + interval
        # before collecting again. Otherwise, fire immediately.
        now = time.time()
        latest = latest_snapshot(self.data_dir)
        if latest is not None and (latest + self.interval_s) > now:
            next_at = float(latest + self.interval_s)
            wait_s = next_at - now
            mins, secs = divmod(int(wait_s), 60)
            logger.info(
                "scheduler: latest snapshot is %s; next collection in %dm %02ds",
                latest, mins, secs,
            )
        else:
            next_at = now
        while not self.stop_event.is_set():
            # --- downtime suspension ---
            if self._downtime.is_set():
                logger.info("scheduler: downtime active — waiting for ESI recovery")
                while self._downtime.is_set() and not self.stop_event.is_set():
                    self.stop_event.wait(5.0)
                if self.stop_event.is_set():
                    return
                logger.info("scheduler: ESI back — collecting immediately")
                next_at = time.time()
                continue
            # --- normal tick ---
            now = time.time()
            if now >= next_at:
                collected_at = self._run_one()
                # Base the next tick on the ESI Last-Modified timestamp
                # (returned by collect_snapshot) so we fire exactly
                # interval_s after ESI's cache epoch, not interval_s after
                # we finished the crawl. Falls back to time.time() if the
                # collection failed or returned no Last-Modified headers.
                next_at = (collected_at or time.time()) + self.interval_s
            self.stop_event.wait(min(5.0, max(0.5, next_at - time.time())))

    def _run_one(self) -> Optional[float]:
        """Collect one snapshot; return the ESI Last-Modified unix time (basis
        for the next scheduled tick), or None on failure.

        Returns the max Last-Modified across all cached regions so the loop
        can schedule the next tick as ``esi_last_modified + interval_s``
        rather than ``time.time() + interval_s``. If ESI returned no
        Last-Modified headers (all-304 run) falls back to the trigger unix.

        Inference and precompute run in a separate daemon thread so the
        collection lock is released as soon as the snapshot file is written.
        """
        if not self._running.acquire(blocking=False):
            logger.warning("scheduler: previous collection still running, skipping tick")
            return None
        try:
            trigger_unix = int(time.time())
            prev_unix = previous_snapshot(self.data_dir, trigger_unix)
            try:
                out, total, t_unix, esi_lm_unix = collect_snapshot(
                    self.sde_dir, self.data_dir,
                    trigger_unix=trigger_unix,
                    client=self.client,
                    stop_event=_CancelToken(self.stop_event, self._cancel),
                )
            except InterruptedError:
                logger.info("scheduler: collection interrupted")
                return None
            except Exception:
                logger.exception("scheduler: collection failed")
                return None
        finally:
            self._running.release()

        if self.on_snapshot is not None:
            try:
                self.on_snapshot(out, total, t_unix)
            except Exception:
                logger.exception("on_snapshot callback failed")

        # Inference and precompute are slow (~50s total). Run them in a
        # daemon thread so the collection loop is free to start the next
        # snapshot fetch as soon as the ESI cache window expires.
        threading.Thread(
            target=self._post_collect,
            args=(out, t_unix, prev_unix),
            name="eveMarket-post-collect",
            daemon=True,
        ).start()

        # Return ESI Last-Modified as the basis for the next scheduled tick.
        # If no region returned a Last-Modified (all-304 run), fall back to
        # the trigger time so the interval is measured from the same epoch.
        return float(esi_lm_unix) if esi_lm_unix is not None else float(t_unix)

    def _post_collect(self, out: Path, t_unix: int, prev_unix: Optional[int]) -> None:
        """Inference + precompute + compression sweep for a completed snapshot."""
        if self.run_inference and prev_unix is not None:
            try:
                prev_path = orders_path(self.data_dir, prev_unix)
                trades = diff_snapshots(prev_path, out,
                                        data_dir=self.data_dir,
                                        client=self.client)
                inf_path, n = write_inferred(self.data_dir, t_unix, trades)
                logger.info("inferred: %s trades -> %s", n, inf_path)
            except Exception:
                logger.exception("inferred-trade computation failed")

        self._do_precompute(t_unix)

    def catch_up_precompute(self) -> None:
        """Generate precomputed datasets if missing or stale on startup."""
        latest = latest_snapshot(self.data_dir)
        if latest is None:
            return
        if not needs_precompute(self.data_dir, latest):
            logger.info("catch_up_precompute: meta is up-to-date for %s", latest)
            return
        logger.info("catch_up_precompute: regenerating for snapshot %s", latest)
        self._do_precompute(latest)

    def _startup_sweep(self) -> None:
        """One-shot compression backfill for pre-existing historical files."""
        try:
            sweep_old_data(self.data_dir)
        except Exception:
            logger.exception("startup compression sweep failed")


# --- contracts -----------------------------------------------------------

# ESI's public-contracts cache window is ~10 minutes; 30 minutes is well
# above that and keeps load on the upstream low while still presenting a
# fresh-enough view to clients.
CONTRACTS_INTERVAL_DEFAULT_S = 1800


class ContractScheduler:
    """Independent 30-minute scheduler for public-contracts collection.

    Runs separately from :class:`CollectorScheduler` so the slower contracts
    pull never blocks the time-sensitive market snapshot. Persistence of the
    last successful run lives in the per-region meta file maintained by
    :func:`collect_contracts` -- the scheduler just records its own next-run
    time on disk so restarts don't re-pull immediately.
    """

    NEXT_RUN_FILENAME = "contracts_next_run.json"

    def __init__(
        self,
        sde_dir: Path,
        data_dir: Path,
        *,
        interval_s: int = CONTRACTS_INTERVAL_DEFAULT_S,
        client: Optional[EsiClient] = None,
    ) -> None:
        self.sde_dir = Path(sde_dir)
        self.data_dir = Path(data_dir)
        self.interval_s = int(interval_s)
        self.client = client or EsiClient()
        self.stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._running = threading.Lock()
        # Downtime support (see DowntimeGuard).
        self._downtime = threading.Event()
        self._cancel = threading.Event()

    # --- next-run persistence (so restarts don't double-pull) -----------

    def _next_run_path(self) -> Path:
        from .snapshot import contracts_dir as _cdir
        return _cdir(self.data_dir) / self.NEXT_RUN_FILENAME

    def _load_next_run(self) -> Optional[float]:
        path = self._next_run_path()
        if not path.exists():
            return None
        try:
            with path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            return float(data.get("next_run_unix") or 0) or None
        except (OSError, ValueError, json.JSONDecodeError):
            return None

    def _save_next_run(self, next_run_unix: float) -> None:
        path = self._next_run_path()
        tmp = path.with_suffix(".json.tmp")
        try:
            with tmp.open("w", encoding="utf-8") as f:
                json.dump({"next_run_unix": float(next_run_unix)}, f)
            import os as _os
            _os.replace(tmp, path)
        except OSError:
            logger.warning("could not persist contracts next-run marker")

    # --- lifecycle ------------------------------------------------------

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._thread = threading.Thread(
            target=self._loop, name="eveMarket-contracts-scheduler", daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self.stop_event.set()

    def trigger_now(self) -> None:
        self._run_one()

    def _loop(self) -> None:
        now = time.time()
        persisted_next = self._load_next_run()
        if persisted_next is not None and persisted_next > now:
            next_at = persisted_next
            wait_s = next_at - now
            mins, secs = divmod(int(wait_s), 60)
            logger.info(
                "contracts scheduler: next run in %dm %02ds (persisted)",
                mins, secs,
            )
        else:
            next_at = now
        while not self.stop_event.is_set():
            # --- downtime suspension ---
            if self._downtime.is_set():
                logger.info("contracts scheduler: downtime active — waiting for ESI recovery")
                while self._downtime.is_set() and not self.stop_event.is_set():
                    self.stop_event.wait(5.0)
                if self.stop_event.is_set():
                    return
                logger.info("contracts scheduler: ESI back — collecting immediately")
                next_at = time.time()
                continue
            # --- normal tick ---
            now = time.time()
            if now >= next_at:
                esi_lm = self._run_one()
                next_at = (esi_lm or time.time()) + self.interval_s
                self._save_next_run(next_at)
            self.stop_event.wait(min(15.0, max(0.5, next_at - time.time())))

    def _run_one(self) -> Optional[float]:
        """Collect contracts; return ESI Last-Modified unix (basis for next tick), or None."""
        if not self._running.acquire(blocking=False):
            logger.warning("contracts scheduler: previous run still in flight, skipping tick")
            return None
        try:
            esi_lm_unix: Optional[float] = None
            try:
                _, _, _, esi_lm_unix = collect_contracts(
                    self.sde_dir, self.data_dir,
                    trigger_unix=int(time.time()),
                    client=self.client,
                    stop_event=_CancelToken(self.stop_event, self._cancel),
                )
            except InterruptedError:
                logger.info("contracts scheduler: collection interrupted")
            except Exception:
                logger.exception("contracts scheduler: collection failed")
            # Compress old contract snapshots so they don't accumulate on disk.
            try:
                sweep_old_data(self.data_dir)
            except Exception:
                logger.exception("contracts scheduler: compression sweep failed")
            return esi_lm_unix
        finally:
            self._running.release()


# ---------------------------------------------------------------------------
# DowntimeGuard
# ---------------------------------------------------------------------------

class DowntimeGuard:
    """Suspends all ESI collection around EVE server downtime at 11:00 UTC.

    Behaviour
    ---------
    **At 11:00 UTC every day:**

    1. Sends a cancel pulse to any in-flight collection (via ``_cancel``).
    2. Marks all registered schedulers as suspended (they pause their loops).
    3. Polls ``GET /ping`` on ESI every 2 minutes until a ``200`` is returned.
    4. Clears the suspended flag on all schedulers and triggers an immediate
       collection cycle on each.

    **On startup**, if ESI is not reachable the guard pre-suspends all
    schedulers before they make any requests, then enters recovery mode
    immediately.  This covers the case where the server is started while
    EVE downtime is still in progress.
    """

    def __init__(
        self,
        client: EsiClient,
        schedulers: "List[CollectorScheduler | ContractScheduler]",
    ) -> None:
        self._client = client
        self._schedulers = list(schedulers)
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # --- public lifecycle -----------------------------------------------

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        # Startup probe: if ESI is already unreachable, pre-suspend all
        # schedulers *before* they issue their first request.
        if not self._ping():
            logger.info(
                "downtime_guard: ESI unreachable on startup — pre-suspending collectors"
            )
            for s in self._schedulers:
                s._downtime.set()
        self._thread = threading.Thread(
            target=self._guard_loop,
            name="eveMarket-downtime-guard",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    # --- internal -------------------------------------------------------

    def _ping(self) -> bool:
        """Return True if the ESI ping endpoint responds with HTTP 200."""
        try:
            resp = self._client.session.get(_ESI_PING_URL, timeout=10.0)
            return resp.status_code == 200
        except Exception:
            return False

    def _suspend_all(self) -> None:
        """Cancel in-flight collections and mark all schedulers as suspended."""
        logger.info("downtime_guard: suspending all collectors")
        # Pulse _cancel so that any ongoing collect_snapshot / collect_contracts
        # call raises InterruptedError at the next region boundary.
        for s in self._schedulers:
            s._cancel.set()
        # Give the cancel signal a moment to propagate through the collection
        # loops (they check every region, so worst case one region's fetch).
        self._stop.wait(2.0)
        for s in self._schedulers:
            s._downtime.set()
            s._cancel.clear()  # reset for the next collection cycle

    def _resume_all(self) -> None:
        """Clear the suspended flag and trigger an immediate collection tick."""
        logger.info("downtime_guard: ESI is back — resuming all collectors")
        for s in self._schedulers:
            s._downtime.clear()
        # Fire an immediate collection on each scheduler in a daemon thread so
        # we don't block the guard loop (trigger_now acquires _running lock).
        for s in self._schedulers:
            threading.Thread(
                target=s.trigger_now,
                name="eveMarket-downtime-resume",
                daemon=True,
            ).start()

    def _next_downtime_unix(self) -> float:
        """Return the unix timestamp of the next 11:00 UTC."""
        now = datetime.now(timezone.utc)
        candidate = now.replace(
            hour=_DOWNTIME_HOUR_UTC, minute=0, second=0, microsecond=0
        )
        if candidate <= now:
            candidate += timedelta(days=1)
        return candidate.timestamp()

    def _do_recovery(self) -> None:
        """Poll ESI every ``_DOWNTIME_POLL_INTERVAL`` seconds until it returns 200."""
        while not self._stop.is_set():
            logger.info("downtime_guard: pinging %s", _ESI_PING_URL)
            if self._ping():
                self._resume_all()
                return
            logger.info(
                "downtime_guard: ESI still down — retrying in %ds",
                _DOWNTIME_POLL_INTERVAL,
            )
            self._stop.wait(float(_DOWNTIME_POLL_INTERVAL))

    def _guard_loop(self) -> None:
        """Main background thread: watch for 11:00 UTC and manage recovery."""
        # If schedulers were pre-suspended on startup, go straight to recovery.
        if any(s._downtime.is_set() for s in self._schedulers):
            self._do_recovery()

        while not self._stop.is_set():
            next_dt = self._next_downtime_unix()
            wait_s = next_dt - time.time()
            h, rem = divmod(int(max(0, wait_s)), 3600)
            m, s_ = divmod(rem, 60)
            logger.info(
                "downtime_guard: next downtime at 11:00 UTC in %dh %02dm %02ds",
                h, m, s_,
            )
            # Sleep until 11:00 UTC, waking every 30 s to check stop.
            while not self._stop.is_set():
                remaining = next_dt - time.time()
                if remaining <= 0:
                    break
                self._stop.wait(min(30.0, remaining))
            if self._stop.is_set():
                return
            logger.info("downtime_guard: 11:00 UTC reached — suspending collectors")
            self._suspend_all()
            self._do_recovery()

