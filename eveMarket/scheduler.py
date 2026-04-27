"""20-minute collection scheduler.

Runs forever in a daemon thread. Skips a tick if the previous collection is
still running (no overlapping snapshots).
"""
from __future__ import annotations

import logging
import threading
import time
from pathlib import Path
from typing import Callable, Optional

from .collector import collect_snapshot
from .esi import EsiClient
from .inferred import diff_snapshots, is_complete, write_inferred
from .jumps import JumpGraph
from .location import LocationResolver
from .precompute import needs_precompute, run_precompute
from .snapshot import latest_snapshot, previous_snapshot, orders_path

logger = logging.getLogger(__name__)


class CollectorScheduler:
    def __init__(
        self,
        sde_dir: Path,
        data_dir: Path,
        *,
        interval_s: int = 1200,
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
        # Precompute artefacts share resolver+jumps with the HTTP server when
        # possible (set via `attach_precompute_resources`); otherwise they are
        # lazy-loaded on first use.
        self._resolver: Optional[LocationResolver] = None
        self._jumps: Optional[JumpGraph] = None

    def attach_precompute_resources(
        self,
        resolver: Optional[LocationResolver],
        jumps: Optional[JumpGraph],
    ) -> None:
        """Reuse already-loaded SDE caches to avoid double-loading them."""
        self._resolver = resolver
        self._jumps = jumps

    def _do_precompute(self, snapshot_unix: int) -> None:
        try:
            run_precompute(
                self.sde_dir, self.data_dir,
                snapshot_unix=snapshot_unix,
                resolver=self._resolver,
                jumps=self._jumps,
                client=self.client,
            )
        except Exception:
            logger.exception("precompute failed for snapshot %s", snapshot_unix)

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
            now = time.time()
            if now >= next_at:
                self._run_one()
                next_at = time.time() + self.interval_s
            self.stop_event.wait(min(5.0, max(0.5, next_at - time.time())))

    def _run_one(self) -> None:
        if not self._running.acquire(blocking=False):
            logger.warning("scheduler: previous collection still running, skipping tick")
            return
        try:
            trigger_unix = int(time.time())
            prev_unix = previous_snapshot(self.data_dir, trigger_unix)
            try:
                out, total, t_unix = collect_snapshot(
                    self.sde_dir, self.data_dir,
                    trigger_unix=trigger_unix,
                    client=self.client,
                    stop_event=self.stop_event,
                )
            except InterruptedError:
                logger.info("scheduler: collection interrupted")
                return
            except Exception:
                logger.exception("scheduler: collection failed")
                return

            if self.on_snapshot is not None:
                try:
                    self.on_snapshot(out, total, t_unix)
                except Exception:
                    logger.exception("on_snapshot callback failed")

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

            # Precompute /stats + /history for the new snapshot.
            self._do_precompute(t_unix)
        finally:
            self._running.release()

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
