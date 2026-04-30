"""eveMarket service entry point.

Usage:
    python eveMarket.py [--sde ./sde] [--data ./data]
                        [--bind 0.0.0.0] [--port 13307] [--interval 301]

Starts:
  - 20-minute market-snapshot scheduler (writes data/orders/orders-<unix>.jsonl)
  - HTTP API on bind:port
"""
from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import threading
from pathlib import Path

# Make the eveMarket package importable when launched directly.
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from eveMarket import EsiClient, CollectorScheduler, ContractScheduler, DowntimeGuard, serve  # noqa: E402
from eveMarket.sde_download import download_sde  # noqa: E402

DEFAULT_SDE = HERE / "sde"
DEFAULT_DATA = HERE / "data"


def main() -> int:
    ap = argparse.ArgumentParser(description="eveMarket collector + API")
    ap.add_argument("--sde", type=Path, default=DEFAULT_SDE,
                    help="Path to SDE jsonl directory (default: ./sde)")
    ap.add_argument("--data", type=Path, default=DEFAULT_DATA,
                    help="Path to data directory (default: ./data)")
    ap.add_argument("--bind", default="0.0.0.0", help="HTTP bind address")
    ap.add_argument("--port", type=int, default=13307, help="HTTP port")
    ap.add_argument("--interval", type=int, default=301,
                    help="Collection interval in seconds (default 301 = 5m 1s; "
                         "one second past ESI's market cache so we always pick "
                         "up the next refresh)")
    ap.add_argument("--contracts-interval", type=int, default=1800,
                    help="Contracts collection interval in seconds (default 1800 = 30 min)")
    ap.add_argument("--no-inference", action="store_true",
                    help="Disable inferred-trade computation in the scheduler")
    ap.add_argument("--no-contracts", action="store_true",
                    help="Disable the public-contracts scheduler")
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("eveMarket")

    if not args.sde.exists():
        log.info("SDE dir not found at %s — downloading now...", args.sde)
        download_sde(args.sde, progress=lambda msg, _: log.info("SDE: %s", msg))
    args.data.mkdir(parents=True, exist_ok=True)

    client = EsiClient()

    scheduler = CollectorScheduler(
        sde_dir=args.sde,
        data_dir=args.data,
        interval_s=args.interval,
        client=client,
        run_inference=not args.no_inference,
    )

    contract_scheduler: ContractScheduler | None = None
    if not args.no_contracts:
        contract_scheduler = ContractScheduler(
            sde_dir=args.sde,
            data_dir=args.data,
            interval_s=args.contracts_interval,
            client=client,
        )

    # DowntimeGuard: started BEFORE the schedulers so that if ESI is already
    # unreachable (server launched during EVE downtime) the guard can
    # pre-suspend the schedulers before they attempt any ESI requests.
    downtime_guard = DowntimeGuard(
        client,
        [scheduler] + ([contract_scheduler] if contract_scheduler is not None else []),
    )
    downtime_guard.start()
    log.info("downtime_guard started (downtime at 11:00 UTC, poll every 2 min)")

    scheduler.start()
    log.info("scheduler started (interval=%ss, inference=%s)",
             args.interval, not args.no_inference)

    if contract_scheduler is not None:
        contract_scheduler.start()
        log.info("contracts scheduler started (interval=%ss)",
                 args.contracts_interval)

    httpd = serve(
        sde_dir=args.sde,
        data_dir=args.data,
        bind=args.bind,
        port=args.port,
        client=client,
    )

    shutting_down = threading.Event()

    def _shutdown(*_):
        if shutting_down.is_set():
            # Second Ctrl+C: bail out hard.
            log.warning("second shutdown signal — forcing exit")
            os._exit(1)
        shutting_down.set()
        log.info("shutdown signal received")
        # httpd.shutdown() blocks until serve_forever() returns, but
        # serve_forever() is on this very (main) thread. Run shutdown
        # from a helper thread to avoid the deadlock.
        def _do():
            scheduler.stop()
            if contract_scheduler is not None:
                contract_scheduler.stop()
            downtime_guard.stop()
            httpd.shutdown()
        threading.Thread(target=_do, name="eveMarket-shutdown", daemon=True).start()

    signal.signal(signal.SIGINT, _shutdown)
    try:
        signal.signal(signal.SIGTERM, _shutdown)
    except (AttributeError, ValueError):
        pass

    try:
        httpd.serve_forever()
    finally:
        scheduler.stop()
        if contract_scheduler is not None:
            contract_scheduler.stop()
        downtime_guard.stop()
        httpd.server_close()
        log.info("eveMarket stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
