"""Lightweight ESI HTTP client with floating-window rate limiting.

Verbatim copy of eveHauler/esi.py — kept independent so eveMarket has no
dependency on the eveHauler package.
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Optional

import requests

logger = logging.getLogger(__name__)

USER_AGENT = "eveMarket/1.0 (+https://github.com/NolieRavioli/eveMarket)"


def _parse_limit_header(value: Optional[str]) -> tuple[Optional[int], Optional[int]]:
    if not value:
        return None, None
    try:
        limit_s, _, window_s = value.strip().partition("/")
        limit = int(float(limit_s))
        window_s = window_s.strip()
        if not window_s:
            return limit, 900
        if window_s.endswith("m"):
            return limit, int(window_s[:-1]) * 60
        if window_s.endswith("h"):
            return limit, int(window_s[:-1]) * 3600
        if window_s.endswith("s"):
            return limit, int(window_s[:-1])
        return limit, int(window_s)
    except (ValueError, IndexError):
        return None, None


def _parse_retry_after(value: Optional[str], default: float) -> float:
    if not value:
        return default
    try:
        return max(default, float(value))
    except (TypeError, ValueError):
        return default


class EsiClient:
    def __init__(
        self,
        *,
        low_water: int = 5,
        max_retries: int = 5,
        default_retry_after: float = 2.0,
        timeout: float = 30.0,
        user_agent: str = USER_AGENT,
    ) -> None:
        self.low_water = max(1, low_water)
        self.max_retries = max(1, max_retries)
        self.default_retry_after = default_retry_after
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": user_agent,
            "Accept": "application/json",
        })
        # Rate-limit state — all fields protected by _rl_lock because
        # CollectorScheduler and ContractScheduler share one EsiClient and
        # call get() from different threads.
        self._rl_lock = threading.Lock()
        # Per-route-group request rate limits: {group: {remaining, limit, window_s}}.
        # Different ESI endpoint groups (e.g. "market", "contracts") have
        # independent limits; we track each separately so the dashboard shows
        # all groups, not just whichever responded most recently.
        self._rl_by_group: dict = {}
        # Flat "last-seen" fields kept for backward compatibility with any
        # callers that read them directly (e.g. the pre-group dashboard code).
        self.rl_remaining: Optional[int] = None
        self.rl_limit: Optional[int] = None
        self.rl_window_seconds: Optional[int] = None
        self.rl_group: Optional[str] = None
        # Global error-rate-limit (X-ESI-Error-Limit-*).  Shared across ALL
        # ESI requests — contract-item 404s count against the same bucket as
        # market-order fetches.  ESI returns 420 when it hits zero.
        self.err_remain: Optional[int] = None
        self._err_reset_at: Optional[float] = None  # absolute unix of next reset

    def update_from_headers(self, headers) -> None:
        with self._rl_lock:
            # --- per-group request rate limit ---
            try:
                rem = headers.get("X-Ratelimit-Remaining")
                if rem is not None:
                    self.rl_remaining = int(rem)
            except (TypeError, ValueError):
                pass
            lim, window = _parse_limit_header(headers.get("X-Ratelimit-Limit"))
            if lim is not None:
                self.rl_limit = lim
            if window is not None:
                self.rl_window_seconds = window
            grp = headers.get("X-Ratelimit-Group")
            if grp:
                self.rl_group = grp
                entry = self._rl_by_group.setdefault(grp, {})
                if self.rl_remaining is not None:
                    entry["remaining"] = self.rl_remaining
                if self.rl_limit is not None:
                    entry["limit"] = self.rl_limit
                if self.rl_window_seconds is not None:
                    entry["window_s"] = self.rl_window_seconds
            # --- global error rate limit ---
            try:
                err_rem = headers.get("X-ESI-Error-Limit-Remain")
                if err_rem is not None:
                    self.err_remain = int(err_rem)
            except (TypeError, ValueError):
                pass
            try:
                err_reset = headers.get("X-ESI-Error-Limit-Reset")
                if err_reset is not None:
                    self._err_reset_at = time.time() + float(err_reset)
            except (TypeError, ValueError):
                pass

    def rl_snapshot(self) -> dict:
        """Return a thread-safe copy of all rate-limit state for the dashboard."""
        with self._rl_lock:
            return {
                "by_group": {k: dict(v) for k, v in self._rl_by_group.items()},
                "err_remain": self.err_remain,
                "err_reset_at": self._err_reset_at,
            }

    def _proactive_sleep(self) -> None:
        with self._rl_lock:
            groups = dict(self._rl_by_group)
            err_remain = self.err_remain
        sleep_needed = 0.0
        for grp, state in groups.items():
            rem = state.get("remaining")
            lim = state.get("limit")
            win = state.get("window_s")
            if rem is not None and lim and win and rem <= self.low_water:
                s = win / max(lim, 1)
                logger.info("[ESI] %s group low (%s/%s) — sleeping %.2fs", grp, rem, lim, s)
                sleep_needed = max(sleep_needed, s)
        if err_remain is not None and err_remain <= self.low_water:
            logger.info("[ESI] error budget low (%s) — backing off 2s", err_remain)
            sleep_needed = max(sleep_needed, 2.0)
        if sleep_needed > 0:
            time.sleep(sleep_needed)

    def get(
        self,
        url: str,
        params: Optional[dict] = None,
        *,
        headers: Optional[dict] = None,
    ) -> requests.Response:
        last_exc: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            self._proactive_sleep()
            try:
                resp = self.session.get(
                    url, params=params, headers=headers, timeout=self.timeout
                )
            except requests.RequestException as exc:
                last_exc = exc
                wait = min(2 ** attempt, 30)
                logger.warning("[ESI] %s on %s - retry in %ss (%s/%s)",
                               type(exc).__name__, url, wait, attempt, self.max_retries)
                time.sleep(wait)
                continue

            self.update_from_headers(resp.headers)

            if resp.status_code == 429:
                wait = _parse_retry_after(resp.headers.get("Retry-After"),
                                          self.default_retry_after)
                logger.warning("[ESI] 429 on %s - sleeping %.1fs (%s/%s)",
                               url, wait, attempt, self.max_retries)
                time.sleep(wait)
                continue
            if resp.status_code == 420:
                wait = _parse_retry_after(resp.headers.get("X-ESI-Error-Limit-Reset"),
                                          self.default_retry_after)
                logger.warning("[ESI] 420 on %s - sleeping %.1fs (%s/%s)",
                               url, wait, attempt, self.max_retries)
                time.sleep(wait)
                continue
            if 500 <= resp.status_code < 600:
                wait = min(2 ** attempt, 30)
                logger.warning("[ESI] %s on %s - retry in %ss (%s/%s)",
                               resp.status_code, url, wait, attempt, self.max_retries)
                time.sleep(wait)
                continue
            return resp

        if last_exc is not None:
            raise last_exc
        raise RuntimeError(f"ESI request to {url} failed after {self.max_retries} attempts")
