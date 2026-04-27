"""Lightweight ESI HTTP client with floating-window rate limiting.

Verbatim copy of eveHauler/esi.py — kept independent so eveMarket has no
dependency on the eveHauler package.
"""
from __future__ import annotations

import logging
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
        self.rl_remaining: Optional[int] = None
        self.rl_limit: Optional[int] = None
        self.rl_window_seconds: Optional[int] = None
        self.rl_group: Optional[str] = None

    def update_from_headers(self, headers) -> None:
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

    def _proactive_sleep(self) -> None:
        if (
            self.rl_remaining is not None
            and self.rl_limit
            and self.rl_window_seconds
            and self.rl_remaining <= self.low_water
        ):
            sleep_s = self.rl_window_seconds / max(self.rl_limit, 1)
            logger.info(
                "[ESI] floating window low (%s/%s) - sleeping %.2fs",
                self.rl_remaining, self.rl_limit, sleep_s,
            )
            time.sleep(sleep_s)

    def get(self, url: str, params: Optional[dict] = None) -> requests.Response:
        last_exc: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            self._proactive_sleep()
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
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
