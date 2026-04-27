"""Tiny shared helper for in-place ``\r`` status lines on stderr."""
from __future__ import annotations

import sys
from datetime import datetime
from typing import Any, Optional

_state: dict[str, int] = {"len": 0}


def _rl_suffix(client: Optional[Any]) -> str:
    """Return ' rl=<remaining>/<limit>' for the given EsiClient (or '?/?')."""
    if client is None:
        return ""
    rem = getattr(client, "rl_remaining", None)
    lim = getattr(client, "rl_limit", None)
    rem_s = str(rem) if rem is not None else "?"
    lim_s = str(lim) if lim is not None else "?"
    return f" rl={rem_s}/{lim_s}"


def rprint(
    logger_name: str,
    msg: str,
    *,
    end: bool = False,
    client: Optional[Any] = None,
) -> None:
    """Overwrite the current stderr line with ``msg`` (matching log format).

    Pads with spaces to clear any longer previous status line. When ``end``
    is True the line is finalized with a newline. If ``client`` is provided,
    appends ' rl=<remaining>/<limit>' from its rate-limit fields.
    """
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
    full = f"{ts} INFO {logger_name}: {msg}{_rl_suffix(client)}"
    pad = max(0, _state["len"] - len(full))
    if end:
        sys.stderr.write(f"\r{full}{' ' * pad}\n")
        _state["len"] = 0
    else:
        sys.stderr.write(f"\r{full}{' ' * pad}")
        _state["len"] = len(full)
    sys.stderr.flush()
