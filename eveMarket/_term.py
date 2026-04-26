"""Tiny shared helper for in-place ``\r`` status lines on stderr."""
from __future__ import annotations

import sys
from datetime import datetime

_state: dict[str, int] = {"len": 0}


def rprint(logger_name: str, msg: str, *, end: bool = False) -> None:
    """Overwrite the current stderr line with ``msg`` (matching log format).

    Pads with spaces to clear any longer previous status line. When ``end``
    is True the line is finalized with a newline.
    """
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
    full = f"{ts} INFO {logger_name}: {msg}"
    pad = max(0, _state["len"] - len(full))
    if end:
        sys.stderr.write(f"\r{full}{' ' * pad}\n")
        _state["len"] = 0
    else:
        sys.stderr.write(f"\r{full}{' ' * pad}")
        _state["len"] = len(full)
    sys.stderr.flush()
