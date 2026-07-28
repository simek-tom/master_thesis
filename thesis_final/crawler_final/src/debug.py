"""Lightweight, opt-in run tracing.

Why this exists: Telethon auto-sleeps on FloodWaits below its
flood_sleep_threshold (default 60s) with no output, so a run that is actually
rate-limited looks identical to one that is hung. dlog() prints timestamped,
flushed progress markers at every network seam; enable_telethon_logging()
surfaces Telethon's own MTProto + "Sleeping Ns" messages so we can tell a
silent flood-sleep apart from a real stall.

Toggle with the --debug flag (main.py) or CRAWLER_DEBUG=1 in the environment.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from datetime import datetime

_enabled: bool = os.environ.get("CRAWLER_DEBUG", "") not in ("", "0", "false")
_t0: float = time.monotonic()


def is_enabled() -> bool:
    return _enabled


def enable() -> None:
    """Turn tracing on for the rest of the process (called by --debug)."""
    global _enabled
    _enabled = True


def dlog(msg: str) -> None:
    """Timestamped, flushed trace line. No-op unless debugging is enabled.

    Format: [DBG +<elapsed>s HH:MM:SS] <msg> — the elapsed counter makes it
    obvious *how long* the last printed step has been running when it stalls.
    """
    if not _enabled:
        return
    elapsed = time.monotonic() - _t0
    wall = datetime.now().strftime("%H:%M:%S")
    print(f"[DBG +{elapsed:7.1f}s {wall}] {msg}", flush=True)


def enable_telethon_logging(level: int = logging.INFO) -> None:
    """Route Telethon's logger to stderr. At INFO this already shows the
    'Sleeping for Ns (...) on <Request>' lines that explain a silent stall;
    DEBUG additionally dumps every MTProto request/response."""
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("[telethon %(levelname)s] %(name)s: %(message)s"))
    tl = logging.getLogger("telethon")
    tl.setLevel(level)
    tl.addHandler(handler)
