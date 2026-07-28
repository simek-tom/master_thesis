"""Paths and shared constants.

Runtime state (sessions, data/, config/accounts.json) lives inside the repo at
ROOT/{sessions,data,config} — all three are gitignored. Override with
CRAWLER_RUNTIME_DIR if you need to point elsewhere (e.g. an external drive).
"""

import os
from pathlib import Path

# Code path — always the repo checkout, regardless of where the user cloned it.
ROOT = Path(__file__).parent.parent  # crawler/

# Runtime path — where the crawler reads + writes during a run.
_runtime_env = os.environ.get("CRAWLER_RUNTIME_DIR")
RUNTIME_DIR = Path(_runtime_env).expanduser() if _runtime_env else ROOT

CONFIG_DIR = RUNTIME_DIR / "config"
ACCOUNTS_FILE = CONFIG_DIR / "accounts.json"
SESSIONS_DIR = RUNTIME_DIR / "sessions"
DATA_DIR = RUNTIME_DIR / "data"

# Device fingerprint sent during TDC handshake — mimic a mid-range Android phone.
DEVICE = {
    "device_model": "Samsung Galaxy S21",
    "system_version": "Android 13",
    "app_version": "10.3.1",
    "lang_code": "en",
    "system_lang_code": "en-US",
}

# Default rolling window for a round, in days. Overridable per round via CLI.
DEFAULT_WINDOW_DAYS = 30

# Short FloodWait threshold — below this we sleep once and retry the same channel.
# At/above this we stop the round and wait for human input.
SHORT_FLOODWAIT_SECONDS = 60

# Per-account daily ResolveUsername budget. Telegram's per-account ceiling is
# ~200/day for an aged account and noticeably less for fresh ones. We default
# to 75 — operator raises individual accounts to 200 by editing accounts.json
# once they've proven stable. The window is a rolling 24h per account.
DEFAULT_DAILY_CAP = 75
DAILY_WINDOW_SECONDS = 24 * 60 * 60
