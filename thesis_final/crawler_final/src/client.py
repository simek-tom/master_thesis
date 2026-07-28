"""Thin wrapper around telethon.TelegramClient bound to one account record."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from telethon import TelegramClient

from .config import DEFAULT_DAILY_CAP, DEVICE, SESSIONS_DIR
from .debug import dlog


@dataclass
class AccountRecord:
    """One entry from config/accounts.json."""

    api_id: int
    api_hash: str
    phone: str
    session_name: str
    proxy: dict | None = None  # {"host", "port", "user", "pass"} or None
    healthy: bool = True
    note: str = ""
    # Quota + cooldown + age. All optional — older accounts.json files
    # backfill with defaults on load.
    created_at: str | None = None          # ISO 8601 UTC, stamped by verify_account.py
    daily_cap: int = DEFAULT_DAILY_CAP     # operator raises to 200 once the account is aged
    scrapes_today: int = 0                 # get_entity attempts in the current 24h window
    day_started_at: str | None = None      # ISO 8601 UTC; rolls when older than 24h
    cooldown_until: str | None = None      # ISO 8601 UTC; account skipped while in the future
    last_flood_at: str | None = None       # informational — helps spot repeat floods
    # Device fingerprint sent during the MTProto handshake. None means "use the
    # DEVICE default from config.py"; set per-account to diversify the pool.
    # Dict with keys: device_model, system_version, app_version, lang_code, system_lang_code.
    device: dict | None = None

    @classmethod
    def from_dict(cls, d: dict) -> "AccountRecord":
        return cls(
            api_id=int(d["api_id"]),
            api_hash=d["api_hash"],
            phone=d["phone"],
            session_name=d["session_name"],
            proxy=d.get("proxy"),
            healthy=d.get("healthy", True),
            note=d.get("note", ""),
            created_at=d.get("created_at"),
            daily_cap=int(d.get("daily_cap", DEFAULT_DAILY_CAP)),
            scrapes_today=int(d.get("scrapes_today", 0)),
            day_started_at=d.get("day_started_at"),
            cooldown_until=d.get("cooldown_until"),
            last_flood_at=d.get("last_flood_at"),
            device=d.get("device"),
        )

    def to_dict(self) -> dict:
        return {
            "api_id": self.api_id,
            "api_hash": self.api_hash,
            "phone": self.phone,
            "session_name": self.session_name,
            "proxy": self.proxy,
            "healthy": self.healthy,
            "note": self.note,
            "created_at": self.created_at,
            "daily_cap": self.daily_cap,
            "scrapes_today": self.scrapes_today,
            "day_started_at": self.day_started_at,
            "cooldown_until": self.cooldown_until,
            "last_flood_at": self.last_flood_at,
            "device": self.device,
        }


def _proxy_tuple(proxy: dict | None) -> tuple | None:
    if proxy is None:
        return None
    # Position 4 (True) = rdns — DNS queries also go through the proxy.
    return (
        "socks5",
        proxy["host"],
        int(proxy["port"]),
        True,
        proxy.get("user", ""),
        proxy.get("pass", ""),
    )


def build_client(account: AccountRecord) -> TelegramClient:
    """Construct (but do not connect) a TelegramClient for *account*."""
    SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
    session_path = str(SESSIONS_DIR / f"{account.session_name}.session")
    device = account.device if account.device else DEVICE
    return TelegramClient(
        session_path,
        account.api_id,
        account.api_hash,
        proxy=_proxy_tuple(account.proxy),
        request_retries=3,  # Telethon default is 5; reduce so we get TimeoutError sooner
        **device,
    )


async def connect(client: TelegramClient, account: AccountRecord) -> None:
    """Connect + authenticate. Prompts for SMS code / 2FA on stdin if needed."""
    dlog(f"connect: starting client.start() for '{account.session_name}' ({account.phone})")
    await client.start(phone=account.phone)
    dlog(f"connect: '{account.session_name}' connected + authorized")


async def disconnect(client: TelegramClient) -> None:
    maybe = client.disconnect()
    if maybe is not None:  # telethon returns a coroutine for disconnect()
        await maybe
