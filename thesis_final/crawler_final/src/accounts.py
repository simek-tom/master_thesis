"""AccountPool — load accounts, round-robin at channel granularity, mark unhealthy."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from itertools import cycle
from typing import Iterator

from telethon import TelegramClient

from .client import AccountRecord, build_client, connect, disconnect
from .config import ACCOUNTS_FILE, CONFIG_DIR, DAILY_WINDOW_SECONDS


class AccountPool:
    """Owns N account records and the live TelegramClient for each.

    Usage:
        pool = AccountPool.load()
        await pool.connect_all()
        client = pool.next_client()      # one per channel
        ...
        pool.mark_unhealthy(client, "PeerFloodError")
        await pool.disconnect_all()
        pool.save()
    """

    def __init__(self, accounts: list[AccountRecord]) -> None:
        if not accounts:
            raise ValueError("AccountPool requires at least one account. Run scripts/verify_account.py.")
        self._accounts = accounts
        self._clients: dict[str, TelegramClient] = {}  # session_name → client
        self._rotation: Iterator[AccountRecord] | None = None

    # --- persistence ------------------------------------------------------

    @classmethod
    def load(cls) -> "AccountPool":
        if not ACCOUNTS_FILE.exists():
            raise FileNotFoundError(
                f"{ACCOUNTS_FILE} not found. Run scripts/verify_account.py to add one."
            )
        with open(ACCOUNTS_FILE, encoding="utf-8") as fh:
            raw = json.load(fh)
        return cls([AccountRecord.from_dict(d) for d in raw])

    def save(self) -> None:
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        with open(ACCOUNTS_FILE, "w", encoding="utf-8") as fh:
            json.dump([a.to_dict() for a in self._accounts], fh, ensure_ascii=False, indent=2)

    # --- connection -------------------------------------------------------

    async def connect_all(self, only_sessions: list[str] | None = None) -> None:
        """Connect every healthy account, or only those whose session_name is
        in *only_sessions*. Unknown names raise immediately so a typo doesn't
        silently fall through to a zero-account run."""
        if only_sessions is not None:
            known = {a.session_name for a in self._accounts}
            missing = [s for s in only_sessions if s not in known]
            if missing:
                raise ValueError(
                    f"Unknown account session_name(s): {missing}. "
                    f"Available: {sorted(known)}"
                )
            wanted = set(only_sessions)
        else:
            wanted = None

        for account in self._healthy_accounts():
            if wanted is not None and account.session_name not in wanted:
                continue
            client = build_client(account)
            await connect(client, account)
            self._clients[account.session_name] = client
        self._rebuild_rotation()

    async def disconnect_all(self) -> None:
        for client in self._clients.values():
            await disconnect(client)
        self._clients.clear()

    # --- rotation ---------------------------------------------------------

    def next_client(self) -> TelegramClient:
        """Return the next available client in round-robin order.

        "Available" = healthy, connected, not cooling down, and under today's
        daily cap (with lazy rollover of the 24h window).
        """
        if self._rotation is None:
            raise RuntimeError("AccountPool.connect_all() must be called before next_client().")
        for _ in range(len(self._accounts)):  # bounded to avoid infinite loop
            account = next(self._rotation)
            if self._is_available(account):
                return self._clients[account.session_name]
        raise RuntimeError(
            "No accounts currently available — all are unhealthy, cooling down, "
            "or over today's cap. Caller should check available_count() first."
        )

    def mark_unhealthy(self, client: TelegramClient, reason: str) -> None:
        """Flag the account backing *client* as unhealthy and remove it from rotation."""
        for account in self._accounts:
            if self._clients.get(account.session_name) is client:
                account.healthy = False
                account.note = reason
                break
        self._rebuild_rotation()

    def healthy_count(self) -> int:
        return sum(1 for a in self._accounts if a.healthy and a.session_name in self._clients)

    def available_count(self) -> int:
        """Accounts that can take a call right now — healthy, connected, not on cooldown, under cap."""
        return sum(1 for a in self._accounts if self._is_available(a))

    def account_for_client(self, client: TelegramClient) -> AccountRecord | None:
        for account in self._accounts:
            if self._clients.get(account.session_name) is client:
                return account
        return None

    # --- quota + cooldown -------------------------------------------------

    def record_scrape_attempt(self, client: TelegramClient) -> None:
        """Count one ResolveUsername against the account's daily budget.

        Called once per scrape attempt — success, FloodWait, or unresolved
        handle all burn the same resolve call server-side. Lazily rolls the
        24h window if the stored `day_started_at` is older than that.

        If this attempt tips the counter to the daily cap, we re-anchor
        `day_started_at` to *now*, so the 24h rollover is measured from the
        moment the cap was hit — not from the window's first scrape. Without
        this, an account that burned its cap in the first hour of yesterday's
        window would unblock at the same hour today (~23h after cap), which
        is shorter than the 24h cooldown we mean to enforce.
        """
        account = self.account_for_client(client)
        if account is None:
            return
        self._roll_daily_counter(account)
        account.scrapes_today += 1
        if account.scrapes_today >= account.daily_cap:
            account.day_started_at = datetime.now(tz=timezone.utc).isoformat()

    def set_cooldown(self, client: TelegramClient, seconds: int) -> None:
        """Park an account until now+seconds (used when a FloodWait escapes Telethon's auto-sleep)."""
        account = self.account_for_client(client)
        if account is None:
            return
        now = datetime.now(tz=timezone.utc)
        account.cooldown_until = (now + timedelta(seconds=seconds)).isoformat()
        account.last_flood_at = now.isoformat()
        self._rebuild_rotation()

    # --- internals --------------------------------------------------------

    def _healthy_accounts(self) -> list[AccountRecord]:
        return [a for a in self._accounts if a.healthy]

    def _rebuild_rotation(self) -> None:
        # Rotation is built over all connected + healthy accounts. Cooldown
        # and daily-cap are applied in next_client()/available_count() per
        # call, because their state changes as the clock advances.
        live = [a for a in self._accounts if a.healthy and a.session_name in self._clients]
        self._rotation = cycle(live) if live else None

    def _is_available(self, account: AccountRecord) -> bool:
        if not account.healthy or account.session_name not in self._clients:
            return False
        now = datetime.now(tz=timezone.utc)
        if account.cooldown_until is not None:
            try:
                if datetime.fromisoformat(account.cooldown_until) > now:
                    return False
            except ValueError:
                pass  # malformed timestamp — treat as expired
        self._roll_daily_counter(account, _now=now)
        if account.scrapes_today >= account.daily_cap:
            return False
        return True

    def _roll_daily_counter(self, account: AccountRecord, _now: datetime | None = None) -> None:
        """Reset scrapes_today to 0 if the stored window is older than DAILY_WINDOW_SECONDS."""
        now = _now or datetime.now(tz=timezone.utc)
        if account.day_started_at is None:
            account.day_started_at = now.isoformat()
            account.scrapes_today = 0
            return
        try:
            started = datetime.fromisoformat(account.day_started_at)
        except ValueError:
            account.day_started_at = now.isoformat()
            account.scrapes_today = 0
            return
        if (now - started).total_seconds() >= DAILY_WINDOW_SECONDS:
            account.day_started_at = now.isoformat()
            account.scrapes_today = 0
