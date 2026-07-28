"""
scraper.py — scrape one channel end-to-end for a given date window.

Writes into the step folder:
    <step_dir>/scraped/<handle>.pkl           raw Telethon messages (pickled)
    <step_dir>/scraped/<handle>_inspect.json  lossy JSON for notebook analysis
    <step_dir>/scraped/<handle>_meta.json     per-channel metadata

Network errors and FloodWaits are NOT handled here. The caller (round_runner)
catches Telethon exceptions and decides whether to pause, retry, or mark the
channel as hard-failed.
"""

from __future__ import annotations

import json
import pickle
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from telethon import TelegramClient

from . import registry
from .debug import dlog


# ---------------------------------------------------------------------------
# Pickle-safety helpers
# ---------------------------------------------------------------------------


def _detach_client(obj: Any, _seen: set[int] | None = None) -> None:
    """Recursively null out _client on Telethon TLObjects so they pickle cleanly.

    Telethon attaches a back-reference to the live TelegramClient on Message,
    Forward, and other wrapper objects. That client holds an unpicklable local
    class (_Loggers). Walking __dict__ and nulling _client wherever it appears
    is the safest way to strip it without losing any message data.
    """
    if _seen is None:
        _seen = set()
    if id(obj) in _seen or not hasattr(obj, "__dict__"):
        return
    _seen.add(id(obj))
    if "_client" in obj.__dict__:
        obj._client = None
    for v in obj.__dict__.values():
        if hasattr(v, "__dict__"):
            _detach_client(v, _seen)


# ---------------------------------------------------------------------------
# Inspect-row builder (lossy JSON shape for notebooks)
# ---------------------------------------------------------------------------


def _entity_to_dict(e) -> dict:
    d = {
        "type": type(e).__name__,
        "offset": getattr(e, "offset", None),
        "length": getattr(e, "length", None),
    }
    if hasattr(e, "url") and e.url is not None:
        d["url"] = e.url
    if hasattr(e, "user_id") and e.user_id is not None:
        d["user_id"] = e.user_id
    if hasattr(e, "language") and e.language:
        d["language"] = e.language
    return d


def _fwd_from_to_dict(fwd) -> dict | None:
    if fwd is None:
        return None
    source_channel_id = None
    from_id = getattr(fwd, "from_id", None)
    if from_id is not None:
        source_channel_id = getattr(from_id, "channel_id", None)
    return {
        "source_channel_id": source_channel_id,
        "source_msg_id": getattr(fwd, "channel_post", None),
        "source_date": fwd.date.isoformat() if fwd.date else None,
        "source_author": getattr(fwd, "post_author", None),
    }


def _reactions_to_dict(reactions) -> dict:
    out = {}
    if reactions is None:
        return out
    for rc in reactions.results:
        emoji = getattr(rc.reaction, "emoticon", None)
        if emoji:
            out[emoji] = rc.count
    return out


def _message_to_inspect_row(msg) -> dict:
    channel_id = None
    if msg.peer_id is not None:
        channel_id = getattr(msg.peer_id, "channel_id", None)
    return {
        "id": msg.id,
        "date": msg.date.isoformat(),
        "edit_date": msg.edit_date.isoformat() if msg.edit_date else None,
        "channel_id": channel_id,
        "message": msg.message,
        "views": msg.views,
        "forwards": msg.forwards,
        "replies_count": msg.replies.replies if msg.replies else None,
        "has_media": msg.media is not None,
        "media_type": type(msg.media).__name__ if msg.media else None,
        "grouped_id": msg.grouped_id,
        "post_author": msg.post_author,
        "pinned": msg.pinned,
        "noforwards": msg.noforwards,
        "reactions_json": _reactions_to_dict(msg.reactions),
        "entities_json": [_entity_to_dict(e) for e in (msg.entities or [])],
        "fwd_from": _fwd_from_to_dict(msg.fwd_from),
    }


# ---------------------------------------------------------------------------
# Scrape result
# ---------------------------------------------------------------------------


@dataclass
class ScrapeResult:
    handle: str
    channel_id: int
    title: str
    username: str | None
    count: int
    actual_first_date: str
    actual_last_date: str
    duration_seconds: float


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


async def scrape_channel(
    client: TelegramClient,
    handle: str,
    date_start: datetime,
    date_end: datetime,
    step_dir: Path,
    account_session: str,
    step_num: int,
    tag: str,
) -> ScrapeResult:
    """Pull messages for *handle* in [date_start, date_end) and dump to step_dir/scraped/.

    Raises any Telethon exception (FloodWait, PeerFloodError, etc.) up to the
    caller — this function does not decide what to do about rate limits.

    Channel-specific hard errors (ChannelPrivateError, UsernameNotOccupiedError,
    ChannelInvalidError) are also propagated; round_runner marks the channel as
    `failed` in step_meta.json in that case.
    """
    started_at = datetime.now(tz=timezone.utc)

    dlog(f"scrape @{handle}: get_entity() …")
    entity = await client.get_entity(handle)
    channel_id = entity.id
    title = getattr(entity, "title", handle)
    username = getattr(entity, "username", handle)
    dlog(f"scrape @{handle}: resolved id={channel_id} title={title!r} — iter_messages() …")

    messages: list = []
    last_beat = time.monotonic()
    async for msg in client.iter_messages(entity, offset_date=date_end, reverse=False):
        if msg.date.replace(tzinfo=timezone.utc) < date_start:
            break
        messages.append(msg)
        # Heartbeat: a long quiet stretch here = Telethon paging slowly or
        # silently flood-sleeping, NOT a true hang.
        now = time.monotonic()
        if now - last_beat >= 5.0:
            dlog(f"scrape @{handle}: {len(messages)} msgs so far "
                 f"(last date {msg.date.isoformat()})")
            last_beat = now
    dlog(f"scrape @{handle}: iter_messages done — {len(messages)} msgs in window, writing files")

    scraped_dir = step_dir / "scraped"
    scraped_dir.mkdir(parents=True, exist_ok=True)

    # Always write the three files — even if messages is empty — so downstream
    # tooling can distinguish "scrape ran and found nothing" from "scrape never
    # ran".
    actual_first = min((m.date for m in messages), default=date_start)
    actual_last = max((m.date for m in messages), default=date_start)

    # --- pickle -----------------------------------------------------------
    for msg in messages:
        _detach_client(msg)
    with open(scraped_dir / f"{handle}.pkl", "wb") as fh:
        pickle.dump(messages, fh, protocol=pickle.HIGHEST_PROTOCOL)

    # --- inspect JSON -----------------------------------------------------
    rows = [_message_to_inspect_row(m) for m in messages]
    with open(scraped_dir / f"{handle}_inspect.json", "w", encoding="utf-8") as fh:
        json.dump(rows, fh, ensure_ascii=False, indent=2)

    # --- meta -------------------------------------------------------------
    finished_at = datetime.now(tz=timezone.utc)
    duration = (finished_at - started_at).total_seconds()
    meta = {
        "handle": handle,
        "channel_id": channel_id,
        "title": title,
        "username": username,
        "requested_start": date_start.isoformat(),
        "requested_end": date_end.isoformat(),
        "actual_first_date": actual_first.isoformat() if messages else None,
        "actual_last_date": actual_last.isoformat() if messages else None,
        "count": len(messages),
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_seconds": duration,
        "scraped_by_session": account_session,
    }
    with open(scraped_dir / f"{handle}_meta.json", "w", encoding="utf-8") as fh:
        json.dump(meta, fh, ensure_ascii=False, indent=2)

    registry.upsert(channel_id=channel_id, handle=handle, title=title, step=step_num, tag=tag)
    dlog(f"scrape @{handle}: files written, registry updated, duration={duration:.1f}s")

    return ScrapeResult(
        handle=handle,
        channel_id=channel_id,
        title=title,
        username=username,
        count=len(messages),
        actual_first_date=meta["actual_first_date"] or "",
        actual_last_date=meta["actual_last_date"] or "",
        duration_seconds=duration,
    )
