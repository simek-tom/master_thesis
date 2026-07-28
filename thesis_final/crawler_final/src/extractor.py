"""
extractor.py — five mention-detection methods, returning per-message evidence.

Lifted from spike-prototype/scripts/explore.py. Pure functions, no Telethon
dependency at call time (entities are a plain list of objects; we inspect
type().__name__ and offset/length attributes).

Evidence schema:
    {
        "method": "regex_mention" | "regex_tme" | "entity_mention" | "entity_url",
        "handle": str,         # lowercased
        "msg_id": int,
        "snippet": str,        # ~±30 chars of surrounding text
    }

Forward-source detection returns a raw channel_id (int), not a handle; the
notebook resolves those (or leaves them as IDs).
"""

from __future__ import annotations

import re
from typing import TypedDict

RE_MENTION = re.compile(r"@([A-Za-z][A-Za-z0-9_]{4,31})(?=[^A-Za-z0-9_]|$)")
RE_TME = re.compile(
    r"(?:https?://)?(?:t\.me|telegram\.me)/([A-Za-z][A-Za-z0-9_]{4,31})",
    re.IGNORECASE,
)


class Evidence(TypedDict):
    method: str
    handle: str
    msg_id: int
    snippet: str


def _utf16_slice(text: str, offset: int, length: int) -> str:
    """Extract a substring using Telegram's UTF-16 offset/length encoding."""
    utf16 = text.encode("utf-16-le")
    return utf16[offset * 2 : (offset + length) * 2].decode("utf-16-le", errors="replace")


def _window(text: str, start: int, end: int, pad: int = 30) -> str:
    """Return a snippet of text with ±pad chars, whitespace-collapsed."""
    s = max(0, start - pad)
    e = min(len(text), end + pad)
    prefix = "…" if s > 0 else ""
    suffix = "…" if e < len(text) else ""
    return prefix + re.sub(r"\s+", " ", text[s:e]).strip() + suffix


def extract_regex_mentions(text: str, msg_id: int) -> list[Evidence]:
    out: list[Evidence] = []
    for m in RE_MENTION.finditer(text):
        out.append(Evidence(
            method="regex_mention",
            handle=m.group(1).lower(),
            msg_id=msg_id,
            snippet=_window(text, m.start(), m.end()),
        ))
    return out


def extract_regex_tme(text: str, msg_id: int) -> list[Evidence]:
    out: list[Evidence] = []
    for m in RE_TME.finditer(text):
        out.append(Evidence(
            method="regex_tme",
            handle=m.group(1).lower(),
            msg_id=msg_id,
            snippet=_window(text, m.start(), m.end()),
        ))
    return out


def extract_entity_mentions(text: str, entities: list, msg_id: int) -> list[Evidence]:
    """MessageEntityMention — server-parsed, UTF-16-accurate."""
    out: list[Evidence] = []
    for e in entities:
        if type(e).__name__ != "MessageEntityMention":
            continue
        raw = _utf16_slice(text, e.offset, e.length)
        handle = raw.lstrip("@").lower()
        if not handle:
            continue
        idx = text.find(raw)
        snippet = _window(text, idx, idx + len(raw)) if idx >= 0 else raw
        out.append(Evidence(method="entity_mention", handle=handle, msg_id=msg_id, snippet=snippet))
    return out


def extract_entity_urls(text: str, entities: list, msg_id: int) -> list[Evidence]:
    """t.me/<h> links discovered via MessageEntityUrl / MessageEntityTextUrl."""
    out: list[Evidence] = []
    for e in entities:
        name = type(e).__name__
        if name == "MessageEntityUrl":
            url = _utf16_slice(text, e.offset, e.length)
        elif name == "MessageEntityTextUrl":
            url = getattr(e, "url", "") or ""
        else:
            continue
        m = RE_TME.search(url)
        if m:
            out.append(Evidence(
                method="entity_url",
                handle=m.group(1).lower(),
                msg_id=msg_id,
                snippet=url,
            ))
    return out


def extract_forward_source(msg) -> int | None:
    """Numeric channel_id of a forwarded-from channel, if present."""
    if msg.fwd_from and hasattr(getattr(msg.fwd_from, "from_id", None), "channel_id"):
        return msg.fwd_from.from_id.channel_id
    return None


def extract_all(text: str, entities: list, msg_id: int) -> list[Evidence]:
    """Run all four text-based methods on one message."""
    out: list[Evidence] = []
    out.extend(extract_regex_mentions(text, msg_id))
    out.extend(extract_regex_tme(text, msg_id))
    out.extend(extract_entity_mentions(text, entities, msg_id))
    out.extend(extract_entity_urls(text, entities, msg_id))
    return out
