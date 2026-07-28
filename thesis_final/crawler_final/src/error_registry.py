"""
error_registry.py — cross-round log of handles that fail to resolve.

Separate file from channel_registry.json so the live resolved-channel data is
never touched by error-path code. One flat JSON at data/error_registry.json
with one entry per unresolvable handle.

Used by round_runner's pre-filter: a handle already in this registry is marked
`skipped` without spending an API call, and its `attempt_count` is bumped so
we can tell honest typos (count stays at 1) from handles that keep showing up
across rounds (count grows — signal that the channel likely existed).

Serial write model, matching registry.py.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from .config import DATA_DIR

ERROR_REGISTRY_PATH = DATA_DIR / "error_registry.json"


def load() -> list[dict]:
    if not ERROR_REGISTRY_PATH.exists():
        return []
    with open(ERROR_REGISTRY_PATH, encoding="utf-8") as fh:
        return json.load(fh)


def _save(records: list[dict]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp = ERROR_REGISTRY_PATH.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(records, fh, ensure_ascii=False, indent=2)
    tmp.replace(ERROR_REGISTRY_PATH)


def known_handles() -> set[str]:
    """All handles that previously failed to resolve, lowercased."""
    return {r["handle"] for r in load()}


def bulk_upsert(entries: list[tuple[str, int, str, str | None]]) -> dict[str, dict]:
    """Upsert multiple handles in a single read+write cycle.

    entries: list of (handle, step, tag, error_or_None) — same semantics as upsert().
    Returns: {handle_lc -> record} for every entry processed.
    """
    if not entries:
        return {}
    records = load()
    by_handle = {r["handle"]: r for r in records}
    now = datetime.now(tz=timezone.utc).isoformat()
    result: dict[str, dict] = {}

    for handle, step, tag, error in entries:
        handle_lc = handle.lower()
        if handle_lc in by_handle:
            r = by_handle[handle_lc]
            r["attempt_count"] += 1
            if tag not in r["seen_in_tags"]:
                r["seen_in_tags"].append(tag)
            r["last_attempt_at"] = now
            if error is not None:
                r["last_error"] = error
        else:
            r = {
                "handle": handle_lc,
                "first_seen_step": step,
                "first_seen_tag": tag,
                "seen_in_tags": [tag],
                "attempt_count": 1,
                "last_attempt_at": now,
                "last_error": error,
            }
            records.append(r)
            by_handle[handle_lc] = r
        result[handle_lc] = r

    _save(records)
    return result


def upsert(handle: str, step: int, tag: str, error: str | None) -> dict:
    """Record that *handle* was encountered and (tried or pre-filtered) unresolvable.

    If *handle* is already logged: bump `attempt_count`, add *tag* to
    `seen_in_tags` if new, update `last_attempt_at`, overwrite `last_error`
    only when *error* is non-None (pre-filter skips pass error=None so we
    don't clobber the real message with a placeholder).

    Returns the updated (or newly created) record.
    """
    records = load()
    handle_lc = handle.lower()
    now = datetime.now(tz=timezone.utc).isoformat()

    for r in records:
        if r["handle"] == handle_lc:
            r["attempt_count"] += 1
            if tag not in r["seen_in_tags"]:
                r["seen_in_tags"].append(tag)
            r["last_attempt_at"] = now
            if error is not None:
                r["last_error"] = error
            _save(records)
            return r

    record = {
        "handle": handle_lc,
        "first_seen_step": step,
        "first_seen_tag": tag,
        "seen_in_tags": [tag],
        "attempt_count": 1,
        "last_attempt_at": now,
        "last_error": error,
    }
    records.append(record)
    _save(records)
    return record
