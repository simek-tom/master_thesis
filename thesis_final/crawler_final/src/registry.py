"""
registry.py — cross-round record of every channel we've scraped.

Schema (v2): each record carries a `discoveries` list tracking every (tag, step)
pair that reached this channel — whether it was actually scraped there or just
encountered during pre-filtering in a different seed track:

    {
      "channel_id": 123,
      "handles": ["handle"],
      "title": "...",
      "discoveries": [
        {"tag": "cz", "step": 1},
        {"tag": "sk", "step": 1}   ← added even when sk skips this channel
      ],
      "last_scraped_step": 1,
      "last_scraped_tag": "cz",
      "last_scraped_at": "2023-01-01T00:00:00+00:00"
    }

v1 records (first_seen_tag / first_seen_step fields) are migrated on load.

Serial write model — the round runner processes channels one at a time, so
there's no concurrency to worry about.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from .config import DATA_DIR

REGISTRY_PATH = DATA_DIR / "channel_registry.json"


def _migrate(r: dict) -> dict:
    """In-place: convert v1 first_seen_tag/first_seen_step to discoveries list."""
    if "discoveries" not in r:
        tag = r.pop("first_seen_tag", None)
        step = r.pop("first_seen_step", None)
        r["discoveries"] = [{"tag": tag, "step": step}] if tag is not None else []
    return r


def load() -> list[dict]:
    if not REGISTRY_PATH.exists():
        return []
    with open(REGISTRY_PATH, encoding="utf-8") as fh:
        records = json.load(fh)
    return [_migrate(r) for r in records]


def _save(records: list[dict]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp = REGISTRY_PATH.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(records, fh, ensure_ascii=False, indent=2)
    tmp.replace(REGISTRY_PATH)


def known_handles() -> set[str]:
    """All handles ever scraped, lowercased. Used by the notebook export filter."""
    return {h.lower() for r in load() for h in r.get("handles", [])}


def upsert(channel_id: int, handle: str, title: str, step: int, tag: str) -> None:
    """Record that we scraped *channel_id* (reached via *handle*) in step/*tag*.

    If *channel_id* is already in the registry, appends *handle* if new (catches
    renames) and adds (tag, step) to discoveries if not already present.
    """
    records = load()
    handle_lc = handle.lower()
    now = datetime.now(tz=timezone.utc).isoformat()

    for r in records:
        if r["channel_id"] == channel_id:
            existing_handles = {h.lower() for h in r["handles"]}
            if handle_lc not in existing_handles:
                r["handles"].append(handle_lc)
            r["title"] = title
            existing_disc = {(d["tag"], d["step"]) for d in r.get("discoveries", [])}
            if (tag, step) not in existing_disc:
                r.setdefault("discoveries", []).append({"tag": tag, "step": step})
            r["last_scraped_step"] = step
            r["last_scraped_tag"] = tag
            r["last_scraped_at"] = now
            _save(records)
            return

    records.append({
        "channel_id": channel_id,
        "handles": [handle_lc],
        "title": title,
        "discoveries": [{"tag": tag, "step": step}],
        "last_scraped_step": step,
        "last_scraped_tag": tag,
        "last_scraped_at": now,
    })
    _save(records)


def bulk_record_discoveries(entries: list[tuple[str, int, str]]) -> None:
    """Record that handles were reached by (tag, step) during pre-filtering.

    Called by round_runner when a handle is already in the registry and gets
    marked `skipped`. Even though no scrape happens, the reaching seed track
    gets credited as a discoverer so multi-track attribution is preserved.

    entries: list of (handle, step, tag).
    Handles not found in the registry are silently ignored.
    """
    if not entries:
        return
    records = load()
    by_handle: dict[str, dict] = {}
    for r in records:
        for h in r.get("handles", []):
            by_handle[h.lower()] = r

    changed = False
    for handle, step, tag in entries:
        r = by_handle.get(handle.lower())
        if r is None:
            continue
        existing = {(d["tag"], d["step"]) for d in r.get("discoveries", [])}
        if (tag, step) not in existing:
            r.setdefault("discoveries", []).append({"tag": tag, "step": step})
            changed = True

    if changed:
        _save(records)
