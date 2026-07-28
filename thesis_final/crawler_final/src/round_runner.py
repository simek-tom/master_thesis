"""
round_runner.py — batch driver for one round (step_num, tag) pair.

Reads data/step_NN_<tag>_<start>_<end>/input_handles.json, scrapes each handle
using AccountPool (round-robin), maintains step_meta.json with pending/
in_progress/done/failed/skipped status for crash recovery, and pauses for
human input on any system-level rate-limit signal.

Before scraping, the runner filters input_handles.json against the shared
channel_registry.json: handles already recorded there are marked `skipped`
with note `already in registry`, so cross-track seed overlap never causes
double-scrapes.

The rules:
- FloodWait < 60s → handled silently by Telethon (default flood_sleep_threshold=60).
- FloodWait ≥ 60s → put that account on a cooldown until the flood expires and
  keep going on the next available account. Only pause if *all* accounts become
  unavailable (unhealthy, cooling down, or over the daily cap).
- PeerFloodError / UserDeactivatedBanError → mark account unhealthy, pause if
  it was the last healthy one.
- Hard channel errors (ChannelPrivate, UsernameNotOccupied, UsernameInvalid,
  ChannelInvalid) → mark that one channel `failed` and continue. Unresolved
  handles also go into error_registry so we don't waste another API call on
  them next round.
- "Pause" = print event, wait on stdin; operator types `continue` to retry
  or Ctrl-C to exit cleanly.
"""

from __future__ import annotations

import asyncio
import json
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from telethon.errors import (
    ChannelInvalidError,
    ChannelPrivateError,
    FloodWaitError,
    PeerFloodError,
    UserDeactivatedBanError,
    UsernameInvalidError,
    UsernameNotOccupiedError,
)

from . import error_registry, registry
from .accounts import AccountPool
from .client import AccountRecord
from .config import DAILY_WINDOW_SECONDS, DATA_DIR
from .debug import dlog
from .scraper import scrape_channel


_TIMEOUT_RETRY_LIMIT = 2  # per-channel retries on TimeoutError before marking failed

HARD_CHANNEL_ERRORS = (
    ChannelPrivateError,
    UsernameNotOccupiedError,
    UsernameInvalidError,
    ChannelInvalidError,
)
UNRESOLVED_HANDLE_ERRORS = (UsernameNotOccupiedError, UsernameInvalidError)
ACCOUNT_BAN_ERRORS = (PeerFloodError, UserDeactivatedBanError)


# ---------------------------------------------------------------------------
# Step folder discovery + metadata
# ---------------------------------------------------------------------------


def find_step_dir(step_num: int, tag: str) -> Path:
    """Return the unique data/step_NN_<tag>_*/ folder, or raise."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    prefix = f"step_{step_num:02d}_{tag}_"
    matches = sorted(p for p in DATA_DIR.iterdir() if p.is_dir() and p.name.startswith(prefix))
    if not matches:
        raise FileNotFoundError(
            f"No folder matching {prefix}* under {DATA_DIR}. "
            f"Create data/step_{step_num:02d}_{tag}_<start>_<end>/input_handles.json first."
        )
    if len(matches) > 1:
        raise RuntimeError(f"Multiple folders match {prefix}*: {matches}")
    return matches[0]


def _parse_window_from_name(step_dir: Path) -> tuple[datetime, datetime]:
    """step_NN_<tag>_YYYY-MM-DD_YYYY-MM-DD → (start, end) in UTC."""
    parts = step_dir.name.split("_")
    # Format: step_NN_<tag>_<start>_<end> → 5 parts
    if len(parts) != 5:
        raise ValueError(f"Cannot parse window from folder name: {step_dir.name}")
    start = datetime.strptime(parts[3], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = datetime.strptime(parts[4], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return start, end


def _load_input(step_dir: Path) -> list[dict]:
    path = step_dir / "input_handles.json"
    if not path.exists():
        raise FileNotFoundError(f"Missing {path}")
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def _make_handle_entry(entry: dict) -> dict:
    return {
        "handle": entry["handle"].lower(),
        "origin_channels": entry.get("origin_channels", []),
        "discovered_via": entry.get("discovered_via", []),
        "is_seed": entry.get("is_seed", False),
        "status": "pending",
        "error": None,
        "note": None,
        "scraped_by_session": None,
        "duration_seconds": None,
        "message_count": None,
    }


def _load_or_init_meta(
    step_dir: Path,
    input_entries: list[dict],
    window: tuple[datetime, datetime],
    tag: str,
) -> dict:
    """Load existing step_meta.json (merging any new input_handles entries), or
    build a fresh one from input_entries.

    Two merge behaviours on load:
    - New handles in input_handles.json → appended as pending.
    - Existing handles with is_seed=True that were previously skipped → reset to
      pending so they get re-scraped. This ensures seed channels always produce
      pkl files in the correct step folder regardless of registry state.
    """
    meta_path = step_dir / "step_meta.json"
    if meta_path.exists():
        with open(meta_path, encoding="utf-8") as fh:
            meta = json.load(fh)

        input_by_handle = {e["handle"].lower(): e for e in input_entries}
        known = {h["handle"] for h in meta["handles"]}
        changed = False

        for entry in meta["handles"]:
            inp = input_by_handle.get(entry["handle"])
            # Propagate is_seed from input onto existing meta entries
            if inp and inp.get("is_seed", False) and not entry.get("is_seed"):
                entry["is_seed"] = True
                changed = True
            # Seeds that were previously skipped need to be re-scraped
            if entry.get("is_seed") and entry["status"] == "skipped":
                entry["status"] = "pending"
                entry["note"] = None
                changed = True
                print(f"  [seed reset to pending: @{entry['handle']}]")

        new_entries = [
            _make_handle_entry(e)
            for e in input_entries
            if e["handle"].lower() not in known
        ]
        if new_entries:
            meta["handles"].extend(new_entries)
            changed = True
            print(f"[+{len(new_entries)} new handle(s) from input_handles.json added as pending]")

        if changed:
            _save_meta(step_dir, meta)
        return meta

    handles_meta = [_make_handle_entry(e) for e in input_entries]
    return {
        "step": int(step_dir.name.split("_")[1]),
        "tag": tag,
        "window_start": window[0].isoformat(),
        "window_end": window[1].isoformat(),
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
        "handles": handles_meta,
    }


def _save_meta(step_dir: Path, meta: dict) -> None:
    """Atomic write — temp file then rename."""
    meta_path = step_dir / "step_meta.json"
    tmp = meta_path.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(meta, fh, ensure_ascii=False, indent=2)
    tmp.replace(meta_path)


# ---------------------------------------------------------------------------
# Pause prompt + operator command channel
# ---------------------------------------------------------------------------


_cmd_queue: asyncio.Queue[str] | None = None


class _AbortRequested(Exception):
    """Raised when operator types 'abort' or 'kill' to stop the round gracefully."""


def _start_stdin_watcher(loop: asyncio.AbstractEventLoop) -> None:
    """Spawn a daemon thread that forwards stdin lines into _cmd_queue.

    Runs for the whole round so commands work both during active scraping
    (consumed between handles) and during a pause (awaited in _pause)."""
    global _cmd_queue
    _cmd_queue = asyncio.Queue()

    def watch() -> None:
        for line in sys.stdin:
            cmd = line.strip().lower()
            if not cmd:
                continue
            try:
                asyncio.run_coroutine_threadsafe(_cmd_queue.put(cmd), loop)
            except RuntimeError:
                return  # loop closed

    threading.Thread(target=watch, daemon=True, name="stdin-watcher").start()


async def _drain_for_abort() -> bool:
    """Non-blocking peek: True if operator typed 'abort' since last check.
    Unknown commands outside a pause have no meaning, so we drop them."""
    if _cmd_queue is None:
        return False
    abort = False
    while not _cmd_queue.empty():
        cmd = _cmd_queue.get_nowait()
        if cmd == "abort":
            abort = True
    return abort


async def _pause(reason: str) -> None:
    """Wait until operator types 'continue'; raise _AbortRequested on 'abort'."""
    print("\n" + "=" * 62)
    print(f"PAUSED: {reason}")
    print("Type 'continue' to retry the current handle,")
    print("     'abort' for graceful shutdown (summary + state saved),")
    print("     or Ctrl-C twice for a hard exit.")
    print("=" * 62)
    if _cmd_queue is None:
        raise RuntimeError("stdin watcher not initialized — call _start_stdin_watcher first")
    while True:
        cmd = await _cmd_queue.get()
        if cmd == "continue":
            return
        if cmd == "abort":
            raise _AbortRequested()
        print(f"  (unknown command '{cmd}' — expected 'continue' or 'abort')")


def _to_local(iso: str | None) -> str:
    """UTC ISO → operator-local wall clock for display. Storage stays UTC."""
    if not iso:
        return "—"
    try:
        return datetime.fromisoformat(iso).astimezone().strftime("%Y-%m-%d %H:%M %Z")
    except (ValueError, TypeError):
        return iso


def _fmt_duration(seconds: int) -> str:
    if seconds < 60:
        return f"{seconds}s"
    minutes, secs = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m {secs}s"
    hours, mins = divmod(minutes, 60)
    return f"{hours}h {mins}m"


def _dump_pool_state(pool: AccountPool) -> None:
    """Print each account's live state (health / flood cooldown / quota).
    Shared by the pause path and the end-of-run summary."""
    print("\n--- Account pool state ---")
    now = datetime.now(tz=timezone.utc)
    for account in pool._accounts:
        parts = [f"  {account.session_name:20s}"]
        if not account.healthy:
            parts.append(f"unhealthy ({account.note})")
        elif account.session_name not in pool._clients:
            parts.append("not connected")
        else:
            cd = account.cooldown_until
            if cd:
                try:
                    remaining = int((datetime.fromisoformat(cd) - now).total_seconds())
                    if remaining > 0:
                        parts.append(
                            f"flood cooldown {_fmt_duration(remaining)} remaining (until {_to_local(cd)})"
                        )
                except ValueError:
                    pass
            parts.append(f"scrapes_today={account.scrapes_today}/{account.daily_cap}")
        print(" ".join(parts))


async def _pause_with_pool_state(pool: AccountPool, reason: str) -> None:
    _dump_pool_state(pool)
    await _pause(reason)


# ---------------------------------------------------------------------------
# Per-handle scrape with the §11 rules
# ---------------------------------------------------------------------------


async def _scrape_with_rate_limit_rules(
    pool: AccountPool,
    handle: str,
    window: tuple[datetime, datetime],
    step_dir: Path,
    step_num: int,
    tag: str,
) -> tuple[str, str | None, dict[str, Any]]:
    """
    Try to scrape *handle* until it succeeds, or an unrecoverable event happens.

    Returns: (status, error_msg_or_None, extras)
      status ∈ {"done", "failed"}.
      "failed" is reserved for hard channel-specific errors only.

    Raises KeyboardInterrupt if the operator Ctrl-Cs out of a pause.
    """
    timeout_retries = 0
    while True:
        avail = pool.available_count()
        dlog(f"@{handle}: available_count={avail}")
        if avail == 0:
            await _pause_with_pool_state(pool, f"no accounts available to scrape @{handle}")
            continue

        client = pool.next_client()
        account = pool.account_for_client(client)
        assert account is not None
        dlog(f"@{handle}: picked account '{account.session_name}' "
             f"(scrapes_today={account.scrapes_today}/{account.daily_cap})")
        try:
            result = await scrape_channel(
                client=client,
                handle=handle,
                date_start=window[0],
                date_end=window[1],
                step_dir=step_dir,
                account_session=account.session_name,
                step_num=step_num,
                tag=tag,
            )
            pool.record_scrape_attempt(client)
            pool.save()
            return "done", None, {
                "scraped_by_session": account.session_name,
                "duration_seconds": result.duration_seconds,
                "message_count": result.count,
            }

        except FloodWaitError as exc:
            seconds = int(exc.seconds)
            # The resolve call was charged even though the scrape failed.
            pool.record_scrape_attempt(client)
            pool.set_cooldown(client, seconds)
            pool.save()
            print(f"  FloodWait {seconds}s on @{handle} via '{account.session_name}' "
                  f"— cooldown set, trying next account.")
            continue  # loop re-enters, picks a different account via next_client()

        except ACCOUNT_BAN_ERRORS as exc:
            pool.mark_unhealthy(client, f"{type(exc).__name__}: {exc}")
            pool.save()
            print(f"  Account '{account.session_name}' marked unhealthy: {type(exc).__name__}")
            continue

        except HARD_CHANNEL_ERRORS as exc:
            err = f"{type(exc).__name__}: {exc}"
            if isinstance(exc, UNRESOLVED_HANDLE_ERRORS):
                error_registry.upsert(handle, step_num, tag, err)
            pool.record_scrape_attempt(client)
            pool.save()
            return "failed", err, {}

        except ValueError as exc:
            # Telethon wraps server-side UsernameNotOccupiedError as a ValueError
            # when get_entity is called with a string handle. See
            # telethon/client/users.py::_get_entity_from_string.
            msg = str(exc)
            if "as username" in msg or "Cannot find any entity" in msg:
                err = f"ValueError: {msg}"
                error_registry.upsert(handle, step_num, tag, err)
                pool.record_scrape_attempt(client)
                pool.save()
                return "failed", err, {}
            raise

        except (TimeoutError, asyncio.TimeoutError) as exc:
            timeout_retries += 1
            pool.record_scrape_attempt(client)
            pool.save()
            if timeout_retries >= _TIMEOUT_RETRY_LIMIT:
                err = f"TimeoutError (gave up after {timeout_retries} retries): {exc}"
                return "failed", err, {}
            print(f"  Timeout on @{handle} via '{account.session_name}' "
                  f"({timeout_retries}/{_TIMEOUT_RETRY_LIMIT}) — retrying on next account.")
            continue


# ---------------------------------------------------------------------------
# Interactive account picker
# ---------------------------------------------------------------------------


def _availability_snapshot(account: AccountRecord, now: datetime) -> tuple[bool, str]:
    """Compute (available?, one-line status) from raw record fields.
    Mirrors AccountPool._is_available, but works without a live connection
    so it can run before connect_all()."""
    if not account.healthy:
        return False, f"unhealthy ({account.note or 'no reason'})"

    if account.cooldown_until:
        try:
            until = datetime.fromisoformat(account.cooldown_until)
            remaining = int((until - now).total_seconds())
            if remaining > 0:
                return False, f"flood cooldown {_fmt_duration(remaining)} remaining"
        except ValueError:
            pass

    started = None
    if account.day_started_at:
        try:
            started = datetime.fromisoformat(account.day_started_at)
        except ValueError:
            started = None
    if started is not None and (now - started).total_seconds() >= DAILY_WINDOW_SECONDS:
        return True, f"{account.daily_cap} left (window rolled, counter resets on use)"

    left = account.daily_cap - account.scrapes_today
    if left <= 0:
        return False, f"daily cap reached ({account.scrapes_today}/{account.daily_cap})"
    return True, f"{left}/{account.daily_cap} left"


def _pick_accounts_interactive(pool: AccountPool) -> list[str]:
    """Prompt the operator to choose which accounts to use this round.
    Returns the selected session_names. Blocked accounts are shown but
    cannot be picked."""
    now = datetime.now(tz=timezone.utc)
    print("\n=== Account picker ===")
    rows: list[tuple[int, AccountRecord, bool, str]] = []
    for i, account in enumerate(pool._accounts, start=1):
        available, status = _availability_snapshot(account, now)
        rows.append((i, account, available, status))
        marker = " " if available else "x"
        print(f"  [{i}] {marker} {account.session_name:24s}  {status}")

    available_idxs = [i for i, _, av, _ in rows if av]
    if not available_idxs:
        print("\nNo accounts have free bandwidth right now. "
              "Wait for cooldowns / quota rollover, or add another account.")
        raise SystemExit(1)

    default_hint = ",".join(str(i) for i in available_idxs)
    print(f"\nSelect accounts: comma-separated numbers (e.g. 1,3),")
    print(f"                 'all' or Enter to use every available one [{default_hint}]")
    while True:
        try:
            raw = input("> ").strip().lower()
        except EOFError:
            raise KeyboardInterrupt from None

        if not raw or raw == "all":
            return [a.session_name for _, a, av, _ in rows if av]

        try:
            picked = [int(p.strip()) for p in raw.split(",") if p.strip()]
        except ValueError:
            print("  not a list of numbers, try again")
            continue
        if not picked:
            print("  empty selection, try again")
            continue
        out_of_range = [p for p in picked if p < 1 or p > len(rows)]
        if out_of_range:
            print(f"  out of range: {out_of_range}")
            continue
        blocked = [rows[p - 1][1].session_name for p in picked if not rows[p - 1][2]]
        if blocked:
            print(f"  these are blocked, pick available ones: {blocked}")
            continue
        return [rows[p - 1][1].session_name for p in picked]


# ---------------------------------------------------------------------------
# Round driver
# ---------------------------------------------------------------------------


async def run_round(step_num: int, tag: str, only_sessions: list[str] | None = None) -> None:
    print(f"Loading step {step_num:02d} / tag '{tag}' …")
    step_dir = find_step_dir(step_num, tag)
    window = _parse_window_from_name(step_dir)
    input_entries = _load_input(step_dir)
    meta = _load_or_init_meta(step_dir, input_entries, window, tag)

    # Pre-filter — only touches entries still in `pending` state so resumes
    # never overwrite results.
    #   • handle already in channel_registry → skipped (no API call)
    #   • handle already in error_registry   → skipped, bump attempt_count
    print("Pre-filtering against registry …")
    known = registry.known_handles()
    unresolved = error_registry.known_handles()
    skipped_registry = 0
    skipped_unresolved = 0
    unresolved_to_upsert: list[str] = []
    registry_discovery_entries: list[tuple[str, int, str]] = []

    for entry in meta["handles"]:
        if entry["status"] != "pending":
            continue
        h = entry["handle"]
        if entry.get("is_seed"):
            pass  # seeds are always scraped regardless of registry state
        elif h in known:
            entry["status"] = "skipped"
            entry["note"] = "already in registry"
            skipped_registry += 1
            registry_discovery_entries.append((h, step_num, tag))
        elif h in unresolved:
            entry["status"] = "skipped"
            unresolved_to_upsert.append(h)
            skipped_unresolved += 1

    # Record that this tag/step reached these channels, even though they weren't re-scraped.
    if registry_discovery_entries:
        registry.bulk_record_discoveries(registry_discovery_entries)

    # Bulk-update error registry in one read+write instead of one per handle
    if unresolved_to_upsert:
        upserted = error_registry.bulk_upsert([(h, step_num, tag, None) for h in unresolved_to_upsert])
        for entry in meta["handles"]:
            if entry["status"] == "skipped" and entry["note"] is None:
                rec = upserted.get(entry["handle"])
                if rec:
                    entry["note"] = f"unresolved handle (attempt {rec['attempt_count']})"

    _save_meta(step_dir, meta)

    status_counts: dict[str, int] = {}
    for h in meta["handles"]:
        status_counts[h["status"]] = status_counts.get(h["status"], 0) + 1
    remaining = status_counts.get("pending", 0) + status_counts.get("in_progress", 0)

    print(f"\n=== Round {step_num:02d} / track '{tag}' ===")
    print(f"Folder : {step_dir.name}")
    print(f"Window : {window[0].date()} … {window[1].date()}")
    print(f"Handles: {len(meta['handles'])} total — "
          f"done={status_counts.get('done', 0)}, "
          f"failed={status_counts.get('failed', 0)}, "
          f"skipped={status_counts.get('skipped', 0)}, "
          f"to-do={remaining}")
    if skipped_registry or skipped_unresolved:
        print(f"         this run's pre-filter: "
              f"{skipped_registry} already-scraped, {skipped_unresolved} unresolved")

    pool = AccountPool.load()
    if only_sessions is None:
        only_sessions = _pick_accounts_interactive(pool)
    print(f"Connecting {len(only_sessions)} account(s) …")
    await pool.connect_all(only_sessions=only_sessions)
    dlog("connect_all done")
    print(f"Pool   : {pool.healthy_count()} connected account(s), "
          f"{pool.available_count()} available right now")
    print(f"         using: {', '.join(only_sessions)}")

    _start_stdin_watcher(asyncio.get_running_loop())
    print("[Type 'abort' + Enter to stop gracefully. "
          "Ctrl-C twice for hard shutdown.]")

    try:
        if remaining > 0 and pool.available_count() == 0:
            await _pause_with_pool_state(pool, "no accounts available at round start")

        processed = 0
        run_start = time.monotonic()
        durations: list[float] = []

        for entry in meta["handles"]:
            if entry["status"] in ("done", "skipped", "failed"):
                continue
            if await _drain_for_abort():
                print("\n[abort requested — stopping before next handle]")
                break
            handle = entry["handle"]
            processed += 1

            pct = 100 * processed / remaining if remaining else 0
            elapsed = time.monotonic() - run_start
            if durations:
                avg = sum(durations) / len(durations)
                eta_s = avg * (remaining - processed)
                eta_str = f"  ETA {_fmt_duration(int(eta_s))}"
            else:
                eta_str = ""
            print(f"\n[{processed}/{remaining}  {pct:.0f}%  +{_fmt_duration(int(elapsed))}{eta_str}]  @{handle}")

            entry["status"] = "in_progress"
            _save_meta(step_dir, meta)
            dlog(f"=== begin handle @{handle} ===")

            status, err, extras = await _scrape_with_rate_limit_rules(
                pool, handle, window, step_dir, step_num, tag
            )
            entry["status"] = status
            entry["error"] = err
            entry.update(extras)
            _save_meta(step_dir, meta)

            if status == "done":
                msgs = extras.get("message_count", "?")
                dur = extras.get("duration_seconds", 0.0)
                durations.append(dur)
                print(f"  ✓ {msgs} msgs  {dur:.1f}s")
            else:
                print(f"  ✗ {err}")
    except _AbortRequested:
        print("\n[abort requested during pause — shutting down gracefully]")
    finally:
        # Runs on normal completion, abort, Ctrl-C, or any exception — the
        # operator always gets a tally + pool state before we tear down.
        _print_summary(meta, pool)
        await pool.disconnect_all()
        pool.save()


def _print_summary(meta: dict, pool: AccountPool | None = None) -> None:
    counts = {"done": 0, "failed": 0, "pending": 0, "in_progress": 0, "skipped": 0}
    for h in meta["handles"]:
        counts[h["status"]] = counts.get(h["status"], 0) + 1
    total = len(meta["handles"])
    processed = counts["done"] + counts["failed"] + counts["skipped"]
    remaining = counts["pending"] + counts["in_progress"]
    print("\n=== Summary ===")
    print(f"  scraped this run : {processed}/{total}  "
          f"(done={counts['done']}, failed={counts['failed']}, skipped={counts['skipped']})")
    print(f"  remaining        : {remaining}/{total}  "
          f"(pending={counts['pending']}, in_progress={counts['in_progress']})")
    done = [h for h in meta["handles"] if h["status"] == "done" and h.get("duration_seconds")]
    if done:
        durations = sorted(h["duration_seconds"] for h in done)
        median = durations[len(durations) // 2]
        print(f"  median duration  : {median:.1f}s / channel")
    if pool is not None:
        _dump_pool_state(pool)
