"""
main.py — round-based crawler entry point.

Usage:
    python main.py --step 0 --tag cz
    python main.py --step 1 --tag ru
    python main.py --step 1 --tag ru --accounts acct1-20-4,hdsn_acc_ger

Expects data/step_NN_<tag>_<start>_<end>/input_handles.json to already exist.
For round 0 the operator hand-writes that file; for round N>0 it is produced
by the previous round's notebook.
"""

import argparse
import asyncio
import re
import sys

from src.round_runner import run_round

TAG_RE = re.compile(r"^[a-z0-9]+$")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run one crawl round for a given track.")
    p.add_argument(
        "--step",
        type=int,
        required=True,
        help="Step number (0, 1, 2, …). Matches data/step_NN_<tag>_* folder.",
    )
    p.add_argument(
        "--tag",
        type=str,
        required=True,
        help="Track tag (e.g. 'cz', 'ru'). Lowercase alphanumeric only.",
    )
    p.add_argument(
        "--accounts",
        type=str,
        default=None,
        help=(
            "Comma-separated list of account session_name(s) to use for this run. "
            "Bypasses the interactive picker — handy for scripted / unattended runs. "
            "If omitted, an interactive picker prompts you to select from the pool."
        ),
    )
    p.add_argument(
        "--debug",
        action="store_true",
        help=(
            "Print timestamped progress at every network seam and surface "
            "Telethon's own log (incl. silent FloodWait sleeps). Use this when "
            "a run appears stuck."
        ),
    )
    args = p.parse_args()
    if not TAG_RE.match(args.tag):
        p.error(f"--tag must match {TAG_RE.pattern} (got: {args.tag!r})")
    if args.accounts is not None:
        names = [s.strip() for s in args.accounts.split(",") if s.strip()]
        if not names:
            p.error("--accounts must not be empty")
        args.accounts = names
    return args


def main() -> None:
    args = _parse_args()
    if args.debug:
        from src.debug import dlog, enable, enable_telethon_logging

        enable()
        enable_telethon_logging()
        dlog("debug tracing on")
    try:
        asyncio.run(run_round(args.step, args.tag, only_sessions=args.accounts))
    except KeyboardInterrupt:
        print("\nExiting cleanly.")
        sys.exit(0)


if __name__ == "__main__":
    main()
