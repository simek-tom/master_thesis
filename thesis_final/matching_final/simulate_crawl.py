"""
simulate_crawl.py — replay the BFS crawl offline and define the dataset.

The live crawl's seed set was expanded mid-crawl, so folder step numbers record
chronological rounds, not BFS depth. This script replays discovery from the
FINAL seed set (step_00 input files) over the scraped data on disk, as if the
seed set had been complete from the beginning:

  depth 0 = seeds; depth d+1 = handles extracted from depth-d channels' messages
  (same four mention methods the live crawl used, incl. entity mentions).

Output: data/dataset_channels.json
    {"handle": {...}, "gap": [...]}  where
    handle -> {"depth": 0|1|2}            channels IN the dataset (data on disk)
    gap    -> [{handle, depth, origins, status}]  discovered at depth<=2, no data
              status: "failed" (in error registry) | "unattempted"

Every analysis artifact is built from this membership — folder steps are only
storage. Re-run after any new scrape lands in data/.

Usage:
    python scripts/simulate_crawl.py
"""
from __future__ import annotations

import json
import re
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.config import DATA_DIR

MAX_DEPTH = 2

RE_MENTION = re.compile(r"@([A-Za-z][A-Za-z0-9_]{4,31})(?=[^A-Za-z0-9_]|$)")
RE_TME = re.compile(r"(?:https?://)?(?:t\.me|telegram\.me)/([A-Za-z][A-Za-z0-9_]{4,31})", re.IGNORECASE)
# t.me path keywords that the regexes can misread as channel handles
RESERVED = {"joinchat", "addlist", "addstickers", "addemoji", "addtheme", "share",
            "proxy", "socks", "login", "boost", "giftcode", "setlanguage"}


def utf16_slice(text: str, offset: int, length: int) -> str:
    b = text.encode("utf-16-le")
    return b[2 * offset: 2 * (offset + length)].decode("utf-16-le", errors="ignore")


def extract_handles(msgs: list[dict]) -> set[str]:
    """All handles mentioned in a channel's messages (4 methods, lowercased)."""
    out: set[str] = set()
    for m in msgs:
        text = m.get("message") or ""
        for mt in RE_MENTION.finditer(text):
            out.add(mt.group(1).lower())
        for mt in RE_TME.finditer(text):
            out.add(mt.group(1).lower())
        for e in m.get("entities_json") or []:
            etype = e.get("type")
            if etype == "MessageEntityTextUrl" and e.get("url"):
                mt = RE_TME.search(e["url"])
                if mt:
                    out.add(mt.group(1).lower())
            elif etype == "MessageEntityMention":
                h = utf16_slice(text, e.get("offset", 0), e.get("length", 0)).lstrip("@").lower()
                if h:
                    out.add(h)
    return {h for h in out if h not in RESERVED}


def main() -> None:
    # canonicalization from the (full) crawl registry
    reg = json.load(open(DATA_DIR / "channel_registry.json"))
    handle2canon = {h.lower(): e["handles"][0].lower() for e in reg for h in e["handles"]}

    # scraped data on disk: canon -> inspect path (earliest round wins)
    inspect: dict[str, Path] = {}
    for folder in sorted(DATA_DIR.iterdir()):
        if not (folder.is_dir() and re.match(r"step_\d+_\w+_", folder.name)):
            continue
        scraped = folder / "scraped"
        if not scraped.is_dir():
            continue
        for p in sorted(scraped.glob("*_inspect.json")):
            h = p.name[: -len("_inspect.json")].lower()
            inspect.setdefault(handle2canon.get(h, h), p)
    print(f"scraped data on disk: {len(inspect):,} channels")

    # final seed set = step_00 input files (both tags)
    seeds: set[str] = set()
    for folder in DATA_DIR.glob("step_00_*"):
        f = folder / "input_handles.json"
        if f.exists():
            for e in json.load(open(f)):
                seeds.add(handle2canon.get(e["handle"].lower(), e["handle"].lower()))
    print(f"seed set: {len(seeds)}")

    # BFS
    depth: dict[str, int] = {s: 0 for s in seeds}
    origins: dict[str, list[str]] = defaultdict(list)   # first-discoverers of each handle
    frontier = [s for s in seeds if s in inspect]
    for d in range(1, MAX_DEPTH + 1):
        newly: set[str] = set()
        for canon in frontier:
            try:
                msgs = json.load(open(inspect[canon]))
            except Exception as ex:
                print(f"BAD FILE {inspect[canon]}: {ex}", file=sys.stderr)
                continue
            for h in extract_handles(msgs):
                t = handle2canon.get(h, h)
                if t not in depth:
                    depth[t] = d
                    newly.add(t)
                if t != canon and depth[t] == d and len(origins[t]) < 8 and canon not in origins[t]:
                    origins[t].append(canon)
        frontier = [c for c in newly if c in inspect]
        print(f"depth {d}: {len(newly):,} discovered, {len(frontier):,} with data")

    # membership + gap
    err = {e["handle"].lower() for e in json.load(open(DATA_DIR / "error_registry.json"))}
    # some hard channel errors (e.g. ChannelPrivate) are recorded only in step_meta
    for folder in DATA_DIR.glob("step_*"):
        mf = folder / "step_meta.json"
        if mf.exists():
            for h in json.load(open(mf)).get("handles", []):
                if h.get("status") == "failed":
                    err.add(h["handle"].lower())
    members = {c: {"depth": d} for c, d in depth.items() if c in inspect}
    gap = [{"handle": c, "depth": d, "origins": origins.get(c, []),
            "status": "failed" if c in err else "unattempted"}
           for c, d in sorted(depth.items()) if c not in inspect]

    out = {"handles": members, "gap": gap}
    json.dump(out, open(DATA_DIR / "dataset_channels.json", "w"), ensure_ascii=False, indent=1)
    n_fail = sum(1 for g in gap if g["status"] == "failed")
    print(f"\ndataset: {len(members):,} channels "
          f"(depth 0: {sum(1 for m in members.values() if m['depth']==0)}, "
          f"1: {sum(1 for m in members.values() if m['depth']==1)}, "
          f"2: {sum(1 for m in members.values() if m['depth']==2)})")
    print(f"gap: {len(gap):,} discovered without data ({n_fail:,} failed, {len(gap)-n_fail:,} unattempted)")
    print(f"-> {DATA_DIR / 'dataset_channels.json'}")


if __name__ == "__main__":
    main()
