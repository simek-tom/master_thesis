"""Representative post link per forward edge.

For every forward edge (origin -> target) in the marked network, record one representative
target message id — the id of a message on `target` that was forwarded from `origin` — so the
edge list can carry a clickable `t.me/<target>/<msg_id>` link. Replicates the exact edge-keying
of analysis/extract_forwards_stats_dupes.py (dataset-gated id2handle; only forwards with a native
source_channel_id form an edge), so the keys join 1:1 onto fwd_edges.csv. The representative is
the highest (most recent) message id for the pair — deterministic.

Run from crawler/ root:  python scripts/build_forward_links.py
Output: data/analysis/fwd_edge_msgids.csv   (origin, target, target_msg_id)
"""
from __future__ import annotations

import csv
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
OUT = DATA / "analysis"

# ---- dataset membership + registry, identical to the forward extractor ----
dataset = json.load(open(DATA / "dataset_channels.json"))["handles"]
reg = json.load(open(DATA / "channel_registry.json"))
id2handle: dict[int, str] = {}
handle2canon: dict[str, str] = {}
for e in reg:
    canon = e["handles"][0].lower()
    if canon in dataset:
        id2handle[e["channel_id"]] = canon
    for h in e["handles"]:
        handle2canon[h.lower()] = canon

folders = []
for f in sorted(DATA.iterdir()):
    m = re.match(r"step_(\d+)_(\w+)_", f.name)
    if m and (f / "scraped").exists():
        folders.append((int(m.group(1)), m.group(2), f))
folders.sort()

rep: dict[tuple[str, str], int] = {}      # (origin, target) -> representative (max) target msg id
seen_canon: set[str] = set()
n_files = 0

for step, tag, folder in folders:
    for fp in sorted((folder / "scraped").glob("*_inspect.json")):
        handle = fp.name[: -len("_inspect.json")].lower()
        canon = handle2canon.get(handle, handle)
        if canon not in dataset or canon in seen_canon:
            continue
        seen_canon.add(canon)
        try:
            msgs = json.load(open(fp))
        except Exception as ex:
            print(f"BAD FILE {fp}: {ex}", file=sys.stderr)
            continue
        n_files += 1
        if n_files % 500 == 0:
            print(f"{n_files} files done", flush=True)

        own_id = next((m["channel_id"] for m in msgs if m.get("channel_id")), None)
        if own_id:
            id2handle.setdefault(own_id, canon)

        for m in msgs:
            fwd = m.get("fwd_from")
            if not fwd:
                continue
            sid = fwd.get("source_channel_id")
            if not sid:                       # no edge is formed without a native source id
                continue
            origin = id2handle.get(sid, f"id:{sid}")
            key = (origin, canon)
            mid = m["id"]
            if mid > rep.get(key, -1):        # keep the most recent repost as representative
                rep[key] = mid

with open(OUT / "fwd_edge_msgids.csv", "w", newline="", encoding="utf-8") as fh:
    w = csv.writer(fh)
    w.writerow(["origin", "target", "target_msg_id"])
    for (origin, target), mid in rep.items():
        w.writerow([origin, target, mid])

print(f"done: {n_files} channels, {len(rep):,} edge->msgid pairs -> {OUT / 'fwd_edge_msgids.csv'}")
