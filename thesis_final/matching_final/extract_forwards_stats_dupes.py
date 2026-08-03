"""One streaming pass over all *_inspect.json of dataset channels (see
scripts/simulate_crawl.py — membership gates inclusion, folder steps are storage).

Outputs into data/analysis/:
  channel_stats.csv   per-channel message stats
  fwd_edges.csv       forward edges: origin -> channel that forwarded (info flow direction)
  dupe_clusters.jsonl text-duplicate clusters spanning >=2 channels
"""
from __future__ import annotations

import csv
import hashlib
import json
import re
import statistics
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
OUT = DATA / "analysis"
OUT.mkdir(exist_ok=True)

MIN_DUPE_CHARS = 80

# ---------------------------------------------------------------------------
# dataset membership (true BFS depth <= 2 from the final seed set)
dataset = json.load(open(DATA / "dataset_channels.json"))["handles"]

# registry: channel_id -> canonical handle.
# id2handle is restricted to dataset members: forward origins outside the
# dataset stay anonymous ("id:NNN"), exactly as a crawl stopped at depth 2
# would see them. handle2canon stays full (it only canonicalizes file stems).
reg = json.load(open(DATA / "channel_registry.json"))
id2handle: dict[int, str] = {}
handle2canon: dict[str, str] = {}
for e in reg:
    canon = e["handles"][0].lower()
    if canon in dataset:
        id2handle[e["channel_id"]] = canon
    for h in e["handles"]:
        handle2canon[h.lower()] = canon

# all step folders — membership decides inclusion, not the folder number
folders = []
for f in sorted(DATA.iterdir()):
    m = re.match(r"step_(\d+)_(\w+)_", f.name)
    if m and (f / "scraped").exists():
        folders.append((int(m.group(1)), m.group(2), f))
folders.sort()

ws_re = re.compile(r"\s+")

stats_rows = []
fwd_counter: Counter[tuple[str, str]] = Counter()          # (origin, dst) -> n
fwd_dates: dict[tuple[str, str], list[str]] = defaultdict(list)
seen_canon: set[str] = set()

# dupe index: hash -> first occurrence or list
dupe_first: dict[str, tuple] = {}
dupe_multi: dict[str, list] = {}

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

        dates, views, n_fwd, n_text, n_media = [], [], 0, 0, 0
        fwd_sources: Counter[int] = Counter()
        reactions_total = 0
        for m in msgs:
            if m.get("date"):
                dates.append(m["date"])
            if m.get("views") is not None:
                views.append(m["views"])
            if m.get("has_media"):
                n_media += 1
            reactions_total += sum((m.get("reactions_json") or {}).values())
            text = m.get("message") or ""
            if text.strip():
                n_text += 1
            fwd = m.get("fwd_from")
            if fwd:
                n_fwd += 1
                sid = fwd.get("source_channel_id")
                if sid:
                    fwd_sources[sid] += 1
                    origin = id2handle.get(sid, f"id:{sid}")
                    key = (origin, canon)
                    fwd_counter[key] += 1
                    if fwd.get("source_date"):
                        fwd_dates[key].append(fwd["source_date"])
            # duplicate text index
            norm = ws_re.sub(" ", text).strip().lower()
            if len(norm) >= MIN_DUPE_CHARS:
                h = hashlib.md5(norm.encode()).hexdigest()
                occ = (canon, m["id"], m.get("date"), bool(fwd))
                if h in dupe_multi:
                    dupe_multi[h].append(occ)
                elif h in dupe_first:
                    dupe_multi[h] = [dupe_first.pop(h), occ]
                else:
                    dupe_first[h] = occ

        stats_rows.append({
            "handle": canon,
            "tag": tag,
            "depth": dataset[canon]["depth"],
            "n_msgs": len(msgs),
            "n_text_msgs": n_text,
            "n_media_msgs": n_media,
            "n_forward_msgs": n_fwd,
            "n_distinct_fwd_sources": len(fwd_sources),
            "first_date": min(dates) if dates else "",
            "last_date": max(dates) if dates else "",
            "median_views": int(statistics.median(views)) if views else "",
            "total_reactions": reactions_total,
        })

del dupe_first  # singletons not needed

with open(OUT / "channel_stats.csv", "w", newline="", encoding="utf-8") as fh:
    w = csv.DictWriter(fh, fieldnames=list(stats_rows[0].keys()))
    w.writeheader()
    w.writerows(stats_rows)

with open(OUT / "fwd_edges.csv", "w", newline="", encoding="utf-8") as fh:
    w = csv.writer(fh)
    w.writerow(["origin", "target", "count", "origin_resolved", "first_src_date", "last_src_date"])
    for (origin, dst), n in sorted(fwd_counter.items(), key=lambda kv: -kv[1]):
        ds = fwd_dates.get((origin, dst), [])
        w.writerow([origin, dst, n, not origin.startswith("id:"),
                    min(ds) if ds else "", max(ds) if ds else ""])

# only keep clusters spanning >= 2 distinct channels
n_clusters = 0
with open(OUT / "dupe_clusters.jsonl", "w", encoding="utf-8") as fh:
    for h, occs in dupe_multi.items():
        chans = {o[0] for o in occs}
        if len(chans) < 2:
            continue
        n_clusters += 1
        fh.write(json.dumps({"hash": h, "n_channels": len(chans), "n_msgs": len(occs),
                             "occurrences": occs}, ensure_ascii=False) + "\n")

print(f"done: {n_files} channels, {len(fwd_counter)} fwd edges, {n_clusters} multi-channel dupe clusters")
