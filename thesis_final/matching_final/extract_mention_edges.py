"""Fast mention-edge extraction from *_inspect.json of dataset channels.

Replicates src/extractor.extract_all using the serialized entities_json:
  - regex @mention and t.me/ links over message text
  - MessageEntityTextUrl hidden urls
  - MessageEntityMention entities (catches grandfathered short handles like
    @shot that the >=5-char regex misses)

Output: data/analysis/mention_edges.csv  (source, target, methods, n_msgs)
"""
from __future__ import annotations

import csv
import json
import re
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
OUT = DATA / "analysis"

RE_MENTION = re.compile(r"@([A-Za-z][A-Za-z0-9_]{4,31})(?=[^A-Za-z0-9_]|$)")
RE_TME = re.compile(r"(?:https?://)?(?:t\.me|telegram\.me)/([A-Za-z][A-Za-z0-9_]{4,31})", re.IGNORECASE)


def utf16_slice(text: str, offset: int, length: int) -> str:
    b = text.encode("utf-16-le")
    return b[2 * offset: 2 * (offset + length)].decode("utf-16-le", errors="ignore")


reg = json.load(open(DATA / "channel_registry.json"))
handle2canon = {h.lower(): e["handles"][0].lower() for e in reg for h in e["handles"]}

dataset = json.load(open(DATA / "dataset_channels.json"))["handles"]

folders = []
for f in sorted(DATA.iterdir()):
    m = re.match(r"step_(\d+)_(\w+)_", f.name)
    if m and (f / "scraped").exists():
        folders.append((int(m.group(1)), m.group(2), f))
folders.sort()

methods_map: dict[tuple[str, str], set[str]] = defaultdict(set)
count_map: Counter[tuple[str, str]] = Counter()
seen_canon: set[str] = set()
n_files = 0

for step, tag, folder in folders:
    for fp in sorted((folder / "scraped").glob("*_inspect.json")):
        handle = fp.name[: -len("_inspect.json")].lower()
        canon = handle2canon.get(handle, handle)
        if canon not in dataset or canon in seen_canon:
            continue
        seen_canon.add(canon)
        msgs = json.load(open(fp))
        n_files += 1
        if n_files % 1000 == 0:
            print(f"{n_files} files", flush=True)
        for m in msgs:
            text = m.get("message") or ""
            found: dict[str, set[str]] = defaultdict(set)
            for mt in RE_MENTION.finditer(text):
                found[mt.group(1).lower()].add("regex_mention")
            for mt in RE_TME.finditer(text):
                found[mt.group(1).lower()].add("regex_tme")
            for e in m.get("entities_json") or []:
                etype = e.get("type")
                if etype == "MessageEntityTextUrl" and e.get("url"):
                    mt = RE_TME.search(e["url"])
                    if mt:
                        found[mt.group(1).lower()].add("entity_url")
                elif etype == "MessageEntityMention":
                    h = utf16_slice(text, e.get("offset", 0), e.get("length", 0)).lstrip("@").lower()
                    if h:
                        found[h].add("entity_mention")
            for target, methods in found.items():
                tc = handle2canon.get(target, target)
                if tc == canon:
                    continue
                key = (canon, tc)
                methods_map[key] |= methods
                count_map[key] += 1

with open(OUT / "mention_edges.csv", "w", newline="", encoding="utf-8") as fh:
    w = csv.writer(fh)
    w.writerow(["source", "target", "methods", "n_msgs"])
    for (s, t), n in sorted(count_map.items(), key=lambda kv: -kv[1]):
        w.writerow([s, t, "|".join(sorted(methods_map[(s, t)])), n])

print(f"done: {n_files} channels, {len(count_map)} mention edges")
