"""Build the canonical handle -> language map used by every downstream script.

Languages come from network/nodes.csv (langdetect, written by
scripts/build_nodes.py); handles are canonicalized via channel_registry.json
(first handle of each channel wins, aliases of renamed channels collapse).

Output: data/analysis/lang.json    { canonical_handle: lang_code }
"""
from __future__ import annotations

import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
OUT = DATA / "analysis"

reg = json.load(open(DATA / "channel_registry.json"))
handle2canon = {h.lower(): e["handles"][0].lower() for e in reg for h in e["handles"]}

nodes = {r["handle"]: r["language"] for r in csv.DictReader(open(DATA / "network" / "nodes.csv"))}

lang: dict[str, str] = {}
for handle, language in nodes.items():
    canon = handle2canon.get(handle, handle)
    if canon not in lang or handle == canon:  # canonical row wins over aliases
        lang[canon] = language

json.dump(lang, open(OUT / "lang.json", "w"), ensure_ascii=False)
print(f"done: {len(nodes)} nodes -> {len(lang)} canonical handles -> lang.json")
