"""Per-channel modal posting hour (UTC), for the schedule-offset control.

Streams the raw scraped messages (*_inspect.json, all steps/tags), gated on the
canonical dataset membership (same convention as build_terms_final.py), and records
each channel's hour-of-day posting histogram from the message `date` (always
UTC, `...T HH:MM:SS+00:00`). Writes the modal hour + the 24-bin histogram so the
notebook can rule the timezone/schedule confound in or out empirically.

Run from crawler/ root:  python scripts/build_modal_hour.py
Output: data/analysis/channel_modal_hour.json
    { handle: {"modal_hour": int, "n": int, "hist": [24 ints]} }
"""
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent          # crawler/
DATA = ROOT / "data"
OUT = DATA / "analysis" / "channel_modal_hour.json"

reg = json.load(open(DATA / "channel_registry.json"))
handle2canon = {h.lower(): e["handles"][0].lower() for e in reg for h in e["handles"]}
dataset = set(json.load(open(DATA / "dataset_channels.json"))["handles"])

folders = []
for f in sorted(DATA.iterdir()):
    m = re.match(r"step_(\d+)_(\w+)_", f.name)
    if m and (f / "scraped").exists():
        folders.append((int(m.group(1)), m.group(2), f))
folders.sort()

out = {}
seen = set()
n_files = 0
for step, tag, folder in folders:
    for fp in sorted((folder / "scraped").glob("*_inspect.json")):
        handle = fp.name[: -len("_inspect.json")].lower()
        canon = handle2canon.get(handle, handle)
        if canon not in dataset or canon in seen:
            continue
        seen.add(canon)
        try:
            msgs = json.load(open(fp))
        except Exception:
            continue
        n_files += 1
        if n_files % 1000 == 0:
            print(f"{n_files} channels", flush=True)
        hist = [0] * 24
        for msg in msgs:
            d = msg.get("date")
            if not d or len(d) < 13:
                continue
            try:
                hist[int(d[11:13])] += 1          # 'YYYY-MM-DDTHH:MM:SS+00:00' -> HH, UTC
            except ValueError:
                continue
        n = sum(hist)
        if n == 0:
            continue
        out[canon] = {"modal_hour": max(range(24), key=lambda h: hist[h]), "n": n, "hist": hist}

json.dump(out, open(OUT, "w"))
print(f"wrote {OUT}: {len(out):,} channels")
