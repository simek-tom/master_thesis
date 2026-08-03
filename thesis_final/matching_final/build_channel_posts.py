"""Per-channel *user-visible post* counts (album-collapsed), frozen for the bundle.

`channel_stats.csv` counts raw message rows: a grouped-media album arrives as one
row per attachment, so its `n_msgs` (3,511,143) overshoots what a reader of the
channel actually saw. The thesis quotes the album-collapsed figure (2,897,619).
Collapsing needs the raw Telethon objects, so it is done here once and frozen —
`build_data_final.py` joins the result into `nodes.csv` as `n_posts`, which lets
the final notebooks reproduce the by-depth post table without the pickles.

Run from the crawler/ root:  python scripts/build_channel_posts.py
"""
import csv
import json
import pickle
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent          # crawler/
DATA = ROOT / 'data'
OUT = DATA / 'analysis' / 'channel_posts.json'

registry = json.load(open(DATA / 'channel_registry.json'))
dataset = json.load(open(DATA / 'dataset_channels.json'))
# Key on the registry's FIRST handle — the same canonical choice
# `build_data_final.py` makes, so the frozen `nodes.csv` and this file agree on
# what a renamed channel is called.
alias = {}
for e in registry:
    hs = [h.lower() for h in e['handles']]
    for h in hs:
        alias[h] = hs[0]
members = {alias.get(h, h) for h in dataset['handles']} | {
    alias.get(r['handle'], r['handle'])
    for r in csv.DictReader(open(DATA / 'network' / 'nodes.csv'))}


def step_folders():
    out = []
    for f in DATA.iterdir():
        p = f.name.split('_')
        if f.is_dir() and len(p) == 5 and p[0] == 'step' and (f / 'scraped').exists():
            try:
                out.append((int(p[1]), p[2], f))
            except ValueError:
                pass
    return sorted(out)


posts, raw, seen = {}, {}, set()
for step, tag, folder in step_folders():
    for pkl in sorted((folder / 'scraped').glob('*.pkl')):
        h = alias.get(pkl.stem.lower(), pkl.stem.lower())
        if h not in members or h in seen:
            continue
        seen.add(h)
        msgs = pickle.load(open(pkl, 'rb'))
        grouped, n = set(), 0
        for m in msgs:
            gid = getattr(m, 'grouped_id', None)
            if gid is None:
                n += 1
            elif gid not in grouped:
                grouped.add(gid)
                n += 1
        posts[h] = n
        raw[h] = len(msgs)

json.dump({'n_posts': posts, 'n_raw_rows': raw}, open(OUT, 'w'))
print(f'wrote {OUT}')
print(f'  channels        : {len(posts):,} of {len(members):,} dataset members')
print(f'  posts (collapsed): {sum(posts.values()):,}')
print(f'  raw message rows : {sum(raw.values()):,}')
