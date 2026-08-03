#!/usr/bin/env python
"""Item 8 fix: the marked language labels are contaminated because build_lang_map detects
language over ALL messages incl. forwarded text, so heavy forwarders inherit their label from
what they relay — then we measure whether forwards stay in-language (circular).

Fix: for every marked-network channel that forwards a lot (forward-share >= 0.4), re-derive its
language from ORIGINAL (non-forwarded) posts only (majority langdetect vote over <=40 sampled
texts >=20 chars). Channels with no original text -> 'unknown' (language genuinely undefined).
Recompute marked assortativity under corrected labels; report the flips and the impact.
"""
import csv, json, glob, random
from collections import Counter, defaultdict
from pathlib import Path
import igraph as ig
from langdetect import detect, DetectorFactory, LangDetectException
DetectorFactory.seed = 42
random.seed(42)

ROOT = Path(__file__).resolve().parent.parent.parent   # repo root
A = ROOT / "crawler/data/analysis"
lang = json.load(open(A / "lang.json"))
reg = json.load(open(ROOT / "crawler/data/channel_registry.json"))
h2c = {h.lower(): e["handles"][0].lower() for e in reg for h in e["handles"]}
canon = lambda h: h2c.get(h.lower(), h.lower())

BUCK = {"ru", "cs", "sk", "uk", "en"}
def bucket(l): return l if l in BUCK else ("unknown" if l == "unknown" else "other")
CLASSES = ["ru", "cs", "sk", "uk", "en", "other", "unknown"]; CIX = {c: i for i, c in enumerate(CLASSES)}

# forward shares
fshare = {}
for r in csv.DictReader(open(A / "channel_stats.csv")):
    nm = int(r.get("n_msgs") or 0); nf = int(r.get("n_forward_msgs") or 0)
    if nm > 0: fshare[canon(r["handle"])] = nf / nm

# marked giant WCC undirected collapse
fpairs = []
with open(A / "fwd_edges.csv") as fh:
    for r in csv.DictReader(fh):
        o, t = r["origin"], r["target"]
        if o.startswith("id:") or o == t: continue
        fpairs.append((o, t))
mn = sorted({n for e in fpairs for n in e}); mi = {n: i for i, n in enumerate(mn)}
M = ig.Graph(directed=True); M.add_vertices(len(mn)); M.vs["name"] = mn
M.add_edges([(mi[s], mi[t]) for s, t in fpairs])
Mg = M.induced_subgraph(max(M.connected_components(mode="weak"), key=len))
marked_u = Mg.as_undirected(mode="collapse")
marked_names = set(marked_u.vs["name"])

def r_nom(g, labels):
    return g.assortativity_nominal([CIX[bucket(labels.get(n, "unknown"))] for n in g.vs["name"]],
                                   directed=False)

# map canonical handle -> inspect files
HP = defaultdict(list)
for f in glob.glob(str(ROOT / "crawler/data/step_*/scraped/*_inspect.json")):
    h = canon(Path(f).name[:-len("_inspect.json")])
    HP[h].append(f)

def original_lang(h):
    texts = []
    for f in HP.get(h, []):
        try:
            for m in json.load(open(f)):
                if m.get("fwd_from") in (None, "None"):
                    t = (m.get("message") or "").strip()
                    if len(t) >= 20: texts.append(t[:400])
        except Exception:
            pass
    if not texts:
        return "unknown"
    random.shuffle(texts)
    votes = []
    for t in texts[:40]:
        try: votes.append(detect(t))
        except LangDetectException: pass
    return Counter(votes).most_common(1)[0][0] if votes else "unknown"

candidates = sorted(h for h in marked_names if fshare.get(h, 0) >= 0.4)
print(f"relabelling {len(candidates):,} heavy-forwarder candidates on original text...", flush=True)
corrected = dict(lang); flips = {}
for i, h in enumerate(candidates):
    new = original_lang(h)
    old = lang.get(h, "unknown")
    if bucket(new) != bucket(old):
        flips[h] = {"old": old, "new": new, "fwd_share": round(fshare.get(h, 0), 2)}
        corrected[h] = new
    if (i + 1) % 300 == 0:
        print(f"  {i+1}/{len(candidates)} ({len(flips)} flips)", flush=True)

json.dump(corrected, open(A / "lang_corrected.json", "w"))
json.dump(flips, open(A / "marked_label_flips.json", "w"), indent=1, ensure_ascii=False)

print(f"\n=== FLIPS: {len(flips)} of {len(candidates)} candidates ===")
tr = Counter((bucket(v["old"]), bucket(v["new"])) for v in flips.values())
for (a, b), n in tr.most_common():
    print(f"  {a:>7} -> {b:<7} {n}")
cssk_flips = [h for h, v in flips.items() if bucket(v["old"]) in ("cs", "sk") or bucket(v["new"]) in ("cs", "sk")]
print(f"\ncs/sk-involved flips (pocket-relevant): {len(cssk_flips)}  {cssk_flips[:10]}")

print("\n=== MARKED LANGUAGE ASSORTATIVITY ===")
print(f"  default labels          r = {r_nom(marked_u, lang):.3f}")
print(f"  corrected (relabel)     r = {r_nom(marked_u, corrected):.3f}")
def drop(g, labels, cs):
    keep = [v.index for v in g.vs if bucket(labels.get(v["name"], "unknown")) not in cs]
    return g.induced_subgraph(keep)
print(f"  corrected, -unknown     r = {r_nom(drop(marked_u, corrected, {'unknown'}), corrected):.3f}")
print(f"  corrected, -unknown,other r = {r_nom(drop(marked_u, corrected, {'unknown','other'}), corrected):.3f}")
