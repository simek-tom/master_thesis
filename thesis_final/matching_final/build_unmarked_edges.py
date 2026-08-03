"""Stage C: expand unique-text pairs into the channel-level unmarked network.

Two unmarked-transmission layers, per the signal taxonomy:
  copy      — the same unique text posted by >=2 distinct channels (sim = 1.0)
  semantic  — two different unique texts with cosine >= 0.85 (translations,
              paraphrases; the 0.90 analysis threshold filters via sim columns)

Direction: earliest occurrence -> later occurrence. Channel-level edges use
each channel's earliest posting of the text. Expansion of a text pair whose
channel fan-out product exceeds EXPAND_CAP falls back to rep-vs-rep only
(counted and reported — no silent truncation).

Outputs (data/analysis/):
  unmarked_pairs_semantic.csv.gz  all unique-text pairs >= 0.85, rep-to-rep refs
  unmarked_edges.csv              channel-level edges, both layers, with
                                  n_texts / n_ge90 / sim stats / latency / fwd flags
"""
from __future__ import annotations
import json, csv, gzip, time
from datetime import datetime
from pathlib import Path
from collections import defaultdict
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
A = ROOT / "data" / "analysis"
ANALYSIS_TH = 0.90
EXPAND_CAP = 10_000

t0 = time.time()
occurrences = [json.loads(l) for l in open(A / "emb" / "global_occurrences.jsonl")]
lang = json.load(open(A / "lang.json"))
z = np.load(A / "unique_pairs.npz")
I, J, S = z["i"], z["j"], z["sim"].astype(np.float32)
print(f"loaded {len(occurrences):,} unique texts, {len(I):,} semantic pairs ({time.time()-t0:.0f}s)", flush=True)


def ts(d):
    try:
        return datetime.fromisoformat(d).timestamp()
    except ValueError:
        return None


# per unique text: earliest occurrence per channel
def per_channel(occ):
    best = {}
    for h, mid, d, fwd in occ:
        t = ts(d)
        if t is not None and (h not in best or t < best[h][0]):
            best[h] = (t, mid, fwd)
    return best   # handle -> (ts, msg_id, is_fwd)


chan_occ = [per_channel(o) for o in occurrences]

# ---- edge accumulator (running aggregates; latency list capped per edge) ----
LAT_CAP = 1000
edges = defaultdict(lambda: {"n_texts": 0, "n_ge90": 0, "sum_sim": 0.0, "max_sim": 0.0,
                             "lats": [], "n_dst_fwd": 0})


def add_edge(src, dst, sim, lat, dst_fwd):
    e = edges[(src, dst)]
    e["n_texts"] += 1
    e["n_ge90"] += sim >= ANALYSIS_TH
    e["sum_sim"] += sim
    if sim > e["max_sim"]:
        e["max_sim"] = sim
    if lat is not None and len(e["lats"]) < LAT_CAP:
        e["lats"].append(lat)
    e["n_dst_fwd"] += dst_fwd


# ---- copy layer: same text in >=2 channels ----
n_copy_texts = 0
for co in chan_occ:
    if len(co) < 2:
        continue
    n_copy_texts += 1
    ranked = sorted(co.items(), key=lambda kv: kv[1][0])
    h0, (t0_, _, _) = ranked[0]
    for h, (t, _, fwd) in ranked[1:]:
        add_edge(h0, h, 1.0, t - t0_, fwd)     # origin = earliest poster
copy_edges = len(edges)
print(f"copy layer: {n_copy_texts:,} multi-channel texts -> {copy_edges:,} edges ({time.time()-t0:.0f}s)", flush=True)

# ---- semantic layer ----
n_capped = 0
pairs_out = gzip.open(A / "unmarked_pairs_semantic.csv.gz", "wt", newline="")
pw = csv.writer(pairs_out)
pw.writerow(["a_handle", "a_msg_id", "a_date", "b_handle", "b_msg_id", "b_date",
             "sim", "a_n_channels", "b_n_channels"])
for n, (a, b, sim) in enumerate(zip(I, J, S)):
    oa, ob = occurrences[a], occurrences[b]
    pw.writerow([oa[0][0], oa[0][1], oa[0][2], ob[0][0], ob[0][1], ob[0][2],
                 round(float(sim), 4), len(chan_occ[a]), len(chan_occ[b])])
    ca, cb = chan_occ[a], chan_occ[b]
    if not ca or not cb:
        continue
    if len(ca) * len(cb) > EXPAND_CAP:
        n_capped += 1
        ca = dict([min(ca.items(), key=lambda kv: kv[1][0])])
        cb = dict([min(cb.items(), key=lambda kv: kv[1][0])])
    for ha, (ta, _, fa) in ca.items():
        for hb, (tb, _, fb) in cb.items():
            if ha == hb:
                continue
            if ta <= tb:
                add_edge(ha, hb, float(sim), tb - ta, fb)
            else:
                add_edge(hb, ha, float(sim), ta - tb, fa)
    if n % 1_000_000 == 0:
        print(f"  {n:,}/{len(I):,} pairs ({time.time()-t0:.0f}s)", flush=True)
pairs_out.close()
if n_capped:
    print(f"NOTE: {n_capped:,} text pairs exceeded fan-out cap {EXPAND_CAP:,}; "
          f"expanded rep-vs-rep only", flush=True)

# ---- write channel-level edges ----
with open(A / "unmarked_edges.csv", "w", newline="") as fh:
    w = csv.writer(fh)
    w.writerow(["src", "dst", "src_lang", "dst_lang", "n_texts", "n_ge90",
                "mean_sim", "max_sim", "median_latency_s", "n_dst_fwd"])
    for (src, dst), e in edges.items():
        lat = float(np.median(e["lats"])) if e["lats"] else ""
        w.writerow([src, dst, lang.get(src, "?"), lang.get(dst, "?"),
                    e["n_texts"], int(e["n_ge90"]),
                    round(e["sum_sim"] / e["n_texts"], 4), round(e["max_sim"], 4),
                    lat, e["n_dst_fwd"]])
print(f"\ndone: {len(edges):,} channel edges "
      f"({sum(1 for e in edges.values() if e['n_ge90'])} with >=1 pair at {ANALYSIS_TH}) "
      f"in {(time.time()-t0)/60:.1f} min")
