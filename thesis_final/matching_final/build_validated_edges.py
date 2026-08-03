#!/usr/bin/env python
"""Re-aggregate message-level content matches into a channel-level UNMARKED network,
STRATIFIED into a semantic layer (re-authored / translated, cosine-validated) and a copy
layer (verbatim non-forward reposts). The semantic layer is the primary analytical object;
the copy layer is quantified separately (per-edge `n_copy`).

Validated per-language thresholds (100 hand-labelled pairs each):
    same-language pair   -> keep sim >= 0.98
    cross-language pair   -> keep sim >= 0.95
    any side 'unknown'   -> keep sim >= 0.98   (conservative: can't verify translation)

Two exclusions applied to BOTH layers (a match must be UNdeclared):
  * dataset-member gate on both endpoints;
  * BIDIRECTIONAL mention exclusion — drop the pair if EITHER message cites the other
    channel via @mention / t.me (per-message index msg_mentions.jsonl), like a forward.

Outputs:
  * validated_edges_undirected.csv  — one row per undirected pair; carries n_semantic + n_copy
    and the SEMANTIC directional / latency tallies (directionality is a semantic-layer measure).
  * validated_build_summary.json    — global tallies + the semantic-layer latency histograms
    and direction pivot (the directionality write-up).
"""
import csv, gzip, json, sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent   # repo root
A = ROOT / "crawler/data/analysis"
PAIRS = A / "unmarked_pairs_semantic.csv.gz"
COPY_PAIRS = A / "unmarked_pairs_copy_nonfwd.csv.gz"   # non-forward exact-copy layer (sim=1.0)
OUT_EDGES = A / "validated_edges_undirected.csv"
# The per-pair evidence behind every edge: one row per surviving message match.
# The raw scan is ~28.3M candidate rows; what reaches an edge is ~317k, so this is
# the small, publishable form of "which posts actually back this channel pair".
OUT_PAIRS = A / "validated_pairs.csv.gz"
OUT_SUMMARY = A / "validated_build_summary.json"

SAME_LANG = 0.98
CROSS_LANG = 0.95

lang = json.load(open(A / "lang.json"))
MEMBERS = {r["handle"] for r in csv.DictReader(open(ROOT / "crawler/data/network/nodes.csv"))}

# per-message mention index: (channel, msg_id) -> {cited member handles}
MENT = {}
with open(A / "msg_mentions.jsonl") as _fh:
    for _line in _fh:
        _d = json.loads(_line)
        MENT[(_d["c"], _d["m"])] = set(_d["t"])

def declared_mention(a_h, a_id, b_h, b_id):
    """Bidirectional: True if EITHER message cites the other channel (declared -> drop)."""
    return b_h in MENT.get((a_h, a_id), ()) or a_h in MENT.get((b_h, b_id), ())

def keep(la, lb, s):
    """Return (keep?, is_cross) under the validated threshold policy."""
    if la == "unknown" or lb == "unknown":
        return s >= SAME_LANG, None
    if la == lb:
        return s >= SAME_LANG, False
    return s >= CROSS_LANG, True

BUCKETS = [("<5min", 300), ("5-60min", 3600), ("1-24h", 86400),
           ("1-7d", 604800), (">7d", float("inf"))]
def bucket(sec):
    for name, hi in BUCKETS:
        if sec < hi:
            return name
    return ">7d"

# per-edge accumulator. SEMANTIC directional tallies + COPY directional tallies (the copy
# layer is a positive control, so it gets its own direction/latency, kept in parallel slots):
#   [0]n_semantic [1]n_copy [2]sem_u_first [3]sem_v_first [4]sem_tie [5]sum_sim [6]max_sim
#   [7]sem_sum_lat [8]copy_u_first [9]copy_v_first [10]copy_tie [11]copy_sum_lat
edges = {}
lat_hist = {True: {b[0]: 0 for b in BUCKETS}, False: {b[0]: 0 for b in BUCKETS},
            None: {b[0]: 0 for b in BUCKETS}}
lat_vals = {True: [], False: [], None: []}
edge_lats = {}                                # (u,v) -> [(dir, gap)] over SEMANTIC pairs
lat_hist_copy = {True: {b[0]: 0 for b in BUCKETS}, False: {b[0]: 0 for b in BUCKETS},
                 None: {b[0]: 0 for b in BUCKETS}}
lat_vals_copy = {True: [], False: [], None: []}
kept_sem = kept_copy = 0
pair_rows = []          # the surviving matches, dumped alongside the edge table
kept_by_class = {"same": 0, "cross": 0, "unknown": 0}   # semantic pairs by class
n_declared = 0
n_rows = 0

def _iter_rows():
    for _src, _layer in ((PAIRS, 0), (COPY_PAIRS, 1)):     # 0 = semantic, 1 = copy
        with gzip.open(_src, "rt") as fh:
            rdr = csv.reader(fh)
            next(rdr)
            for _row in rdr:
                yield _row, _layer

for row, layer in _iter_rows():
    n_rows += 1
    if n_rows % 5_000_000 == 0:
        print(f"  {n_rows:,} rows, {kept_sem + kept_copy:,} kept, {len(edges):,} edges", file=sys.stderr)
    a_h, a_id, a_d, b_h, b_id, b_d, s = row[0], row[1], row[2], row[3], row[4], row[5], row[6]
    if a_h == b_h:
        continue
    if a_h not in MEMBERS or b_h not in MEMBERS:
        continue
    s = float(s)
    la, lb = lang.get(a_h, "unknown"), lang.get(b_h, "unknown")
    ok, is_cross = keep(la, lb, s)
    if not ok:
        continue
    if declared_mention(a_h, a_id, b_h, b_id):
        n_declared += 1
        continue

    if a_h <= b_h:
        u, v, ud, vd, uid, vid = a_h, b_h, a_d, b_d, a_id, b_id
    else:
        u, v, ud, vd, uid, vid = b_h, a_h, b_d, a_d, b_id, a_id
    # oriented exactly like the edge row, so a reader can join pair -> edge on (u, v)
    pair_rows.append((u, v, uid, ud, vid, vd, s, "copy" if layer == 1 else "semantic"))
    e = edges.get((u, v))
    if e is None:
        e = [0, 0, 0, 0, 0, 0.0, 0.0, 0.0, 0, 0, 0, 0.0]
        edges[(u, v)] = e

    if layer == 1:                       # copy layer: quantified separately (own direction/latency)
        e[1] += 1
        kept_copy += 1
        latc = abs((datetime.fromisoformat(ud) - datetime.fromisoformat(vd)).total_seconds())
        if ud < vd:
            e[8] += 1
        elif vd < ud:
            e[9] += 1
        else:
            e[10] += 1
        e[11] += latc
        lat_hist_copy[is_cross][bucket(latc)] += 1
        lat_vals_copy[is_cross].append(latc)
        continue

    # semantic layer: full directional / latency evidence
    e[0] += 1
    kept_sem += 1
    kept_by_class["cross" if is_cross else ("unknown" if is_cross is None else "same")] += 1
    e[5] += s
    if s > e[6]:
        e[6] = s
    if ud < vd:
        e[2] += 1; d = 1
    elif vd < ud:
        e[3] += 1; d = -1
    else:
        e[4] += 1; d = 0
    lat = abs((datetime.fromisoformat(ud) - datetime.fromisoformat(vd)).total_seconds())
    e[7] += lat
    lat_hist[is_cross][bucket(lat)] += 1
    lat_vals[is_cross].append(lat)
    edge_lats.setdefault((u, v), []).append((d, lat))

print(f"done: {n_rows:,} rows -> {kept_sem:,} semantic + {kept_copy:,} copy pairs "
      f"-> {len(edges):,} edges", file=sys.stderr)

# write the per-pair evidence (gzipped: ~317k rows)
with gzip.open(OUT_PAIRS, "wt", newline="") as fh:
    w = csv.writer(fh)
    w.writerow(["u", "v", "u_msg_id", "u_date", "v_msg_id", "v_date", "sim", "layer"])
    w.writerows(pair_rows)
print(f"wrote {OUT_PAIRS.name}: {len(pair_rows):,} validated pairs", file=sys.stderr)

# write edge table (n_semantic/n_copy per edge; semantic directional tallies)
recip = 0
n_edges_sem = n_edges_copy = 0
with open(OUT_EDGES, "w", newline="") as fh:
    w = csv.writer(fh)
    w.writerow(["u", "v", "lang_u", "lang_v", "is_cross", "n_semantic", "n_copy",
                "n_u_first", "n_v_first", "n_tie", "mean_sim", "max_sim", "mean_latency_s",
                "n_u_first_copy", "n_v_first_copy", "n_tie_copy", "mean_latency_copy"])
    for (u, v), e in edges.items():
        ns, nc = e[0], e[1]
        n_edges_sem += ns > 0
        n_edges_copy += nc > 0
        lu, lv = lang.get(u, "unknown"), lang.get(v, "unknown")
        is_cross = None if (lu == "unknown" or lv == "unknown") else (lu != lv)
        if e[2] > 0 and e[3] > 0:
            recip += 1
        msim = round(e[5] / ns, 4) if ns else ""
        xsim = round(e[6], 4) if ns else ""
        mlat = round(e[7] / ns, 1) if ns else ""
        mlatc = round(e[11] / nc, 1) if nc else ""
        w.writerow([u, v, lu, lv, "" if is_cross is None else int(is_cross), ns, nc,
                    e[2], e[3], e[4], msim, xsim, mlat, e[8], e[9], e[10], mlatc])

summary = {
    "thresholds": {"same_language": SAME_LANG, "cross_language": CROSS_LANG,
                   "unknown_side": SAME_LANG},
    "layers_separated": True,
    "mention_exclusion": "bidirectional",
    "n_pairs_dropped_declared_mention": n_declared,
    "n_pairs_kept": kept_sem + kept_copy,
    "n_semantic_pairs_kept": kept_sem,
    "n_copy_pairs_kept": kept_copy,
    "kept_by_class_semantic": kept_by_class,
    "n_edges": len(edges),
    "n_edges_semantic": n_edges_sem,
    "n_edges_copy": n_edges_copy,
    # everything below is the SEMANTIC layer (the primary analytical object)
    "n_edges_both_directions_semantic": recip,
    "reciprocal_share_semantic": round(recip / n_edges_sem, 4) if n_edges_sem else 0,
    "latency_hist_by_class": {("cross" if k is True else "same" if k is False else "unknown"): v
                              for k, v in lat_hist.items()},
    "latency_buckets_order": [b[0] for b in BUCKETS],
    "latency_median_s_by_class": {
        ("cross" if k is True else "same" if k is False else "unknown"):
            (round(sorted(v)[len(v) // 2], 1) if v else None)
        for k, v in lat_vals.items()},
    # COPY-layer latency (positive control: bot-speed vs the semantic editorial-speed)
    "latency_hist_by_class_copy": {("cross" if k is True else "same" if k is False else "unknown"): v
                                   for k, v in lat_hist_copy.items()},
    "latency_median_s_by_class_copy": {
        ("cross" if k is True else "same" if k is False else "unknown"):
            (round(sorted(v)[len(v) // 2], 1) if v else None)
        for k, v in lat_vals_copy.items()},
}

# --- semantic-layer latency by reuse bar (n_semantic >= k) ------------------------------
THRESHOLDS = [1, 2, 3, 5, 10]
_cls_of = {}
for (u, v) in edge_lats:
    lu, lv = lang.get(u, "unknown"), lang.get(v, "unknown")
    _cls_of[(u, v)] = "unknown" if (lu == "unknown" or lv == "unknown") else ("cross" if lu != lv else "same")
lat_by_threshold = {c: {} for c in ("same", "cross", "unknown")}
for k in THRESHOLDS:
    pool = {"same": [], "cross": [], "unknown": []}
    for key, dl in edge_lats.items():
        if len(dl) >= k:
            pool[_cls_of[key]].extend(g for _, g in dl)
    for c, vals in pool.items():
        vals.sort()
        lat_by_threshold[c][str(k)] = {
            "n_pairs": len(vals),
            "lt5min": sum(1 for x in vals if x < 300),
            "median_s": round(vals[len(vals) // 2], 1) if vals else None,
        }
summary["latency_by_threshold"] = lat_by_threshold
summary["latency_threshold_order"] = THRESHOLDS

# --- semantic-layer direction x latency PIVOT ------------------------------------------
FLOORS = [0, 300, 3600, 21600, 86400]
KS = [1, 2, 3, 4, 5, 7, 10]
dpiv = {c: {k: {L: [0, 0] for L in FLOORS} for k in KS} for c in ("same", "cross", "unknown")}
for key, dl in edge_lats.items():
    cell_c = dpiv[_cls_of[key]]
    for L in FLOORS:
        votes = [d for d, g in dl if g >= L and d != 0]
        m = len(votes)
        if m == 0:
            continue
        unan = not (any(d > 0 for d in votes) and any(d < 0 for d in votes))
        for k in KS:
            if m >= k:
                cell = cell_c[k][L]
                cell[0] += 1
                cell[1] += unan
summary["direction_pivot"] = {
    c: {str(k): {str(L): {"n_edges": v[0], "n_unanimous": v[1]} for L, v in kv.items()}
        for k, kv in ck.items()} for c, ck in dpiv.items()}
summary["direction_pivot_ks"] = KS
summary["direction_pivot_floors"] = FLOORS
json.dump(summary, open(OUT_SUMMARY, "w"), indent=2)
print(json.dumps({k: v for k, v in summary.items() if not isinstance(v, dict)}, indent=2))
