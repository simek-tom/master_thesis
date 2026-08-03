#!/usr/bin/env python
"""Stage C-bis: emit the NON-FORWARD exact-copy layer as message-pair rows in the
SAME schema as unmarked_pairs_semantic.csv.gz, so build_validated_edges.py and
edge_match_decomp.py can fold verbatim copy into the unmarked network alongside
the semantic pairs (n>=2 bar then applies to the COMBINED per-pair evidence).

Copy convention mirrors build_unmarked_edges.py exactly: for each unique text
posted by >=2 dataset-member channels, the EARLIEST poster is the origin and is
linked to every LATER poster (star, not clique). "excluding forwards" = the later
channel's own occurrence is not a Telegram forward (fwd flag False) -> a genuine
re-post/copy-paste, not a declared forward.

Each output row: (a=origin, b=later copier), sim = 1.0. Direction/latency/lang are
derived downstream from the two dates + handles, identically to semantic rows.
"""
import json, csv, gzip
from datetime import datetime
from pathlib import Path

A = Path(__file__).resolve().parent.parent / "data" / "analysis"
MEMBERS = set(json.load(open(A.parent / "dataset_channels.json"))["handles"].keys())
OCC = A / "emb" / "global_occurrences.jsonl"
OUT = A / "unmarked_pairs_copy_nonfwd.csv.gz"


def ts(d):
    try:
        return datetime.fromisoformat(d).timestamp()
    except Exception:
        return None


n_multi = 0          # unique texts posted by >=2 member channels
n_rows = 0           # emitted copy pairs (origin -> later non-forward)
n_pairs = set()      # distinct undirected channel pairs touched

with gzip.open(OUT, "wt", newline="") as gz:
    w = csv.writer(gz)
    w.writerow(["a_handle", "a_msg_id", "a_date", "b_handle", "b_msg_id", "b_date",
                "sim", "a_n_channels", "b_n_channels"])
    with open(OCC) as fh:
        for line in fh:
            occ = json.loads(line)
            # earliest occurrence per member channel: handle -> (ts, msg_id, date, fwd)
            best = {}
            for h, mid, d, fwd in occ:
                h = h.lower()
                if h not in MEMBERS:
                    continue
                t = ts(d)
                if t is None:
                    continue
                if h not in best or t < best[h][0]:
                    best[h] = (t, mid, d, bool(fwd))
            if len(best) < 2:
                continue
            n_multi += 1
            ranked = sorted(best.items(), key=lambda kv: kv[1][0])
            n_ch = len(ranked)
            oh, (ot, omid, od, _ofwd) = ranked[0]     # origin = earliest poster
            for h, (t, mid, d, fwd) in ranked[1:]:
                if fwd:                                # later channel forwarded -> declared, skip
                    continue
                if h == oh:
                    continue
                w.writerow([oh, omid, od, h, mid, d, "1.0", n_ch, n_ch])
                n_rows += 1
                n_pairs.add((oh, h) if oh <= h else (h, oh))

print(f"multi-channel texts (>=2 members) : {n_multi:,}")
print(f"non-forward copy pair-rows emitted: {n_rows:,}")
print(f"distinct undirected channel pairs : {len(n_pairs):,}")
print(f"wrote {OUT}")
