"""Freeze the thesis analysis inputs into thesis_final/data_final/.

The "final form" the opponent receives: a single master node table, the two
marked edge lists (forward + mention), the undeclared-reuse layer, and the
frozen build-time summaries. Everything is self-contained under
thesis_final/data_final/ so the two final notebooks never reach back into
data/analysis/.

Run from the crawler/ root:  python scripts/build_data_final.py
"""
import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent          # crawler/
DATA = ROOT / 'data'
ANALYSIS = DATA / 'analysis'
NETWORK = DATA / 'network'
OUT = ROOT.parent / 'thesis_final' / 'data_final'   # repo root, not crawler/
OUT.mkdir(parents=True, exist_ok=True)

# ------------------------------------------------------------- handle aliases
# Six channels were renamed during the collection window, so one channel_id
# carries several handles (registry entry `['zlochan', 'localcrew']`). The master
# node table stores one alias and the raw edge extracts name the other, which is
# why the forward graph carries a vertex for each: the analysis object sees 9,986
# vertices for 9,985 channels, with the unused alias sitting as an extra isolate.
#
# That is left EXACTLY as it stands. The published analysis ran on this vertex
# vocabulary, and folding the aliases together changes it — the Leiden partition
# is sensitive to vertex ordering, and the merged channel's edges and attributes
# would move Table 3 and Figure 2 in their last digit. The alias is instead
# handled where it belongs: the notebook's construction cell counts the pair once
# when it reports how many *channels* the dataset holds.
#
# The map below is used only to fill per-channel attributes that a source artifact
# happens to file under the other alias (message stats, post counts, subscribers,
# posting hour). That affects no graph and no published figure — it only stops the
# methodology totals from losing one channel's messages.
_registry = json.load(open(DATA / 'channel_registry.json'))
ALIASES = {}
for _e in _registry:
    _hs = [h.lower() for h in _e['handles']]
    if len(_hs) < 2:
        continue
    for _h in _hs:
        ALIASES[_h] = [a for a in _hs if a != _h]


def lookup(d, h, default=None):
    """Fetch `h` from a per-channel dict, falling back to its aliases."""
    if h in d:
        return d[h]
    for a in ALIASES.get(h, ()):
        if a in d:
            return d[a]
    return default

# ---------------------------------------------------------------- node table
# language is taken from lang.json — the committed original-text-only labelling
# that the analysis is canonically run on (nodes.csv.language is the pre-policy
# per-message-majority label and is kept only as provenance elsewhere).
lang = json.load(open(ANALYSIS / 'lang.json'))

# The methodology chapter's language census predates the original-text-only policy: it was
# taken on the default per-message-majority labelling, preserved here. Chapters 3 and 4 run
# on `lang.json` above; both labellings ship so each printed figure can be reproduced from
# the bundle rather than argued about.
lang_default = {r['handle']: r['language'] for r in
                csv.DictReader(open(ANALYSIS / 'policy_backup_2026-07-24' /
                                    'nodes_default_policy.csv'))}

stats = {}
with open(ANALYSIS / 'channel_stats.csv') as fh:
    for r in csv.DictReader(fh):
        stats[r['handle']] = r

# subscriber counts (participants_count) from the GetFullChannel description scrape
subs = {}
_desc = json.load(open(ANALYSIS / 'channel_descriptions.json'))
for h, d in _desc.items():
    if d.get('participants_count') is not None:
        subs[h] = d['participants_count']

# freeze the full description records into the bundle (title / username / about / participants_count),
# so the community-3 battery ('infodefense' scan, titles) reads from data_final and stays self-contained.
json.dump(_desc, open(OUT / 'channel_descriptions.json', 'w'), ensure_ascii=False)

# modal posting hour (UTC) from scripts/build_modal_hour.py — the schedule-offset control
modal_hour = {}
_mh_path = ANALYSIS / 'channel_modal_hour.json'
if _mh_path.exists():
    for h, d in json.load(open(_mh_path)).items():
        modal_hour[h] = d['modal_hour']

# album-collapsed user-visible post counts from scripts/build_channel_posts.py — the
# methodology chapter's post figures (channel_stats.n_msgs counts raw rows instead).
_posts = {}
_posts_path = ANALYSIS / 'channel_posts.json'
if _posts_path.exists():
    _posts = json.load(open(_posts_path))['n_posts']

# columns: identity/provenance + every node attribute the marked analysis uses
#   used by analysis : language, depth, type, n_msgs, median_views
#   identity/provenance: handle, tag, seed, lang_confidence, subscribers
#   modal_hour_utc: schedule-offset control for the unmarked directionality section
NODE_COLS = ['handle', 'tag', 'depth', 'seed', 'type',
             'language', 'language_default_policy', 'lang_confidence',
             'n_msgs', 'n_posts', 'n_text_msgs', 'n_forward_msgs',
             'median_views', 'subscribers', 'modal_hour_utc']

n_nodes = 0
with open(NETWORK / 'nodes.csv') as fh, \
     open(OUT / 'nodes.csv', 'w', newline='') as out:
    w = csv.DictWriter(out, fieldnames=NODE_COLS)
    w.writeheader()
    for r in csv.DictReader(fh):
        h = r['handle']                 # verbatim from the master table, as published
        st = lookup(stats, h, {})
        w.writerow({
            'handle': h,
            'tag': r['tag'],
            'depth': r['depth'],
            'seed': r['seed'],
            'type': r['type'],
            'language': lang.get(h, 'unknown'),
            'language_default_policy': lookup(lang_default, h, 'unknown'),
            'lang_confidence': r['lang_confidence'],
            'n_msgs': st.get('n_msgs', ''),
            'n_posts': lookup(_posts, h, ''),
            'n_text_msgs': st.get('n_text_msgs', ''),
            'n_forward_msgs': st.get('n_forward_msgs', ''),
            'median_views': st.get('median_views', ''),
            'subscribers': lookup(subs, h, ''),
            'modal_hour_utc': lookup(modal_hour, h, ''),
        })
        n_nodes += 1

# ------------------------------------------------------------- forward edges
# origin -> forwarder, weighted by count. Unresolved outside-dataset origins keep
# their `id:NNN` form (the notebook needs the prefix to censor / count them).
# `target_msg_id` / `post_url` = a representative repost on the target channel (the id of a
# message on `target` forwarded from `origin`), from scripts/build_forward_links.py. The
# target is always a dataset handle, so t.me/<target>/<id> is constructible for every edge.
links = {}
_links_path = ANALYSIS / 'fwd_edge_msgids.csv'
if _links_path.exists():
    for r in csv.DictReader(open(_links_path)):
        links[(r['origin'], r['target'])] = r['target_msg_id']

n_fwd = n_linked = 0
with open(ANALYSIS / 'fwd_edges.csv') as fh, \
     open(OUT / 'edges_forward.csv', 'w', newline='') as out:
    w = csv.DictWriter(out, fieldnames=['origin', 'target', 'count', 'target_msg_id', 'post_url'])
    w.writeheader()
    for r in csv.DictReader(fh):
        mid = links.get((r['origin'], r['target']), '')
        w.writerow({'origin': r['origin'], 'target': r['target'], 'count': r['count'],
                    'target_msg_id': mid,
                    'post_url': f'https://t.me/{r["target"]}/{mid}' if mid else ''})
        n_fwd += 1
        n_linked += bool(mid)

# ------------------------------------------------------------- mention edges
# source -> mentioned channel, weighted by n_msgs; `methods` records how the
# mention was detected (entity_url / regex_mention / regex_tme / ...).
n_men = 0
with open(ANALYSIS / 'mention_edges.csv') as fh, \
     open(OUT / 'edges_mention.csv', 'w', newline='') as out:
    w = csv.DictWriter(out, fieldnames=['source', 'target', 'methods', 'n_msgs'])
    w.writeheader()
    for r in csv.DictReader(fh):
        w.writerow({'source': r['source'], 'target': r['target'],
                    'methods': r['methods'], 'n_msgs': r['n_msgs']})
        n_men += 1

# ------------------------------------------------------------ unmarked edges
# The undeclared-reuse (laundered) layer: an UNDIRECTED channel pair backed by
# validated content matches (semantic re-authoring ∪ non-forward exact copy).
# Rebuilt by scripts/build_validated_edges.py into validated_edges_undirected.csv;
# per-language thresholds already applied (same-lang ≥0.98, cross-lang ≥0.95).
# We carry only the pair + the evidence the analysis consumes:
#   n_pairs                          reuse-bar weight (the n≥2 cut runs on this)
#   n_u_first / n_v_first / n_tie    time-order tallies (the directionality probe)
#   mean_sim / max_sim               cosine strength (the threshold-axis sweep)
#   mean_latency_s                   relaunch latency (the same-event / deliberate test)
#   is_cross                         FROZEN build-time class: 1 cross-lang / 0 same-lang /
#                                    '' unknown-side. Load-bearing — which cosine bar the
#                                    edge had to clear depended on this at build time, so it
#                                    is preserved verbatim, not re-derived from node labels.
# Language per node is NOT stored on the edge (it is a node attribute in nodes.csv,
# mirroring the forward/mention layers); the notebook joins it back.
UNM_COLS = ['u', 'v', 'n_semantic', 'n_copy', 'n_u_first', 'n_v_first', 'n_tie',
            'mean_sim', 'max_sim', 'mean_latency_s', 'is_cross',
            'n_u_first_copy', 'n_v_first_copy', 'n_tie_copy', 'mean_latency_copy']
n_unm = 0
with open(ANALYSIS / 'validated_edges_undirected.csv') as fh, \
     open(OUT / 'edges_unmarked.csv', 'w', newline='') as out:
    w = csv.DictWriter(out, fieldnames=UNM_COLS)
    w.writeheader()
    for r in csv.DictReader(fh):
        w.writerow({k: r[k] for k in UNM_COLS})
        n_unm += 1

# -------------------------------------------------- unmarked latency histogram
# Pair-level post-time-gap distribution per language class, carried over from the
# build summary (build_validated_edges.py). The unmarked edge list only stores an
# edge's MEAN latency, which cannot reproduce the pair-level <5min share (averaging
# hides the bimodality); this compact histogram is the frozen source for the
# "matches are relaunches, not simultaneous same-event coverage" figure. Rows =
# language class; columns = the fixed latency buckets + total pair count.
_summary = json.load(open(ANALYSIS / 'validated_build_summary.json'))
_hist = _summary['latency_hist_by_class']
_buckets = _summary['latency_buckets_order']          # ['<5min','5-60min','1-24h','1-7d','>7d']
_med = _summary.get('latency_median_s_by_class', {})  # exact pair-level median gap (s), per class
with open(OUT / 'unmarked_latency.csv', 'w', newline='') as out:
    w = csv.writer(out)
    w.writerow(['class'] + _buckets + ['n_pairs', 'median_s'])
    for cls in ('same', 'cross', 'unknown'):
        counts = [_hist[cls][b] for b in _buckets]
        w.writerow([cls] + counts + [sum(counts), _med.get(cls, '')])

# copy-layer latency (same schema) — the positive-control panel: bot-speed vs the semantic
# editorial-speed, and whether the residual same-language near-simultaneity is copy leakage.
_hist_c = _summary.get('latency_hist_by_class_copy', {})
_med_c = _summary.get('latency_median_s_by_class_copy', {})
with open(OUT / 'unmarked_latency_copy.csv', 'w', newline='') as out:
    w = csv.writer(out)
    w.writerow(['class'] + _buckets + ['n_pairs', 'median_s'])
    for cls in ('same', 'cross', 'unknown'):
        counts = [_hist_c.get(cls, {}).get(b, 0) for b in _buckets]
        w.writerow([cls] + counts + [sum(counts), _med_c.get(cls, '')])

# pair-level latency vs. the reuse bar: for each n>=k, the matched pairs on edges with
# >= k matches, split by class (within-5min share + median gap). Shows same-language reuse
# speeding up (automated) while cross-language slows down (deliberate) as evidence accrues.
_lbt = _summary.get('latency_by_threshold', {})
_thr = _summary.get('latency_threshold_order', [1, 2, 3, 5, 10])
with open(OUT / 'unmarked_latency_by_threshold.csv', 'w', newline='') as out:
    w = csv.writer(out)
    w.writerow(['class', 'threshold', 'n_pairs', 'lt5min', 'median_s'])
    for cls in ('same', 'cross', 'unknown'):
        for k in _thr:
            c = _lbt.get(cls, {}).get(str(k), {})
            w.writerow([cls, k, c.get('n_pairs', ''), c.get('lt5min', ''), c.get('median_s', '')])

# direction x latency PIVOT: reuse bar (k = >= directional votes) x gap floor. Each cell will
# hold the share of edges that are UNANIMOUS in time-order = n_unanimous / n_edges. Long form
# (class, k, floor) so the notebook can pivot any class or sum to "all".
_dp = _summary.get('direction_pivot', {})
_ks = _summary.get('direction_pivot_ks', [1, 2, 3, 4, 5, 7, 10])
_floors = _summary.get('direction_pivot_floors', [0, 300, 3600, 21600, 86400])
with open(OUT / 'unmarked_direction_pivot.csv', 'w', newline='') as out:
    w = csv.writer(out)
    w.writerow(['class', 'k', 'latency_floor_s', 'n_edges', 'n_unanimous'])
    for cls in ('same', 'cross', 'unknown'):
        for k in _ks:
            for L in _floors:
                c = _dp.get(cls, {}).get(str(k), {}).get(str(L), {})
                w.writerow([cls, k, L, c.get('n_edges', ''), c.get('n_unanimous', '')])

# ------------------------------------------- per-pair evidence behind the edges
# One row per surviving message match, oriented like its edge so it joins on (u, v).
# This is the layer under `edges_unmarked.csv`: for any channel pair, which posts
# matched, when, and how closely. The raw candidate scan is ~28.3M rows and is not
# shipped; what backs an edge is 317k and is.
import shutil as _shutil
_pairs_path = ANALYSIS / 'validated_pairs.csv.gz'
n_pairs = 0
if _pairs_path.exists():
    _shutil.copyfile(_pairs_path, OUT / 'validated_pairs.csv.gz')
    import gzip as _gzip
    with _gzip.open(_pairs_path, 'rt') as _fh:
        n_pairs = sum(1 for _ in _fh) - 1

# --------------------------------------------- manual content-match validation
# The two hand-labelled 100-pair samples behind the methodology chapter's accuracy
# figures (ru-ru at the 0.98 same-language bar, cs/sk-ru at the 0.95 cross-language
# bar). Normalised onto one schema so the notebook can just group and count; the
# matched texts travel with the verdict so the labelling can be re-read, not just
# re-tallied.
VAL_COLS = ['sample', 'threshold', 'id', 'sim', 'verdict',
            'a_handle', 'a_lang', 'a_date', 'b_handle', 'b_lang', 'b_date',
            'a_text', 'b_text']
_val_src = [('ru-ru', 0.98, 'validate_ru-ru_0 98.csv', ';'),
            ('cssk-ru', 0.95, 'validate_cssk-ru_0.95.csv', ';')]
n_val = 0
with open(OUT / 'validation_samples.csv', 'w', newline='') as out:
    w = csv.DictWriter(out, fieldnames=VAL_COLS)
    w.writeheader()
    for sample, thr, fname, delim in _val_src:
        for r in csv.DictReader(open(ANALYSIS / 'validation' / fname,
                                     encoding='utf-8-sig'), delimiter=delim):
            row = {k: r.get(k, '') for k in VAL_COLS}
            row['sample'], row['threshold'] = sample, thr
            row['verdict'] = r.get('same', '')      # y = same information, n = not
            w.writerow(row)
            n_val += 1

# ------------------------------------------------------- build-time provenance
# The counts the methodology chapter quotes that no per-node or per-edge file can
# carry: the embedding corpus before/after deduplication, the raw extraction
# artifacts the crawl produced, and the model + thresholds the matching ran under.
_corpus = json.load(open(ANALYSIS / 'emb' / 'global_corpus_meta.json'))


def _nrows(p):
    with open(p) as fh:
        return sum(1 for _ in fh) - 1


meta = {
    'dataset': {
        'n_channels': n_nodes,
        'n_seeds': 50,
        'window': ['2022-12-28', '2023-01-28'],
    },
    'embedding_corpus': {
        'min_chars': _corpus['minlen'],
        'truncate_chars': _corpus['trunc'],
        'n_msgs_over_min_chars': _corpus['n_msgs'],
        'n_unique_after_dedup': _corpus['n_unique'],
        'model': _corpus['model'],
        'threshold_same_language': 0.98,
        'threshold_cross_language': 0.95,
    },
    'raw_extraction': {
        'fwd_edges_rows': _nrows(ANALYSIS / 'fwd_edges.csv'),
        'mention_edges_rows': _nrows(ANALYSIS / 'mention_edges.csv'),
        'registry_channels': len(_registry),
    },
    'unmarked_build': {k: _summary[k] for k in
                       ('n_rows_scanned', 'n_pairs_kept', 'n_edges')
                       if k in _summary},
}
json.dump(meta, open(OUT / 'methodology_meta.json', 'w'), indent=1, ensure_ascii=False)

print(f'wrote {OUT}')
print(f'  nodes.csv           : {n_nodes:,} rows, cols = {NODE_COLS}')
print(f'  edges_forward.csv   : {n_fwd:,} rows (origin, target, count, target_msg_id, post_url) '
      f'| {n_linked:,} linked ({n_linked / n_fwd:.1%})')
print(f'  edges_mention.csv   : {n_men:,} rows (source, target, methods, n_msgs)')
print(f'  edges_unmarked.csv  : {n_unm:,} rows, cols = {UNM_COLS}')
print(f'  unmarked_latency.csv: pair-time-gap histogram, classes = same/cross/unknown x {_buckets}')
print(f'  validated_pairs.csv.gz: {n_pairs:,} per-pair matches behind the edges')
print(f'  validation_samples.csv: {n_val:,} hand-labelled pairs '
      f'({", ".join(s for s, *_ in _val_src)})')
print(f'  methodology_meta.json : corpus {meta["embedding_corpus"]["n_msgs_over_min_chars"]:,} '
      f'-> {meta["embedding_corpus"]["n_unique_after_dedup"]:,} unique')
print(f'  handle aliases        : {len(ALIASES) // 2} renamed channels; per-channel attributes '
      f'joined across aliases, edge/node handles left verbatim (as published)')
