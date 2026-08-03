"""The derivation chain: raw scrape -> data_final/.

The seventeen scripts in this folder ran in the order below. They are shipped for
inspection: each stage reads the raw scraped messages, which are not part of this
bundle (2.9M posts across 9,985 channels, ~4 GB of pickles), so the chain cannot be
re-run from what is published here. What it documents is how every file in
`data_final/` was produced, and on what parameters.

Print the chain:      python pipeline.py
Inspect one stage:    python pipeline.py <script name>
"""
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent

# (script, what it does, what it writes)
STAGES = [
    # -- dataset definition -------------------------------------------------
    ('simulate_crawl.py',
     'Replays BFS discovery from the final 50 seeds over everything scraped, so '
     'membership is true depth <=2 rather than the chronological crawl round a '
     'channel happened to land in.',
     'dataset_channels.json — the 9,985-channel master list'),
    ('build_nodes.py',
     'Per-channel attributes for the members: depth, type, message stats, views.',
     'network/nodes.csv'),

    # -- language labelling -------------------------------------------------
    ('build_lang_map.py',
     'langdetect majority vote over a sample of each channel\'s messages.',
     'lang_default_policy — the labelling the methodology census was taken on'),
    ('relabel_original_text.py',
     'Re-runs the vote over ORIGINAL (non-forwarded) messages only, so a channel '
     'is not labelled by the language of content it merely reposts. This is the '
     'committed labelling; Chapters 3 and 4 run on it.',
     'lang.json'),

    # -- the declared (marked) layers ---------------------------------------
    ('extract_forwards_stats_dupes.py',
     'Forward headers -> directed origin->forwarder pairs; per-channel message '
     'stats; duplicate-text clusters.',
     'fwd_edges.csv, channel_stats.csv, dupe_clusters.jsonl'),
    ('extract_mention_edges.py',
     'Channel mentions by four detection methods (@handle regex, t.me regex, '
     'entity mention, entity URL) -> the mention layer. Mentions drove crawl '
     'discovery; they are edges only in the mention layer, never in the marked '
     'network.',
     'mention_edges.csv'),
    ('build_forward_links.py',
     'A representative repost message id per forward edge, so each edge has a '
     'resolvable t.me permalink.',
     'fwd_edge_msgids.csv'),

    # -- the matching (unmarked) layer --------------------------------------
    ('build_global_corpus.py',
     'Messages over 100 characters, truncated to 400, whitespace-collapsed and '
     'lowercased, MD5-hashed and deduplicated: 1,918,478 -> 1,505,931 unique '
     'texts. Embedded with SBERT paraphrase-multilingual-MiniLM-L12-v2 (384-d) — '
     'paraphrase-trained so similarity tracks information rather than wording, '
     'and multilingual so a translation stays near its source.',
     'emb/global_emb.f16.npy, emb/global_occurrences.jsonl, corpus meta'),
    ('allpairs_gpu.py',
     'Cosine similarity for all 1,133,913,335,415 pairings of the 1.5M vectors, '
     'blocked on the GPU.',
     'candidate pairs above the retention floor'),
    ('build_unmarked_edges.py',
     'Candidate pairs -> the semantic match list, with each deduplicated text '
     'inheriting the matches of its representative.',
     'unmarked_pairs_semantic.csv.gz — 28.3M candidate rows'),
    ('build_copy_pairs_nonfwd.py',
     'The other match layer: verbatim non-forward reposts, recovered from the '
     'dedup clusters. Held separate because exact copies almost never cross a '
     'language boundary and are largely automated mirroring.',
     'unmarked_pairs_copy_nonfwd.csv.gz'),
    ('build_msg_mentions.py',
     'Per-message record of which member channels that post cites. Drives the '
     'declared-match exclusion at message level rather than pair level.',
     'msg_mentions.jsonl'),
    ('build_validated_edges.py',
     'The validation gate, and where the thresholds live: same-language matches '
     'must clear cosine 0.98, cross-language 0.95 (translation depresses cosine). '
     'Drops self-pairs, non-members, and declared matches — where either post '
     'cites the other channel. Tallies time-order votes and latency per edge. '
     '28.3M candidate rows -> 317,156 surviving pairs -> 116,863 edges.',
     'validated_edges_undirected.csv, validated_pairs.csv.gz, build summary'),

    # -- supporting attributes ----------------------------------------------
    ('build_modal_hour.py',
     'Modal posting hour (UTC) per channel — the schedule-offset control on the '
     'directionality argument.',
     'channel_modal_hour.json'),
    ('build_channel_posts.py',
     'Album-collapsed post counts. A grouped-media post arrives as one API row '
     'per attachment, so raw rows (3,511,143) overshoot what a reader saw '
     '(2,897,619).',
     'channel_posts.json'),
    ('build_terms_final.py',
     'Per-channel term bag, top-120 letters-only tokens. Deliberately raw: the '
     'stopword removal and c-TF-IDF happen in the notebook, not here.',
     'data_final/channel_terms.json'),

    # -- freeze --------------------------------------------------------------
    ('build_data_final.py',
     'Freezes everything above into the published bundle, carrying over the '
     'build-time summaries the edge list cannot reconstruct (latency histograms, '
     'the direction x latency pivot).',
     'data_final/ — every file the notebooks read'),
]


def main():
    if len(sys.argv) > 1:
        target = sys.argv[1]
        path = HERE / target if target.endswith('.py') else HERE / f'{target}.py'
        if not path.exists():
            sys.exit(f'no such stage: {target}')
        subprocess.run([sys.executable, '-c',
                        f'import ast;print(ast.get_docstring(ast.parse(open({str(path)!r}).read())) '
                        f'or "(no docstring)")'])
        return

    width = max(len(s) for s, _, _ in STAGES)
    for i, (script, what, out) in enumerate(STAGES, 1):
        print(f'\n{i:2}. {script:<{width}}')
        for line in (what, f'-> {out}'):
            words, cur = line.split(), ''
            for w in words:
                if len(cur) + len(w) > 76:
                    print(f'    {cur}')
                    cur = w
                else:
                    cur = f'{cur} {w}'.strip()
            print(f'    {cur}')


if __name__ == '__main__':
    main()
