"""
build_nodes.py — build the per-channel node table from scraped pickles.

Includes exactly the channels in data/dataset_channels.json (written by
scripts/simulate_crawl.py — membership gates inclusion, folder steps are
storage). Streams one pickle at a time (low memory) and writes:

  data/network/nodes.csv — one row per dataset channel:
                           handle, tag, depth, seed flag, dominant language

Language detection (requires: pip install langdetect) samples up to
LANG_SAMPLE_SIZE messages per channel (deterministic, seeded) and detects the
dominant language. Pass --skip-lang to disable if langdetect is not installed.

LABELING POLICY (committed 2026-07-24): ORIGINAL TEXT ONLY. Only non-forwarded
messages enter the language sample — a channel is labeled by what it AUTHORS,
not what it relays (labels derived from forwarded text would make forward-edge
language homophily partly self-confirming). Channels with no detectable
authored text get 'unknown' (pure relays have no own language).
DetectorFactory is seeded for reproducible detection.

No minimum-detection-count guard, deliberately: a sweep of guard thresholds
(analysis/guard_threshold_sweep.py, 2026-07-24) showed each +1 of required
detections un-labels ~100 validly-labelable thin-author channels to prevent
~1-2 confident mislabels, and makes marked-network language assortativity a
function of the guard constant (0.494 / 0.457 / 0.404 / 0.365 at guard
1/2/3/5) — an arbitrary tunable the finding must not depend on. The residual
noise (single-digit odd-language labels, e.g. bg-for-ru on one authored
message) lands in the 'other' bucket with negligible metric effect.

Mention edges are NOT built here — analysis/extract_mention_edges.py is the
single source for those (data/analysis/mention_edges.csv). After nodes.csv,
run analysis/build_lang_map.py to refresh data/analysis/lang.json.

Usage:
    python scripts/build_nodes.py
    python scripts/build_nodes.py --skip-lang  # skip language detection
"""

from __future__ import annotations

import argparse
import csv
import json
import pickle
import random
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.config import DATA_DIR

NETWORK_DIR = DATA_DIR / "network"
NODES_PATH  = NETWORK_DIR / "nodes.csv"

SEED             = 42   # rng seed for language sampling (reproducible reruns)
LANG_SAMPLE_SIZE = 50   # messages sampled per channel for language detection
LANG_MIN_CHARS   = 20   # minimum message length to include in language sample

NODE_FIELDS = ["handle", "tag", "depth", "seed", "type", "language", "lang_confidence", "lang_sample_size"]


# ---------------------------------------------------------------------------
# Language detection
# ---------------------------------------------------------------------------

def _try_import_langdetect():
    try:
        from langdetect import detect_langs, LangDetectException, DetectorFactory
        DetectorFactory.seed = SEED   # reproducible detection
        return detect_langs, LangDetectException
    except ImportError:
        return None, None


def detect_language(messages: list, rng: random.Random, detect_langs, LangDetectException) -> tuple[str, float, int]:
    """Sample ORIGINAL (non-forwarded) messages and detect dominant language
    via majority vote.

    Returns (language_code, avg_confidence, sample_size_used).
    'unknown' if no usable original messages or detection fails consistently.
    Czech (cs) and Slovak (sk) are very similar — langdetect sometimes
    confuses them, so treat sk detections as low-confidence cs when the
    channel is likely Czech (caller can decide).
    """
    texts = [
        msg.message for msg in messages
        if getattr(msg, "fwd_from", None) is None
        and msg.message and len(msg.message.strip()) >= LANG_MIN_CHARS
    ]
    if not texts:
        return "unknown", 0.0, 0

    sample = texts if len(texts) <= LANG_SAMPLE_SIZE else rng.sample(texts, LANG_SAMPLE_SIZE)

    lang_counts: Counter[str] = Counter()
    lang_conf_sum: dict[str, float] = defaultdict(float)

    for text in sample:
        try:
            results = detect_langs(text)
            if results:
                top = results[0]
                lang_counts[top.lang] += 1
                lang_conf_sum[top.lang] += top.prob
        except Exception:
            pass

    if not lang_counts:
        return "unknown", 0.0, len(sample)

    dominant = lang_counts.most_common(1)[0][0]
    avg_conf = lang_conf_sum[dominant] / lang_counts[dominant]
    return dominant, round(avg_conf, 3), len(sample)


# ---------------------------------------------------------------------------
# Step folder discovery
# ---------------------------------------------------------------------------

def find_step_folders() -> list[tuple[int, str, Path]]:
    results = []
    for folder in DATA_DIR.iterdir():
        if not folder.is_dir():
            continue
        parts = folder.name.split("_")
        if len(parts) != 5 or parts[0] != "step":
            continue
        try:
            step = int(parts[1])
        except ValueError:
            continue
        tag = parts[2]
        if (folder / "scraped").exists():
            results.append((step, tag, folder))
    return sorted(results)


def load_seeds(tag: str) -> set[str]:
    seed_folder = next(
        (p for p in DATA_DIR.iterdir()
         if p.is_dir() and p.name.startswith(f"step_00_{tag}_")),
        None,
    )
    if seed_folder is None:
        return set()
    seed_file = seed_folder / "input_handles.json"
    if not seed_file.exists():
        return set()
    with open(seed_file) as fh:
        return {e["handle"].lower() for e in json.load(fh) if e.get("handle")}


# ---------------------------------------------------------------------------
# Main build
# ---------------------------------------------------------------------------

def build(skip_lang: bool) -> None:
    NETWORK_DIR.mkdir(parents=True, exist_ok=True)

    dataset = json.load(open(DATA_DIR / "dataset_channels.json"))["handles"]
    reg = json.load(open(DATA_DIR / "channel_registry.json"))
    handle2canon = {h.lower(): e["handles"][0].lower() for e in reg for h in e["handles"]}

    step_folders = find_step_folders()
    if not step_folders:
        print("No step folders found.")
        return

    # Language detection setup
    detect_langs = lang_exc = None
    if not skip_lang:
        detect_langs, lang_exc = _try_import_langdetect()
        if detect_langs is None:
            print("langdetect not installed — skipping language detection.")
            print("  Run: pip install langdetect")
        else:
            print("Language detection: enabled (langdetect)")

    all_tags = {tag for _, tag, _ in step_folders}
    seeds: dict[str, set[str]] = {tag: load_seeds(tag) for tag in all_tags}
    seen_handles: set[str] = set()

    with open(NODES_PATH, "w", newline="", encoding="utf-8") as nf:
        node_writer = csv.DictWriter(nf, fieldnames=NODE_FIELDS)
        node_writer.writeheader()

        total_channels = 0

        for step, tag, folder in step_folders:
            scraped_dir = folder / "scraped"
            pkl_paths = sorted(scraped_dir.glob("*.pkl"))
            print(f"  step {step:02d} {tag}: {len(pkl_paths)} channels", flush=True)

            for pkl_path in pkl_paths:
                handle = pkl_path.stem.lower()
                canon = handle2canon.get(handle, handle)
                if canon not in dataset or canon in seen_handles:
                    continue
                seen_handles.add(canon)

                with open(pkl_path, "rb") as fh:
                    messages = pickle.load(fh)

                # broadcast channel vs discussion (mega)group: channel posts carry
                # post=True; the flag is uniform per entity, sample a few messages
                if not messages:
                    ctype = "empty"
                elif any(getattr(m, "post", None) for m in messages[:20]):
                    ctype = "channel"
                else:
                    ctype = "group"

                if detect_langs is not None:
                    # per-channel rng: deterministic and independent of processing order
                    rng = random.Random(f"{SEED}:{canon}")
                    lang, lang_conf, lang_n = detect_language(messages, rng, detect_langs, lang_exc)
                else:
                    lang, lang_conf, lang_n = "unknown", 0.0, 0

                node_writer.writerow({
                    "handle":           handle,
                    "tag":              tag,
                    "depth":            dataset[canon]["depth"],
                    "seed":             handle in seeds.get(tag, set()),
                    "type":             ctype,
                    "language":         lang,
                    "lang_confidence":  lang_conf,
                    "lang_sample_size": lang_n,
                })
                total_channels += 1

    print(f"\nDone: {total_channels} channels")
    print(f"  {NODES_PATH}")


def main() -> None:
    p = argparse.ArgumentParser(description="Build node table from scraped pickles.")
    p.add_argument("--skip-lang", action="store_true",
                   help="Skip language detection (faster, no langdetect required)")
    args = p.parse_args()
    print("Building node table (dataset membership from dataset_channels.json) …")
    build(args.skip_lang)


if __name__ == "__main__":
    main()
