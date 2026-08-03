"""Stage A of the full hidden-network run: global corpus + embeddings.

Collects ALL text messages >=100 chars from every scraped dataset channel
(membership gated on dataset_channels.json — true BFS depth <= 2 — across all
step folders; every language, including 'unknown'), deduplicates globally by normalized
truncated text (identical scheme to match_translations.py), and produces one
embedding matrix over the unique texts. Cached ru/cssk embeddings are reused
row-for-row (matched via their (handle, msg_id) refs); only texts never
embedded before go through the model (CPU — MPS stalls for this model).

Outputs (data/analysis/emb/):
  global_emb.f16.npy     (n_unique, 384) unit-normalized
  global_occurrences.jsonl  line i = occurrences of unique text i:
                            [[handle, msg_id, date_iso, is_fwd], ...] (first = rep)
  global_corpus_meta.json   counts + provenance
"""
from __future__ import annotations
import json, re, time, os, hashlib
from pathlib import Path
import numpy as np
import torch
from sentence_transformers import SentenceTransformer

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"; A = DATA / "analysis"; EMB = A / "emb"
MODEL = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
MINLEN, TRUNC = 100, 400
ws = re.compile(r"\s+")
torch.set_num_threads(os.cpu_count() or 4)

h2c = {h.lower(): e["handles"][0].lower()
       for e in json.load(open(DATA / "channel_registry.json")) for h in e["handles"]}
# membership = dataset_channels.json (true BFS depth <= 2), NOT folder number:
# seed expansion mid-crawl put some depth-2 channels in step_03/04 folders
dataset = set(json.load(open(DATA / "dataset_channels.json"))["handles"])
HP = {}
for f in sorted(DATA.iterdir()):
    m = re.match(r"step_(\d+)_(\w+)_", f.name)
    if m and (f / "scraped").exists():
        for fp in (f / "scraped").glob("*_inspect.json"):
            hh = fp.name[:-len("_inspect.json")].lower()
            canon = h2c.get(hh, hh)
            if canon in dataset:
                HP.setdefault(canon, fp)
print(f"channels with scraped content: {len(HP):,}", flush=True)

# ---- global dedup over all channels ----
t0 = time.time()
uniq_idx = {}          # norm-hash -> unique index
occurrences = []       # unique index -> [(handle, msg_id, date, is_fwd), ...]
texts = []             # unique index -> truncated text
for n_ch, (h, p) in enumerate(sorted(HP.items())):
    for m in json.load(open(p)):
        t = m.get("message") or ""
        if len(t) < MINLEN:
            continue
        t = t[:TRUNC]
        key = hashlib.md5(ws.sub(" ", t).strip().lower().encode()).hexdigest()
        occ = (h, str(m["id"]), m.get("date") or "", m.get("fwd_from") not in (None, "None"))
        i = uniq_idx.get(key)
        if i is None:
            uniq_idx[key] = len(texts); texts.append(t); occurrences.append([occ])
        else:
            occurrences[i].append(occ)
    if n_ch % 1000 == 0:
        print(f"  {n_ch:,}/{len(HP):,} channels, {len(texts):,} unique ({time.time()-t0:.0f}s)", flush=True)
n_msgs = sum(len(o) for o in occurrences)
print(f"corpus: {n_msgs:,} text msgs -> {len(texts):,} unique ({time.time()-t0:.0f}s)", flush=True)

# ---- reuse cached embeddings via (handle, msg_id) refs ----
cache = {}             # (handle, msg_id) -> (arr, row)
Eru = np.load(EMB / "ru_uniq_emb.f16.npy")
for row, (h, mid) in enumerate(json.load(open(EMB / "ru_uniq_refs.json"))):
    cache[(h, str(mid))] = ("ru", row)
Ecs = np.load(EMB / "cssk_emb.f16.npy")
for row, (h, mid) in enumerate(json.load(open(EMB / "cssk_refs.json"))):
    cache.setdefault((h, str(mid)), ("cs", row))
arrs = {"ru": Eru, "cs": Ecs}

E = np.zeros((len(texts), Eru.shape[1]), dtype=np.float16)
todo = []
for i, occ in enumerate(occurrences):
    hit = next((cache[(h, mid)] for h, mid, _, _ in occ if (h, mid) in cache), None)
    if hit:
        E[i] = arrs[hit[0]][hit[1]]
    else:
        todo.append(i)
print(f"embeddings: {len(texts)-len(todo):,} reused from cache, {len(todo):,} to embed", flush=True)

if todo:
    model = SentenceTransformer(MODEL, device="cpu")
    t1 = time.time()
    for s in range(0, len(todo), 20000):
        chunk = todo[s:s+20000]
        E[chunk] = model.encode([texts[i] for i in chunk], normalize_embeddings=True,
                                batch_size=256, convert_to_numpy=True,
                                show_progress_bar=False).astype(np.float16)
        print(f"  embedded {min(s+20000, len(todo)):,}/{len(todo):,} ({time.time()-t1:.0f}s)", flush=True)

np.save(EMB / "global_emb.f16.npy", E)
with open(EMB / "global_occurrences.jsonl", "w") as fh:
    for occ in occurrences:
        fh.write(json.dumps(occ, ensure_ascii=False) + "\n")
json.dump({"n_unique": len(texts), "n_msgs": n_msgs, "n_channels": len(HP),
           "reused": len(texts) - len(todo), "embedded_new": len(todo),
           "minlen": MINLEN, "trunc": TRUNC, "model": MODEL},
          open(EMB / "global_corpus_meta.json", "w"), indent=1)
print(f"done: global_emb.f16.npy {E.shape} ({time.time()-t0:.0f}s total)")
