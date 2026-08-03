"""Rebuild the per-channel term bag straight into thesis_final/data_final/.

Isolated, self-contained recompute of the message token counts the Marked-network
TF-IDF cell needs — NOT a copy of data/analysis/channel_terms.json. Streams the raw
scraped messages (*_inspect.json, steps 00-02 both tags), gated on the canonical
dataset membership, and writes a fresh top-K term bag into the frozen bundle.

Tokenisation is deliberately minimal (letters only, len>=3, lowercased) so the
term bag stays a raw *input*: the real stopword removal + TF-IDF live in the
notebook, where the analysis choices are visible. A tiny multilingual function-word
stoplist only keeps the stored top-K from being all glue words.

Run from crawler/ root:  python scripts/build_terms_final.py

Output: thesis_final/data_final/channel_terms.json
    { handle: {"n_tokens": int, "terms": {term: count, ...}} }   # top-K terms only
"""
from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent          # crawler/
DATA = ROOT / "data"
OUT = ROOT.parent / "thesis_final" / "data_final"   # repo root, not crawler/
OUT.mkdir(parents=True, exist_ok=True)

TOP_K = 120
MIN_LEN = 3

# compact multilingual function-word stoplist (ru / cs / sk / en) — only to keep the
# stored top-K clean; the notebook applies the authoritative stopwordsiso filter.
STOP = set("""
the and for that with this from have are was you not but all can has its our out who
про что как для это так его она они оно при над под без либо если чтобы когда уже еще
или нас вам наш этот этого этом была были было быть есть нет был вот там тут где этой
тех тот того тому теми этих этим году год день дня дней раз нам них ним ему ей мы вы он
она они мне тебя меня себя свой своя свои наша наши ваш ваша ваши также если чем чём
a aby ako ale alebo ani az ako už už som si sa se ze že jak již jen jako pro pre při bez
nad pod tak ten ta to ty te tato tento této toho tom tím při jsou jsem byl byla bylo být
ako co čo ked keď lebo preto teda však viac tu tam kde kto čo ako ich jeho jej náš naša
naše vaš naši ten tá to tie ako že ze za na do od po ku ze zo isi ešte este už už len iba
""".split())

tok_re = re.compile(r"[^\W\d_]{%d,}" % MIN_LEN, re.UNICODE)

reg = json.load(open(DATA / "channel_registry.json"))
handle2canon = {h.lower(): e["handles"][0].lower() for e in reg for h in e["handles"]}
dataset = set(json.load(open(DATA / "dataset_channels.json"))["handles"])

folders = []
for f in sorted(DATA.iterdir()):
    m = re.match(r"step_(\d+)_(\w+)_", f.name)
    if m and (f / "scraped").exists():
        folders.append((int(m.group(1)), m.group(2), f))
folders.sort()

out: dict[str, dict] = {}
seen: set[str] = set()
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
        counts: Counter[str] = Counter()
        n_tokens = 0
        for msg in msgs:
            text = msg.get("message") or ""
            if not text:
                continue
            for t in tok_re.findall(text.lower()):
                n_tokens += 1
                if t in STOP:
                    continue
                counts[t] += 1
        out[canon] = {"n_tokens": n_tokens, "terms": dict(counts.most_common(TOP_K))}

json.dump(out, open(OUT / "channel_terms.json", "w"), ensure_ascii=False)
print(f"done: {n_files} channels -> {OUT / 'channel_terms.json'}")
