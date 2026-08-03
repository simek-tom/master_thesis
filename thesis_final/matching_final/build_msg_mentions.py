"""Per-message mention index: (channel, msg_id) -> {mentioned dataset-member handles}.

The message-level analog of the per-message `is_fwd` flag in global_occurrences.jsonl.
Lets the unmarked-network build drop a matched pair when the receiver's specific post
CITES the source (@mention / t.me link) — a declared interaction, treated like a forward.

Replicates analysis/extract_mention_edges.py detection EXACTLY (same regexes, same
entity handling, same registry canonicalisation, same dataset-membership gate), but keys
by (canon channel, msg_id) instead of aggregating to channel-pair edges. Only mentions
whose canonical target is a dataset member are kept (the source of any matched pair is
always a member). Streaming skeleton mirrors scripts/build_modal_hour.py.

Run from crawler/ root:  python scripts/build_msg_mentions.py
Output: data/analysis/msg_mentions.jsonl  ({"c": channel, "m": msg_id, "t": [targets]})
"""
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent          # crawler/
DATA = ROOT / "data"
OUT = DATA / "analysis" / "msg_mentions.jsonl"

RE_MENTION = re.compile(r"@([A-Za-z][A-Za-z0-9_]{4,31})(?=[^A-Za-z0-9_]|$)")
RE_TME = re.compile(r"(?:https?://)?(?:t\.me|telegram\.me)/([A-Za-z][A-Za-z0-9_]{4,31})", re.IGNORECASE)

def utf16_slice(text, offset, length):
    b = text.encode("utf-16-le")
    return b[2 * offset: 2 * (offset + length)].decode("utf-16-le", errors="ignore")

reg = json.load(open(DATA / "channel_registry.json"))
handle2canon = {h.lower(): e["handles"][0].lower() for e in reg for h in e["handles"]}
MEMBERS = set(json.load(open(DATA / "dataset_channels.json"))["handles"])

folders = []
for f in sorted(DATA.iterdir()):
    m = re.match(r"step_(\d+)_(\w+)_", f.name)
    if m and (f / "scraped").exists():
        folders.append((int(m.group(1)), m.group(2), f))
folders.sort()

seen = set()
n_files = n_rows = n_targets = 0
with open(OUT, "w", encoding="utf-8") as out:
    for step, tag, folder in folders:
        for fp in sorted((folder / "scraped").glob("*_inspect.json")):
            handle = fp.name[: -len("_inspect.json")].lower()
            canon = handle2canon.get(handle, handle)
            if canon not in MEMBERS or canon in seen:
                continue
            seen.add(canon)
            try:
                msgs = json.load(open(fp))
            except Exception:
                continue
            n_files += 1
            if n_files % 1000 == 0:
                print(f"{n_files} channels", flush=True)
            for m in msgs:
                text = m.get("message") or ""
                found = set()
                for mt in RE_MENTION.finditer(text):
                    found.add(mt.group(1).lower())
                for mt in RE_TME.finditer(text):
                    found.add(mt.group(1).lower())
                for e in m.get("entities_json") or []:
                    etype = e.get("type")
                    if etype == "MessageEntityTextUrl" and e.get("url"):
                        mt = RE_TME.search(e["url"])
                        if mt:
                            found.add(mt.group(1).lower())
                    elif etype == "MessageEntityMention":
                        h = utf16_slice(text, e.get("offset", 0), e.get("length", 0)).lstrip("@").lower()
                        if h:
                            found.add(h)
                # canonicalise targets, drop self, keep only dataset members
                targets = sorted({tc for t in found
                                  if (tc := handle2canon.get(t, t)) != canon and tc in MEMBERS})
                if targets:
                    out.write(json.dumps({"c": canon, "m": str(m.get("id")), "t": targets}) + "\n")
                    n_rows += 1
                    n_targets += len(targets)

print(f"wrote {OUT}: {n_rows:,} messages-with-member-mentions over {n_files:,} channels "
      f"({n_targets:,} (msg,target) pairs)")
