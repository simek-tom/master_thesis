"""Stage B: all-pairs similarity over the global unique-text embeddings.

Matmul on MPS; pair extraction on CPU via numpy (torch.nonzero on MPS is
pathologically slow — it turned a ~15-min job into ~10h on the first attempt).
Triangle-only: query block rows i..i+B against columns i..N, keep local c > r.
Extracts every pair with cosine >= EXTRACT_TH (0.85 = sensitivity floor; the
0.90 analysis threshold filters downstream).

Checkpointed: pairs are flushed to chunk files every FLUSH blocks; a restart
resumes after the last complete chunk. Final output merges the chunks.

Output (data/analysis/):
  unique_pairs.npz          i (int32), j (int32), sim (float16)
  unique_pairs_chunks/      intermediate chunks (removed on success)
"""
from __future__ import annotations
import time
from pathlib import Path
import numpy as np
import torch

ROOT = Path(__file__).resolve().parents[1]
A = ROOT / "data" / "analysis"; EMB = A / "emb"
CHUNKS = A / "unique_pairs_chunks"; CHUNKS.mkdir(exist_ok=True)
EXTRACT_TH = 0.85
B = 256
FLUSH = 400            # blocks per checkpoint chunk (~100k rows)

E16 = np.load(EMB / "global_emb.f16.npy")
N, D = E16.shape
print(f"global matrix: {N:,} x {D}", flush=True)
E = torch.from_numpy(E16).to("mps", torch.float32)
torch.mps.synchronize()

done_chunks = sorted(int(p.stem.split("_")[1]) for p in CHUNKS.glob("chunk_*.npz"))
start_blk = (done_chunks[-1] + 1) * FLUSH if done_chunks else 0
if start_blk:
    print(f"resuming after chunk {done_chunks[-1]} (row {start_blk * B:,})", flush=True)

buf_i, buf_j, buf_s = [], [], []
n_pairs = 0
t0 = time.time()


def flush(chunk_no):
    global buf_i, buf_j, buf_s
    np.savez(CHUNKS / f"chunk_{chunk_no:05d}.npz",
             i=np.concatenate(buf_i) if buf_i else np.array([], np.int32),
             j=np.concatenate(buf_j) if buf_j else np.array([], np.int32),
             sim=np.concatenate(buf_s) if buf_s else np.array([], np.float16))
    buf_i, buf_j, buf_s = [], [], []


# NB: always matmul against the FULL transpose view. E[i:].T (offset slice) forces
# MPS to re-copy ~2.3GB per block — that alone turned this into a multi-hour job.
ET = E.T
n_blocks = (N + B - 1) // B
for blk in range(start_blk, n_blocks):
    tb = time.time()
    i = blk * B
    sims = E[i:i+B] @ ET                         # (b, N), contiguous
    mask = sims >= EXTRACT_TH                    # GPU compare (fast)
    m = mask.cpu().numpy()                       # bool copy: 4x smaller than f32
    del mask
    r, c = np.nonzero(m)
    keep = c > (r + i)                           # strict global upper triangle
    r, c = r[keep], c[keep]
    if len(r):
        flat = torch.from_numpy(r.astype(np.int64) * N + c.astype(np.int64)).to("mps")
        vals = torch.index_select(sims.view(-1), 0, flat).cpu().numpy()
        buf_i.append((r + i).astype(np.int32))
        buf_j.append(c.astype(np.int32))
        buf_s.append(vals.astype(np.float16))
        n_pairs += len(r)
        del flat
    del sims, m
    if blk < start_blk + 5:
        print(f"  block {blk}: {time.time()-tb:.2f}s", flush=True)
    if (blk + 1) % FLUSH == 0:
        flush(blk // FLUSH)
        torch.mps.empty_cache()
    if blk % 100 == 0:
        el = time.time() - t0
        done_frac = 1 - (N - i) ** 2 / N ** 2
        eta = el / max(done_frac, 1e-9) * (1 - done_frac)
        print(f"  row {i:,}/{N:,}  ({100*done_frac:.0f}% of work, {n_pairs:,} pairs, "
              f"{el:.0f}s, ETA {eta/60:.0f}min)", flush=True)
if buf_i or not (CHUNKS / f"chunk_{(n_blocks - 1) // FLUSH:05d}.npz").exists():
    flush((n_blocks - 1) // FLUSH)

parts = [np.load(p) for p in sorted(CHUNKS.glob("chunk_*.npz"))]
I = np.concatenate([p["i"] for p in parts])
J = np.concatenate([p["j"] for p in parts])
S = np.concatenate([p["sim"] for p in parts])
np.savez_compressed(A / "unique_pairs.npz", i=I, j=J, sim=S,
                    extract_threshold=np.float32(EXTRACT_TH))
for p in CHUNKS.glob("chunk_*.npz"):
    p.unlink()
CHUNKS.rmdir()
print(f"\ndone: {len(I):,} pairs >= {EXTRACT_TH} in {(time.time()-t0)/60:.1f} min "
      f"(>=0.90: {int((S >= 0.90).sum()):,}, >=0.95: {int((S >= 0.95).sum()):,})")
