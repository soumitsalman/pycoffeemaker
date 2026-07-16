#!/usr/bin/env python3
"""Test ClassificationCache.search at different distance thresholds.

  .venv/bin/python tests/test_clscache_search.py
  .venv/bin/python tests/test_clscache_search.py -n 50 --top-k 5 --distances 0.1,0.2,0.3,0.4
"""

from __future__ import annotations

import argparse
import os
import random
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as ipc

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from utils.env import load_coffeemaker_env

load_coffeemaker_env(str(ROOT))

from workers.states import BEANS
from workers.workercache.clscache import EMBEDDING, ID, ClassificationCache

CACHE_PATH = os.getenv("CLASSIFICATION_CACHE", str(ROOT / ".cache" / "clscache"))


def sample_uids(coll_path: str, n: int) -> list[str]:
    files = sorted(Path(coll_path).rglob("scalar*.ipc"))
    if not files:
        raise SystemExit(f"no scalar*.ipc under {coll_path}")

    uids: list[str] = []
    for path in files:
        with pa.memory_map(str(path), "r") as src:
            try:
                table = ipc.open_file(src).read_all()
            except Exception:
                src.seek(0)
                table = ipc.open_stream(src).read_all()
        uids.extend(table.column("_zvec_uid_").to_pylist())
        if len(uids) >= n * 5:
            break

    if not uids:
        raise SystemExit("no uids found")
    return random.sample(uids, min(n, len(uids)))


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("-n", type=int, default=100, help="number of samples (default: 100)")
    p.add_argument("--top-k", type=int, default=5, help="search top_k (default: 5)")
    p.add_argument(
        "--distances",
        default="0.1,0.2,0.3,0.4,0.5,0.6",
        help="comma-separated distances (default: 0.1,0.2,0.3,0.4,0.5,0.6)",
    )
    p.add_argument("--cache", default=CACHE_PATH, help=f"clscache path (default: {CACHE_PATH})")
    args = p.parse_args()

    distances = [float(x) for x in args.distances.split(",") if x.strip()]
    cache = ClassificationCache(args.cache, {BEANS: {"id_key": "url", "distance_func": "l2"}})
    coll = cache.collections[BEANS]

    docs = list(coll.fetch(sample_uids(coll.path, args.n)).values())
    print(f"docs={coll.stats.doc_count}  samples={len(docs)}  top_k={args.top_k}")
    print(f"distances={distances}\n")

    # hits[distance] = list of hit-counts across samples
    hits: dict[float, list[int]] = {d: [] for d in distances}

    for doc in docs:
        emb = list(doc.vectors[EMBEDDING])
        url = doc.fields[ID]
        print(f"url={url}")
        for d in distances:
            urls = cache.search(BEANS, emb, distance=d, top_n=args.top_k)
            if len(urls) <= 1:  continue
            
            hits[d].append(len(urls))
            print(f"  distance={d:g}  hits={len(urls)}")
            for u in urls:
                print(f"    {u}")
        print()

    print("summary (avg hits / sample)")
    for d in distances:
        vals = hits[d]
        avg = sum(vals) / len(vals)
        print(f"  distance={d:g}  avg={avg:.2f}  min={min(vals)}  max={max(vals)}")

    cache.collections.clear()


if __name__ == "__main__":
    main()
