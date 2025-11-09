# src/spectorr_pipeline/mockgen.py
from __future__ import annotations

import argparse
import csv
import random
from pathlib import Path

from .io.paths import raw_dir


def generate(n: int) -> Path:
    out_dir = raw_dir()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "notes.csv"
    with out_file.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["asset_id", "text", "source_date", "sentiment_score"])
        assets = ["AAPL", "AMZN", "GOOGL", "MSFT", "NVDA", "TSLA"]
        for _ in range(n):
            a = random.choice(assets)
            # ... emit a synthetic row ...
            w.writerow([a, "synthetic note ...", "2025-11-08", round(random.uniform(-1, 1), 2)])
    return out_file


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--n", type=int, default=200)
    args = p.parse_args()
    path = generate(args.n)
    print(f"[MOCKGEN] wrote {path}", flush=True)


if __name__ == "__main__":
    main()


# add at bottom (or near generate)
def generate_raw(n: int):
    """Backward compatible alias for old CLI/tests."""
    return generate(n)
