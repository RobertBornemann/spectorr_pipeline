# src/spectorr_pipeline/mockgen.py
from __future__ import annotations

import csv
import datetime as dt
import random
from pathlib import Path

ASSETS = ["AAPL", "MSFT", "TSLA", "AMZN", "NVDA", "GOOGL"]
POSITIVES = ["great", "bullish", "up", "positive", "strong"]
NEGATIVES = ["weak", "bearish", "down", "negative", "concern"]


def synth_line(asset: str) -> str:
    words = POSITIVES if random.random() > 0.5 else NEGATIVES
    return f"{asset} {random.choice(words)} outlook; {random.choice(words)} guidance"


def make_feedback_row(asset: str) -> dict:
    return {
        "asset_id": asset,
        "text": synth_line(asset),
        "source_date": (dt.date.today() - dt.timedelta(days=random.randint(0, 10))).isoformat(),
    }


def generate_raw(out_dir: Path, n: int = 200):
    out_dir.mkdir(parents=True, exist_ok=True)
    # Feedback CSV
    with open(out_dir / "feedback.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["asset_id", "text", "source_date"])
        w.writeheader()
        for _ in range(n):
            w.writerow(make_feedback_row(random.choice(ASSETS)))
    # Analyst notes as .txt
    notes_dir = out_dir / "analyst_notes"
    notes_dir.mkdir(exist_ok=True)
    for i in range(n):
        a = random.choice(ASSETS)
        (notes_dir / f"note_{i:04d}_{a}.txt").write_text(synth_line(a))
