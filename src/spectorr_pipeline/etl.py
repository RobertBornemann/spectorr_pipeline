from __future__ import annotations

import datetime as dt
import os
import re
from pathlib import Path
from typing import Iterable, List

import pandas as pd

# Tiny lexicon for a first-pass score
LEXICON = {
    "good": 1.0,
    "great": 1.5,
    "positive": 0.8,
    "bullish": 1.2,
    "up": 0.4,
    "bad": -1.0,
    "poor": -1.2,
    "negative": -0.8,
    "bearish": -1.2,
    "down": -0.4,
}

COLUMNS = ["asset_id", "text", "source_date"]


def _clean_text(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def _simple_sentiment(text: str) -> float:
    words = re.findall(r"[a-z]+", text.lower())
    return float(sum(LEXICON.get(w, 0.0) for w in words))


def _infer_asset_from_filename(p: Path) -> str | None:
    # e.g., note_0001_AAPL.txt -> AAPL
    m = re.search(r"_([A-Z]{2,6})\.", p.name)
    return m.group(1) if m else None


def extract(sources: Iterable[str]) -> pd.DataFrame:
    """Read CSVs from raw/ (e.g., feedback.csv) and .txt from raw/analyst_notes/."""
    rows: List[pd.DataFrame] = []

    # CSVs (must have asset_id,text,source_date)
    for src in sorted(s for s in sources if s.endswith(".csv")):
        df = pd.read_csv(src)
        missing = [c for c in COLUMNS if c not in df.columns]
        if missing:
            raise ValueError(f"{src} missing columns: {missing}")
        rows.append(df[COLUMNS])

    # analyst_notes/*.txt → build rows with inferred asset_id and today’s date
    txt_dir = (
        Path(os.getenv("SPECTORR_DATA", "~/Documents/Projects/spectorr/spectorr-data")).expanduser()
        / "raw"
        / "analyst_notes"
    )
    if txt_dir.exists():
        for p in txt_dir.glob("*.txt"):
            asset = _infer_asset_from_filename(p) or "UNKNOWN"
            rows.append(
                pd.DataFrame(
                    [
                        {
                            "asset_id": asset,
                            "text": p.read_text(),
                            "source_date": dt.date.today().isoformat(),
                        }
                    ]
                )
            )

    if not rows:
        return pd.DataFrame(columns=COLUMNS)

    return pd.concat(rows, ignore_index=True)


def transform(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=COLUMNS + ["sentiment_score"])
    out = df.copy()
    out["text"] = out["text"].fillna("").map(_clean_text)
    out["sentiment_score"] = out["text"].map(_simple_sentiment)
    # basic sanity: keep only needed columns
    out = out[["asset_id", "text", "source_date", "sentiment_score"]]
    return out


def load(
    df: pd.DataFrame, out_dir: str | Path = "~/Documents/Projects/spectorr/spectorr-data/curated"
) -> Path:
    out_path = Path(out_dir).expanduser()
    out_path.mkdir(parents=True, exist_ok=True)
    target = out_path / "cleaned.csv"
    # Always write header; overwrite for determinism
    df.to_csv(target, index=False)
    return target  # <-- IMPORTANT: return a Path
