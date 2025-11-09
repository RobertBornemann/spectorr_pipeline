# src/spectorr_pipeline/etl.py
from __future__ import annotations

import os
from pathlib import Path
from typing import List

import pandas as pd

from .io.paths import curated_cleaned_csv, raw_dir

REQUIRED_COLS = ["asset_id", "text", "source_date", "sentiment_score"]


def _find_raw_files() -> List[Path]:
    """Return all *.csv files under raw/ (non-recursive)."""
    rdir = raw_dir()
    rdir.mkdir(parents=True, exist_ok=True)
    return sorted(rdir.glob("*.csv"))


def _read_concat(files: List[Path]) -> pd.DataFrame:
    """Read and concatenate raw CSV files."""
    frames = []
    for f in files:
        df = pd.read_csv(f)
        frames.append(df)
    if not frames:
        raise RuntimeError(f"No raw CSV files found in {raw_dir()}")
    df = pd.concat(frames, ignore_index=True)
    return df


def _clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize schema & types:

    - Keep only required columns
    - Coerce types (date, float)
    - Drop empty text / invalid rows
    - Clip sentiment_score to [-1, 1]
    - Ensure ISO date string (YYYY-MM-DD)
    """
    # Missing columns?
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in raw data: {missing}")

    df = df[REQUIRED_COLS].copy()

    # Basic sanitization
    df["asset_id"] = df["asset_id"].astype(str).str.strip()
    df["text"] = df["text"].astype(str).str.strip()

    # Dates: to datetime (coerce), then to date
    df["source_date"] = pd.to_datetime(df["source_date"], errors="coerce").dt.date

    # Sentiment: numeric, then clip
    df["sentiment_score"] = pd.to_numeric(df["sentiment_score"], errors="coerce")
    df["sentiment_score"] = df["sentiment_score"].clip(lower=-1.0, upper=1.0)

    # Drop bad rows
    df = df.dropna(subset=["asset_id", "text", "source_date", "sentiment_score"])
    df = df[df["asset_id"] != ""]
    df = df[df["text"] != ""]

    # Write as ISO date strings (reader will parse back to date)
    df["source_date"] = df["source_date"].astype(str)

    # Optional row cap for demos (set SPECTORR_ETL_MAX_ROWS=1000)
    cap = os.getenv("SPECTORR_ETL_MAX_ROWS")
    if cap:
        try:
            n = int(cap)
            if n > 0:
                df = df.head(n)
        except Exception:
            pass

    # Final column order
    df = df[REQUIRED_COLS]
    return df


def run_etl() -> Path:
    """Main ETL entry: raw/*.csv -> curated/cleaned.csv"""
    print(f"[ETL] raw dir: {raw_dir()}", flush=True)
    files = _find_raw_files()
    if not files:
        raise RuntimeError(f"No raw CSV files found in {raw_dir()}")

    print(f"[ETL] found {len(files)} raw file(s)", flush=True)
    df_raw = _read_concat(files)
    print(f"[ETL] concatenated rows: {len(df_raw)}", flush=True)

    df = _clean(df_raw)
    print(f"[ETL] cleaned rows: {len(df)}", flush=True)

    out = curated_cleaned_csv()
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    print(f"[ETL] wrote {out}", flush=True)
    return out


def main():
    try:
        run_etl()
    except Exception as e:
        # Make sure errors are visible in SSE/subprocess logs
        print(f"[ETL][ERROR] {type(e).__name__}: {e}", flush=True)
        raise


if __name__ == "__main__":
    main()


# --- Compatibility shims for older CLI/tests ---------------------------------
def extract():
    """Old name: read raw/*.csv into a single DataFrame."""
    files = _find_raw_files()
    return _read_concat(files)


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Old name: clean/normalize the dataframe."""
    return _clean(df)


def load(df: pd.DataFrame) -> Path:
    """Old name: write curated/cleaned.csv."""
    out = curated_cleaned_csv()
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    return out
