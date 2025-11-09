# src/spectorr_pipeline/e2e.py
from __future__ import annotations

import sys
from typing import Any, Dict, List

from dotenv import load_dotenv  # <— add this

from .insights.aggregator import group_daily
from .io.readers import load_cleaned
from .io.writers import write_insights
from .llm.anthropic_adapter import ClaudeAdapter

load_dotenv()


def run(asset_id: str | None = None, date: str | None = None) -> List[Dict[str, Any]]:
    df = load_cleaned()
    if asset_id:
        df = df[df["asset_id"] == asset_id]
    if date:
        df = df[df["source_date"].astype(str) == date]

    groups = group_daily(df)
    adapter = ClaudeAdapter()

    results: List[Dict[str, Any]] = []
    for (aid, dstr), bundle in groups.items():
        rec = adapter.summarize(
            asset_id=aid,
            date_str=dstr,
            texts=bundle["texts"],
            avg=bundle["avg"],
            n=bundle["n"],
        )
        results.append(rec)

    write_insights(results)
    return results


if __name__ == "__main__":
    # ultra-simple arg parsing:
    # python -m spectorr_pipeline.e2e [asset_id] [YYYY-MM-DD]
    asset = sys.argv[1] if len(sys.argv) > 1 else None
    day = sys.argv[2] if len(sys.argv) > 2 else None
    out = run(asset, day)
    print(f"Wrote {len(out)} insight records to curated/insights.json")
