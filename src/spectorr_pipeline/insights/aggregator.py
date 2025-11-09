import statistics
from typing import Dict, Tuple

import pandas as pd


def group_daily(df: pd.DataFrame) -> Dict[Tuple[str, str], Dict]:
    """
    Returns {(asset_id, date_str): {"texts": [...], "avg": float, "n": int}}
    """
    out = {}
    for (asset, day), g in df.groupby(["asset_id", "source_date"]):
        texts = list(g["text"].astype(str).values)
        scores = list(g["sentiment_score"].astype(float).values)
        avg = statistics.fmean(scores) if scores else 0.0
        out[(str(asset), str(day))] = {"texts": texts, "avg": avg, "n": len(texts)}
    return out
