import json
from typing import Any, Dict, List

from .paths import curated_insights_json


def write_insights(records: List[Dict[str, Any]]) -> None:
    out_path = curated_insights_json()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
