import os
from pathlib import Path


def data_root(run_id: str | None = None) -> Path:
    base = Path(
        os.getenv("SPECTORR_DATA_ROOT", "~/Documents/Projects/spectorr/spectorr-data")
    ).expanduser()
    return base / "runs" / run_id if run_id else base


def curated_dir(run_id: str | None = None) -> Path:
    return data_root(run_id) / "curated"


def raw_dir(run_id: str | None = None) -> Path:
    return data_root(run_id) / "raw"


def curated_cleaned_csv(run_id: str | None = None) -> Path:
    return curated_dir(run_id) / "cleaned.csv"


def curated_insights_json(run_id: str | None = None) -> Path:
    return curated_dir(run_id) / "insights.json"
