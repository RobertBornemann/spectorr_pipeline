import os
from pathlib import Path


def data_root() -> Path:
    return Path(
        os.getenv("SPECTORR_DATA_ROOT", "~/Documents/Projects/spectorr/spectorr-data")
    ).expanduser()


def curated_dir() -> Path:
    return data_root() / "curated"


def curated_cleaned_csv() -> Path:
    return curated_dir() / "cleaned.csv"


def curated_insights_json() -> Path:
    return curated_dir() / "insights.json"
