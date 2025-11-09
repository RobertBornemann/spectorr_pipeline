import pandas as pd

from .paths import curated_cleaned_csv


def load_cleaned(run_id: str | None = None) -> pd.DataFrame:
    df = pd.read_csv(curated_cleaned_csv(run_id))
    df["source_date"] = pd.to_datetime(df["source_date"]).dt.date
    return df
