import pandas as pd

from .paths import curated_cleaned_csv


def load_cleaned() -> pd.DataFrame:
    df = pd.read_csv(curated_cleaned_csv())
    # expected columns: asset_id, text, source_date, sentiment_score
    df["source_date"] = pd.to_datetime(df["source_date"]).dt.date
    return df
