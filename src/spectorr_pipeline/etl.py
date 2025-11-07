from typing import Iterable

import pandas as pd


def extract(sources: Iterable[str]) -> pd.DataFrame:
    # TODO: read raw analyst PDFs/emails pre-parsed from S3
    return pd.DataFrame({"asset_id": [], "text": [], "source_date": []})


def transform(df: pd.DataFrame) -> pd.DataFrame:
    # TODO: clean, tokenize, tag by asset, compute sentiment
    df = df.copy()
    df["sentiment_score"] = pd.Series(dtype="float")
    return df


def load(df: pd.DataFrame) -> None:
    # TODO: write to curated store (e.g., S3 parquet / AWS Glue table)
    pass
