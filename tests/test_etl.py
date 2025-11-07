import pandas as pd

from spectorr_pipeline.etl import transform


def test_transform_schema():
    df = pd.DataFrame({"asset_id": ["A"], "text": ["Hello"], "source_date": ["2025-01-01"]})
    out = transform(df)
    assert "sentiment_score" in out.columns
