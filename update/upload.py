import os
import math
from datetime import date, datetime
from typing import Iterable

import pandas as pd
from pandas.api import types as pdt
from supabase import create_client


def _normalize_dates(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    for col in normalized.columns:
        series = normalized[col]
        if pdt.is_datetime64_any_dtype(series) or pdt.is_period_dtype(series):
            normalized[col] = series.dt.strftime("%Y-%m-%d")
            continue
        if pdt.is_object_dtype(series):
            normalized[col] = series.map(
                lambda value: (
                    value.strftime("%Y-%m-%d")
                    if isinstance(value, (date, datetime, pd.Timestamp))
                    else value
                )
            )
    return normalized


def _chunk_iterable(items: list, size: int) -> Iterable[list]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _json_compatible_value(value):
    if pd.isna(value):
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _json_compatible_records(records: list[dict]) -> list[dict]:
    return [
        {key: _json_compatible_value(value) for key, value in record.items()}
        for record in records
    ]


def _supabase_client():
    sb_url = os.environ["SUPABASE_URL"]
    sb_key = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
    return create_client(sb_url, sb_key)


def upload_dataset(
    name: str, df: pd.DataFrame, keys: list[str], chunk_size: int = 5000
) -> None:
    sb = _supabase_client()
    normalized = _normalize_dates(df)
    records = _json_compatible_records(normalized.to_dict(orient="records"))
    on_conflict = ",".join(keys)

    for chunk in _chunk_iterable(records, chunk_size):
        sb.table(name).upsert(chunk, on_conflict=on_conflict).execute()
