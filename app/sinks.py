## app/sinks.py

from __future__ import annotations
import os
import pandas as pd
from sqlalchemy import create_engine
import requests


def to_parquet(df: pd.DataFrame, path: str, mode: str = "append"):
    if mode == "overwrite" or not os.path.exists(path):
        df.to_parquet(path, index=False)
    else:
        old = pd.read_parquet(path)
        pd.concat([old, df], ignore_index=True).to_parquet(path, index=False)


def to_sqlite(df: pd.DataFrame, uri: str, table: str):
    eng = create_engine(uri)
    df.to_sql(table, eng, if_exists="append", index=False)


def powerbi_push(df: pd.DataFrame, dataset_url: str):
    payload = {"rows": df.to_dict(orient="records")}
    r = requests.post(dataset_url, json=payload, timeout=10)
    r.raise_for_status()

