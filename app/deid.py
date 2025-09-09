## app/deid.py
from __future__ import annotations
import os
import pandas as pd
from .utils import hmac_sha256

# HIPAA Safe Harbor transforms

def apply_safe_harbor(df: pd.DataFrame, *, cfg: dict) -> pd.DataFrame:
    df = df.copy()
    salt = os.getenv(cfg["hipaa_safe_harbor"]["hash_salt_env"], "")
    id_col = cfg["hipaa_safe_harbor"]["hash_id_column"]

    # Hash patient_id -> patient_key
    df["patient_key"] = df[id_col].astype(str).apply(lambda v: hmac_sha256(v, salt))

    # Dates
    if cfg["hipaa_safe_harbor"]["dates"].get("dob") == "year_only" and "dob" in df:
        df["dob_year"] = pd.to_datetime(df["dob"], errors="coerce").dt.year
    if cfg["hipaa_safe_harbor"]["dates"].get("event_ts") == "date_only" and "event_ts" in df:
        df["event_date"] = pd.to_datetime(df["event_ts"], errors="coerce").dt.date

    # ZIP to ZIP3
    if cfg["hipaa_safe_harbor"].get("zip_truncate_to_3", True) and "zip" in df:
        df["zip3"] = df["zip"].astype(str).str.zfill(5).str[:3]

    # Remove direct identifiers
    cols_to_remove = set(cfg["hipaa_safe_harbor"].get("remove", [])) | {id_col, "dob", "zip", "event_ts"}
    df = df.drop(columns=[c for c in cols_to_remove if c in df.columns], errors="ignore")
    return df
