## app/pipeline.py

from __future__ import annotations
import os
import pandas as pd
from .utils import logger, load_yaml
from . import deid, qc
from .alerts import send_email, send_slack
from .sinks import to_parquet, to_sqlite, powerbi_push


def read_input(path: str, cfg: dict) -> pd.DataFrame:
    fmt = cfg.get("input_format", "csv")
    if fmt == "jsonl":
        return pd.read_json(path, lines=True)
    return pd.read_csv(path)


def enforce_schema(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    req = set(cfg["schema"]["required_columns"]) if cfg.get("schema") else set()
    missing = req - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")
    types = cfg["schema"].get("types", {})
    for col, t in types.items():
        if col not in df.columns: continue
        if t == "date":
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
        elif t == "datetime":
            df[col] = pd.to_datetime(df[col], errors="coerce")
        elif t == "float":
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif t == "string":
            df[col] = df[col].astype(str)
    return df


def clean(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    if "zip" in df.columns and cfg["cleaning"].get("zip_pad_left", True):
        df["zip"] = df["zip"].astype(str).str.zfill(cfg["cleaning"].get("zip_length", 5))
    df = qc.clip_ranges(df, cfg["cleaning"].get("clip_ranges", {}))
    return df


def quality_checks(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    df = qc.outlier_flags(df, cfg)
    return df


def sink(df: pd.DataFrame, cfg: dict):
    sinks = cfg.get("sinks", {})
    if sinks.get("parquet", {}).get("enabled"):
        to_parquet(df, sinks["parquet"]["path"], sinks["parquet"].get("mode", "append"))
    if sinks.get("sqlite", {}).get("enabled"):
        to_sqlite(df, sinks["sqlite"]["uri"], sinks["sqlite"]["table"])
    if sinks.get("powerbi_push", {}).get("enabled"):
        url = os.getenv(sinks["powerbi_push"]["dataset_url_env"], "")
        if url:
            powerbi_push(df, url)


def process_file(path: str, cfg_path: str = "config.yaml"):
    cfg = load_yaml(cfg_path)
    try:
        raw = read_input(path, cfg)
        raw = enforce_schema(raw, cfg)
        raw = clean(raw, cfg)
        qc_done = quality_checks(raw, cfg)
        masked = deid.apply_safe_harbor(qc_done, cfg=cfg)
        outlier_action = cfg["outliers"].get("action", "flag")
        if outlier_action == "quarantine" and (masked.get("outlier_flags", "") != "").any():
            raise ValueError("Outliers detected; quarantining file per config")
        sink(masked, cfg)
        logger.info(f"Processed OK: {path} -> {len(masked)} records")
    except Exception as e:
        logger.exception(f"Failed processing {path}: {e}")
        base = os.path.basename(path)
        qpath = os.path.join("quarantine", base)
        try:
            import shutil; shutil.move(path, qpath)
        except Exception:
            pass
        send_email("ETL Failure", f"File: {path}\nError: {e}")
        send_slack(f":rotating_light: ETL failure for {path}: {e}")
        return False
    else:
        return True

