## app/qc.py

from __future__ import annotations
import pandas as pd
from typing import Dict, Tuple


def clip_ranges(df: pd.DataFrame, ranges: Dict[str, Tuple[float, float]]):
    for col, (lo, hi) in ranges.items():
        if col in df:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df.loc[df[col] < lo, col] = lo
            df.loc[df[col] > hi, col] = hi
    return df


def detect_outliers_iqr(s: pd.Series, k: float = 1.5):
    q1, q3 = s.quantile(0.25), s.quantile(0.75)
    iqr = q3 - q1
    lower, upper = q1 - k * iqr, q3 + k * iqr
    return (s < lower) | (s > upper)


def detect_outliers_mad(s: pd.Series, threshold: float = 6.0):
    m = s.median()
    mad = (s - m).abs().median()
    if mad == 0:
        return pd.Series([False] * len(s), index=s.index)
    z = 0.6745 * (s - m).abs() / mad
    return z > threshold


def outlier_flags(df: pd.DataFrame, cfg: dict):
    method = cfg["outliers"].get("method", "iqr")
    flags = []
    for col in [c for c in ("systolic_bp", "diastolic_bp", "heart_rate") if c in df.columns]:
        s = pd.to_numeric(df[col], errors="coerce")
        if method == "mad":
            mask = detect_outliers_mad(s, cfg["outliers"].get("mad_threshold", 6.0))
        else:
            mask = detect_outliers_iqr(s, cfg["outliers"].get("iqr_multiplier", 1.5))
        flags.append(mask.rename(f"flag_{col}"))
    if flags:
        F = pd.concat(flags, axis=1)
        any_out = F.any(axis=1)
        df["outlier_flags"] = (
            F.apply(lambda r: ",".join([c for c, v in r.items() if v]), axis=1).where(any_out, "")
        )
    return df

