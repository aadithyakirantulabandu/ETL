from __future__ import annotations
import sys, random
import pandas as pd
from pathlib import Path

"""
Noise Injector for Synthea CSVs
- Adds missingness (ZIP, DOB, numeric values)
- Injects out-of-range lab spikes (glucose, sodium, potassium, creatinine, hemoglobin)
- Swaps systolic/diastolic BP to simulate errors
- Skews timestamps to simulate late-arriving data
- Emits duplicates for deduplication testing

Usage:
  python -m app.noise_injector <synthea_csv_dir> <incoming_dir>
"""

RAND = random.Random(42)

LAB_MAP = {
    "GLUCOSE": {"loinc": ["2345-7", "2339-0"], "outliers": (20, 600)},
    "SODIUM": {"loinc": ["2951-2"], "outliers": (100, 180)},
    "POTASSIUM": {"loinc": ["2823-3"], "outliers": (2.2, 9.9)},
    "CREATININE": {"loinc": ["2160-0"], "outliers": (0.1, 12.0)},
    "HEMOGLOBIN": {"loinc": ["718-7"], "outliers": (3.0, 23.0)},
}

def _inject_missing(series: pd.Series, frac: float):
    """Randomly replace a fraction of values with None"""
    if len(series) == 0: 
        return series
    k = max(1, int(len(series) * frac))
    idx = RAND.sample(list(series.index), k=k)
    series.loc[idx] = None
    return series

def _perturb_numeric(series: pd.Series, frac_outliers: float, lo: float, hi: float):
    """Replace a fraction of numeric values with extreme outliers"""
    idx_pool = list(series.dropna().index)
    if not idx_pool: 
        return series
    k = max(1, int(len(series) * frac_outliers))
    for i in RAND.sample(idx_pool, k=k):
        series.loc[i] = lo + RAND.random() * (hi - lo)
    return series

def _maybe_swap_bp(df: pd.DataFrame, frac: float = 0.02):
    """Swap systolic/diastolic in a fraction of rows"""
    cols = {c.lower(): c for c in df.columns}
    if not {"systolic", "diastolic"}.issubset(set(cols)):
        return df
    sys_c, dia_c = cols["systolic"], cols["diastolic"]
    k = max(1, int(len(df) * frac))
    for i in RAND.sample(range(len(df)), k=k):
        s, d = df.loc[i, [sys_c, dia_c]]
        df.loc[i, [sys_c, dia_c]] = [d, s]
    return df

def _skew_timestamps(df: pd.DataFrame, time_col: str, frac: float = 0.03):
    """Shift timestamps backwards by random minutes"""
    if time_col not in df: 
        return df
    k = max(1, int(len(df) * frac))
    for i in RAND.sample(list(df.index), k=k):
        try:
            ts = pd.to_datetime(df.loc[i, time_col])
            df.loc[i, time_col] = (ts - pd.Timedelta(minutes=RAND.randint(1, 180))).isoformat()
        except Exception:
            pass
    return df

def _write_with_stamp(df: pd.DataFrame, dest_dir: Path, base: str):
    """Write a CSV with timestamp in filename"""
    dest_dir.mkdir(parents=True, exist_ok=True)
    out = dest_dir / f"{base}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S_%f')}.csv"
    df.to_csv(out, index=False)
    return out

def process(src_dir: Path, incoming_dir: Path):
    # Patients
    p = pd.read_csv(src_dir / "patients.csv")
    if "ZIP" in p.columns:
        p["ZIP"] = _inject_missing(p["ZIP"], 0.03)
    if "BIRTHDATE" in p.columns:
        p["BIRTHDATE"] = _inject_missing(p["BIRTHDATE"], 0.01)
    _write_with_stamp(p, incoming_dir, "patients")

    # Encounters
    e = pd.read_csv(src_dir / "encounters.csv")
    for col in ["START", "STOP", "DATE"]:
        if col in e.columns:
            e = _skew_timestamps(e, col, 0.05)
    _write_with_stamp(e, incoming_dir, "encounters")

    # Observations (labs & vitals)
    o = pd.read_csv(src_dir / "observations.csv")
    for col in [c for c in ["VALUE", "UNITS"] if c in o.columns]:
        o[col] = _inject_missing(o[col], 0.02)
    if "CODE" in o.columns and "VALUE" in o.columns:
        o_numeric = pd.to_numeric(o["VALUE"], errors="coerce")
        for spec in LAB_MAP.values():
            mask = o["CODE"].astype(str).isin(spec["loinc"]).values
            if mask.any():
                sub = o_numeric[mask].copy()
                _perturb_numeric(sub, 0.01, *spec["outliers"])  # 1% outliers
                o.loc[sub.index, "VALUE"] = sub
    _write_with_stamp(o, incoming_dir, "observations")

    # Optional vitals CSV
    for candidate in ["vital_signs.csv", "vitals.csv"]:
        vpath = src_dir / candidate
        if vpath.exists():
            v = pd.read_csv(vpath)
            for c in ["heart_rate", "systolic", "diastolic", "respiratory_rate", "temperature"]:
                if c in v.columns:
                    v[c] = _inject_missing(v[c], 0.02)
            v = _maybe_swap_bp(v, 0.02)
            _write_with_stamp(v, incoming_dir, Path(candidate).stem)
            break

    # Duplicates for dedup testing
    if len(o) > 0:
        dup = o.sample(frac=0.02, random_state=RAND.randint(0, 99999))
        _write_with_stamp(dup, incoming_dir, "observations_dup")

def main():
    if len(sys.argv) < 3:
        print("Usage: python -m app.noise_injector <synthea_csv_dir> <incoming_dir>")
        sys.exit(1)
    src = Path(sys.argv[1])
    dest = Path(sys.argv[2])
    if not src.exists():
        raise FileNotFoundError(src)
    process(src, dest)

if __name__ == "__main__":
    main()
