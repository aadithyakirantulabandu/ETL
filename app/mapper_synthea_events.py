from __future__ import annotations
import sys
from pathlib import Path
import pandas as pd

"""
Build standardized 'events' rows from Synthea patients + observations.
Output schema:
patient_id, first_name, last_name, dob, zip, event_ts, systolic_bp, diastolic_bp, heart_rate
"""

# LOINC codes
LOINC_HR = "8867-4"
LOINC_SBP = "8480-6"
LOINC_DBP = "8462-4"

def load_csv(base: Path, name: str) -> pd.DataFrame:
    p = base / name
    if not p.exists():
        raise FileNotFoundError(p)
    return pd.read_csv(p)

def build_events(src_dir: Path) -> pd.DataFrame:
    # Patients
    p = load_csv(src_dir, "patients.csv")
    cols = {c.upper(): c for c in p.columns}
    pid_col = cols.get("ID", "Id")
    first_col = cols.get("FIRST", "FIRST")
    last_col  = cols.get("LAST", "LAST")
    dob_col   = cols.get("BIRTHDATE", "BIRTHDATE")
    zip_col   = cols.get("ZIP", "ZIP")

    patients = p[[pid_col, first_col, last_col, dob_col, zip_col]].rename(columns={
        pid_col: "patient_id",
        first_col: "first_name",
        last_col: "last_name",
        dob_col: "dob",
        zip_col: "zip",
    })

    # Observations (vitals)
    o = load_csv(src_dir, "observations.csv")
    o_cols = {c.upper(): c for c in o.columns}
    date_c = o_cols.get("DATE", "DATE")
    pat_c  = o_cols.get("PATIENT", "PATIENT")
    code_c = o_cols.get("CODE", "CODE")
    val_c  = o_cols.get("VALUE", "VALUE")

    keep = o[o[code_c].astype(str).isin([LOINC_HR, LOINC_SBP, LOINC_DBP])].copy()
    keep["VALUE_NUM"] = pd.to_numeric(keep[val_c], errors="coerce")
    code_map = {LOINC_HR: "heart_rate", LOINC_SBP: "systolic_bp", LOINC_DBP: "diastolic_bp"}
    keep["metric"] = keep[code_c].map(code_map)

    pivot = (keep
             .pivot_table(index=[pat_c, date_c], columns="metric", values="VALUE_NUM", aggfunc="last")
             .reset_index())

    pivot = pivot.rename(columns={pat_c: "patient_id", date_c: "event_ts"})
    events = pivot.merge(patients, on="patient_id", how="left")

    cols_out = ["patient_id", "first_name", "last_name", "dob", "zip",
                "event_ts", "systolic_bp", "diastolic_bp", "heart_rate"]
    for c in cols_out:
        if c not in events.columns:
            events[c] = pd.NA
    events = events[cols_out]

    events["event_ts"] = pd.to_datetime(events["event_ts"], errors="coerce")
    events["dob"] = pd.to_datetime(events["dob"], errors="coerce").dt.date
    return events

def main():
    if len(sys.argv) < 3:
        print("Usage: python -m app.mapper_synthea_events <synthea_csv_dir> <incoming_dir>")
        sys.exit(1)
    src = Path(sys.argv[1])
    dest = Path(sys.argv[2])
    dest.mkdir(parents=True, exist_ok=True)

    events = build_events(src)
    out = dest / f"events_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S_%f')}.csv"
    events.to_csv(out, index=False)
    print(f"Wrote {len(events)} rows -> {out}")

if __name__ == "__main__":
    main()
