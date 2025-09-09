from __future__ import annotations
import os, sys, glob, time, sqlite3, traceback
from datetime import datetime, timedelta
from pathlib import Path

# Optional deps
try:
    import pandas as pd  # needed for parquet stats
except Exception:
    pd = None

# ---------- Paths ----------
ROOT = Path(__file__).resolve().parent
LOG_PATH = ROOT / "logs" / "pipeline.log"
INCOMING = ROOT / "incoming"
QUARANTINE = ROOT / "quarantine"
PARQUET_PATH = ROOT / "masked_out" / "cleaned.parquet"
SQLITE_PATH = ROOT / "masked_out" / "cleaned.sqlite"

def human(n: float) -> str:
    return f"{n:,.0f}"

def count_dir(p: Path, pattern: str = "*"):
    if not p.exists():
        return 0
    return sum(1 for _ in p.glob(pattern))

def recent_files_per_minute(p: Path, minutes: int = 5, pattern: str = "events_*.csv") -> float:
    if not p.exists():
        return 0.0
    cutoff = datetime.now() - timedelta(minutes=minutes)
    hits = 0
    for f in p.glob(pattern):
        try:
            ts = datetime.fromtimestamp(f.stat().st_mtime)
            if ts >= cutoff:
                hits += 1
        except Exception:
            pass
    return hits / max(minutes, 1)

def parquet_count(path: Path) -> int | None:
    if not path.exists():
        return None
    if pd is None:
        return -1  # indicates pandas missing
    try:
        df = pd.read_parquet(str(path))
        return int(len(df))
    except Exception:
        return -2  # indicates parquet engine issue

def sqlite_count(path: Path, table: str = "cleaned_events") -> int | None:
    if not path.exists():
        return None
    try:
        con = sqlite3.connect(str(path))
        cur = con.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        n = cur.fetchone()[0]
        con.close()
        return int(n)
    except Exception:
        return -1

def parquet_outliers(path: Path) -> tuple[int,int] | None:
    """Return (flagged_rows, total_rows) from parquet if possible."""
    if not path.exists() or pd is None:
        return None
    try:
        df = pd.read_parquet(str(path), columns=["outlier_flags"])
        total = len(df)
        flagged = int((df["outlier_flags"].fillna("") != "").sum())
        return flagged, total
    except Exception:
        return None

def tail(path: Path, lines: int = 20) -> list[str]:
    if not path.exists():
        return ["<log file not found>"]
    # Efficient tail for small logs (good enough here)
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            block = 1024
            data = b""
            while size > 0 and data.count(b"\n") <= lines:
                step = min(block, size)
                f.seek(size - step)
                data = f.read(step) + data
                size -= step
        txt = data.decode("utf-8", errors="replace").splitlines()[-lines:]
        return txt if txt else ["<empty>"]
    except Exception:
        return [traceback.format_exc()]

def main():
    print("="*70)
    print("Realtime Healthcare ETL — Health Report")
    print(f"As of: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)

    # Ingestion & files
    in_count = count_dir(INCOMING, "events_*.csv")
    q_count  = count_dir(QUARANTINE, "events_*.csv")
    rpm = recent_files_per_minute(INCOMING, minutes=5, pattern="events_*.csv")

    print(f"\nIncoming folder: {INCOMING}")
    print(f"  events_*.csv present: {human(in_count)}")
    print(f"  Quarantined files:    {human(q_count)}")
    print(f"  Arrival rate (5m):    {rpm:.2f} files/min")

    # Parquet stats
    pq = parquet_count(PARQUET_PATH)
    if pq is None:
        print(f"\nParquet: {PARQUET_PATH} not found")
    elif pq == -1:
        print("\nParquet: pandas not installed (cannot compute)")
    elif pq == -2:
        print("\nParquet: engine error (pyarrow/fastparquet).")
    else:
        print(f"\nParquet rows: {human(pq)}")
        out_stats = parquet_outliers(PARQUET_PATH)
        if out_stats:
            flagged, total = out_stats
            pct = (flagged/total*100) if total else 0
            print(f"  Outlier-flagged rows: {human(flagged)} ({pct:.2f}%)")

    # SQLite stats
    sql = sqlite_count(SQLITE_PATH, "cleaned_events")
    if sql is None:
        print(f"\nSQLite: {SQLITE_PATH} not found")
    elif sql == -1:
        print("\nSQLite: query error (is the table 'cleaned_events' present?)")
    else:
        print(f"\nSQLite rows (cleaned_events): {human(sql)}")

    # Log tail
    print(f"\nLog tail: {LOG_PATH}")
    for line in tail(LOG_PATH, lines=20):
        print("  " + line)

    print("\nDone.\n")

if __name__ == "__main__":
    main()
