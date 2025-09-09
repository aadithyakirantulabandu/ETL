"""
app package for the Realtime Healthcare ETL:
- watcher: file watcher loop
- pipeline: read → validate/clean → QC → de-ID → sinks
- qc: range clipping & outlier detection (IQR/MAD)
- deid: HIPAA Safe Harbor transforms
- sinks: parquet/sqlite/powerbi
- alerts: email/slack on failures
"""

__all__ = [
    "watcher",
    "pipeline",
    "qc",
    "deid",
    "sinks",
    "alerts",
    "schemas",
    "utils",
]

# Optional: package version
__version__ = "0.1.0"

# Optional: load environment variables early if python-dotenv is installed
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass
