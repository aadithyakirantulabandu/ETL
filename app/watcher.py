## app/watcher.py

from __future__ import annotations
import time, glob, os
from .utils import logger, ensure_dirs, load_yaml
from .pipeline import process_file


def run(cfg_path: str = "config.yaml"):
    ensure_dirs()
    cfg = load_yaml(cfg_path)
    pattern = os.path.join("incoming", cfg.get("file_glob", "*.csv"))
    poll = cfg["watcher"].get("poll_seconds", 3)

    seen = set()
    logger.info("Watching for new files...")
    while True:
        for path in sorted(glob.glob(pattern)):
            if path in seen: continue
            ok = process_file(path, cfg_path)
            seen.add(path)
        time.sleep(poll)

if __name__ == "__main__":
    run()
