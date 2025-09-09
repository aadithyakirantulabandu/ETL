## app/utils.py

from __future__ import annotations
import hashlib, hmac, os, time, logging
from functools import wraps
from typing import Any, Callable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/pipeline.log"),
        logging.StreamHandler()
    ],
)
logger = logging.getLogger("pipeline")

class Retryable(Exception):
    pass

def retry(times: int = 3, delay: float = 1.0):
    def deco(fn: Callable[..., Any]):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            last = None
            for i in range(times):
                try:
                    return fn(*args, **kwargs)
                except Retryable as e:
                    last = e
                    logger.warning(f"Retry {i+1}/{times} for {fn.__name__}: {e}")
                    time.sleep(delay * (2 ** i))
            raise last if last else Exception("Retry failed")
        return wrapper
    return deco

def hmac_sha256(value: str, salt: str) -> str:
    return hmac.new(salt.encode(), value.encode(), hashlib.sha256).hexdigest()

def load_yaml(path: str) -> dict:
    import yaml
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def ensure_dirs():
    for d in ("incoming", "quarantine", "masked_out", "logs"):
        os.makedirs(d, exist_ok=True)
