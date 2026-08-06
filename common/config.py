"""
common/config.py
=================
Single source of truth for UERIS storage-layer configuration.

Every path, threshold, and tunable used by the storage layer lives here,
driven by environment variables with sane local-dev defaults. Nothing
else in the project should hardcode a folder path or a retention window —
import from here instead.

Environment variables
----------------------
    UERIS_ROOT               Project root (default: parent of this file's
                              parent, i.e. auto-detected)
    UERIS_MODELS_DIR          Trained model files            (default: <root>/models)
    UERIS_ARCHIVES_DIR        Cold storage for old data       (default: <root>/archives)
    UERIS_PROCESSED_DIR       Intermediate processed datasets (default: <root>/processed)
    UERIS_CACHE_DIR           Short-lived scratch files       (default: <root>/cache)
    UERIS_LOGS_DIR            Application logs                (default: <root>/logs)
    UERIS_CACHE_MAX_AGE_DAYS  Cache files older than this get purged (default: 3)
    UERIS_ARCHIVE_AFTER_DAYS  Processed files older than this get archived (default: 30)
    UERIS_LOG_LEVEL           DEBUG / INFO / WARNING / ERROR   (default: INFO)
"""

import os
from pathlib import Path


def _env_path(var_name: str, default: Path) -> Path:
    raw = os.environ.get(var_name)
    return Path(raw).resolve() if raw else default.resolve()


def _env_int(var_name: str, default: int) -> int:
    try:
        return int(os.environ.get(var_name, default))
    except (TypeError, ValueError):
        return default


PROJECT_ROOT = Path(os.environ.get("UERIS_ROOT", Path(__file__).resolve().parent.parent))

MODELS_DIR    = _env_path("UERIS_MODELS_DIR",    PROJECT_ROOT / "models")
ARCHIVES_DIR  = _env_path("UERIS_ARCHIVES_DIR",  PROJECT_ROOT / "archives")
PROCESSED_DIR = _env_path("UERIS_PROCESSED_DIR", PROJECT_ROOT / "processed")
CACHE_DIR     = _env_path("UERIS_CACHE_DIR",     PROJECT_ROOT / "cache")
LOGS_DIR      = _env_path("UERIS_LOGS_DIR",      PROJECT_ROOT / "logs")

CACHE_MAX_AGE_DAYS  = _env_int("UERIS_CACHE_MAX_AGE_DAYS", 3)
ARCHIVE_AFTER_DAYS  = _env_int("UERIS_ARCHIVE_AFTER_DAYS", 30)
LOG_LEVEL           = os.environ.get("UERIS_LOG_LEVEL", "INFO").upper()

# All folders the storage layer owns. Kept as one tuple so both
# StorageManager (at runtime) and setup scripts (once, at install time)
# use the identical list -- no duplicated folder lists to drift apart.
MANAGED_DIRS = (MODELS_DIR, ARCHIVES_DIR, PROCESSED_DIR, CACHE_DIR, LOGS_DIR)


def ensure_dirs() -> None:
    """Create every managed folder (and a models/<city> placeholder pattern
    is created lazily per-city by StorageManager.save_model)."""
    for d in MANAGED_DIRS:
        d.mkdir(parents=True, exist_ok=True)
