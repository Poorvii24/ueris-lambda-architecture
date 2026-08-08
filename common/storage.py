"""
common/storage.py
==================
UERIS centralized Storage Manager.

This is the ONE place every layer (batch, speed, serving, streaming, ai)
should go through for:
  - MongoDB access            (delegates to common/db.py -- not reimplemented)
  - Saving/loading trained models as files (joblib/pickle/onnx), never as
    base64 blobs inside MongoDB documents
  - Archiving stale processed data instead of letting it accumulate forever
  - Purging expired cache files
  - A consistently-configured logger

Why this exists
-----------------
Before this module, trained models (IsolationForest / LOF / OneClassSVM
ensembles, forecasting models) were pickled, base64-encoded, and embedded
directly inside MongoDB documents -- in THREE separate places
(batch_processing.py, ai_batch_processor.py, and model_registry.py), with
the anomaly ensemble in ai_batch_processor.py being saved *twice*
(once by hand, once via ModelRegistry) into two different collections.
A single trained ensemble could be several MB; base64 inflates that by
~33%; multiplied across 26+ cities and re-run every batch cycle, this is
exactly what exhausts a 512MB Atlas free-tier cluster.

Models now live on disk under MODELS_DIR/<city>/<model_type>__<model_name>
__<horizon>.<ext>. MongoDB stores only a small metadata document pointing
at that file (path, format, size, metrics, timestamps) -- see
ai_layer/model_registry.py, which now uses this module internally.
"""

from __future__ import annotations

import gzip
import json
import logging
import logging.handlers
import pickle
import shutil
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from common import config
from common import db as mongo_db

try:
    import joblib
    _HAS_JOBLIB = True
except ImportError:  # joblib ships with scikit-learn, but don't hard-fail without it
    _HAS_JOBLIB = False


# ── Logging ──────────────────────────────────────────────────────────────────
_loggers: dict[str, logging.Logger] = {}


def get_logger(name: str = "ueris") -> logging.Logger:
    """Return a process-wide logger that writes to logs/<name>.log (rotating,
    5MB x 3 backups) as well as stdout. Safe to call repeatedly -- handlers
    are only attached once per logger name.
    """
    if name in _loggers:
        return _loggers[name]

    config.ensure_dirs()
    logger = logging.getLogger(f"ueris.{name}")
    logger.setLevel(getattr(logging, config.LOG_LEVEL, logging.INFO))

    if not logger.handlers:
        fmt = logging.Formatter(
            "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        file_handler = logging.handlers.RotatingFileHandler(
            config.LOGS_DIR / f"{name}.log", maxBytes=5 * 1024 * 1024, backupCount=3,
        )
        file_handler.setFormatter(fmt)
        logger.addHandler(file_handler)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(fmt)
        logger.addHandler(stream_handler)

        logger.propagate = False

    _loggers[name] = logger
    return logger


_log = get_logger("storage")


class StorageError(Exception):
    """Raised for unrecoverable storage-layer failures (bad format, missing
    file, etc). Callers should catch this rather than letting raw
    pickle/joblib/IO exceptions leak into business logic."""


class StorageManager:
    """
    Central façade for everything storage-related in UERIS.

    Usage:
        from common.storage import StorageManager
        storage = StorageManager()

        path = storage.save_model(model, city="Delhi", model_type="anomaly_ensemble",
                                   model_name="AnomalyEnsemble", horizon="realtime")
        model = storage.load_model(path)

        storage.archive_stale_processed_files()
        storage.purge_expired_cache()

    MongoDB access is exposed via .db / .client, both of which simply
    delegate to common/db.py -- this class does not open its own
    connections, so there is exactly one pooled MongoClient per process
    regardless of how many StorageManager instances exist.
    """

    def __init__(self):
        config.ensure_dirs()
        self.log = get_logger("storage")

    # ── MongoDB (delegates to common/db.py -- single source of truth) ──────
    @property
    def client(self):
        return mongo_db.get_client()

    def db(self, name: Optional[str] = None):
        return mongo_db.get_db(name)

    def ping(self) -> bool:
        return mongo_db.ping()

    # ── Model file storage ──────────────────────────────────────────────────
    def _model_path(self, city: str, model_type: str, model_name: str,
                     horizon: str, fmt: str) -> Path:
        safe_city = "".join(c if c.isalnum() else "_" for c in city)
        ext = {"joblib": "joblib", "pickle": "pkl", "onnx": "onnx"}.get(fmt, fmt)
        fname = f"{model_type}__{model_name}__{horizon}.{ext}"
        return config.MODELS_DIR / safe_city / fname

    def save_model(self, model: Any, city: str, model_type: str, model_name: str,
                    horizon: str = "monthly", fmt: str = "joblib") -> dict:
        """
        Persist a trained model object to disk. Returns a small dict
        {path, format, size_bytes} meant to be stored in MongoDB as
        metadata -- never store the model bytes themselves in Mongo.
        """
        path = self._model_path(city, model_type, model_name, horizon, fmt)
        path.parent.mkdir(parents=True, exist_ok=True)

        try:
            if fmt == "joblib":
                if not _HAS_JOBLIB:
                    raise StorageError("joblib is not installed; run: pip install joblib")
                joblib.dump(model, path, compress=3)
            elif fmt == "pickle":
                with gzip.open(path, "wb") as f:
                    pickle.dump(model, f)
            elif fmt == "onnx":
                # Caller is responsible for producing ONNX bytes (e.g. via
                # skl2onnx) and passing them as `model` when fmt="onnx".
                with open(path, "wb") as f:
                    f.write(model if isinstance(model, (bytes, bytearray)) else model.SerializeToString())
            else:
                raise StorageError(f"Unsupported model format: {fmt}")
        except Exception as e:
            self.log.error(f"save_model failed for {city}/{model_type}/{model_name}: {e}")
            raise StorageError(str(e)) from e

        size_bytes = path.stat().st_size
        rel_path = str(path.relative_to(config.PROJECT_ROOT))
        self.log.info(f"Saved model {rel_path} ({size_bytes/1024:.1f} KB)")
        return {"model_path": rel_path, "model_format": fmt, "model_size_bytes": size_bytes}

    def load_model(self, model_path: str, fmt: Optional[str] = None) -> Any:
        """Load a model previously saved with save_model(). `model_path` is
        the relative path returned in that call's metadata (or an absolute
        path -- both work)."""
        path = Path(model_path)
        if not path.is_absolute():
            path = config.PROJECT_ROOT / model_path
        if not path.exists():
            raise StorageError(f"Model file not found: {path}")

        fmt = fmt or {"joblib": "joblib", "pkl": "pickle", "onnx": "onnx"}.get(path.suffix.lstrip("."), "joblib")
        try:
            if fmt == "joblib":
                if not _HAS_JOBLIB:
                    raise StorageError("joblib is not installed; run: pip install joblib")
                return joblib.load(path)
            elif fmt == "pickle":
                with gzip.open(path, "rb") as f:
                    return pickle.load(f)
            elif fmt == "onnx":
                return path.read_bytes()
            raise StorageError(f"Unsupported model format: {fmt}")
        except Exception as e:
            self.log.error(f"load_model failed for {path}: {e}")
            raise StorageError(str(e)) from e

    def delete_model_file(self, model_path: str) -> bool:
        path = Path(model_path)
        if not path.is_absolute():
            path = config.PROJECT_ROOT / model_path
        if path.exists():
            path.unlink()
            self.log.info(f"Deleted model file {model_path}")
            return True
        return False

    # ── Processed-dataset storage (Parquet primary, CSV fallback) ──────────
    def save_dataset(self, name: str, df, fmt: str = "parquet") -> Path:
        """
        Persist an intermediate/processed dataset (a pandas DataFrame) to
        processed/ instead of MongoDB. Timestamped, so archive_stale_
        processed_files() can find and age it out later.

        fmt="parquet" (default) requires pyarrow or fastparquet -- falls
        back to CSV automatically if neither is installed, so this never
        hard-fails a batch run over a missing optional dependency.
        """
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        if fmt == "parquet":
            path = config.PROCESSED_DIR / f"{name}_{ts}.parquet"
            try:
                df.to_parquet(path, index=False)
                self.log.info(f"Saved processed dataset {path.name} "
                               f"({path.stat().st_size/1024:.1f} KB, parquet)")
                return path
            except Exception as e:
                self.log.warning(f"Parquet write failed ({e}); falling back to CSV")
                fmt = "csv"
        path = config.PROCESSED_DIR / f"{name}_{ts}.csv"
        df.to_csv(path, index=False)
        self.log.info(f"Saved processed dataset {path.name} "
                       f"({path.stat().st_size/1024:.1f} KB, csv)")
        return path

    def load_dataset(self, path):
        """Load a dataset saved with save_dataset(). Auto-detects Parquet
        vs CSV from the file extension."""
        import pandas as pd
        p = Path(path)
        if not p.is_absolute():
            p = config.PROJECT_ROOT / path
        if not p.exists():
            raise StorageError(f"Dataset file not found: {p}")
        try:
            return pd.read_parquet(p) if p.suffix == ".parquet" else pd.read_csv(p)
        except Exception as e:
            self.log.error(f"load_dataset failed for {p}: {e}")
            raise StorageError(str(e)) from e

    def save_processed(self, name: str, data: Any) -> Path:
        """For non-tabular intermediate data (dicts/lists) that doesn't fit
        a DataFrame -- e.g. a raw API response snapshot. Tabular data
        should use save_dataset() instead (Parquet/CSV, per project
        convention); this is the fallback for everything else."""
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        path = config.PROCESSED_DIR / f"{name}_{ts}.json.gz"
        with gzip.open(path, "wt", encoding="utf-8") as f:
            json.dump(data, f, default=str)
        return path

    def archive_stale_processed_files(self, older_than_days: Optional[int] = None) -> int:
        """Move processed/ files older than the retention window into
        archives/, preserving the year-month as a subfolder so archives/
        doesn't itself become one giant flat directory. Returns count moved."""
        cutoff_days = older_than_days if older_than_days is not None else config.ARCHIVE_AFTER_DAYS
        cutoff = time.time() - cutoff_days * 86400
        moved = 0
        for f in config.PROCESSED_DIR.glob("*"):
            if f.is_file() and f.stat().st_mtime < cutoff:
                month_dir = config.ARCHIVES_DIR / datetime.fromtimestamp(f.stat().st_mtime, tz=timezone.utc).strftime("%Y-%m")
                month_dir.mkdir(parents=True, exist_ok=True)
                shutil.move(str(f), str(month_dir / f.name))
                moved += 1
        if moved:
            self.log.info(f"Archived {moved} stale processed file(s) (older than {cutoff_days}d)")
        return moved

    # ── Cache cleanup ────────────────────────────────────────────────────────
    def cache_path(self, key: str) -> Path:
        safe_key = "".join(c if c.isalnum() or c in "._-" else "_" for c in key)
        return config.CACHE_DIR / safe_key

    def purge_expired_cache(self, max_age_days: Optional[int] = None) -> int:
        max_age = max_age_days if max_age_days is not None else config.CACHE_MAX_AGE_DAYS
        cutoff = time.time() - max_age * 86400
        removed = 0
        for f in config.CACHE_DIR.glob("*"):
            if f.is_file() and f.stat().st_mtime < cutoff:
                f.unlink()
                removed += 1
        if removed:
            self.log.info(f"Purged {removed} expired cache file(s) (older than {max_age}d)")
        return removed

    # ── Housekeeping entrypoint (run periodically, e.g. via cron or a
    #    scheduled task -- see maintenance.py) ──────────────────────────────
    def run_maintenance(self) -> dict:
        result = {
            "cache_purged": self.purge_expired_cache(),
            "processed_archived": self.archive_stale_processed_files(),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self.log.info(f"Maintenance run complete: {result}")
        return result

    # ── Disk usage introspection (for the /api/health / dashboard) ─────────
    def storage_stats(self) -> dict:
        def dir_size(p: Path) -> int:
            return sum(f.stat().st_size for f in p.rglob("*") if f.is_file()) if p.exists() else 0

        return {
            "models_bytes":    dir_size(config.MODELS_DIR),
            "archives_bytes":  dir_size(config.ARCHIVES_DIR),
            "processed_bytes": dir_size(config.PROCESSED_DIR),
            "cache_bytes":     dir_size(config.CACHE_DIR),
            "logs_bytes":      dir_size(config.LOGS_DIR),
        }
