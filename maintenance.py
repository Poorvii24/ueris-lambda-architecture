"""
maintenance.py
================
Run this periodically (e.g. a daily cron job, a scheduled Render/Task
Scheduler job, or just by hand) to keep local storage tidy:

  - Purges cache/ files older than UERIS_CACHE_MAX_AGE_DAYS (default 3d)
  - Moves processed/ files older than UERIS_ARCHIVE_AFTER_DAYS (default 30d)
    into archives/<year-month>/
  - Prints a storage usage report (local disk + MongoDB) so you can see
    at a glance whether anything is trending toward a problem again

Run:
    py -3.11 maintenance.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from common.storage import StorageManager


def human(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


if __name__ == "__main__":
    storage = StorageManager()

    print("=" * 60)
    print("  UERIS storage maintenance")
    print("=" * 60)

    result = storage.run_maintenance()
    print(f"Cache files purged:      {result['cache_purged']}")
    print(f"Processed files archived: {result['processed_archived']}")

    print("\nLocal disk usage:")
    stats = storage.storage_stats()
    for key, val in stats.items():
        print(f"  {key:<18} {human(val)}")

    print("\nMongoDB:")
    if storage.ping():
        try:
            db_stats = storage.db().command("dbStats")
            print(f"  dataSize           {human(db_stats.get('dataSize', 0))}")
            print(f"  storageSize        {human(db_stats.get('storageSize', 0))}")
        except Exception as e:
            print(f"  Could not fetch dbStats: {e}")
    else:
        print("  Unreachable (check MONGO_URI)")

    print("=" * 60)
