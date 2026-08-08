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

    # Idempotent -- confirms the TTL index exists (self-healing if it was
    # ever dropped or a fresh cluster hasn't had it created yet).
    storage.ensure_ttl_indexes()

    result = storage.run_maintenance()
    print(f"Cache files purged:      {result['cache_purged']}")
    print(f"Processed files archived: {result['processed_archived']}")

    print("\nLocal disk usage:")
    stats = storage.storage_stats()
    for key, val in stats.items():
        print(f"  {key:<18} {human(val)}")

    print("\nMongoDB:")
    if storage.ping():
        mongo_stats = storage.get_mongo_storage_stats()
        if "error" in mongo_stats:
            print(f"  Could not fetch stats: {mongo_stats['error']}")
        else:
            print(f"  dataSize           {human(mongo_stats['data_size_bytes'])}")
            print(f"  storageSize        {human(mongo_stats['storage_size_bytes'])}")
            print(f"  indexSize          {human(mongo_stats['index_size_bytes'])}")
            print("  Per collection:")
            for name, c in sorted(mongo_stats["collections"].items(),
                                   key=lambda kv: -kv[1]["storage_size_bytes"]):
                print(f"    {name:<20} {c['count']:>6} docs   {human(c['storage_size_bytes'])}")
    else:
        print("  Unreachable (check MONGO_URI)")

    print("=" * 60)
