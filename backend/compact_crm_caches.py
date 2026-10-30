"""One-time compaction for legacy oversized FileMaker CRM JSON caches.

Run with the web service stopped so the compactor has the VM memory to itself.
The files are derived caches; writes use atomic replacement and preserve all
activity metadata while bounding email bodies used for timeline previews.
"""

import gc
import json
from pathlib import Path

from crm import (
    compact_crm_activities_in_place,
    get_filemaker_crm_cache_path,
    get_filemaker_crm_recent_cache_max_activities,
    get_filemaker_crm_recent_cache_path,
    sort_crm_activities,
)


def compact_cache(path: Path, recent=False):
    if not path.exists():
        print(f"skip missing {path}")
        return

    before = path.stat().st_size
    with path.open(encoding="utf-8") as source:
        payload = json.load(source)

    activities = compact_crm_activities_in_place(payload.get("activities", []))
    if recent:
        activities = sort_crm_activities(activities)[
            :get_filemaker_crm_recent_cache_max_activities()
        ]
    payload["activities"] = activities
    counts = dict(payload.get("counts") or {})
    counts["kept_rows"] = len(activities)
    if recent:
        counts["total_rows"] = len(activities)
    payload["counts"] = counts

    temporary_path = path.with_suffix(path.suffix + ".compact.tmp")
    with temporary_path.open("w", encoding="utf-8") as destination:
        json.dump(payload, destination, separators=(",", ":"))
    temporary_path.replace(path)
    after = path.stat().st_size
    print(
        f"compacted {path.name}: {before / 1024 / 1024:.1f} MB -> "
        f"{after / 1024 / 1024:.1f} MB ({len(activities)} activities)"
    )

    del payload, activities
    gc.collect()


if __name__ == "__main__":
    compact_cache(get_filemaker_crm_cache_path())
    compact_cache(get_filemaker_crm_recent_cache_path(), recent=True)
