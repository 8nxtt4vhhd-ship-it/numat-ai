import json
from pathlib import Path

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=BASE_DIR / ".env")

import main  # noqa: E402


def main_entry():
    payload = main.get_cached_weekly_kpi_dashboard_payload(force_refresh=True)
    with main._WEEKLY_KPI_PAYLOAD_CACHE_LOCK:
        saved_at = main._WEEKLY_KPI_PAYLOAD_CACHE.get("saved_at")
    print(json.dumps({
        "status": payload.get("status", "unknown"),
        "saved_at": saved_at,
        "cache_path": str(main.get_weekly_kpi_payload_cache_path()),
    }))


if __name__ == "__main__":
    main_entry()
