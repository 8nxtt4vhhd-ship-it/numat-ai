import json
from pathlib import Path

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=BASE_DIR / ".env")

from data_sources import get_filemaker_order_limit, write_filemaker_orders_cache  # noqa: E402
from filemaker import fetch_order_records  # noqa: E402


def main_entry():
    result = fetch_order_records(limit=get_filemaker_order_limit())
    if result.get("status") == "ok":
        write_filemaker_orders_cache(result)
    print(json.dumps({
        "status": result.get("status", "unknown"),
        "order_count": len(result.get("orders") or []),
        "saved": result.get("status") == "ok",
    }))
    if result.get("status") != "ok":
        raise SystemExit(1)


if __name__ == "__main__":
    main_entry()
