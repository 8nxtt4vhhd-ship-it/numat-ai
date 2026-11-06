import json
from pathlib import Path

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=BASE_DIR / ".env")

from data_sources import (  # noqa: E402
    clear_filemaker_orders_cache,
    get_filemaker_orders_cache_path,
    get_orders_for_analysis,
)
from filemaker import (  # noqa: E402
    clear_filemaker_master_data_cache,
    fetch_filemaker_master_data,
    get_filemaker_master_cache_path,
)
from mat_data import sync_filemaker_mat_counts  # noqa: E402


def read_cache_backup(path):
    try:
        return path.read_bytes()
    except FileNotFoundError:
        return None


def restore_cache_backup(path, backup):
    if backup is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(path.suffix + ".restore")
    temporary_path.write_bytes(backup)
    temporary_path.replace(path)


def refresh_customer_and_order_data():
    order_cache_path = get_filemaker_orders_cache_path()
    order_cache_backup = read_cache_backup(order_cache_path)
    clear_filemaker_orders_cache(clear_persisted=True)
    order_result = get_orders_for_analysis()
    if order_result.get("status") != "ok":
        restore_cache_backup(order_cache_path, order_cache_backup)

    master_cache_path = get_filemaker_master_cache_path()
    master_cache_backup = read_cache_backup(master_cache_path)
    clear_filemaker_master_data_cache(clear_persisted=True)
    customer_result = fetch_filemaker_master_data(force_refresh=True)
    if customer_result.get("status") != "ok":
        restore_cache_backup(master_cache_path, master_cache_backup)

    mat_result = sync_filemaker_mat_counts()

    return {
        "status": (
            "ok"
            if (
                order_result.get("status") == "ok"
                and customer_result.get("status") == "ok"
                and mat_result.get("status") == "ok"
            )
            else "error"
        ),
        "orders": {
            "status": order_result.get("status", "unknown"),
            "count": len(order_result.get("orders") or []),
            "cache_updated_at": order_result.get("cache_updated_at", ""),
        },
        "customers": {
            "status": customer_result.get("status", "unknown"),
            "count": len(customer_result.get("companies") or []),
            "contact_count": len(customer_result.get("all_contacts") or []),
        },
        "mats": {
            "status": mat_result.get("status", "unknown"),
            "count": mat_result.get("record_count", 0),
            "order_count": mat_result.get("order_count", 0),
            "cache_updated_at": mat_result.get("updated_at", ""),
        },
    }


def main_entry():
    result = refresh_customer_and_order_data()
    print(json.dumps(result))
    if result["status"] != "ok":
        raise SystemExit(1)


if __name__ == "__main__":
    main_entry()
