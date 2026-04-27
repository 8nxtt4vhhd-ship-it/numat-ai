import csv
import io
import json
import os
import time
from pathlib import Path

from dotenv import load_dotenv

from analysis import orders as mock_orders
from filemaker import (
    fetch_order_records,
    get_field_value,
    get_filemaker_config,
    map_filemaker_record_to_order,
)

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_SAMPLE_CSV_PATH = BASE_DIR / "data" / "sample_orders.csv"
DEFAULT_FILEMAKER_ORDERS_CACHE_PATH = BASE_DIR / "data" / "filemaker_orders_cache.json"
MAX_SAMPLE_ROWS = 10000
_FILEMAKER_ORDER_CACHE = {
    "key": None,
    "expires_at": 0,
    "result": None,
}

load_dotenv(dotenv_path=BASE_DIR / ".env")


def get_order_data_source():
    return os.getenv("ORDER_DATA_SOURCE", "mock").strip().lower()


def get_orders_for_analysis():
    source = get_order_data_source()

    if source == "sample_csv":
        return fetch_sample_csv_orders()

    if source == "filemaker":
        result = fetch_cached_filemaker_orders()
        return {
            "source": "filemaker",
            "status": result["status"],
            "orders": result["orders"],
            "stale": result.get("stale", False),
            "cache_updated_at": result.get("cache_updated_at", ""),
            "warning": result.get("warning", ""),
        }

    return {
        "source": "mock",
        "status": "ok",
        "orders": mock_orders,
    }


def get_filemaker_order_limit():
    raw_limit = os.getenv("FILEMAKER_ORDER_LIMIT", "1000")

    try:
        return int(raw_limit)
    except ValueError:
        return 1000


def get_filemaker_cache_seconds():
    raw_seconds = os.getenv("FILEMAKER_ORDER_CACHE_SECONDS", "120")

    try:
        return max(0, int(raw_seconds))
    except ValueError:
        return 120


def fetch_cached_filemaker_orders():
    limit = get_filemaker_order_limit()
    cache_seconds = get_filemaker_cache_seconds()
    cache_key = build_filemaker_cache_key(limit)
    now = time.time()

    if (
        cache_seconds
        and _FILEMAKER_ORDER_CACHE["key"] == cache_key
        and _FILEMAKER_ORDER_CACHE["result"] is not None
        and _FILEMAKER_ORDER_CACHE["expires_at"] > now
    ):
        return _FILEMAKER_ORDER_CACHE["result"]

    result = fetch_order_records(limit=limit)

    if result.get("status") == "ok":
        write_filemaker_orders_cache(result)

    if result.get("status") == "ok" and cache_seconds:
        _FILEMAKER_ORDER_CACHE["key"] = cache_key
        _FILEMAKER_ORDER_CACHE["expires_at"] = now + cache_seconds
        _FILEMAKER_ORDER_CACHE["result"] = result
        return result

    cached_snapshot = read_filemaker_orders_cache()

    if cached_snapshot is not None:
        return {
            "connected": False,
            "status": "ok",
            "orders": cached_snapshot.get("orders", []),
            "stale": True,
            "cache_updated_at": cached_snapshot.get("updated_at", ""),
            "warning": (
                "FileMaker is currently unavailable. Showing the last successful cached order data."
            ),
        }

    return result


def build_filemaker_cache_key(limit):
    config = get_filemaker_config()
    return (
        limit,
        config["url"],
        config["database"],
        config["orders_layout"],
        config["customer_field"],
        config["order_date_field"],
        config["amount_field"],
        tuple(config["extra_fields"]),
    )


def get_filemaker_orders_cache_path():
    raw_path = os.getenv("FILEMAKER_ORDERS_CACHE_PATH", "").strip()

    if not raw_path:
        return DEFAULT_FILEMAKER_ORDERS_CACHE_PATH

    path = Path(raw_path).expanduser()
    return path if path.is_absolute() else BASE_DIR / path


def write_filemaker_orders_cache(result):
    cache_path = get_filemaker_orders_cache_path()
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "orders": result.get("orders", []),
    }
    cache_path.write_text(json.dumps(payload), encoding="utf-8")


def read_filemaker_orders_cache():
    cache_path = get_filemaker_orders_cache_path()

    if not cache_path.exists():
        return None

    try:
        return json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def fetch_sample_csv_orders():
    path = get_sample_csv_path()

    if not path.exists():
        return {
            "source": "sample_csv",
            "status": "csv_not_found",
            "orders": [],
        }

    with path.open(newline="", encoding="utf-8-sig") as csv_file:
        reader = csv.DictReader(csv_file)
        orders = [
            map_filemaker_record_to_order({
                "recordId": str(index),
                "fieldData": row,
            })
            for index, row in enumerate(reader, start=1)
        ]

    return {
        "source": "sample_csv",
        "status": "ok",
        "orders": orders,
    }


def get_sample_csv_path():
    raw_path = os.getenv("SAMPLE_ORDERS_CSV_PATH")

    if not raw_path:
        return DEFAULT_SAMPLE_CSV_PATH

    path = Path(raw_path).expanduser()

    if path.is_absolute():
        return path

    return BASE_DIR / path


def validate_sample_csv_path(path=None):
    csv_path = path or get_sample_csv_path()

    if not csv_path.exists():
        return {
            "valid": False,
            "status": "csv_not_found",
            "path": str(csv_path),
            "row_count": 0,
            "customer_count": 0,
            "warnings": [],
            "errors": ["CSV file was not found."],
        }

    with csv_path.open(newline="", encoding="utf-8-sig") as csv_file:
        content = csv_file.read().encode("utf-8")

    result = validate_sample_csv_content(content)
    result["path"] = str(csv_path)
    return result


def validate_sample_csv_content(content):
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError:
        return {
            "valid": False,
            "status": "invalid_encoding",
            "path": None,
            "row_count": 0,
            "customer_count": 0,
            "warnings": [],
            "errors": ["CSV must be UTF-8 encoded."],
        }

    reader = csv.DictReader(io.StringIO(text))
    fieldnames = reader.fieldnames or []
    rows = list(reader)
    config = get_filemaker_config()
    required_fields = [
        config["customer_field"],
        config["order_date_field"],
        config["amount_field"],
    ]
    missing_fields = [
        field for field in required_fields
        if not has_matching_field(fieldnames, field)
    ]
    warnings = []

    if len(rows) > MAX_SAMPLE_ROWS:
        warnings.append(
            f"CSV has {len(rows)} rows. Over {MAX_SAMPLE_ROWS} rows may feel slower."
        )

    customers = {
        get_field_value(row, config["customer_field"])
        for row in rows
        if get_field_value(row, config["customer_field"])
    }

    return {
        "valid": not missing_fields,
        "status": "ok" if not missing_fields else "missing_required_fields",
        "path": None,
        "row_count": len(rows),
        "customer_count": len(customers),
        "warnings": warnings,
        "errors": [
            f"Missing required field: {field}"
            for field in missing_fields
        ],
    }


def has_matching_field(fieldnames, required_field):
    required_lower = required_field.lower()
    required_suffix = f"::{required_field.split('::')[-1].lower()}"

    for fieldname in fieldnames:
        field_lower = fieldname.lower()

        if field_lower == required_lower:
            return True

        if field_lower.endswith(required_suffix):
            return True

    return False
