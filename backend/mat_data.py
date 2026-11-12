import json
import os
from pathlib import Path
import time

from filemaker import fetch_layout_records, get_field_value


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_MAT_COUNTS_CACHE_PATH = BASE_DIR / "data" / "filemaker_mat_counts_cache.json"


def get_filemaker_mats_layout():
    return os.getenv("FILEMAKER_MATS_API_LAYOUT", "numat_ai_Mats").strip()


def get_mat_counts_cache_path():
    configured = os.getenv("FILEMAKER_MAT_COUNTS_CACHE_PATH", "").strip()
    if not configured:
        return DEFAULT_MAT_COUNTS_CACHE_PATH
    path = Path(configured).expanduser()
    return path if path.is_absolute() else BASE_DIR / path


def normalize_count(value):
    try:
        return max(0, int(float(str(value or "0").replace(",", ""))))
    except (TypeError, ValueError):
        return 0


def normalize_number(value):
    try:
        text = str(value or "0").replace(",", "").replace("$", "").strip()
        return float(text) if text else 0.0
    except (TypeError, ValueError):
        return 0.0


def display_number(value):
    numeric = normalize_number(value)
    return f"{numeric:g}" if numeric else ""


def add_mat_record_to_counts(counts_by_order, record):
    field_data = record.get("fieldData") or {}
    order_ref = str(get_field_value(field_data, "Order Ref") or "").strip()
    if not order_ref:
        return False

    outcome = str(get_field_value(field_data, "Mat Outcome") or "").strip().casefold()
    row = counts_by_order.setdefault(order_ref, {
        "mats": 0,
        "repairs": 0,
        "rejects": 0,
        "repair_actions": 0,
        "other": 0,
        "sizes": {},
    })
    row["mats"] += 1
    if outcome == "repair":
        row["repairs"] += 1
    elif outcome == "reject":
        row["rejects"] += 1
    else:
        row["other"] += 1
    row["repair_actions"] += normalize_count(get_field_value(field_data, "Rep_Total No"))
    width = normalize_number(get_field_value(field_data, "Width"))
    height = normalize_number(get_field_value(field_data, "Height"))
    square_feet = normalize_number(get_field_value(field_data, "Square Foot"))
    if not square_feet and width and height:
        square_feet = width * height / 144.0
    mat_size = str(get_field_value(field_data, "Mat Size") or "").strip()
    if not mat_size and width and height:
        mat_size = f"{display_number(width)} × {display_number(height)}"
    mat_size = mat_size or "Unspecified"
    if mat_size:
        size_row = row["sizes"].setdefault(mat_size, {
            "mats": 0,
            "repairs": 0,
            "rejects": 0,
            "repair_actions": 0,
            "repair_value": 0.0,
            "repair_value_records": 0,
            "square_feet": 0.0,
            "square_feet_records": 0,
        })
        size_row["mats"] += 1
        size_row["repair_actions"] += normalize_count(get_field_value(field_data, "Rep_Total No"))
        if outcome == "repair":
            size_row["repairs"] += 1
            repair_cost = get_field_value(field_data, "Repair Cost")
            size_row["repair_value"] += normalize_number(repair_cost)
            if str(repair_cost or "").strip():
                size_row["repair_value_records"] += 1
            if square_feet:
                size_row["square_feet"] += square_feet
                size_row["square_feet_records"] += 1
        elif outcome == "reject":
            size_row["rejects"] += 1
    return True


def sync_filemaker_mat_counts(batch_size=5000):
    batch_size = max(1, int(batch_size or 5000))
    offset = 1
    record_count = 0
    skipped_count = 0
    counts_by_order = {}

    while True:
        result = fetch_layout_records(
            get_filemaker_mats_layout(),
            limit=batch_size,
            offset=offset,
        )
        if result.get("status") != "ok":
            return {
                "status": result.get("status", "error"),
                "record_count": record_count,
                "order_count": len(counts_by_order),
                "saved": False,
            }

        records = result.get("records") or []
        for record in records:
            if add_mat_record_to_counts(counts_by_order, record):
                record_count += 1
            else:
                skipped_count += 1

        print(
            f"Mat sync: processed {record_count + skipped_count:,} records "
            f"across {len(counts_by_order):,} orders...",
            flush=True,
        )

        if len(records) < batch_size:
            break
        offset += batch_size

    payload = {
        "status": "ok",
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "layout": get_filemaker_mats_layout(),
        "record_count": record_count,
        "skipped_count": skipped_count,
        "order_count": len(counts_by_order),
        "orders": counts_by_order,
    }
    cache_path = get_mat_counts_cache_path()
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = cache_path.with_suffix(cache_path.suffix + ".tmp")
    temporary_path.write_text(json.dumps(payload), encoding="utf-8")
    temporary_path.replace(cache_path)
    return {**payload, "saved": True, "cache_path": str(cache_path)}


def read_filemaker_mat_counts_cache():
    cache_path = get_mat_counts_cache_path()
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return {
            "status": "not_synced",
            "updated_at": "",
            "record_count": 0,
            "order_count": 0,
            "orders": {},
        }
    if not isinstance(payload, dict) or not isinstance(payload.get("orders"), dict):
        return {"status": "invalid_cache", "orders": {}}
    return payload


def main_entry():
    result = sync_filemaker_mat_counts()
    print(json.dumps({
        "status": result.get("status"),
        "record_count": result.get("record_count", 0),
        "order_count": result.get("order_count", 0),
        "updated_at": result.get("updated_at", ""),
        "cache_path": result.get("cache_path", ""),
    }))
    if result.get("status") != "ok":
        raise SystemExit(1)


if __name__ == "__main__":
    main_entry()
