from collections import defaultdict
import calendar
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import re
from threading import Lock
import time

from filemaker import (
    fetch_all_layout_records,
    find_layout_records,
    get_field_value,
    normalize_filemaker_date,
)


_PRODUCTION_CACHE = {"expires_at": 0, "result": None}
_PRODUCTIVITY_DETAIL_CACHE = {}
_PRODUCTION_CACHE_LOCK = Lock()
_PRODUCTION_DISK_CACHE_LOADED = False
_PRODUCTION_CACHE_VERSION = 1
DEFAULT_PRODUCTION_CACHE_PATH = Path(__file__).resolve().parent / "data" / "production_dashboard_cache.json"

PRODUCTION_LAYOUT_DEFAULT = "ai_Numat Production Data"
OPERATOR_LAYOUT_DEFAULT = "ai_Numat Production Data Op Data"
TIME_BOOKINGS_LAYOUT_DEFAULT = "ai_Time Bookings"
CLOCKINGS_LAYOUT_DEFAULT = "ai_Clockings Table"
CYCLES_LAYOUT_DEFAULT = "ai_Cycles"
MATS_LAYOUT_DEFAULT = "ai_Mats"
EXCLUDED_OPERATOR_KEYS = {
    "trudydunlap",
    "trudysdunlap",
    "kellybainbridge",
    "loishorace",
    "temp1",
    "temp2",
}
MANAGER_OPERATOR_KEYS = {"loishorace"}
PRODUCTIVE_DEPARTMENTS = {
    "sort",
    "grind",
    "press",
    "trim",
    "extruder",
    "bag building",
    "housekeeping",
    "engineering",
    "re-runs",
    "press room help",
}


def get_production_layout():
    return os.getenv("FILEMAKER_PRODUCTION_LAYOUT", PRODUCTION_LAYOUT_DEFAULT).strip()


def get_operator_layout():
    return os.getenv("FILEMAKER_PRODUCTION_OPERATOR_LAYOUT", OPERATOR_LAYOUT_DEFAULT).strip()


def get_time_bookings_layout():
    return os.getenv("FILEMAKER_TIME_BOOKINGS_LAYOUT", TIME_BOOKINGS_LAYOUT_DEFAULT).strip()


def get_clockings_layout():
    return os.getenv("FILEMAKER_CLOCKINGS_LAYOUT", CLOCKINGS_LAYOUT_DEFAULT).strip()


def get_cycles_layout():
    return os.getenv("FILEMAKER_CYCLES_LAYOUT", CYCLES_LAYOUT_DEFAULT).strip()


def get_mats_layout():
    return os.getenv("FILEMAKER_MATS_LAYOUT", MATS_LAYOUT_DEFAULT).strip()


def get_average_labour_rate():
    return number(os.getenv("PRODUCTION_AVERAGE_LABOUR_RATE", "22.50")) or 22.50


def get_monthly_insurance_cost():
    return number(os.getenv("PRODUCTION_MONTHLY_INSURANCE_COST", "6000")) or 6000.0


def get_production_cache_seconds():
    try:
        return max(0, int(os.getenv("FILEMAKER_PRODUCTION_CACHE_SECONDS", "300")))
    except ValueError:
        return 300


def get_production_cache_path():
    configured = os.getenv("FILEMAKER_PRODUCTION_CACHE_PATH", "").strip()
    return Path(configured).expanduser() if configured else DEFAULT_PRODUCTION_CACHE_PATH


def _load_production_disk_cache():
    global _PRODUCTION_DISK_CACHE_LOADED
    with _PRODUCTION_CACHE_LOCK:
        if _PRODUCTION_DISK_CACHE_LOADED:
            return
        _PRODUCTION_DISK_CACHE_LOADED = True
        try:
            payload = json.loads(get_production_cache_path().read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            return
        if payload.get("version") != _PRODUCTION_CACHE_VERSION:
            return
        production_result = payload.get("production_result")
        if isinstance(production_result, dict):
            _PRODUCTION_CACHE["result"] = production_result
        detail_results = payload.get("productivity_details")
        if isinstance(detail_results, dict):
            for key, result in detail_results.items():
                if "|" not in key or not isinstance(result, dict):
                    continue
                start, end = key.split("|", 1)
                _PRODUCTIVITY_DETAIL_CACHE[(start, end)] = {"expires_at": 0, "result": result}


def _save_production_disk_cache():
    path = get_production_cache_path()
    with _PRODUCTION_CACHE_LOCK:
        payload = {
            "version": _PRODUCTION_CACHE_VERSION,
            "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "production_result": _PRODUCTION_CACHE.get("result"),
            "productivity_details": {
                f"{start}|{end}": cached.get("result")
                for (start, end), cached in _PRODUCTIVITY_DETAIL_CACHE.items()
                if isinstance(cached, dict) and isinstance(cached.get("result"), dict)
            },
        }
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_suffix(path.suffix + ".tmp")
            temporary.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
            os.chmod(temporary, 0o600)
            os.replace(temporary, path)
        except (OSError, TypeError, ValueError) as error:
            print(f"Production dashboard cache write failed: {error}")


def number(value):
    if value is None or isinstance(value, bool):
        return None
    text = str(value).strip().replace(",", "")
    if not text or text in {"?", "-", "—"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def duration_hours(value):
    """Convert FileMaker time/duration values to decimal hours."""
    if value is None or isinstance(value, bool):
        return None
    if hasattr(value, "total_seconds"):
        return value.total_seconds() / 3600
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    if ":" in text:
        parts = text.split(":")
        try:
            hours, minutes = float(parts[0]), float(parts[1])
            seconds = float(parts[2]) if len(parts) > 2 else 0
            return hours + (minutes / 60) + (seconds / 3600)
        except (TypeError, ValueError):
            return None
    return number(text)


def operator_key(value):
    return re.sub(r"[^a-z0-9]", "", str(value or "").casefold())


def canonical_department(value):
    text = re.sub(r"\s+", " ", str(value or "").strip())
    key = re.sub(r"[^a-z0-9]", "", text.casefold())
    aliases = {
        "sorting": "Sort",
        "sort": "Sort",
        "grinding": "Grind",
        "grind": "Grind",
        "pressing": "Press",
        "press": "Press",
        "trimming": "Trim",
        "trim": "Trim",
        "extruder": "Extruder",
        "bagbuilding": "Bag Building",
        "housekeeping": "Housekeeping",
        "engineering": "Engineering",
        "reruns": "Re-Runs",
        "pressroomhelp": "Press Room Help",
    }
    return aliases.get(key, text or "Unassigned")


def department_rollup(value):
    department = canonical_department(value)
    return "Press" if department == "Press Room Help" else department


def normalize_time_booking_record(record):
    row = record.get("fieldData", {})
    value = lambda field: get_field_value(row, field)
    operator = str(value("Operator") or value("CreatedBy") or "").strip()
    department = canonical_department(value("Department"))
    return {
        "record_id": str(record.get("recordId") or ""),
        "primary_key": str(value("PrimaryKey") or "").strip(),
        "modified_at": str(value("ModificationTimestamp") or "").strip(),
        "date": normalize_filemaker_date(value("Date") or value("Start Time")),
        "start_time": str(value("Start Time") or "").strip(),
        "finish_time": str(value("Finish Time") or "").strip(),
        "operator": operator,
        "operator_key": operator_key(operator),
        "department": department,
        "department_rollup": department_rollup(department),
        "hours": duration_hours(value("Time")),
        "manual_hours": duration_hours(value("Manual Time")),
        "extruder_hours": duration_hours(value("Extruder Time")),
        "sales_order": str(value("Sales Order No") or "").strip(),
        "notes": str(value("Notes") or "").strip(),
        "excluded": (
            operator_key(operator) in EXCLUDED_OPERATOR_KEYS
            and operator_key(operator) not in MANAGER_OPERATOR_KEYS
        ),
        "manager_support": operator_key(operator) in MANAGER_OPERATOR_KEYS,
    }


def normalize_clocking_record(record):
    row = record.get("fieldData", {})
    value = lambda field: get_field_value(row, field)
    operator = str(value("Name") or "").strip()
    return {
        "record_id": str(record.get("recordId") or ""),
        "primary_key": str(value("PrimaryKey") or "").strip(),
        "modified_at": str(value("ModificationTimestamp") or "").strip(),
        "date": normalize_filemaker_date(value("Clock In") or value("CreationTimestamp")),
        "clock_in": str(value("Clock In") or "").strip(),
        "clock_out": str(value("Clock Out") or "").strip(),
        "operator": operator,
        "operator_key": operator_key(operator),
        "clocked_hours": duration_hours(value("Time Booked")),
        "notes": str(value("Notes") or "").strip(),
        "excluded": (
            operator_key(operator) in EXCLUDED_OPERATOR_KEYS
            and operator_key(operator) not in MANAGER_OPERATOR_KEYS
        ),
        "manager": operator_key(operator) in MANAGER_OPERATOR_KEYS,
    }


def normalize_cycle_record(record):
    row = record.get("fieldData", {})
    value = lambda field: get_field_value(row, field)
    return {
        "record_id": str(record.get("recordId") or ""),
        "primary_key": str(value("PrimaryKey") or "").strip(),
        "modified_at": str(value("ModificationTimestamp") or "").strip(),
        "date": normalize_filemaker_date(value("Start Time") or value("CreationTimestamp")),
        "machine": str(value("Machine No") or "").strip(),
        "operator": str(value("Operator") or "").strip(),
        "repair_value": number(value("Revenue in Cycle")),
        "linear_feet": number(value("Linear Feet in Cycle")),
        "cycle_hours": duration_hours(value("Time")),
    }


def prorated_monthly_cost(period_start, period_end, monthly_cost):
    start = datetime.strptime(str(period_start), "%Y-%m-%d").date()
    end = datetime.strptime(str(period_end), "%Y-%m-%d").date()
    if end < start:
        return 0.0
    total = 0.0
    current = start
    while current <= end:
        days_in_month = calendar.monthrange(current.year, current.month)[1]
        total += float(monthly_cost) / days_in_month
        current += timedelta(days=1)
    return total


def _find_all_records(layout, query, batch_size=500, max_records=20000):
    records = []
    offset = 1
    status = "ok"
    while len(records) < max_records:
        result = find_layout_records(
            layout,
            query,
            limit=min(batch_size, max_records - len(records)),
            offset=offset,
        )
        status = result.get("status", "error")
        batch = result.get("records", [])
        if status != "ok":
            break
        records.extend(batch)
        if len(batch) < batch_size:
            break
        offset += len(batch)
    return {"status": status, "records": records}


def build_productivity_detail(time_bookings, clockings):
    """Build paid-time productivity without counting summary fields twice."""
    people = defaultdict(lambda: {
        "operator": "",
        "booked_hours": 0.0,
        "clocked_hours": 0.0,
        "departments": defaultdict(float),
        "days": set(),
    })
    departments = defaultdict(lambda: {
        "booked_hours": 0.0,
        "manager_support_hours": 0.0,
        "helper_hours": 0.0,
        "operators": set(),
    })
    manager_support_hours = 0.0

    for item in clockings:
        if item.get("excluded") or item.get("manager"):
            continue
        key = item.get("operator_key") or operator_key(item.get("operator"))
        if not key:
            continue
        person = people[key]
        person["operator"] = item.get("operator") or person["operator"]
        person["clocked_hours"] += item.get("clocked_hours") or 0
        if item.get("date"):
            person["days"].add(item["date"])

    for item in time_bookings:
        if item.get("excluded"):
            continue
        hours = item.get("hours") or 0
        department = item.get("department") or "Unassigned"
        rollup = item.get("department_rollup") or department
        if item.get("manager_support"):
            departments[rollup]["manager_support_hours"] += hours
            manager_support_hours += hours
            continue
        key = item.get("operator_key") or operator_key(item.get("operator"))
        if not key:
            continue
        person = people[key]
        person["operator"] = item.get("operator") or person["operator"]
        person["booked_hours"] += hours
        person["departments"][department] += hours
        if item.get("date"):
            person["days"].add(item["date"])
        departments[rollup]["booked_hours"] += hours
        departments[rollup]["operators"].add(item.get("operator") or "")
        if department == "Press Room Help":
            departments[rollup]["helper_hours"] += hours

    operator_rows = []
    for person in people.values():
        clocked = person["clocked_hours"]
        booked = person["booked_hours"]
        operator_rows.append({
            "name": person["operator"],
            "days": len(person["days"]),
            "booked_hours": booked,
            "clocked_hours": clocked,
            "unbooked_hours": max(clocked - booked, 0),
            "overbooked_hours": max(booked - clocked, 0),
            "productivity": (booked / clocked * 100) if clocked else None,
            "departments": dict(sorted(person["departments"].items())),
        })
    operator_rows.sort(key=lambda item: (-(item.get("unbooked_hours") or 0), item["name"]))

    total_booked = sum(item["booked_hours"] for item in operator_rows if item["clocked_hours"] > 0)
    unmatched_booked = sum(item["booked_hours"] for item in operator_rows if item["clocked_hours"] <= 0)
    total_clocked = sum(item["clocked_hours"] for item in operator_rows)
    department_total_booked = sum(values["booked_hours"] for values in departments.values())
    department_rows = [
        {
            "department": department,
            "booked_hours": values["booked_hours"],
            "manager_support_hours": values["manager_support_hours"],
            "helper_hours": values["helper_hours"],
            "operators": len({item for item in values["operators"] if item}),
            "share_of_booked": (
                values["booked_hours"] / department_total_booked * 100
                if department_total_booked else None
            ),
        }
        for department, values in departments.items()
    ]
    department_rows.sort(key=lambda item: (-(item["booked_hours"] + item["manager_support_hours"]), item["department"]))
    return {
        "summary": {
            "booked_hours": total_booked,
            "clocked_hours": total_clocked,
            "unbooked_hours": max(total_clocked - total_booked, 0),
            "unmatched_booked_hours": unmatched_booked,
            "plant_productivity": (total_booked / total_clocked * 100) if total_clocked else None,
            "manager_support_hours": manager_support_hours,
        },
        "operator_rows": operator_rows,
        "department_rows": department_rows,
    }


def fetch_productivity_detail(days=90, force_refresh=False, today=None, period_start=None, period_end=None):
    today = today or datetime.now()
    end = (
        datetime.strptime(str(period_end), "%Y-%m-%d")
        if period_end else today - timedelta(days=1)
    )
    start = (
        datetime.strptime(str(period_start), "%Y-%m-%d")
        if period_start else end - timedelta(days=max(1, int(days)) - 1)
    )
    cache_key = (start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    now = time.time()
    _load_production_disk_cache()
    cached = _PRODUCTIVITY_DETAIL_CACHE.get(cache_key)
    # Production data is intentionally a manual-refresh cache. Normal page loads,
    # drilldowns and back-navigation must never trigger another FileMaker pull.
    if not force_refresh and cached:
        return cached["result"]

    filemaker_range = f'{start.strftime("%m/%d/%Y")}...{end.strftime("%m/%d/%Y")}'
    booking_result = _find_all_records(get_time_bookings_layout(), {"Date": filemaker_range})
    clocking_result = _find_all_records(get_clockings_layout(), {"Clock In": filemaker_range})
    cycle_result = _find_all_records(get_cycles_layout(), {"Start Time": filemaker_range})
    bookings = [normalize_time_booking_record(item) for item in booking_result.get("records", [])]
    clockings = [normalize_clocking_record(item) for item in clocking_result.get("records", [])]
    cycles = [normalize_cycle_record(item) for item in cycle_result.get("records", [])]
    statuses = {booking_result.get("status"), clocking_result.get("status")}
    productivity = build_productivity_detail(bookings, clockings)
    manager_clocked_hours = sum(
        item.get("clocked_hours") or 0
        for item in clockings
        if item.get("manager") and not item.get("excluded")
    )
    production_clocked_hours = productivity.get("summary", {}).get("clocked_hours") or 0
    labour_clocked_hours = production_clocked_hours + manager_clocked_hours
    hourly_rate = get_average_labour_rate()
    hourly_labour_cost = labour_clocked_hours * hourly_rate
    insurance_cost = prorated_monthly_cost(
        start.strftime("%Y-%m-%d"),
        end.strftime("%Y-%m-%d"),
        get_monthly_insurance_cost(),
    )
    press_repair_value = sum(item.get("repair_value") or 0 for item in cycles)
    productivity["summary"].update({
        "manager_clocked_hours": manager_clocked_hours,
        "labour_clocked_hours": labour_clocked_hours,
        "average_labour_rate": hourly_rate,
        "hourly_labour_cost": hourly_labour_cost,
        "insurance_cost": insurance_cost,
        "total_labour_cost": hourly_labour_cost + insurance_cost,
        "press_repair_value": press_repair_value,
        "labour_percentage": (
            (hourly_labour_cost + insurance_cost) / press_repair_value * 100
            if press_repair_value > 0 else None
        ),
    })
    result = {
        "status": "ok" if statuses == {"ok"} else "partial" if "ok" in statuses else next(iter(statuses), "error"),
        "period_start": start.strftime("%Y-%m-%d"),
        "period_end": end.strftime("%Y-%m-%d"),
        "booking_status": booking_result.get("status"),
        "clocking_status": clocking_result.get("status"),
        "cycle_status": cycle_result.get("status"),
        "booking_count": len(bookings),
        "clocking_count": len(clockings),
        "cycle_count": len(cycles),
        "time_bookings": bookings,
        "clockings": clockings,
        "cycles": cycles,
        **productivity,
    }
    if result.get("status") == "ok":
        cache_seconds = get_production_cache_seconds()
        _PRODUCTIVITY_DETAIL_CACHE[cache_key] = {"expires_at": now + cache_seconds, "result": result}
        _save_production_disk_cache()
    return result


def clear_productivity_detail_cache():
    _load_production_disk_cache()
    _PRODUCTIVITY_DETAIL_CACHE.clear()
    _save_production_disk_cache()


def normalize_production_record(record):
    row = record.get("fieldData", {})
    value = lambda field: get_field_value(row, field)
    normalized = {
        "record_id": str(record.get("recordId") or ""),
        "primary_key": str(value("PrimaryKey") or "").strip(),
        "date": normalize_filemaker_date(value("Date")),
        "creation_timestamp": str(value("CreationTimestamp") or "").strip(),
        "supervisor_notes": str(value("Supervisor Notes") or "").strip(),
    }
    numeric_fields = {
        "accumulated_revenue_target": "Accumulated Daily Revenue Target",
        "aged_debtor_days": "Aged debtor days",
        "backlog_grind": "Backlog at Grind",
        "backlog_press": "Backlog at Press",
        "backlog_sort": "Backlog at Sort",
        "backlog_trim": "Backlog at Trim",
        "backlog_weeks": "Backlog weeks",
        "grind_lf_per_hour": "Grind LF per Hour",
        "grind_throughput": "Grind LF Throughput",
        "grind_hours": "Grind production Time as decimal",
        "headcount": "Headcount",
        "incoming_skids": "Incoming skids",
        "invoiced_revenue_mtd": "Invoiced Revenue MTD",
        "labour_cost": "Labour Hours Cost",
        "labour_hours": "Labour Hours decimal",
        "labour_percentage": "Labour percentage",
        "press_lf_per_hour": "Press LF per Hour",
        "press_throughput": "Press LF Throughput",
        "press_hours": "Press production Time as decimal",
        "production_revenue_mtd": "Production Revenue MTD",
        "production_revenue_today": "Production Revenue Today",
        "recook_lf": "Recook LF",
        "sort_lf_per_hour": "Sort LF per Hour",
        "sort_throughput": "Sort LF Throughput",
        "sort_hours": "Sort production Time as decimal",
        "target_invoiced_percentage": "Target invoiced revenue percentage achieved",
        "time_booked_today": "Time booked today as decimal",
        "total_press_booking": "Total Press Time Booking inc helpers as decimal",
        "trim_lf_per_hour": "Trim LF per Hour",
        "trim_throughput": "Trim LF Throughput",
        "trim_hours": "Trim production Time as decimal",
        "utilisation_percentage": "Utilisation percentage",
    }
    for key, field in numeric_fields.items():
        normalized[key] = number(value(field))
    for press in (1, 2, 3):
        prefix = f"ff{press}_"
        filemaker_prefix = f"FF{press} "
        for key, suffix in {
            "mats": "Mats processed",
            "cycles": "no of Cycles",
            "lf": "total LF",
            "lost_hours": "Lost Time",
            "utilisation": "Production Time as decimal",
            "revenue": "Revenue",
            "avg_lf_cycle": "Avg LF per cycle",
            "avg_revenue_cycle": "Avg dollar per cycle",
        }.items():
            normalized[prefix + key] = number(value(filemaker_prefix + suffix))
    press_totals = [normalized.get(f"ff{press}_lf") for press in (1, 2, 3)]
    normalized["press_throughput_reported"] = normalized.get("press_throughput")
    if any(item is not None for item in press_totals):
        normalized["press_throughput"] = sum(item or 0 for item in press_totals)
    return normalized


def normalize_operator_record(record):
    row = record.get("fieldData", {})
    value = lambda field: get_field_value(row, field)
    name = str(value("Name") or "").strip()
    return {
        "record_id": str(record.get("recordId") or ""),
        "date": normalize_filemaker_date(value("Date")),
        "name": name,
        "excluded": operator_key(name) in EXCLUDED_OPERATOR_KEYS,
        "booked_hours": number(value("Booked Time")),
        "clocked_hours": number(value("Clocked Time")),
        "target_achieved": number(value("Target Achieved")),
        "productivity": number(value("Utilisation")),
    }


def production_snapshot_rank(item):
    timestamp = str(item.get("creation_timestamp") or "").strip()
    for pattern in ("%m/%d/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return (datetime.strptime(timestamp, pattern).timestamp(), int(item.get("record_id") or 0))
        except (TypeError, ValueError):
            continue
    try:
        return (0, int(item.get("record_id") or 0))
    except ValueError:
        return (0, 0)


def deduplicate_production_days(rows):
    latest_by_date = {}
    for item in rows:
        date_key = item.get("date")
        if not date_key:
            continue
        existing = latest_by_date.get(date_key)
        if existing is None or production_snapshot_rank(item) >= production_snapshot_rank(existing):
            latest_by_date[date_key] = item
    return sorted(latest_by_date.values(), key=lambda item: item["date"])


def fetch_production_analysis_data(force_refresh=False, include_today=False):
    now = time.time()
    if not include_today:
        _load_production_disk_cache()
    # Keep serving the last production snapshot until Apply or Refresh explicitly
    # requests fresh FileMaker data.
    if not include_today and not force_refresh and _PRODUCTION_CACHE["result"] is not None:
        return _PRODUCTION_CACHE["result"]

    sort = [{"fieldName": "Date", "sortOrder": "ascend"}]
    production_result = fetch_all_layout_records(get_production_layout(), batch_size=500, sort_fields=sort)
    operator_result = fetch_all_layout_records(get_operator_layout(), batch_size=500, sort_fields=sort)
    statuses = {production_result.get("status"), operator_result.get("status")}
    production_rows = [normalize_production_record(item) for item in production_result.get("records", [])]
    operator_rows = [normalize_operator_record(item) for item in operator_result.get("records", [])]
    today = datetime.now().strftime("%Y-%m-%d")
    date_limit = (lambda value: value <= today) if include_today else (lambda value: value < today)
    production_rows = [item for item in production_rows if item.get("date") and date_limit(item["date"])]
    operator_rows = [item for item in operator_rows if item.get("date") and date_limit(item["date"])]
    production_rows = deduplicate_production_days(production_rows)
    plant_operator_rows = list(operator_rows)
    operator_rows = [item for item in operator_rows if not item.get("excluded")]
    plant_operator_rows.sort(key=lambda item: (item["date"], item["name"]))
    operator_rows.sort(key=lambda item: (item["date"], item["name"]))
    status = "ok" if statuses == {"ok"} else "partial" if "ok" in statuses else next(iter(statuses), "error")
    warning = ""
    if status != "ok":
        warning = (
            "The production API layouts are not currently available to the FileMaker API account. "
            "Grant layout View access to both dedicated production layouts."
        )
    result = {
        "status": status,
        "warning": warning,
        "production_status": production_result.get("status"),
        "operator_status": operator_result.get("status"),
        "production_rows": production_rows,
        "plant_operator_rows": plant_operator_rows,
        "operator_rows": operator_rows,
        "synced_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    cache_seconds = get_production_cache_seconds()
    if not include_today and result.get("status") == "ok":
        _PRODUCTION_CACHE["result"] = result
        _PRODUCTION_CACHE["expires_at"] = now + cache_seconds
        _save_production_disk_cache()
    return result


def filter_production_period(result, days=90):
    rows = result.get("production_rows", [])
    operators = result.get("operator_rows", [])
    if not rows or not days:
        return rows, operators
    latest = datetime.strptime(rows[-1]["date"], "%Y-%m-%d")
    start = (latest - timedelta(days=max(1, int(days)) - 1)).strftime("%Y-%m-%d")
    return (
        [item for item in rows if item["date"] >= start],
        [item for item in operators if item["date"] >= start],
    )


def filter_production_date_range(result, period_start, period_end):
    return (
        [item for item in result.get("production_rows", []) if period_start <= item["date"] <= period_end],
        [item for item in result.get("operator_rows", []) if period_start <= item["date"] <= period_end],
    )


def build_operator_summary(operator_rows):
    grouped = defaultdict(list)
    for item in operator_rows:
        grouped[item["name"]].append(item)
    summaries = []
    for name, items in grouped.items():
        active = [item for item in items if (item.get("clocked_hours") or 0) > 0]
        def average(key):
            values = [item[key] for item in active if item.get(key) is not None]
            return sum(values) / len(values) if values else None
        summaries.append({
            "name": name,
            "days": len(active),
            "booked_hours": sum(item.get("booked_hours") or 0 for item in active),
            "clocked_hours": sum(item.get("clocked_hours") or 0 for item in active),
            "productivity": average("productivity"),
            "target_achieved": average("target_achieved"),
        })
    return sorted(summaries, key=lambda item: (-(item.get("productivity") or 0), item["name"]))


def build_production_kpi_payload(result=None, days=90, period_start=None, period_end=None):
    result = result or fetch_production_analysis_data()
    rows, operators = (
        filter_production_date_range(result, period_start, period_end)
        if period_start and period_end
        else filter_production_period(result, days=days)
    )
    plant_operators = result.get("plant_operator_rows", operators)
    if rows:
        period_start = rows[0]["date"]
        period_end = rows[-1]["date"]
        plant_operators = [
            item for item in plant_operators
            if period_start <= str(item.get("date") or "") <= period_end
        ]
    latest = rows[-1] if rows else {}
    operator_summary = build_operator_summary(operators)
    active_operator_rows = [item for item in operators if (item.get("clocked_hours") or 0) > 0]
    productivity_values = [item["productivity"] for item in active_operator_rows if item.get("productivity") is not None]
    target_values = [item["target_achieved"] for item in active_operator_rows if item.get("target_achieved") is not None]
    # Plant productivity includes every operator clocking. Exclusions apply to
    # the individual operator-performance table only; applying them here removes
    # genuine plant hours and overstates the result.
    active_plant_operator_rows = [
        item for item in plant_operators if (item.get("clocked_hours") or 0) > 0
    ]
    total_booked_hours = sum(item.get("booked_hours") or 0 for item in active_plant_operator_rows)
    total_clocked_hours = sum(item.get("clocked_hours") or 0 for item in active_plant_operator_rows)
    press_throughput_values = [
        item["press_throughput"]
        for item in rows
        if item.get("press_throughput") is not None
    ]
    production_revenue_values = [
        item["production_revenue_today"]
        for item in rows
        # Blank and zero-value days are not production days and must not
        # dilute the average daily production value.
        if (item.get("production_revenue_today") or 0) > 0
    ]
    return {
        "status": result.get("status", "error"),
        "warning": result.get("warning", ""),
        "production_status": result.get("production_status", ""),
        "operator_status": result.get("operator_status", ""),
        "synced_at": result.get("synced_at", ""),
        "days": days,
        "period_start": period_start or (rows[0]["date"] if rows else ""),
        "period_end": period_end or (rows[-1]["date"] if rows else ""),
        "rows": rows,
        "operator_rows": operators,
        "operator_summary": operator_summary,
        "latest": latest,
        "summary": {
            "plant_productivity": (
                (total_booked_hours / total_clocked_hours) * 100
                if total_clocked_hours else None
            ),
            "average_productivity": sum(productivity_values) / len(productivity_values) if productivity_values else None,
            "average_target": sum(target_values) / len(target_values) if target_values else None,
            "average_press_throughput": (
                sum(press_throughput_values) / len(press_throughput_values)
                if press_throughput_values else None
            ),
            "average_production_revenue": (
                sum(production_revenue_values) / len(production_revenue_values)
                if production_revenue_values else None
            ),
            "recook_lf": sum(item.get("recook_lf") or 0 for item in rows),
            "press_lf": sum(item.get("press_throughput") or 0 for item in rows),
            "production_revenue": sum(item.get("production_revenue_today") or 0 for item in rows),
        },
    }
