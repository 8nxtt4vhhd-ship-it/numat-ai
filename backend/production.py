from collections import defaultdict
from datetime import datetime, timedelta
import os
import re
import time

from filemaker import fetch_all_layout_records, get_field_value, normalize_filemaker_date


_PRODUCTION_CACHE = {"expires_at": 0, "result": None}

PRODUCTION_LAYOUT_DEFAULT = "ai_Numat Production Data"
OPERATOR_LAYOUT_DEFAULT = "ai_Numat Production Data Op Data"
EXCLUDED_OPERATOR_KEYS = {
    "trudydunlap",
    "trudysdunlap",
    "kellybainbridge",
    "loishorace",
    "temp1",
    "temp2",
}


def get_production_layout():
    return os.getenv("FILEMAKER_PRODUCTION_LAYOUT", PRODUCTION_LAYOUT_DEFAULT).strip()


def get_operator_layout():
    return os.getenv("FILEMAKER_PRODUCTION_OPERATOR_LAYOUT", OPERATOR_LAYOUT_DEFAULT).strip()


def get_production_cache_seconds():
    try:
        return max(0, int(os.getenv("FILEMAKER_PRODUCTION_CACHE_SECONDS", "300")))
    except ValueError:
        return 300


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


def operator_key(value):
    return re.sub(r"[^a-z0-9]", "", str(value or "").casefold())


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
    if not include_today and not force_refresh and _PRODUCTION_CACHE["result"] is not None and _PRODUCTION_CACHE["expires_at"] > now:
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
    if cache_seconds and not include_today:
        _PRODUCTION_CACHE["result"] = result
        _PRODUCTION_CACHE["expires_at"] = now + cache_seconds
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


def build_production_kpi_payload(result=None, days=90):
    result = result or fetch_production_analysis_data()
    rows, operators = filter_production_period(result, days=days)
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
        if item.get("production_revenue_today") is not None
    ]
    return {
        "status": result.get("status", "error"),
        "warning": result.get("warning", ""),
        "production_status": result.get("production_status", ""),
        "operator_status": result.get("operator_status", ""),
        "synced_at": result.get("synced_at", ""),
        "days": days,
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
