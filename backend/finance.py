from datetime import datetime
import os
import time

from filemaker import fetch_all_layout_records, get_field_value


AGED_DEBT_LAYOUT_DEFAULT = "ai_Aged Debt Summary"
_AGED_DEBT_CACHE = {"expires_at": 0, "result": None}


def number(value):
    if value is None or isinstance(value, bool):
        return None
    text = str(value).strip().replace(",", "").replace("$", "")
    if not text or text in {"-", "—", "?"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def get_aged_debt_layout():
    return os.getenv("FILEMAKER_AGED_DEBT_LAYOUT", AGED_DEBT_LAYOUT_DEFAULT).strip()


def get_finance_cache_seconds():
    try:
        return max(0, int(os.getenv("FILEMAKER_FINANCE_CACHE_SECONDS", "300")))
    except ValueError:
        return 300


def normalize_aged_debt_record(record):
    row = record.get("fieldData", {})
    value = lambda field: get_field_value(row, field)
    return {
        "record_id": str(record.get("recordId") or ""),
        "primary_key": str(value("PrimaryKey") or "").strip(),
        "average_debtor_days": number(value("Avg Debtor Days")),
        "current": number(value("Current Total")),
        "zero_thirty": number(value("Zero Thirty")),
        "thirtyone_sixty": number(value("Thirtyone Sixty")),
        "sixtyone_ninety": number(value("Sixtyone Ninty")),
        "ninety_plus": number(value("Ninty Plus")),
        "total_outstanding": number(value("Total Outstanding")),
        "last_refreshed": str(value("Last Refreshed") or "").strip(),
        "creation_timestamp": str(value("CreationTimestamp") or "").strip(),
        "modification_timestamp": str(value("ModificationTimestamp") or "").strip(),
    }


def record_rank(item):
    text = item.get("last_refreshed") or item.get("modification_timestamp") or item.get("creation_timestamp") or ""
    for pattern in ("%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return (datetime.strptime(text, pattern).timestamp(), int(item.get("record_id") or 0))
        except (TypeError, ValueError):
            continue
    try:
        return (0, int(item.get("record_id") or 0))
    except ValueError:
        return (0, 0)


def fetch_aged_debt_summary(force_refresh=False):
    now = time.time()
    if not force_refresh and _AGED_DEBT_CACHE["result"] is not None and _AGED_DEBT_CACHE["expires_at"] > now:
        return _AGED_DEBT_CACHE["result"]

    source = fetch_all_layout_records(get_aged_debt_layout(), batch_size=100)
    rows = [normalize_aged_debt_record(record) for record in source.get("records", [])]
    if rows:
        latest = max(rows, key=record_rank)
        result = {"status": "ok", "source": "filemaker", "layout": get_aged_debt_layout(), **latest}
    else:
        result = {
            "status": "no_record" if source.get("status") == "fetch_failed" else source.get("status", "error"),
            "source": "filemaker",
            "layout": get_aged_debt_layout(),
            "warning": "The aged-debt API layout is visible, but it does not currently return a stored record.",
        }

    cache_seconds = get_finance_cache_seconds()
    if cache_seconds:
        _AGED_DEBT_CACHE["result"] = result
        _AGED_DEBT_CACHE["expires_at"] = now + cache_seconds
    return result
