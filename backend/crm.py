import csv
import io
import json
import os
import re
import time
from pathlib import Path

from dateutil import parser
from dotenv import load_dotenv
from filemaker import fetch_layout_records, get_filemaker_config

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_CRM_SAMPLE_CSV_PATH = Path(
    "/Users/kellybainbridge/Documents/sample data crm.csv"
)
DEFAULT_UPLOADED_CRM_CSV_PATH = BASE_DIR / "data" / "sample_crm.csv"
DEFAULT_FILEMAKER_CRM_CACHE_PATH = BASE_DIR / "data" / "filemaker_crm_cache.json"
MAX_CRM_SAMPLE_ROWS = 50000
_CRM_CACHE = {
    "key": None,
    "expires_at": 0,
    "result": None,
}

load_dotenv(dotenv_path=BASE_DIR / ".env")


def get_crm_sample_csv_path():
    uploaded_path = get_uploaded_crm_csv_path()

    if uploaded_path.exists():
        return uploaded_path

    raw_path = os.getenv("CRM_SAMPLE_CSV_PATH", "").strip()

    if not raw_path:
        return DEFAULT_CRM_SAMPLE_CSV_PATH

    path = Path(raw_path).expanduser()

    if path.is_absolute():
        return path

    return BASE_DIR / path


def get_uploaded_crm_csv_path():
    return DEFAULT_UPLOADED_CRM_CSV_PATH


def get_filemaker_crm_cache_path():
    raw_path = os.getenv("FILEMAKER_CRM_CACHE_PATH", "").strip()

    if not raw_path:
        return DEFAULT_FILEMAKER_CRM_CACHE_PATH

    path = Path(raw_path).expanduser()

    if path.is_absolute():
        return path

    return BASE_DIR / path


def get_internal_domains():
    raw_domains = os.getenv(
        "CRM_INTERNAL_DOMAINS",
        "numatsystems.com,nufox.com",
    )
    return {
        domain.strip().lower()
        for domain in raw_domains.split(",")
        if domain.strip()
    }


def get_crm_data_source():
    return os.getenv("CRM_DATA_SOURCE", "sample_csv").strip().lower()


def get_filemaker_emails_layout():
    return os.getenv("FILEMAKER_EMAILS_LAYOUT", "").strip()


def get_filemaker_crm_limit():
    raw_limit = os.getenv("FILEMAKER_CRM_LIMIT", "5000")

    try:
        return max(1, int(raw_limit))
    except ValueError:
        return 5000


def get_filemaker_crm_fetch_all():
    return os.getenv("FILEMAKER_CRM_FETCH_ALL", "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def get_filemaker_crm_batch_size():
    raw_limit = os.getenv("FILEMAKER_CRM_BATCH_SIZE", "5000")

    try:
        return max(1, int(raw_limit))
    except ValueError:
        return 5000


def get_filemaker_crm_use_sync_cache():
    return os.getenv("FILEMAKER_CRM_USE_SYNC_CACHE", "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def get_filemaker_crm_sort_field():
    return os.getenv("FILEMAKER_CRM_SORT_FIELD", "Date Created").strip()


def get_row_value(row, *field_names):
    for field_name in field_names:
        if not field_name:
            continue

        value = row.get(field_name)

        if value is not None and str(value).strip():
            return value

    return ""


def get_crm_cache_seconds():
    raw_seconds = os.getenv("CRM_CACHE_SECONDS", "120")

    try:
        return max(0, int(raw_seconds))
    except ValueError:
        return 120


def fetch_crm_activities():
    source = get_crm_data_source()
    cache_key = get_crm_cache_key(source)
    cached_result = get_cached_crm_result(cache_key)

    if cached_result is not None:
        return cached_result

    if source == "filemaker":
        if get_filemaker_crm_use_sync_cache():
            cached_sync_result = read_filemaker_crm_sync_cache()

            if cached_sync_result is not None:
                cache_crm_result(cache_key, cached_sync_result)
                return cached_sync_result

        result = build_filemaker_crm_result()
    else:
        path = get_crm_sample_csv_path()
        result = build_csv_crm_result(path)

    cache_crm_result(cache_key, result)
    return result


def get_cached_crm_result(cache_key):
    if cache_key is None:
        return None

    if _CRM_CACHE["key"] != cache_key:
        return None

    if _CRM_CACHE["expires_at"] <= time.time():
        return None

    return _CRM_CACHE["result"]


def cache_crm_result(cache_key, result):
    cache_seconds = get_crm_cache_seconds()

    if cache_key is None or not cache_seconds:
        return

    _CRM_CACHE["key"] = cache_key
    _CRM_CACHE["expires_at"] = time.time() + cache_seconds
    _CRM_CACHE["result"] = result


def clear_crm_cache():
    _CRM_CACHE["key"] = None
    _CRM_CACHE["expires_at"] = 0
    _CRM_CACHE["result"] = None


def get_crm_cache_key(source):
    if source == "filemaker":
        cache_path = get_filemaker_crm_cache_path()
        sync_signature = None

        if get_filemaker_crm_use_sync_cache() and cache_path.exists():
            stat = cache_path.stat()
            sync_signature = (
                str(cache_path),
                stat.st_mtime_ns,
                stat.st_size,
            )

        config = get_filemaker_config()
        return (
            "filemaker",
            config["url"],
            config["database"],
            get_filemaker_emails_layout(),
            get_filemaker_crm_limit(),
            get_filemaker_crm_fetch_all(),
            get_filemaker_crm_batch_size(),
            get_filemaker_crm_use_sync_cache(),
            sync_signature,
            get_filemaker_crm_sort_field(),
            tuple(sorted(get_internal_domains())),
        )

    path = get_crm_sample_csv_path()

    try:
        stat = path.stat()
    except FileNotFoundError:
        return None

    return (
        "sample_csv",
        str(path),
        stat.st_mtime_ns,
        stat.st_size,
        tuple(sorted(get_internal_domains())),
    )


def build_csv_crm_result(path):

    if not path.exists():
        return {
            "source": "crm_sample_csv",
            "status": "csv_not_found",
            "path": str(path),
            "activities": [],
            "activity_map": {},
            "counts": {
                "total_rows": 0,
                "kept_rows": 0,
                "excluded_internal_only": 0,
                "customer_count": 0,
                "unknown_direction_count": 0,
            },
        }

    with path.open(newline="", encoding="utf-8-sig") as csv_file:
        reader = csv.DictReader(csv_file)
        rows = list(reader)

    return build_crm_result_from_rows(
        rows,
        source="crm_sample_csv",
        path=str(path),
    )


def build_filemaker_crm_result():
    layout = get_filemaker_emails_layout()
    limit = get_filemaker_crm_limit()
    fetch_all = get_filemaker_crm_fetch_all()
    batch_size = get_filemaker_crm_batch_size()
    rows = []

    if fetch_all:
        offset = 1

        while True:
            result = fetch_layout_records(
                layout,
                limit=batch_size,
                offset=offset,
            )

            if result["status"] != "ok":
                return empty_crm_result("filemaker", layout, result["status"])

            batch_rows = [
                record.get("fieldData", {})
                for record in result.get("records", [])
            ]
            rows.extend(batch_rows)

            if len(batch_rows) < batch_size:
                break

            offset += batch_size
    else:
        sort_field = get_filemaker_crm_sort_field()
        sort_fields = []

        if sort_field:
            sort_fields.append({
                "fieldName": sort_field,
                "sortOrder": "descend",
            })

        result = fetch_layout_records(
            layout,
            limit=limit,
            offset=1,
            sort_fields=sort_fields,
        )

        if result["status"] != "ok":
            return empty_crm_result("filemaker", layout, result["status"])

        rows = [
            record.get("fieldData", {})
            for record in result.get("records", [])
        ]

    return build_crm_result_from_rows(
        rows,
        source="filemaker",
        path=layout,
    )


def sync_filemaker_crm_cache():
    layout = get_filemaker_emails_layout()
    batch_size = get_filemaker_crm_batch_size()
    offset = 1
    rows = []

    while True:
        result = fetch_layout_records(
            layout,
            limit=batch_size,
            offset=offset,
        )

        if result["status"] != "ok":
            return empty_crm_result("filemaker_sync_cache", str(get_filemaker_crm_cache_path()), result["status"])

        batch_rows = [
            record.get("fieldData", {})
            for record in result.get("records", [])
        ]
        rows.extend(batch_rows)

        if len(batch_rows) < batch_size:
            break

        offset += batch_size

    synced_at = time.strftime("%Y-%m-%d %H:%M:%S")
    result = build_crm_result_from_rows(
        rows,
        source="filemaker_sync_cache",
        path=str(get_filemaker_crm_cache_path()),
        synced_at=synced_at,
    )

    if result["status"] == "ok":
        cache_path = get_filemaker_crm_cache_path()
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "synced_at": synced_at,
            "source": result["source"],
            "status": result["status"],
            "path": result["path"],
            "counts": result["counts"],
            "activities": result["activities"],
        }
        cache_path.write_text(json.dumps(payload), encoding="utf-8")
        clear_crm_cache()

    return result


def read_filemaker_crm_sync_cache():
    cache_path = get_filemaker_crm_cache_path()

    if not cache_path.exists():
        return None

    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None

    activities = sort_crm_activities(payload.get("activities", []))
    activity_map = build_activity_map(activities)
    counts = payload.get("counts", {})

    return {
        "source": payload.get("source", "filemaker_sync_cache"),
        "status": payload.get("status", "ok"),
        "path": payload.get("path", str(cache_path)),
        "activities": activities,
        "activity_map": activity_map,
        "counts": {
            "total_rows": counts.get("total_rows", len(activities)),
            "kept_rows": counts.get("kept_rows", len(activities)),
            "excluded_internal_only": counts.get("excluded_internal_only", 0),
            "customer_count": counts.get("customer_count", len(activity_map)),
            "unknown_direction_count": counts.get("unknown_direction_count", 0),
        },
        "synced_at": payload.get("synced_at", ""),
    }


def build_crm_result_from_rows(rows, source, path, synced_at=""):
    activities = []
    excluded_internal_only = 0
    unknown_direction_count = 0

    for index, row in enumerate(rows, start=1):
        activity = normalize_crm_row(row, index=index)

        if activity["exclude"]:
            excluded_internal_only += 1
            continue

        if activity["direction"] == "unknown":
            unknown_direction_count += 1

        activities.append(activity)

    activities = sort_crm_activities(activities)
    activity_map = build_activity_map(activities)

    return {
        "source": source,
        "status": "ok",
        "path": path,
        "activities": activities,
        "activity_map": activity_map,
        "counts": {
            "total_rows": len(rows),
            "kept_rows": len(activities),
            "excluded_internal_only": excluded_internal_only,
            "customer_count": len(activity_map),
            "unknown_direction_count": unknown_direction_count,
        },
        "synced_at": synced_at,
    }


def empty_crm_result(source, path, status):
    return {
        "source": source,
        "status": status,
        "path": path,
        "activities": [],
        "activity_map": {},
        "counts": {
            "total_rows": 0,
            "kept_rows": 0,
            "excluded_internal_only": 0,
            "customer_count": 0,
            "unknown_direction_count": 0,
        },
        "synced_at": "",
    }


def sort_crm_activities(activities):
    return sorted(
        activities,
        key=lambda activity: (
            activity.get("date_created", ""),
            activity.get("row_number", 0),
        ),
        reverse=True,
    )


def validate_crm_csv_path(path=None):
    if get_crm_data_source() == "filemaker" and path is None:
        active_result = fetch_crm_activities()

        return {
            "valid": active_result.get("status") == "ok",
            "status": active_result.get("status", "unknown"),
            "path": get_filemaker_emails_layout(),
            "row_count": active_result.get("counts", {}).get("total_rows", 0),
            "customer_count": active_result.get("counts", {}).get("customer_count", 0),
            "usable_count": active_result.get("counts", {}).get("kept_rows", 0),
            "excluded_internal_only": active_result.get("counts", {}).get(
                "excluded_internal_only", 0
            ),
            "warnings": [],
            "errors": [] if active_result.get("status") == "ok" else [
                f"FileMaker CRM source returned status: {active_result.get('status', 'unknown')}"
            ],
        }

    csv_path = path or get_crm_sample_csv_path()

    if csv_path == get_crm_sample_csv_path():
        cached_result = fetch_crm_activities()

        if cached_result["status"] == "ok":
            return {
                "valid": True,
                "status": "ok",
                "path": str(csv_path),
                "row_count": cached_result["counts"]["total_rows"],
                "customer_count": cached_result["counts"]["customer_count"],
                "usable_count": cached_result["counts"]["kept_rows"],
                "excluded_internal_only": cached_result["counts"]["excluded_internal_only"],
                "warnings": (
                    [f"CSV has {cached_result['counts']['total_rows']} rows. Over {MAX_CRM_SAMPLE_ROWS} rows may feel slower in browser views."]
                    if cached_result["counts"]["total_rows"] > MAX_CRM_SAMPLE_ROWS else []
                ),
                "errors": [],
            }

    if not csv_path.exists():
        return {
            "valid": False,
            "status": "csv_not_found",
            "path": str(csv_path),
            "row_count": 0,
            "customer_count": 0,
            "usable_count": 0,
            "excluded_internal_only": 0,
            "warnings": [],
            "errors": ["CSV file was not found."],
        }

    with csv_path.open(newline="", encoding="utf-8-sig") as csv_file:
        content = csv_file.read().encode("utf-8")

    result = validate_crm_csv_content(content)
    result["path"] = str(csv_path)
    return result


def validate_crm_csv_content(content):
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError:
        return {
            "valid": False,
            "status": "invalid_encoding",
            "path": None,
            "row_count": 0,
            "customer_count": 0,
            "usable_count": 0,
            "excluded_internal_only": 0,
            "warnings": [],
            "errors": ["CSV must be UTF-8 encoded."],
        }

    reader = csv.DictReader(io.StringIO(text))
    fieldnames = reader.fieldnames or []
    rows = list(reader)
    required_fields = [
        "emails::Date Created",
        "emails::body",
        "emails::sender_email",
        "emails::To",
        "Companies 10::PrimaryKey",
        "Companies 10::Company",
        "Companies 8::PrimaryKey",
        "Companies 8::Company",
    ]
    missing_fields = [
        field for field in required_fields
        if not has_matching_field(fieldnames, field)
    ]
    warnings = []

    if len(rows) > MAX_CRM_SAMPLE_ROWS:
        warnings.append(
            f"CSV has {len(rows)} rows. Over {MAX_CRM_SAMPLE_ROWS} rows may feel slower in browser views."
        )

    if missing_fields:
        return {
            "valid": False,
            "status": "missing_required_fields",
            "path": None,
            "row_count": len(rows),
            "customer_count": 0,
            "usable_count": 0,
            "excluded_internal_only": 0,
            "warnings": warnings,
            "errors": [
                f"Missing required field: {field}"
                for field in missing_fields
            ],
        }

    activities = [
        normalize_crm_row(row, index=index)
        for index, row in enumerate(rows, start=1)
    ]
    usable_activities = [
        activity for activity in activities
        if not activity["exclude"]
    ]
    customer_keys = {
        activity["customer_primary_key"]
        for activity in usable_activities
        if activity["customer_primary_key"]
    }

    return {
        "valid": True,
        "status": "ok",
        "path": None,
        "row_count": len(rows),
        "customer_count": len(customer_keys),
        "usable_count": len(usable_activities),
        "excluded_internal_only": len(rows) - len(usable_activities),
        "warnings": warnings,
        "errors": [],
    }


def normalize_crm_row(row, index):
    sender_email = str(
        get_row_value(row, "emails::sender_email", "sender_email")
    ).strip()
    to_field = str(
        get_row_value(row, "emails::To", "To")
    ).strip()
    sender_company = str(
        get_row_value(
            row,
            "Companies 10::Company",
            "ai_SenderCompany",
        )
    ).strip()
    sender_primary_key = str(
        get_row_value(
            row,
            "Companies 10::PrimaryKey",
            "ai_SenderPK",
        )
    ).strip()
    recipient_company = str(
        get_row_value(
            row,
            "Companies 8::Company",
            "ai_ReceiverCompany",
        )
    ).strip()
    recipient_primary_key = str(
        get_row_value(
            row,
            "Companies 8::PrimaryKey",
            "ai_ReceiverPK",
        )
    ).strip()
    sender_domain = get_email_domain(sender_email)
    recipient_emails = extract_emails(to_field)
    recipient_domains = {
        get_email_domain(email)
        for email in recipient_emails
        if get_email_domain(email)
    }
    internal_domains = get_internal_domains()
    sender_is_internal = sender_domain in internal_domains
    recipient_has_internal = bool(recipient_domains & internal_domains)
    recipient_has_external = any(
        domain not in internal_domains
        for domain in recipient_domains
        if domain
    )
    sender_is_external = bool(sender_domain) and sender_domain not in internal_domains

    direction = "unknown"
    customer_primary_key = ""
    customer_company = ""

    if sender_is_internal and recipient_has_external:
        direction = "outbound"
        customer_primary_key = recipient_primary_key
        customer_company = recipient_company
    elif sender_is_external and recipient_has_internal:
        direction = "inbound"
        customer_primary_key = sender_primary_key
        customer_company = sender_company

    if not customer_primary_key:
        if sender_is_external:
            customer_primary_key = sender_primary_key
            customer_company = sender_company
        elif recipient_has_external:
            customer_primary_key = recipient_primary_key
            customer_company = recipient_company

    exclude = (
        (sender_is_internal or not sender_domain)
        and (recipient_has_internal or not recipient_has_external)
    )

    return {
        "row_number": index,
        "date_created": normalize_crm_date(
            get_row_value(row, "emails::Date Created", "Date Created")
        ),
        "subject": str(get_row_value(row, "emails::subject", "subject")).strip(),
        "body": str(get_row_value(row, "emails::body", "body")).strip(),
        "crm_category": str(
            get_row_value(row, "emails::CRM Category", "CRM Category")
        ).strip(),
        "crm_type": str(
            get_row_value(row, "emails::CRM Type", "CRM Type")
        ).strip(),
        "customer_label": str(
            get_row_value(
                row,
                "emails::Customer",
                recipient_company,
                sender_company,
            )
        ).strip(),
        "sender_email": sender_email,
        "to": to_field,
        "sender_company": sender_company,
        "sender_primary_key": sender_primary_key,
        "recipient_company": recipient_company,
        "recipient_primary_key": recipient_primary_key,
        "direction": direction,
        "customer_primary_key": customer_primary_key,
        "customer_company": customer_company,
        "sender_is_internal": sender_is_internal,
        "recipient_has_internal": recipient_has_internal,
        "exclude": exclude,
    }


def normalize_crm_date(value):
    if not value:
        return ""

    try:
        return parser.parse(str(value), dayfirst=False).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    except (TypeError, ValueError):
        return str(value)


def extract_emails(value):
    return re.findall(r"[\w.+-]+@[\w.-]+\.\w+", str(value or "").lower())


def get_email_domain(email):
    email = str(email or "").strip().lower()

    if "@" not in email:
        return ""

    return email.split("@")[-1]


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


def build_activity_map(activities):
    activity_map = {}

    for activity in activities:
        key = str(activity.get("customer_primary_key") or "").strip()

        if not key:
            continue

        activity_map.setdefault(key, []).append(activity)

    for key, items in activity_map.items():
        items.sort(
            key=lambda activity: activity.get("date_created", ""),
            reverse=True,
        )

    return activity_map
