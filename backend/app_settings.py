from datetime import date, datetime, timedelta
import json
import os
from pathlib import Path
import re


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_SETTINGS_PATH = BASE_DIR / "data" / "app_settings.json"
DEFAULT_DAILY_INVOICE_TARGET = 8000.0
DEFAULT_PRODUCTION_ISSUES_SEND_TIME = "18:30"
PRODUCTION_BREAK_CONTEXT_LINE = "Around 1 hour of unbooked paid time per operator per day is normal because paid breaks are not deducted. Allow up to and including 1.5 hours; only report an unbooked-time anomaly when the daily gap is more than 1.5 hours."
DEFAULT_PRODUCTION_ISSUES_CONTEXT = """NuMat repairs rubber-backed carpeted mats; it is not a new-product manufacturing operation.
Orders are commonly shared by several operators and may run across FF1, FF2 and FF3. This is normal and must not be raised as an issue by itself.
Work can move between presses. A single press being below its own recent average is not automatically a concern when total press output is normal.
Booked time exceeding clocked time should always be checked because it should not occur.
Around 1 hour of unbooked paid time per operator per day is normal because paid breaks are not deducted. Allow up to and including 1.5 hours; only report an unbooked-time anomaly when the daily gap is more than 1.5 hours.
Re-cook activity is a quality measure and is separate from new press throughput and backlog.
Backlog contains new customer orders awaiting their first repair only. The preferred backlog is approximately two weeks.
Housekeeping, Engineering, Re-Runs, Extruder, Bag Building and Press Room Help are productive work and must not be treated as lost time.
Lois's management time is expected and should not be treated as an operator productivity failure.
State only what the supplied figures support. Present possible causes as questions to check, never as established facts."""


def get_settings_path():
    configured = os.getenv("APP_SETTINGS_PATH", "").strip()
    return Path(configured).expanduser() if configured else DEFAULT_SETTINGS_PATH


def load_app_settings():
    try:
        payload = json.loads(get_settings_path().read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def save_app_settings(payload):
    path = get_settings_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def get_daily_invoice_target():
    value = load_app_settings().get("daily_invoice_target", DEFAULT_DAILY_INVOICE_TARGET)
    try:
        value = float(value)
    except (TypeError, ValueError):
        value = DEFAULT_DAILY_INVOICE_TARGET
    return value if value >= 0 else DEFAULT_DAILY_INVOICE_TARGET


def set_daily_invoice_target(value):
    try:
        target = float(str(value or "").strip().replace(",", "").replace("$", ""))
    except ValueError as error:
        raise ValueError("Enter a valid daily invoiced-sales target.") from error
    if target < 0:
        raise ValueError("The daily invoiced-sales target cannot be negative.")
    settings = load_app_settings()
    settings["daily_invoice_target"] = target
    save_app_settings(settings)
    return target


def normalize_email_recipients(value):
    values = value if isinstance(value, (list, tuple, set)) else re.split(r"[,;\n]+", str(value or ""))
    recipients = []
    seen = set()
    for item in values:
        email = str(item or "").strip().lower()
        if not email:
            continue
        if not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", email):
            raise ValueError(f"Enter a valid email address: {email}")
        if email not in seen:
            recipients.append(email)
            seen.add(email)
    return recipients


def get_production_daily_issues_settings():
    settings = load_app_settings().get("production_daily_issues", {})
    if not isinstance(settings, dict):
        settings = {}
    try:
        recipients = normalize_email_recipients(settings.get("recipients", []))
    except ValueError:
        recipients = []
    send_time = str(settings.get("send_time") or DEFAULT_PRODUCTION_ISSUES_SEND_TIME).strip()
    if not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", send_time):
        send_time = DEFAULT_PRODUCTION_ISSUES_SEND_TIME
    context = str(settings.get("context") or DEFAULT_PRODUCTION_ISSUES_CONTEXT).strip()
    if PRODUCTION_BREAK_CONTEXT_LINE not in context:
        context = f"{context}\n{PRODUCTION_BREAK_CONTEXT_LINE}".strip()
    return {
        "enabled": bool(settings.get("enabled", False)),
        "recipients": recipients,
        "send_time": send_time,
        "timezone": "America/Chicago",
        "context": context,
    }


def set_production_daily_issues_settings(enabled=False, recipients=None, send_time="", context=""):
    recipient_list = normalize_email_recipients(recipients)
    normalized_time = str(send_time or "").strip()
    if not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", normalized_time):
        raise ValueError("Enter the nightly send time in 24-hour HH:MM format.")
    if enabled and not recipient_list:
        raise ValueError("Add at least one recipient before enabling the nightly email.")
    normalized_context = str(context or DEFAULT_PRODUCTION_ISSUES_CONTEXT).strip()
    if PRODUCTION_BREAK_CONTEXT_LINE not in normalized_context:
        normalized_context = f"{normalized_context}\n{PRODUCTION_BREAK_CONTEXT_LINE}".strip()
    payload = load_app_settings()
    payload["production_daily_issues"] = {
        "enabled": bool(enabled),
        "recipients": recipient_list,
        "send_time": normalized_time,
        "context": normalized_context,
    }
    save_app_settings(payload)
    return get_production_daily_issues_settings()


def parse_date(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.strptime(str(value), "%Y-%m-%d").date()


def elapsed_monday_thursday_workdays(through_date):
    through_date = parse_date(through_date)
    current = through_date.replace(day=1)
    count = 0
    while current <= through_date:
        if current.weekday() in {0, 1, 2, 3}:
            count += 1
        current += timedelta(days=1)
    return count


def calculate_elapsed_invoice_target(through_date, daily_target=None):
    daily_target = get_daily_invoice_target() if daily_target is None else float(daily_target)
    return elapsed_monday_thursday_workdays(through_date) * daily_target
