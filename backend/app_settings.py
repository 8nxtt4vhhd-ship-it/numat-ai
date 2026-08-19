from datetime import date, datetime, timedelta
import json
import os
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_SETTINGS_PATH = BASE_DIR / "data" / "app_settings.json"
DEFAULT_DAILY_INVOICE_TARGET = 8000.0


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
