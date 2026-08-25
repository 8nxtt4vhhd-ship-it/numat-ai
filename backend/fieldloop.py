from datetime import datetime, timezone
import os
import re
import secrets
from threading import Lock
import time


FIELDLOOP_VISIT_NOTE_PATH = "/api/fieldloop/visit-note"
EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
IDEMPOTENCY_KEY_PATTERN = re.compile(r"^[A-Za-z0-9._:-]{8,200}$")
RATE_LIMIT_LOCK = Lock()
RATE_LIMIT_REQUESTS = []


def get_fieldloop_config():
    return {
        "api_key": os.getenv("FIELDLOOP_API_KEY", "").strip(),
        "layout": os.getenv("FILEMAKER_FIELDLOOP_LAYOUT", "ai_fieldLoopCRM").strip(),
        "requests_per_minute": get_positive_int_env("FIELDLOOP_REQUESTS_PER_MINUTE", 30),
        "allowed_senders": {
            item.strip().lower()
            for item in os.getenv("FIELDLOOP_ALLOWED_SENDERS", "").split(",")
            if item.strip()
        },
    }


def get_positive_int_env(name, default):
    try:
        return max(1, int(os.getenv(name, str(default)).strip()))
    except ValueError:
        return default


def check_fieldloop_rate_limit(now=None):
    current = float(now if now is not None else time.time())
    cutoff = current - 60
    maximum = get_fieldloop_config()["requests_per_minute"]
    with RATE_LIMIT_LOCK:
        RATE_LIMIT_REQUESTS[:] = [timestamp for timestamp in RATE_LIMIT_REQUESTS if timestamp > cutoff]
        if len(RATE_LIMIT_REQUESTS) >= maximum:
            return False
        RATE_LIMIT_REQUESTS.append(current)
    return True


def authenticate_fieldloop_request(authorization):
    expected_key = get_fieldloop_config()["api_key"]
    if not expected_key:
        return "missing_config"

    supplied = str(authorization or "").strip()
    if not supplied.lower().startswith("bearer "):
        return "unauthorized"

    supplied_key = supplied.split(" ", 1)[1].strip()
    if not supplied_key or not secrets.compare_digest(supplied_key, expected_key):
        return "unauthorized"
    return "ok"


def is_fieldloop_sender_allowed(sender_email):
    allowed_senders = get_fieldloop_config()["allowed_senders"]
    return str(sender_email or "").strip().lower() in allowed_senders


def normalize_text(value, maximum):
    text = str(value or "").strip()
    if len(text) > maximum:
        return ""
    return text


def validate_visit_note_payload(payload):
    if not isinstance(payload, dict):
        return {"status": "invalid", "errors": ["The request body must be a JSON object."]}

    values = {
        "customer_id": normalize_text(payload.get("customer_id"), 200),
        "customer_name": normalize_text(payload.get("customer_name"), 250),
        "recipient_email": normalize_text(payload.get("recipient_email"), 320).lower(),
        "sender_email": normalize_text(payload.get("sender_email"), 320).lower(),
        "sender_name": normalize_text(payload.get("sender_name"), 250),
        "note": normalize_text(payload.get("note"), 20000),
        "idempotency_key": normalize_text(payload.get("idempotency_key"), 200),
    }

    errors = []
    for name in values:
        if not values[name]:
            errors.append(f"{name} is required and must be within the allowed length.")

    for name in ("recipient_email", "sender_email"):
        if values[name] and not EMAIL_PATTERN.fullmatch(values[name]):
            errors.append(f"{name} must be a valid email address.")

    if values["idempotency_key"] and not IDEMPOTENCY_KEY_PATTERN.fullmatch(values["idempotency_key"]):
        errors.append("idempotency_key may contain only letters, numbers, dots, underscores, colons and hyphens.")

    if errors:
        return {"status": "invalid", "errors": errors}
    return {"status": "ok", "values": values}


def build_filemaker_field_data(values, now=None):
    current = now or datetime.now(timezone.utc)
    return {
        "sentDateTime": current.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "ai_ReceiverCompany": values["customer_name"],
        "ai_ReceiverPK": values["customer_id"],
        "To": values["recipient_email"],
        "sender_email": values["sender_email"],
        "sender_name": values["sender_name"],
        "body": values["note"],
        "CRM Category": "Sales",
        "CRM Type": "Meeting",
        "idempotency_key": values["idempotency_key"],
    }
