import json
import os
import time
from pathlib import Path
from threading import Lock
from urllib.parse import urlencode

import requests
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).with_name(".env"))

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_M365_TOKENS_PATH = BASE_DIR / "data" / "m365_tokens.json"
M365_GRAPH_ROOT = "https://graph.microsoft.com/v1.0"
_M365_TOKENS_LOCK = Lock()


def get_bool_env(name, default=False):
    value = os.getenv(name)

    if value is None:
        return default

    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def get_m365_config():
    return {
        "enabled": get_bool_env("M365_GRAPH_ENABLED", default=False),
        "tenant_id": os.getenv("M365_TENANT_ID", "").strip(),
        "client_id": os.getenv("M365_CLIENT_ID", "").strip(),
        "client_secret": os.getenv("M365_CLIENT_SECRET", "").strip(),
        "redirect_uri": os.getenv("M365_REDIRECT_URI", "").strip(),
        "scopes": os.getenv(
            "M365_GRAPH_SCOPES",
            "offline_access,openid,profile,email,Mail.Send,User.Read",
        ).strip(),
        "logging_enabled": get_bool_env("ENABLE_M365_LOGS", default=False)
        or get_bool_env("ENABLE_PERF_LOGS", default=False),
    }


def has_m365_config():
    config = get_m365_config()
    return bool(
        config["enabled"]
        and config["tenant_id"]
        and config["client_id"]
        and config["client_secret"]
        and config["redirect_uri"]
    )


def should_log_m365():
    return bool(get_m365_config().get("logging_enabled"))


def log_m365_event(event, **details):
    if not should_log_m365():
        return

    detail_text = " ".join(
        f"{key}={str(value).replace(chr(10), ' ')}"
        for key, value in details.items()
        if str(value).strip()
    )
    if detail_text:
        print(f"M365 {event} {detail_text}")
    else:
        print(f"M365 {event}")


def get_m365_tokens_path():
    configured = os.getenv("M365_TOKENS_PATH", "").strip()
    return Path(configured).expanduser() if configured else DEFAULT_M365_TOKENS_PATH


def ensure_m365_tokens_file():
    path = get_m365_tokens_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text("{}\n", encoding="utf-8")
    return path


def load_m365_tokens():
    path = ensure_m365_tokens_file()
    try:
        with _M365_TOKENS_LOCK:
            payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            return payload
    except Exception:
        pass
    return {}


def save_m365_tokens(tokens):
    path = ensure_m365_tokens_file()
    with _M365_TOKENS_LOCK:
        path.write_text(json.dumps(tokens, indent=2), encoding="utf-8")


def get_m365_token_record(username):
    records = load_m365_tokens()
    record = records.get(str(username or "").strip()) or {}
    return record if isinstance(record, dict) else {}


def set_m365_token_record(username, record):
    records = load_m365_tokens()
    records[str(username or "").strip()] = dict(record or {})
    save_m365_tokens(records)


def delete_m365_token_record(username):
    records = load_m365_tokens()
    normalized = str(username or "").strip()
    if normalized in records:
        records.pop(normalized, None)
        save_m365_tokens(records)


def get_scope_list():
    config = get_m365_config()
    return [scope.strip() for scope in str(config["scopes"]).split(",") if scope.strip()]


def get_oauth_base_url():
    tenant_id = get_m365_config()["tenant_id"]
    return f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0"


def build_m365_authorize_url(state):
    config = get_m365_config()
    params = {
        "client_id": config["client_id"],
        "response_type": "code",
        "redirect_uri": config["redirect_uri"],
        "response_mode": "query",
        "scope": " ".join(get_scope_list()),
        "state": state,
        "prompt": "select_account",
    }
    return f"{get_oauth_base_url()}/authorize?{urlencode(params)}"


def exchange_m365_code_for_tokens(code):
    config = get_m365_config()

    if not has_m365_config():
        return {"status": "m365_disabled"}

    try:
        response = requests.post(
            f"{get_oauth_base_url()}/token",
            data={
                "client_id": config["client_id"],
                "client_secret": config["client_secret"],
                "grant_type": "authorization_code",
                "code": str(code or "").strip(),
                "redirect_uri": config["redirect_uri"],
                "scope": " ".join(get_scope_list()),
            },
            timeout=30,
        )
    except requests.RequestException as error:
        return {"status": get_request_error_status(error)}

    return normalize_token_response(response)


def refresh_m365_tokens(refresh_token):
    config = get_m365_config()

    if not has_m365_config():
        return {"status": "m365_disabled"}

    try:
        response = requests.post(
            f"{get_oauth_base_url()}/token",
            data={
                "client_id": config["client_id"],
                "client_secret": config["client_secret"],
                "grant_type": "refresh_token",
                "refresh_token": str(refresh_token or "").strip(),
                "redirect_uri": config["redirect_uri"],
                "scope": " ".join(get_scope_list()),
            },
            timeout=30,
        )
    except requests.RequestException as error:
        return {"status": get_request_error_status(error)}

    return normalize_token_response(response)


def normalize_token_response(response):
    if response.status_code != 200:
        error_message = ""
        try:
            payload = response.json()
            error_message = str(
                payload.get("error_description")
                or payload.get("error")
                or payload.get("message")
                or ""
            ).strip()
        except ValueError:
            error_message = str(response.text or "").strip()
        return {
            "status": f"http_{response.status_code}",
            "error_message": error_message,
        }

    try:
        payload = response.json()
    except ValueError:
        return {"status": "invalid_response"}

    expires_in = int(payload.get("expires_in") or 3600)
    return {
        "status": "ok",
        "token_data": {
            "access_token": str(payload.get("access_token") or "").strip(),
            "refresh_token": str(payload.get("refresh_token") or "").strip(),
            "scope": str(payload.get("scope") or "").strip(),
            "token_type": str(payload.get("token_type") or "").strip(),
            "expires_at": int(time.time()) + max(60, expires_in - 60),
            "connected_at": int(time.time()),
        },
    }


def fetch_m365_profile(access_token):
    headers = {
        "Authorization": f"Bearer {str(access_token or '').strip()}",
        "Accept": "application/json",
    }
    try:
        response = requests.get(
            f"{M365_GRAPH_ROOT}/me",
            headers=headers,
            timeout=30,
        )
    except requests.RequestException as error:
        return {"status": get_request_error_status(error)}

    if response.status_code != 200:
        return {
            "status": f"http_{response.status_code}",
            "error_message": str(response.text or "").strip(),
        }

    try:
        payload = response.json()
    except ValueError:
        return {"status": "invalid_response"}

    mailbox = str(
        payload.get("mail")
        or payload.get("userPrincipalName")
        or ""
    ).strip().lower()

    return {
        "status": "ok",
        "profile": payload,
        "mailbox": mailbox,
    }


def ensure_valid_access_token(username):
    record = get_m365_token_record(username)
    access_token = str(record.get("access_token") or "").strip()
    refresh_token = str(record.get("refresh_token") or "").strip()
    expires_at = int(record.get("expires_at") or 0)
    now = int(time.time())

    if access_token and expires_at > (now + 120):
        return {
            "status": "ok",
            "access_token": access_token,
            "record": record,
            "refreshed": False,
        }

    if not refresh_token:
        return {"status": "missing_refresh_token"}

    refresh_result = refresh_m365_tokens(refresh_token)
    if refresh_result.get("status") != "ok":
        return refresh_result

    new_record = dict(record)
    new_record.update(refresh_result["token_data"])
    if not new_record.get("refresh_token"):
        new_record["refresh_token"] = refresh_token
    set_m365_token_record(username, new_record)
    log_m365_event("token_refreshed", username=username)
    return {
        "status": "ok",
        "access_token": str(new_record.get("access_token") or "").strip(),
        "record": new_record,
        "refreshed": True,
    }


def send_m365_mail(access_token, recipient, subject, body):
    headers = {
        "Authorization": f"Bearer {str(access_token or '').strip()}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    payload = {
        "message": {
            "subject": str(subject or "").strip(),
            "body": {
                "contentType": "Text",
                "content": str(body or ""),
            },
            "toRecipients": [
                {"emailAddress": {"address": str(recipient or "").strip()}}
            ],
        },
        "saveToSentItems": True,
    }
    try:
        response = requests.post(
            f"{M365_GRAPH_ROOT}/me/sendMail",
            headers=headers,
            json=payload,
            timeout=30,
        )
    except requests.RequestException as error:
        return {"status": get_request_error_status(error)}

    if response.status_code not in {200, 202}:
        return {
            "status": f"http_{response.status_code}",
            "error_message": str(response.text or "").strip(),
        }

    return {"status": "ok"}


def get_request_error_status(error):
    if isinstance(error, requests.Timeout):
        return "timeout"
    if isinstance(error, requests.ConnectionError):
        return "connection_error"
    return "request_error"
