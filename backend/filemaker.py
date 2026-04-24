import json
import os
from pathlib import Path
from urllib.parse import quote

import requests
from dateutil import parser
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

load_dotenv(dotenv_path=Path(__file__).with_name(".env"))


def get_filemaker_config():
    return {
        "url": os.getenv("FILEMAKER_URL", "").rstrip("/"),
        "database": os.getenv("FILEMAKER_DATABASE"),
        "username": os.getenv("FILEMAKER_USERNAME"),
        "password": os.getenv("FILEMAKER_PASSWORD"),
        "verify_ssl": get_bool_env("FILEMAKER_VERIFY_SSL", default=True),
        "orders_layout": os.getenv("FILEMAKER_ORDERS_LAYOUT"),
        "customer_field": os.getenv("FILEMAKER_CUSTOMER_FIELD", "customer"),
        "order_date_field": os.getenv("FILEMAKER_ORDER_DATE_FIELD", "order_date"),
        "amount_field": os.getenv("FILEMAKER_AMOUNT_FIELD", "amount"),
        "date_order": os.getenv("FILEMAKER_DATE_ORDER", "mdy").strip().lower(),
        "last_activity_content_field": os.getenv(
            "FILEMAKER_LAST_ACTIVITY_CONTENT_FIELD", ""
        ),
        "extra_fields": get_extra_fields(),
    }


def get_bool_env(name, default=True):
    value = os.getenv(name)

    if value is None:
        return default

    return value.strip().lower() in ["1", "true", "yes", "on"]


def get_extra_fields():
    raw_fields = os.getenv("FILEMAKER_EXTRA_FIELDS", "")
    fields = [
        field.strip()
        for field in raw_fields.split(",")
        if field.strip()
    ]
    activity_content_field = os.getenv("FILEMAKER_LAST_ACTIVITY_CONTENT_FIELD", "")

    if activity_content_field and activity_content_field not in fields:
        fields.append(activity_content_field)

    return fields


def has_filemaker_config():
    config = get_filemaker_config()
    return all([
        config["url"],
        config["database"],
        config["username"],
        config["password"],
    ])


def get_database_path(config):
    database = quote(config["database"], safe="")
    return f"{config['url']}/fmi/data/vLatest/databases/{database}"


def get_session_token():
    config = get_filemaker_config()

    if not has_filemaker_config():
        print("Missing FileMaker environment variables")
        return None

    url = f"{get_database_path(config)}/sessions"

    response = requests.post(
        url,
        auth=HTTPBasicAuth(config["username"], config["password"]),
        headers={"Content-Type": "application/json"},
        timeout=15,
        verify=config["verify_ssl"]
    )

    if response.status_code != 200:
        print(f"FileMaker login failed with status {response.status_code}")
        return None

    token = response.json()["response"]["token"]
    return token


def close_session(token):
    if not token:
        return

    config = get_filemaker_config()
    url = f"{get_database_path(config)}/sessions/{token}"

    try:
        requests.delete(
            url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=15,
            verify=config["verify_ssl"]
        )
    except requests.RequestException as error:
        print(f"FileMaker logout error: {error.__class__.__name__}")


def check_filemaker_connection():
    if not has_filemaker_config():
        return {
            "configured": False,
            "connected": False,
            "status": "missing_config"
        }

    token = None

    try:
        token = get_session_token()
    except requests.RequestException as error:
        print(f"FileMaker connection error: {error.__class__.__name__}")
        return {
            "configured": True,
            "connected": False,
            "status": get_request_error_status(error)
        }
    except (KeyError, ValueError):
        print("FileMaker login response was not in the expected format")
        return {
            "configured": True,
            "connected": False,
            "status": "invalid_response"
        }
    finally:
        close_session(token)

    return {
        "configured": True,
        "connected": bool(token),
        "status": "connected" if token else "login_failed"
    }


def fetch_layout_records(layout, limit=100, offset=1, sort_fields=None):
    if not layout:
        return {
            "connected": False,
            "status": "missing_layout",
            "records": []
        }

    token = None

    try:
        config = get_filemaker_config()
        token = get_session_token()

        if not token:
            return {
                "connected": False,
                "status": "login_failed",
                "records": []
            }

        layout_name = quote(layout, safe="")
        url = f"{get_database_path(config)}/layouts/{layout_name}/records"
        params = {"_limit": limit, "_offset": offset}

        if sort_fields:
            params["_sort"] = json.dumps(sort_fields)

        response = requests.get(
            url,
            headers={"Authorization": f"Bearer {token}"},
            params=params,
            timeout=30,
            verify=config["verify_ssl"]
        )

        if response.status_code != 200:
            print(
                "FileMaker records fetch failed "
                f"with status {response.status_code}"
            )
            return {
                "connected": True,
                "status": "fetch_failed",
                "records": []
            }

        data = response.json()
        records = data.get("response", {}).get("data", [])

        return {
            "connected": True,
            "status": "ok",
            "records": records
        }
    except requests.RequestException as error:
        print(f"FileMaker records connection error: {error.__class__.__name__}")
        return {
            "connected": False,
            "status": get_request_error_status(error),
            "records": []
        }
    except ValueError:
        print("FileMaker records response was not valid JSON")
        return {
            "connected": True,
            "status": "invalid_response",
            "records": []
        }
    finally:
        close_session(token)


def map_filemaker_record_to_order(record):
    config = get_filemaker_config()
    field_data = record.get("fieldData", {})
    extra = {
        field_name: get_field_value(field_data, field_name)
        for field_name in config["extra_fields"]
    }

    return {
        "filemaker_record_id": record.get("recordId"),
        "customer": get_field_value(field_data, config["customer_field"]),
        "order_date": normalize_filemaker_date(
            get_field_value(field_data, config["order_date_field"])
        ),
        "amount": get_field_value(field_data, config["amount_field"]),
        "extra": extra,
    }


def get_field_value(field_data, field_name):
    if field_name in field_data:
        return field_data.get(field_name)

    normalized_name = field_name.lower()
    bare_field_name = field_name.split("::")[-1].lower()
    field_suffix = f"::{bare_field_name}"

    for key, value in field_data.items():
        if key.lower() == normalized_name:
            return value

    for key, value in field_data.items():
        if key.lower() == bare_field_name:
            return value

    for key, value in field_data.items():
        if key.lower().endswith(field_suffix):
            return value

    return None


def normalize_filemaker_date(value):
    if not value:
        return None

    config = get_filemaker_config()
    dayfirst = config["date_order"] == "dmy"

    try:
        parsed_date = parser.parse(str(value), dayfirst=dayfirst)
    except (TypeError, ValueError):
        return value

    return parsed_date.strftime("%Y-%m-%d")


def get_request_error_status(error):
    if isinstance(error, requests.exceptions.SSLError):
        return "ssl_error"

    if isinstance(error, requests.exceptions.Timeout):
        return "timeout"

    if isinstance(error, requests.exceptions.ConnectionError):
        return "connection_error"

    return "request_error"


def fetch_order_records(limit=100, offset=1):
    config = get_filemaker_config()
    result = fetch_layout_records(
        config["orders_layout"],
        limit=limit,
        offset=offset
    )

    if result["status"] != "ok":
        return {
            "connected": result["connected"],
            "status": result["status"],
            "orders": []
        }

    orders = [
        map_filemaker_record_to_order(record)
        for record in result["records"]
    ]

    return {
        "connected": True,
        "status": "ok",
        "orders": orders
    }


if __name__ == "__main__":
    token = get_session_token()
    print("Connected:", bool(token))
