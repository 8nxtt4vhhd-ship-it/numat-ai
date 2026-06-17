import json
import os
from pathlib import Path
import time
from urllib.parse import quote

import requests
from dateutil import parser
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

load_dotenv(dotenv_path=Path(__file__).with_name(".env"))

_FILEMAKER_MASTER_DATA_CACHE = {
    "key": None,
    "expires_at": 0,
    "result": None,
}


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
        "customers_layout": os.getenv("FILEMAKER_CUSTOMERS_LAYOUT", "").strip(),
        "customers_key_field": os.getenv("FILEMAKER_CUSTOMERS_KEY_FIELD", "PrimaryKey").strip(),
        "customers_name_field": os.getenv("FILEMAKER_CUSTOMERS_NAME_FIELD", "Company").strip(),
        "customers_city_field": os.getenv("FILEMAKER_CUSTOMERS_CITY_FIELD", "City").strip(),
        "customers_state_field": os.getenv("FILEMAKER_CUSTOMERS_STATE_FIELD", "State").strip(),
        "customers_country_field": os.getenv("FILEMAKER_CUSTOMERS_COUNTRY_FIELD", "Country").strip(),
        "customers_zip_field": os.getenv("FILEMAKER_CUSTOMERS_ZIP_FIELD", "ZIP Code").strip(),
        "customers_activity_status_field": os.getenv(
            "FILEMAKER_CUSTOMERS_ACTIVITY_STATUS_FIELD", "Activity Status"
        ).strip(),
        "customers_type_field": os.getenv("FILEMAKER_CUSTOMERS_TYPE_FIELD", "Type").strip(),
        "contacts_layout": os.getenv("FILEMAKER_CONTACTS_LAYOUT", "").strip(),
        "contacts_key_field": os.getenv("FILEMAKER_CONTACTS_KEY_FIELD", "PrimaryKey").strip(),
        "contacts_customer_ref_field": os.getenv(
            "FILEMAKER_CONTACTS_CUSTOMER_REF_FIELD", "Co Ref"
        ).strip(),
        "contacts_name_field": os.getenv("FILEMAKER_CONTACTS_NAME_FIELD", "Name").strip(),
        "contacts_email_field": os.getenv(
            "FILEMAKER_CONTACTS_EMAIL_FIELD", "Email Address"
        ).strip(),
        "contacts_position_field": os.getenv(
            "FILEMAKER_CONTACTS_POSITION_FIELD", "Postion"
        ).strip(),
        "contacts_phone_field": os.getenv("FILEMAKER_CONTACTS_PHONE_FIELD", "Phone").strip(),
        "contacts_cell_field": os.getenv("FILEMAKER_CONTACTS_CELL_FIELD", "Cell").strip(),
        "contacts_active_field": os.getenv("FILEMAKER_CONTACTS_ACTIVE_FIELD", "Active").strip(),
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


def get_int_env(name, default):
    value = os.getenv(name)

    if value is None:
        return default

    try:
        return int(str(value).strip())
    except ValueError:
        return default


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


def format_request_error(error):
    detail = str(error or "").strip()
    if detail:
        return f"{error.__class__.__name__}: {detail}"
    return error.__class__.__name__


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
        print(f"FileMaker logout error: {format_request_error(error)}")


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
        print(f"FileMaker connection error: {format_request_error(error)}")
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
        print(f"FileMaker records connection error: {format_request_error(error)}")
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


def fetch_all_layout_records(layout, batch_size=500, sort_fields=None, max_records=None):
    offset = 1
    records = []

    while True:
        current_limit = batch_size
        if max_records is not None:
            remaining = max_records - len(records)

            if remaining <= 0:
                break

            current_limit = min(current_limit, remaining)

        result = fetch_layout_records(
            layout,
            limit=current_limit,
            offset=offset,
            sort_fields=sort_fields,
        )

        if result["status"] != "ok":
            return {
                "connected": result.get("connected", False),
                "status": result["status"],
                "records": [],
            }

        batch = result.get("records", [])
        records.extend(batch)

        if len(batch) < current_limit:
            break

        offset += current_limit

    return {
        "connected": True,
        "status": "ok",
        "records": records,
    }


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


def normalize_text_value(value):
    return str(value or "").strip()


def normalize_email_value(value):
    return normalize_text_value(value).lower()


def get_filemaker_master_data_cache_seconds():
    return max(0, get_int_env("FILEMAKER_MASTER_DATA_CACHE_SECONDS", 300))


def clear_filemaker_master_data_cache():
    _FILEMAKER_MASTER_DATA_CACHE["key"] = None
    _FILEMAKER_MASTER_DATA_CACHE["expires_at"] = 0
    _FILEMAKER_MASTER_DATA_CACHE["result"] = None


def build_filemaker_master_data_cache_key():
    config = get_filemaker_config()
    return (
        config["url"],
        config["database"],
        config["customers_layout"],
        config["customers_key_field"],
        config["customers_name_field"],
        config["customers_city_field"],
        config["customers_state_field"],
        config["customers_country_field"],
        config["customers_zip_field"],
        config["customers_activity_status_field"],
        config["customers_type_field"],
        config["contacts_layout"],
        config["contacts_key_field"],
        config["contacts_customer_ref_field"],
        config["contacts_name_field"],
        config["contacts_email_field"],
        config["contacts_position_field"],
        config["contacts_phone_field"],
        config["contacts_cell_field"],
        config["contacts_active_field"],
    )


def build_empty_filemaker_master_data(status):
    return {
        "connected": status == "ok",
        "status": status,
        "customers_by_key": {},
        "all_contacts": [],
        "contacts_by_customer_key": {},
        "contacts_by_email": {},
    }


def map_filemaker_customer_master_record(record):
    config = get_filemaker_config()
    field_data = record.get("fieldData", {})
    return {
        "primary_key": normalize_text_value(
            get_field_value(field_data, config["customers_key_field"])
        ),
        "company": normalize_text_value(
            get_field_value(field_data, config["customers_name_field"])
        ),
        "city": normalize_text_value(
            get_field_value(field_data, config["customers_city_field"])
        ),
        "state": normalize_text_value(
            get_field_value(field_data, config["customers_state_field"])
        ),
        "country": normalize_text_value(
            get_field_value(field_data, config["customers_country_field"])
        ),
        "zip_code": normalize_text_value(
            get_field_value(field_data, config["customers_zip_field"])
        ),
        "activity_status": normalize_text_value(
            get_field_value(field_data, config["customers_activity_status_field"])
        ),
        "type": normalize_text_value(
            get_field_value(field_data, config["customers_type_field"])
        ),
    }


def map_filemaker_contact_master_record(record):
    config = get_filemaker_config()
    field_data = record.get("fieldData", {})
    return {
        "primary_key": normalize_text_value(
            get_field_value(field_data, config["contacts_key_field"])
        ),
        "customer_ref": normalize_text_value(
            get_field_value(field_data, config["contacts_customer_ref_field"])
        ),
        "name": normalize_text_value(
            get_field_value(field_data, config["contacts_name_field"])
        ),
        "email": normalize_email_value(
            get_field_value(field_data, config["contacts_email_field"])
        ),
        "position": normalize_text_value(
            get_field_value(field_data, config["contacts_position_field"])
        ),
        "phone": normalize_text_value(
            get_field_value(field_data, config["contacts_phone_field"])
        ),
        "cell": normalize_text_value(
            get_field_value(field_data, config["contacts_cell_field"])
        ),
        "active": normalize_text_value(
            get_field_value(field_data, config["contacts_active_field"])
        ),
    }


def fetch_filemaker_master_data():
    config = get_filemaker_config()
    cache_seconds = get_filemaker_master_data_cache_seconds()
    cache_key = build_filemaker_master_data_cache_key()
    now = time.time()

    if (
        cache_seconds
        and _FILEMAKER_MASTER_DATA_CACHE["key"] == cache_key
        and _FILEMAKER_MASTER_DATA_CACHE["result"] is not None
        and _FILEMAKER_MASTER_DATA_CACHE["expires_at"] > now
    ):
        return _FILEMAKER_MASTER_DATA_CACHE["result"]

    if not config["customers_layout"] or not config["contacts_layout"]:
        return build_empty_filemaker_master_data("missing_master_layout")

    customers_result = fetch_all_layout_records(
        config["customers_layout"],
        batch_size=500,
    )

    if customers_result["status"] != "ok":
        return build_empty_filemaker_master_data(customers_result["status"])

    contacts_result = fetch_all_layout_records(
        config["contacts_layout"],
        batch_size=500,
    )

    if contacts_result["status"] != "ok":
        return build_empty_filemaker_master_data(contacts_result["status"])

    customers_by_key = {}
    all_contacts = []
    contacts_by_customer_key = {}
    contacts_by_email = {}

    for raw_record in customers_result.get("records", []):
        customer = map_filemaker_customer_master_record(raw_record)

        if customer["type"].lower() != "customer":
            continue

        if not customer["primary_key"]:
            continue

        customers_by_key[customer["primary_key"]] = customer

    for raw_record in contacts_result.get("records", []):
        contact = map_filemaker_contact_master_record(raw_record)

        if not contact["customer_ref"]:
            continue

        all_contacts.append(contact)

        if contact["active"].lower() == "active":
            contacts_by_customer_key.setdefault(contact["customer_ref"], []).append(contact)

        if contact["email"]:
            existing_contact = contacts_by_email.get(contact["email"])

            if (
                existing_contact is None
                or (
                    str(existing_contact.get("active") or "").strip().lower() != "active"
                    and contact["active"].lower() == "active"
                )
            ):
                contacts_by_email[contact["email"]] = contact

    for customer_key, customer_contacts in contacts_by_customer_key.items():
        contacts_by_customer_key[customer_key] = sorted(
            customer_contacts,
            key=lambda item: (
                item["name"].lower(),
                item["email"],
            ),
        )

    result = {
        "connected": True,
        "status": "ok",
        "customers_by_key": customers_by_key,
        "all_contacts": all_contacts,
        "contacts_by_customer_key": contacts_by_customer_key,
        "contacts_by_email": contacts_by_email,
    }

    if cache_seconds:
        _FILEMAKER_MASTER_DATA_CACHE["key"] = cache_key
        _FILEMAKER_MASTER_DATA_CACHE["expires_at"] = now + cache_seconds
        _FILEMAKER_MASTER_DATA_CACHE["result"] = result

    return result


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
