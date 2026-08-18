from datetime import datetime, timedelta
import os
from threading import Lock
import time
from urllib.parse import quote
from zoneinfo import ZoneInfo

import requests

from m365 import M365_GRAPH_ROOT, get_m365_reporting_access_token

UK_TIMEZONE = ZoneInfo("Europe/London")
GRAPH_TIMEZONE = "GMT Standard Time"
_CALENDAR_LOCK = Lock()
_CALENDAR_ID_CACHE = {"value": "", "expires_at": 0}
_EVENT_CACHE = {}


def get_calendar_config():
    return {
        "owner": os.getenv("M365_SHARED_CALENDAR_OWNER", "customerservice@numatsystems.com").strip().lower(),
        "name": os.getenv("M365_SHARED_CALENDAR_NAME", "Shared NuMat Calendar").strip(),
        "cache_seconds": max(0, int(os.getenv("M365_SHARED_CALENDAR_CACHE_SECONDS", "180") or 180)),
    }


def graph_headers(access_token, content_type=False):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Prefer": f'outlook.timezone="{GRAPH_TIMEZONE}", outlook.body-content-type="text"',
    }
    if content_type:
        headers["Content-Type"] = "application/json"
    return headers


def graph_error(response):
    try:
        payload = response.json()
        error = payload.get("error") or {}
        return str(error.get("message") or error.get("code") or response.text or "").strip()
    except ValueError:
        return str(response.text or "").strip()


def reporting_token():
    token_result = get_m365_reporting_access_token()
    if token_result.get("status") != "ok":
        return token_result
    return {"status": "ok", "access_token": token_result["access_token"]}


def discover_shared_calendar(force_refresh=False):
    now = time.time()
    with _CALENDAR_LOCK:
        if not force_refresh and _CALENDAR_ID_CACHE["value"] and _CALENDAR_ID_CACHE["expires_at"] > now:
            return {"status": "ok", "calendar_id": _CALENDAR_ID_CACHE["value"], "cached": True}
    config = get_calendar_config()
    token = reporting_token()
    if token.get("status") != "ok":
        return token
    url = f"{M365_GRAPH_ROOT}/users/{quote(config['owner'])}/calendars"
    calendars = []
    while url:
        try:
            response = requests.get(url, headers=graph_headers(token["access_token"]), timeout=30)
        except requests.RequestException as error:
            return {"status": "request_error", "error_message": str(error)}
        if response.status_code != 200:
            return {"status": f"http_{response.status_code}", "error_message": graph_error(response)}
        payload = response.json()
        calendars.extend(payload.get("value") or [])
        url = payload.get("@odata.nextLink")
    calendar = next(
        (item for item in calendars if str(item.get("name") or "").strip().casefold() == config["name"].casefold()),
        None,
    )
    if not calendar:
        return {
            "status": "calendar_not_found",
            "error_message": f"Calendar '{config['name']}' was not found in {config['owner']}.",
            "available_calendars": [str(item.get("name") or "") for item in calendars],
        }
    calendar_id = str(calendar.get("id") or "").strip()
    with _CALENDAR_LOCK:
        _CALENDAR_ID_CACHE["value"] = calendar_id
        _CALENDAR_ID_CACHE["expires_at"] = now + 3600
    return {"status": "ok", "calendar_id": calendar_id, "calendar_name": calendar.get("name"), "cached": False}


def parse_graph_datetime(payload):
    text = str((payload or {}).get("dateTime") or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UK_TIMEZONE)
    return parsed.astimezone(UK_TIMEZONE)


def normalize_event(item):
    start = parse_graph_datetime(item.get("start"))
    end = parse_graph_datetime(item.get("end"))
    location = item.get("location") or {}
    organizer = (item.get("organizer") or {}).get("emailAddress") or {}
    attendees = []
    for attendee in item.get("attendees") or []:
        address = (attendee.get("emailAddress") or {}).get("address")
        if address:
            attendees.append(str(address).strip().lower())
    return {
        "id": str(item.get("id") or ""),
        "subject": str(item.get("subject") or "Untitled event").strip(),
        "start": start.isoformat() if start else "",
        "end": end.isoformat() if end else "",
        "start_date": start.strftime("%Y-%m-%d") if start else "",
        "end_date": end.strftime("%Y-%m-%d") if end else "",
        "is_all_day": bool(item.get("isAllDay")),
        "location": str(location.get("displayName") or "").strip(),
        "body_preview": str(item.get("bodyPreview") or "").strip(),
        "notes": str((item.get("body") or {}).get("content") or item.get("bodyPreview") or "").strip(),
        "organizer": str(organizer.get("address") or "").strip().lower(),
        "attendees": attendees,
        "web_link": str(item.get("webLink") or ""),
        "series_master_id": str(item.get("seriesMasterId") or ""),
        "event_type": str(item.get("type") or "singleInstance"),
        "is_cancelled": bool(item.get("isCancelled")),
        "last_modified": str(item.get("lastModifiedDateTime") or ""),
    }


def clear_calendar_cache():
    with _CALENDAR_LOCK:
        _EVENT_CACHE.clear()


def fetch_calendar_events(start, end, force_refresh=False):
    start_dt = start if isinstance(start, datetime) else datetime.fromisoformat(str(start))
    end_dt = end if isinstance(end, datetime) else datetime.fromisoformat(str(end))
    if start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=UK_TIMEZONE)
    if end_dt.tzinfo is None:
        end_dt = end_dt.replace(tzinfo=UK_TIMEZONE)
    cache_key = (start_dt.isoformat(), end_dt.isoformat())
    now = time.time()
    with _CALENDAR_LOCK:
        cached = _EVENT_CACHE.get(cache_key)
        if not force_refresh and cached and cached["expires_at"] > now:
            return {**cached["result"], "cached": True}

    calendar = discover_shared_calendar(force_refresh=force_refresh)
    if calendar.get("status") != "ok":
        return calendar
    token = reporting_token()
    if token.get("status") != "ok":
        return token
    config = get_calendar_config()
    calendar_id = quote(calendar["calendar_id"], safe="")
    params = {
        "startDateTime": start_dt.astimezone(UK_TIMEZONE).isoformat(),
        "endDateTime": end_dt.astimezone(UK_TIMEZONE).isoformat(),
        "$top": "250",
        "$orderby": "start/dateTime",
    }
    url = f"{M365_GRAPH_ROOT}/users/{quote(config['owner'])}/calendars/{calendar_id}/calendarView"
    events = []
    while url:
        try:
            response = requests.get(
                url,
                headers=graph_headers(token["access_token"]),
                params=params if not events else None,
                timeout=45,
            )
        except requests.RequestException as error:
            return {"status": "request_error", "error_message": str(error)}
        if response.status_code != 200:
            return {"status": f"http_{response.status_code}", "error_message": graph_error(response)}
        payload = response.json()
        events.extend(normalize_event(item) for item in payload.get("value") or [])
        url = payload.get("@odata.nextLink")
        params = None
    events = [item for item in events if not item["is_cancelled"]]
    result = {
        "status": "ok",
        "calendar_id": calendar["calendar_id"],
        "calendar_name": config["name"],
        "owner": config["owner"],
        "events": events,
        "synced_at": datetime.now(UK_TIMEZONE).isoformat(timespec="seconds"),
        "cached": False,
    }
    with _CALENDAR_LOCK:
        _EVENT_CACHE[cache_key] = {
            "expires_at": now + config["cache_seconds"],
            "result": result,
        }
    return result


def event_endpoint(event_id=""):
    calendar = discover_shared_calendar()
    if calendar.get("status") != "ok":
        return calendar
    config = get_calendar_config()
    base = (
        f"{M365_GRAPH_ROOT}/users/{quote(config['owner'])}/calendars/"
        f"{quote(calendar['calendar_id'], safe='')}/events"
    )
    return {"status": "ok", "url": f"{base}/{quote(event_id, safe='')}" if event_id else base}


def build_event_payload(subject, start_value, end_value, is_all_day=False, location="", notes="", attendees=""):
    try:
        start = datetime.fromisoformat(str(start_value))
        end = datetime.fromisoformat(str(end_value))
    except ValueError:
        return {"status": "invalid_datetime", "error_message": "Enter a valid start and end date/time."}
    if is_all_day:
        start = start.replace(hour=0, minute=0, second=0, microsecond=0)
        end = end.replace(hour=0, minute=0, second=0, microsecond=0)
        if end <= start:
            end = start + timedelta(days=1)
    elif end <= start:
        return {"status": "invalid_datetime", "error_message": "The end must be after the start."}
    attendee_addresses = []
    for value in str(attendees or "").replace(";", ",").split(","):
        address = value.strip().lower()
        if address and "@" in address and address not in attendee_addresses:
            attendee_addresses.append(address)
    payload = {
        "subject": str(subject or "").strip(),
        "body": {"contentType": "Text", "content": str(notes or "")},
        "start": {"dateTime": start.strftime("%Y-%m-%dT%H:%M:%S"), "timeZone": GRAPH_TIMEZONE},
        "end": {"dateTime": end.strftime("%Y-%m-%dT%H:%M:%S"), "timeZone": GRAPH_TIMEZONE},
        "isAllDay": bool(is_all_day),
        "location": {"displayName": str(location or "").strip()},
        "attendees": [
            {"emailAddress": {"address": address}, "type": "required"}
            for address in attendee_addresses
        ],
    }
    if not payload["subject"]:
        return {"status": "missing_subject", "error_message": "Enter an event title."}
    return {"status": "ok", "payload": payload}


def write_calendar_event(method, event_id="", **values):
    endpoint = event_endpoint(event_id if method in {"patch", "delete"} else "")
    if endpoint.get("status") != "ok":
        return endpoint
    token = reporting_token()
    if token.get("status") != "ok":
        return token
    if method == "delete":
        request = requests.delete
        kwargs = {}
    else:
        built = build_event_payload(**values)
        if built.get("status") != "ok":
            return built
        request = requests.patch if method == "patch" else requests.post
        kwargs = {"json": built["payload"]}
    try:
        response = request(
            endpoint["url"],
            headers=graph_headers(token["access_token"], content_type=True),
            timeout=45,
            **kwargs,
        )
    except requests.RequestException as error:
        return {"status": "request_error", "error_message": str(error)}
    expected = {204} if method == "delete" else {200, 201}
    if response.status_code not in expected:
        return {"status": f"http_{response.status_code}", "error_message": graph_error(response)}
    clear_calendar_cache()
    if method == "delete":
        return {"status": "ok"}
    return {"status": "ok", "event": normalize_event(response.json())}


def create_calendar_event(**values):
    return write_calendar_event("post", **values)


def update_calendar_event(event_id, **values):
    return write_calendar_event("patch", event_id=event_id, **values)


def delete_calendar_event(event_id):
    return write_calendar_event("delete", event_id=event_id)
