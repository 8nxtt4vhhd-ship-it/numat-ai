import os
from pathlib import Path
import time

import requests
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).with_name(".env"))

APOLLO_ROOT_URL = "https://api.apollo.io"
_APOLLO_CACHE = {}


def get_apollo_config():
    return {
        "enabled": get_bool_env("APOLLO_ENABLED", default=False),
        "api_key": os.getenv("APOLLO_API_KEY", "").strip(),
        "cache_seconds": get_int_env("APOLLO_CACHE_SECONDS", default=1800),
        "sticky_cache": get_bool_env("APOLLO_CACHE_UNTIL_REFRESH", default=False),
        "logging_enabled": get_bool_env("ENABLE_APOLLO_LOGS", default=False)
        or get_bool_env("ENABLE_PERF_LOGS", default=False),
    }


def get_bool_env(name, default=False):
    value = os.getenv(name)

    if value is None:
        return default

    return value.strip().lower() in ["1", "true", "yes", "on"]


def has_apollo_config():
    config = get_apollo_config()
    return bool(config["enabled"] and config["api_key"])


def get_int_env(name, default):
    value = os.getenv(name)

    if value is None:
        return default

    try:
        return int(str(value).strip())
    except ValueError:
        return default


def get_apollo_headers():
    config = get_apollo_config()

    return {
        "Content-Type": "application/json",
        "Cache-Control": "no-cache",
        "X-Api-Key": config["api_key"],
    }


def build_apollo_cache_key(kind, **parts):
    config = get_apollo_config()
    normalized_parts = tuple(sorted((key, str(value or "")) for key, value in parts.items()))
    return (kind, config["api_key"], normalized_parts)


def should_log_apollo():
    config = get_apollo_config()
    return bool(config.get("logging_enabled"))


def log_apollo_event(event, **details):
    if not should_log_apollo():
        return

    detail_text = " ".join(
        f"{key}={str(value).replace(chr(10), ' ')}"
        for key, value in details.items()
        if str(value).strip()
    )
    if detail_text:
        print(f"APOLLO {event} {detail_text}")
    else:
        print(f"APOLLO {event}")


def get_cached_apollo_result(cache_key):
    config = get_apollo_config()
    sticky_cache = bool(config.get("sticky_cache"))
    cache_seconds = max(0, int(config.get("cache_seconds") or 0))

    if cache_seconds <= 0 and not sticky_cache:
        return None

    cached = _APOLLO_CACHE.get(cache_key)
    if not cached:
        log_apollo_event("cache_miss", kind=cache_key[0] if cache_key else "")
        return None

    if not sticky_cache and cached["expires_at"] <= time.time():
        _APOLLO_CACHE.pop(cache_key, None)
        log_apollo_event("cache_expired", kind=cache_key[0] if cache_key else "")
        return None

    log_apollo_event("cache_hit", kind=cache_key[0] if cache_key else "")
    result = cached["result"]
    if isinstance(result, dict):
        cached_result = dict(result)
        cached_result["cached"] = True
        return cached_result
    return result


def set_cached_apollo_result(cache_key, result):
    config = get_apollo_config()
    sticky_cache = bool(config.get("sticky_cache"))
    cache_seconds = max(0, int(config.get("cache_seconds") or 0))

    if cache_seconds <= 0 and not sticky_cache:
        return

    result_to_store = dict(result) if isinstance(result, dict) else result
    if isinstance(result_to_store, dict):
        result_to_store.pop("cached", None)

    _APOLLO_CACHE[cache_key] = {
        "expires_at": (time.time() + cache_seconds) if not sticky_cache else float("inf"),
        "result": result_to_store,
    }
    log_apollo_event(
        "cache_store",
        kind=cache_key[0] if cache_key else "",
        seconds=("until_refresh" if sticky_cache else cache_seconds),
    )


def clear_cached_apollo_result(cache_key):
    removed = _APOLLO_CACHE.pop(cache_key, None)
    if removed is not None:
        log_apollo_event("cache_clear", kind=cache_key[0] if cache_key else "")


def check_apollo_connection():
    config = get_apollo_config()

    if not config["enabled"]:
        return {
            "configured": False,
            "connected": False,
            "status": "apollo_disabled",
        }

    if not config["api_key"]:
        return {
            "configured": False,
            "connected": False,
            "status": "missing_api_key",
        }

    try:
        response = requests.get(
            f"{APOLLO_ROOT_URL}/v1/auth/health",
            headers=get_apollo_headers(),
            timeout=20,
        )
    except requests.RequestException as error:
        return {
            "configured": True,
            "connected": False,
            "status": get_request_error_status(error),
        }

    if response.status_code != 200:
        return {
            "configured": True,
            "connected": False,
            "status": f"http_{response.status_code}",
        }

    try:
        payload = response.json()
    except ValueError:
        return {
            "configured": True,
            "connected": False,
            "status": "invalid_response",
        }

    is_logged_in = bool(payload.get("is_logged_in"))
    has_master_key = bool(payload.get("has_master_api_key"))

    status = "connected"
    if not is_logged_in:
        status = "auth_failed"
    elif not has_master_key:
        status = "connected_no_master_key"

    return {
        "configured": True,
        "connected": is_logged_in,
        "status": status,
        "is_logged_in": is_logged_in,
        "has_master_api_key": has_master_key,
    }


def search_apollo_people(
    domain="",
    titles=None,
    organization_locations=None,
    person_locations=None,
    organization_ids=None,
    organization_names=None,
    person_seniorities=None,
    keywords="",
    per_page=10,
    page=1,
    force_refresh=False,
):
    health = check_apollo_connection()

    if not health.get("configured"):
        return {
            "status": health.get("status", "missing_config"),
            "results": [],
            "total_entries": 0,
        }

    if not health.get("connected"):
        return {
            "status": health.get("status", "connection_failed"),
            "results": [],
            "total_entries": 0,
        }

    params = {
        "per_page": max(1, min(int(per_page or 10), 25)),
        "page": max(1, int(page or 1)),
    }

    normalized_domain = normalize_domain(domain)
    normalized_titles = normalize_string_list(titles or [])
    normalized_locations = normalize_string_list(organization_locations or [])
    normalized_person_locations = normalize_string_list(person_locations or [])
    normalized_organization_ids = normalize_string_list(organization_ids or [])
    normalized_organization_names = normalize_string_list(organization_names or [])
    normalized_person_seniorities = normalize_string_list(person_seniorities or [])
    normalized_keywords = str(keywords or "").strip()

    if normalized_domain:
        params["q_organization_domains_list[]"] = [normalized_domain]

    if normalized_titles:
        params["person_titles[]"] = normalized_titles

    if normalized_organization_ids:
        params["organization_ids[]"] = normalized_organization_ids

    if normalized_organization_names:
        params["q_organization_names[]"] = normalized_organization_names
        if len(normalized_organization_names) == 1:
            params["q_organization_name"] = normalized_organization_names[0]

    if normalized_person_seniorities:
        params["person_seniorities[]"] = normalized_person_seniorities

    if normalized_keywords:
        params["q_keywords"] = normalized_keywords

    if normalized_locations:
        params["organization_locations[]"] = normalized_locations

    if normalized_person_locations:
        params["person_locations[]"] = normalized_person_locations

    cache_key = build_apollo_cache_key(
        "people_search",
        domain=normalized_domain,
        titles="|".join(normalized_titles),
        organization_ids="|".join(normalized_organization_ids),
        organization_names="|".join(normalized_organization_names),
        person_seniorities="|".join(normalized_person_seniorities),
        keywords=normalized_keywords,
        organization_locations="|".join(normalized_locations),
        person_locations="|".join(normalized_person_locations),
        per_page=params["per_page"],
        page=params["page"],
    )
    if force_refresh:
        clear_cached_apollo_result(cache_key)
    cached = get_cached_apollo_result(cache_key)
    if cached is not None:
        return cached

    try:
        log_apollo_event(
            "fresh_lookup",
            kind="people_search",
            domain=normalized_domain,
            titles="|".join(normalized_titles),
            organization_ids="|".join(normalized_organization_ids),
            organization_names="|".join(normalized_organization_names),
            person_seniorities="|".join(normalized_person_seniorities),
            keywords=normalized_keywords,
            organization_locations="|".join(normalized_locations),
            person_locations="|".join(normalized_person_locations),
            per_page=params["per_page"],
            page=params["page"],
        )
        response = requests.post(
            f"{APOLLO_ROOT_URL}/api/v1/mixed_people/api_search",
            params=params,
            headers=get_apollo_headers(),
            timeout=30,
        )
    except requests.RequestException as error:
        return {
            "status": get_request_error_status(error),
            "results": [],
            "total_entries": 0,
        }

    if response.status_code == 403:
        return {
            "status": "master_api_key_required",
            "results": [],
            "total_entries": 0,
        }

    if response.status_code != 200:
        return {
            "status": f"http_{response.status_code}",
            "results": [],
            "total_entries": 0,
        }

    try:
        payload = response.json()
    except ValueError:
        return {
            "status": "invalid_response",
            "results": [],
            "total_entries": 0,
        }

    result = {
        "status": "ok",
        "results": payload.get("people", []),
        "total_entries": payload.get("total_entries", 0),
        "domain": normalized_domain,
        "titles": normalized_titles,
        "organization_ids": normalized_organization_ids,
        "organization_names": normalized_organization_names,
        "person_seniorities": normalized_person_seniorities,
        "keywords": normalized_keywords,
        "organization_locations": normalized_locations,
        "person_locations": normalized_person_locations,
        "page": params["page"],
        "cached": False,
    }
    set_cached_apollo_result(cache_key, result)
    return result


def enrich_apollo_organization(domain, force_refresh=False):
    health = check_apollo_connection()

    if not health.get("configured"):
        return {"status": health.get("status", "missing_config"), "result": None}

    normalized_domain = normalize_domain(domain)
    if not normalized_domain:
        return {"status": "missing_domain", "result": None}

    cache_key = build_apollo_cache_key("organization_enrich", domain=normalized_domain)
    if force_refresh:
        clear_cached_apollo_result(cache_key)
    cached = get_cached_apollo_result(cache_key)
    if cached is not None:
        return cached

    try:
        log_apollo_event("fresh_lookup", kind="organization_enrich", domain=normalized_domain)
        response = requests.get(
            f"{APOLLO_ROOT_URL}/api/v1/organizations/enrich",
            params={"domain": normalized_domain},
            headers=get_apollo_headers(),
            timeout=30,
        )
    except requests.RequestException as error:
        return {"status": get_request_error_status(error), "result": None}

    if response.status_code != 200:
        return {"status": f"http_{response.status_code}", "result": None}

    try:
        payload = response.json()
    except ValueError:
        return {"status": "invalid_response", "result": None}

    result = {
        "status": "ok",
        "result": payload.get("organization") or payload.get("account") or payload,
        "domain": normalized_domain,
        "cached": False,
    }
    set_cached_apollo_result(cache_key, result)
    return result


def enrich_apollo_person(name="", email="", domain="", organization_name="", force_refresh=False):
    health = check_apollo_connection()

    if not health.get("configured"):
        return {"status": health.get("status", "missing_config"), "result": None}

    params = {}
    normalized_domain = normalize_domain(domain)
    normalized_email = str(email or "").strip().lower()
    normalized_name = str(name or "").strip()
    normalized_org_name = str(organization_name or "").strip()

    if normalized_email:
        params["email"] = normalized_email
    if normalized_name:
        params["name"] = normalized_name
    if normalized_domain:
        params["domain"] = normalized_domain
    if normalized_org_name:
        params["organization_name"] = normalized_org_name

    if not params:
        return {"status": "missing_person_inputs", "result": None}

    cache_key = build_apollo_cache_key(
        "person_enrich",
        name=normalized_name,
        email=normalized_email,
        domain=normalized_domain,
        organization_name=normalized_org_name,
    )
    if force_refresh:
        clear_cached_apollo_result(cache_key)
    cached = get_cached_apollo_result(cache_key)
    if cached is not None:
        return cached

    try:
        log_apollo_event(
            "fresh_lookup",
            kind="person_enrich",
            email=normalized_email,
            name=normalized_name,
            domain=normalized_domain,
            organization_name=normalized_org_name,
        )
        response = requests.post(
            f"{APOLLO_ROOT_URL}/api/v1/people/match",
            params=params,
            headers=get_apollo_headers(),
            timeout=30,
        )
    except requests.RequestException as error:
        return {"status": get_request_error_status(error), "result": None}

    if response.status_code != 200:
        return {"status": f"http_{response.status_code}", "result": None}

    try:
        payload = response.json()
    except ValueError:
        return {"status": "invalid_response", "result": None}

    person = payload.get("person") or payload.get("contact") or payload
    matched = bool(person and any(person.get(field) for field in ["id", "name", "first_name", "email"]))

    result = {
        "status": "ok" if matched else "no_match",
        "result": person if matched else None,
        "domain": normalized_domain,
        "email": normalized_email,
        "name": normalized_name,
        "cached": False,
    }
    set_cached_apollo_result(cache_key, result)
    return result


def normalize_domain(domain):
    value = (domain or "").strip().lower()
    value = value.removeprefix("https://")
    value = value.removeprefix("http://")
    value = value.removeprefix("www.")
    value = value.split("/", 1)[0]
    value = value.lstrip("@")
    return value


def normalize_string_list(values):
    normalized = []

    for value in values:
        cleaned = str(value).strip()
        if cleaned:
            normalized.append(cleaned)

    return normalized


def parse_multivalue_input(raw_value):
    return [
        chunk.strip()
        for chunk in str(raw_value or "").split(",")
        if chunk.strip()
    ]


def get_request_error_status(error):
    if isinstance(error, requests.Timeout):
        return "timeout"

    if isinstance(error, requests.ConnectionError):
        return "connection_error"

    return "request_error"
