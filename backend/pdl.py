import os
from pathlib import Path
import time

import requests
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).with_name(".env"))

PDL_ROOT_URL = "https://api.peopledatalabs.com"
_PDL_CACHE = {}


def get_bool_env(name, default=False):
    value = os.getenv(name)

    if value is None:
        return default

    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def get_int_env(name, default):
    value = os.getenv(name)

    if value is None:
        return default

    try:
        return int(str(value).strip())
    except ValueError:
        return default


def get_pdl_config():
    return {
        "enabled": get_bool_env("PDL_ENABLED", default=False),
        "api_key": os.getenv("PDL_API_KEY", "").strip(),
        "cache_seconds": get_int_env("PDL_CACHE_SECONDS", default=1800),
        "sticky_cache": get_bool_env("PDL_CACHE_UNTIL_REFRESH", default=False),
        "logging_enabled": get_bool_env("ENABLE_PDL_LOGS", default=False)
        or get_bool_env("ENABLE_PERF_LOGS", default=False),
    }


def has_pdl_config():
    config = get_pdl_config()
    return bool(config["enabled"] and config["api_key"])


def get_pdl_headers():
    config = get_pdl_config()
    return {
        "Accept": "application/json",
        "X-Api-Key": config["api_key"],
    }


def build_pdl_cache_key(kind, **parts):
    config = get_pdl_config()
    normalized_parts = tuple(sorted((key, str(value or "")) for key, value in parts.items()))
    return (kind, config["api_key"], normalized_parts)


def should_log_pdl():
    return bool(get_pdl_config().get("logging_enabled"))


def log_pdl_event(event, **details):
    if not should_log_pdl():
        return

    detail_text = " ".join(
        f"{key}={str(value).replace(chr(10), ' ')}"
        for key, value in details.items()
        if str(value).strip()
    )
    if detail_text:
        print(f"PDL {event} {detail_text}")
    else:
        print(f"PDL {event}")


def log_pdl_credit_headers(response):
    if not should_log_pdl() or response is None:
        return

    interesting_headers = extract_pdl_credit_headers(response)
    populated = {key: value for key, value in interesting_headers.items() if str(value).strip()}
    if populated:
        log_pdl_event("credit_headers", **populated)


def extract_pdl_credit_headers(response):
    if response is None:
        return {}
    return {
        "x-call-credits-spent": str(response.headers.get("X-Call-Credits-Spent", "")).strip(),
        "x-totallimit-remaining": str(response.headers.get("X-TotalLimit-Remaining", "")).strip(),
        "x-lifetime-used": str(response.headers.get("X-Lifetime-Used", "")).strip(),
        "x-ratelimit-remaining": str(response.headers.get("X-RateLimit-Remaining", "")).strip(),
    }


def get_cached_pdl_result(cache_key):
    config = get_pdl_config()
    sticky_cache = bool(config.get("sticky_cache"))
    cache_seconds = max(0, int(config.get("cache_seconds") or 0))

    if cache_seconds <= 0 and not sticky_cache:
        return None

    cached = _PDL_CACHE.get(cache_key)
    if not cached:
        log_pdl_event("cache_miss", kind=cache_key[0] if cache_key else "")
        return None

    if not sticky_cache and cached["expires_at"] <= time.time():
        _PDL_CACHE.pop(cache_key, None)
        log_pdl_event("cache_expired", kind=cache_key[0] if cache_key else "")
        return None

    log_pdl_event("cache_hit", kind=cache_key[0] if cache_key else "")
    result = cached["result"]
    if isinstance(result, dict):
        cached_result = dict(result)
        cached_result["cached"] = True
        return cached_result
    return result


def set_cached_pdl_result(cache_key, result):
    config = get_pdl_config()
    sticky_cache = bool(config.get("sticky_cache"))
    cache_seconds = max(0, int(config.get("cache_seconds") or 0))

    if cache_seconds <= 0 and not sticky_cache:
        return

    result_to_store = dict(result) if isinstance(result, dict) else result
    if isinstance(result_to_store, dict):
        result_to_store.pop("cached", None)

    _PDL_CACHE[cache_key] = {
        "expires_at": (time.time() + cache_seconds) if not sticky_cache else float("inf"),
        "result": result_to_store,
    }
    log_pdl_event(
        "cache_store",
        kind=cache_key[0] if cache_key else "",
        seconds=("until_refresh" if sticky_cache else cache_seconds),
    )


def clear_cached_pdl_result(cache_key):
    removed = _PDL_CACHE.pop(cache_key, None)
    if removed is not None:
        log_pdl_event("cache_clear", kind=cache_key[0] if cache_key else "")


def check_pdl_connection():
    config = get_pdl_config()

    if not config["enabled"]:
        return {
            "configured": False,
            "connected": False,
            "status": "pdl_disabled",
        }

    if not config["api_key"]:
        return {
            "configured": False,
            "connected": False,
            "status": "missing_api_key",
        }

    sql = "SELECT * FROM person WHERE job_company_name LIKE '%cintas%'"

    try:
        response = requests.get(
            f"{PDL_ROOT_URL}/v5/person/search",
            headers=get_pdl_headers(),
            params={"sql": sql, "size": 1},
            timeout=30,
        )
    except requests.RequestException as error:
        return {
            "configured": True,
            "connected": False,
            "status": get_request_error_status(error),
        }

    if response.status_code != 200:
        log_pdl_credit_headers(response)
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

    status_code = int(payload.get("status", 200) or 200)
    log_pdl_credit_headers(response)
    return {
        "configured": True,
        "connected": status_code == 200,
        "status": "connected" if status_code == 200 else f"api_status_{status_code}",
    }


def search_pdl_people(sql_query, size=10, force_refresh=False):
    config = get_pdl_config()

    if not config["enabled"]:
        return {
            "status": "pdl_disabled",
            "results": [],
            "total": 0,
        }

    if not config["api_key"]:
        return {
            "status": "missing_api_key",
            "results": [],
            "total": 0,
        }

    normalized_sql = str(sql_query or "").strip()
    result_size = max(1, min(int(size or 10), 25))

    if not normalized_sql:
        return {
            "status": "missing_sql",
            "results": [],
            "total": 0,
        }

    cache_key = build_pdl_cache_key("person_search", sql=normalized_sql, size=result_size)
    if force_refresh:
        clear_cached_pdl_result(cache_key)
    cached = get_cached_pdl_result(cache_key)
    if cached is not None:
        return cached

    try:
        log_pdl_event("fresh_lookup", kind="person_search", size=result_size)
        response = requests.post(
            f"{PDL_ROOT_URL}/v5/person/search",
            headers={
                **get_pdl_headers(),
                "Content-Type": "application/json",
            },
            json={"sql": normalized_sql, "size": result_size},
            timeout=30,
        )
    except requests.RequestException as error:
        return {
            "status": get_request_error_status(error),
            "results": [],
            "total": 0,
        }

    if response.status_code != 200:
        log_pdl_credit_headers(response)
        error_message = ""
        try:
            payload = response.json()
            error_value = payload.get("error")
            if isinstance(error_value, dict):
                error_message = str(
                    error_value.get("message")
                    or error_value.get("detail")
                    or error_value.get("type")
                    or ""
                ).strip()
            else:
                error_message = str(
                    error_value
                    or payload.get("message")
                    or payload.get("detail")
                    or ""
                ).strip()
        except ValueError:
            error_message = str(response.text or "").strip()
        return {
            "status": f"http_{response.status_code}",
            "error_message": error_message,
            "credit_headers": extract_pdl_credit_headers(response),
            "results": [],
            "total": 0,
        }

    try:
        payload = response.json()
    except ValueError:
        return {
            "status": "invalid_response",
            "results": [],
            "total": 0,
        }

    log_pdl_credit_headers(response)
    results = payload.get("data") or payload.get("results") or []
    total = payload.get("total") or payload.get("total_results") or len(results)
    result = {
        "status": "ok",
        "results": results,
        "total": int(total or 0),
        "sql": normalized_sql,
        "credit_headers": extract_pdl_credit_headers(response),
        "cached": False,
    }
    set_cached_pdl_result(cache_key, result)
    return result


def enrich_pdl_person(email="", full_name="", organization_name="", locality="", region="", force_refresh=False):
    config = get_pdl_config()

    if not config["enabled"]:
        return {
            "status": "pdl_disabled",
            "result": None,
        }

    if not config["api_key"]:
        return {
            "status": "missing_api_key",
            "result": None,
        }

    email = str(email or "").strip().lower()
    full_name = str(full_name or "").strip()
    organization_name = str(organization_name or "").strip()
    locality = str(locality or "").strip()
    region = str(region or "").strip()

    if not email and not full_name:
        return {
            "status": "missing_person_inputs",
            "result": None,
        }

    params = {}
    if email:
        params["email"] = email
    if full_name:
        parts = full_name.split()
        if parts:
            params["first_name"] = parts[0]
        if len(parts) > 1:
            params["last_name"] = parts[-1]
    if organization_name:
        params["company"] = organization_name
    if locality:
        params["locality"] = locality
    if region:
        params["region"] = region

    cache_key = build_pdl_cache_key(
        "person_enrich",
        email=email,
        full_name=full_name,
        organization_name=organization_name,
        locality=locality,
        region=region,
    )
    if force_refresh:
        clear_cached_pdl_result(cache_key)
    cached = get_cached_pdl_result(cache_key)
    if cached is not None:
        return cached

    def perform_enrich_request(request_params, fallback_used=False):
        try:
            log_pdl_event(
                "fresh_lookup",
                kind="person_enrich",
                email=email or full_name,
                fallback=("1" if fallback_used else "0"),
            )
            response = requests.get(
                f"{PDL_ROOT_URL}/v5/person/enrich",
                headers=get_pdl_headers(),
                params=request_params,
                timeout=30,
            )
        except requests.RequestException as error:
            return {
                "status": get_request_error_status(error),
                "result": None,
            }

        if response.status_code != 200:
            log_pdl_credit_headers(response)
            error_message = ""
            try:
                payload = response.json()
                error_value = payload.get("error")
                if isinstance(error_value, dict):
                    error_message = str(
                        error_value.get("message")
                        or error_value.get("detail")
                        or error_value.get("type")
                        or ""
                    ).strip()
                else:
                    error_message = str(
                        error_value
                        or payload.get("message")
                        or payload.get("detail")
                        or ""
                    ).strip()
            except ValueError:
                error_message = str(response.text or "").strip()
            return {
                "status": f"http_{response.status_code}",
                "error_message": error_message,
                "credit_headers": extract_pdl_credit_headers(response),
                "result": None,
            }

        try:
            payload = response.json()
        except ValueError:
            return {
                "status": "invalid_response",
                "result": None,
            }

        log_pdl_credit_headers(response)
        return {
            "status": "ok",
            "result": payload.get("data") or payload,
            "credit_headers": extract_pdl_credit_headers(response),
            "cached": False,
            "fallback_used": fallback_used,
        }

    result = perform_enrich_request(params, fallback_used=False)
    if (
        result.get("status") in {"http_404", "ok"}
        and not result.get("result")
        and (locality or region)
    ):
        looser_params = dict(params)
        looser_params.pop("locality", None)
        looser_params.pop("region", None)
        result = perform_enrich_request(looser_params, fallback_used=True)

    set_cached_pdl_result(cache_key, result)
    return result


def get_request_error_status(error):
    if isinstance(error, requests.Timeout):
        return "timeout"

    if isinstance(error, requests.ConnectionError):
        return "connection_error"

    return "request_error"
