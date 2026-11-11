import argparse
from datetime import datetime, timedelta
from html import escape
import json
import os
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=BASE_DIR / ".env")

from app_settings import get_daily_crm_report_settings, normalize_email_recipients
from crm import fetch_crm_activities, sync_filemaker_crm_recent_cache
from m365 import has_m365_reporting_config, send_m365_reporting_mail


PLANT_TIMEZONE = ZoneInfo("America/Chicago")
DEFAULT_DELIVERY_LOG_PATH = BASE_DIR / "data" / "daily_crm_report_deliveries.json"


def parse_day(value):
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date()
    except ValueError as error:
        raise argparse.ArgumentTypeError("Use YYYY-MM-DD, for example 2026-11-09") from error


def delivery_log_path():
    configured = os.getenv("DAILY_CRM_REPORT_DELIVERY_LOG_PATH", "").strip()
    return Path(configured).expanduser() if configured else DEFAULT_DELIVERY_LOG_PATH


def load_delivery_log():
    try:
        payload = json.loads(delivery_log_path().read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def save_delivery_log(payload):
    path = delivery_log_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def fetch_fresh_crm_activities():
    refreshed = sync_filemaker_crm_recent_cache()
    return refreshed if refreshed.get("status") == "ok" else fetch_crm_activities()


def activities_for_day(crm_result, report_day):
    report_key = report_day.isoformat()
    return [
        item for item in crm_result.get("activities", [])
        if str(item.get("date_created") or "")[:10] == report_key
        and not item.get("exclude")
        and not is_routine_non_sales_activity(item)
        and resolve_customer(item) != "Customer not identified"
    ]


def is_routine_non_sales_activity(activity):
    text = " ".join(str(activity.get(key) or "") for key in ("subject", "body", "crm_type", "crm_category")).casefold()
    return any(phrase in text for phrase in (
        "payroll", "accounts payable", "autopay", "tax filing", "tax payment",
        "w-2", "w2 form", "direct deposit", "payment reminder",
    ))


def resolve_customer(activity):
    candidates = (
        activity.get("customer_company"), activity.get("customer_label"),
        activity.get("recipient_company"), activity.get("sender_company"),
    )
    for candidate in candidates:
        value = str(candidate or "").strip()
        normalized = value.casefold()
        if value and normalized not in {"numat samples", "numat systems", "numat systems customer service"}:
            return value
    return "Customer not identified"


def activity_evidence(activity):
    return {
        "customer": resolve_customer(activity),
        "time": str(activity.get("date_created") or ""),
        "direction": str(activity.get("direction") or "unknown"),
        "employee": str(activity.get("sender_email") or ""),
        "type": str(activity.get("crm_type") or ""),
        "category": str(activity.get("crm_category") or activity.get("activity_category") or ""),
        "subject": str(activity.get("subject") or "")[:300],
        "notes": str(activity.get("body") or "")[:2500],
    }


def generate_analysis(report_day, activities):
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("OpenAI is not configured.")
    from openai import OpenAI

    item_schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {"customer": {"type": "string"}, "summary": {"type": "string"}},
        "required": ["customer", "summary"],
    }
    schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "overall_summary": {"type": "string"},
            "opportunities": {"type": "array", "items": item_schema},
            "follow_ups": {"type": "array", "items": item_schema},
            "concerns": {"type": "array", "items": item_schema},
            "meaningful_updates": {"type": "array", "items": item_schema},
        },
        "required": ["overall_summary", "opportunities", "follow_ups", "concerns", "meaningful_updates"],
    }
    instructions = {
        "task": "Create a concise management summary of today's NuMat CRM activity.",
        "report_date": report_day.isoformat(),
        "rules": [
            "Use only the supplied CRM evidence and never invent a fact.",
            "Do not include activity counts; these are reported elsewhere.",
            "Focus on commercial meaning, actions and management awareness rather than listing every email.",
            "Treat shipping documents, invoices, payroll, accounts payable and routine acknowledgements as operational noise unless they reveal a meaningful order, opportunity, risk or required action.",
            "Do not group unrelated customers under NuMat Samples; use the external company shown in each evidence item.",
            "Combine related entries for the same customer.",
            "Opportunities include new repair orders, appointments, expected repair loads, expansion interest and positive commercial signals.",
            "Follow-ups include promised callbacks, messages awaiting response, scheduled future contact and clear next actions.",
            "Concerns include complaints, quality issues, pricing resistance, reduced demand, repeated non-response or signs of disengagement.",
            "Meaningful updates are useful customer or order developments that do not fit the other sections.",
            "Keep the overall summary to two or three sentences and each item to one concise sentence.",
            "Omit empty or immaterial points.",
        ],
        "activities": [activity_evidence(item) for item in activities],
    }
    response = OpenAI(api_key=api_key).responses.create(
        model=os.getenv("OPENAI_MODEL", "gpt-4.1-mini"),
        input=[
            {"role": "system", "content": "You are a careful B2B sales operations analyst preparing a nightly management email."},
            {"role": "user", "content": json.dumps(instructions, ensure_ascii=False)},
        ],
        temperature=0.1,
        max_output_tokens=3000,
        text={"format": {"type": "json_schema", "name": "daily_crm_summary", "strict": True, "schema": schema}},
    )
    return json.loads(response.output_text)


def build_email(report_day, analysis):
    sections = []
    for key, label, color in (
        ("opportunities", "Opportunities and positive signals", "#087a4b"),
        ("follow_ups", "Follow-ups required", "#245cff"),
        ("concerns", "Concerns to be aware of", "#b42318"),
        ("meaningful_updates", "Meaningful customer updates", "#5b21b6"),
    ):
        items = analysis.get(key) or []
        if not items:
            continue
        rows = "".join(
            f'<li style="margin:0 0 12px"><strong>{escape(str(item.get("customer") or "Customer"))}</strong><div style="margin-top:3px">{escape(str(item.get("summary") or ""))}</div></li>'
            for item in items
        )
        sections.append(f'<section style="margin:0 0 24px"><h2 style="margin:0 0 10px;color:{color};font-size:18px">{label}</h2><ul style="margin:0;padding-left:22px;line-height:1.45">{rows}</ul></section>')
    date_label = report_day.strftime("%B %-d, %Y")
    html = (
        '<div style="font-family:Arial,sans-serif;max-width:760px;color:#172033">'
        '<div style="border-top:6px solid #245cff;padding:22px 24px;border-radius:10px;background:#f8fbff">'
        '<p style="margin:0 0 5px;color:#64748b;font-size:12px;font-weight:bold;text-transform:uppercase">NuMat CRM</p>'
        f'<h1 style="margin:0 0 6px;font-size:24px">Daily CRM management summary</h1><p style="margin:0">{escape(date_label)}</p></div>'
        f'<div style="padding:22px 8px"><p style="font-size:16px;line-height:1.5">{escape(str(analysis.get("overall_summary") or "No material CRM updates were identified."))}</p>{"".join(sections) or "<p>No material CRM updates were identified.</p>"}'
        '<p style="font-size:12px;color:#64748b">AI summary based on CRM activity recorded for the report date. Check the underlying CRM record before making a material decision.</p></div></div>'
    )
    return {"subject": f"NuMat Daily CRM Summary - {report_day.strftime('%m/%d/%Y')}", "body_html": html}


def generate_report(report_day):
    result = fetch_fresh_crm_activities()
    if result.get("status") != "ok":
        raise RuntimeError("CRM data is unavailable.")
    activities = activities_for_day(result, report_day)
    analysis = generate_analysis(report_day, activities) if activities else {
        "overall_summary": "No material CRM activity was recorded for this date.",
        "opportunities": [], "follow_ups": [], "concerns": [], "meaningful_updates": [],
    }
    return {**build_email(report_day, analysis), "analysis": analysis, "activity_count": len(activities)}


def schedule_is_due(now=None, settings=None):
    settings = settings or get_daily_crm_report_settings()
    if not settings.get("enabled"):
        return False
    current = (now or datetime.now(PLANT_TIMEZONE)).astimezone(PLANT_TIMEZONE)
    if current.weekday() >= 5:
        return False
    hour, minute = (int(part) for part in settings["send_time"].split(":"))
    scheduled = current.replace(hour=hour, minute=minute, second=0, microsecond=0)
    return scheduled <= current < scheduled + timedelta(minutes=30)


def deliver(report_day=None, dry_run=False, output_dir=None, force=False, recipients=None):
    report_day = report_day or datetime.now(PLANT_TIMEZONE).date()
    key = f"daily-crm:{report_day.isoformat()}"
    log = load_delivery_log()
    if not dry_run and not force and (log.get(key) or {}).get("status") == "sent":
        return {"status": "already_sent", "key": key}
    settings = get_daily_crm_report_settings()
    recipient_list = normalize_email_recipients(recipients if recipients is not None else settings.get("recipients", []))
    if not recipient_list:
        return {"status": "missing_recipients", "key": key}
    try:
        report = generate_report(report_day)
    except Exception as error:
        return {"status": "generation_error", "key": key, "error": str(error)}
    if dry_run:
        destination = Path(output_dir or BASE_DIR.parent / "output" / "daily-crm")
        destination.mkdir(parents=True, exist_ok=True)
        path = destination / f"daily-crm-summary-{report_day.isoformat()}.html"
        path.write_text(report["body_html"], encoding="utf-8")
        return {"status": "dry_run", "key": key, "path": str(path), "report": report}
    if not has_m365_reporting_config():
        return {"status": "m365_reporting_disabled", "key": key}
    result = send_m365_reporting_mail(recipient_list, report["subject"], report["body_html"], body_content_type="HTML")
    log[key] = {"status": "sent" if result.get("status") == "ok" else "failed", "attempted_at": datetime.now(PLANT_TIMEZONE).isoformat(timespec="seconds"), "recipients": recipient_list, "error": str(result.get("error_message") or "")}
    save_delivery_log(log)
    return {**result, "key": key, "recipients": recipient_list}


def main(argv=None):
    parser = argparse.ArgumentParser(description="Generate and send the NuMat Daily CRM Summary.")
    parser.add_argument("--date", type=parse_day)
    parser.add_argument("--recipient", action="append", dest="recipients")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--output-dir")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--run-if-due", action="store_true")
    args = parser.parse_args(argv)
    settings = get_daily_crm_report_settings()
    if args.run_if_due and not schedule_is_due(settings=settings):
        print(json.dumps({"status": "not_due", "enabled": settings.get("enabled"), "send_time": settings.get("send_time")}))
        return 0
    outcome = deliver(args.date, args.dry_run, args.output_dir, args.force, args.recipients)
    print(json.dumps({key: value for key, value in outcome.items() if key != "report"}, default=str))
    return 0 if outcome.get("status") in {"ok", "dry_run", "already_sent", "not_due"} else 1


if __name__ == "__main__":
    sys.exit(main())
