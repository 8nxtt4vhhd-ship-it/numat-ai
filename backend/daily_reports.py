import argparse
from datetime import date, datetime, timedelta
import json
import os
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=BASE_DIR / ".env")

from daily_dash import build_daily_dash_payload, build_daily_dash_pdf
from data_sources import get_orders_for_analysis
from finance import fetch_aged_debt_summary
from m365 import has_m365_reporting_config, send_m365_reporting_mail
from monthly_reports import failure_recipients, load_users
from production import fetch_production_analysis_data

DEFAULT_DELIVERY_LOG_PATH = BASE_DIR / "data" / "daily_dash_deliveries.json"
ALABAMA_TIMEZONE = ZoneInfo("America/Chicago")


def parse_day(value):
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date()
    except ValueError as error:
        raise argparse.ArgumentTypeError("Use YYYY-MM-DD, for example 2026-08-18") from error


def all_active_user_recipients():
    recipients = []
    seen = set()
    for user in load_users():
        email = str(user.get("m365_email") or "").strip().lower()
        if not bool(user.get("active", True)) or "@" not in email or email in seen:
            continue
        recipients.append(email)
        seen.add(email)
    return recipients


def delivery_log_path():
    configured = os.getenv("M365_DAILY_DASH_DELIVERY_LOG_PATH", "").strip()
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


def notify_failure(report_day, reason):
    recipients = failure_recipients()
    if not recipients or not has_m365_reporting_config():
        return {"status": "notification_not_sent"}
    return send_m365_reporting_mail(
        recipients,
        f"NuMat scheduled Daily Dash failed - {report_day}",
        f"The Daily Dash for {report_day} was not sent.\n\nReason: {reason}",
    )


def build_report(production_result, orders_result, finance_result, requested_day=None):
    payload = build_daily_dash_payload(
        production_result,
        orders_result=orders_result,
        finance_result=finance_result,
        report_date=requested_day.isoformat() if requested_day else None,
    )
    report_day = payload["report_date"]
    return {
        "payload": payload,
        "content": build_daily_dash_pdf(payload),
        "filename": f"numat-daily-dash-{report_day}.pdf",
        "subject": f"NuMat Daily Dash - {datetime.strptime(report_day, '%Y-%m-%d').strftime('%-d %B %Y')}",
        "body": (
            f"Attached is the NuMat Daily Dash for the completed production day {report_day}.\n\n"
            "The report includes daily and month-to-date production, labour, press, backlog and commercial controls."
        ),
    }


def deliver(requested_day=None, dry_run=False, output_dir=None, force=False, recipients=None):
    production_result = fetch_production_analysis_data(force_refresh=True, include_today=True)
    if production_result.get("status") not in {"ok", "partial"}:
        return {"status": "production_data_error", "error": production_result.get("warning") or production_result.get("status")}
    orders_result = get_orders_for_analysis()
    finance_result = fetch_aged_debt_summary(force_refresh=True)
    try:
        report = build_report(production_result, orders_result, finance_result, requested_day=requested_day)
    except Exception as error:
        report_day = requested_day.isoformat() if requested_day else str(date.today() - timedelta(days=1))
        if not dry_run:
            notify_failure(report_day, str(error))
        return {"status": "generation_error", "error": str(error)}

    report_day = report["payload"]["report_date"]
    if requested_day and report_day != requested_day.isoformat():
        reason = f"No production record is available for {requested_day.isoformat()}; latest available day is {report_day}."
        if not dry_run:
            notify_failure(requested_day.isoformat(), reason)
        return {"status": "requested_day_unavailable", "error": reason}
    key = f"daily-dash:{report_day}"
    log = load_delivery_log()
    if not dry_run and not force and (log.get(key) or {}).get("status") == "sent":
        return {"status": "already_sent", "key": key}
    recipients = recipients or all_active_user_recipients()
    recipients = list(dict.fromkeys(str(email).strip().lower() for email in recipients if "@" in str(email)))
    if not recipients:
        return {"status": "missing_recipients", "key": key}

    if dry_run:
        destination = Path(output_dir or BASE_DIR.parent / "output" / "daily-dash")
        destination.mkdir(parents=True, exist_ok=True)
        path = destination / report["filename"]
        path.write_bytes(report["content"])
        return {"status": "dry_run", "key": key, "path": str(path), "recipients": recipients, "unavailable": report["payload"].get("unavailable", [])}

    if not has_m365_reporting_config():
        return {"status": "m365_reporting_disabled", "key": key}
    send_result = send_m365_reporting_mail(
        recipients,
        report["subject"],
        report["body"],
        attachments=[{"name": report["filename"], "content_type": "application/pdf", "content": report["content"]}],
    )
    log[key] = {
        "status": "sent" if send_result.get("status") == "ok" else "failed",
        "attempted_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "recipients": recipients,
        "filename": report["filename"],
        "error": str(send_result.get("error_message") or send_result.get("status") or ""),
    }
    save_delivery_log(log)
    if send_result.get("status") != "ok":
        notify_failure(report_day, log[key]["error"])
    return {**send_result, "key": key, "recipients": recipients}


def main(argv=None):
    parser = argparse.ArgumentParser(description="Generate and send the NuMat Daily Dash PDF.")
    parser.add_argument("--date", type=parse_day, help="Completed production date as YYYY-MM-DD; defaults to the latest available day.")
    parser.add_argument("--today", action="store_true", help="Use today's Alabama calendar date.")
    parser.add_argument("--recipient", action="append", dest="recipients", help="Send only to this email address; may be repeated.")
    parser.add_argument("--dry-run", action="store_true", help="Generate the PDF without sending email.")
    parser.add_argument("--output-dir")
    parser.add_argument("--force", action="store_true", help="Send even if this report date is already recorded as sent.")
    args = parser.parse_args(argv)
    if args.date and args.today:
        parser.error("Use either --date or --today, not both.")
    requested_day = datetime.now(ALABAMA_TIMEZONE).date() if args.today else args.date
    outcome = deliver(
        requested_day=requested_day,
        dry_run=args.dry_run,
        output_dir=args.output_dir,
        force=args.force,
        recipients=args.recipients,
    )
    print(json.dumps(outcome, default=str))
    return 0 if outcome.get("status") in {"ok", "dry_run", "already_sent"} else 1


if __name__ == "__main__":
    sys.exit(main())
