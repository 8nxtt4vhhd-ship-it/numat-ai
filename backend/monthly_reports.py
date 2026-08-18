import argparse
from datetime import date, datetime, timedelta
import json
import os
from pathlib import Path
import sys

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=BASE_DIR / ".env")

from ai import generate_production_analysis_report
from m365 import has_m365_reporting_config, send_m365_reporting_mail
from production import build_production_kpi_payload, fetch_production_analysis_data
from production_reports import (
    build_ai_analysis_docx,
    build_ai_report_facts_for_calendar_period,
    build_anomaly_workbook,
)

ANOMALY_USERNAMES = {"kelly", "trudy", "lois"}
DEFAULT_USERS_PATH = BASE_DIR / "data" / "app_users.json"
DEFAULT_DELIVERY_LOG_PATH = BASE_DIR / "data" / "monthly_report_deliveries.json"


class ReportGenerationError(RuntimeError):
    pass


def parse_month(value):
    try:
        return datetime.strptime(str(value), "%Y-%m").date().replace(day=1)
    except ValueError as error:
        raise argparse.ArgumentTypeError("Use YYYY-MM, for example 2026-07") from error


def previous_calendar_month(reference=None):
    reference = reference or date.today()
    first_of_current = reference.replace(day=1)
    end = first_of_current - timedelta(days=1)
    return end.replace(day=1), end


def prior_month(month_start):
    end = month_start - timedelta(days=1)
    return end.replace(day=1), end


def get_users_path():
    configured = os.getenv("APP_USERS_PATH", "").strip()
    return Path(configured).expanduser() if configured else DEFAULT_USERS_PATH


def load_users():
    try:
        payload = json.loads(get_users_path().read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    return payload if isinstance(payload, list) else []


def report_recipients(report_type):
    recipients = []
    seen = set()
    for user in load_users():
        username = str(user.get("username") or "").strip().lower()
        email = str(user.get("m365_email") or "").strip().lower()
        if not bool(user.get("active", True)) or not email or "@" not in email:
            continue
        if report_type == "anomaly" and username not in ANOMALY_USERNAMES:
            continue
        if email not in seen:
            recipients.append(email)
            seen.add(email)
    return recipients


def failure_recipients():
    configured = os.getenv("M365_REPORTING_FAILURE_RECIPIENTS", "").strip()
    if configured:
        return [item.strip().lower() for item in configured.split(",") if "@" in item]
    for user in load_users():
        if (
            str(user.get("username") or "").strip().lower() == "kelly"
            and bool(user.get("active", True))
            and "@" in str(user.get("m365_email") or "")
        ):
            return [str(user["m365_email"]).strip().lower()]
    return []


def notify_failure(report_type, period_slug, reason):
    recipients = failure_recipients()
    if not recipients or not has_m365_reporting_config():
        return {"status": "notification_not_sent"}
    return send_m365_reporting_mail(
        recipients,
        f"NuMat scheduled {report_type} report failed — {period_slug}",
        (
            f"The scheduled NuMat production {report_type} report for {period_slug} was not sent.\n\n"
            f"Reason: {reason}\n\n"
            "Please review the numat-ai service logs and rerun the report after the issue is corrected."
        ),
    )


def delivery_log_path():
    configured = os.getenv("M365_REPORTING_DELIVERY_LOG_PATH", "").strip()
    return Path(configured).expanduser() if configured else DEFAULT_DELIVERY_LOG_PATH


def load_delivery_log():
    path = delivery_log_path()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def save_delivery_log(payload):
    path = delivery_log_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def exact_period_payload(result, start, end):
    start_key = start.isoformat()
    end_key = end.isoformat()
    period_result = {
        **result,
        "production_rows": [
            item for item in result.get("production_rows", [])
            if start_key <= str(item.get("date") or "") <= end_key
        ],
        "operator_rows": [
            item for item in result.get("operator_rows", [])
            if start_key <= str(item.get("date") or "") <= end_key
        ],
    }
    return build_production_kpi_payload(period_result, days=0)


def build_report(report_type, result, start, end):
    period_text = start.strftime("%B %Y")
    period_slug = start.strftime("%Y-%m")
    if report_type == "anomaly":
        content, anomaly_count = build_anomaly_workbook(exact_period_payload(result, start, end))
        return {
            "content": content,
            "filename": f"numat-production-anomalies-{period_slug}.xlsx",
            "content_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "subject": f"NuMat production anomalies — {period_text}",
            "body": (
                f"Attached is the production data-quality and anomaly report for {period_text}.\n\n"
                f"It contains {anomaly_count} item(s) for review. Flagged values are not automatically assumed to be incorrect."
            ),
        }

    previous_start, previous_end = prior_month(start)
    facts = build_ai_report_facts_for_calendar_period(
        result,
        start.isoformat(),
        end.isoformat(),
        previous_start.isoformat(),
        previous_end.isoformat(),
    )
    analysis = generate_production_analysis_report(facts)
    if analysis.get("_analysis_status") != "ok":
        raise ReportGenerationError(
            f"AI analysis was unavailable ({analysis.get('_analysis_status') or 'unknown status'})"
        )
    return {
        "content": build_ai_analysis_docx(facts, analysis),
        "filename": f"numat-production-analysis-{period_slug}.docx",
        "content_type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "subject": f"NuMat production analysis — {period_text}",
        "body": (
            f"Attached is the AI-assisted production analysis for {period_text}, compared with "
            f"{previous_start.strftime('%B %Y')}.\n\n"
            "The report uses completed production days and the agreed NuMat production context."
        ),
    }


def deliver(report_type, result, start, end, dry_run=False, output_dir=None, force=False):
    key = f"{report_type}:{start.strftime('%Y-%m')}"
    log = load_delivery_log()
    if not dry_run and not force and (log.get(key) or {}).get("status") == "sent":
        return {"status": "already_sent", "key": key}

    try:
        report = build_report(report_type, result, start, end)
    except Exception as error:
        failure_status = f"generation_error: {error}"
        if not dry_run:
            notify_failure(report_type, start.strftime("%Y-%m"), failure_status)
            log[key] = {
                "status": "failed",
                "attempted_at": datetime.now().astimezone().isoformat(timespec="seconds"),
                "recipients": report_recipients(report_type),
                "filename": "",
                "error": failure_status,
            }
            save_delivery_log(log)
        return {"status": "generation_error", "key": key, "error": str(error)}
    recipients = report_recipients(report_type)
    if not recipients:
        return {"status": "missing_recipients", "key": key}

    if dry_run:
        destination = Path(output_dir or BASE_DIR.parent / "output" / "monthly-reports")
        destination.mkdir(parents=True, exist_ok=True)
        path = destination / report["filename"]
        path.write_bytes(report["content"])
        return {"status": "dry_run", "key": key, "path": str(path), "recipients": recipients}

    if not has_m365_reporting_config():
        return {"status": "m365_reporting_disabled", "key": key}
    send_result = send_m365_reporting_mail(
        recipients,
        report["subject"],
        report["body"],
        attachments=[{
            "name": report["filename"],
            "content_type": report["content_type"],
            "content": report["content"],
        }],
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
        notify_failure(
            report_type,
            start.strftime("%Y-%m"),
            str(send_result.get("error_message") or send_result.get("status") or "unknown send error"),
        )
    return {**send_result, "key": key, "recipients": recipients}


def main(argv=None):
    parser = argparse.ArgumentParser(description="Generate and send scheduled NuMat production reports.")
    parser.add_argument("report", choices=("anomaly", "analysis", "all"))
    parser.add_argument("--month", type=parse_month, help="Report month as YYYY-MM; defaults to the previous calendar month.")
    parser.add_argument("--dry-run", action="store_true", help="Generate files without authenticating or sending email.")
    parser.add_argument("--output-dir")
    parser.add_argument("--force", action="store_true", help="Send even if the delivery log already records success.")
    args = parser.parse_args(argv)

    start, end = previous_calendar_month()
    if args.month:
        start = args.month
        next_month = (start.replace(day=28) + timedelta(days=4)).replace(day=1)
        end = next_month - timedelta(days=1)

    result = fetch_production_analysis_data(force_refresh=True)
    if result.get("status") not in {"ok", "partial"}:
        print(json.dumps({"status": "production_data_error", "detail": result.get("warning") or result.get("status")}))
        return 1
    report_types = ("anomaly", "analysis") if args.report == "all" else (args.report,)
    exit_code = 0
    for report_type in report_types:
        outcome = deliver(
            report_type,
            result,
            start,
            end,
            dry_run=args.dry_run,
            output_dir=args.output_dir,
            force=args.force,
        )
        print(json.dumps(outcome, default=str))
        if outcome.get("status") not in {"ok", "dry_run", "already_sent"}:
            exit_code = 1
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
