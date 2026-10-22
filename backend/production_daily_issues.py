import argparse
from collections import defaultdict
from datetime import date, datetime, timedelta
from html import escape, unescape
import json
import os
from pathlib import Path
import re
from statistics import mean, median
import sys
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=BASE_DIR / ".env")

from ai import generate_daily_production_issue_selection
from app_settings import get_production_daily_issues_settings, normalize_email_recipients
from crm import fetch_crm_activities
from finance import fetch_aged_debt_summary
from m365 import has_m365_reporting_config, send_m365_reporting_mail
from monthly_reports import failure_recipients
from production import fetch_productivity_detail, fetch_production_analysis_data


DEFAULT_DELIVERY_LOG_PATH = BASE_DIR / "data" / "production_daily_issue_deliveries.json"
PLANT_TIMEZONE = ZoneInfo("America/Chicago")
SALES_PERSON_NAMES = {
    "barnold@numatsystems.com": "Ben Arnold",
    "pbinnington@numatsystems.com": "Peter Binnington",
}


def delivery_log_path():
    configured = os.getenv("PRODUCTION_DAILY_ISSUES_DELIVERY_LOG_PATH", "").strip()
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


def parse_day(value):
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date()
    except ValueError as error:
        raise argparse.ArgumentTypeError("Use YYYY-MM-DD, for example 2026-10-15") from error


def safe_mean(values):
    numeric = [float(value) for value in values if value is not None]
    return mean(numeric) if numeric else None


def percentage_change(current, baseline):
    if current is None or baseline in (None, 0):
        return None
    return (float(current) - float(baseline)) / float(baseline) * 100


def daily_plant_productivity(operator_rows):
    grouped = defaultdict(lambda: {"booked": 0.0, "clocked": 0.0})
    for item in operator_rows:
        day = str(item.get("date") or "")
        clocked = float(item.get("clocked_hours") or 0)
        if not day or clocked <= 0:
            continue
        grouped[day]["booked"] += float(item.get("booked_hours") or 0)
        grouped[day]["clocked"] += clocked
    return {
        day: values["booked"] / values["clocked"] * 100
        for day, values in grouped.items()
        if values["clocked"] > 0
    }


def add_candidate(candidates, candidate_id, priority, title, check, why, evidence, category):
    candidates.append({
        "id": candidate_id,
        "priority": round(float(priority), 1),
        "category": category,
        "title": title,
        "check": check,
        "why": why,
        "evidence": [str(item) for item in evidence if str(item or "").strip()],
    })


def crm_activity_day(item):
    value = str(item.get("date_created") or "").strip()
    return value[:10] if re.fullmatch(r"\d{4}-\d{2}-\d{2}.*", value) else ""


def crm_preview(item):
    text = re.sub(r"<[^>]+>", " ", str(item.get("body") or ""))
    text = re.sub(r"\s+", " ", unescape(text)).strip()
    return text[:320]


def build_sales_email_summary(crm_result, report_key):
    summary = {
        email: {"name": name, "total": 0, "sales_related": 0}
        for email, name in SALES_PERSON_NAMES.items()
    }
    for item in (crm_result or {}).get("activities", []):
        sender = str(item.get("sender_email") or "").strip().lower()
        if sender not in summary or crm_activity_day(item) != report_key:
            continue
        if str(item.get("direction") or "").strip().lower() != "outbound":
            continue
        activity_type = str(item.get("crm_type") or "").strip().lower()
        if activity_type not in {"email", "outlook", "mail"}:
            continue
        summary[sender]["total"] += 1
        if str(item.get("activity_category") or "").strip().lower() == "sales_outreach":
            summary[sender]["sales_related"] += 1
    return list(summary.values())


def add_commercial_candidates(candidates, report_key, crm_result=None, finance_result=None):
    activities = [
        item for item in (crm_result or {}).get("activities", [])
        if not item.get("exclude") and str(item.get("activity_category") or "").strip().lower() == "sales_outreach"
    ]
    report_date = datetime.strptime(report_key, "%Y-%m-%d").date()
    outbound_by_person_day = defaultdict(lambda: defaultdict(int))
    for item in activities:
        sender = str(item.get("sender_email") or "").strip().lower()
        if str(item.get("direction") or "").strip().lower() == "outbound" and crm_activity_day(item) and sender:
            outbound_by_person_day[sender][crm_activity_day(item)] += 1
    previous_weekdays = []
    cursor = report_date - timedelta(days=1)
    while len(previous_weekdays) < 10:
        if cursor.weekday() < 5:
            previous_weekdays.append(cursor.isoformat())
        cursor -= timedelta(days=1)
    if report_date.weekday() < 5:
        for sender, daily_counts in sorted(outbound_by_person_day.items()):
            historical_total = sum(daily_counts[day] for day in previous_weekdays)
            if historical_total <= 0:
                continue
            baseline = safe_mean([daily_counts[day] for day in previous_weekdays]) or 0
            current_outreach = daily_counts[report_key]
            materially_below_own_average = baseline >= 3 and current_outreach <= baseline * 0.5 and baseline - current_outreach >= 2
            below_daily_floor = current_outreach < 5
            if not (materially_below_own_average or below_daily_floor):
                continue
            name = SALES_PERSON_NAMES.get(sender, sender)
            sender_key = re.sub(r"[^a-z0-9]+", "-", sender).strip("-")
            add_candidate(
                candidates, f"crm-outreach-low-{sender_key}", 78 + max(0, 5 - current_outreach),
                f"Sales outreach is low for {name}",
                f"Check whether {name}'s customer visits or other sales follow-up were completed and recorded in CRM.",
                "Fewer than five outbound sales-related activities were recorded for the individual on this weekday.",
                [f"Today: {current_outreach} outbound sales activities", f"Previous 10-weekday average: {baseline:.1f}", f"CRM sender: {sender}"],
                "Sales",
            )

    concern_pattern = re.compile(r"\b(complaint|unhappy|cancel(?:led|lation)?|lost account|quality issue|disappointed|overdue account|payment overdue|no response from|not received|late delivery|price too high|switch(?:ing)? to (?:a )?competitor)\b", re.I)
    for index, item in enumerate(item for item in activities if crm_activity_day(item) == report_key):
        subject = str(item.get("subject") or "").strip()
        preview = crm_preview(item)
        if not concern_pattern.search(f"{subject} {preview}"):
            continue
        customer = str(item.get("customer_company") or item.get("customer_label") or "Customer").strip()
        add_candidate(
            candidates, f"crm-concern-{index}", 92,
            f"Review a potential sales concern for {customer}",
            "Read the full CRM thread and confirm whether management follow-up is needed.",
            "A sales-related CRM note contains language that may indicate a customer or commercial concern.",
            [f"Subject: {subject or 'No subject'}", f"Direction: {str(item.get('direction') or 'unknown').title()}", f"Note: {preview or 'No preview available'}"],
            "Sales",
        )

    finance = finance_result or {}
    over_sixty = float(finance.get("sixtyone_ninety") or 0) + float(finance.get("ninety_plus") or 0)
    if finance.get("status") == "ok" and over_sixty > 0:
        total = float(finance.get("total_outstanding") or 0)
        add_candidate(
            candidates, "aged-debt-over-sixty", 88 + min(over_sixty / 10000, 20),
            "Aged debt over 60 days is high",
            "Review the overdue balance and confirm the collection actions and account risks requiring attention.",
            "Customers are expected to pay within 30 days, so any balance aged beyond 60 days requires attention.",
            [f"61–90 days: ${float(finance.get('sixtyone_ninety') or 0):,.0f}", f"90+ days: ${float(finance.get('ninety_plus') or 0):,.0f}", f"Total over 60 days: ${over_sixty:,.0f}", f"Total outstanding: ${total:,.0f}"],
            "Finance",
        )


def build_daily_issue_facts(production_result, productivity_detail, report_day=None, lookback_days=20, production_context="", crm_result=None, finance_result=None):
    rows = sorted(production_result.get("production_rows", []), key=lambda item: str(item.get("date") or ""))
    if not rows:
        raise ValueError("No production records are available.")
    if report_day:
        report_key = report_day.isoformat() if isinstance(report_day, date) else str(report_day)
        current = next((item for item in rows if item.get("date") == report_key), None)
        if not current:
            raise ValueError(f"No production record is available for {report_key}.")
    else:
        current = rows[-1]
        report_key = current["date"]
    history = [item for item in rows if str(item.get("date") or "") < report_key][-max(5, int(lookback_days)):]
    candidates = []

    current_press = current.get("press_throughput")
    baseline_press_values = [item.get("press_throughput") for item in history if item.get("press_throughput") is not None]
    baseline_press = safe_mean(baseline_press_values)
    press_change = percentage_change(current_press, baseline_press)
    if press_change is not None and len(baseline_press_values) >= 5 and abs(press_change) >= 20:
        direction = "below" if press_change < 0 else "above"
        current_value = current.get("production_revenue_today")
        historical_value_pairs = [
            (float(item.get("press_throughput")), float(item.get("production_revenue_today")))
            for item in history
            if item.get("press_throughput") not in (None, 0) and item.get("production_revenue_today") is not None
        ]
        baseline_value = safe_mean([value for _, value in historical_value_pairs])
        current_value_per_lf = (
            float(current_value) / float(current_press)
            if current_value is not None and current_press not in (None, 0) else None
        )
        baseline_value_per_lf = (
            sum(value for _, value in historical_value_pairs) / sum(linear_feet for linear_feet, _ in historical_value_pairs)
            if historical_value_pairs and sum(linear_feet for linear_feet, _ in historical_value_pairs) else None
        )
        value_change = percentage_change(current_value, baseline_value)
        value_mix_explanation = (
            press_change < 0
            and value_change is not None
            and abs(value_change) <= 20
            and current_value_per_lf is not None
            and baseline_value_per_lf is not None
            and current_value_per_lf >= baseline_value_per_lf * 1.2
        )
        why = f"Total press output is {abs(press_change):.1f}% {direction} the recent completed-day average."
        check = "Check that the FF1, FF2 and FF3 completed-day linear-foot totals and cycle assignments are correct."
        evidence = [
            f"Today: {float(current_press or 0):,.0f} LF",
            f"Recent {len(baseline_press_values)}-day average: {baseline_press:,.0f} LF",
            f"FF1 {float(current.get('ff1_lf') or 0):,.0f} LF; FF2 {float(current.get('ff2_lf') or 0):,.0f} LF; FF3 {float(current.get('ff3_lf') or 0):,.0f} LF",
        ]
        if current_value is not None and baseline_value is not None:
            evidence.append(f"Repair value: ${float(current_value):,.0f}; recent average: ${baseline_value:,.0f}")
        if current_value_per_lf is not None and baseline_value_per_lf is not None:
            evidence.append(f"Value per linear foot: ${current_value_per_lf:,.2f}; recent average: ${baseline_value_per_lf:,.2f}")
        if value_mix_explanation:
            why += " Repair value remained close to normal because the mats processed were worth more per linear foot than the recent mix."
            check = "Check the lower physical throughput, while noting that a higher-value mat mix kept repair value close to normal."
        add_candidate(
            candidates,
            "press-total-variance",
            60 + abs(press_change),
            f"Total press linear feet {direction} recent norm",
            check,
            why,
            evidence,
            "Press",
        )

    for press in (1, 2, 3):
        key = f"ff{press}_lf"
        current_value = current.get(key)
        baseline_values = [item.get(key) for item in history if item.get(key) is not None]
        baseline_value = safe_mean(baseline_values)
        change = percentage_change(current_value, baseline_value)
        if change is None or len(baseline_values) < 5 or change > -30 or press_change is None or press_change > -15:
            continue
        add_candidate(
            candidates,
            f"ff{press}-low-output",
            55 + abs(change),
            f"FF{press} linear feet below recent norm",
            f"Check FF{press}'s cycles, mat allocation and recorded linear feet for the day.",
            f"FF{press} output is {abs(change):.1f}% below its recent completed-day average.",
            [f"Today: {float(current_value or 0):,.0f} LF", f"Recent {len(baseline_values)}-day average: {baseline_value:,.0f} LF"],
            "Press",
        )

    recook = float(current.get("recook_lf") or 0)
    recook_values = [float(item.get("recook_lf") or 0) for item in history]
    recook_baseline = safe_mean(recook_values)
    if recook >= 100 and (recook_baseline is None or recook > max(100, recook_baseline * 1.75)):
        add_candidate(
            candidates,
            "recook-high",
            95 + min(recook / 10, 50),
            "Re-cook activity above recent norm",
            "Check which mats required re-cooking and whether a repeatable quality issue needs attention.",
            "Re-cook work is materially above the recent completed-day average.",
            [f"Today: {recook:,.0f} LF", f"Recent {len(recook_values)}-day average: {float(recook_baseline or 0):,.0f} LF"],
            "Quality",
        )

    detail_summary = productivity_detail.get("summary", {})
    current_productivity = detail_summary.get("plant_productivity")
    historical_productivity = daily_plant_productivity(production_result.get("plant_operator_rows", []))
    baseline_productivity_values = [value for day, value in historical_productivity.items() if day < report_key][-lookback_days:]
    baseline_productivity = safe_mean(baseline_productivity_values)
    if current_productivity is not None and baseline_productivity is not None and len(baseline_productivity_values) >= 5:
        point_change = float(current_productivity) - baseline_productivity
        if point_change <= -12 or float(current_productivity) > 110:
            add_candidate(
                candidates,
                "plant-productivity-variance",
                75 + abs(point_change),
                "Plant productivity outside its recent range",
                "Check the day's time bookings and clockings, especially operators with unbooked or overbooked time.",
                f"Plant productivity is {abs(point_change):.1f} percentage points {'below' if point_change < 0 else 'above'} the recent average.",
                [f"Today: {float(current_productivity):.1f}%", f"Recent {len(baseline_productivity_values)}-day average: {baseline_productivity:.1f}%"],
                "Labour",
            )

    for item in productivity_detail.get("operator_rows", []):
        name = str(item.get("name") or "Unknown operator")
        clocked = float(item.get("clocked_hours") or 0)
        booked = float(item.get("booked_hours") or 0)
        if clocked <= 0 and booked > 0:
            add_candidate(
                candidates,
                f"booking-without-clocking-{item.get('operator_key') or name}",
                125 + booked,
                f"{name} has bookings without clocked time",
                f"Check {name}'s clocking and time-booking records before using the productivity figure.",
                "Booked productive time has no matching paid clocked time.",
                [f"Booked: {booked:.2f} hrs", "Clocked: 0.00 hrs"],
                "Time data",
            )
            continue
        difference = booked - clocked
        if difference > 0.25:
            add_candidate(
                candidates,
                f"overbooked-{item.get('operator_key') or name}",
                120 + difference * 10,
                f"{name} booked more time than clocked",
                f"Check {name}'s individual bookings for overlaps or an incorrect start or finish time.",
                "Booked time should not exceed paid clocked time for the day.",
                [f"Booked: {booked:.2f} hrs", f"Clocked: {clocked:.2f} hrs", f"Difference: +{difference:.2f} hrs"],
                "Time data",
            )
        elif clocked - booked > 1.5:
            gap = clocked - booked
            add_candidate(
                candidates,
                f"unbooked-{item.get('operator_key') or name}",
                85 + gap * 8,
                f"{name} has material unbooked paid time",
                f"Check where {name}'s {gap:.2f} hours were spent and whether a productive booking is missing.",
                "The clocked-versus-booked reconciliation shows a material gap.",
                [f"Clocked: {clocked:.2f} hrs", f"Booked: {booked:.2f} hrs", f"Unbooked: {gap:.2f} hrs"],
                "Time data",
            )

    dashboard_lf = float(current.get("press_throughput") or 0)
    report_cycles = [item for item in productivity_detail.get("cycles", []) if item.get("date") == report_key]
    cycle_lf = sum(float(item.get("linear_feet") or 0) for item in report_cycles)
    cycles_without_press = [
        item for item in report_cycles
        if str(item.get("machine") or "").strip().upper() not in {"FF1", "FF2", "FF3"}
    ]
    if cycles_without_press:
        count = len(cycles_without_press)
        operators = sorted({str(item.get("operator") or "").strip() for item in cycles_without_press if str(item.get("operator") or "").strip()})
        orders = sorted({str(item.get("order_reference") or "").strip() for item in cycles_without_press if str(item.get("order_reference") or "").strip()})
        add_candidate(
            candidates,
            "press-cycles-missing-ff",
            120 + count,
            f"There {'is' if count == 1 else 'are'} {count} cycle{'s' if count != 1 else ''} with no FF number recorded against {'it' if count == 1 else 'them'}",
            "Check the affected cycle records and add the correct FF1, FF2 or FF3 number.",
            "Cycles without an FF number cannot be reconciled reliably with the daily press totals.",
            [
                f"Affected cycles: {count}",
                f"Linear feet: {sum(float(item.get('linear_feet') or 0) for item in cycles_without_press):,.0f} LF",
                f"Operators: {', '.join(operators) or 'Not recorded'}",
                f"Orders: {', '.join(orders) or 'Not recorded'}",
            ],
            "Data reconciliation",
        )

    reconciliation_difference = cycle_lf - dashboard_lf
    if dashboard_lf > 0 and cycle_lf > 0 and not cycles_without_press and abs(reconciliation_difference) >= 150:
        add_candidate(
            candidates,
            "press-cycle-reconciliation",
            110 + abs(reconciliation_difference) / 100,
            "Dashboard and cycle linear feet do not reconcile",
            "Check whether all completed cycles are included once in the daily FF1, FF2 and FF3 totals.",
            "The cycle-level linear feet and completed-day dashboard total differ by at least 150 LF.",
            [f"Dashboard total: {dashboard_lf:,.0f} LF", f"Cycle-detail total: {cycle_lf:,.0f} LF", f"Difference: {reconciliation_difference:+,.0f} LF"],
            "Data reconciliation",
        )

    backlog = current.get("backlog_weeks")
    if backlog is not None and (float(backlog) < 1 or float(backlog) > 3):
        direction = "below" if float(backlog) < 1 else "above"
        add_candidate(
            candidates,
            "backlog-range",
            65 + abs(float(backlog) - 2) * 10,
            f"Backlog is {direction} the preferred operating range",
            "Check the current workload and agree whether production or sales follow-up is required.",
            "NuMat's preferred backlog is approximately two weeks.",
            [f"Current backlog: {float(backlog):.1f} weeks", "Preferred level: approximately 2.0 weeks"],
            "Workload",
        )

    add_commercial_candidates(candidates, report_key, crm_result=crm_result, finance_result=finance_result)

    candidates.sort(key=lambda item: (-item["priority"], item["title"]))
    return {
        "report_date": report_key,
        "generated_at": datetime.now(PLANT_TIMEZONE).isoformat(timespec="seconds"),
        "comparison_days": len(history),
        "production_context": str(production_context or "").strip(),
        "sales_email_summary": build_sales_email_summary(crm_result, report_key),
        "current_snapshot": {
            "press_linear_feet": current_press,
            "plant_productivity": current_productivity,
            "recook_linear_feet": recook,
            "backlog_weeks": backlog,
            "production_value": current.get("production_revenue_today"),
        },
        "candidates": candidates,
    }


def build_email_report(facts, selection):
    candidate_by_id = {str(item.get("id")): item for item in facts.get("candidates", [])}
    selected = []
    for item in selection.get("issues", []):
        candidate = candidate_by_id.get(str(item.get("id") or ""))
        if not candidate:
            continue
        selected.append(candidate)
    report_date = datetime.strptime(facts["report_date"], "%Y-%m-%d")
    display_date = report_date.strftime("%-d %B %Y")
    subject = f"NuMat management checks - {display_date}"
    email_summary = facts.get("sales_email_summary", [])
    email_summary_text = "\n".join(
        f"- {item['name']}: {int(item.get('total') or 0)} total emails sent; "
        f"{int(item.get('sales_related') or 0)} sales-related"
        for item in email_summary
    )
    email_summary_html = "".join(
        f"<tr><td style='padding:5px 18px 5px 0'>{escape(str(item['name']))}</td>"
        f"<td style='padding:5px 18px;text-align:right'>{int(item.get('total') or 0)}</td>"
        f"<td style='padding:5px 0 5px 18px;text-align:right'>{int(item.get('sales_related') or 0)}</td></tr>"
        for item in email_summary
    )
    summary_text_footer = (
        f"\n\nEmails sent today\n{email_summary_text}"
        if email_summary_text else ""
    )
    summary_html_footer = (
        "<div style='margin-top:24px;padding-top:16px;border-top:1px solid #dbe6f2'>"
        "<h2 style='margin:0 0 8px;font-size:16px;color:#17324d'>Emails sent today</h2>"
        "<table style='border-collapse:collapse;font-size:14px'>"
        "<thead><tr><th style='padding:5px 18px 5px 0;text-align:left'>Salesperson</th>"
        "<th style='padding:5px 18px;text-align:right'>Total</th>"
        "<th style='padding:5px 0 5px 18px;text-align:right'>Sales-related</th></tr></thead>"
        f"<tbody>{email_summary_html}</tbody></table></div>"
        if email_summary_html else ""
    )
    if selected:
        plain_sections = []
        html_sections = []
        for index, issue in enumerate(selected, start=1):
            evidence_text = "\n".join(f"   - {line}" for line in issue.get("evidence", []))
            plain_sections.append(f"{index}. {issue['title']}\nCheck: {issue['check']}\nWhy: {issue['why']}\nEvidence:\n{evidence_text}")
            evidence_html = "".join(f"<li>{escape(line)}</li>" for line in issue.get("evidence", []))
            html_sections.append(
                f"<div style='margin:0 0 20px;padding:16px;border:1px solid #dbe6f2;border-left:4px solid #245cff;border-radius:8px'>"
                f"<h2 style='margin:0 0 8px;font-size:18px;color:#17324d'>{index}. {escape(issue['title'])}</h2>"
                f"<p style='margin:0 0 6px'><strong>Check:</strong> {escape(issue['check'])}</p>"
                f"<p style='margin:0 0 8px'><strong>Why:</strong> {escape(issue['why'])}</p>"
                f"<ul style='margin:0;padding-left:20px'>{evidence_html}</ul></div>"
            )
        body_text = (
            f"NuMat management checks - {display_date}\n\n{selection.get('summary') or ''}\n\n"
            + "\n\n".join(plain_sections)
            + "\n\nThese are prompts for verification, not confirmed causes. Figures use the completed-day record and recent completed production days."
            + summary_text_footer
        )
        body_html = (
            "<div style='font-family:Arial,sans-serif;max-width:760px;color:#1f2933'>"
            f"<h1 style='color:#245cff;margin-bottom:4px'>Management checks</h1><p style='color:#5e7184;margin-top:0'>{escape(display_date)}</p>"
            f"<p>{escape(str(selection.get('summary') or ''))}</p>{''.join(html_sections)}"
            "<p style='font-size:12px;color:#5e7184'>These are prompts for verification, not confirmed causes. Figures use the completed-day record and recent completed production days.</p>"
            f"{summary_html_footer}</div>"
        )
    else:
        body_text = f"NuMat management checks - {display_date}\n\nNo material exceptions were identified in the available management data.{summary_text_footer}"
        body_html = (
            "<div style='font-family:Arial,sans-serif;max-width:760px;color:#1f2933'>"
            f"<h1 style='color:#245cff'>Management checks</h1><p>{escape(display_date)}</p>"
            "<p>No material exceptions were identified in the available management data.</p>"
            f"{summary_html_footer}</div>"
        )
    return {"subject": subject, "body_text": body_text, "body_html": body_html, "issues": selected}


def generate_report(requested_day=None, force_refresh=True):
    production_result = fetch_production_analysis_data(force_refresh=force_refresh, include_today=True)
    if production_result.get("status") not in {"ok", "partial"}:
        raise RuntimeError(production_result.get("warning") or production_result.get("status") or "Production data is unavailable.")
    rows = production_result.get("production_rows", [])
    if not rows:
        raise RuntimeError("No production records are available.")
    report_day = requested_day or datetime.now(PLANT_TIMEZONE).date()
    report_key = report_day.isoformat()
    if not any(item.get("date") == report_key for item in rows):
        latest = max(str(item.get("date") or "") for item in rows)
        raise RuntimeError(f"No completed production record is available for {report_key}; latest available day is {latest}.")
    detail = fetch_productivity_detail(
        days=1,
        force_refresh=force_refresh,
        period_start=report_key,
        period_end=report_key,
    )
    crm_result = fetch_crm_activities()
    finance_result = fetch_aged_debt_summary(force_refresh=force_refresh)
    settings = get_production_daily_issues_settings()
    facts = build_daily_issue_facts(
        production_result,
        detail,
        report_day=report_day,
        production_context=settings.get("context", ""),
        crm_result=crm_result,
        finance_result=finance_result,
    )
    selection = generate_daily_production_issue_selection(facts)
    return {"facts": facts, "selection": selection, **build_email_report(facts, selection)}


def notify_failure(report_day, reason):
    recipients = failure_recipients()
    if not recipients or not has_m365_reporting_config():
        return {"status": "notification_not_sent"}
    return send_m365_reporting_mail(
        recipients,
        f"NuMat daily management checks failed - {report_day}",
        f"The daily management checks email for {report_day} was not sent.\n\nReason: {reason}",
    )


def deliver(requested_day=None, dry_run=False, output_dir=None, force=False, recipients=None, force_refresh=True):
    report_day = requested_day or datetime.now(PLANT_TIMEZONE).date()
    key = f"production-daily-issues:{report_day.isoformat()}"
    log = load_delivery_log()
    existing_status = (log.get(key) or {}).get("status")
    if not dry_run and not force and existing_status in {"sent", "no_data"}:
        return {"status": "already_sent" if existing_status == "sent" else "no_data", "key": key}
    settings = get_production_daily_issues_settings()
    recipient_list = normalize_email_recipients(recipients if recipients is not None else settings["recipients"])
    if not recipient_list:
        return {"status": "missing_recipients", "key": key}
    try:
        report = generate_report(requested_day=report_day, force_refresh=force_refresh)
    except Exception as error:
        if str(error).startswith("No completed production record is available"):
            if not dry_run:
                log[key] = {
                    "status": "no_data",
                    "attempted_at": datetime.now(PLANT_TIMEZONE).isoformat(timespec="seconds"),
                    "recipients": recipient_list,
                    "error": str(error),
                }
                save_delivery_log(log)
            return {"status": "no_data", "key": key, "error": str(error)}
        if not dry_run:
            notify_failure(report_day.isoformat(), str(error))
        return {"status": "generation_error", "key": key, "error": str(error)}
    if dry_run:
        destination = Path(output_dir or BASE_DIR.parent / "output" / "production-daily-issues")
        destination.mkdir(parents=True, exist_ok=True)
        path = destination / f"production-checks-{report_day.isoformat()}.html"
        path.write_text(report["body_html"], encoding="utf-8")
        return {"status": "dry_run", "key": key, "path": str(path), "recipients": recipient_list, "report": report}
    if not has_m365_reporting_config():
        return {"status": "m365_reporting_disabled", "key": key}
    send_result = send_m365_reporting_mail(
        recipient_list,
        report["subject"],
        report["body_html"],
        body_content_type="HTML",
    )
    log[key] = {
        "status": "sent" if send_result.get("status") == "ok" else "failed",
        "attempted_at": datetime.now(PLANT_TIMEZONE).isoformat(timespec="seconds"),
        "recipients": recipient_list,
        "issue_ids": [item.get("id") for item in report.get("issues", [])],
        "error": str(send_result.get("error_message") or send_result.get("status") or ""),
    }
    save_delivery_log(log)
    if send_result.get("status") != "ok":
        notify_failure(report_day.isoformat(), log[key]["error"])
    return {**send_result, "key": key, "recipients": recipient_list, "issue_count": len(report.get("issues", []))}


def schedule_is_due(now=None, settings=None):
    settings = settings or get_production_daily_issues_settings()
    if not settings.get("enabled"):
        return False
    current = (now or datetime.now(PLANT_TIMEZONE)).astimezone(PLANT_TIMEZONE)
    scheduled_hour, scheduled_minute = (int(part) for part in settings["send_time"].split(":"))
    scheduled = current.replace(hour=scheduled_hour, minute=scheduled_minute, second=0, microsecond=0)
    return scheduled <= current < scheduled + timedelta(minutes=30)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Generate and send the NuMat daily management checks email.")
    parser.add_argument("--date", type=parse_day, help="Plant date as YYYY-MM-DD; defaults to today's America/Chicago date.")
    parser.add_argument("--recipient", action="append", dest="recipients", help="Override recipient; may be repeated.")
    parser.add_argument("--dry-run", action="store_true", help="Generate the email preview without sending it.")
    parser.add_argument("--output-dir")
    parser.add_argument("--force", action="store_true", help="Send even if this date is already recorded as sent.")
    parser.add_argument("--run-if-due", action="store_true", help="Exit without sending unless the enabled configured schedule is currently due.")
    args = parser.parse_args(argv)
    settings = get_production_daily_issues_settings()
    if args.run_if_due and not schedule_is_due(settings=settings):
        print(json.dumps({"status": "not_due", "enabled": settings.get("enabled"), "send_time": settings.get("send_time")}))
        return 0
    outcome = deliver(
        requested_day=args.date,
        dry_run=args.dry_run,
        output_dir=args.output_dir,
        force=args.force,
        recipients=args.recipients,
    )
    print(json.dumps({key: value for key, value in outcome.items() if key != "report"}, default=str))
    return 0 if outcome.get("status") in {"ok", "dry_run", "already_sent", "not_due", "no_data"} else 1


if __name__ == "__main__":
    sys.exit(main())
