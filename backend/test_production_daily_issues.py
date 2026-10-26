from datetime import datetime
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import os
from zoneinfo import ZoneInfo

import production_daily_issues as issues


def sample_results():
    production_rows = []
    plant_operator_rows = []
    for day in range(8, 15):
        date_key = f"2026-10-{day:02d}"
        production_rows.append({
            "date": date_key,
            "press_throughput": 4000,
            "ff1_lf": 1400,
            "ff2_lf": 1400,
            "ff3_lf": 1200,
            "recook_lf": 20,
            "backlog_weeks": 1.8,
            "production_revenue_today": 7000,
        })
        plant_operator_rows.append({"date": date_key, "name": "Operator", "booked_hours": 7, "clocked_hours": 8})
    production_rows.append({
        "date": "2026-10-15",
        "press_throughput": 2000,
        "ff1_lf": 1000,
        "ff2_lf": 500,
        "ff3_lf": 500,
        "recook_lf": 300,
        "backlog_weeks": 0.8,
        "production_revenue_today": 4000,
    })
    detail = {
        "summary": {"plant_productivity": 60},
        "operator_rows": [
            {"name": "Zach Holmes", "operator_key": "zachholmes", "booked_hours": 9.5, "clocked_hours": 8},
            {"name": "James Frieson", "operator_key": "jamesfrieson", "booked_hours": 5.4, "clocked_hours": 8},
        ],
        "cycles": [
            {"date": "2026-10-15", "order_reference": "3736", "operator": "Kenley Pike", "machine": "FF1", "linear_feet": 100},
            {"date": "2026-10-15", "order_reference": "3736", "operator": "Cedrick Brown", "machine": "FF2", "linear_feet": 120},
        ],
    }
    return {"production_rows": production_rows, "plant_operator_rows": plant_operator_rows}, detail


class ProductionDailyIssueTests(unittest.TestCase):
    def test_calculated_candidates_include_time_press_and_quality_checks_but_not_shared_orders(self):
        production_result, detail = sample_results()
        facts = issues.build_daily_issue_facts(production_result, detail, report_day=issues.date(2026, 10, 15))
        ids = {item["id"] for item in facts["candidates"]}

        self.assertIn("press-total-variance", ids)
        self.assertIn("recook-high", ids)
        self.assertIn("plant-productivity-variance", ids)
        self.assertIn("overbooked-zachholmes", ids)
        self.assertIn("unbooked-jamesfrieson", ids)
        self.assertNotIn("shared-order-3736", ids)
        self.assertIn("press-cycle-reconciliation", ids)
        self.assertIn("backlog-range", ids)

    def test_email_keeps_calculated_wording_and_evidence_after_ai_selection(self):
        production_result, detail = sample_results()
        facts = issues.build_daily_issue_facts(production_result, detail, report_day=issues.date(2026, 10, 15))
        report = issues.build_email_report(facts, {
            "summary": "Three checks need attention.",
            "issues": [{"id": "overbooked-zachholmes", "check": "Review Zach's bookings.", "reason": "The daily reconciliation is overbooked."}],
        })

        self.assertIn("15 October 2026", report["subject"])
        self.assertIn("Check Zach Holmes's individual bookings", report["body_text"])
        self.assertNotIn("Review Zach's bookings.", report["body_text"])
        self.assertIn("Booked: 9.50 hrs", report["body_text"])
        self.assertIn("Difference: +1.50 hrs", report["body_text"])
        self.assertIn("prompts for verification", report["body_html"])

    def test_email_footer_counts_total_and_sales_related_emails_for_peter_and_ben(self):
        production_result, detail = sample_results()
        activities = [
            {
                "date_created": "2026-10-15 09:00:00", "direction": "outbound",
                "crm_type": "Outlook", "activity_category": "sales_outreach",
                "sender_email": "pbinnington@numatsystems.com",
            },
            {
                "date_created": "2026-10-15 10:00:00", "direction": "outbound",
                "crm_type": "Email", "activity_category": "customer_services",
                "sender_email": "pbinnington@numatsystems.com",
            },
            {
                "date_created": "2026-10-15 11:00:00", "direction": "outbound",
                "crm_type": "Outlook", "activity_category": "sales_outreach",
                "sender_email": "barnold@numatsystems.com",
            },
            {
                "date_created": "2026-10-15 12:00:00", "direction": "outbound",
                "crm_type": "Meeting", "activity_category": "sales_outreach",
                "sender_email": "barnold@numatsystems.com",
            },
            {
                "date_created": "2026-10-14 15:00:00", "direction": "outbound",
                "crm_type": "Outlook", "activity_category": "sales_outreach",
                "sender_email": "barnold@numatsystems.com",
            },
        ]
        facts = issues.build_daily_issue_facts(
            production_result, detail, report_day=issues.date(2026, 10, 15),
            crm_result={"activities": activities},
        )
        report = issues.build_email_report(facts, {"summary": "", "issues": []})

        self.assertEqual(facts["sales_email_summary"], [
            {"name": "Ben Arnold", "total": 1, "sales_related": 1},
            {"name": "Peter Binnington", "total": 2, "sales_related": 1},
        ])
        self.assertIn("Ben Arnold: 1 total emails sent; 1 sales-related", report["body_text"])
        self.assertIn("Peter Binnington: 2 total emails sent; 1 sales-related", report["body_text"])
        self.assertIn("Emails sent today", report["body_html"])
        self.assertIn("Sales-related", report["body_html"])

    def test_single_press_low_output_is_not_flagged_when_total_output_is_normal(self):
        production_result, detail = sample_results()
        current = production_result["production_rows"][-1]
        current.update({"press_throughput": 4000, "ff1_lf": 400, "ff2_lf": 1800, "ff3_lf": 1800})
        facts = issues.build_daily_issue_facts(production_result, detail, report_day=issues.date(2026, 10, 15))
        ids = {item["id"] for item in facts["candidates"]}

        self.assertNotIn("ff1-low-output", ids)
        self.assertNotIn("press-total-variance", ids)

    def test_low_throughput_explains_when_high_value_mix_offsets_the_shortfall(self):
        production_result, detail = sample_results()
        production_result["production_rows"][-1]["production_revenue_today"] = 7000
        facts = issues.build_daily_issue_facts(production_result, detail, report_day=issues.date(2026, 10, 15))
        candidate = next(item for item in facts["candidates"] if item["id"] == "press-total-variance")

        self.assertIn("higher-value mat mix", candidate["check"])
        self.assertIn("worth more per linear foot", candidate["why"])
        self.assertTrue(any("Value per linear foot" in line for line in candidate["evidence"]))

    def test_unbooked_time_is_only_flagged_when_daily_gap_exceeds_one_and_a_half_hours(self):
        production_result, detail = sample_results()
        james = detail["operator_rows"][1]
        james["clocked_hours"] = 6.9
        facts = issues.build_daily_issue_facts(production_result, detail, report_day=issues.date(2026, 10, 15))
        ids = {item["id"] for item in facts["candidates"]}
        self.assertNotIn("unbooked-jamesfrieson", ids)

        james["clocked_hours"] = 6.91
        facts = issues.build_daily_issue_facts(production_result, detail, report_day=issues.date(2026, 10, 15))
        ids = {item["id"] for item in facts["candidates"]}
        self.assertIn("unbooked-jamesfrieson", ids)

    def test_cycles_without_an_ff_number_replace_generic_reconciliation_warning(self):
        production_result, detail = sample_results()
        detail["cycles"] = [
            {"date": "2026-10-15", "operator": "Xavier Slaughter", "order_reference": "3744", "machine": "", "linear_feet": 220},
            {"date": "2026-10-15", "operator": "Xavier Slaughter", "order_reference": "3744", "machine": "", "linear_feet": 180},
        ]
        facts = issues.build_daily_issue_facts(production_result, detail, report_day=issues.date(2026, 10, 15))
        candidates = {item["id"]: item for item in facts["candidates"]}

        self.assertIn("press-cycles-missing-ff", candidates)
        self.assertNotIn("press-cycle-reconciliation", candidates)
        self.assertEqual(candidates["press-cycles-missing-ff"]["title"], "There are 2 cycles with no FF number recorded against them")
        self.assertIn("Linear feet: 400 LF", candidates["press-cycles-missing-ff"]["evidence"])

    def test_press_reconciliation_variance_below_150_lf_is_ignored(self):
        production_result, detail = sample_results()
        production_result["production_rows"][-1]["press_throughput"] = 2000
        detail["cycles"] = [
            {"date": "2026-10-15", "machine": "FF1", "linear_feet": 2149},
        ]
        facts = issues.build_daily_issue_facts(production_result, detail, report_day=issues.date(2026, 10, 15))
        ids = {item["id"] for item in facts["candidates"]}
        self.assertNotIn("press-cycle-reconciliation", ids)

        detail["cycles"][0]["linear_feet"] = 2150
        facts = issues.build_daily_issue_facts(production_result, detail, report_day=issues.date(2026, 10, 15))
        ids = {item["id"] for item in facts["candidates"]}
        self.assertIn("press-cycle-reconciliation", ids)

    def test_commercial_candidates_include_low_sales_outreach_concern_and_aged_debt(self):
        production_result, detail = sample_results()
        activities = []
        for day in range(1, 15):
            if issues.date(2026, 10, day).weekday() < 5:
                activities.extend({
                    "date_created": f"2026-10-{day:02d} 09:00:00",
                    "direction": "outbound",
                    "activity_category": "sales_outreach",
                    "sender_email": "seller@example.com",
                    "subject": "Customer follow-up",
                    "body": "Routine sales contact",
                } for _ in range(4))
        activities.append({
            "date_created": "2026-10-15 11:00:00",
            "direction": "inbound",
            "activity_category": "sales_outreach",
            "customer_company": "Example Customer",
            "subject": "Concern about pricing",
            "body": "We are disappointed and may cancel.",
        })
        activities.append({
            "date_created": "2026-10-15 12:00:00",
            "direction": "outbound",
            "activity_category": "customer_services",
            "subject": "Problem with pickup",
            "body": "Operational note that must be excluded.",
        })
        finance = {"status": "ok", "sixtyone_ninety": 18000, "ninety_plus": 12000, "total_outstanding": 90000}

        facts = issues.build_daily_issue_facts(
            production_result, detail, report_day=issues.date(2026, 10, 15),
            crm_result={"activities": activities}, finance_result=finance,
        )
        ids = {item["id"] for item in facts["candidates"]}
        self.assertIn("crm-outreach-low-seller-example-com", ids)
        self.assertTrue(any(item.startswith("crm-concern-") for item in ids))
        self.assertIn("aged-debt-over-sixty", ids)
        self.assertEqual(len([item for item in ids if item.startswith("crm-concern-")]), 1)

    def test_any_aged_debt_over_sixty_days_is_reported(self):
        production_result, detail = sample_results()
        facts = issues.build_daily_issue_facts(
            production_result, detail, report_day=issues.date(2026, 10, 15),
            finance_result={"status": "ok", "sixtyone_ninety": 1, "ninety_plus": 0, "total_outstanding": 100},
        )
        candidates = {item["id"]: item for item in facts["candidates"]}

        self.assertIn("aged-debt-over-sixty", candidates)
        self.assertIn("pay within 30 days", candidates["aged-debt-over-sixty"]["why"])

    def test_management_selection_keeps_up_to_two_checks_per_area(self):
        candidates = [
            {"id": "p1", "priority": 120, "category": "Time data", "check": "p1", "why": "p1"},
            {"id": "p2", "priority": 110, "category": "Workload", "check": "p2", "why": "p2"},
            {"id": "p3", "priority": 100, "category": "Quality", "check": "p3", "why": "p3"},
            {"id": "s1", "priority": 80, "category": "Sales", "check": "s1", "why": "s1"},
            {"id": "f1", "priority": 70, "category": "Finance", "check": "f1", "why": "f1"},
        ]
        with patch.dict(os.environ, {"OPENAI_API_KEY": ""}):
            result = issues.generate_daily_production_issue_selection({"candidates": candidates})

        self.assertEqual([item["id"] for item in result["issues"]], ["p1", "p2", "s1", "f1"])

    def test_routine_link_problem_is_not_a_crm_concern(self):
        candidates = []
        issues.add_commercial_candidates(candidates, "2026-10-08", crm_result={"activities": [{
            "date_created": "2026-10-08 09:00:00",
            "direction": "outbound",
            "activity_category": "sales_outreach",
            "subject": "Convention registration",
            "body": "Is there a problem with the registration link?",
        }]})

        self.assertFalse(any(item["id"].startswith("crm-concern-") for item in candidates))

    def test_sales_outreach_is_checked_per_individual(self):
        activities = []
        for day in (1, 2, 5, 6, 7, 8, 9, 12, 13):
            for sender in ("barnold@numatsystems.com", "pbinnington@numatsystems.com"):
                activities.extend({
                    "date_created": f"2026-10-{day:02d} 09:00:00",
                    "direction": "outbound",
                    "activity_category": "sales_outreach",
                    "sender_email": sender,
                } for _ in range(6))
        activities.extend({
            "date_created": "2026-10-14 09:00:00",
            "direction": "outbound",
            "activity_category": "sales_outreach",
            "sender_email": "barnold@numatsystems.com",
        } for _ in range(4))
        activities.extend({
            "date_created": "2026-10-14 09:00:00",
            "direction": "outbound",
            "activity_category": "sales_outreach",
            "sender_email": "pbinnington@numatsystems.com",
        } for _ in range(37))

        candidates = []
        issues.add_commercial_candidates(candidates, "2026-10-14", crm_result={"activities": activities})
        ids = {item["id"] for item in candidates}

        self.assertIn("crm-outreach-low-barnold-numatsystems-com", ids)
        self.assertNotIn("crm-outreach-low-pbinnington-numatsystems-com", ids)

    def test_dry_run_writes_html_without_sending(self):
        report = {
            "subject": "Production checks",
            "body_text": "Check this",
            "body_html": "<p>Check this</p>",
            "issues": [{"id": "one"}],
            "facts": {},
            "selection": {},
        }
        with tempfile.TemporaryDirectory() as directory:
            with (
                patch.object(issues, "get_production_daily_issues_settings", return_value={"recipients": ["manager@example.com"]}),
                patch.object(issues, "generate_report", return_value=report),
                patch.object(issues, "send_m365_reporting_mail") as send,
            ):
                result = issues.deliver(
                    requested_day=issues.date(2026, 10, 15),
                    dry_run=True,
                    output_dir=directory,
                )

            self.assertEqual(result["status"], "dry_run")
            self.assertIn("Check this", Path(result["path"]).read_text(encoding="utf-8"))
            send.assert_not_called()

    def test_schedule_runs_only_in_configured_half_hour_window(self):
        settings = {"enabled": True, "send_time": "18:30"}
        timezone = ZoneInfo("America/Chicago")
        self.assertTrue(issues.schedule_is_due(datetime(2026, 10, 15, 18, 30, tzinfo=timezone), settings))
        self.assertTrue(issues.schedule_is_due(datetime(2026, 10, 15, 18, 59, tzinfo=timezone), settings))
        self.assertFalse(issues.schedule_is_due(datetime(2026, 10, 15, 19, 0, tzinfo=timezone), settings))

    def test_schedule_skips_saturdays_and_sundays(self):
        settings = {"enabled": True, "send_time": "18:30"}
        timezone = ZoneInfo("America/Chicago")

        self.assertFalse(issues.schedule_is_due(datetime(2026, 10, 24, 18, 30, tzinfo=timezone), settings))
        self.assertFalse(issues.schedule_is_due(datetime(2026, 10, 25, 18, 30, tzinfo=timezone), settings))
        self.assertTrue(issues.schedule_is_due(datetime(2026, 10, 26, 18, 30, tzinfo=timezone), settings))
        self.assertFalse(issues.schedule_is_due(datetime(2026, 10, 15, 18, 45, tzinfo=timezone), {"enabled": False, "send_time": "18:30"}))

    def test_delivery_sends_html_to_configured_distribution_list(self):
        report = {
            "subject": "Production checks",
            "body_text": "Check this",
            "body_html": "<p>Check this</p>",
            "issues": [{"id": "one"}],
            "facts": {},
            "selection": {},
        }
        with tempfile.TemporaryDirectory() as directory:
            log_path = Path(directory) / "deliveries.json"
            with (
                patch.object(issues, "delivery_log_path", return_value=log_path),
                patch.object(issues, "get_production_daily_issues_settings", return_value={"recipients": ["manager@example.com", "team@example.com"]}),
                patch.object(issues, "generate_report", return_value=report),
                patch.object(issues, "has_m365_reporting_config", return_value=True),
                patch.object(issues, "send_m365_reporting_mail", return_value={"status": "ok"}) as send,
            ):
                result = issues.deliver(requested_day=issues.date(2026, 10, 15))

        self.assertEqual(result["status"], "ok")
        send.assert_called_once_with(
            ["manager@example.com", "team@example.com"],
            "Production checks",
            "<p>Check this</p>",
            body_content_type="HTML",
        )

    def test_no_production_day_is_logged_once_without_failure_notification(self):
        with tempfile.TemporaryDirectory() as directory:
            log_path = Path(directory) / "deliveries.json"
            with (
                patch.object(issues, "delivery_log_path", return_value=log_path),
                patch.object(issues, "get_production_daily_issues_settings", return_value={"recipients": ["manager@example.com"]}),
                patch.object(issues, "generate_report", side_effect=RuntimeError("No completed production record is available for 2026-10-18; latest available day is 2026-10-16.")),
                patch.object(issues, "notify_failure") as notify,
            ):
                first = issues.deliver(requested_day=issues.date(2026, 10, 18))
                second = issues.deliver(requested_day=issues.date(2026, 10, 18))

        self.assertEqual(first["status"], "no_data")
        self.assertEqual(second["status"], "no_data")
        notify.assert_not_called()


if __name__ == "__main__":
    unittest.main()
