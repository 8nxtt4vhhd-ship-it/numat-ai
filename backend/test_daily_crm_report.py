from datetime import date, datetime
import unittest
from zoneinfo import ZoneInfo

import daily_crm_report


class DailyCrmReportTests(unittest.TestCase):
    def test_filters_requested_date_and_internal_activity(self):
        result = {"activities": [
            {"date_created": "2026-11-09 08:00:00", "subject": "Sales call", "exclude": False},
            {"date_created": "2026-11-09 09:00:00", "subject": "Internal", "exclude": True},
            {"date_created": "2026-11-08 16:00:00", "subject": "Yesterday", "exclude": False},
        ]}
        rows = daily_crm_report.activities_for_day(result, date(2026, 11, 9))
        self.assertEqual([item["subject"] for item in rows], ["Sales call"])

    def test_filters_payroll_and_resolves_external_customer(self):
        result = {"activities": [
            {"date_created": "2026-11-09 08:00:00", "subject": "Payroll review", "exclude": False},
            {"date_created": "2026-11-09 09:00:00", "subject": "Repair mats", "exclude": False, "customer_company": "NuMat Samples", "recipient_company": "Clemens Uniform Rental"},
        ]}
        rows = daily_crm_report.activities_for_day(result, date(2026, 11, 9))
        self.assertEqual(len(rows), 1)
        self.assertEqual(daily_crm_report.resolve_customer(rows[0]), "Clemens Uniform Rental")

    def test_email_has_management_sections_without_counts(self):
        report = daily_crm_report.build_email(date(2026, 11, 9), {
            "overall_summary": "A useful day of customer contact.",
            "opportunities": [{"customer": "Example Co", "summary": "An appointment was arranged."}],
            "follow_ups": [], "concerns": [], "meaningful_updates": [],
        })
        self.assertIn("Opportunities and positive signals", report["body_html"])
        self.assertIn("Example Co", report["body_html"])
        self.assertNotIn("activity count", report["body_html"].lower())

    def test_schedule_excludes_weekends(self):
        settings = {"enabled": True, "send_time": "18:00"}
        timezone = ZoneInfo("America/Chicago")
        self.assertTrue(daily_crm_report.schedule_is_due(datetime(2026, 11, 9, 18, 5, tzinfo=timezone), settings))
        self.assertFalse(daily_crm_report.schedule_is_due(datetime(2026, 11, 8, 18, 5, tzinfo=timezone), settings))


if __name__ == "__main__":
    unittest.main()
