from datetime import date, datetime
import unittest
from unittest.mock import patch
from zoneinfo import ZoneInfo

import daily_crm_report


class DailyCrmReportTests(unittest.TestCase):
    def test_successful_refresh_uses_combined_crm_result_like_management_checks(self):
        refreshed = {"status": "ok", "activities": []}
        combined = {
            "status": "ok",
            "activities": [{"subject": "Customer visit", "crm_type": "Meeting"}],
        }
        with patch.object(
            daily_crm_report,
            "sync_filemaker_crm_recent_cache",
            return_value=refreshed,
        ), patch.object(
            daily_crm_report,
            "fetch_crm_activities",
            return_value=combined,
        ) as fetch:
            result = daily_crm_report.fetch_fresh_crm_activities()

        self.assertIs(result, combined)
        fetch.assert_called_once_with()

    def test_failed_fresh_refresh_marks_fallback_as_unsafe_for_delivery(self):
        with patch.object(
            daily_crm_report,
            "sync_filemaker_crm_recent_cache",
            return_value={"status": "filemaker_error", "error_message": "Status 500"},
        ), patch.object(
            daily_crm_report,
            "fetch_crm_activities",
            return_value={"status": "ok", "activities": [{"subject": "Old cached activity"}]},
        ):
            result = daily_crm_report.fetch_fresh_crm_activities()

        self.assertTrue(result["fresh_refresh_failed"])
        self.assertEqual(result["fresh_refresh_status"], "filemaker_error")
        self.assertEqual(result["activities"][0]["subject"], "Old cached activity")

    def test_report_is_not_generated_from_stale_cache_after_refresh_failure(self):
        stale_result = {
            "status": "ok",
            "fresh_refresh_failed": True,
            "activities": [],
        }
        with patch.object(daily_crm_report, "fetch_fresh_crm_activities", return_value=stale_result), patch.object(
            daily_crm_report, "generate_analysis"
        ) as analysis:
            with self.assertRaisesRegex(RuntimeError, "not sent from stale data"):
                daily_crm_report.generate_report(date(2026, 11, 17))

        analysis.assert_not_called()

    def test_filters_requested_date_and_internal_activity(self):
        result = {"activities": [
            {"date_created": "2026-11-09 08:00:00", "subject": "Sales call", "exclude": False, "customer_company": "Example Customer"},
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

    def test_excludes_activity_without_an_identified_customer(self):
        result = {"activities": [
            {
                "date_created": "2026-11-09 08:00:00",
                "subject": "Secure IP address registration",
                "body": "Assisted the accountant with the Child Support Portal.",
                "sender_company": "NuMat Samples",
                "exclude": False,
            },
            {
                "date_created": "2026-11-09 09:00:00",
                "subject": "Repair requirement",
                "customer_company": "Clemens Uniform Rental",
                "exclude": False,
            },
        ]}

        rows = daily_crm_report.activities_for_day(result, date(2026, 11, 9))

        self.assertEqual([item["subject"] for item in rows], ["Repair requirement"])

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
