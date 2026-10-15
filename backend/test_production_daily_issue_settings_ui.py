import unittest
from datetime import date
from unittest.mock import patch

import main


class ProductionDailyIssueSettingsUiTests(unittest.TestCase):
    def test_admin_settings_page_shows_distribution_schedule_and_controls(self):
        with (
            patch.object(main, "get_current_session_user", return_value={"username": "admin", "role": "admin"}),
            patch.object(main, "get_production_daily_issues_settings", return_value={
                "enabled": True,
                "recipients": ["manager@example.com", "team@example.com"],
                "send_time": "18:45",
                "timezone": "America/Chicago",
                "context": "Orders are normally shared between operators.",
            }),
        ):
            response = main.get_admin_settings_page()

        html = response.body.decode() if hasattr(response, "body") else str(response)
        self.assertIn("Daily management checks email", html)
        self.assertIn('id="production-daily-issues"', html)
        self.assertIn("manager@example.com", html)
        self.assertIn('value="18:45"', html)
        self.assertIn("Management AI context", html)
        self.assertIn("Orders are normally shared between operators.", html)
        self.assertIn("Test a report date", html)
        self.assertIn('name="report_date"', html)
        self.assertIn("Preview selected date", html)
        self.assertIn("Send selected date now", html)

    def test_admin_menu_links_directly_to_production_email_settings(self):
        with patch.object(main, "get_current_session_user", return_value={"username": "admin", "role": "admin"}):
            html = main.render_global_nav("Home")

        self.assertIn("Management Email Checks", html)
        self.assertIn('/admin-settings#production-daily-issues', html)

    def test_admin_can_save_daily_issue_email_settings(self):
        saved = {"enabled": True, "recipients": ["manager@example.com"], "send_time": "18:30", "context": "Normal plant context"}
        with (
            patch.object(main, "get_current_session_user", return_value={"username": "admin", "role": "admin"}),
            patch.object(main, "set_production_daily_issues_settings", return_value=saved) as setter,
            patch.object(main, "record_audit_event") as audit,
        ):
            response = main.post_admin_production_daily_issues(
                enabled="1",
                recipients="manager@example.com",
                send_time="18:30",
                context="Normal plant context",
            )

        self.assertEqual(response.status_code, 303)
        setter.assert_called_once_with(enabled=True, recipients="manager@example.com", send_time="18:30", context="Normal plant context")
        audit.assert_called_once()

    def test_preview_renders_generated_email_without_sending(self):
        report = {
            "subject": "NuMat management checks - 15 October 2026",
            "body_html": "<div>Evidence-backed preview</div>",
            "issues": [{"id": "one"}, {"id": "two"}],
            "selection": {"_analysis_status": "ok"},
        }
        with (
            patch.object(main, "get_current_session_user", return_value={"username": "admin", "role": "admin"}),
            patch.object(main, "generate_production_daily_issues_report", return_value=report) as generate,
        ):
            response = main.get_admin_production_daily_issues_preview(report_date="2026-10-08")

        html = response.body.decode() if hasattr(response, "body") else str(response)
        self.assertIn("Evidence-backed preview", html)
        self.assertIn("2 check(s) selected", html)
        self.assertIn("AI status: ok", html)
        generate.assert_called_once_with(
            requested_day=date(2026, 10, 8),
            force_refresh=True,
        )

    def test_manual_send_uses_selected_historical_date(self):
        with (
            patch.object(main, "get_current_session_user", return_value={"username": "admin", "role": "admin"}),
            patch.object(main, "has_m365_reporting_config", return_value=True),
            patch.object(main, "deliver_production_daily_issues", return_value={"status": "ok", "recipients": ["manager@example.com"]}) as deliver,
            patch.object(main, "record_audit_event"),
        ):
            response = main.post_admin_production_daily_issues_test(report_date="2026-10-08")

        self.assertEqual(response.status_code, 303)
        deliver.assert_called_once_with(requested_day=date(2026, 10, 8), force=True)
        self.assertIn("10/08/2026", response.headers["location"])

    def test_manual_send_uses_connected_admin_mailbox_when_reporting_mailbox_is_disabled(self):
        report = {
            "subject": "NuMat management checks - 8 October 2026",
            "body_html": "<div>Daily checks</div>",
        }
        with (
            patch.object(main, "get_current_session_user", return_value={"username": "admin", "role": "admin"}),
            patch.object(main, "has_m365_reporting_config", return_value=False),
            patch.object(main, "get_production_daily_issues_settings", return_value={"recipients": ["manager@example.com"]}),
            patch.object(main, "ensure_valid_access_token", return_value={"status": "ok", "access_token": "token"}),
            patch.object(main, "generate_production_daily_issues_report", return_value=report),
            patch.object(main, "send_m365_mail", return_value={"status": "ok"}) as send,
            patch.object(main, "record_audit_event"),
        ):
            response = main.post_admin_production_daily_issues_test(report_date="2026-10-08")

        self.assertEqual(response.status_code, 303)
        send.assert_called_once_with(
            "token",
            "manager@example.com",
            report["subject"],
            report["body_html"],
            body_content_type="HTML",
        )
        self.assertIn("sent", response.headers["location"])


if __name__ == "__main__":
    unittest.main()
