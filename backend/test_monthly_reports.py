import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

import monthly_reports
import m365


class FakeResponse:
    def __init__(self, status_code, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self):
        if self._payload is None:
            raise ValueError("No JSON")
        return self._payload


class MonthlyReportTests(unittest.TestCase):
    def test_previous_calendar_month_and_prior_month(self):
        start, end = monthly_reports.previous_calendar_month(monthly_reports.date(2026, 8, 18))
        self.assertEqual((start.isoformat(), end.isoformat()), ("2026-07-01", "2026-07-31"))
        previous_start, previous_end = monthly_reports.prior_month(start)
        self.assertEqual((previous_start.isoformat(), previous_end.isoformat()), ("2026-06-01", "2026-06-30"))

    def test_recipient_groups_use_active_users_with_email(self):
        users = [
            {"username": "kelly", "m365_email": "kelly@example.com", "active": True},
            {"username": "trudy", "m365_email": "trudy@example.com", "active": True},
            {"username": "lois", "m365_email": "lois@example.com", "active": False},
            {"username": "ben", "m365_email": "ben@example.com", "active": True},
            {"username": "noemail", "active": True},
        ]
        with patch.object(monthly_reports, "load_users", return_value=users):
            self.assertEqual(
                monthly_reports.report_recipients("anomaly"),
                ["kelly@example.com", "trudy@example.com"],
            )
            self.assertEqual(
                monthly_reports.report_recipients("analysis"),
                ["kelly@example.com", "trudy@example.com", "ben@example.com"],
            )

    def test_dry_run_writes_valid_anomaly_workbook_without_sending(self):
        result = {
            "status": "ok",
            "production_rows": [{"date": "2026-07-10", "record_id": "1", "time_booked_today": 250}],
            "operator_rows": [],
        }
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(monthly_reports, "report_recipients", return_value=["kelly@example.com"]):
                with patch.object(monthly_reports, "send_m365_reporting_mail") as send:
                    outcome = monthly_reports.deliver(
                        "anomaly",
                        result,
                        monthly_reports.date(2026, 7, 1),
                        monthly_reports.date(2026, 7, 31),
                        dry_run=True,
                        output_dir=directory,
                    )
            self.assertEqual(outcome["status"], "dry_run")
            self.assertTrue(Path(outcome["path"]).read_bytes().startswith(b"PK"))
            send.assert_not_called()

    def test_analysis_generation_failure_is_not_sent(self):
        result = {"status": "ok", "production_rows": [], "operator_rows": []}
        with patch.object(monthly_reports, "report_recipients", return_value=["kelly@example.com"]):
            with patch.object(monthly_reports, "generate_production_analysis_report", return_value={"_analysis_status": "error"}):
                with patch.object(monthly_reports, "send_m365_reporting_mail") as send:
                    outcome = monthly_reports.deliver(
                        "analysis",
                        result,
                        monthly_reports.date(2026, 7, 1),
                        monthly_reports.date(2026, 7, 31),
                        dry_run=True,
                    )
        self.assertEqual(outcome["status"], "generation_error")
        send.assert_not_called()


class ReportingMailTests(unittest.TestCase):
    def test_app_only_sender_uses_configured_mailbox_and_attachment(self):
        environment = {
            "M365_REPORTING_ENABLED": "true",
            "M365_REPORTING_TENANT_ID": "tenant-id",
            "M365_REPORTING_CLIENT_ID": "client-id",
            "M365_REPORTING_CLIENT_SECRET": "secret-value",
            "M365_REPORTING_SENDER_EMAIL": "apps@numatsystems.com",
        }
        responses = [
            FakeResponse(200, {"access_token": "test-token"}),
            FakeResponse(202, None),
        ]
        with patch.dict(os.environ, environment, clear=False):
            with patch.object(m365.requests, "post", side_effect=responses) as request:
                result = m365.send_m365_reporting_mail(
                    ["kelly@example.com"],
                    "Report",
                    "Attached",
                    attachments=[{"name": "report.txt", "content_type": "text/plain", "content": b"hello"}],
                )
        self.assertEqual(result["status"], "ok")
        self.assertIn("/users/apps@numatsystems.com/sendMail", request.call_args_list[1].args[0])
        payload = request.call_args_list[1].kwargs["json"]
        self.assertEqual(payload["message"]["attachments"][0]["contentBytes"], "aGVsbG8=")
        self.assertEqual(payload["message"]["toRecipients"][0]["emailAddress"]["address"], "kelly@example.com")


if __name__ == "__main__":
    unittest.main()
