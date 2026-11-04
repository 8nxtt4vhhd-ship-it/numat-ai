from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import management_checks


class ManagementChecksTests(unittest.TestCase):
    def test_report_issue_and_comment_round_trip(self):
        report = {
            "subject": "NuMat production management checks - 4 November 2026",
            "facts": {"report_date": "2026-11-04", "generated_at": "2026-11-04T18:30:00-06:00"},
            "selection": {"summary": "One production check."},
            "issues": [{
                "id": "press-total-variance", "title": "Press output is low",
                "check": "Check the completed cycles.", "why": "Output is below normal.",
                "evidence": ["Today: 2,000 LF"], "category": "Throughput",
            }],
        }
        with tempfile.TemporaryDirectory() as directory, patch.object(
            management_checks, "get_db_path", return_value=Path(directory) / "checks.db"
        ), patch.dict("os.environ", {"MANAGEMENT_CHECKS_COMMENT_SECRET": "test-secret"}):
            management_checks.save_report("production", report, ["manager@example.com"], "sent")
            token = management_checks.comment_token("2026-11-04", "production", "press-total-variance")
            issue = management_checks.get_issue_by_token(token)
            self.assertEqual(issue["status"], "ok")
            self.assertEqual(issue["issue"]["title"], "Press output is low")

            result = management_checks.add_issue_comment(
                token, "Lois", "lois@example.com", "Cycle entry corrected.", "Resolved"
            )
            self.assertEqual(result["status"], "ok")
            updated = management_checks.get_issue_by_token(token)["issue"]
            self.assertEqual(updated["status"], "Resolved")
            self.assertEqual(updated["comments"][0]["comment_text"], "Cycle entry corrected.")

            history = management_checks.list_report_history()
            self.assertEqual(history[0]["report_type"], "production")
            self.assertEqual(history[0]["issues"][0]["comments"][0]["commenter_name"], "Lois")

            completion = management_checks.get_completion_update("2026-11-04")
            self.assertEqual(completion["status"], "ready")
            self.assertEqual(completion["total_count"], 1)
            self.assertEqual(management_checks.begin_completion_notification("2026-11-04")["status"], "claimed")
            management_checks.finish_completion_notification("2026-11-04", True)
            self.assertEqual(management_checks.get_completion_update("2026-11-04")["notification"]["status"], "sent")
            self.assertEqual(management_checks.begin_completion_notification("2026-11-04")["status"], "already_sent")

    def test_completion_waits_until_every_production_and_sales_issue_has_a_response(self):
        def report(report_type, issue_id):
            return {
                "subject": report_type,
                "facts": {"report_date": "2026-11-04", "generated_at": "2026-11-04T18:30:00-06:00"},
                "selection": {"summary": "Checks"},
                "issues": [{
                    "id": issue_id, "title": issue_id, "check": "Check it", "why": "Reason",
                    "evidence": [], "category": "Sales" if report_type == "sales" else "Throughput",
                }],
            }

        with tempfile.TemporaryDirectory() as directory, patch.object(
            management_checks, "get_db_path", return_value=Path(directory) / "checks.db"
        ), patch.dict("os.environ", {"MANAGEMENT_CHECKS_COMMENT_SECRET": "test-secret"}):
            management_checks.save_report("production", report("production", "prod-one"), [], "sent")
            management_checks.save_report("sales", report("sales", "sales-one"), [], "sent")
            production_token = management_checks.comment_token("2026-11-04", "production", "prod-one")
            sales_token = management_checks.comment_token("2026-11-04", "sales", "sales-one")

            management_checks.add_issue_comment(production_token, "Lois", "", "Checked", "Resolved")
            self.assertEqual(management_checks.get_completion_update("2026-11-04")["status"], "pending")
            management_checks.add_issue_comment(sales_token, "Peter", "", "Followed up", "Acknowledged")
            completion = management_checks.get_completion_update("2026-11-04")

            self.assertEqual(completion["status"], "ready")
            self.assertEqual(completion["total_count"], 2)

    def test_invalid_comment_token_is_rejected(self):
        with patch.dict("os.environ", {"MANAGEMENT_CHECKS_COMMENT_SECRET": "test-secret"}):
            self.assertEqual(management_checks.decode_comment_token("not-a-token")["status"], "invalid")


if __name__ == "__main__":
    unittest.main()
