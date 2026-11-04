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

    def test_invalid_comment_token_is_rejected(self):
        with patch.dict("os.environ", {"MANAGEMENT_CHECKS_COMMENT_SECRET": "test-secret"}):
            self.assertEqual(management_checks.decode_comment_token("not-a-token")["status"], "invalid")


if __name__ == "__main__":
    unittest.main()
