from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import app_settings


class AppSettingsTests(unittest.TestCase):
    def test_elapsed_target_counts_monday_through_thursday_inclusively(self):
        self.assertEqual(app_settings.elapsed_monday_thursday_workdays("2026-08-18"), 10)
        self.assertEqual(app_settings.calculate_elapsed_invoice_target("2026-08-18", 8000), 80000)

    def test_daily_target_can_be_saved_and_loaded(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "settings.json"
            with patch.object(app_settings, "get_settings_path", return_value=path):
                app_settings.set_daily_invoice_target("9,250")
                self.assertEqual(app_settings.get_daily_invoice_target(), 9250)

    def test_invalid_target_is_rejected(self):
        with self.assertRaises(ValueError):
            app_settings.set_daily_invoice_target("not a number")
        with self.assertRaises(ValueError):
            app_settings.set_daily_invoice_target("-1")

    def test_production_daily_issue_email_settings_round_trip(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "settings.json"
            with patch.object(app_settings, "get_settings_path", return_value=path):
                saved = app_settings.set_production_daily_issues_settings(
                    enabled=True,
                    recipients="Manager@Example.com\nteam@example.com, manager@example.com",
                    send_time="18:45",
                    context="Jobs are commonly shared across operators.",
                )
                loaded = app_settings.get_production_daily_issues_settings()

        self.assertEqual(saved, loaded)
        self.assertTrue(loaded["enabled"])
        self.assertEqual(loaded["recipients"], ["manager@example.com", "team@example.com"])
        self.assertEqual(loaded["send_time"], "18:45")
        self.assertEqual(loaded["timezone"], "America/Chicago")
        self.assertIn("Jobs are commonly shared across operators.", loaded["context"])
        self.assertIn("Around 1 hour of unbooked paid time", loaded["context"])
        self.assertIn("more than 1.5 hours", loaded["context"])

    def test_existing_context_is_augmented_with_unbooked_time_rule(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "settings.json"
            path.write_text(
                '{"production_daily_issues":{"context":"Existing plant guidance."}}',
                encoding="utf-8",
            )
            with patch.object(app_settings, "get_settings_path", return_value=path):
                loaded = app_settings.get_production_daily_issues_settings()

        self.assertIn("Existing plant guidance.", loaded["context"])
        self.assertIn("Around 1 hour of unbooked paid time", loaded["context"])
        self.assertIn("more than 1.5 hours", loaded["context"])

    def test_enabled_daily_issue_email_requires_recipient_and_valid_time(self):
        with self.assertRaises(ValueError):
            app_settings.set_production_daily_issues_settings(enabled=True, recipients="", send_time="18:30")
        with self.assertRaises(ValueError):
            app_settings.set_production_daily_issues_settings(enabled=False, recipients="manager@example.com", send_time="6:30 PM")


if __name__ == "__main__":
    unittest.main()
