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


if __name__ == "__main__":
    unittest.main()
