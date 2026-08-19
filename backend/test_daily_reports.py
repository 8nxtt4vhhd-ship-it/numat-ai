from datetime import date
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import daily_reports
from daily_dash import build_daily_dash_payload, build_daily_dash_pdf


def sample_production_result():
    return {
        "status": "ok",
        "production_rows": [{
            "date": "2026-08-18",
            "press_throughput": 100,
            "press_throughput_reported": 100,
            "production_revenue_today": 200,
            "labour_percentage": 25,
            "backlog_weeks": 1.2,
            "backlog_sort": 10,
            "backlog_grind": 20,
            "backlog_press": 30,
            "backlog_trim": 40,
            "incoming_skids": 5,
            "ff1_mats": 4,
            "ff1_cycles": 2,
            "ff1_lf": 100,
        }],
        "operator_rows": [],
        "plant_operator_rows": [
            {"date": "2026-08-18", "name": "Operator", "booked_hours": 6, "clocked_hours": 8},
            {"date": "2026-08-18", "name": "Manager", "booked_hours": 0, "clocked_hours": 2, "excluded": True},
        ],
    }


class DailyDashTests(unittest.TestCase):
    def test_payload_uses_all_plant_clockings(self):
        payload = build_daily_dash_payload(sample_production_result(), {"status": "ok", "orders": []}, daily_invoice_target=8000)
        self.assertEqual(payload["productivity_today"], 60)
        self.assertEqual(payload["productivity_mtd"], 60)
        self.assertEqual(payload["labour_hours_today"], 10)
        self.assertEqual(payload["monthly_invoice_target"], 80000)

    def test_pdf_is_valid(self):
        payload = build_daily_dash_payload(sample_production_result(), {"status": "ok", "orders": []}, daily_invoice_target=8000)
        content = build_daily_dash_pdf(payload)
        self.assertTrue(content.startswith(b"%PDF"))
        self.assertGreater(len(content), 1000)

    def test_dry_run_writes_pdf_and_does_not_send(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(daily_reports, "fetch_production_analysis_data", return_value=sample_production_result()):
                with patch.object(daily_reports, "get_orders_for_analysis", return_value={"status": "ok", "orders": []}):
                    with patch.object(daily_reports, "fetch_aged_debt_summary", return_value={"status": "ok"}):
                        with patch.object(daily_reports, "all_active_user_recipients", return_value=["kelly@example.com"]):
                            with patch.object(daily_reports, "send_m365_reporting_mail") as send:
                                result = daily_reports.deliver(date(2026, 8, 18), dry_run=True, output_dir=directory)
            self.assertEqual(result["status"], "dry_run")
            self.assertTrue(Path(result["path"]).read_bytes().startswith(b"%PDF"))
            send.assert_not_called()


if __name__ == "__main__":
    unittest.main()
