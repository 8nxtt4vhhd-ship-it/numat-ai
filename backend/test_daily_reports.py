from datetime import date
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import daily_reports
from daily_dash import build_daily_dash_payload, build_daily_dash_pdf
from filemaker import map_filemaker_record_to_order


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
    def test_order_mapping_accepts_filemaker_field_name_variations(self):
        record = {"recordId": "1", "fieldData": {
            "Orders::CreationTimestamp": "11/09/2026 09:37:25",
            "Orders::Created By": "NuMat Systems Customer Service",
            "Orders::ExpectedArrivalDate": "11/16/2026",
            "Orders::Expected Arrival No of Cages": 4,
            "Orders::ExpectedArrivalSkids": 2,
            "Orders::Expected Arrival Gross Weight": 3104,
            "Orders::Order No": "3801",
            "Orders::Customer Name": "Wildman Business Group",
        }}

        order = map_filemaker_record_to_order(record)

        self.assertEqual(order["extra"]["CreatedBy"], "NuMat Systems Customer Service")
        self.assertEqual(order["extra"]["Expected Arrival Date"], "11/16/2026")
        self.assertEqual(order["extra"]["Expected Arrival Cages"], 4)
        self.assertEqual(order["extra"]["Expected Arrival Skids"], 2)
        self.assertEqual(order["extra"]["Expected Arrival Weight"], 3104)
    def test_payload_uses_all_plant_clockings(self):
        payload = build_daily_dash_payload(sample_production_result(), {"status": "ok", "orders": []}, daily_invoice_target=8000)
        self.assertEqual(payload["productivity_today"], 60)
        self.assertEqual(payload["productivity_mtd"], 60)
        self.assertEqual(payload["labour_hours_today"], 10)
        self.assertEqual(payload["monthly_invoice_target"], 80000)

    def test_payload_prefers_dashboard_raw_time_productivity(self):
        productivity_detail = {
            "status": "ok",
            "clocking_count": 10,
            "summary": {"plant_productivity": 95.4, "clocked_hours": 260.2, "labour_clocked_hours": 291.5},
            "day_summary": {"plant_productivity": 91.2, "clocked_hours": 85.75, "labour_clocked_hours": 96.0},
            "cycle_status": "ok",
            "cycles": [],
        }

        payload = build_daily_dash_payload(
            sample_production_result(),
            {"status": "ok", "orders": []},
            daily_invoice_target=8000,
            productivity_detail=productivity_detail,
        )

        self.assertEqual(payload["productivity_today"], 91.2)
        self.assertEqual(payload["productivity_mtd"], 95.4)
        self.assertEqual(payload["labour_hours_today"], 85.75)
        self.assertEqual(payload["labour_hours_mtd"], 260.2)

    def test_daily_report_visible_labour_hours_exclude_manager_clockings(self):
        detail = {
            "status": "ok",
            "clocking_count": 2,
            "time_bookings": [],
            "clockings": [
                {"date": "2026-08-18", "operator": "Operator", "operator_key": "operator", "clocked_hours": 8, "manager": False, "excluded": False},
                {"date": "2026-08-18", "operator": "Lois Horace", "operator_key": "loishorace", "clocked_hours": 2, "manager": True, "excluded": False},
            ],
            "cycles": [],
            "cycle_status": "ok",
            "summary": {"plant_productivity": 90, "clocked_hours": 8, "labour_clocked_hours": 10},
        }
        with (
            patch.object(daily_reports, "fetch_production_analysis_data", return_value=sample_production_result()),
            patch.object(daily_reports, "fetch_productivity_detail", return_value=detail),
            patch.object(daily_reports, "fetch_order_records", return_value={"status": "ok", "orders": []}),
            patch.object(daily_reports, "write_filemaker_orders_cache"),
            patch.object(daily_reports, "fetch_aged_debt_summary", return_value={"status": "ok"}),
        ):
            report = daily_reports.build_report(
                sample_production_result(),
                {"status": "ok", "orders": []},
                {"status": "ok"},
                requested_day=date(2026, 8, 18),
                productivity_detail={**detail, "day_summary": {"plant_productivity": 0, "clocked_hours": 8, "labour_clocked_hours": 10}},
            )

        self.assertEqual(report["payload"]["labour_hours_today"], 8)

    def test_pdf_is_valid(self):
        payload = build_daily_dash_payload(sample_production_result(), {"status": "ok", "orders": []}, daily_invoice_target=8000)
        content = build_daily_dash_pdf(payload)
        self.assertTrue(content.startswith(b"%PDF"))
        self.assertGreater(len(content), 1000)

    def test_payload_uses_all_cycle_repair_value_and_lists_orders_created_that_day(self):
        orders = [{
            "order_date": "2026-08-18",
            "order_created_at": "08/18/2026 09:37:25",
            "customer": "Wildman Business Group",
            "extra": {
                "Orders::Order No": "3801",
                "CreatedBy": "NuMat Systems Customer Service",
                "Expected Arrival Date": "08/25/2026",
                "Expected Arrival Cages": 4,
                "Expected Arrival Skids": "",
                "Expected Arrival Weight": 3104,
            },
        }]
        cycles = {"status": "ok", "cycles": [
            {"date": "2026-08-18", "machine": "FF1", "repair_value": 8571},
            {"date": "2026-08-18", "machine": "", "operator": "Kenley Pike", "repair_value": 793},
        ]}

        payload = build_daily_dash_payload(
            sample_production_result(),
            {"status": "ok", "orders": orders},
            daily_invoice_target=8000,
            cycles_result=cycles,
        )

        self.assertEqual(payload["production_revenue_today"], 9364)
        self.assertEqual(len(payload["new_orders_today"]), 1)
        self.assertEqual(payload["new_orders_today"][0]["extra"]["Orders::Order No"], "3801")

    def test_dry_run_writes_pdf_and_does_not_send(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(daily_reports, "fetch_production_analysis_data", return_value=sample_production_result()):
                with patch.object(daily_reports, "fetch_productivity_detail", return_value={"status": "ok", "clocking_count": 0, "time_bookings": [], "clockings": [], "cycles": [], "cycle_status": "ok", "summary": {}}):
                    with patch.object(daily_reports, "fetch_order_records", return_value={"status": "ok", "orders": []}):
                        with patch.object(daily_reports, "write_filemaker_orders_cache"):
                            with patch.object(daily_reports, "fetch_aged_debt_summary", return_value={"status": "ok"}):
                                with patch.object(daily_reports, "all_active_user_recipients", return_value=["kelly@example.com"]):
                                    with patch.object(daily_reports, "send_m365_reporting_mail") as send:
                                        result = daily_reports.deliver(date(2026, 8, 18), dry_run=True, output_dir=directory)
            self.assertEqual(result["status"], "dry_run")
            self.assertTrue(Path(result["path"]).read_bytes().startswith(b"%PDF"))
            send.assert_not_called()


if __name__ == "__main__":
    unittest.main()
