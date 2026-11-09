import unittest
from io import BytesIO
from unittest.mock import patch

import filemaker
import main
from openpyxl import load_workbook


class FreightAnalysisTests(unittest.TestCase):
    def test_freight_fields_are_always_requested_from_filemaker(self):
        for field in main.FREIGHT_FIELDS + main.FREIGHT_WEIGHT_FIELDS:
            self.assertIn(field, filemaker.get_extra_fields())

    def test_money_parser_accepts_filemaker_currency_and_rejects_reference_numbers(self):
        self.assertEqual(main.parse_freight_value("$1,234.56"), 1234.56)
        self.assertEqual(main.parse_freight_value("(25.50)"), -25.5)
        self.assertIsNone(main.parse_freight_value("705-294670-9"))

    def test_freight_population_excludes_orders_without_freight_values(self):
        self.assertTrue(main.order_has_freight_data({"extra": {"Incoming Cost": 25}}))
        self.assertFalse(main.order_has_freight_data({"extra": {"Incoming Cost": ""}}))

    def test_analysis_calculates_current_revenue_and_unit_averages(self):
        orders = [{
            "customer": "Special Customer",
            "amount": 1000,
            "extra": {
                "Incoming Freight": 150,
                "Incoming Cost": 100,
                "Outgoing Freight": 150,
                "Outgoing Cost": 100,
                "Act Arrival_Gross Weight": 869,
                "Act Arrival_Nett Weight": 829,
                "Despatch_Gross Wright": 502,
                "Despatch_Nett Weight": 462,
                "Orders::Order No": 1234,
            },
        }]

        result = main.build_freight_analysis(orders, mat_counts_by_order={
            "1234": {"mats": 20, "repairs": 18, "rejects": 2, "repair_actions": 40},
        })
        row = result["rows"][0]

        self.assertEqual(row["total_revenue"], 1300)
        self.assertEqual(row["freight_charged"], 300)
        self.assertEqual(row["freight_cost"], 200)
        self.assertEqual(row["freight_margin"], 100)
        self.assertEqual(row["average_freight_cost_per_order"], 200)
        self.assertEqual(row["average_freight_income_per_mat"], 15)
        self.assertEqual(row["average_freight_cost_per_mat"], 10)
        self.assertEqual(row["repairs"], 18)
        self.assertEqual(row["rejects"], 2)
        self.assertEqual(row["freight_legs"], 2)
        self.assertEqual(row["incoming_gross_weight"], 869)
        self.assertEqual(row["outgoing_gross_weight"], 502)
        self.assertEqual(row["gross_weight_variance"], 367)
        self.assertEqual(row["net_weight_variance"], 367)

    def test_analysis_flags_incomplete_and_invalid_cost_data(self):
        orders = [{
            "customer": "Review Customer",
            "amount": 1000,
            "extra": {
                "Incoming Freight": 200,
                "Incoming Cost": "invoice 123",
            },
        }]

        result = main.build_freight_analysis(orders)
        row = result["rows"][0]

        self.assertEqual(result["invalid_values"], 1)
        self.assertEqual(row["cost_coverage"], 0)
        self.assertIsNone(row["average_freight_cost_per_mat"])

    def test_freight_analysis_page_renders_current_picture_and_location_filters(self):
        order_result = {
            "source": "filemaker",
            "status": "ok",
            "orders": [{
                "customer": "Special Customer",
                "amount": 1000,
                "order_date": "2026-10-01",
                "extra": {
                    "Customer Ref": "customer-key",
                    "Orders::Order No": 1234,
                    "Incoming Freight": 150,
                    "Incoming Cost": 100,
                    "Outgoing Freight": 150,
                    "Outgoing Cost": 100,
                },
            }],
        }
        master_data = {
            "status": "ok",
            "companies": [{"primary_key": "customer-key", "type": "Customer"}],
            "customers_by_key": {"customer-key": {
                "primary_key": "customer-key",
                "type": "Customer",
                "city": "LaGrange",
                "state": "GA",
                "zip_code": "30240",
            }},
        }

        with patch.object(main, "get_orders_for_analysis", return_value=order_result), patch.object(
            main, "fetch_filemaker_customer_data", return_value=master_data
        ), patch.object(main, "read_filemaker_mat_counts_cache", return_value={
            "status": "ok",
            "updated_at": "2026-11-06 03:00:00",
            "record_count": 20,
            "orders": {"1234": {"mats": 20, "repairs": 18, "rejects": 2}},
        }):
            response = main.get_freight_analysis_view(
                city="LaGrange",
                state="GA",
                start="2026-10-01",
                end="2026-10-01",
            )

        html = str(response)
        self.assertIn("Special Customer", html)
        self.assertIn("Freight revenue and cost analysis", html)
        self.assertIn("Total billed revenue", html)
        self.assertIn('id="freight-overview-tab"', html)
        self.assertIn('id="freight-breakdown-tab"', html)
        self.assertIn('id="mat-breakdown-tab"', html)
        self.assertIn("Buy-new price", html)
        self.assertIn('id="freight-breakdown"', html)
        self.assertIn("Customer summary Excel", html)
        self.assertIn("Job detail Excel", html)
        self.assertIn("/freight-job-detail-export.xlsx", html)
        self.assertIn("data-sort-type", html)
        self.assertIn("country = 'ca'", html)
        self.assertIn("rawPostalCode.slice(0, 3)", html)
        self.assertNotIn("Cost coverage", html)
        self.assertNotIn("discount scenario", html.lower())

    def test_excel_export_contains_numeric_customer_breakdown(self):
        analysis = main.build_freight_analysis([{
            "customer": "Export Customer",
            "amount": 1000,
            "extra": {
                "Incoming Freight": 150,
                "Incoming Cost": 100,
                "Outgoing Freight": 150,
                "Outgoing Cost": 100,
            },
        }])
        workbook = load_workbook(BytesIO(main.build_freight_analysis_workbook(
            analysis, {"state": "GA"}
        )))
        sheet = workbook["Customer breakdown"]

        self.assertEqual(sheet["A5"].value, "Export Customer")
        self.assertEqual(sheet["K5"].value, 300)
        self.assertEqual(sheet["L5"].value, 200)
        self.assertEqual(sheet.max_column, 27)
        self.assertNotIn("Cost Coverage", [cell.value for cell in sheet[4]])
        self.assertEqual(sheet.freeze_panes, "A5")

    def test_job_detail_export_itemises_cost_charge_and_variance(self):
        orders = [{
            "customer": "Variance Customer",
            "order_date": "2026-10-15",
            "amount": 1250,
            "filemaker_record_id": "987",
            "extra": {
                "Customer Ref": "customer-key",
                "Orders::Order No": "JOB-1001",
                "Incoming Freight": 80,
                "Incoming Cost": 100,
                "Outgoing Freight": 150,
                "Outgoing Cost": 120,
                "Act Arrival_Gross Weight": 500,
                "Despatch_Gross Wright": 450,
            },
        }, {
            "customer": "Variance Customer",
            "order_date": "2026-10-14",
            "amount": 800,
            "extra": {
                "Customer Ref": "customer-key",
                "Orders::Order No": "JOB-1000",
                "Incoming Freight": 90,
                "Incoming Cost": "",
            },
        }]
        customer_records = {
            "customer-key": {"city": "Patterson", "state": "CA"},
        }
        rows = main.build_freight_job_rows(
            orders,
            customer_records=customer_records,
            mat_counts_by_order={"JOB-1001": {"mats": 12, "repairs": 10, "rejects": 2}},
        )

        self.assertEqual(rows[0]["order_number"], "JOB-1001")
        self.assertEqual(rows[0]["incoming_variance"], -20)
        self.assertEqual(rows[0]["outgoing_variance"], 30)
        self.assertEqual(rows[0]["freight_variance"], 10)
        self.assertEqual(rows[0]["recovery_percent"], 230 / 220)
        self.assertEqual(rows[0]["status"], "Charge exceeds cost")
        self.assertEqual(rows[0]["mats"], 12)
        self.assertEqual(rows[1]["status"], "Incomplete incoming data")

        workbook = load_workbook(BytesIO(main.build_freight_job_detail_workbook(
            rows, {"state": "CA"}
        )))
        sheet = workbook["Job freight detail"]
        headers = [cell.value for cell in sheet[4]]
        self.assertIn("Incoming Charge", headers)
        self.assertIn("Incoming Cost", headers)
        self.assertIn("Freight Variance", headers)
        self.assertEqual(sheet["A5"].value, "JOB-1001")
        self.assertEqual(sheet["B5"].value.strftime("%m/%d/%Y"), "10/15/2026")
        self.assertEqual(sheet["K5"].value, 80)
        self.assertEqual(sheet["L5"].value, 100)
        self.assertEqual(sheet["S5"].value, 10)
        self.assertEqual(sheet["U5"].value, "Charge exceeds cost")
        self.assertEqual(sheet.freeze_panes, "A5")

    def test_mat_size_analysis_adds_average_freight_to_repair_price(self):
        orders = [{"extra": {"Orders::Order No": "1016"}}]
        mat_cache = {"orders": {"1016": {"sizes": {"3 x 5": {
            "mats": 3,
            "repairs": 2,
            "repair_value": 30,
            "repair_value_records": 2,
            "square_feet": 30,
            "square_feet_records": 2,
        }, "4x5": {
            "mats": 2,
            "repairs": 0,
            "repair_value": 0,
            "repair_value_records": 0,
            "square_feet": 0,
            "square_feet_records": 0,
        }}}}}
        rows = main.build_mat_size_freight_analysis(
            orders, mat_cache, {"average_freight_cost_per_mat": 4}
        )

        self.assertEqual(rows[0]["average_repair_cost"], 15)
        self.assertEqual(rows[0]["average_square_feet"], 15)
        self.assertEqual(rows[0]["freight_inclusive_repair_cost"], 19)
        self.assertEqual(len(rows), 1)
        self.assertEqual(main.normalize_mat_size_key("3 × 5"), "3x5")


if __name__ == "__main__":
    unittest.main()
