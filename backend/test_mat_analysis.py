import unittest
from io import BytesIO
from unittest.mock import patch

import main
from openpyxl import load_workbook


class MatAnalysisTests(unittest.TestCase):
    def setUp(self):
        self.orders = [{
            "customer": "Mat Customer",
            "order_date": "2026-11-01",
            "amount": 1200,
            "extra": {
                "Customer Ref": "customer-key",
                "Orders::Status": "Complete",
                "Orders::Order No": "MAT-100",
                "Incoming Freight": 80,
                "Incoming Cost": 60,
                "Outgoing Freight": 120,
                "Outgoing Cost": 90,
                "Act Arrival_Gross Weight": 500,
                "Act Arrival_Skids": 5,
            },
        }]
        self.mat_cache = {
            "status": "ok",
            "updated_at": "2026-11-12 03:00:00",
            "record_count": 10,
            "orders": {"MAT-100": {
                "mats": 10,
                "repairs": 8,
                "rejects": 2,
                "sizes": {"3x5": {
                    "mats": 10,
                    "repairs": 8,
                    "repair_value": 160,
                    "repair_value_records": 8,
                    "square_feet": 120,
                    "square_feet_records": 8,
                }},
            }},
        }
        self.master_data = {
            "status": "ok",
            "companies": [{"primary_key": "customer-key", "type": "Customer"}],
            "customers_by_key": {"customer-key": {
                "primary_key": "customer-key",
                "type": "Customer",
                "city": "LaGrange",
                "state": "GA",
            }},
        }

    def test_mat_analysis_uses_all_completed_orders_with_cached_mats(self):
        result = main.build_mat_analysis(
            self.orders,
            customer_records=self.master_data["customers_by_key"],
            mat_counts_by_order=self.mat_cache["orders"],
        )

        self.assertEqual(result["customer_count"], 1)
        self.assertEqual(result["order_count"], 1)
        self.assertEqual(result["mats"], 10)
        self.assertEqual(result["repairs"], 8)
        self.assertEqual(result["rejects"], 2)
        self.assertEqual(result["repair_rate"], 0.8)
        self.assertEqual(result["repair_revenue"], 1200)
        self.assertEqual(result["freight_charged"], 200)
        self.assertEqual(result["rows"][0]["average_repair_revenue_per_mat"], 150)

    def test_overview_groups_non_standard_sizes_as_weighted_others(self):
        rows = [{
            "size": "3 x 5", "repairs": 8, "repair_value": 160,
            "repair_value_records": 8,
        }, {
            "size": "2x3", "repairs": 2, "repair_value": 30,
            "repair_value_records": 2,
        }, {
            "size": "4x10", "repairs": 3, "repair_value": 90,
            "repair_value_records": 3,
        }]

        grouped = main.group_mat_sizes_for_overview(rows)

        self.assertEqual([row["size"] for row in grouped], ["3x4", "3x5", "3x10", "4x6", "Others"])
        self.assertEqual(grouped[1]["repairs"], 8)
        self.assertEqual(grouped[1]["average_repair_cost"], 20)
        self.assertEqual(grouped[4]["repairs"], 5)
        self.assertEqual(grouped[4]["average_repair_cost"], 24)

    def test_mat_analysis_page_has_visual_overview_and_filtered_breakdowns(self):
        order_result = {"source": "filemaker", "status": "ok", "orders": self.orders}
        with (
            patch.object(main, "get_orders_for_analysis", return_value=order_result),
            patch.object(main, "fetch_filemaker_customer_data", return_value=self.master_data),
            patch.object(main, "read_filemaker_mat_counts_cache", return_value=self.mat_cache),
        ):
            response = main.get_mat_analysis_view(
                customer="Mat",
                city="LaGrange",
                state="GA",
                start="2026-11-01",
                end="2026-11-01",
            )

        html = str(response)
        self.assertIn("Mat volume, outcomes and pricing", html)
        self.assertIn("Repaired mats by size", html)
        self.assertIn("Average repair price by size", html)
        self.assertIn('id="mat-customer-tab"', html)
        self.assertIn('id="mat-order-tab"', html)
        self.assertIn('id="mat-size-tab"', html)
        self.assertIn("Mat Customer", html)
        self.assertIn("MAT-100", html)
        self.assertIn("3x5", html)
        self.assertIn("Others", html)
        self.assertIn("Customer summary Excel", html)
        self.assertIn("Order detail Excel", html)
        self.assertIn("Mat breakdown Excel", html)
        self.assertIn("/mat-analysis-customer-export.xlsx", html)
        self.assertIn("/mat-analysis-order-export.xlsx", html)
        self.assertIn("/mat-analysis-size-export.xlsx", html)
        self.assertNotIn("Incoming gross", html)
        self.assertNotIn("Incoming cost", html)
        self.assertNotIn("Incoming skids", html)

    def test_mat_analysis_excel_exports_are_numeric_and_include_buy_new_comparison(self):
        analysis = main.build_mat_analysis(
            self.orders,
            customer_records=self.master_data["customers_by_key"],
            mat_counts_by_order=self.mat_cache["orders"],
        )
        mat_sizes = main.build_mat_size_freight_analysis(self.orders, self.mat_cache, analysis)

        customer_book = load_workbook(BytesIO(main.build_mat_analysis_workbook(
            analysis, mat_sizes, {"customer": "Mat"}, section="customer",
        )))
        customer_sheet = customer_book["Customer breakdown"]
        self.assertEqual(customer_sheet["A5"].value, "Mat Customer")
        self.assertEqual(customer_sheet["C5"].value, 10)
        self.assertEqual(customer_sheet["I5"].value, 1200)
        self.assertEqual(customer_sheet.freeze_panes, "A5")

        order_book = load_workbook(BytesIO(main.build_mat_analysis_workbook(
            analysis, mat_sizes, {"state": "GA"}, section="order",
        )))
        order_sheet = order_book["Order breakdown"]
        self.assertEqual(order_sheet["A5"].value, "MAT-100")
        self.assertEqual(order_sheet["F5"].value, 10)
        self.assertEqual(order_sheet["J5"].value, 1200)

        size_book = load_workbook(BytesIO(main.build_mat_analysis_workbook(
            analysis, mat_sizes, section="size", buy_new_price=2.0,
        )))
        size_sheet = size_book["Mat breakdown"]
        self.assertEqual(size_sheet["A5"].value, "3x5")
        self.assertEqual(size_sheet["H5"].value, 30)
        self.assertAlmostEqual(size_sheet["I5"].value, 10 - analysis["average_freight_cost_per_mat"])
        self.assertAlmostEqual(
            size_sheet["J5"].value,
            (20 + analysis["average_freight_cost_per_mat"]) / 30,
        )


if __name__ == "__main__":
    unittest.main()
