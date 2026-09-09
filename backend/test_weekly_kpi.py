import unittest
from datetime import datetime

import main
from crm import normalize_crm_row


class WeeklyKpiPeriodTests(unittest.TestCase):
    def test_promise_status_received_or_cancelled_is_closed(self):
        for status in ("Received", "Cancelled"):
            activity = normalize_crm_row({
                "CRM Type": "Promise of Order",
                "Promise of Order Cancelled": status,
            }, index=1)
            self.assertTrue(activity["crm_is_complete"])

    def test_blank_promise_status_remains_open(self):
        activity = normalize_crm_row({
            "CRM Type": "Promise of Order",
            "Promise of Order Cancelled": "",
        }, index=1)
        self.assertFalse(activity["crm_is_complete"])

    def test_first_seven_days_review_previous_completed_month(self):
        period = main.get_weekly_kpi_review_period(datetime(2026, 9, 7, 9, 0))

        self.assertTrue(period["is_previous_month"])
        self.assertEqual(period["period_start"].date().isoformat(), "2026-08-01")
        self.assertEqual(period["period_end"].date().isoformat(), "2026-08-31")
        self.assertEqual(period["month_label"], "August 2026")

    def test_after_first_week_uses_current_month_through_yesterday(self):
        period = main.get_weekly_kpi_review_period(datetime(2026, 9, 8, 9, 0))

        self.assertFalse(period["is_previous_month"])
        self.assertEqual(period["period_start"].date().isoformat(), "2026-09-01")
        self.assertEqual(period["period_end"].date().isoformat(), "2026-09-07")

    def test_dashboard_uses_previous_month_revenue_and_debtor_days(self):
        production_result = {
            "status": "ok",
            "production_rows": [
                {"date": "2026-08-30", "invoiced_revenue_mtd": 95000, "aged_debtor_days": 22.0, "labour_cost": 19000},
                {"date": "2026-08-31", "invoiced_revenue_mtd": 100000, "aged_debtor_days": None, "labour_cost": 1000},
                {"date": "2026-09-01", "invoiced_revenue_mtd": 5000, "aged_debtor_days": 24.0, "labour_cost": 1000},
            ],
            "operator_rows": [],
            "plant_operator_rows": [],
        }
        payload = main.build_weekly_kpi_dashboard_payload(
            crm_result={"status": "ok", "activities": []},
            production_result=production_result,
            calendar_result={"status": "ok", "events": []},
            finance_result={"status": "ok", "average_debtor_days": 24.0},
            master_data_result={"status": "ok", "companies": []},
            order_result={"status": "ok", "orders": []},
            today=datetime(2026, 9, 1, 9, 0),
        )

        self.assertEqual(payload["review_period"]["month_label"], "August 2026")
        self.assertEqual(payload["accounts"]["invoiced_revenue_mtd"], 100000)
        self.assertEqual(payload["accounts"]["average_debtor_days"], 22.0)
        self.assertTrue(payload["accounts"]["debtor_days_is_historical"])

    def test_identifies_first_ever_and_return_after_two_year_gap(self):
        order_result = {
            "status": "ok",
            "orders": [
                {"customer": "Brand New Co", "customer_primary_key": "new-1", "order_date": "2026-09-03"},
                {"customer": "Returning Co", "customer_primary_key": "return-1", "order_date": "2023-08-01"},
                {"customer": "Returning Co", "customer_primary_key": "return-1", "order_date": "2026-09-04"},
                {"customer": "Regular Co", "customer_primary_key": "regular-1", "order_date": "2026-01-01"},
                {"customer": "Regular Co", "customer_primary_key": "regular-1", "order_date": "2026-09-05"},
            ],
        }

        results = main.build_new_and_returning_customers(
            order_result,
            datetime(2026, 9, 1),
            datetime(2026, 10, 1),
        )

        self.assertEqual([item["customer"] for item in results], ["Returning Co", "Brand New Co"])
        self.assertEqual(results[0]["category"], "Lost & lapsed return")
        self.assertEqual(results[1]["category"], "New")

    def test_customer_is_only_listed_once_in_review_period(self):
        results = main.build_new_and_returning_customers(
            {
                "status": "ok",
                "orders": [
                    {"customer": "New Co", "customer_primary_key": "new-1", "order_date": "2026-09-02"},
                    {"customer": "New Co", "customer_primary_key": "new-1", "order_date": "2026-09-08"},
                ],
            },
            datetime(2026, 9, 1),
            datetime(2026, 10, 1),
        )

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["order_date"], "2026-09-02")


if __name__ == "__main__":
    unittest.main()
