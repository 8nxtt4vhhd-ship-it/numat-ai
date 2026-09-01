import unittest
from datetime import datetime

import main


class WeeklyKpiPeriodTests(unittest.TestCase):
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
            today=datetime(2026, 9, 1, 9, 0),
        )

        self.assertEqual(payload["review_period"]["month_label"], "August 2026")
        self.assertEqual(payload["accounts"]["invoiced_revenue_mtd"], 100000)
        self.assertEqual(payload["accounts"]["average_debtor_days"], 22.0)
        self.assertTrue(payload["accounts"]["debtor_days_is_historical"])


if __name__ == "__main__":
    unittest.main()
