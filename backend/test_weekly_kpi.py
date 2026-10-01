import unittest
from datetime import datetime
from unittest.mock import patch

import main
from crm import normalize_crm_row


class WeeklyKpiPeriodTests(unittest.TestCase):
    def test_dashboard_loads_each_default_data_source_once(self):
        production = {"status": "ok", "production_rows": [], "operator_rows": [], "plant_operator_rows": []}
        calendar = {"status": "ok", "events": []}
        finance = {"status": "ok", "average_debtor_days": 20.0}
        master = {"status": "ok", "companies": [], "contacts_by_email": {}, "customers_by_key": {}}
        orders = {"status": "ok", "orders": []}
        crm = {"status": "ok", "activities": []}

        with (
            patch.object(main, "fetch_weekly_kpi_crm_result", return_value=crm) as crm_loader,
            patch.object(main, "fetch_production_analysis_data", return_value=production) as production_loader,
            patch.object(main, "fetch_calendar_events", return_value=calendar) as calendar_loader,
            patch.object(main, "fetch_aged_debt_summary", return_value=finance) as finance_loader,
            patch.object(main, "fetch_filemaker_master_data", return_value=master) as master_loader,
            patch.object(main, "get_orders_for_analysis", return_value=orders) as order_loader,
        ):
            payload = main.build_weekly_kpi_dashboard_payload(today=datetime(2026, 9, 17, 9, 0))

        self.assertEqual(payload["status"], "ok")
        for loader in (crm_loader, production_loader, calendar_loader, finance_loader, master_loader, order_loader):
            loader.assert_called_once()

    def test_forced_dashboard_refresh_bypasses_source_caches(self):
        production = {"status": "ok", "production_rows": [], "operator_rows": [], "plant_operator_rows": []}
        calendar = {"status": "ok", "events": []}
        finance = {"status": "ok", "average_debtor_days": 20.0}
        master = {"status": "ok", "companies": [], "contacts_by_email": {}, "customers_by_key": {}}
        orders = {"status": "ok", "orders": []}
        crm = {"status": "ok", "activities": []}

        with (
            patch.object(main, "fetch_weekly_kpi_crm_result", return_value=crm) as crm_loader,
            patch.object(main, "fetch_production_analysis_data", return_value=production) as production_loader,
            patch.object(main, "fetch_calendar_events", return_value=calendar) as calendar_loader,
            patch.object(main, "fetch_aged_debt_summary", return_value=finance) as finance_loader,
            patch.object(main, "fetch_filemaker_master_data", return_value=master) as master_loader,
            patch.object(main, "clear_filemaker_orders_cache") as clear_orders,
            patch.object(main, "get_orders_for_analysis", return_value=orders),
        ):
            main.build_weekly_kpi_dashboard_payload(
                today=datetime(2026, 10, 1, 9, 0),
                force_refresh=True,
            )

        crm_loader.assert_called_once_with(force_refresh=True)
        production_loader.assert_called_once_with(force_refresh=True)
        finance_loader.assert_called_once_with(force_refresh=True)
        master_loader.assert_called_once_with(force_refresh=True)
        self.assertTrue(calendar_loader.call_args.kwargs["force_refresh"])
        clear_orders.assert_called_once_with()

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

    def test_open_promise_uses_contacts_current_filemaker_location(self):
        crm_result = {
            "status": "ok",
            "activities": [{
                "crm_type": "Promise of Order",
                "crm_is_complete": False,
                "date_created": "2026-09-17 09:00:00",
                "subject": "Mats ready for repair",
                "sender_email": "pbinnington@numatsystems.com",
                "to": "moved.contact@example.com",
                "recipient_company": "Alsco Inc Portland Industrial",
                "customer_company": "Alsco Inc Portland Industrial",
                "customer_primary_key": "old-location",
            }],
        }
        master_data_result = {
            "status": "ok",
            "contacts_by_email": {
                "moved.contact@example.com": {
                    "email": "moved.contact@example.com",
                    "customer_ref": "new-location",
                },
            },
            "customers_by_key": {
                "new-location": {"primary_key": "new-location", "company": "Alsco Inc New Location"},
            },
        }

        promises = main.build_open_promises(crm_result, master_data_result)

        self.assertEqual(promises[0]["customer"], "Alsco Inc New Location")
        self.assertEqual(promises[0]["customer_primary_key"], "new-location")

    def test_open_promise_keeps_original_location_when_contact_lookup_is_missing(self):
        crm_result = {
            "status": "ok",
            "activities": [{
                "crm_type": "Promise of Order",
                "crm_is_complete": False,
                "date_created": "2026-09-17 09:00:00",
                "subject": "Mats ready for repair",
                "sender_email": "pbinnington@numatsystems.com",
                "to": "unknown@example.com",
                "customer_company": "Original Location",
                "customer_primary_key": "original-location",
            }],
        }

        promises = main.build_open_promises(
            crm_result,
            {"status": "ok", "contacts_by_email": {}, "customers_by_key": {}},
        )

        self.assertEqual(promises[0]["customer"], "Original Location")
        self.assertEqual(promises[0]["customer_primary_key"], "original-location")

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
