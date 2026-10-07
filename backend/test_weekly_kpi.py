import unittest
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import main
from crm import normalize_crm_row


class WeeklyKpiPeriodTests(unittest.TestCase):
    def setUp(self):
        with main._WEEKLY_KPI_PAYLOAD_CACHE_LOCK:
            main._WEEKLY_KPI_PAYLOAD_CACHE.update({
                "date_key": "",
                "expires_at": 0.0,
                "payload": None,
                "refreshing": False,
            })

    def test_complete_dashboard_payload_is_reused_while_fresh(self):
        payload = {"status": "ok", "summary": {"open_count": 3}}
        date_key = datetime.now(main.UK_TIMEZONE).strftime("%Y-%m-%d")
        main.store_weekly_kpi_payload(payload, date_key=date_key)

        with patch.object(main, "build_weekly_kpi_dashboard_payload") as builder:
            result = main.get_cached_weekly_kpi_dashboard_payload()

        self.assertIs(result, payload)
        builder.assert_not_called()

    def test_strategic_summary_counts_recent_additions_and_outbound_reach(self):
        summary = main.build_strategic_contact_kpi_summary(
            items=[
                {"id": "one", "email": "one@example.com", "active": True, "created_at": "2026-10-03 09:00:00"},
                {"id": "two", "email": "two@example.com", "active": True, "created_at": "2026-09-01 09:00:00"},
            ],
            crm_result={"status": "ok", "activities": [
                {"date_created": "2026-10-05 10:00:00", "direction": "Outbound", "to": "one@example.com"},
                {"date_created": "2026-10-05 11:00:00", "direction": "Inbound", "sender_email": "two@example.com", "to": "sales@numatsystems.com"},
            ]},
            now=datetime(2026, 10, 6, 12, 0),
        )

        self.assertEqual(summary, {"total": 2, "added_last_7_days": 1, "reached_last_7_days": 1})

    def test_weekly_todos_round_trip(self):
        with TemporaryDirectory() as temp_dir, patch.object(
            main, "get_weekly_kpi_todos_path", return_value=Path(temp_dir) / "todos.json"
        ):
            expected = [{"id": "todo-1", "text": "Call customer", "completed_at": ""}]
            main.save_weekly_kpi_todos(expected)
            self.assertEqual(main.load_weekly_kpi_todos(), expected)

    def test_weekly_meeting_records_round_trip_and_finalize_actions(self):
        with TemporaryDirectory() as temp_dir, patch.object(
            main, "get_weekly_kpi_meetings_path", return_value=Path(temp_dir) / "meetings.json"
        ):
            record = {
                "id": "meeting-1",
                "created_at": "2026-10-06 12:00:00",
                "summary": "Reviewed weekly performance.",
                "approved_actions": [],
                "finalized_at": "",
            }
            main.save_weekly_kpi_meetings([record])
            self.assertEqual(main.get_weekly_kpi_meeting("meeting-1")["summary"], "Reviewed weekly performance.")

            updated = main.update_weekly_kpi_meeting_actions(
                "meeting-1", [{"text": "Call the customer", "owner": "Kelly"}]
            )

            self.assertEqual(updated["approved_actions"][0]["owner"], "Kelly")
            self.assertTrue(updated["finalized_at"])

    def test_forced_refresh_rebuilds_and_replaces_complete_payload(self):
        old_payload = {"status": "ok", "summary": {"open_count": 3}}
        new_payload = {"status": "ok", "summary": {"open_count": 4}}
        date_key = datetime.now(main.UK_TIMEZONE).strftime("%Y-%m-%d")
        main.store_weekly_kpi_payload(old_payload, date_key=date_key)

        with patch.object(main, "build_weekly_kpi_dashboard_payload", return_value=new_payload) as builder:
            result = main.get_cached_weekly_kpi_dashboard_payload(force_refresh=True)

        self.assertIs(result, new_payload)
        builder.assert_called_once_with(force_refresh=True)
        self.assertIs(main._WEEKLY_KPI_PAYLOAD_CACHE["payload"], new_payload)

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
            patch.object(main, "fetch_productivity_detail", return_value={"status": "ok", "clocking_count": 1, "summary": {"plant_productivity": 91.2}}) as productivity_loader,
            patch.object(main, "fetch_calendar_events", return_value=calendar) as calendar_loader,
            patch.object(main, "fetch_aged_debt_summary", return_value=finance) as finance_loader,
            patch.object(main, "fetch_filemaker_master_data", return_value=master) as master_loader,
            patch.object(main, "fetch_crm_activities", return_value=crm),
            patch.object(main, "get_orders_for_analysis", return_value=orders) as order_loader,
        ):
            payload = main.build_weekly_kpi_dashboard_payload(today=datetime(2026, 9, 17, 9, 0))

        self.assertEqual(payload["status"], "ok")
        for loader in (crm_loader, production_loader, productivity_loader, calendar_loader, finance_loader, master_loader, order_loader):
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
            patch.object(main, "fetch_productivity_detail", return_value={"status": "ok", "clocking_count": 1, "summary": {"plant_productivity": 91.2}}) as productivity_loader,
            patch.object(main, "fetch_calendar_events", return_value=calendar) as calendar_loader,
            patch.object(main, "fetch_aged_debt_summary", return_value=finance) as finance_loader,
            patch.object(main, "fetch_filemaker_master_data", return_value=master) as master_loader,
            patch.object(main, "fetch_crm_activities", return_value=crm),
            patch.object(main, "clear_filemaker_orders_cache") as clear_orders,
            patch.object(main, "get_orders_for_analysis", return_value=orders),
        ):
            main.build_weekly_kpi_dashboard_payload(
                today=datetime(2026, 10, 1, 9, 0),
                force_refresh=True,
            )

        crm_loader.assert_called_once_with(force_refresh=True)
        production_loader.assert_called_once_with(force_refresh=True)
        self.assertTrue(productivity_loader.call_args.kwargs["force_refresh"])
        self.assertEqual(productivity_loader.call_args.kwargs["period_start"], "2026-09-01")
        self.assertEqual(productivity_loader.call_args.kwargs["period_end"], "2026-09-30")
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

    def test_dashboard_uses_detailed_paid_time_productivity(self):
        production_result = {
            "status": "ok",
            "production_rows": [{"date": "2026-09-30"}],
            "operator_rows": [],
            "plant_operator_rows": [],
        }
        payload = main.build_weekly_kpi_dashboard_payload(
            crm_result={"status": "ok", "activities": []},
            production_result=production_result,
            productivity_detail={
                "status": "ok",
                "clocking_count": 10,
                "summary": {"plant_productivity": 91.2},
            },
            calendar_result={"status": "ok", "events": []},
            finance_result={"status": "ok", "average_debtor_days": 24.0},
            master_data_result={"status": "ok", "companies": []},
            order_result={"status": "ok", "orders": []},
            today=datetime(2026, 10, 1, 9, 0),
        )

        self.assertEqual(payload["production_mtd"]["summary"]["plant_productivity"], 91.2)
        self.assertEqual(
            payload["production_mtd"]["summary"]["plant_productivity_source"],
            "time_bookings_and_clockings",
        )

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
