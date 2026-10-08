import unittest
from unittest.mock import patch

import main
from main import render_production_date_control, resolve_production_date_range


class ProductionDateRangeTests(unittest.TestCase):
    def test_accepts_us_formatted_dates(self):
        start, end = resolve_production_date_range("10/01/2026", "10/06/2026")

        self.assertEqual(start, "2026-10-01")
        self.assertEqual(end, "2026-10-06")

    def test_control_displays_us_formatted_dates(self):
        html = render_production_date_control("2026-10-01", "2026-10-06")

        self.assertIn('value="10/01/2026"', html)
        self.assertIn('value="10/06/2026"', html)
        self.assertIn('placeholder="MM/DD/YYYY"', html)
        self.assertIn('name="refresh" value="1"', html)

    def test_default_dashboard_open_uses_last_cached_date_range(self):
        payload = {
            "status": "ok",
            "synced_at": "2026-10-07 04:00:00",
            "rows": [],
            "operator_summary": [],
            "latest": {},
            "summary": {},
        }
        detail = {"status": "ok", "clocking_count": 1, "summary": {}, "department_rows": [], "operator_rows": []}
        with (
            patch.object(main, "get_last_productivity_detail_range", return_value=("2026-10-01", "2026-10-07")),
            patch.object(main, "fetch_production_analysis_data", return_value={"status": "ok"}),
            patch.object(main, "build_production_kpi_payload", return_value=payload),
            patch.object(main, "fetch_productivity_detail", return_value=detail) as detail_fetch,
            patch.object(main, "build_current_month_production_payload", return_value={"summary": {}}),
        ):
            response = main.production_analysis_view()

        html = response.body.decode() if hasattr(response, "body") else str(response)
        self.assertIn('value="10/01/2026"', html)
        self.assertIn('value="10/07/2026"', html)
        self.assertEqual(detail_fetch.call_args.kwargs["period_start"], "2026-10-01")
        self.assertEqual(detail_fetch.call_args.kwargs["period_end"], "2026-10-07")

    def test_productivity_detail_page_shows_exact_calculation(self):
        detail = {
            "status": "ok",
            "booking_count": 20,
            "clocking_count": 10,
            "summary": {
                "booked_hours": 91.2,
                "clocked_hours": 100,
                "unbooked_hours": 8.8,
                "unmatched_booked_hours": 0,
                "manager_support_hours": 2,
                "manager_clocked_hours": 31.3,
                "labour_clocked_hours": 131.3,
                "plant_productivity": 91.2,
            },
            "department_rows": [],
            "operator_rows": [],
        }
        with patch.object(main, "fetch_productivity_detail", return_value=detail):
            response = main.production_productivity_detail_view("10/01/2026", "10/06/2026")

        html = response.body.decode() if hasattr(response, "body") else str(response)
        self.assertIn("Productive booked hours", html)
        self.assertIn("Production operator clocked hours", html)
        self.assertIn("manager clockings excluded from productivity", html)
        self.assertIn("Total paid plant hours", html)
        self.assertIn("31.3 manager hrs", html)
        self.assertIn("91.2%", html)
        self.assertIn("10/01/2026 to 10/06/2026", html)

    def test_operator_table_links_to_selected_period_detail(self):
        html = main.render_operator_analysis(
            [{"name": "Zach Holmes", "days": 1, "productivity": 120, "clocked_hours": 8, "booked_hours": 9.6, "unbooked_hours": 0}],
            period_start="2026-10-01",
            period_end="2026-10-06",
        )

        self.assertIn("/production-analysis/operator-detail?", html)
        self.assertIn("Zach+Holmes", html)
        self.assertIn("View bookings", html)

    def test_overbooked_report_lists_only_operator_days_above_clocked_time(self):
        detail = {
            "status": "ok",
            "time_bookings": [
                {"operator": "Zach Holmes", "operator_key": "zachholmes", "date": "2026-10-05", "hours": 5.0},
                {"operator": "Zach Holmes", "operator_key": "zachholmes", "date": "2026-10-05", "hours": 4.0},
                {"operator": "Amy Smith", "operator_key": "amysmith", "date": "2026-10-05", "hours": 7.0},
                {"operator": "Lois", "operator_key": "lois", "date": "2026-10-05", "hours": 10.0, "manager_support": True},
            ],
            "clockings": [
                {"operator": "Zach Holmes", "operator_key": "zachholmes", "date": "2026-10-05", "clocked_hours": 8.0},
                {"operator": "Amy Smith", "operator_key": "amysmith", "date": "2026-10-05", "clocked_hours": 8.0},
                {"operator": "Lois", "operator_key": "lois", "date": "2026-10-05", "clocked_hours": 8.0, "manager": True},
            ],
        }
        with patch.object(main, "fetch_productivity_detail", return_value=detail):
            response = main.production_overbooked_report("10/05/2026", "10/05/2026")

        html = response.body.decode() if hasattr(response, "body") else str(response)
        self.assertIn("Zach Holmes", html)
        self.assertIn("+1.00 hrs", html)
        self.assertIn("112.5%", html)
        self.assertIn("Review bookings", html)
        self.assertNotIn("Amy Smith", html)
        self.assertNotIn(">Lois<", html)

    def test_time_booking_update_writes_verified_record_and_clears_cache(self):
        detail = {
            "time_bookings": [
                {
                    "record_id": "123",
                    "operator_key": "zachholmes",
                    "department": "Grind",
                    "start_time": "10/05/2026 08:00:00",
                    "finish_time": "10/05/2026 10:00:00",
                }
            ]
        }
        with (
            patch.object(main, "get_current_session_user", return_value={"username": "admin", "role": "admin"}),
            patch.object(main, "fetch_productivity_detail", return_value=detail),
            patch.object(main, "update_layout_record", return_value={"status": "ok"}) as update,
            patch.object(main, "clear_productivity_detail_cache") as clear_cache,
            patch.object(main, "clear_weekly_kpi_payload_cache") as clear_weekly_cache,
            patch.object(main, "record_audit_event"),
        ):
            response = main.update_production_time_booking(
                record_id="123",
                operator="Zach Holmes",
                start="10/01/2026",
                end="10/06/2026",
                department="Engineering",
                booking_start="2026-10-05T08:30:00",
                booking_finish="2026-10-05T09:30:00",
                notes="Corrected",
            )

        self.assertEqual(response.status_code, 303)
        update.assert_called_once_with(
            "ai_Time Bookings",
            "123",
            {
                "Department": "Engineering",
                "Start Time": "10/05/2026 08:30:00",
                "Finish Time": "10/05/2026 09:30:00",
                "Notes": "Corrected",
            },
        )
        clear_cache.assert_called_once_with()
        clear_weekly_cache.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
