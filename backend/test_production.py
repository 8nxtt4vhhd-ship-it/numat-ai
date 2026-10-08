from datetime import timedelta
import os
import tempfile
import unittest
from unittest.mock import patch

import production

from production import (
    build_productivity_detail,
    build_production_kpi_payload,
    canonical_department,
    duration_hours,
    prorated_monthly_cost,
)


class ProductionKpiTests(unittest.TestCase):
    def test_production_cache_survives_application_restart(self):
        previous_production = dict(production._PRODUCTION_CACHE)
        previous_details = dict(production._PRODUCTIVITY_DETAIL_CACHE)
        previous_loaded = production._PRODUCTION_DISK_CACHE_LOADED
        previous_last_range = production._LAST_PRODUCTIVITY_DETAIL_RANGE
        try:
            with tempfile.TemporaryDirectory() as temporary_directory:
                cache_path = os.path.join(temporary_directory, "production-cache.json")
                with patch.dict(os.environ, {"FILEMAKER_PRODUCTION_CACHE_PATH": cache_path}):
                    production._PRODUCTION_CACHE.update({"expires_at": 0, "result": {"status": "ok", "snapshot": "saved"}})
                    production._PRODUCTIVITY_DETAIL_CACHE.clear()
                    production._PRODUCTIVITY_DETAIL_CACHE[("2026-10-01", "2026-10-06")] = {
                        "expires_at": 0,
                        "result": {"status": "ok", "booked_hours": 237.1},
                    }
                    production._LAST_PRODUCTIVITY_DETAIL_RANGE = ("2026-10-01", "2026-10-06")
                    production._PRODUCTION_DISK_CACHE_LOADED = True
                    production._save_production_disk_cache()

                    production._PRODUCTION_CACHE.update({"expires_at": 0, "result": None})
                    production._PRODUCTIVITY_DETAIL_CACHE.clear()
                    production._LAST_PRODUCTIVITY_DETAIL_RANGE = None
                    production._PRODUCTION_DISK_CACHE_LOADED = False
                    production._load_production_disk_cache()

                    self.assertEqual(production._PRODUCTION_CACHE["result"]["snapshot"], "saved")
                    restored = production._PRODUCTIVITY_DETAIL_CACHE[("2026-10-01", "2026-10-06")]["result"]
                    self.assertEqual(restored["booked_hours"], 237.1)
                    self.assertEqual(production.get_last_productivity_detail_range(), ("2026-10-01", "2026-10-06"))
                    self.assertEqual(oct(os.stat(cache_path).st_mode & 0o777), "0o600")
        finally:
            production._PRODUCTION_CACHE.clear()
            production._PRODUCTION_CACHE.update(previous_production)
            production._PRODUCTIVITY_DETAIL_CACHE.clear()
            production._PRODUCTIVITY_DETAIL_CACHE.update(previous_details)
            production._PRODUCTION_DISK_CACHE_LOADED = previous_loaded
            production._LAST_PRODUCTIVITY_DETAIL_RANGE = previous_last_range

    def test_productivity_cache_does_not_expire_without_manual_refresh(self):
        cache_key = ("2026-10-01", "2026-10-06")
        cached_result = {"status": "ok", "source": "cached"}
        previous_cache = dict(production._PRODUCTIVITY_DETAIL_CACHE)
        previous_loaded = production._PRODUCTION_DISK_CACHE_LOADED
        production._PRODUCTIVITY_DETAIL_CACHE.clear()
        production._PRODUCTION_DISK_CACHE_LOADED = True
        production._PRODUCTIVITY_DETAIL_CACHE[cache_key] = {
            "expires_at": 0,
            "result": cached_result,
        }
        try:
            with patch.object(production, "_find_all_records") as finder:
                result = production.fetch_productivity_detail(
                    period_start="2026-10-01",
                    period_end="2026-10-06",
                )
            self.assertIs(result, cached_result)
            finder.assert_not_called()
        finally:
            production._PRODUCTIVITY_DETAIL_CACHE.clear()
            production._PRODUCTIVITY_DETAIL_CACHE.update(previous_cache)
            production._PRODUCTION_DISK_CACHE_LOADED = previous_loaded

    def test_duration_hours_accepts_filemaker_time_values(self):
        self.assertEqual(duration_hours("10:30:00"), 10.5)
        self.assertEqual(duration_hours(timedelta(hours=2, minutes=15)), 2.25)

    def test_department_names_are_normalized(self):
        self.assertEqual(canonical_department("ReRuns"), "Re-Runs")
        self.assertEqual(canonical_department("Re-Runs"), "Re-Runs")
        self.assertEqual(canonical_department("Press Room Help"), "Press Room Help")

    def test_monthly_insurance_is_prorated_by_calendar_day(self):
        self.assertAlmostEqual(prorated_monthly_cost("2026-09-01", "2026-09-30", 6000), 6000)
        self.assertAlmostEqual(prorated_monthly_cost("2026-10-01", "2026-10-07", 6000), 6000 * 7 / 31)

    def test_productivity_detail_rolls_helpers_into_press_and_separates_manager(self):
        clockings = [
            {"operator": "Operator", "operator_key": "operator", "date": "2026-10-05", "clocked_hours": 10},
            {"operator": "Lois Horace", "operator_key": "loishorace", "date": "2026-10-05", "clocked_hours": 10, "manager": True},
            {"operator": "Trudy S Dunlap", "operator_key": "trudysdunlap", "date": "2026-10-05", "clocked_hours": 8, "excluded": True},
        ]
        bookings = [
            {"operator": "Operator", "operator_key": "operator", "date": "2026-10-05", "department": "Press", "department_rollup": "Press", "hours": 6},
            {"operator": "Operator", "operator_key": "operator", "date": "2026-10-05", "department": "Press Room Help", "department_rollup": "Press", "hours": 2},
            {"operator": "Operator", "operator_key": "operator", "date": "2026-10-05", "department": "Housekeeping", "department_rollup": "Housekeeping", "hours": 1},
            {"operator": "Lois Horace", "operator_key": "loishorace", "date": "2026-10-05", "department": "Press Room Help", "department_rollup": "Press", "hours": 1, "manager_support": True},
            {"operator": "Trudy S Dunlap", "operator_key": "trudysdunlap", "date": "2026-10-05", "department": "Engineering", "department_rollup": "Engineering", "hours": 7, "excluded": True},
        ]

        detail = build_productivity_detail(bookings, clockings)

        self.assertEqual(detail["summary"]["booked_hours"], 9)
        self.assertEqual(detail["summary"]["clocked_hours"], 10)
        self.assertEqual(detail["summary"]["plant_productivity"], 90)
        self.assertEqual(detail["summary"]["manager_support_hours"], 1)
        press = next(item for item in detail["department_rows"] if item["department"] == "Press")
        self.assertEqual(press["booked_hours"], 8)
        self.assertEqual(press["helper_hours"], 2)
        self.assertEqual(press["manager_support_hours"], 1)

    def test_unmatched_bookings_do_not_inflate_plant_productivity(self):
        detail = build_productivity_detail(
            [
                {"operator": "Matched", "operator_key": "matched", "date": "2026-10-05", "department": "Sort", "department_rollup": "Sort", "hours": 8},
                {"operator": "No clock", "operator_key": "noclock", "date": "2026-10-05", "department": "Grind", "department_rollup": "Grind", "hours": 6},
            ],
            [
                {"operator": "Matched", "operator_key": "matched", "date": "2026-10-05", "clocked_hours": 10},
            ],
        )

        self.assertEqual(detail["summary"]["booked_hours"], 8)
        self.assertEqual(detail["summary"]["unmatched_booked_hours"], 6)
        self.assertEqual(detail["summary"]["plant_productivity"], 80)

    def test_plant_productivity_uses_all_operator_clockings(self):
        result = {
            "status": "ok",
            "production_rows": [
                {
                    "date": "2026-08-17",
                    "time_booked_today": 40,
                    "labour_hours": 80,
                },
                {
                    "date": "2026-08-18",
                    "time_booked_today": 20,
                    "labour_hours": 20,
                },
            ],
            "operator_rows": [
                {
                    "date": "2026-08-17",
                    "name": "Included Operator",
                    "booked_hours": 9,
                    "clocked_hours": 10,
                    "productivity": 90,
                }
            ],
            "plant_operator_rows": [
                {
                    "date": "2026-08-17",
                    "name": "Included Operator",
                    "booked_hours": 9,
                    "clocked_hours": 10,
                },
                {
                    "date": "2026-08-17",
                    "name": "Excluded Manager",
                    "booked_hours": 3,
                    "clocked_hours": 10,
                    "excluded": True,
                },
            ],
        }

        payload = build_production_kpi_payload(result, days=0)

        self.assertEqual(payload["summary"]["plant_productivity"], 60)

    def test_plant_productivity_is_unavailable_without_clocked_hours(self):
        result = {
            "status": "ok",
            "production_rows": [
                {
                    "date": "2026-08-18",
                    "time_booked_today": 10,
                    "labour_hours": 0,
                }
            ],
            "operator_rows": [],
            "plant_operator_rows": [],
        }

        payload = build_production_kpi_payload(result, days=0)

        self.assertIsNone(payload["summary"]["plant_productivity"])

    def test_average_daily_production_value_excludes_blank_and_zero_days(self):
        result = {
            "status": "ok",
            "production_rows": [
                {"date": "2026-08-17", "production_revenue_today": 6000},
                {"date": "2026-08-18", "production_revenue_today": None},
                {"date": "2026-08-19", "production_revenue_today": 0},
                {"date": "2026-08-20", "production_revenue_today": 8000},
            ],
            "operator_rows": [],
            "plant_operator_rows": [],
        }

        payload = build_production_kpi_payload(result, days=0)

        self.assertEqual(payload["summary"]["average_production_revenue"], 7000)

    def test_kpi_payload_respects_inclusive_manual_date_range(self):
        result = {
            "status": "ok",
            "production_rows": [
                {"date": "2026-10-01", "production_revenue_today": 1000},
                {"date": "2026-10-02", "production_revenue_today": 2000},
                {"date": "2026-10-03", "production_revenue_today": 3000},
            ],
            "operator_rows": [],
            "plant_operator_rows": [],
        }

        payload = build_production_kpi_payload(
            result,
            period_start="2026-10-02",
            period_end="2026-10-03",
        )

        self.assertEqual([item["date"] for item in payload["rows"]], ["2026-10-02", "2026-10-03"])
        self.assertEqual(payload["summary"]["production_revenue"], 5000)
        self.assertEqual(payload["period_start"], "2026-10-02")
        self.assertEqual(payload["period_end"], "2026-10-03")


if __name__ == "__main__":
    unittest.main()
