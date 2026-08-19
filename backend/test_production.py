import unittest

from production import build_production_kpi_payload


class ProductionKpiTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
