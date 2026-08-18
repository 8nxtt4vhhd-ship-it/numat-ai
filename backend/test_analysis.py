import os
import unittest
from datetime import datetime
from unittest.mock import patch

from analysis import find_late_customers


class LimitedHistoryActionPlanTests(unittest.TestCase):
    def test_one_order_customer_is_included_after_follow_up_threshold(self):
        customers = {
            "One Order Customer": [
                {"customer": "One Order Customer", "order_date": "2026-01-01", "amount": 500},
            ],
        }
        with patch.dict(os.environ, {"ACTION_PLAN_LIMITED_HISTORY_FOLLOW_UP_DAYS": "60"}):
            results = find_late_customers(customers, today=datetime(2026, 3, 2))

        self.assertEqual(len(results), 1)
        self.assertTrue(results[0]["limited_history"])
        self.assertEqual(results[0]["cycle_pattern"], "limited_history")
        self.assertGreaterEqual(results[0]["priority_score"], 1.5)

    def test_recent_limited_history_customer_is_not_contacted_too_soon(self):
        customers = {
            "Recent Customer": [
                {"customer": "Recent Customer", "order_date": "2026-02-15", "amount": 500},
                {"customer": "Recent Customer", "order_date": "2026-02-20", "amount": 600},
            ],
        }
        with patch.dict(os.environ, {"ACTION_PLAN_LIMITED_HISTORY_FOLLOW_UP_DAYS": "60"}):
            results = find_late_customers(customers, today=datetime(2026, 3, 2))

        self.assertEqual(results, [])

    def test_threshold_is_configurable(self):
        customers = {
            "Two Order Customer": [
                {"customer": "Two Order Customer", "order_date": "2026-01-01", "amount": 100},
                {"customer": "Two Order Customer", "order_date": "2026-02-01", "amount": 100},
            ],
        }
        with patch.dict(os.environ, {"ACTION_PLAN_LIMITED_HISTORY_FOLLOW_UP_DAYS": "30"}):
            results = find_late_customers(customers, today=datetime(2026, 3, 3))

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["follow_up_threshold_days"], 30)


if __name__ == "__main__":
    unittest.main()
