import unittest
from concurrent.futures import ThreadPoolExecutor
import time
from unittest.mock import patch

import main
import crm


class ActionPlanRecentContactTests(unittest.TestCase):
    def setUp(self):
        main.clear_home_action_plan_cache()

    def tearDown(self):
        main.clear_home_action_plan_cache()

    def test_contact_ten_days_ago_is_excluded(self):
        outbound = [{"date_created": "recent", "crm_type": "Email"}]
        with patch.object(main, "get_crm_days_since_latest_activity", return_value=10):
            policy = main.build_action_plan_contact_policy(outbound, customer_name="Test Co")

        self.assertTrue(policy["exclude_from_action_plan"])
        self.assertIn("14 days", policy["hold_reason"])

    def test_newer_crm_activity_replaces_older_order_activity(self):
        customer = {
            "days_since_last_activity": 45,
            "last_activity_date": "2026-07-01",
        }

        main.apply_newer_crm_activity_to_customer(
            customer,
            {"date_created": "2026-09-01 10:30:00"},
            2,
        )

        self.assertEqual(customer["days_since_last_activity"], 2)
        self.assertEqual(customer["last_activity_date"], "2026-09-01")

    def test_recent_window_is_configurable(self):
        with patch.dict("os.environ", {"ACTION_PLAN_RECENT_CONTACT_DAYS": "10"}):
            self.assertTrue(main.has_recent_activity(10))
            self.assertFalse(main.has_recent_activity(11))

    def test_recent_crm_rows_are_merged_into_full_sync_cache(self):
        old_activity = {
            "customer_primary_key": "A1",
            "date_created": "2026-08-01 10:00:00",
            "subject": "Old",
        }
        recent_activity = {
            "customer_primary_key": "A1",
            "date_created": "2026-09-02 10:00:00",
            "subject": "Recent",
        }

        result = crm.merge_crm_activity_results(
            {"status": "ok", "activities": [old_activity], "counts": {}},
            {"status": "ok", "activities": [recent_activity]},
        )

        self.assertEqual(result["activities"][0]["subject"], "Recent")
        self.assertEqual(len(result["activity_map"]["A1"]), 2)

    def test_cached_crm_load_does_not_contact_filemaker(self):
        sync_result = {"status": "ok", "activities": [], "counts": {}}
        with patch.object(crm, "get_crm_data_source", return_value="filemaker"), patch.object(
            crm, "get_crm_cache_key", return_value="test-cache-key"
        ), patch.object(crm, "get_cached_crm_result", return_value=None), patch.object(
            crm, "get_filemaker_crm_use_sync_cache", return_value=True
        ), patch.object(
            crm, "read_filemaker_crm_sync_cache", return_value=sync_result
        ), patch.object(
            crm, "read_filemaker_crm_recent_cache", return_value=None
        ), patch.object(
            crm, "filter_inactive_filemaker_customer_activities", side_effect=lambda result, source: result
        ), patch.object(crm, "cache_crm_result"), patch.object(
            crm, "trigger_recent_crm_refresh"
        ) as trigger_refresh, patch.object(
            crm, "build_filemaker_crm_result"
        ) as synchronous_filemaker_fetch:
            result = crm.fetch_crm_activities()

        self.assertEqual(result, sync_result)
        synchronous_filemaker_fetch.assert_not_called()
        trigger_refresh.assert_not_called()

    def test_action_plan_workflow_is_reused_for_navigation(self):
        order_result = {
            "source": "filemaker",
            "status": "ok",
            "cache_updated_at": "orders-v1",
            "orders": [],
        }
        crm_result = {
            "source": "filemaker",
            "status": "ok",
            "cache_updated_at": "crm-v1",
            "counts": {"kept_rows": 0},
            "activity_map": {},
        }
        attention_result = {"late_customers": [], "dismissed_customers": []}

        with patch.object(
            main,
            "build_customers_needing_attention_response",
            return_value=attention_result,
        ) as build_attention, patch.object(
            main, "group_by_customer", return_value={}
        ) as group_orders, patch.object(
            main,
            "build_home_action_plan",
            return_value={"due_today": [], "dismissed": []},
        ) as build_plan, patch.object(
            main, "build_home_queue_summaries", return_value=[]
        ) as build_summaries:
            first = main.build_cached_home_action_plan_workflow(order_result, crm_result)
            second = main.build_cached_home_action_plan_workflow(order_result, crm_result)

        self.assertIs(first, second)
        build_attention.assert_called_once()
        group_orders.assert_called_once()
        build_plan.assert_called_once()
        build_summaries.assert_called_once()

    def test_concurrent_action_plan_requests_only_build_once(self):
        order_result = {
            "source": "filemaker",
            "status": "ok",
            "cache_updated_at": "orders-v2",
            "orders": [],
        }
        crm_result = {
            "source": "filemaker",
            "status": "ok",
            "cache_updated_at": "crm-v2",
            "counts": {"kept_rows": 0},
            "activity_map": {},
        }
        attention_result = {"late_customers": [], "dismissed_customers": []}

        def slow_attention(**_kwargs):
            time.sleep(0.05)
            return attention_result

        with patch.object(
            main,
            "build_customers_needing_attention_response",
            side_effect=slow_attention,
        ) as build_attention, patch.object(
            main, "group_by_customer", return_value={}
        ), patch.object(
            main,
            "build_home_action_plan",
            return_value={"due_today": [], "dismissed": []},
        ), patch.object(main, "build_home_queue_summaries", return_value=[]):
            with ThreadPoolExecutor(max_workers=2) as executor:
                results = list(executor.map(
                    lambda _index: main.build_cached_home_action_plan_workflow(
                        order_result,
                        crm_result,
                    ),
                    range(2),
                ))

        self.assertIs(results[0], results[1])
        build_attention.assert_called_once()


if __name__ == "__main__":
    unittest.main()
