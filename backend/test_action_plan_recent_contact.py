import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
import time
from unittest.mock import patch

import main
import crm


class ActionPlanRecentContactTests(unittest.TestCase):
    def setUp(self):
        main.clear_home_action_plan_cache()

    def tearDown(self):
        main.clear_home_action_plan_cache()

    def test_action_plan_location_prefers_customer_master_city_over_order_territory(self):
        orders = [{
            "customer": "Matman Designs",
            "customer_primary_key": "matman-key",
            "order_date": "2026-09-01",
            "extra": {"ai_Territory": "West", "ai_State": "GA"},
        }]
        master_data = {
            "status": "ok",
            "customers_by_key": {
                "matman-key": {"company": "Matman Designs", "city": "LaGrange", "state": "GA"},
            },
            "contacts_by_customer_key": {},
            "contacts_by_email": {},
            "unsubscribed_emails": [],
        }

        summaries = main.build_home_queue_summaries(
            [{"customer": "Matman Designs", "priority_score": 2}],
            {"Matman Designs": orders},
            {},
            master_data_result=master_data,
        )

        self.assertEqual(summaries[0]["location"], "LaGrange, GA")

    def test_account_overview_name_links_to_customer_and_back_to_selected_action_plan(self):
        return_to = "/action-plan-view?selected_customer=Cintas+%28Round+Rock%2C+TX%29#focus-preview"
        preview = {
            "customer": "Cintas (Round Rock, TX)",
            "location": "Round Rock, TX",
            "mode": "Email",
            "queue_summary": {},
            "context": {"primary_contact": {}},
            "result": {},
            "message_points": [],
        }

        with (
            patch.object(main, "build_home_focus_recipient_options", return_value=[]),
            patch.object(main, "get_m365_connection_state", return_value={"connected": False}),
        ):
            html = main.render_home_focus_work_panel(
                preview,
                "/outreach-prep?customer=Cintas",
                return_to,
            )

        self.assertIn('class="home-focus-customer-link"', html)
        self.assertIn("customer=Cintas%20%28Round%20Rock%2C%20TX%29", html)
        self.assertIn("return_to=%2Faction-plan-view%3Fselected_customer%3DCintas%2B%2528Round%2BRock%252C%2BTX%2529%23focus-preview", html)

    def test_generated_outreach_draft_is_saved_per_user_and_customer(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "outreach_drafts.json"
            with patch.object(main, "get_outreach_drafts_path", return_value=path):
                main.save_outreach_draft("kelly", "Customer A", "Subject A", "Body A")
                saved = main.get_saved_outreach_draft("kelly", "Customer A")
                other_user = main.get_saved_outreach_draft("peter", "Customer A")

        self.assertEqual(saved["email_subject"], "Subject A")
        self.assertEqual(saved["email_body"], "Body A")
        self.assertEqual(other_user, {})

    def test_customer_lookup_replaces_territory_fallback_with_master_city(self):
        orders = [{
            "customer": "Matman Designs",
            "customer_primary_key": "matman-key",
            "order_date": "2026-09-01",
            "extra": {"ai_Territory": "West", "ai_State": "GA"},
        }]
        entries = main.build_customer_lookup_entries(
            {"Matman Designs": orders},
            master_data_result={
                "status": "ok",
                "customers_by_key": {
                    "matman-key": {"company": "Matman Designs", "city": "LaGrange", "state": "GA"},
                },
            },
        )

        self.assertEqual(entries[0]["location"], "LaGrange, GA")

    def test_action_plan_does_not_offer_contact_at_inactive_old_branch(self):
        activity = {
            "direction": "outbound",
            "to": "bauschm@cintas.com",
            "date_created": "2026-10-01 10:00:00",
            "subject": "Follow up",
        }
        active_lawrenceville_contact = {
            "name": "Matt Bausch",
            "email": "bauschm@cintas.com",
            "active": "Active",
            "customer_ref": "lawrenceville-key",
        }

        contacts = main.build_outreach_contact_signals(
            [activity],
            contacts_by_email={"bauschm@cintas.com": active_lawrenceville_contact},
            customer_contacts=[],
        )

        self.assertEqual(contacts, [])

    def test_contact_ten_days_ago_is_excluded(self):
        outbound = [{"date_created": "recent", "crm_type": "Email"}]
        with patch.object(main, "get_crm_days_since_latest_activity", return_value=10):
            policy = main.build_action_plan_contact_policy(outbound, customer_name="Test Co")

        self.assertTrue(policy["exclude_from_action_plan"])
        self.assertIn("14 days", policy["hold_reason"])
        self.assertNotEqual(policy["mode_override"], "Hold")

    def test_recent_contact_never_renders_as_hold_follow_up(self):
        mode = main.determine_home_queue_mode(
            {"customer": "Eagle Mat (Rockville, MD)", "days_since_last_activity": 9},
            outbound_count=1,
            inbound_count=0,
            contact_policy={
                "exclude_from_action_plan": True,
                "why_override": "Recent contact logged 9 days ago",
            },
        )

        self.assertEqual(mode, "Email")
        self.assertNotEqual(main.get_home_next_action_label(mode), "Review Context")

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
        with patch.object(
            main,
            "build_contact_date_action_plan_customers",
            return_value=([], []),
        ) as build_candidates, patch.object(
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
        build_candidates.assert_called_once()
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
        def slow_candidates(*_args, **_kwargs):
            time.sleep(0.05)
            return [], []

        with patch.object(
            main,
            "build_contact_date_action_plan_customers",
            side_effect=slow_candidates,
        ) as build_candidates, patch.object(
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
        build_candidates.assert_called_once()

    def test_contact_date_queue_uses_more_than_four_weeks(self):
        grouped_orders = {
            "Due Co": [{"customer": "Due Co", "customer_primary_key": "due", "order_date": "2026-01-01"}],
            "Not Yet Co": [{"customer": "Not Yet Co", "customer_primary_key": "not-yet", "order_date": "2026-01-01"}],
        }
        activity_map = {
            "due": [{"date_created": "2026-10-08 09:00:00", "direction": "outbound", "crm_type": "Email"}],
            "not-yet": [{"date_created": "2026-10-09 09:00:00", "direction": "outbound", "crm_type": "Email"}],
        }
        with patch.object(main, "read_action_plan_dismissals", return_value={}):
            active, _dismissed = main.build_contact_date_action_plan_customers(
                grouped_orders,
                activity_map,
                today=main.datetime(2026, 11, 6),
                follow_ups={},
            )

        self.assertEqual([item["customer"] for item in active], ["Due Co"])
        self.assertEqual(active[0]["days_since_last_contact"], 29)

    def test_scheduled_date_overrides_four_week_rule(self):
        grouped_orders = {
            "Scheduled Co": [{"customer": "Scheduled Co", "customer_primary_key": "scheduled", "order_date": "2026-01-01"}],
        }
        activity_map = {
            "scheduled": [{"date_created": "2026-08-01 09:00:00", "direction": "outbound", "crm_type": "Call"}],
        }
        follow_ups = {"Scheduled Co": {"follow_up_date": "2026-12-01"}}
        with patch.object(main, "read_action_plan_dismissals", return_value={}):
            active, _dismissed = main.build_contact_date_action_plan_customers(
                grouped_orders,
                activity_map,
                today=main.datetime(2026, 11, 6),
                follow_ups=follow_ups,
            )

        self.assertEqual(active, [])

    def test_master_customer_without_orders_is_included(self):
        grouped = main.include_master_customers_in_action_plan(
            {},
            {
                "customers_by_key": {
                    "customer-key": {"company": "Never Ordered Co", "type": "Customer"},
                    "supplier-key": {"company": "Supplier Co", "type": "Supplier"},
                }
            },
        )

        self.assertIn("Never Ordered Co", grouped)
        self.assertNotIn("Supplier Co", grouped)
        self.assertEqual(grouped["Never Ordered Co"][0]["customer_primary_key"], "customer-key")

    def test_order_backed_non_customer_is_removed_before_action_plan(self):
        order_result = {
            "source": "filemaker",
            "status": "ok",
            "orders": [
                {"customer": "Real Customer", "customer_primary_key": "customer-key"},
                {"customer": "Kleen-Tex (LaGrange, GA)", "customer_primary_key": "other-key"},
            ],
        }
        master_data = {
            "status": "ok",
            "companies": [
                {"primary_key": "customer-key", "type": "Customer"},
                {"primary_key": "other-key", "type": "Other"},
            ],
        }

        filtered = main.filter_order_result_to_customer_type(order_result, master_data)

        self.assertEqual([order["customer"] for order in filtered["orders"]], ["Real Customer"])

    def test_live_action_plan_is_limited_to_ten_customers(self):
        attention = [
            {
                "customer": f"Customer {index:02d}",
                "last_contact_date": f"2026-01-{index + 1:02d}",
                "priority_score": 100 - index,
            }
            for index in range(12)
        ]

        plan = main.build_home_action_plan(attention, {}, {})

        self.assertEqual(len(plan["due_today"]), 10)
        self.assertEqual(plan["due_today"][0]["customer"], "Customer 00")

    def test_manual_add_form_renders_every_supplied_customer(self):
        customers = [f"Customer {index}" for index in range(25)]

        html = main.render_home_add_customer_form(customers)

        self.assertEqual(html.count("<option value="), 25)


if __name__ == "__main__":
    unittest.main()
