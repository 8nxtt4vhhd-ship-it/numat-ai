import unittest
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import filemaker
import main
import crm
import data_sources


class CustomersViewTests(unittest.TestCase):
    def tearDown(self):
        filemaker.clear_filemaker_master_data_cache()
        data_sources.clear_filemaker_orders_cache()
        main.clear_customer_contextual_activity_cache()

    def test_customer_summaries_include_master_customer_without_orders(self):
        master_data = {
            "status": "ok",
            "customers_by_key": {
                "customer-1": {
                    "company": "No Order Customer",
                    "state": "OH",
                }
            },
        }

        summaries = main.build_customer_summaries([], {}, {}, master_data)

        self.assertEqual(len(summaries), 1)
        self.assertEqual(summaries[0]["customer"], "No Order Customer")
        self.assertEqual(summaries[0]["order_count"], 0)
        self.assertEqual(summaries[0]["state"], "OH")
        self.assertEqual(summaries[0]["last_order"], "")

    def test_last_activity_uses_only_sales_outreach(self):
        master_data = {
            "status": "ok",
            "customers_by_key": {
                "customer-1": {"company": "Example Customer", "state": "TX"}
            },
        }
        activities = {
            "customer-1": [
                {
                    "date_created": "2026-10-24 09:00:00",
                    "activity_category": "customer_services",
                },
                {
                    "date_created": "2026-10-20 09:00:00",
                    "activity_category": "sales_outreach",
                },
            ]
        }

        summary = main.build_customer_summaries([], {}, activities, master_data)[0]

        self.assertEqual(summary["display_last_activity"], datetime(2026, 10, 20, 9, 0))

    def test_branch_context_recovers_activity_mapped_to_sibling_customer(self):
        activity = {
            "date_created": "2026-03-30 16:00:06",
            "customer_primary_key": "forest-key",
            "customer_company": "SANICO (Forest, MS)",
            "subject": "Follow up for SANICO Cottondale",
            "body": "Checking in about the Cottondale mat repair order.",
            "sender_email": "sales@numatsystems.com",
            "to": "john@sanicomats.com",
        }
        activity_map = {"forest-key": [activity]}
        entries = [{
            "customer": "SANICO (Cottondale, AL)",
            "customer_primary_key": "cottondale-key",
            "orders": [],
        }]

        contextual = main.build_contextual_crm_activity_map(
            activity_map, [activity], entries
        )

        self.assertEqual(contextual["cottondale-key"], [activity])

    def test_unique_legacy_organization_history_follows_new_customer_key(self):
        activity = {
            "date_created": "2025-12-15 13:29:32",
            "customer_primary_key": "rockville-key",
            "customer_company": "Eagle Mat (Rockville, MD)",
            "subject": "thank you!",
            "body": "",
            "sender_email": "sales@numatsystems.com",
            "to": "mark1duarte@hotmail.com",
        }
        location_activity = {
            **activity,
            "date_created": "2025-04-01 10:00:00",
            "subject": "Delivery to Gaithersburg",
        }
        entries = [{
            "customer": "Eagle Mat (Gaithersburg MD)",
            "customer_primary_key": "gaithersburg-key",
            "orders": [],
        }]

        contextual = main.build_contextual_crm_activity_map(
            {"rockville-key": [activity, location_activity]},
            [activity, location_activity],
            entries,
        )

        self.assertEqual(contextual["gaithersburg-key"], [activity, location_activity])

    def test_branch_parser_accepts_city_state_without_comma(self):
        self.assertEqual(
            main.extract_customer_branch_parts("Eagle Mat (Gaithersburg MD)"),
            {"city": "Gaithersburg", "state": "MD"},
        )

    def test_branch_parser_accepts_location_followed_by_depot_code(self):
        self.assertEqual(
            main.extract_customer_branch_parts(
                "Vestis Uniform Services (Memphis, TN) MC 581"
            ),
            {"city": "Memphis", "state": "TN"},
        )
        self.assertEqual(
            main.get_customer_organization_label(
                "Vestis Uniform Services (Memphis, TN) MC 581"
            ),
            "Vestis Uniform Services",
        )

    def test_vestis_depot_activity_matches_named_vestis_branch(self):
        activity = {
            "date_created": "2024-08-15 14:10:03",
            "customer_primary_key": "ukiah-key",
            "customer_company": "Vestis Uniform Services / Depot (Ukiah, CA)",
            "subject": "Vestis Uniform Services (Novato, CA) Sales Order",
            "body": "Delivery to Novato",
            "sender_email": "customerservice@numatsystems.com",
            "to": "steve.chia@vestis.com",
        }
        entries = [{
            "customer": "Vestis Uniform Services (Novato, CA)",
            "customer_primary_key": "novato-key",
            "orders": [],
        }]

        contextual = main.build_contextual_crm_activity_map(
            {"ukiah-key": [activity]}, [activity], entries
        )

        self.assertEqual(contextual["novato-key"], [activity])

    def test_shared_contact_activity_is_indexed_to_every_customer_location(self):
        activity = {
            "date_created": "2026-10-20 09:00:00",
            "customer_primary_key": "branch-a",
            "sender_email": "sales@numatsystems.com",
            "to": "shared@example.com",
            "subject": "Checking in",
        }
        master_data = {
            "customers_by_key": {"branch-a": {}, "branch-b": {}},
            "all_contacts": [
                {"email": "shared@example.com", "customer_ref": "branch-a"},
                {"email": "SHARED@example.com", "customer_ref": "branch-b"},
            ],
        }

        activity_map = crm.build_shared_contact_activity_map([activity], master_data)

        self.assertEqual(activity_map["branch-a"], [activity])
        self.assertEqual(activity_map["branch-b"], [activity])

    def test_contact_activity_follows_directory_location_when_crm_uses_another_key(self):
        activity = {
            "date_created": "2026-10-01 20:42:24",
            "customer_primary_key": "memphis-276",
            "sender_email": "sales@numatsystems.com",
            "to": "pam.martin@vestis.com",
            "subject": "NuMat pricing update",
        }
        master_data = {
            "customers_by_key": {"memphis-mc581": {}},
            "all_contacts": [{
                "email": "pam.martin@vestis.com",
                "customer_ref": "memphis-mc581",
            }],
        }

        activity_map = crm.build_shared_contact_activity_map([activity], master_data)

        self.assertEqual(activity_map["memphis-276"], [activity])
        self.assertEqual(activity_map["memphis-mc581"], [activity])

    def test_inactive_source_activity_is_kept_for_contact_at_active_location(self):
        activity = {
            "date_created": "2026-10-01 20:42:24",
            "customer_primary_key": "inactive-memphis-276",
            "customer_company": "Vestis Uniform Services (Memphis, TN) 276",
            "sender_email": "sales@numatsystems.com",
            "to": "pam.martin@vestis.com",
            "subject": "NuMat pricing update",
        }
        result = {
            "status": "ok",
            "activities": [activity],
            "activity_map": {"inactive-memphis-276": [activity]},
        }
        master_data = {
            "status": "ok",
            "customers_by_key": {"active-memphis-mc581": {"company": "MC 581"}},
            "inactive_customer_keys": ["inactive-memphis-276"],
            "inactive_customer_names": ["vestis uniform services (memphis, tn) 276"],
            "all_contacts": [{
                "email": "pam.martin@vestis.com",
                "customer_ref": "active-memphis-mc581",
                "active": "Active",
            }],
        }

        with patch.object(crm, "fetch_filemaker_master_data", return_value=master_data):
            filtered = crm.filter_inactive_filemaker_customer_activities(result, "filemaker")

        self.assertEqual(filtered["activities"], [activity])
        self.assertEqual(filtered["activity_map"]["active-memphis-mc581"], [activity])

    def test_customer_crm_timeline_reset_clears_category_and_direction(self):
        controls = main.render_customer_crm_timeline_controls(
            "Vestis Uniform Services (Memphis, TN) MC 581",
            "outbound",
            "customer_services",
            24,
            "order_date",
            "desc",
        )

        reset_href = controls.split('class="button secondary" href="', 1)[1].split('"', 1)[0]
        self.assertNotIn("crm_category", reset_href)
        self.assertNotIn("crm_direction", reset_href)
        self.assertIn("crm_limit=24", reset_href)

    def test_customer_without_orders_still_renders_crm_timeline(self):
        activity = {
            "date_created": "2026-10-20 09:00:00",
            "customer_primary_key": "other-location-key",
            "customer_company": "Other Location",
            "sender_email": "sales@numatsystems.com",
            "to": "contact@example.com",
            "subject": "Checking in",
            "body": "Hello",
            "direction": "outbound",
            "activity_category": "sales_outreach",
        }
        order_result = {"status": "ok", "source": "filemaker", "orders": []}
        crm_result = {
            "status": "ok",
            "source": "filemaker_sync_cache",
            "activities": [activity],
            # Normal CRM cache creation has already indexed a shared contact
            # against every active customer location.
            "activity_map": {
                "other-location-key": [activity],
                "customer-key": [activity],
            },
        }
        master_result = {
            "status": "ok",
            "customers_by_key": {
                "customer-key": {
                    "primary_key": "customer-key",
                    "company": "No Order Customer",
                }
            },
        }
        contact_directory = {
            **master_result,
            "all_contacts": [{
                "email": "contact@example.com",
                "customer_ref": "customer-key",
            }],
        }
        with (
            patch.object(main, "get_orders_for_analysis", return_value=order_result),
            patch.object(main, "fetch_crm_activities", return_value=crm_result),
            patch.object(main, "fetch_filemaker_customer_data", return_value=master_result),
            patch.object(main, "fetch_filemaker_master_data", return_value=contact_directory) as full_directory,
            patch.object(main, "build_customers_needing_attention_response_map", return_value={}),
            patch.object(main, "build_customer_enrichment_href", return_value="/enrichment-view"),
        ):
            response = main.get_customer_view("No Order Customer")

        html = response
        self.assertIn("No order history is linked to this customer yet", html)
        self.assertIn("Checking in", html)
        self.assertIn("<strong>1</strong>", html)
        full_directory.assert_not_called()

    def test_stale_sales_outreach_recommends_reaching_out(self):
        latest_activity = {
            "date_created": "2026-10-23 08:34:00",
            "activity_category": "customer_services",
        }
        latest_sales = {
            "date_created": "2025-11-18 09:00:00",
            "activity_category": "sales_outreach",
        }

        recommendation = main.build_contact_recommendation(
            customer="Example Customer",
            attention=None,
            latest_crm_activity=latest_activity,
            latest_sales_crm_activity=latest_sales,
            crm_activities=[latest_activity, latest_sales],
            last_order="2026-06-02",
            average_gap=190,
            last_activity=None,
        )

        self.assertEqual(recommendation["priority_label"], "Reach out")
        self.assertEqual(recommendation["next_step"], "Send a tailored sales check-in")
        self.assertIn("latest sales outreach 11/18/2025", recommendation["crm_summary"])
        self.assertNotIn("Use the CRM thread", recommendation["next_step"])

    def test_customer_dates_and_controls_render_in_requested_format(self):
        row = main.render_customer_summary_row({
            "customer": "Example Customer",
            "order_count": 1,
            "crm_count": 2,
            "state": "OH",
            "last_order": "2026-10-21",
            "avg_cycle": None,
            "cycle_pattern_label": "Not enough orders",
            "total_value": 100,
            "avg_value": 100,
            "display_last_activity": datetime(2026, 10, 22),
        })
        header = main.render_customer_sort_header(
            "Customer", "customer", "Acme", "OH", "customer", "asc"
        )
        pager = main.render_customers_pagination(
            "Acme", "OH", "customer", "asc", 2, 3, 120, 50, 50
        )

        self.assertIn("10/21/2026", row)
        self.assertIn("10/22/2026", row)
        self.assertIn("Customer ↑", header)
        self.assertIn("Showing 51 to 100 of 120 customers", pager)
        self.assertIn("Page 2 of 3", pager)

    def test_customer_name_remains_linked_without_orders(self):
        row = main.render_customer_summary_row({
            "customer": "No Order Customer",
            "order_count": 0,
            "crm_count": 0,
            "state": "OH",
            "last_order": "",
            "avg_cycle": None,
            "cycle_pattern_label": "No orders",
            "total_value": 0,
            "avg_value": 0,
            "display_last_activity": None,
        })

        self.assertIn('/customer-view?customer=No%20Order%20Customer', row)

    def test_customer_directory_cache_remains_until_forced_refresh(self):
        config = {"customers_layout": "Customers"}
        records = {
            "status": "ok",
            "records": [{"recordId": "1", "fieldData": {}}],
        }
        customer = {
            "primary_key": "customer-1",
            "company": "Cached Customer",
            "state": "OH",
            "type": "Customer",
        }
        with (
            patch.object(filemaker, "get_filemaker_config", return_value=config),
            patch.object(filemaker, "build_filemaker_master_data_cache_key", return_value=("cache-key",)),
            patch.object(filemaker, "fetch_all_layout_records", return_value=records) as fetch_records,
            patch.object(filemaker, "map_filemaker_customer_master_record", return_value=customer),
            patch.object(filemaker, "is_inactive_filemaker_customer", return_value=False),
        ):
            first = filemaker.fetch_filemaker_customer_data()
            second = filemaker.fetch_filemaker_customer_data()
            refreshed = filemaker.fetch_filemaker_customer_data(force_refresh=True)

        self.assertIs(first, second)
        self.assertEqual(refreshed["customers_by_key"]["customer-1"]["company"], "Cached Customer")
        self.assertEqual(fetch_records.call_count, 2)

    def test_full_filemaker_directory_cache_survives_process_memory_reset(self):
        cache_key = ("server", "database", "customers", "contacts")
        result = {
            "status": "ok",
            "customers_by_key": {"customer-1": {"company": "Cached Customer"}},
            "all_contacts": [{"email": "contact@example.com"}],
        }
        with TemporaryDirectory() as temp_dir:
            cache_path = Path(temp_dir) / "master.json"
            with patch.object(filemaker, "get_filemaker_master_cache_path", return_value=cache_path):
                filemaker.write_filemaker_master_data_disk_cache(cache_key, result)
                loaded = filemaker.read_filemaker_master_data_disk_cache(cache_key)

        self.assertEqual(loaded, result)

    def test_order_cache_remains_until_explicitly_cleared(self):
        result = {"status": "ok", "orders": [{"order_number": "1"}]}
        with (
            patch.object(data_sources, "get_filemaker_order_limit", return_value=1000),
            patch.object(data_sources, "get_filemaker_cache_seconds", return_value=120),
            patch.object(data_sources, "build_filemaker_cache_key", return_value=("orders",)),
            patch.object(data_sources, "read_filemaker_orders_cache", return_value=None),
            patch.object(data_sources, "fetch_order_records", return_value=result),
            patch.object(data_sources, "write_filemaker_orders_cache"),
        ):
            loaded = data_sources.fetch_cached_filemaker_orders()

        self.assertIs(loaded, result)
        self.assertEqual(data_sources._FILEMAKER_ORDER_CACHE["expires_at"], float("inf"))

    def test_order_snapshot_is_used_after_process_cache_reset(self):
        snapshot = {
            "updated_at": "2026-10-30 08:00:00",
            "orders": [{"order_number": "1"}],
        }
        with (
            patch.object(data_sources, "get_filemaker_order_limit", return_value=1000),
            patch.object(data_sources, "get_filemaker_cache_seconds", return_value=120),
            patch.object(data_sources, "build_filemaker_cache_key", return_value=("orders",)),
            patch.object(data_sources, "read_filemaker_orders_cache", return_value=snapshot),
            patch.object(data_sources, "fetch_order_records") as fetch,
        ):
            loaded = data_sources.fetch_cached_filemaker_orders()

        self.assertEqual(loaded["orders"], snapshot["orders"])
        self.assertEqual(loaded["cache_updated_at"], snapshot["updated_at"])
        fetch.assert_not_called()

    def test_customer_sync_redirects_back_with_confirmation(self):
        result = {"status": "ok", "customers_by_key": {"one": {}, "two": {}}}
        with (
            patch.object(main, "clear_filemaker_orders_cache") as clear_orders,
            patch.object(main, "get_orders_for_analysis", return_value={"status": "ok", "orders": []}) as sync_orders,
            patch.object(main, "clear_filemaker_master_data_cache") as clear_master,
            patch.object(main, "fetch_filemaker_customer_data", return_value=result) as sync,
            patch.object(main, "get_crm_data_source", return_value="filemaker"),
            patch.object(main, "sync_filemaker_crm_recent_cache", return_value={"status": "ok"}) as sync_crm,
            patch.object(main, "clear_customer_summaries_cache") as clear_summaries,
        ):
            response = main.post_customers_view_sync("Acme", "OH", "customer", "asc", 2)

        self.assertEqual(response.status_code, 303)
        self.assertIn("Customer+and+CRM+data+synced", response.headers["location"])
        clear_orders.assert_called_once_with(clear_persisted=True)
        sync_orders.assert_called_once_with()
        clear_master.assert_called_once_with(clear_persisted=True)
        sync.assert_called_once_with(force_refresh=True)
        sync_crm.assert_called_once_with()
        clear_summaries.assert_called_once_with()

    def test_customer_sync_reports_a_partial_crm_failure(self):
        result = {"status": "ok", "customers_by_key": {"one": {}}}
        with (
            patch.object(main, "clear_filemaker_orders_cache"),
            patch.object(main, "get_orders_for_analysis", return_value={"status": "ok", "orders": []}),
            patch.object(main, "clear_filemaker_master_data_cache"),
            patch.object(main, "fetch_filemaker_customer_data", return_value=result),
            patch.object(main, "get_crm_data_source", return_value="filemaker"),
            patch.object(main, "sync_filemaker_crm_recent_cache", return_value={"status": "connection_error"}),
            patch.object(main, "clear_customer_summaries_cache"),
        ):
            response = main.post_customers_view_sync("", "", "last_order", "desc", 1)

        self.assertIn("Synced+order+data+and+customer+data", response.headers["location"])
        self.assertIn("CRM+data+%28connection_error%29", response.headers["location"])

    def test_manual_crm_sync_uses_a_broad_recent_overlay(self):
        records = [{"fieldData": {"Date Created": "10/01/2026 20:42:24"}}]
        with (
            patch.object(crm, "get_filemaker_crm_limit", return_value=500),
            patch.dict("os.environ", {"FILEMAKER_CRM_MANUAL_SYNC_LIMIT": "7500"}),
            patch.object(crm, "get_filemaker_emails_layout", return_value="emails"),
            patch.object(crm, "fetch_layout_records", return_value={"status": "ok", "records": records}) as fetch,
            patch.object(crm, "build_crm_result_from_rows", return_value={"status": "ok"}) as build,
            patch.object(crm, "write_filemaker_crm_recent_cache") as write,
        ):
            result = crm.sync_filemaker_crm_recent_cache()

        self.assertEqual(result["status"], "ok")
        fetch.assert_called_once_with(
            "emails",
            limit=500,
            offset=1,
            sort_fields=[{"fieldName": "Date Created", "sortOrder": "descend"}],
        )
        build.assert_called_once_with(
            [records[0]["fieldData"]], source="filemaker", path="emails"
        )
        write.assert_called_once_with(result)

    def test_recent_refresh_merges_instead_of_dropping_older_activity(self):
        older = {
            "date_created": "2026-09-03 14:30:05",
            "subject": "Older history",
            "sender_email": "sales@numatsystems.com",
            "to": "contact@example.com",
            "customer_primary_key": "customer-1",
            "direction": "outbound",
        }
        newer = {**older, "date_created": "2026-10-20 13:30:03", "subject": "Newest page"}
        with self.subTest("cache merge"):
            import json
            from tempfile import TemporaryDirectory
            from pathlib import Path
            with TemporaryDirectory() as temp_dir:
                cache_path = Path(temp_dir) / "recent.json"
                cache_path.write_text(json.dumps({"activities": [older]}), encoding="utf-8")
                with (
                    patch.object(crm, "get_filemaker_crm_recent_cache_path", return_value=cache_path),
                    patch.object(crm, "clear_crm_cache") as clear_cache,
                ):
                    crm.write_filemaker_crm_recent_cache({"path": "emails", "activities": [newer]})
                payload = json.loads(cache_path.read_text(encoding="utf-8"))

        self.assertEqual([item["subject"] for item in payload["activities"]], ["Newest page", "Older history"])
        clear_cache.assert_called_once_with()

    def test_cached_crm_result_reuses_prebuilt_shared_contact_index(self):
        cached = {
            "status": "ok",
            "activities": [{"subject": "Already indexed"}],
            "activity_map": {"customer-1": [{"subject": "Already indexed"}]},
        }
        with (
            patch.object(crm, "get_crm_data_source", return_value="filemaker"),
            patch.object(crm, "get_crm_cache_key", return_value=("filemaker", "cache")),
            patch.object(crm, "get_cached_crm_result", return_value=cached),
            patch.object(crm, "filter_inactive_filemaker_customer_activities") as rebuild,
        ):
            result = crm.fetch_crm_activities()

        self.assertIs(result, cached)
        rebuild.assert_not_called()

    def test_customer_detail_contextual_activity_fallback_is_reused(self):
        customer_orders = [
            {"customer": "Example", "customer_primary_key": "customer-1"},
        ]
        activity = {
            "customer_primary_key": "legacy-key",
            "customer_company": "Example",
            "date_created": "2026-10-20 09:00:00",
        }
        crm_result = {
            "status": "ok",
            "activities": [activity],
            "activity_map": {"legacy-key": [activity]},
        }

        with patch.object(
            main,
            "build_contextual_crm_activity_map",
            wraps=main.build_contextual_crm_activity_map,
        ) as build_contextual:
            first = main.get_cached_customer_detail_crm_activities(
                crm_result, "Example", "customer-1", customer_orders
            )
            second = main.get_cached_customer_detail_crm_activities(
                crm_result, "Example", "customer-1", customer_orders
            )

        self.assertIs(first, second)
        self.assertEqual(first, (activity,))
        build_contextual.assert_called_once()

    def test_customer_detail_uses_prebuilt_crm_index_without_contextual_scan(self):
        activity = {"customer_primary_key": "customer-1"}
        crm_result = {
            "status": "ok",
            "activities": [activity],
            "activity_map": {"customer-1": [activity]},
        }

        with patch.object(main, "build_contextual_crm_activity_map") as contextual:
            result = main.get_cached_customer_detail_crm_activities(
                crm_result, "Example", "customer-1", []
            )

        self.assertEqual(result, [activity])
        contextual.assert_not_called()


if __name__ == "__main__":
    unittest.main()
