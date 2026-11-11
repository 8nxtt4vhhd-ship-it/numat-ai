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
        main.clear_customer_summaries_cache()
        main.clear_customer_contextual_activity_cache()

    def test_customer_search_reuses_built_directory_without_reloading_sources(self):
        summaries = [{"customer": "Vestis Uniform Services", "state": "IL"}]
        sources = (
            {"status": "ok", "source": "filemaker", "orders": []},
            {"status": "ok", "source": "filemaker_sync_cache", "activities": []},
            {"status": "ok", "source": "filemaker", "customers_by_key": {}},
        )
        with (
            patch.object(main, "get_cached_customer_directory", return_value=(summaries, sources)),
            patch.object(main, "get_orders_for_analysis") as orders,
            patch.object(main, "fetch_crm_activities") as crm_activities,
            patch.object(main, "fetch_filemaker_customer_data") as customer_data,
            patch.object(main, "render_customer_summary_row", return_value="<tr><td>Vestis Uniform Services</td></tr>"),
        ):
            html = main.get_customers_view(customer="Vestis")

        self.assertIn("Vestis Uniform Services", html)
        orders.assert_not_called()
        crm_activities.assert_not_called()
        customer_data.assert_not_called()

    def test_customers_page_skips_unused_attention_calculation(self):
        order_result = {
            "source": "filemaker",
            "status": "ok",
            "orders": [],
        }
        crm_result = {
            "source": "filemaker_sync_cache",
            "status": "ok",
            "activities": [],
            "activity_map": {},
            "counts": {},
        }
        master_result = {
            "source": "filemaker",
            "status": "ok",
            "companies": [],
            "customers_by_key": {},
        }

        with (
            patch.object(main, "get_orders_for_analysis", return_value=order_result),
            patch.object(main, "fetch_crm_activities", return_value=crm_result),
            patch.object(main, "fetch_filemaker_customer_data", return_value=master_result),
            patch.object(
                main,
                "build_customers_needing_attention_response",
                side_effect=AssertionError("attention should not be built for the directory"),
            ),
            patch.object(main, "build_cached_customer_summaries", return_value=[]) as summaries,
        ):
            html = main.get_customers_view()

        self.assertIn("No customers match the current filters", html)
        self.assertEqual(
            summaries.call_args.kwargs["attention_result"],
            {"status": "ok", "late_customers": []},
        )

    def test_startup_prewarms_exact_customer_directory_summary(self):
        order_result = {
            "source": "filemaker",
            "status": "ok",
            "orders": [
                {"customer": "Customer", "extra": {"Customer Ref": "customer-key"}},
                {"customer": "Corporate", "extra": {"Customer Ref": "corporate-key"}},
            ],
        }
        crm_result = {
            "source": "filemaker_sync_cache",
            "status": "ok",
            "activities": [],
            "activity_map": {},
            "counts": {},
        }
        customer = {"primary_key": "customer-key", "company": "Customer", "type": "Customer"}
        corporate = {"primary_key": "corporate-key", "company": "Corporate", "type": "Corporate"}
        customer_data = {
            "source": "filemaker",
            "status": "ok",
            "companies": [customer, corporate],
            "customers_by_key": {"customer-key": customer, "corporate-key": corporate},
        }

        with (
            patch.object(main, "get_orders_for_analysis", return_value=order_result),
            patch.object(main, "fetch_crm_activities", return_value=crm_result),
            patch.object(main, "fetch_filemaker_customer_data", return_value=customer_data),
            patch.object(main, "fetch_filemaker_master_data", return_value=customer_data),
            patch.object(main, "load_strategic_contacts", return_value=[]),
            patch.object(main, "build_strategic_contact_workspace_maps"),
            patch.object(main, "build_cached_customer_summaries", return_value=[]) as summaries,
        ):
            main.prewarm_app_caches()

        warmed_orders = summaries.call_args.kwargs["order_result"]["orders"]
        warmed_master = summaries.call_args.kwargs["master_data_result"]
        self.assertEqual([item["customer"] for item in warmed_orders], ["Customer"])
        self.assertEqual(list(warmed_master["customers_by_key"]), ["customer-key"])
        self.assertEqual(
            summaries.call_args.kwargs["attention_result"],
            {"status": "ok", "late_customers": []},
        )

    def test_filemaker_sales_views_only_keep_type_customer_orders(self):
        order_result = {
            "source": "filemaker",
            "status": "ok",
            "orders": [
                {"customer": "Real Customer", "extra": {"Customer Ref": "customer-key"}},
                {"customer": "Corporate Parent", "extra": {"Customer Ref": "corporate-key"}},
                {"customer": "Other Record", "extra": {"Customer Ref": "other-key"}},
            ],
        }
        master_data = {
            "status": "ok",
            "companies": [
                {"primary_key": "customer-key", "type": "Customer"},
                {"primary_key": "corporate-key", "type": "Corporate"},
                {"primary_key": "other-key", "type": "Other"},
            ],
        }

        filtered = main.filter_order_result_to_customer_type(order_result, master_data)

        self.assertEqual([order["customer"] for order in filtered["orders"]], ["Real Customer"])
        self.assertEqual(len(order_result["orders"]), 3)

    def test_customers_page_master_data_only_keeps_type_customer(self):
        customer = {"primary_key": "customer-key", "type": "Customer"}
        corporate = {"primary_key": "corporate-key", "type": "Corporate"}
        other = {"primary_key": "other-key", "type": "Other"}
        master_data = {
            "status": "ok",
            "companies": [customer, corporate, other],
            "customers_by_key": {
                "customer-key": customer,
                "corporate-key": corporate,
            },
        }

        filtered = main.filter_master_data_to_customer_type(master_data)

        self.assertEqual(filtered["companies"], [customer])
        self.assertEqual(filtered["customers_by_key"], {"customer-key": customer})

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

    def test_customer_summary_uses_recent_sales_focus_email_before_crm_sync(self):
        master_data = {
            "status": "ok",
            "customers_by_key": {
                "customer-1": {"company": "Example Customer", "state": "TX"}
            },
        }
        recent_send = {
            "customer": "Example Customer",
            "sender_email": "sales@numatsystems.com",
            "recipient": "buyer@example.com",
            "subject": "Checking in",
            "body_preview": "A recent Sales Focus email.",
            "sent_at": "2026-11-09 10:15:00",
            "mode": "email",
        }

        with patch.object(main, "read_recent_sent_emails", return_value=[recent_send]):
            summary = main.build_customer_summaries([], {}, {}, master_data)[0]

        self.assertEqual(summary["display_last_activity"], datetime(2026, 11, 9, 10, 15))
        self.assertEqual(summary["crm_count"], 1)

    def test_action_plan_uses_recent_sales_focus_email_before_crm_sync(self):
        order = {
            "customer": "Example Customer",
            "order_date": "2025-01-01",
            "extra": {"Customer Ref": "customer-1"},
        }
        order_result = {"source": "cache", "status": "ok", "orders": [order]}
        crm_result = {
            "source": "filemaker_sync_cache",
            "status": "ok",
            "activities": [],
            "activity_map": {},
            "counts": {},
        }
        recent_send = {
            "customer": "Example Customer",
            "sender_email": "sales@numatsystems.com",
            "recipient": "buyer@example.com",
            "subject": "Checking in",
            "body_preview": "A recent Sales Focus email.",
            "sent_at": "2026-11-09 10:15:00",
            "mode": "email",
        }
        late_customer = {
            "customer": "Example Customer",
            "avg_gap": 30,
            "days_since_last": 600,
            "priority_score": 80,
            "action": "Reach out",
            "days_since_last_activity": None,
            "last_activity_content": "",
        }
        main.clear_attention_response_cache()
        with (
            patch.object(main, "find_late_customers", return_value=[late_customer]),
            patch.object(main, "read_recent_sent_emails", return_value=[recent_send]),
            patch.object(main, "read_action_plan_dismissals", return_value={}),
            patch.object(main, "get_forced_action_plan_customers", return_value=[]),
            patch.object(main, "should_add_ai_explanations", return_value=False),
            patch.object(main, "get_analysis_today", return_value=datetime(2026, 11, 9, 12, 0)),
        ):
            result = main.build_customers_needing_attention_response(order_result, crm_result)

        customer = result["late_customers"][0]
        self.assertEqual(customer["days_since_last_activity"], 0)
        self.assertEqual(customer["last_activity_date"], "2026-11-09")
        self.assertIn("A recent Sales Focus email", customer["display_last_contact"])

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

    def test_departmental_invoice_activity_routes_by_invoice_number_not_native_contact_key(self):
        activity = {
            "date_created": "2026-01-22 10:00:07",
            "customer_primary_key": "cherry-hill-key",
            "customer_company": "Vestis Uniform Services (Cherry Hill, NJ)",
            "subject": "invoice 10602",
            "sender_email": "customerservice@numatsystems.com",
            "to": "apss@vestis.com",
        }
        entries = [{
            "customer": "Vestis Uniform Services (Cherry Hill, NJ)",
            "customer_primary_key": "cherry-hill-key",
            "orders": [{"extra": {"Orders::Invoice Number": "10599"}}],
        }, {
            "customer": "Vestis Uniform Services (Rockford, IL)",
            "customer_primary_key": "rockford-key",
            "orders": [{"extra": {"Orders::Invoice Number": 10602}}],
        }]

        contextual = main.build_contextual_crm_activity_map(
            {"cherry-hill-key": [activity]},
            [activity],
            entries,
        )

        self.assertEqual(contextual["rockford-key"], [activity])
        self.assertEqual(contextual["cherry-hill-key"], [])

    def test_unresolved_departmental_activity_is_not_left_on_default_location(self):
        activity = {
            "date_created": "2026-10-23 08:34:29",
            "customer_primary_key": "cherry-hill-key",
            "customer_company": "Vestis Uniform Services (Cherry Hill, NJ)",
            "subject": "Numat Systems - Our Invoice No - 10890",
            "sender_email": "customerservice@numatsystems.com",
            "to": "apss@vestis.com",
        }
        entries = [{
            "customer": "Vestis Uniform Services (Cherry Hill, NJ)",
            "customer_primary_key": "cherry-hill-key",
            "orders": [],
        }]

        contextual = main.build_contextual_crm_activity_map(
            {"cherry-hill-key": [activity]},
            [activity],
            entries,
        )

        self.assertEqual(contextual["cherry-hill-key"], [])

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
                {"email": "shared@example.com", "customer_ref": "branch-a", "active": "Active"},
                {"email": "SHARED@example.com", "customer_ref": "branch-b", "active": "Active"},
            ],
        }

        activity_map = crm.build_shared_contact_activity_map([activity], master_data)

        self.assertEqual(activity_map["branch-a"], [activity])
        self.assertEqual(activity_map["branch-b"], [activity])

    def test_departmental_accounts_payable_email_stays_on_original_location(self):
        activity = {
            "date_created": "2026-11-10 09:00:00",
            "customer_primary_key": "vestis-birmingham",
            "sender_email": "customerservice@numatsystems.com",
            "to": "apss@vestis.com",
            "subject": "Invoice 10950",
            "activity_category": "customer_services",
        }
        master_data = {
            "customers_by_key": {
                "vestis-birmingham": {},
                "vestis-charlotte": {},
                "vestis-memphis": {},
            },
            "all_contacts": [
                {
                    "name": "Accounts Payable",
                    "position": "Accounts Payable",
                    "email": "apss@vestis.com",
                    "customer_ref": customer_key,
                    "active": "Active",
                }
                for customer_key in (
                    "vestis-birmingham",
                    "vestis-charlotte",
                    "vestis-memphis",
                )
            ],
        }

        activity_map = crm.build_shared_contact_activity_map([activity], master_data)

        self.assertEqual(activity_map["vestis-birmingham"], [activity])
        self.assertNotIn("vestis-charlotte", activity_map)
        self.assertNotIn("vestis-memphis", activity_map)

    def test_contact_role_can_mark_nonstandard_departmental_mailbox(self):
        contacts = [{
            "name": "Accounts Payable Team",
            "position": "Invoice Processing",
            "email": "shared-box@customer.com",
        }]

        self.assertTrue(crm.is_departmental_contact_email("shared-box@customer.com", contacts))
        self.assertFalse(crm.is_departmental_contact_email("pam.martin@vestis.com", contacts))

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
                "active": "Active",
            }],
        }

        activity_map = crm.build_shared_contact_activity_map([activity], master_data)

        self.assertEqual(activity_map["memphis-276"], [activity])
        self.assertEqual(activity_map["memphis-mc581"], [activity])

    def test_shared_contact_activity_ignores_inactive_old_branch(self):
        activity = {
            "customer_primary_key": "lawrenceville-key",
            "sender_email": "sales@numatsystems.com",
            "to": "bauschm@cintas.com",
        }
        master_data = {
            "customers_by_key": {"vidalia-key": {}, "lawrenceville-key": {}},
            "all_contacts": [
                {
                    "email": "bauschm@cintas.com",
                    "customer_ref": "vidalia-key",
                    "active": "Inactive",
                },
                {
                    "email": "bauschm@cintas.com",
                    "customer_ref": "lawrenceville-key",
                    "active": "Active",
                },
            ],
        }

        activity_map = crm.build_shared_contact_activity_map([activity], master_data)

        self.assertNotIn("vidalia-key", activity_map)
        self.assertEqual(activity_map["lawrenceville-key"], [activity])

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
        self.assertIn("Email outreach", html)
        self.assertIn(">Visit</button>", html)
        self.assertIn("Record visit in FileMaker", html)
        self.assertIn("Schedule follow-up", html)
        self.assertNotIn("<span>Enrich</span>", html)
        self.assertNotIn("Back to customers", html)
        full_directory.assert_called()

        with (
            patch.object(main, "get_orders_for_analysis", return_value=order_result),
            patch.object(main, "fetch_crm_activities", return_value=crm_result),
            patch.object(main, "fetch_filemaker_customer_data", return_value=master_result),
            patch.object(main, "fetch_filemaker_master_data", return_value=contact_directory),
        ):
            from_customers = main.get_customer_view(
                "No Order Customer",
                return_to="/customers-view?group=A&territory=east&page=2",
            )

        self.assertIn("Back to customers", from_customers)
        self.assertIn("group=A&amp;territory=east&amp;page=2", from_customers)

        with (
            patch.object(main, "get_orders_for_analysis", return_value=order_result),
            patch.object(main, "fetch_crm_activities", return_value=crm_result),
            patch.object(main, "fetch_filemaker_customer_data", return_value=master_result),
            patch.object(main, "fetch_filemaker_master_data", return_value=contact_directory),
        ):
            from_action_plan = main.get_customer_view(
                "No Order Customer",
                return_to="/action-plan-view?selected_customer=No+Order+Customer#focus-preview",
            )

        self.assertIn("Back to Action Plan", from_action_plan)
        self.assertIn("selected_customer=No+Order+Customer#focus-preview", from_action_plan)

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

    def test_customer_toolbar_uses_territory_and_group_filters(self):
        toolbar = main.render_customers_filter_toolbar(
            group="C",
            territory="east",
            sort="customer",
            direction="asc",
            customer="Vestis",
        )

        self.assertIn('aria-label="Territory"', toolbar)
        self.assertIn('aria-label="Customer group"', toolbar)
        self.assertIn('class="toggle-chip active"', toolbar)
        self.assertIn("Nationals", toolbar)
        self.assertIn("Find a customer", toolbar)
        self.assertIn('value="Vestis"', toolbar)
        self.assertIn(">Search<", toolbar)
        self.assertIn(">Clear<", toolbar)
        self.assertIn("customer=Vestis", toolbar)

        sync_form = main.render_customers_sync_form("C", "east", "customer", "asc")
        self.assertIn("Sync customer data", sync_form)
        self.assertIn("small-button", sync_form)

    def test_customer_directory_uses_disk_cache_before_filemaker(self):
        cache_key = ("server", "database", "customers", "contacts")
        cached = {"status": "ok", "customers_by_key": {"cached": {"company": "Cached Co"}}}
        filemaker._FILEMAKER_CUSTOMER_DATA_CACHE.update({"key": None, "expires_at": 0, "result": None})
        filemaker._FILEMAKER_MASTER_DATA_CACHE.update({"key": None, "expires_at": 0, "result": None})
        with (
            patch.object(filemaker, "get_filemaker_config", return_value={"customers_layout": "Customers"}),
            patch.object(filemaker, "build_filemaker_master_data_cache_key", return_value=cache_key),
            patch.object(filemaker, "read_filemaker_master_data_disk_cache", return_value=cached),
            patch.object(filemaker, "fetch_all_layout_records") as fetch,
        ):
            result = filemaker.fetch_filemaker_customer_data()

        self.assertIs(result, cached)
        fetch.assert_not_called()

    def test_customer_filters_match_group_and_territory(self):
        summaries = [
            {"customer": "East Vestis", "state": "OH", "group_code": "A", "territory": "East"},
            {"customer": "West Vestis", "state": "CA", "group_code": "A", "territory": "West"},
            {"customer": "East Alsco", "state": "NY", "group_code": "E", "territory": "East"},
        ]

        filtered = main.filter_customer_summaries(
            summaries,
            group="A",
            territory="east",
        )

        self.assertEqual([item["customer"] for item in filtered], ["East Vestis"])

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
        self.assertIn('class="customer-name-link"', row)
        self.assertNotIn("<strong>No Order Customer</strong>", row)

    def test_customer_link_carries_customers_page_filters_and_back_link_is_safe(self):
        return_to = "/customers-view?group=A&territory=west&sort=last_order&direction=desc&page=3"
        row = main.render_customer_summary_row({
            "customer": "Filtered Customer",
            "order_count": 1,
            "crm_count": 0,
            "state": "CA",
            "last_order": "2026-11-01",
            "avg_cycle": None,
            "cycle_pattern_label": "Not enough orders",
            "total_value": 100,
            "avg_value": 100,
            "display_last_activity": None,
        }, return_to=return_to)

        self.assertIn("return_to=%2Fcustomers-view%3Fgroup%3DA%26territory%3Dwest", row)
        self.assertIn("%23customer-filtered-customer", row)
        self.assertIn('id="customer-filtered-customer"', row)
        self.assertEqual(main.normalize_customers_return_to(return_to), return_to)
        self.assertEqual(main.normalize_customers_return_to("https://example.com/customers-view"), "")
        self.assertEqual(main.normalize_customers_return_to("/orders-view"), "")
        self.assertEqual(
            main.normalize_customer_detail_return_to(
                "/action-plan-view?selected_customer=Filtered+Customer#focus-preview"
            ),
            "/action-plan-view?selected_customer=Filtered+Customer#focus-preview",
        )
        self.assertEqual(main.normalize_customer_detail_return_to("https://example.com/action-plan-view"), "")

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
