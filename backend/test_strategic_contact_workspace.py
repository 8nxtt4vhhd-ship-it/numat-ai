import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from datetime import datetime

import main
from strategic_contacts_report import _activity_contacts, _activity_text, build_strategic_contacts_pdf


class StrategicContactWorkspaceTests(unittest.TestCase):
    def test_placeholder_contact_email_detection(self):
        self.assertTrue(main.is_placeholder_contact_email("tbc@vestis.com"))
        self.assertTrue(main.is_placeholder_contact_email("TBD@example.com"))
        self.assertTrue(main.is_placeholder_contact_email(""))
        self.assertFalse(main.is_placeholder_contact_email("person@vestis.com"))

    def test_placeholder_emails_do_not_link_different_strategic_contacts(self):
        bill = {
            "id": "bill-id",
            "name": "Bill Seward",
            "organization": "Vestis Corporate",
            "position": "Vice President",
            "email": "tbc@vestis.com",
        }
        kenny = {
            "id": "kenny-id",
            "name": "Kenny Moorehead",
            "organization": "Vestis Corporate",
            "position": "Vice President",
            "email": "tbc@vestis.com",
        }
        lookup = main.build_saved_strategic_contact_sets([bill, kenny])

        self.assertNotIn("tbc@vestis.com", lookup[0])
        self.assertEqual(
            main.get_saved_strategic_contact_id(
                "tbc@vestis.com",
                "",
                "Bill Seward",
                "Vestis Corporate",
                "Vice President",
                lookup,
            ),
            "bill-id",
        )
        self.assertEqual(
            main.get_saved_strategic_contact_id(
                "tbc@vestis.com",
                "",
                "Kenny Moorehead",
                "Vestis Corporate",
                "Vice President",
                lookup,
            ),
            "kenny-id",
        )

    def test_report_outreach_removes_signature_disclaimer_and_quoted_thread(self):
        content = """Hi Brandon,

Thank you for reaching out. I have copied the new plant manager.

Brandon D. Angeles
General Manager | West Region
4700 Havana Street, Denver, CO 80239
From: Peter Binnington <pbinnington@numatsystems.com>
Sent: Tuesday, April 8, 2025 8:55 AM
To: Brandon Angeles <brandon.angeles@vestis.com>
Subject: Vestis Mat Repair Program

CAUTION: This email was sent from outside of Vestis.

This older quoted message should not appear.
"""

        cleaned = main.clean_report_outreach_content(content, contact_name="Brandon Angeles")

        self.assertEqual(
            cleaned,
            "Hi Brandon,\n\nThank you for reaching out. I have copied the new plant manager.",
        )
        self.assertNotIn("From:", cleaned)
        self.assertNotIn("CAUTION", cleaned)
        self.assertNotIn("General Manager", cleaned)

    def test_existing_filemaker_contact_hides_sync_and_shows_enrich(self):
        contact = main.normalize_strategic_contact_record(
            {
                "id": "contact-1",
                "organization": "Vestis Corporate",
                "name": "Taylor Smith",
                "position": "Director",
                "email": "tbc@vestis.com",
                "scope_type": "Corporate",
                "region": "Shared / Unassigned",
            }
        )
        with patch.object(main, "can_manage_strategic_contacts", return_value=True):
            html = main.render_organisation_chart(
                [contact],
                "vestis-aramark",
                filemaker_presence_map={"contact-1": {"exists": True, "label": "Already in FileMaker"}},
                contact_activity_map={
                    "contact-1": {
                        "date_raw": "2026-10-01 14:30:00",
                        "date_label": "01/10/2026 14:30",
                        "subject": "Plant review",
                        "preview": "We confirmed the next review meeting.",
                    }
                },
            )

        self.assertIn('action="/strategic-contacts/enrich"', html)
        self.assertNotIn('action="/strategic-contacts/sync-filemaker"', html)
        self.assertIn("Most recent contact", html)
        self.assertIn("Plant review", html)
        self.assertIn("We confirmed the next review meeting.", html)
        self.assertIn("org-chart-contact-card contact-recent", html)
        self.assertLess(html.index('class="org-chart-levels"'), html.index('class="panel org-map-panel"'))

    def test_recent_contact_map_uses_latest_matching_email_activity(self):
        contacts = [
            main.normalize_strategic_contact_record(
                {
                    "id": "contact-activity",
                    "name": "Taylor Smith",
                    "email": "taylor@vestis.com",
                }
            )
        ]
        crm_result = {
            "status": "ok",
            "activities": [
                {
                    "date_created": "2026-09-01 09:00:00",
                    "sender_email": "taylor@vestis.com",
                    "to": "sales@numatsystems.com",
                    "subject": "Older note",
                    "body": "This should not be shown.",
                    "direction": "inbound",
                },
                {
                    "date_created": "2026-10-01 10:30:00",
                    "sender_email": "sales@numatsystems.com",
                    "to": "Taylor Smith <taylor@vestis.com>",
                    "subject": "Latest note",
                    "body": "This is the newest contact preview.",
                    "direction": "outbound",
                },
            ],
        }

        result = main.build_strategic_contact_activity_map(contacts, crm_result=crm_result)

        self.assertEqual(result["contact-activity"]["subject"], "Latest note")
        self.assertEqual(result["contact-activity"]["preview"], "This is the newest contact preview.")
        self.assertEqual(result["contact-activity"]["direction"], "outbound")

    def test_non_filemaker_contact_keeps_sync_action(self):
        contact = main.normalize_strategic_contact_record(
            {
                "id": "contact-2",
                "organization": "Vestis Corporate",
                "name": "Morgan Jones",
                "position": "Manager",
                "email": "morgan@vestis.com",
                "scope_type": "Corporate",
                "region": "Shared / Unassigned",
            }
        )
        with patch.object(main, "can_manage_strategic_contacts", return_value=True):
            html = main.render_organisation_chart(
                [contact],
                "vestis-aramark",
                filemaker_presence_map={"contact-2": {"exists": False}},
            )

        self.assertIn('action="/strategic-contacts/enrich"', html)
        self.assertIn('action="/strategic-contacts/sync-filemaker"', html)
        self.assertNotIn("org-chart-contact-card contact-recent", html)
        self.assertNotIn("org-chart-contact-card contact-older", html)

    def test_contact_recency_colours_use_sixty_day_threshold(self):
        today = datetime(2026, 10, 2, 12, 0, 0)

        self.assertEqual(
            main.get_strategic_contact_recency_class(
                {"date_raw": "2026-09-01 09:00:00"}, today=today
            ),
            " contact-recent",
        )
        self.assertEqual(
            main.get_strategic_contact_recency_class(
                {"date_raw": "2026-07-01 09:00:00"}, today=today
            ),
            " contact-older",
        )
        self.assertEqual(main.get_strategic_contact_recency_class({}, today=today), "")

    def test_report_payload_and_pdf_include_chart_and_full_activity(self):
        contacts = [
            main.normalize_strategic_contact_record(
                {
                    "id": "report-1",
                    "organization": "Vestis Corporate",
                    "name": "Jordan Taylor",
                    "position": "VP Operations",
                    "email": "jordan@vestis.com",
                    "phone": "+1 555 0101",
                    "scope_type": "Corporate",
                    "region": "GA",
                }
            )
        ]
        activities = {
            "report-1": {
                "date_raw": "2026-10-01 10:30:00",
                "date_label": "01/10/2026 10:30",
                "direction": "outbound",
                "subject": "Operations review",
                "full_body": "This is the complete most recent outreach message.",
            }
        }

        payload = main.build_strategic_contacts_report_payload(
            contacts,
            "vestis-aramark",
            activities,
            now=datetime(2026, 10, 2, 12, 0, 0),
        )
        pdf_content = build_strategic_contacts_pdf(payload)

        self.assertEqual(payload["contacts"][0]["activity_state"], "recent")
        self.assertEqual(payload["contacts"][0]["full_body"], "This is the complete most recent outreach message.")
        self.assertTrue(pdf_content.startswith(b"%PDF"))
        self.assertGreater(len(pdf_content), 3000)

    def test_pdf_activity_section_selects_only_contacts_with_activity(self):
        active = {
            "name": "Active Contact",
            "activity_state": "recent",
            "subject": "",
            "full_body": "A complete outreach message.",
        }
        inactive = {"name": "No Activity Contact", "activity_state": "none"}

        self.assertEqual(_activity_contacts([active, inactive]), [active])
        self.assertEqual(_activity_text(active), ("", "A complete outreach message."))

    def test_send_report_targets_logged_in_users_m365_email(self):
        with patch.object(
            main,
            "get_current_session_user",
            return_value={"username": "kelly", "m365_email": "kelly@example.com", "active": True},
        ), patch.object(main, "can_manage_strategic_contacts", return_value=True), patch.object(
            main, "load_strategic_contacts", return_value=[]
        ), patch.object(main, "build_strategic_contact_activity_map", return_value={}), patch.object(
            main, "build_strategic_contacts_pdf", return_value=b"%PDF-test"
        ), patch.object(
            main, "ensure_valid_access_token", return_value={"status": "ok", "access_token": "user-token"}
        ), patch.object(
            main, "send_m365_mail", return_value={"status": "ok"}
        ) as send_mail, patch.object(main, "record_audit_event"):
            response = main.post_organisation_chart_report_send("vestis-aramark")

        self.assertEqual(response.status_code, 303)
        self.assertIn("kelly%40example.com", response.headers["location"])
        self.assertEqual(send_mail.call_args.args[0], "user-token")
        self.assertEqual(send_mail.call_args.args[1], "kelly@example.com")
        self.assertEqual(send_mail.call_args.kwargs["attachments"][0]["content_type"], "application/pdf")

    def test_send_report_falls_back_to_reporting_mail_without_user_token(self):
        with patch.object(
            main,
            "get_current_session_user",
            return_value={"username": "kelly", "m365_email": "kelly@example.com", "active": True},
        ), patch.object(main, "can_manage_strategic_contacts", return_value=True), patch.object(
            main, "load_strategic_contacts", return_value=[]
        ), patch.object(main, "build_strategic_contact_activity_map", return_value={}), patch.object(
            main, "build_strategic_contacts_pdf", return_value=b"%PDF-test"
        ), patch.object(
            main, "ensure_valid_access_token", return_value={"status": "not_connected"}
        ), patch.object(
            main, "send_m365_reporting_mail", return_value={"status": "ok"}
        ) as send_mail, patch.object(main, "record_audit_event"):
            response = main.post_organisation_chart_report_send("vestis-aramark")

        self.assertEqual(response.status_code, 303)
        self.assertEqual(send_mail.call_args.args[0], ["kelly@example.com"])

    def test_enrichment_replaces_placeholder_email_and_adds_phone(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            contacts_path = Path(temp_dir) / "strategic_contacts.json"
            with patch.dict(os.environ, {"STRATEGIC_CONTACTS_PATH": str(contacts_path)}):
                main.save_strategic_contacts(
                    [
                        main.normalize_strategic_contact_record(
                            {
                                "id": "contact-3",
                                "organization": "Vestis",
                                "name": "Jamie Lee",
                                "position": "VP Operations",
                                "email": "tbc@vestis.com",
                                "phone": "",
                                "region": "Georgia",
                            }
                        )
                    ]
                )
                with patch.object(
                    main,
                    "enrich_pdl_person",
                    return_value={
                        "status": "ok",
                        "result": {
                            "work_email": "jamie.lee@vestis.com",
                            "mobile_phone": "+1 555 0100",
                        },
                    },
                ) as pdl_enrich, patch.object(main, "discover_strategic_contacts_with_openai") as openai_discovery:
                    result = main.enrich_saved_strategic_contact("contact-3")

                saved = main.load_strategic_contacts()[0]

        self.assertTrue(result["ok"])
        self.assertEqual(saved["email"], "jamie.lee@vestis.com")
        self.assertEqual(saved["phone"], "+1 555 0100")
        self.assertEqual(saved["enrichment_source"], "PDL")
        self.assertTrue(saved["enriched_at"])
        self.assertEqual(pdl_enrich.call_args.kwargs["email"], "")
        openai_discovery.assert_not_called()

    def test_openai_discovery_replaces_apollo_as_enrichment_fallback(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            contacts_path = Path(temp_dir) / "strategic_contacts.json"
            with patch.dict(os.environ, {"STRATEGIC_CONTACTS_PATH": str(contacts_path)}):
                main.save_strategic_contacts(
                    [
                        main.normalize_strategic_contact_record(
                            {
                                "id": "contact-4",
                                "organization": "Vestis",
                                "name": "Alex Morgan",
                                "position": "Director of Operations",
                                "email": "tbc@vestis.com",
                                "phone": "",
                            }
                        )
                    ]
                )
                with patch.object(
                    main,
                    "enrich_pdl_person",
                    return_value={"status": "no_match", "result": None},
                ), patch.object(
                    main,
                    "discover_strategic_contacts_with_openai",
                    return_value={
                        "status": "ok",
                        "results": [
                            {
                                "name": "Alex Morgan",
                                "email": "alex.morgan@vestis.com",
                                "phone": "+1 555 0199",
                            }
                        ],
                    },
                ) as openai_discovery, patch.object(main, "enrich_apollo_person") as apollo_enrich:
                    result = main.enrich_saved_strategic_contact("contact-4")

                saved = main.load_strategic_contacts()[0]

        self.assertTrue(result["ok"])
        self.assertEqual(saved["email"], "alex.morgan@vestis.com")
        self.assertEqual(saved["phone"], "+1 555 0199")
        self.assertEqual(saved["enrichment_source"], "OpenAI")
        self.assertEqual(openai_discovery.call_args.kwargs["contact_name"], "Alex Morgan")
        apollo_enrich.assert_not_called()


if __name__ == "__main__":
    unittest.main()
