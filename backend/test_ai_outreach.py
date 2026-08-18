import unittest

from ai import (
    build_primary_fact_fallback_email,
    build_text_tone_email_body,
    draft_violates_outreach_style,
    redact_expired_scheduling_notes,
    select_primary_outreach_fact,
)


class OutreachStyleTests(unittest.TestCase):
    def test_vague_location_and_sales_language_are_rejected(self):
        self.assertTrue(draft_violates_outreach_style("I was in your area last week."))
        self.assertTrue(draft_violates_outreach_style("We have an exciting opportunity to partner with you."))
        self.assertTrue(draft_violates_outreach_style("I was due to schedule a visit, so I wanted to check."))
        self.assertTrue(draft_violates_outreach_style("Do you have mats ready? Would you like a pickup?"))

    def test_direct_factual_language_is_allowed(self):
        text = "I was visiting customers in Chicago last week, and it reminded me that it has been a while since your last repair order."
        self.assertFalse(draft_violates_outreach_style(text))

    def test_preferred_single_thread_example_is_allowed(self):
        text = "We missed each other a few weeks ago. You previously mentioned collecting wavy mats for edge repair. Are you still setting those aside?"
        self.assertFalse(draft_violates_outreach_style(text))

    def test_fallback_uses_direct_american_style(self):
        body = build_text_tone_email_body(
            {
                "customer": "Example Customer",
                "days_since_last_order": 120,
                "sales_outreach_sent_count": 0,
                "sales_reply_count": 0,
            }
        )
        self.assertIn("since the last repair order", body)
        self.assertIn("send me an approximate quantity", body)
        self.assertFalse(draft_violates_outreach_style(body))

    def test_specific_repair_signal_outranks_order_age_and_expired_visit(self):
        items = redact_expired_scheduling_notes([
            {
                "date": "2026-07-01",
                "direction": "Outbound",
                "subject": "Visit",
                "preview": "I will be in Chicago next week. Greg may have wavy mats for edge repair.",
                "relative_timing_expired": True,
                "later_order_recorded": False,
            },
            {
                "date": "2022-06-01",
                "direction": "Outbound",
                "subject": "Order history",
                "preview": "Last order was four years ago.",
                "later_order_recorded": False,
            },
        ])
        fact = select_primary_outreach_fact(items)
        self.assertEqual(fact["type"], "unresolved_repair_signal")
        self.assertEqual(fact["direction"], "Outbound")
        self.assertIn("wavy mats", fact["preview"])
        fallback = build_primary_fact_fallback_email(
            {"primary_contact": {"name": "Greg Smith"}}, fact
        )
        self.assertIn("I had noted possible wavy mats for edge repair", fallback["email_body"])
        self.assertNotIn("You previously mentioned", fallback["email_body"])
        self.assertIn("Are you still seeing those?", fallback["email_body"])

    def test_inbound_repair_fact_is_attributed_to_customer(self):
        fact = select_primary_outreach_fact([
            {
                "direction": "Inbound",
                "preview": "We are collecting wavy mats for edge repair.",
                "later_order_recorded": False,
            }
        ])
        fallback = build_primary_fact_fallback_email(
            {"primary_contact": {"name": "Greg Smith"}}, fact
        )
        self.assertIn("You previously mentioned collecting wavy mats for edge repair", fallback["email_body"])

    def test_salesperson_conditional_question_does_not_become_a_customer_fact(self):
        fact = select_primary_outreach_fact([
            {
                "direction": "Outbound",
                "preview": "If you have any damaged or wavy mats, we can repair them.",
                "later_order_recorded": False,
            }
        ])
        self.assertEqual(fact["speech_act"], "salesperson_question")
        fallback = build_primary_fact_fallback_email(
            {"primary_contact": {"name": "Greg Smith"}}, fact
        )
        self.assertIn("Do you have any damaged or wavy mats that need repair?", fallback["email_body"])
        self.assertNotIn("you mentioned", fallback["email_body"].lower())
        self.assertNotIn("I had noted", fallback["email_body"])

    def test_repair_signal_is_not_reused_after_later_order(self):
        fact = select_primary_outreach_fact([
            {
                "preview": "Wavy mats for edge repair",
                "later_order_recorded": True,
            }
        ])
        self.assertEqual(fact, {})

    def test_recent_outbound_thread_outranks_older_repair_history(self):
        fact = select_primary_outreach_fact([
            {
                "age_days": 13,
                "direction": "Outbound",
                "recipient": "neil@alsco.com",
                "subject": "cost saving initiative",
                "preview": "We signed a corporate agreement with Alsco for remanufacturing. Do you have rippled or damaged mats?",
                "later_order_recorded": False,
            },
            {
                "age_days": 120,
                "direction": "Outbound",
                "subject": "Repair opportunity",
                "preview": "Possible wavy mats for edge repair.",
                "later_order_recorded": False,
            },
        ])
        self.assertEqual(fact["type"], "recent_outbound_thread")
        self.assertEqual(fact["subject"], "cost saving initiative")

    def test_recent_thread_fallback_handles_a_change_of_contact(self):
        fact = {
            "type": "recent_outbound_thread",
            "direction": "Outbound",
            "recipient": "neil@alsco.com",
            "subject": "cost saving initiative",
            "preview": "We signed a corporate agreement with Alsco for remanufacturing and asked about rippled or damaged mats.",
        }
        fallback = build_primary_fact_fallback_email(
            {"primary_contact": {"name": "Clinton", "email": "clinton@alsco.com"}}, fact
        )
        self.assertIn("I recently emailed your team", fallback["email_body"])
        self.assertIn("Are you the right person to speak with about this?", fallback["email_body"])
        self.assertNotIn("two years", fallback["email_body"])


if __name__ == "__main__":
    unittest.main()
