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

    def test_repair_signal_is_not_reused_after_later_order(self):
        fact = select_primary_outreach_fact([
            {
                "preview": "Wavy mats for edge repair",
                "later_order_recorded": True,
            }
        ])
        self.assertEqual(fact, {})


if __name__ == "__main__":
    unittest.main()
