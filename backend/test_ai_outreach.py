import unittest

from ai import build_text_tone_email_body, draft_violates_outreach_style


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


if __name__ == "__main__":
    unittest.main()
