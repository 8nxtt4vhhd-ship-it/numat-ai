import unittest

import main


class CrmActivitiesFilterTests(unittest.TestCase):
    def test_sender_filter_matches_email_case_insensitively(self):
        activities = [
            {"sender_email": "Alex.Smith@example.com", "date_created": "2026-10-01 09:00:00"},
            {"sender_email": "sales@example.com", "date_created": "2026-10-01 10:00:00"},
        ]

        filtered = main.filter_crm_activities(activities, sender="SMITH@EXAMPLE")

        self.assertEqual(filtered, activities[:1])

    def test_sender_filter_is_preserved_in_filter_links(self):
        form = main.render_crm_filter_form(
            "Acme",
            "alex+sales@example.com",
            "outbound",
            "sales_outreach",
            "Quote",
            100,
            "",
            "",
            "all",
        )

        self.assertIn('name="sender"', form)
        self.assertIn('value="alex+sales@example.com"', form)
        self.assertIn("sender=alex%2Bsales%40example.com", form)

    def test_sender_filter_is_preserved_in_pagination(self):
        pagination = main.render_crm_pagination(
            "",
            "alex@example.com",
            "",
            "",
            "",
            "",
            "",
            "all",
            2,
            3,
            50,
            120,
        )

        self.assertIn("sender=alex%40example.com", pagination)


if __name__ == "__main__":
    unittest.main()
