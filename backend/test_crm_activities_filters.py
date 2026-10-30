import unittest
from unittest.mock import patch

import crm
import main


class CrmActivitiesFilterTests(unittest.TestCase):
    def test_sender_filter_matches_email_case_insensitively(self):
        activities = [
            {"sender_email": "Alex.Smith@example.com", "date_created": "2026-10-01 09:00:00"},
            {"sender_email": "sales@example.com", "date_created": "2026-10-01 10:00:00"},
        ]

        filtered = main.filter_crm_activities(activities, sender="SMITH@EXAMPLE")

        self.assertEqual(filtered, activities[:1])

    def test_activities_without_a_date_are_excluded(self):
        dated = {
            "subject": "Valid activity",
            "date_created": "2026-10-01 09:00:00",
        }
        undated = {
            "subject": "trash mats?",
            "date_created": "",
        }

        filtered = main.filter_crm_activities([undated, dated])

        self.assertEqual(filtered, [dated])

    def test_unfiltered_activity_list_does_not_recalculate_dates_or_categories(self):
        activities = [{
            "subject": "Existing cached activity",
            "date_created": "2026-10-01 09:00:00",
            "body": "A deliberately large body that should not be inspected.",
        }]

        with (
            patch.object(main, "parse_crm_datetime") as parse_date,
            patch.object(main, "get_activity_category") as get_category,
        ):
            filtered = main.filter_crm_activities(activities)

        self.assertEqual(filtered, activities)
        parse_date.assert_not_called()
        get_category.assert_not_called()

    def test_crm_activity_preview_cleanup_is_reused(self):
        main._CRM_ACTIVITY_PREVIEW_CACHE.clear()
        with patch.object(main, "clean_activity_content", return_value="Clean body") as clean:
            first = main.get_cached_crm_activity_preview("<p>Body</p>")
            second = main.get_cached_crm_activity_preview("<p>Body</p>")

        self.assertEqual(first, "Clean body")
        self.assertEqual(second, "Clean body")
        clean.assert_called_once_with("<p>Body</p>")

    def test_shared_crm_result_excludes_undated_activities(self):
        dated = {
            "customer_primary_key": "customer-1",
            "date_created": "2026-10-01 09:00:00",
        }
        undated = {
            "customer_primary_key": "customer-1",
            "date_created": "",
        }
        result = crm.filter_undated_crm_activities({
            "status": "ok",
            "activities": [undated, dated],
            "activity_map": {"customer-1": [undated, dated]},
            "counts": {"total_rows": 2, "kept_rows": 2},
        })

        self.assertEqual(result["activities"], [dated])
        self.assertEqual(result["activity_map"]["customer-1"], [dated])
        self.assertEqual(result["counts"]["kept_rows"], 1)
        self.assertEqual(result["counts"]["excluded_missing_date"], 1)

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
