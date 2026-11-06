import unittest

import main


class PriceListETests(unittest.TestCase):
    def test_confirmed_filemaker_price_list_drives_customer_group(self):
        self.assertEqual(
            main.infer_customer_group_code(
                "Alsco, Inc. (Portland, OR)",
                customer_orders=[{"extra": {"ai_PriceList": "C"}}],
                customer_record={"price_list": "E"},
            ),
            "E",
        )

    def test_company_fallbacks_match_current_price_list_structure(self):
        self.assertEqual(main.infer_customer_group_code("Alsco Denver"), "E")
        self.assertEqual(main.infer_customer_group_code("Prudential Uniforms"), "C")
        self.assertEqual(main.infer_customer_group_code("Independent Laundry"), "B")

    def test_order_trend_shows_price_list_e_separately(self):
        markup = main.render_monthly_orders_chart(
            [{"order_date": "2026-10-30", "extra": {"ai_PriceList": "E"}}]
        )

        self.assertIn(">E</span>", markup)
        self.assertNotIn(">Other</span>", markup)


if __name__ == "__main__":
    unittest.main()
