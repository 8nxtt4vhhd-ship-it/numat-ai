import unittest

import main


class ContactsViewTests(unittest.TestCase):
    def test_contact_row_includes_mobile_labels_phone_and_cell_links(self):
        html = main.render_contact_master_row({
            "company": "Example Linen (Columbus, OH)",
            "customer_ref": "customer-1",
            "name": "Taylor Smith",
            "email": "taylor@example.com",
            "phone": "(614) 555-0100",
            "cell": "+1 614 555 0101",
            "position": "Manager",
            "active": "Active",
        })

        self.assertIn('data-label="Name"', html)
        self.assertIn('data-label="Email"', html)
        self.assertIn('data-label="Telephone"', html)
        self.assertIn('data-label="Cell"', html)
        self.assertIn('href="mailto:taylor@example.com"', html)
        self.assertIn('href="tel:6145550100"', html)
        self.assertIn('href="tel:+16145550101"', html)

    def test_missing_phone_values_remain_visible_as_empty_placeholders(self):
        html = main.render_contact_master_row({
            "company": "Example Linen",
            "customer_ref": "customer-1",
            "name": "Taylor Smith",
            "email": "",
            "phone": "",
            "cell": "",
            "position": "",
            "active": "Active",
        })

        self.assertNotIn('href="mailto:', html)
        self.assertNotIn('href="tel:', html)
        self.assertEqual(html.count("<span class='muted'>—</span>"), 3)


if __name__ == "__main__":
    unittest.main()
