import unittest
from unittest.mock import patch

import crm
import data_sources
import filemaker


class InactiveCustomerFilteringTests(unittest.TestCase):
    def test_customer_active_field_marks_inactive_record(self):
        config = filemaker.get_filemaker_config()
        record = {
            "fieldData": {
                config["customers_key_field"]: "I1",
                config["customers_name_field"]: "Inactive Co",
                config["customers_activity_status_field"]: "Inactive",
            }
        }

        customer = filemaker.map_filemaker_customer_master_record(record)

        self.assertTrue(filemaker.is_inactive_filemaker_customer(customer))

    def test_filemaker_orders_exclude_inactive_customer_key(self):
        orders = [
            {"customer": "Active Co", "extra": {"Customer Ref": "A1"}},
            {"customer": "Active Co Old Name", "extra": {"Customer Ref": "A1"}},
            {"customer": "Inactive Co", "extra": {"Customer Ref": "I1"}},
            {"customer": "Unknown Co", "extra": {}},
        ]
        with patch.object(data_sources, "get_order_data_source", return_value="filemaker"), patch.object(
            data_sources,
            "fetch_cached_filemaker_orders",
            return_value={"status": "ok", "orders": orders},
        ), patch.object(
            data_sources,
            "fetch_filemaker_master_data",
            return_value={
                "status": "ok",
                "inactive_customer_keys": ["I1"],
                "customers_by_key": {"A1": {"company": "Active Co Renamed"}},
            },
        ):
            result = data_sources.get_orders_for_analysis()

        self.assertEqual(
            [order["customer"] for order in result["orders"]],
            ["Active Co Renamed", "Active Co Renamed", "Unknown Co"],
        )

    def test_filemaker_crm_excludes_inactive_customer_key(self):
        activities = [
            {"customer_primary_key": "A1", "date_created": "2026-01-02"},
            {"customer_primary_key": "I1", "date_created": "2026-01-01"},
            {"customer_primary_key": "", "date_created": "2026-01-03"},
        ]
        with patch.object(
            crm,
            "fetch_filemaker_master_data",
            return_value={
                "status": "ok",
                "inactive_customer_keys": ["I1"],
                "customers_by_key": {"A1": {"company": "Active Co Renamed"}},
            },
        ):
            result = crm.filter_inactive_filemaker_customer_activities(
                {"status": "ok", "activities": activities},
                "filemaker",
            )

        self.assertEqual(
            [activity["customer_primary_key"] for activity in result["activities"]],
            ["A1", ""],
        )
        self.assertNotIn("I1", result["activity_map"])
        self.assertEqual(result["activities"][0]["customer"], "Active Co Renamed")


if __name__ == "__main__":
    unittest.main()
