import unittest
from unittest.mock import patch

import customer_order_refresh


class CustomerOrderRefreshTests(unittest.TestCase):
    @patch.object(customer_order_refresh, "sync_filemaker_mat_counts", return_value={"status": "ok", "record_count": 10, "order_count": 2})
    @patch.object(customer_order_refresh, "restore_cache_backup")
    @patch.object(customer_order_refresh, "read_cache_backup", return_value=b"old cache")
    @patch.object(customer_order_refresh, "fetch_filemaker_master_data")
    @patch.object(customer_order_refresh, "clear_filemaker_master_data_cache")
    @patch.object(customer_order_refresh, "get_orders_for_analysis")
    @patch.object(customer_order_refresh, "clear_filemaker_orders_cache")
    def test_refresh_rebuilds_orders_and_full_customer_directory(
        self,
        clear_orders,
        get_orders,
        clear_master,
        fetch_master,
        read_backup,
        restore_backup,
        sync_mats,
    ):
        get_orders.return_value = {"status": "ok", "orders": [{}, {}]}
        fetch_master.return_value = {
            "status": "ok",
            "companies": [{}],
            "all_contacts": [{}, {}, {}],
        }

        result = customer_order_refresh.refresh_customer_and_order_data()

        clear_orders.assert_called_once_with(clear_persisted=True)
        clear_master.assert_called_once_with(clear_persisted=True)
        fetch_master.assert_called_once_with(force_refresh=True)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["orders"]["count"], 2)
        self.assertEqual(result["customers"]["count"], 1)
        self.assertEqual(result["customers"]["contact_count"], 3)
        self.assertEqual(result["mats"]["count"], 10)
        restore_backup.assert_not_called()

    @patch.object(customer_order_refresh, "sync_filemaker_mat_counts", return_value={"status": "ok", "record_count": 0, "order_count": 0})
    @patch.object(customer_order_refresh, "restore_cache_backup")
    @patch.object(customer_order_refresh, "read_cache_backup", return_value=b"old cache")
    @patch.object(customer_order_refresh, "fetch_filemaker_master_data")
    @patch.object(customer_order_refresh, "clear_filemaker_master_data_cache")
    @patch.object(customer_order_refresh, "get_orders_for_analysis")
    @patch.object(customer_order_refresh, "clear_filemaker_orders_cache")
    def test_refresh_reports_partial_failure(
        self,
        clear_orders,
        get_orders,
        clear_master,
        fetch_master,
        read_backup,
        restore_backup,
        sync_mats,
    ):
        get_orders.return_value = {"status": "connection_error", "orders": []}
        fetch_master.return_value = {"status": "ok", "companies": [], "all_contacts": []}

        result = customer_order_refresh.refresh_customer_and_order_data()

        self.assertEqual(result["status"], "error")
        self.assertEqual(result["orders"]["status"], "connection_error")
        restore_backup.assert_called_once()


if __name__ == "__main__":
    unittest.main()
