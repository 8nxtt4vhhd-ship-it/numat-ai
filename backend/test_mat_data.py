import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import mat_data


class MatDataTests(unittest.TestCase):
    def test_mat_records_are_counted_by_order_and_outcome(self):
        counts = {}
        records = [
            {"fieldData": {"Order Ref": "1016", "Mat Outcome": "Repair", "Rep_Total No": 3}},
            {"fieldData": {"Order Ref": "1016", "Mat Outcome": "Reject", "Rep_Total No": ""}},
            {"fieldData": {"Order Ref": "1016", "Mat Outcome": "Repair", "Rep_Total No": 2}},
        ]

        for record in records:
            mat_data.add_mat_record_to_counts(counts, record)

        self.assertEqual(counts["1016"], {
            "mats": 3,
            "repairs": 2,
            "rejects": 1,
            "repair_actions": 5,
            "other": 0,
            "sizes": {"Unspecified": {
                "mats": 3,
                "repairs": 2,
                "rejects": 1,
                "repair_actions": 5,
                "repair_value": 0.0,
                "repair_value_records": 0,
                "square_feet": 0.0,
                "square_feet_records": 0,
            }},
        })

    def test_mat_records_are_aggregated_by_size_for_freight_pricing(self):
        counts = {}
        records = [
            {"fieldData": {"Order Ref": "1016", "Mat Outcome": "Repair", "Mat Size": "3 x 5", "Square Foot": 15, "Repair Cost": 12.50}},
            {"fieldData": {"Order Ref": "1016", "Mat Outcome": "Repair", "Mat Size": "3 x 5", "Square Foot": 15, "Repair Cost": 17.50}},
            {"fieldData": {"Order Ref": "1016", "Mat Outcome": "Reject", "Mat Size": "3 x 5", "Square Foot": 15, "Repair Cost": 0}},
        ]

        for record in records:
            mat_data.add_mat_record_to_counts(counts, record)

        self.assertEqual(counts["1016"]["sizes"]["3 x 5"], {
            "mats": 3,
            "repairs": 2,
            "rejects": 1,
            "repair_actions": 0,
            "repair_value": 30.0,
            "repair_value_records": 2,
            "square_feet": 30.0,
            "square_feet_records": 2,
        })

    def test_sync_streams_batches_and_writes_aggregated_cache(self):
        pages = [
            {"status": "ok", "records": [
                {"fieldData": {"Order Ref": "1", "Mat Outcome": "Repair", "Rep_Total No": 1}},
                {"fieldData": {"Order Ref": "1", "Mat Outcome": "Reject"}},
            ]},
            {"status": "ok", "records": [
                {"fieldData": {"Order Ref": "2", "Mat Outcome": "Repair", "Rep_Total No": 2}},
            ]},
        ]
        with TemporaryDirectory() as directory:
            cache_path = Path(directory) / "mats.json"
            with patch.object(mat_data, "fetch_layout_records", side_effect=pages), patch.object(
                mat_data, "get_mat_counts_cache_path", return_value=cache_path
            ):
                result = mat_data.sync_filemaker_mat_counts(batch_size=2)

            saved = json.loads(cache_path.read_text(encoding="utf-8"))

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["record_count"], 3)
        self.assertEqual(result["order_count"], 2)
        self.assertEqual(saved["orders"]["1"]["rejects"], 1)

    def test_incremental_sync_replaces_recent_orders_and_preserves_older_orders(self):
        existing = {
            "status": "ok",
            "updated_at": "2026-11-22 03:00:00",
            "record_count": 4,
            "skipped_count": 0,
            "order_count": 2,
            "orders": {
                "100": {"mats": 3, "repairs": 3, "rejects": 0, "repair_actions": 0, "other": 0, "sizes": {}},
                "200": {"mats": 1, "repairs": 1, "rejects": 0, "repair_actions": 0, "other": 0, "sizes": {}},
            },
        }
        refreshed = {
            "status": "ok",
            "records": [
                {"fieldData": {"Order Ref": "200", "Mat Outcome": "Repair"}},
                {"fieldData": {"Order Ref": "200", "Mat Outcome": "Reject"}},
            ],
        }
        with TemporaryDirectory() as directory:
            cache_path = Path(directory) / "mats.json"
            with (
                patch.object(mat_data, "read_filemaker_mat_counts_cache", return_value=existing),
                patch.object(mat_data, "find_layout_records", return_value=refreshed) as find,
                patch.object(mat_data, "get_mat_counts_cache_path", return_value=cache_path),
            ):
                result = mat_data.sync_filemaker_mat_counts(order_refs=["200", "300"])

            saved = json.loads(cache_path.read_text(encoding="utf-8"))

        self.assertEqual(result["mode"], "incremental")
        self.assertEqual(result["refreshed_order_count"], 2)
        self.assertEqual(saved["orders"]["100"]["mats"], 3)
        self.assertEqual(saved["orders"]["200"]["mats"], 2)
        self.assertNotIn("300", saved["orders"])
        self.assertEqual(saved["record_count"], 5)
        find.assert_called_once()
        self.assertEqual(
            find.call_args.args[1],
            [{"Order Ref": "==200"}, {"Order Ref": "==300"}],
        )

    def test_incremental_request_falls_back_to_full_sync_without_existing_cache(self):
        with TemporaryDirectory() as directory:
            cache_path = Path(directory) / "mats.json"
            with (
                patch.object(mat_data, "read_filemaker_mat_counts_cache", return_value={"status": "not_synced", "orders": {}}),
                patch.object(mat_data, "fetch_layout_records", return_value={"status": "ok", "records": []}) as fetch,
                patch.object(mat_data, "find_layout_records") as find,
                patch.object(mat_data, "get_mat_counts_cache_path", return_value=cache_path),
            ):
                result = mat_data.sync_filemaker_mat_counts(order_refs=["200"])

        self.assertEqual(result["mode"], "full")
        fetch.assert_called_once()
        find.assert_not_called()


if __name__ == "__main__":
    unittest.main()
