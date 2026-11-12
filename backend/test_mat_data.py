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


if __name__ == "__main__":
    unittest.main()
