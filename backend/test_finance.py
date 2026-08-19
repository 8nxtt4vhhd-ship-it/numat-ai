import unittest
from unittest.mock import patch

import finance


class FinanceTests(unittest.TestCase):
    def setUp(self):
        finance._AGED_DEBT_CACHE.update({"expires_at": 0, "result": None})

    def test_maps_filemaker_aged_debt_fields(self):
        source = {
            "status": "ok",
            "records": [{
                "recordId": "1",
                "fieldData": {
                    "Avg Debtor Days": "23.5",
                    "Current Total": "50243.85",
                    "Zero Thirty": "52800.05",
                    "Thirtyone Sixty": "16801.38",
                    "Sixtyone Ninty": "0",
                    "Ninty Plus": "-792.2",
                    "Total Outstanding": "119053.08",
                    "Last Refreshed": "8/19/2026 7:49:01 AM",
                    "PrimaryKey": "F7221803-FE84-E246-9080",
                },
            }],
        }
        with patch.object(finance, "fetch_all_layout_records", return_value=source):
            result = finance.fetch_aged_debt_summary(force_refresh=True)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["current"], 50243.85)
        self.assertEqual(result["ninety_plus"], -792.2)
        self.assertEqual(result["total_outstanding"], 119053.08)

    def test_missing_stored_record_is_reported(self):
        with patch.object(finance, "fetch_all_layout_records", return_value={"status": "fetch_failed", "records": []}):
            result = finance.fetch_aged_debt_summary(force_refresh=True)
        self.assertEqual(result["status"], "no_record")


if __name__ == "__main__":
    unittest.main()
