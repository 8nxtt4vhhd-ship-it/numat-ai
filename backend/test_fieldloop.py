from datetime import datetime, timezone
import os
import unittest
from unittest.mock import patch

import fieldloop


class FieldLoopTests(unittest.TestCase):
    def valid_payload(self):
        return {
            "customer_id": "customer-pk-123",
            "customer_name": "Example Customer",
            "recipient_email": "buyer@example.com",
            "sender_email": "ben@example.com",
            "sender_name": "Ben",
            "note": "Discussed the upcoming requirement.",
            "idempotency_key": "fieldloop-meeting-123",
        }

    def test_authentication_requires_configured_matching_bearer_key(self):
        with patch.dict(os.environ, {"FIELDLOOP_API_KEY": "expected-secret"}):
            self.assertEqual(fieldloop.authenticate_fieldloop_request("Bearer expected-secret"), "ok")
            self.assertEqual(fieldloop.authenticate_fieldloop_request("Bearer wrong-secret"), "unauthorized")
            self.assertEqual(fieldloop.authenticate_fieldloop_request(""), "unauthorized")

    def test_payload_validation_normalizes_email_addresses(self):
        payload = self.valid_payload()
        payload["recipient_email"] = " Buyer@Example.com "
        result = fieldloop.validate_visit_note_payload(payload)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["values"]["recipient_email"], "buyer@example.com")

    def test_payload_validation_rejects_invalid_email_and_short_idempotency_key(self):
        payload = self.valid_payload()
        payload["recipient_email"] = "not-an-email"
        payload["idempotency_key"] = "short"
        result = fieldloop.validate_visit_note_payload(payload)
        self.assertEqual(result["status"], "invalid")
        self.assertEqual(len(result["errors"]), 2)

    def test_filemaker_mapping_uses_utc_timestamp_and_fixed_crm_values(self):
        values = fieldloop.validate_visit_note_payload(self.valid_payload())["values"]
        result = fieldloop.build_filemaker_field_data(
            values,
            now=datetime(2026, 8, 21, 13, 4, 7, tzinfo=timezone.utc),
        )
        self.assertEqual(result["sentDateTime"], "2026-08-21T13:04:07Z")
        self.assertEqual(result["To"], "buyer@example.com")
        self.assertEqual(result["CRM Category"], "Sales")
        self.assertEqual(result["CRM Type"], "Meeting")
        self.assertNotIn("CRM Reference", result)

    def test_rate_limit_rejects_requests_over_configured_maximum(self):
        fieldloop.RATE_LIMIT_REQUESTS.clear()
        with patch.dict(os.environ, {"FIELDLOOP_REQUESTS_PER_MINUTE": "2"}):
            self.assertTrue(fieldloop.check_fieldloop_rate_limit(now=100))
            self.assertTrue(fieldloop.check_fieldloop_rate_limit(now=101))
            self.assertFalse(fieldloop.check_fieldloop_rate_limit(now=102))
            self.assertTrue(fieldloop.check_fieldloop_rate_limit(now=161))


if __name__ == "__main__":
    unittest.main()
