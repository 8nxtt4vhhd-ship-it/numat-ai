from datetime import datetime
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

import calendar_sync


class FakeResponse:
    def __init__(self, status_code, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload or {}
        self.text = text

    def json(self):
        return self._payload


class CalendarSyncTests(unittest.TestCase):
    def setUp(self):
        calendar_sync.clear_calendar_cache()
        calendar_sync._CALENDAR_ID_CACHE.update({"value": "", "expires_at": 0})

    def test_event_payload_uses_uk_graph_timezone_and_attendees(self):
        result = calendar_sync.build_event_payload(
            subject="Customer visit",
            start_value="2026-08-20T09:00",
            end_value="2026-08-20T10:30",
            location="Bergen Linen",
            notes="Review repair programme",
            attendees="one@example.com; two@example.com, one@example.com",
        )
        self.assertEqual(result["status"], "ok")
        payload = result["payload"]
        self.assertEqual(payload["start"]["timeZone"], "GMT Standard Time")
        self.assertEqual(payload["end"]["dateTime"], "2026-08-20T10:30:00")
        self.assertEqual(
            [item["emailAddress"]["address"] for item in payload["attendees"]],
            ["one@example.com", "two@example.com"],
        )

    def test_event_payload_rejects_end_before_start(self):
        result = calendar_sync.build_event_payload(
            subject="Invalid",
            start_value="2026-08-20T10:00",
            end_value="2026-08-20T09:00",
        )
        self.assertEqual(result["status"], "invalid_datetime")

    def test_calendar_discovery_matches_name_case_insensitively(self):
        response = FakeResponse(200, {"value": [{"name": "Shared NuMat Calendar", "id": "calendar-id"}]})
        with patch.object(calendar_sync, "reporting_token", return_value={"status": "ok", "access_token": "token"}):
            with patch.object(calendar_sync.requests, "get", return_value=response):
                result = calendar_sync.discover_shared_calendar(force_refresh=True)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["calendar_id"], "calendar-id")

    def test_fetch_calendar_view_normalizes_event(self):
        response = FakeResponse(200, {"value": [{
            "id": "event-id",
            "subject": "Planned visit",
            "start": {"dateTime": "2026-08-20T09:00:00", "timeZone": "GMT Standard Time"},
            "end": {"dateTime": "2026-08-20T10:00:00", "timeZone": "GMT Standard Time"},
            "body": {"contentType": "text", "content": "Full notes"},
            "location": {"displayName": "Customer site"},
        }]})
        with patch.object(calendar_sync, "discover_shared_calendar", return_value={"status": "ok", "calendar_id": "calendar-id"}):
            with patch.object(calendar_sync, "reporting_token", return_value={"status": "ok", "access_token": "token"}):
                with patch.object(calendar_sync.requests, "get", return_value=response):
                    result = calendar_sync.fetch_calendar_events(
                        datetime(2026, 8, 1),
                        datetime(2026, 9, 1),
                        force_refresh=True,
                    )
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["events"][0]["subject"], "Planned visit")
        self.assertEqual(result["events"][0]["notes"], "Full notes")


if __name__ == "__main__":
    unittest.main()
