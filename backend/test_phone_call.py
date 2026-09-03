import unittest
from unittest.mock import patch

import main
from main import build_filemaker_phone_call_fields


class PhoneCallActivityTests(unittest.TestCase):
    def test_filemaker_call_activity_uses_email_layout_fields(self):
        fields = build_filemaker_phone_call_fields(
            "pbinnington@numatsystems.com",
            "rharris612@icloud.com",
            "Re: CSC Mat Repair Program",
            "Spoke with the customer and agreed to follow up next week.",
        )

        self.assertEqual(fields, {
            "body": "Spoke with the customer and agreed to follow up next week.",
            "subject": "Re: CSC Mat Repair Program",
            "CRM Category": "",
            "CRM Type": "Telephone Call",
            "sender_email": "pbinnington@numatsystems.com",
            "To": "rharris612@icloud.com",
        })

    def test_successful_call_write_returns_to_action_plan(self):
        with patch.object(main, "get_current_session_user", return_value={
            "username": "ben",
            "m365_email": "pbinnington@numatsystems.com",
        }):
            with patch.object(main, "get_m365_connection_state", return_value={"connected": False}):
                with patch.object(main, "create_layout_record", return_value={"status": "ok", "record_id": "123"}) as create:
                    with patch.object(main, "record_audit_event"):
                        response = main.post_filemaker_record_call(
                            customer="Customer Ltd",
                            return_to="/action-plan-view?selected=Customer+Ltd",
                            to="contact@example.com",
                            subject="Telephone call — Customer Ltd",
                            body="Discussed the next repair order.",
                            crm_category="",
                        )

        self.assertEqual(response.status_code, 303)
        self.assertIn("Telephone%20call%20recorded%20in%20FileMaker", response.headers["location"])
        fields = create.call_args.args[1]
        self.assertEqual(fields["sender_email"], "pbinnington@numatsystems.com")
        self.assertEqual(fields["To"], "contact@example.com")
        self.assertEqual(fields["CRM Type"], "Telephone Call")


if __name__ == "__main__":
    unittest.main()
