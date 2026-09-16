import unittest
from unittest.mock import patch

import main


class LoginMfaTests(unittest.TestCase):
    def test_mfa_sender_account_owner_still_requires_mfa(self):
        user = {
            "username": "trudy",
            "m365_email": "customerservice@numatsystems.com",
            "active": True,
        }

        with patch.object(main, "get_login_mfa_enabled", return_value=True), patch.object(
            main,
            "get_login_mfa_sender_username",
            return_value="trudy",
        ):
            self.assertTrue(main.user_requires_email_mfa(user))

    def test_inactive_user_does_not_start_mfa(self):
        with patch.object(main, "get_login_mfa_enabled", return_value=True):
            self.assertFalse(
                main.user_requires_email_mfa({"username": "trudy", "active": False})
            )

    def test_global_mfa_switch_is_respected(self):
        with patch.object(main, "get_login_mfa_enabled", return_value=False):
            self.assertFalse(
                main.user_requires_email_mfa({"username": "trudy", "active": True})
            )


if __name__ == "__main__":
    unittest.main()
