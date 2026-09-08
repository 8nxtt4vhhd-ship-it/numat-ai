import os
import unittest
from unittest.mock import patch

import main


class StrategicContactPermissionTests(unittest.TestCase):
    def test_active_standard_user_can_manage_strategic_contacts(self):
        with patch.dict(os.environ, {"APP_LOGIN_ENABLED": "true"}):
            self.assertTrue(
                main.can_manage_strategic_contacts(
                    {"username": "peterb", "role": "user", "active": True}
                )
            )

    def test_inactive_user_cannot_manage_strategic_contacts(self):
        with patch.dict(os.environ, {"APP_LOGIN_ENABLED": "true"}):
            self.assertFalse(
                main.can_manage_strategic_contacts(
                    {"username": "former-user", "role": "user", "active": False}
                )
            )

    def test_signed_out_visitor_cannot_manage_strategic_contacts(self):
        with patch.dict(os.environ, {"APP_LOGIN_ENABLED": "true"}):
            self.assertFalse(main.can_manage_strategic_contacts(None))

    def test_disabled_app_login_preserves_unrestricted_local_mode(self):
        with patch.dict(os.environ, {"APP_LOGIN_ENABLED": "false"}):
            self.assertTrue(main.can_manage_strategic_contacts(None))


if __name__ == "__main__":
    unittest.main()
