import unittest
from unittest.mock import patch

import main


class NavigationTests(unittest.TestCase):
    def render_for(self, title, user):
        token = main.CURRENT_SESSION_USER.set(user)
        try:
            with patch.dict("os.environ", {"APP_LOGIN_ENABLED": "true"}):
                return main.render_global_nav(title)
        finally:
            main.CURRENT_SESSION_USER.reset(token)

    def test_regular_user_gets_module_navigation_without_admin(self):
        html = self.render_for(
            "Customers",
            {"username": "rep", "display_name": "Sales Rep", "role": "user", "active": True},
        )

        self.assertIn('aria-label="Main navigation"', html)
        self.assertIn('class="nav-module-menu active"', html)
        self.assertIn('href="/customers-view"', html)
        self.assertNotIn('class="module-subnav"', html)
        self.assertNotIn('class="nav-admin-menu"', html)
        self.assertIn('<span class="nav-user-role">User</span>', html)

    def test_admin_gets_admin_menu_and_role_label(self):
        html = self.render_for(
            "System Settings",
            {"username": "admin", "display_name": "Admin User", "role": "admin", "active": True},
        )

        self.assertIn('class="nav-admin-menu"', html)
        self.assertIn('<span class="nav-user-role">Administrator</span>', html)
        self.assertIn('href="/admin-settings"', html)

    def test_each_workspace_is_available_from_dropdowns(self):
        user = {"username": "rep", "display_name": "Sales Rep", "role": "user", "active": True}

        sales_html = self.render_for("CRM Activities", user)
        analysis_html = self.render_for("Insights", user)
        management_html = self.render_for("Production Analysis", user)

        self.assertIn('href="/crm-activities-view"', sales_html)
        self.assertIn('href="/insights-view"', analysis_html)
        self.assertIn('href="/ask-data-view"', analysis_html)
        self.assertIn('href="/production-analysis-view"', management_html)
        self.assertEqual(sales_html.count('class="nav-module-menu active"'), 1)

    def test_page_closes_navigation_menus_when_clicking_elsewhere(self):
        token = main.CURRENT_SESSION_USER.set(
            {"username": "rep", "display_name": "Sales Rep", "role": "user", "active": True}
        )
        try:
            with patch.dict("os.environ", {"APP_LOGIN_ENABLED": "true"}):
                html = main.render_page("Customers", "<p>Body</p>")
        finally:
            main.CURRENT_SESSION_USER.reset(token)

        self.assertIn('document.addEventListener("click"', html)
        self.assertIn('!menu.contains(event.target)', html)
        self.assertIn('event.key === "Escape"', html)
        self.assertIn('<header class="app-header">', html)
        self.assertLess(html.index('<header class="app-header">'), html.index('<main class="">'))

    def test_home_uses_workspace_banner_without_ask_data_panel(self):
        html = main.render_dashboard_home()

        self.assertIn('<h1>Numat Systems</h1>', html)
        self.assertIn('Choose a workspace to begin.', html)
        self.assertNotIn('class="panel home-chat-panel"', html)

    def test_ask_data_has_its_own_analysis_page(self):
        html = main.render_ask_data_page()

        self.assertIn('<title>Ask The Data</title>', html)
        self.assertIn('action="/ask-data-view"', html)
        self.assertIn('href="/ask-data-view">Clear</a>', html)


if __name__ == "__main__":
    unittest.main()
