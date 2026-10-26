import unittest
from datetime import datetime

import main


class CalendarAgendaViewTests(unittest.TestCase):
    def test_multiple_events_share_the_agenda_event_column(self):
        events = [
            {
                "id": "one",
                "start_date": "2026-10-12",
                "start": "2026-10-12T14:00:00+01:00",
                "end": "2026-10-12T14:30:00+01:00",
                "subject": "First event",
            },
            {
                "id": "two",
                "start_date": "2026-10-12",
                "start": "2026-10-12T18:30:00+01:00",
                "end": "2026-10-12T20:30:00+01:00",
                "subject": "Second event",
            },
        ]

        html = main.render_calendar_agenda(events, datetime(2026, 10, 1))

        self.assertEqual(html.count("calendar-agenda-day"), 1)
        self.assertEqual(html.count("calendar-agenda-event"), 3)  # wrapper plus two cards
        self.assertIn(
            "<div class='calendar-agenda-events'><a class=\"calendar-agenda-event\"",
            html,
        )
        self.assertIn("First event", html)
        self.assertIn("Second event", html)


if __name__ == "__main__":
    unittest.main()
