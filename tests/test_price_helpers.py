from datetime import datetime, timedelta
import unittest

from test_support import install_package_stub

install_package_stub()
from custom_components.smart_energy_planner.price_helpers import (
    aggregate_price_windows_to_hourly,
    extract_price_windows,
    extend_price_window_tail,
    infer_series_interval_minutes,
)
from custom_components.smart_energy_planner.price_models import PlannerWindow


class PriceHelpersTest(unittest.TestCase):
    def test_extract_price_windows_from_24_hour_series(self):
        now = datetime(2026, 5, 20, 12, 30)
        windows = extract_price_windows(
            {"today": list(range(24))},
            current_price=None,
            price_resolution="raw",
            now=now,
        )

        self.assertEqual(len(windows), 12)
        self.assertEqual(windows[0].start, datetime(2026, 5, 20, 12, 0))
        self.assertEqual(windows[-1].price, 23)

    def test_aggregate_price_windows_to_hourly_averages_subhourly_prices(self):
        start = datetime(2026, 5, 20, 10, 0)
        windows = [
            PlannerWindow(start=start, end=start + timedelta(minutes=30), price=0.10),
            PlannerWindow(start=start + timedelta(minutes=30), end=start + timedelta(hours=1), price=0.30),
        ]

        hourly = aggregate_price_windows_to_hourly(windows)

        self.assertEqual(len(hourly), 1)
        self.assertEqual(hourly[0].price, 0.20)

    def test_extend_price_window_tail_fills_to_horizon(self):
        start = datetime(2026, 5, 20, 10, 0)
        windows = [PlannerWindow(start=start, end=start + timedelta(hours=1), price=0.25)]

        extended = extend_price_window_tail(
            windows=windows,
            horizon_end=start + timedelta(hours=3),
            fallback_price=None,
        )

        self.assertEqual(len(extended), 3)
        self.assertEqual(extended[-1].end, start + timedelta(hours=3))
        self.assertTrue(all(window.price == 0.25 for window in extended))

    def test_infer_series_interval_minutes(self):
        self.assertEqual(infer_series_interval_minutes(24), 60)
        self.assertEqual(infer_series_interval_minutes(48), 30)
        self.assertEqual(infer_series_interval_minutes(96), 15)
        self.assertIsNone(infer_series_interval_minutes(12))
