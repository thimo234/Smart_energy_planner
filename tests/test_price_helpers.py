from datetime import datetime, timedelta
import unittest

from test_support import install_package_stub

install_package_stub()
from custom_components.smart_energy_planner.price_helpers import (
    aggregate_price_windows_to_hourly,
    extract_price_windows,
    extend_price_window_tail,
    infer_series_interval_minutes,
    select_contiguous_price_window,
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

    def test_select_contiguous_price_window_finds_cheapest_block(self):
        start = datetime(2026, 5, 20, 0, 0)
        windows = [
            PlannerWindow(start=start + timedelta(hours=index), end=start + timedelta(hours=index + 1), price=price)
            for index, price in enumerate([0.40, 0.30, 0.10, 0.20, 0.50])
        ]

        selected = select_contiguous_price_window(
            windows,
            now=start + timedelta(hours=12),
            duration_hours=2,
            cheapest=True,
            whole_hour_start=True,
        )

        self.assertIsNotNone(selected)
        self.assertEqual(selected["start"], (start + timedelta(hours=2)).isoformat())
        self.assertEqual(selected["end"], (start + timedelta(hours=4)).isoformat())
        self.assertEqual(selected["average_price"], 0.15)

    def test_select_contiguous_price_window_finds_most_expensive_block(self):
        start = datetime(2026, 5, 20, 0, 0)
        windows = [
            PlannerWindow(start=start + timedelta(hours=index), end=start + timedelta(hours=index + 1), price=price)
            for index, price in enumerate([0.40, 0.30, 0.10, 0.20, 0.50])
        ]

        selected = select_contiguous_price_window(
            windows,
            now=start + timedelta(hours=12),
            duration_hours=2,
            cheapest=False,
            whole_hour_start=True,
        )

        self.assertIsNotNone(selected)
        self.assertEqual(selected["start"], start.isoformat())
        self.assertEqual(selected["average_price"], 0.35)

    def test_select_contiguous_price_window_can_require_whole_hour_start(self):
        start = datetime(2026, 5, 20, 0, 0)
        windows = [
            PlannerWindow(
                start=start + timedelta(minutes=15 * index),
                end=start + timedelta(minutes=15 * (index + 1)),
                price=0.01 if index == 1 else 0.50,
            )
            for index in range(8)
        ]

        selected = select_contiguous_price_window(
            windows,
            now=start,
            duration_hours=0.25,
            cheapest=True,
            whole_hour_start=True,
        )

        self.assertIsNotNone(selected)
        self.assertEqual(selected["start"], start.isoformat())

    def test_select_contiguous_price_window_can_select_tomorrow(self):
        start = datetime(2026, 5, 20, 0, 0)
        windows = [
            PlannerWindow(
                start=start + timedelta(hours=index),
                end=start + timedelta(hours=index + 1),
                price=0.50,
            )
            for index in range(24)
        ] + [
            PlannerWindow(
                start=start + timedelta(days=1, hours=index),
                end=start + timedelta(days=1, hours=index + 1),
                price=price,
            )
            for index, price in enumerate([0.40, 0.30, 0.10, 0.20, 0.50])
        ]

        selected = select_contiguous_price_window(
            windows,
            now=start + timedelta(hours=12),
            duration_hours=2,
            cheapest=True,
            whole_hour_start=True,
            day_offset=1,
        )

        self.assertIsNotNone(selected)
        self.assertEqual(selected["start"], (start + timedelta(days=1, hours=2)).isoformat())
        self.assertEqual(selected["end"], (start + timedelta(days=1, hours=4)).isoformat())
        self.assertEqual(selected["average_price"], 0.15)

    def test_select_contiguous_price_window_returns_none_when_tomorrow_unavailable(self):
        start = datetime(2026, 5, 20, 0, 0)
        windows = [
            PlannerWindow(
                start=start + timedelta(hours=index),
                end=start + timedelta(hours=index + 1),
                price=0.50,
            )
            for index in range(24)
        ]

        selected = select_contiguous_price_window(
            windows,
            now=start + timedelta(hours=12),
            duration_hours=2,
            cheapest=True,
            whole_hour_start=True,
            day_offset=1,
        )

        self.assertIsNone(selected)
