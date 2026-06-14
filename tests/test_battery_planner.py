import unittest
from datetime import datetime, timedelta

from test_support import install_package_stub

install_package_stub()
from custom_components.smart_energy_planner.battery_planner import (
    BATTERY_MODE_GRID_CHARGE,
    BATTERY_MODE_OFF,
    BATTERY_MODE_SOLAR_CHARGE,
    collapse_short_off_mode_windows,
    normalize_full_battery_charge_mode,
    normalize_full_battery_mode_windows,
)


class BatteryPlannerTest(unittest.TestCase):
    def test_full_battery_grid_charge_becomes_solar_hold(self):
        mode = normalize_full_battery_charge_mode(
            mode=BATTERY_MODE_GRID_CHARGE,
            usable_energy_kwh=8.0,
            usable_capacity_kwh=8.0,
        )

        self.assertEqual(mode, BATTERY_MODE_SOLAR_CHARGE)

    def test_full_battery_mode_windows_convert_grid_charge(self):
        windows = [
            {
                "start": "2026-05-31T10:00:00",
                "end": "2026-05-31T11:00:00",
                "mode": BATTERY_MODE_GRID_CHARGE,
                "price": 0.12,
                "usable_hours": 1.0,
            }
        ]

        normalized = normalize_full_battery_mode_windows(
            windows=windows,
            usable_energy_kwh=8.0,
            usable_capacity_kwh=8.0,
        )

        self.assertEqual(normalized[0]["mode"], BATTERY_MODE_SOLAR_CHARGE)
        self.assertEqual(windows[0]["mode"], BATTERY_MODE_GRID_CHARGE)

    def test_short_off_mode_window_collapses_into_next_active_mode(self):
        start = datetime(2026, 6, 14, 10, 0)
        windows = [
            {
                "start": start,
                "end": start + timedelta(minutes=15),
                "mode": BATTERY_MODE_OFF,
                "price": 0.10,
                "usable_hours": 0.25,
            },
            {
                "start": start + timedelta(minutes=15),
                "end": start + timedelta(hours=1),
                "mode": BATTERY_MODE_SOLAR_CHARGE,
                "price": 0.10,
                "usable_hours": 0.75,
            },
        ]

        collapsed = collapse_short_off_mode_windows(windows)

        self.assertEqual(len(collapsed), 1)
        self.assertEqual(collapsed[0]["start"], start)
        self.assertEqual(collapsed[0]["end"], start + timedelta(hours=1))
        self.assertEqual(collapsed[0]["mode"], BATTERY_MODE_SOLAR_CHARGE)

    def test_long_off_mode_window_is_kept(self):
        start = datetime(2026, 6, 14, 10, 0)
        windows = [
            {
                "start": start,
                "end": start + timedelta(hours=1),
                "mode": BATTERY_MODE_OFF,
                "price": 0.10,
                "usable_hours": 1.0,
            },
            {
                "start": start + timedelta(hours=1),
                "end": start + timedelta(hours=2),
                "mode": BATTERY_MODE_SOLAR_CHARGE,
                "price": 0.10,
                "usable_hours": 1.0,
            },
        ]

        collapsed = collapse_short_off_mode_windows(windows)

        self.assertEqual(len(collapsed), 2)
        self.assertEqual(collapsed[0]["mode"], BATTERY_MODE_OFF)

