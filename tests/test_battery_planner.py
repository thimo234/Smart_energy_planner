import unittest

from test_support import install_package_stub

install_package_stub()
from custom_components.smart_energy_planner.battery_planner import (
    BATTERY_MODE_GRID_CHARGE,
    BATTERY_MODE_SOLAR_CHARGE,
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

