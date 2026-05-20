from datetime import datetime, timedelta
import unittest

from test_support import install_package_stub

install_package_stub()
from custom_components.smart_energy_planner.price_models import PlannerWindow
from custom_components.smart_energy_planner.thermostat_planner import (
    estimate_cooling_profile_from_model,
    find_next_valley_start,
    select_thermostat_peak_eco_windows,
)


class ThermostatPlannerTest(unittest.TestCase):
    def test_find_next_valley_start_after_expensive_period(self):
        base = datetime(2026, 5, 20, 10, 0)
        windows = [
            PlannerWindow(base, base + timedelta(hours=1), 0.10),
            PlannerWindow(base + timedelta(hours=1), base + timedelta(hours=2), 0.40),
            PlannerWindow(base + timedelta(hours=2), base + timedelta(hours=3), 0.12),
            PlannerWindow(base + timedelta(hours=3), base + timedelta(hours=4), 0.35),
        ]

        valley = find_next_valley_start(windows, base, average_price=0.20)

        self.assertEqual(valley, base + timedelta(hours=2))

    def test_select_thermostat_peak_eco_windows_picks_expensive_block(self):
        base = datetime(2026, 5, 20, 10, 0)
        windows = [
            PlannerWindow(base + timedelta(hours=index), base + timedelta(hours=index + 1), price)
            for index, price in enumerate([0.10, 0.35, 0.50, 0.45, 0.12])
        ]

        eco_windows = select_thermostat_peak_eco_windows(
            windows=windows,
            now=base,
            cooldown_hours=2.0,
            expensive_threshold=0.30,
        )

        self.assertEqual(len(eco_windows), 1)
        self.assertEqual(eco_windows[0]["start"], base + timedelta(hours=2))
        self.assertEqual(eco_windows[0]["end"], base + timedelta(hours=4))

    def test_estimate_cooling_profile_uses_learned_model_when_available(self):
        rate, hours = estimate_cooling_profile_from_model(
            cooling_model={
                "rolling_cooling_factor": 0.05,
                "eco_sample_count": 3,
                "last_eco_duration_hours": 4.0,
            },
            outdoor_temperature_c=10.0,
            room_temperature_c=22.0,
            cooldown_delta_c=3.0,
        )

        self.assertGreater(rate, 0)
        self.assertGreaterEqual(hours, 4.0)
