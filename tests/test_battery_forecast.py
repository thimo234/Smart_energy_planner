from datetime import datetime, timedelta
import unittest

from test_support import install_package_stub

install_package_stub()
from custom_components.smart_energy_planner.battery_forecast import (
    build_fallback_solar_windows_for_day,
    build_hourly_home_demand_forecast,
    sum_remaining_home_demand_until,
    sum_remaining_solar_until,
)
from custom_components.smart_energy_planner.battery_models import SolarWindow


class BatteryForecastTest(unittest.TestCase):
    def test_sum_remaining_solar_until_scales_partial_overlap(self):
        now = datetime(2026, 5, 20, 10, 30)
        until = datetime(2026, 5, 20, 11, 0)
        windows = [
            SolarWindow(
                start=datetime(2026, 5, 20, 10, 0),
                end=datetime(2026, 5, 20, 11, 0),
                forecast_kwh=2.0,
                forecast_kwh_p10=None,
                forecast_kwh_p90=None,
            )
        ]

        self.assertEqual(sum_remaining_solar_until(windows, now, until), 1.0)

    def test_sum_remaining_home_demand_until_scales_partial_overlap(self):
        now = datetime(2026, 5, 20, 10, 15)
        until = datetime(2026, 5, 20, 10, 45)
        demand = [
            {
                "start": "2026-05-20T10:00:00",
                "end": "2026-05-20T11:00:00",
                "estimated_kwh": 1.2,
            }
        ]

        self.assertEqual(round(sum_remaining_home_demand_until(demand, now, until), 3), 0.6)

    def test_build_hourly_home_demand_forecast_covers_horizon_days(self):
        now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        forecast = build_hourly_home_demand_forecast(
            non_heating_daily_average_kwh=12.0,
            heating_estimate_kwh=0.0,
            horizon_end=now + timedelta(days=1),
        )

        self.assertEqual(len(forecast), 48)
        self.assertTrue(all(slot["estimated_kwh"] >= 0 for slot in forecast))

    def test_build_fallback_solar_windows_for_tomorrow_has_daylight_windows(self):
        windows = build_fallback_solar_windows_for_day(10.0, day_offset=1)

        self.assertTrue(windows)
        self.assertTrue(all(window.forecast_kwh > 0 for window in windows))
        self.assertEqual(round(sum(window.forecast_kwh for window in windows), 1), 10.0)
