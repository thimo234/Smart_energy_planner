from datetime import datetime, timedelta
import unittest

from test_support import install_package_stub

install_package_stub()
from custom_components.smart_energy_planner.battery_forecast import (
    align_price_responsive_demand_to_cheap_hours,
    build_expected_hourly_demand_table,
    build_expected_hourly_demand_table_from_observations,
    build_fallback_solar_windows_for_day,
    build_hourly_home_demand_forecast,
    observed_hourly_demand_table,
    sum_remaining_home_demand_until,
    sum_remaining_solar_until,
    update_expected_hourly_demand_stats,
)
from custom_components.smart_energy_planner.battery_models import SolarWindow
from custom_components.smart_energy_planner.price_models import PlannerWindow


class BatteryForecastTest(unittest.TestCase):
    def test_price_responsive_demand_moves_peak_to_cheapest_hour(self):
        day = datetime(2026, 6, 29)
        demand = []
        prices = []
        for hour in range(24):
            start = day + timedelta(hours=hour)
            demand.append(
                {
                    "start": start.isoformat(),
                    "end": (start + timedelta(hours=1)).isoformat(),
                    "estimated_kwh": 3.0 if hour == 20 else 0.5,
                }
            )
            prices.append(
                PlannerWindow(
                    start=start,
                    end=start + timedelta(hours=1),
                    price=0.10 if hour == 3 else 0.45,
                )
            )

        adjusted = align_price_responsive_demand_to_cheap_hours(demand, prices)
        adjusted_values = [float(slot["estimated_kwh"]) for slot in adjusted]

        self.assertLess(adjusted_values[20], 3.0)
        self.assertGreater(adjusted_values[3], 0.5)
        self.assertEqual(round(sum(adjusted_values), 3), round(sum(float(slot["estimated_kwh"]) for slot in demand), 3))

    def test_price_responsive_demand_reduces_expensive_hours_even_for_flat_profile(self):
        day = datetime(2026, 6, 29)
        demand = []
        prices = []
        for hour in range(24):
            start = day + timedelta(hours=hour)
            demand.append(
                {
                    "start": start.isoformat(),
                    "end": (start + timedelta(hours=1)).isoformat(),
                    "estimated_kwh": 0.7,
                }
            )
            prices.append(PlannerWindow(start=start, end=start + timedelta(hours=1), price=0.10 + hour))

        adjusted = align_price_responsive_demand_to_cheap_hours(demand, prices)
        adjusted_values = [float(slot["estimated_kwh"]) for slot in adjusted]

        self.assertLess(adjusted_values[23], 0.7)
        self.assertGreater(adjusted_values[0], 0.7)
        self.assertEqual(round(sum(adjusted_values), 3), round(sum(float(slot["estimated_kwh"]) for slot in demand), 3))

    def test_price_responsive_demand_keeps_flat_profile_when_prices_are_flat(self):
        day = datetime(2026, 6, 29)
        demand = []
        prices = []
        for hour in range(24):
            start = day + timedelta(hours=hour)
            demand.append(
                {
                    "start": start.isoformat(),
                    "end": (start + timedelta(hours=1)).isoformat(),
                    "estimated_kwh": 0.7,
                }
            )
            prices.append(PlannerWindow(start=start, end=start + timedelta(hours=1), price=0.20))

        adjusted = align_price_responsive_demand_to_cheap_hours(demand, prices)

        self.assertEqual(adjusted, demand)

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

    def test_build_hourly_home_demand_forecast_uses_profile_fallback(self):
        now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        forecast = build_hourly_home_demand_forecast(
            non_heating_daily_average_kwh=24.0,
            heating_estimate_kwh=0.0,
            horizon_end=now + timedelta(days=1),
        )

        values = [float(slot["estimated_kwh"]) for slot in forecast[:24]]
        self.assertGreater(max(values) - min(values), 0.5)
        self.assertEqual(round(sum(values), 1), 24.0)

    def test_build_hourly_home_demand_forecast_uses_same_hour_history_fallback(self):
        now = datetime.now().astimezone()
        opposite_day_type_weekday = 0 if now.weekday() >= 5 else 5
        hourly = {str(opposite_day_type_weekday * 24 + hour): 0.5 for hour in range(24)}
        hourly[str(opposite_day_type_weekday * 24 + 10)] = 1.8
        forecast = build_hourly_home_demand_forecast(
            non_heating_daily_average_kwh=13.3,
            heating_estimate_kwh=0.0,
            horizon_end=now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(hours=24),
            hourly_demand_table=hourly,
        )

        ten_o_clock = forecast[10]
        self.assertEqual(ten_o_clock["estimated_kwh"], 1.8)

    def test_build_hourly_home_demand_forecast_prefers_similar_day_type_fallback(self):
        now = datetime.now().astimezone()
        weekday = now.weekday()
        similar_weekday = (6 if weekday == 5 else 5) if weekday >= 5 else (1 if weekday == 0 else 0)
        hourly = {str(similar_weekday * 24 + hour): 0.5 for hour in range(24)}
        hourly[str(similar_weekday * 24 + 10)] = 1.5
        forecast = build_hourly_home_demand_forecast(
            non_heating_daily_average_kwh=13.0,
            heating_estimate_kwh=0.0,
            horizon_end=now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(hours=24),
            hourly_demand_table=hourly,
        )

        ten_o_clock = forecast[10]
        self.assertEqual(ten_o_clock["estimated_kwh"], 1.5)

    def test_build_hourly_home_demand_forecast_prefers_exact_slot(self):
        now = datetime.now().astimezone()
        weekday = now.weekday()
        other_weekday = (weekday - 1) % 7
        hourly = {
            **{str(weekday * 24 + hour): 0.5 for hour in range(24)},
            str(weekday * 24 + 10): 2.0,
            str(other_weekday * 24 + 10): 1.0,
        }
        forecast = build_hourly_home_demand_forecast(
            non_heating_daily_average_kwh=13.5,
            heating_estimate_kwh=0.0,
            horizon_end=now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(hours=24),
            hourly_demand_table=hourly,
        )

        ten_o_clock = forecast[10]
        self.assertEqual(ten_o_clock["estimated_kwh"], 2.0)

    def test_build_hourly_home_demand_forecast_applies_bounded_today_adjustment(self):
        now = datetime.now().astimezone()
        hourly = {str(now.weekday() * 24 + hour): 1.0 for hour in range(24)}
        forecast = build_hourly_home_demand_forecast(
            non_heating_daily_average_kwh=24.0,
            heating_estimate_kwh=0.0,
            horizon_end=now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(hours=24),
            hourly_demand_table=hourly,
            demand_adjustment_factor=2.0,
        )

        first_slot = forecast[0]
        self.assertEqual(first_slot["estimated_kwh"], 1.35)

    def test_build_hourly_home_demand_forecast_matches_daily_average(self):
        now = datetime.now().astimezone()
        weekday = now.weekday()
        hourly = {
            str(weekday * 24): 2.0,
            str(weekday * 24 + 1): 1.5,
        }

        forecast = build_hourly_home_demand_forecast(
            non_heating_daily_average_kwh=6.0,
            heating_estimate_kwh=0.0,
            horizon_end=now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(hours=24),
            hourly_demand_table=hourly,
        )

        today_total = sum(float(slot["estimated_kwh"]) for slot in forecast[:24])
        self.assertEqual(round(today_total, 1), 6.0)

    def test_build_hourly_home_demand_forecast_only_adjusts_today(self):
        now = datetime.now().astimezone()
        hourly = {str(weekday * 24 + hour): 1.0 for weekday in range(7) for hour in range(24)}

        forecast = build_hourly_home_demand_forecast(
            non_heating_daily_average_kwh=24.0,
            heating_estimate_kwh=0.0,
            horizon_end=now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1),
            hourly_demand_table=hourly,
            demand_adjustment_factor=1.35,
        )

        today_total = sum(float(slot["estimated_kwh"]) for slot in forecast[:24])
        tomorrow_total = sum(float(slot["estimated_kwh"]) for slot in forecast[24:48])
        self.assertEqual(round(today_total, 1), 32.4)
        self.assertEqual(round(tomorrow_total, 1), 24.0)

    def test_forecast_ignores_synthetic_filled_slots(self):
        now = datetime.now().astimezone()
        tomorrow_weekday = (now.weekday() + 1) % 7
        observed_weekday = now.weekday()
        table = {
            str(weekday * 24 + hour): 1.2
            for weekday in range(7)
            for hour in range(24)
        }
        observed_slots = []
        for hour in range(24):
            slot = str(observed_weekday * 24 + hour)
            table[slot] = 0.3 + (hour * 0.03)
            observed_slots.append(slot)

        forecast = build_hourly_home_demand_forecast(
            non_heating_daily_average_kwh=18.0,
            heating_estimate_kwh=0.0,
            hourly_demand_table=observed_hourly_demand_table(table, observed_slots),
            horizon_end=now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=2),
        )

        tomorrow_values = [
            float(slot["estimated_kwh"])
            for slot in forecast
            if datetime.fromisoformat(str(slot["start"])).weekday() == tomorrow_weekday
        ]
        self.assertGreater(max(tomorrow_values) - min(tomorrow_values), 0.5)

    def test_forecast_uses_full_planned_table_after_midnight(self):
        now = datetime.now().astimezone()
        tomorrow_weekday = (now.weekday() + 1) % 7
        table = {
            str(weekday * 24 + hour): 0.4
            for weekday in range(7)
            for hour in range(24)
        }
        for hour in range(24):
            table[str(tomorrow_weekday * 24 + hour)] = 0.25 + (hour * 0.04)

        forecast = build_hourly_home_demand_forecast(
            non_heating_daily_average_kwh=18.0,
            heating_estimate_kwh=0.0,
            hourly_demand_table=observed_hourly_demand_table(table, []),
            horizon_end=now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=2),
        )

        tomorrow_values = [
            float(slot["estimated_kwh"])
            for slot in forecast
            if datetime.fromisoformat(str(slot["start"])).weekday() == tomorrow_weekday
        ]
        self.assertGreater(max(tomorrow_values) - min(tomorrow_values), 0.5)

    def test_forecast_keeps_flat_planned_table_when_all_slots_are_available(self):
        now = datetime.now().astimezone()
        tomorrow_weekday = (now.weekday() + 1) % 7
        table = {
            str(weekday * 24 + hour): 1.205
            for weekday in range(7)
            for hour in range(24)
        }

        forecast = build_hourly_home_demand_forecast(
            non_heating_daily_average_kwh=28.92,
            heating_estimate_kwh=0.0,
            hourly_demand_table=observed_hourly_demand_table(table, table.keys()),
            horizon_end=now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=2),
        )

        tomorrow_values = [
            float(slot["estimated_kwh"])
            for slot in forecast
            if datetime.fromisoformat(str(slot["start"])).weekday() == tomorrow_weekday
        ]
        self.assertEqual(tomorrow_values, [1.205] * 24)

    def test_build_expected_hourly_demand_table_learns_same_hour_pattern(self):
        table = {
            "10": 0.7,
            str(1 * 24 + 10): 0.8,
        }

        expected = build_expected_hourly_demand_table_from_observations(
            table,
            observed_slots=table.keys(),
            daily_average_kwh=12.0,
        )

        self.assertEqual(len(expected), 168)
        self.assertGreater(expected["10"], expected["11"])
        self.assertGreater(expected[str(2 * 24 + 10)], expected[str(2 * 24 + 11)])

    def test_build_expected_hourly_demand_table_does_not_flatten_from_single_value(self):
        expected = build_expected_hourly_demand_table_from_observations(
            {"10": 1.2},
            observed_slots=["10"],
            daily_average_kwh=24.0,
        )

        self.assertEqual(len(expected), 168)
        self.assertNotEqual(set(expected.values()), {1.2})
        self.assertGreater(max(expected.values()) - min(expected.values()), 0.4)

    def test_build_expected_hourly_demand_table_tempers_sparse_high_outlier(self):
        table = {
            str(0 * 24 + 18): 0.4,
            str(1 * 24 + 18): 0.5,
            str(2 * 24 + 18): 3.0,
        }

        expected = build_expected_hourly_demand_table_from_observations(
            table,
            observed_slots=table.keys(),
            daily_average_kwh=12.0,
        )

        self.assertLess(expected[str(2 * 24 + 18)], 1.0)

    def test_expected_hourly_demand_stats_store_compact_running_means(self):
        stats = {}
        stats = update_expected_hourly_demand_stats(stats, slot_key="10", measured_kwh=1.0)
        stats = update_expected_hourly_demand_stats(stats, slot_key="10", measured_kwh=2.0)
        expected = build_expected_hourly_demand_table(stats, daily_average_kwh=24.0)

        self.assertEqual(stats["10"]["count"], 2)
        self.assertLess(len(str(stats)), 100)
        self.assertGreater(expected["10"], expected["11"])

    def test_build_fallback_solar_windows_for_tomorrow_has_daylight_windows(self):
        windows = build_fallback_solar_windows_for_day(10.0, day_offset=1)

        self.assertTrue(windows)
        self.assertTrue(all(window.forecast_kwh > 0 for window in windows))
        self.assertEqual(round(sum(window.forecast_kwh for window in windows), 1), 10.0)
