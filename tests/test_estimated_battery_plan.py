"""Tomorrow's provisional plan must not alter today's executable strategy."""
from datetime import datetime, timedelta
from unittest.mock import patch
import unittest

from test_battery_energy_accounting import replay_full_plan, coordinator, energy_trace
from test_battery_price_priority import scenario
from custom_components.smart_energy_planner.price_helpers import extend_price_window_tail
from custom_components.smart_energy_planner.price_models import PlannerWindow


class EstimatedBatteryPlanTest(unittest.TestCase):
    def test_estimated_negative_prices_cannot_release_live_reserve(self):
        now, slots = scenario(grid_price=-.05, simultaneous_solar=True)
        for slot in slots:
            slot['price_known'] = False
        solar, grid = coordinator()._plan_charge_windows_for_horizon(
            slots=slots, now=now, usable_capacity_kwh=8, current_remaining_capacity_kwh=8,
            max_charge_kw=2, max_discharge_kw=2, battery_min_profit=.08,
        )
        self.assertEqual(solar, [])
        self.assertEqual(grid, [])

    def test_duration_weighted_day_mean_not_last_price(self):
        start = datetime(2026, 9, 25)
        windows = [PlannerWindow(start-timedelta(days=2), start-timedelta(days=1), 9.),
                   PlannerWindow(start, start+timedelta(hours=.5), .1),
                   PlannerWindow(start+timedelta(hours=.5), start+timedelta(hours=2), .3)]
        extended = extend_price_window_tail(windows=windows, horizon_end=start+timedelta(hours=4), fallback_price=.8)
        self.assertEqual(extended[:3], windows)
        for w in extended[3:]:
            self.assertAlmostEqual(w.price, .25)
            self.assertFalse(w.price_known)
        again = extend_price_window_tail(windows=extended, horizon_end=start+timedelta(hours=5), fallback_price=.8)
        self.assertAlmostEqual(again[-1].price, .25)

    def test_negative_day_mean_remains_negative(self):
        start = datetime(2026, 9, 25)
        windows = [PlannerWindow(start, start+timedelta(hours=12), -.1),
                   PlannerWindow(start+timedelta(hours=12), start+timedelta(days=1), 0)]
        extended = extend_price_window_tail(windows=windows, horizon_end=start+timedelta(days=2), fallback_price=.2)
        self.assertAlmostEqual(extended[-1].price, -.05)

    def test_preview_shows_solar_and_evening_without_affecting_current_control(self):
        args = ('2026_09_24_cheap_solar', '2026-09-24T20:04:29.361358+02:00')
        options = dict(soc=28, reserve=60, max_charge=2.5, tax_deduction=.11, estimate_tomorrow=True)
        control, shown = coordinator(), coordinator()
        control._charge_session_started = shown._charge_session_started = False
        control._discharge_session_started = shown._discharge_session_started = True
        with patch.object(type(control), '_estimate_unknown_price_plan', return_value=[]):
            _, _, baseline = replay_full_plan(*args, **options, instance=control)
        now, slots, result = replay_full_plan(*args, **options, instance=shown)
        self.assertEqual(result.battery_strategy, baseline.battery_strategy)
        self.assertEqual(result.planned_battery_mode_windows, baseline.planned_battery_mode_windows)
        self.assertEqual(result.battery_reserved_energy_kwh, baseline.battery_reserved_energy_kwh)
        self.assertEqual(shown._charge_session_started, control._charge_session_started)
        self.assertEqual(shown._discharge_session_started, control._discharge_session_started)
        estimated = result.estimated_battery_mode_windows
        self.assertTrue(all(w['price_estimated'] for w in estimated))
        self.assertTrue(any(w['mode'] == 'laden_met_zonne_energie' for w in estimated))
        self.assertTrue(any(w['mode'] == 'ontladen' and 'T19:' in w['start'] for w in estimated))
        self.assertTrue(estimated[-1]['end'].startswith('2026-09-26T00:00'))
        start = estimated[0]['start']
        prefix = [{**w, 'end': min(w['end'], start)} for w in result.planned_battery_mode_windows if w['start'] < start]
        trace = energy_trace(now, slots, prefix+estimated, initial=2.8, max_charge=2.5)
        self.assertGreaterEqual(min(row[1] for row in trace), 2-.005)
        self.assertLessEqual(max(row[1] for row in trace), 10+.005)

    def test_real_prices_replace_estimated_plan(self):
        _, _, result = replay_full_plan('2026_09_24_cheap_solar', '2026-09-24T20:04:29.361358+02:00',
                                        soc=28, discharging=True, reserve=60, tax_deduction=.11)
        self.assertEqual(result.estimated_battery_mode_windows, [])
