"""Tomorrow's executable refill and its outlook must reach physical capacity."""
from datetime import datetime, timedelta
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from test_battery_energy_accounting import coordinator, energy_trace
from custom_components.smart_energy_planner.price_models import PlannerWindow
from custom_components.smart_energy_planner.battery_models import SolarWindow
from custom_components.smart_energy_planner.battery_forecast import build_energy_balance_slots


def replay_tomorrow_fill():
    data = json.loads((Path(__file__).parent / 'fixtures/battery_2026_11_12_incomplete_tomorrow.json').read_text())
    a = data['attributes']
    now = datetime.fromisoformat(a['planned_battery_mode_windows'][0]['start'])
    prices = [PlannerWindow(datetime.fromisoformat(w['start']), datetime.fromisoformat(w['end']),
                            w['price'], w.get('price_known', True)) for w in a['upcoming_energy_price_windows']]
    exports = [PlannerWindow(datetime.fromisoformat(w['start']), datetime.fromisoformat(w['end']),
                             w['price'], w.get('price_known', True)) for w in a['upcoming_export_price_windows']]
    solar = [SolarWindow(datetime.fromisoformat(w['start']), datetime.fromisoformat(w['end']),
                         w['estimated_kwh'], None, None) for w in a['estimated_hourly_solar_forecast']]
    demand = a['estimated_hourly_home_demand']
    slots = build_energy_balance_slots(price_windows=prices, export_price_windows=exports,
                                       solar_windows=solar, hourly_demand=demand,
                                       horizon_start=now, demand_safety_margin=0.)
    c = coordinator()
    c._charge_session_started = a['battery_charge_cycle_active']
    c._discharge_session_started = a['battery_discharge_cycle_active']
    c._battery_cycle_state_initialized = True
    c._battery_cycle_state_restored_recent = False
    c._store_battery_cycle_state_snapshot = lambda now: None
    c.config_entry = SimpleNamespace(data={
        'battery_enabled': True, 'battery_capacity_kwh': 10., 'battery_min_soc_percent': 20.,
        'battery_no_charge_min_soc_percent': a['battery_no_charge_min_soc_percent'],
        'battery_max_charge_kw': 2.5, 'battery_max_discharge_kw': 3.,
        'battery_min_profit_per_kwh': a['battery_min_profit_per_kwh'],
        'battery_demand_safety_margin': 0., 'battery_charge_safety_margin': 0.,
    }, options={})
    namespace = c._build_plan_for_horizon.__globals__
    with patch.object(namespace['dt_util'], 'now', return_value=now), patch.dict(namespace, {
        'build_hourly_home_demand_forecast': lambda **kwargs: demand,
        'align_price_responsive_demand_to_cheap_hours': lambda demand, prices, **kwargs: demand,
    }):
        result = c._build_plan(
            planner_kind='battery', windows=prices, all_windows=prices,
            export_windows=exports, all_export_windows=exports, battery_switch_windows=prices,
            price_average=None, export_price_average=None, current_price=a['current_price'],
            solar_forecast_kwh=sum(w.forecast_kwh for w in solar if w.start.date() == now.date()),
            solar_windows=solar, all_solar_windows=solar, solcast_confidence=None,
            lookback_average_kwh=20., total_energy_daily_average_kwh=20.,
            non_heating_daily_average_kwh=20., hourly_demand_table={}, demand_adjustment_factor=1.,
            battery_soc_percent=a['battery_soc_percent'], price_resolution='quarter_hourly',
            source_status={'price_sensor': 'ok'}, source_errors=[])
    return now, slots, result, a


class TomorrowFillRegressionTest(unittest.TestCase):
    def test_captured_commands_physically_reach_full(self):
        now, slots, _, a = replay_tomorrow_fill()
        windows = a['planned_battery_mode_windows'] + a['estimated_battery_mode_windows']
        trace = energy_trace(now, slots, windows, initial=a['battery_total_energy_kwh'], max_charge=2.5)
        tomorrow = (now + timedelta(days=1)).date()
        charging = [energy for at, energy, mode in trace
                    if datetime.fromisoformat(at).date() == tomorrow and mode.startswith('laden_')]
        self.assertTrue(charging)
        self.assertAlmostEqual(max(charging), 10., delta=.005)
        self.assertLessEqual(max(energy for _, energy, _ in trace), 10.005)

    def test_replayed_discharge_reaches_minimum_before_tomorrow_refill(self):
        now, slots, result, a = replay_tomorrow_fill()
        windows = result.planned_battery_mode_windows + result.estimated_battery_mode_windows
        tomorrow = (now + timedelta(days=1)).date()
        first_charge = next(w for w in windows if w['mode'].startswith('laden_')
                            and datetime.fromisoformat(w['start']).date() == tomorrow)
        charge_start = datetime.fromisoformat(first_charge['start'])
        before_charge = [w for w in windows if datetime.fromisoformat(w['end']) <= charge_start]
        trace = energy_trace(now, slots, before_charge,
                             initial=a['battery_total_energy_kwh'], max_charge=2.5)
        self.assertAlmostEqual(trace[-1][1], 2., delta=.005)
        # The refill must be recalculated from 20%, not the old partial drain.
        first_discharge = next(w for w in windows if w['mode'].startswith('ontladen')
                               and datetime.fromisoformat(w['start']) > charge_start)
        filled = [w for w in windows
                  if datetime.fromisoformat(w['end']) <= datetime.fromisoformat(first_discharge['start'])]
        fill_trace = energy_trace(now, slots, filled,
                                  initial=a['battery_total_energy_kwh'], max_charge=2.5)
        self.assertAlmostEqual(fill_trace[-1][1], 10., delta=.005)

    def test_replayed_refill_reaches_full_before_discharge(self):
        now, slots, result, a = replay_tomorrow_fill()
        windows = result.planned_battery_mode_windows + result.estimated_battery_mode_windows
        trace = energy_trace(now, slots, windows, initial=a['battery_total_energy_kwh'], max_charge=2.5)
        charging = [e for _, e, mode in trace if mode.startswith('laden_')]
        self.assertTrue(charging)
        self.assertAlmostEqual(max(charging), 10., delta=.005)
        self.assertLessEqual(max(e for _, e, _ in trace), 10.005)
        self.assertGreaterEqual(min(e for _, e, _ in trace), 1.995)


if __name__ == '__main__':
    unittest.main()
