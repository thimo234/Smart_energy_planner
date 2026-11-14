"""Captured live inputs, not a reconstruction of unavailable persisted costs.

Capacity/minimum/reserve, discharge power and profit are recoverable from the
entity. Charge power 2.5 kW and zero safety margins are replay assumptions.
No tariff/forecast is modified to manufacture the captured all-idle failure.
"""
import copy
import json
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import unittest

from test_battery_energy_accounting import coordinator, energy_trace
from custom_components.smart_energy_planner.battery_forecast import build_energy_balance_slots
from custom_components.smart_energy_planner.battery_models import SolarWindow
from custom_components.smart_energy_planner.price_models import PlannerWindow


class November14LiveReplayTest(unittest.TestCase):
    def setUp(self):
        self.raw = json.loads((Path(__file__).parent / 'fixtures/battery_2026_11_14_missing_discharge.json').read_text())
        self.a = self.raw['attributes']
        self.now = datetime.fromisoformat(self.a['planned_battery_mode_windows'][0]['start'])
        parse = datetime.fromisoformat
        def prices(key):
            return [PlannerWindow(parse(w['start']), parse(w['end']), w['price'], w.get('price_known', True))
                    for w in self.a[key]]
        imports = prices('upcoming_energy_price_windows')
        exports = prices('upcoming_export_price_windows')
        solar = [SolarWindow(parse(w['start']), parse(w['end']), w['estimated_kwh'], None, None)
                 for w in self.a['estimated_hourly_solar_forecast']]
        demand = self.a['estimated_hourly_home_demand']
        self.slots = build_energy_balance_slots(price_windows=imports, export_price_windows=exports,
            solar_windows=solar, hourly_demand=demand, horizon_start=self.now, demand_safety_margin=0.)
        self.inputs = dict(planner_kind='battery', windows=imports, all_windows=imports,
            export_windows=exports, all_export_windows=exports, battery_switch_windows=imports,
            price_average=None, export_price_average=None, current_price=self.a['current_price'],
            solar_forecast_kwh=0., solar_windows=solar, all_solar_windows=solar, solcast_confidence=None,
            lookback_average_kwh=20., total_energy_daily_average_kwh=20., non_heating_daily_average_kwh=20.,
            hourly_demand_table={}, demand_adjustment_factor=1., battery_soc_percent=self.a['battery_soc_percent'],
            price_resolution='quarter_hourly', source_status={'price_sensor': 'ok'}, source_errors=[],
            external_hourly_demand=demand)
        self.c = coordinator()
        self.c._charge_session_started = self.a['battery_charge_cycle_active']
        self.c._discharge_session_started = self.a['battery_discharge_cycle_active']
        # Boundary is inferred from the reported active no-charge reserve, not
        # claimed to have been read from Home Assistant's persisted cycle state.
        self.c._battery_cycle_discharge_floor_kwh = self.a['battery_reserved_energy_kwh']
        self.c.config_entry = SimpleNamespace(entry_id='nov14-replay', options={}, data=dict(
            battery_enabled=True, battery_capacity_kwh=10., battery_min_soc_percent=20.,
            battery_no_charge_min_soc_percent=60., battery_max_charge_kw=2.5,
            battery_max_discharge_kw=3., battery_min_profit_per_kwh=.1,
            battery_demand_safety_margin=0., battery_charge_safety_margin=0.))

    def replay(self):
        namespace = self.c._build_plan_for_horizon.__globals__
        with patch.object(namespace['dt_util'], 'now', return_value=self.now):
            return self.c._build_plan(**self.inputs)

    def test_capture_really_has_no_discharge_despite_active_cycle(self):
        self.assertEqual(self.raw['state'], 'accu_uit')
        self.assertEqual(self.a['battery_soc_percent'], 88.)
        self.assertFalse(self.a['battery_charge_cycle_active'])
        self.assertTrue(self.a['battery_discharge_cycle_active'])
        self.assertFalse(any(w['mode'].startswith('ontladen') for w in
            self.a['planned_battery_mode_windows'] + self.a['estimated_battery_mode_windows']))

    def test_fresh_cost_state_upgrades_reserve_for_profitable_refill(self):
        result = self.replay()
        self.assertEqual(result.battery_strategy, 'ontladen_naar_net')
        windows = result.planned_battery_mode_windows + result.estimated_battery_mode_windows
        trace = energy_trace(self.now, self.slots, windows, initial=8.8, max_charge=2.5)
        self.assertAlmostEqual(min(e for _, e, _ in trace), 2., delta=.005)
        self.assertLessEqual(max(e for _, e, _ in trace), 10.005)
        peak = datetime.fromisoformat('2026-11-14T18:00:00+01:00')
        self.assertTrue(any(w['mode'].startswith('ontladen') and datetime.fromisoformat(w['start']) <= peak
                            < datetime.fromisoformat(w['end']) for w in windows))

    def test_unchanged_measurement_replay_is_idempotent_and_restart_preserves_bound(self):
        first = self.replay()
        before = copy.deepcopy(self.c._battery_margin_state)
        second = self.replay()
        self.assertEqual(first.planned_battery_mode_windows, second.planned_battery_mode_windows)
        self.assertEqual(first.estimated_battery_mode_windows, second.estimated_battery_mode_windows)
        self.assertEqual(before, self.c._battery_margin_state)
        self.c.hass = SimpleNamespace(data={})
        self.c._store_battery_cycle_state_snapshot(self.now)
        restarted = coordinator()
        restarted.hass, restarted.config_entry = self.c.hass, self.c.config_entry
        namespace = self.c._restore_recent_battery_cycle_state.__globals__
        with patch.object(namespace['dt_util'], 'now', return_value=self.now):
            restarted._restore_recent_battery_cycle_state()
        self.c = restarted
        restored = self.replay()
        self.assertEqual(self.c._battery_cycle_discharge_floor_kwh, 0.)
        self.assertTrue(self.c._discharge_session_started)
        self.assertFalse(self.c._charge_session_started)
        self.assertEqual(first.planned_battery_mode_windows, restored.planned_battery_mode_windows)
        self.assertEqual(first.estimated_battery_mode_windows, restored.estimated_battery_mode_windows)


if __name__ == '__main__':
    unittest.main()
