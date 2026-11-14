"""Approved sunk-stock policy through the real coordinator entry point."""
import json
from datetime import datetime, timedelta
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from test_battery_energy_accounting import coordinator, energy_trace
from custom_components.smart_energy_planner.battery_forecast import build_energy_balance_slots
from custom_components.smart_energy_planner.price_models import PlannerWindow
from custom_components.smart_energy_planner.battery_models import SolarWindow


class DischargePolicyTest(unittest.TestCase):
    now = datetime(2026, 11, 14, 12)

    def instance(self):
        c = coordinator()
        c._charge_session_started = False
        c._discharge_session_started = True
        c._battery_cycle_discharge_floor_kwh = 4.
        c._battery_grid_charge_price = .90
        c._battery_margin_state = dict(at=self.now.isoformat(), energy=8., reserve=0.,
                                      lots=[[8., .90]], credit=-3., actions=[])
        return c

    def build(self, c=None, reserve=60, soc=100, prices=None, solar=False, now=None, demand_kwh=1.):
        c = c or self.instance()
        now = now or self.now
        prices = prices or [.20]*4 + [.27]*4
        windows = [PlannerWindow(self.now+timedelta(hours=i), self.now+timedelta(hours=i+1), p, True)
                   for i, p in enumerate(prices)]
        exports = [PlannerWindow(w.start, w.end, w.price-.12, True) for w in windows]
        sun = [SolarWindow(w.start, w.end, 3.5 if solar and i < 4 else 0., None, None)
               for i, w in enumerate(windows)]
        demand = [dict(start=w.start.isoformat(), end=w.end.isoformat(), estimated_kwh=demand_kwh) for w in windows]
        c.config_entry = SimpleNamespace(entry_id='battery', data=dict(battery_enabled=True, battery_capacity_kwh=10,
            battery_min_soc_percent=20, battery_max_charge_kw=2.5, battery_max_discharge_kw=3,
            battery_min_profit_per_kwh=.10, battery_demand_safety_margin=0,
            battery_charge_safety_margin=0, battery_no_charge_min_soc_percent=reserve), options={})
        namespace = c._build_plan_for_horizon.__globals__
        with patch.object(namespace['dt_util'], 'now', return_value=now), patch.dict(namespace, {
            'build_hourly_home_demand_forecast': lambda **kwargs: demand,
            'align_price_responsive_demand_to_cheap_hours': lambda demand, prices, **kwargs: demand,
        }):
            result = c._build_plan(planner_kind='battery', windows=windows, all_windows=windows,
                export_windows=exports, all_export_windows=exports, battery_switch_windows=windows,
                price_average=None, export_price_average=None, current_price=prices[0],
                solar_forecast_kwh=sum(w.forecast_kwh for w in sun), solar_windows=sun,
                all_solar_windows=sun, solcast_confidence=None, lookback_average_kwh=8,
                total_energy_daily_average_kwh=8, non_heating_daily_average_kwh=8,
                hourly_demand_table={}, demand_adjustment_factor=1, battery_soc_percent=soc,
                price_resolution='hourly', source_status={'price_sensor': 'ok'}, source_errors=[])
        slots = build_energy_balance_slots(price_windows=windows, export_price_windows=exports,
            solar_windows=sun, hourly_demand=demand, horizon_start=now, demand_safety_margin=0)
        return c, result, slots

    def test_high_cost_debt_waits_for_expensive_hours_without_profitable_refill(self):
        c, result, slots = self.build()
        self.assertFalse(result.planned_grid_charge_windows)
        self.assertFalse(result.planned_solar_charge_windows)
        trace = energy_trace(self.now, slots, result.planned_battery_mode_windows, initial=10, max_charge=2.5)
        self.assertAlmostEqual(trace[-1][1], 6., delta=.005)
        self.assertTrue(all(e >= 5.995 for _, e, _ in trace))
        discharge = [w for w in result.planned_battery_mode_windows if w['mode'].startswith('ontladen')]
        self.assertTrue(discharge)
        self.assertGreaterEqual(datetime.fromisoformat(discharge[0]['start']), self.now+timedelta(hours=4))
        self.assertEqual(c._battery_margin_state['lots'], [[8., .90]])
        self.assertEqual(c._battery_margin_state['credit'], -3.)

    def test_configured_non60_reserve(self):
        _, result, slots = self.build(reserve=70)
        trace = energy_trace(self.now, slots, result.planned_battery_mode_windows, initial=10, max_charge=2.5)
        self.assertAlmostEqual(trace[-1][1], 7., delta=.005)
        self.assertGreaterEqual(min(e for _, e, _ in trace), 6.995)

    def test_repeated_updates_and_json_restart_keep_debt_and_schedule(self):
        c = self.instance()
        for index in range(8):
            now = self.now+timedelta(hours=index)
            energy = 10. if index <= 4 else 14.-index
            c, result, slots = self.build(c=c, now=now, soc=energy*10)
            command = next(w for w in result.planned_battery_mode_windows
                           if datetime.fromisoformat(w['start']) <= now < datetime.fromisoformat(w['end']))
            self.assertEqual(command['mode'], 'accu_uit' if index < 4 else 'ontladen')
            self.assertLessEqual(c._battery_margin_state['credit'], -3.)
            if index == 5:
                ns = c._restore_recent_battery_cycle_state.__globals__
                c.hass = SimpleNamespace(data={})
                c._store_battery_cycle_state_snapshot(now)
                restored = self.instance()
                restored.config_entry = c.config_entry
                restored.hass = SimpleNamespace(data=json.loads(json.dumps(c.hass.data)))
                with patch.object(ns['dt_util'], 'now', return_value=now+timedelta(days=3)):
                    restored._restore_recent_battery_cycle_state()
                c = restored
        self.assertLess(c._battery_margin_state['credit'], -3.)
        self.assertTrue(all(cost == .90 for _, cost in c._battery_margin_state['lots']))

    def test_unprofitable_new_grid_and_solar_charge_rejected(self):
        for solar in (False, True):
            with self.subTest(solar=solar):
                c = coordinator()
                c._charge_session_started = False
                # Each case starts without an accepted charging session;
                # reusing a previous profitable solar result tests continuation,
                # not admission of a new unprofitable fill.
                prices = [.30]*4+[.20]*4 if solar else [.25]*8
                _, result, _ = self.build(c=c, soc=60, prices=prices, solar=solar)
                self.assertFalse(result.planned_grid_charge_windows)
                self.assertFalse(result.planned_solar_charge_windows)

    def test_completed_discharge_does_not_admit_unprofitable_new_solar_fill(self):
        for floor, soc in ((4., 60), (0., 20)):
            with self.subTest(floor=floor):
                c = self.instance()
                c._battery_cycle_state_initialized = True
                c._battery_cycle_discharge_floor_kwh = floor
                c._battery_margin_state.update(energy=floor, lots=[[floor, .90]])
                for minute in (0, 5):
                    c, result, _ = self.build(c=c, soc=soc,
                        now=self.now+timedelta(minutes=minute),
                        prices=[.30]*4+[.20]*4, solar=True)
                    self.assertFalse(result.planned_solar_charge_windows)
                    self.assertFalse(result.planned_grid_charge_windows)
                    self.assertFalse(c._charge_session_started)
                    self.assertFalse(any(w['mode'].startswith('laden')
                        for w in result.planned_battery_mode_windows))

    def test_completed_reserve_restart_still_requires_new_fill_profit(self):
        c = self.instance()
        c._battery_margin_state.update(energy=4., lots=[[4., .90]])
        c, result, _ = self.build(c=c, soc=60, prices=[.30]*4+[.20]*4, solar=True)
        c.hass = SimpleNamespace(data={})
        c._store_battery_cycle_state_snapshot(self.now)
        restored = self.instance()
        restored.config_entry = c.config_entry
        restored.hass = SimpleNamespace(data=json.loads(json.dumps(c.hass.data)))
        ns = restored._restore_recent_battery_cycle_state.__globals__
        with patch.object(ns['dt_util'], 'now', return_value=self.now+timedelta(minutes=5)):
            restored._restore_recent_battery_cycle_state()
        _, result, _ = self.build(c=restored, soc=60,
            now=self.now+timedelta(minutes=5), prices=[.30]*4+[.20]*4, solar=True)
        self.assertFalse(result.planned_solar_charge_windows)
        self.assertFalse(restored._charge_session_started)

    def test_admitted_partial_solar_fill_continues_after_price_refresh_and_restart(self):
        c = coordinator()
        c._charge_session_started = False
        c._discharge_session_started = False
        c, result, _ = self.build(c=c, soc=60, prices=[.15]*4+[.60]*4, solar=True)
        self.assertTrue(result.planned_solar_charge_windows)
        self.assertTrue(c._charge_session_started)
        c.hass = SimpleNamespace(data={})
        c._store_battery_cycle_state_snapshot(self.now)
        restored = coordinator()
        restored.config_entry = c.config_entry
        restored.hass = SimpleNamespace(data=json.loads(json.dumps(c.hass.data)))
        ns = restored._restore_recent_battery_cycle_state.__globals__
        with patch.object(ns['dt_util'], 'now', return_value=self.now+timedelta(minutes=30)):
            restored._restore_recent_battery_cycle_state()
        _, result, _ = self.build(c=restored, soc=70,
            now=self.now+timedelta(minutes=30), prices=[.30]*4+[.20]*4, solar=True)
        self.assertTrue(result.planned_solar_charge_windows)
        self.assertTrue(restored._charge_session_started)

    def test_mode_boundary_release_is_not_a_started_fill(self):
        from test_battery_cycle_safety import modes, slots_at
        for floor in (0., 4.):
            with self.subTest(floor=floor):
                c = self.instance()
                c._battery_cycle_state_initialized = True
                c._battery_cycle_discharge_floor_kwh = floor
                c._build_mode_windows_from_hourly_plan(
                    slots=slots_at(self.now), now=self.now,
                    planned_solar_charge_windows=[], planned_grid_charge_windows=[],
                    initial_usable_energy_kwh=floor, usable_capacity_kwh=8.,
                    battery_soc_percent=20+10*floor, average_price=.30,
                    average_export_price=.19, max_charge_kw=2., max_discharge_kw=2.,
                    no_charge_reserve_kwh=floor, battery_min_profit=.10)
                self.assertFalse(c._charge_session_started)
                self.assertFalse(c._discharge_session_started)

    def test_empty_observed_fill_keeps_continuation_latch(self):
        from test_battery_cycle_safety import modes, slots_at
        c = self.instance()
        c._battery_cycle_state_initialized = True
        c._battery_cycle_discharge_floor_kwh = None
        c._charge_session_started = True
        c._discharge_session_started = False
        modes(c, self.now, slots_at(self.now), 0., [], [])
        self.assertTrue(c._charge_session_started)
        self.assertFalse(c._discharge_session_started)

    def test_export_fallback_without_home_demand_uses_expensive_hours(self):
        _, result, slots = self.build(demand_kwh=0.)
        trace = energy_trace(self.now, slots, result.planned_battery_mode_windows, initial=10, max_charge=2.5)
        self.assertAlmostEqual(trace[-1][1], 6., delta=.005)
        export = [w for w in result.planned_battery_mode_windows if w['mode'] == 'ontladen_naar_net']
        self.assertTrue(export)
        self.assertGreaterEqual(datetime.fromisoformat(export[0]['start']), self.now+timedelta(hours=4))

    def test_late_export_beats_cheaper_early_home_use(self):
        _, result, slots = self.build(demand_kwh=.5, prices=[.08]*4+[.25]*4)
        trace = energy_trace(self.now, slots, result.planned_battery_mode_windows, initial=10, max_charge=2.5)
        self.assertAlmostEqual(trace[-1][1], 6., delta=.005)
        discharge = [w for w in result.planned_battery_mode_windows if w['mode'].startswith('ontladen')]
        self.assertGreaterEqual(datetime.fromisoformat(discharge[0]['start']), self.now+timedelta(hours=4))

    def test_profitable_complete_new_charge_control(self):
        c = coordinator()
        c._charge_session_started = False
        c._discharge_session_started = False
        _, result, slots = self.build(c=c, soc=60, prices=[.05]*4+[.60]*4)
        self.assertTrue(result.planned_grid_charge_windows)
        trace = energy_trace(self.now, slots, result.planned_battery_mode_windows, initial=6, max_charge=2.5)
        self.assertAlmostEqual(max(e for _, e, _ in trace), 10., delta=.005)


if __name__ == '__main__':
    unittest.main()
