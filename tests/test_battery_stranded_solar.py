"""Reduced-PV capture, with explicit assumed 97% measured solar-fill plateau."""
from datetime import datetime, timedelta
import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from test_battery_energy_accounting import coordinator, energy_trace, replay_full_plan
from test_battery_solar_topup_regression import slot
from custom_components.smart_energy_planner.price_models import PlannerWindow
from custom_components.smart_energy_planner.battery_models import SolarWindow


def completion_plan(c, now, soc=97., *, price=.20, demand=2., known=True):
    """Synthetic inputs: depleted PV, two peak hours, cheaper import after peak."""
    origin = now.replace(hour=16, minute=0, second=0, microsecond=0)
    slots = [slot(origin, 0, price, .01, solar=.05),
             slot(origin, 1, .50, .01, demand=demand),
             slot(origin, 2, .50, .01, demand=demand),
             slot(origin, 3, .10, .01)]
    slots[0]['price_known'] = known
    imports = [PlannerWindow(s['start'], s['end'], s['import_price'], s['price_known']) for s in slots]
    exports = [PlannerWindow(s['start'], s['end'], s['export_price'], s['price_known']) for s in slots]
    solar = [SolarWindow(s['start'], s['end'], s['solar_kwh'], None, None) for s in slots]
    c.config_entry = SimpleNamespace(data={
        'battery_enabled': True, 'battery_capacity_kwh': 10., 'battery_min_soc_percent': 20.,
        'battery_no_charge_min_soc_percent': 60., 'battery_max_charge_kw': 2.5,
        'battery_max_discharge_kw': 3., 'battery_min_profit_per_kwh': .08,
        'battery_demand_safety_margin': 0., 'battery_charge_safety_margin': 0.,
    }, options={}, entry_id='stranded-solar')
    namespace = c._build_plan_for_horizon.__globals__
    with patch.object(namespace['dt_util'], 'now', return_value=now), patch.dict(namespace, {
        'build_hourly_home_demand_forecast': lambda **kwargs: [
            {'start': s['start'].isoformat(), 'end': s['end'].isoformat(),
             'estimated_kwh': s['demand_kwh']} for s in slots],
        'align_price_responsive_demand_to_cheap_hours': lambda demand, prices, **kwargs: demand,
    }):
        result = c._build_plan(
            planner_kind='battery', windows=imports, all_windows=imports,
            export_windows=exports, all_export_windows=exports, battery_switch_windows=imports,
            price_average=None, export_price_average=None, current_price=price,
            solar_forecast_kwh=.05, solar_windows=solar, all_solar_windows=solar,
            solcast_confidence=None, lookback_average_kwh=4., total_energy_daily_average_kwh=4.,
            non_heating_daily_average_kwh=4., hourly_demand_table={}, demand_adjustment_factor=1.,
            battery_soc_percent=soc, price_resolution='hourly',
            source_status={'price_sensor': 'ok'}, source_errors=[])
    return slots, result


class StrandedSolarTest(unittest.TestCase):
    def solar_cycle(self, now):
        c = coordinator()
        c._active_charge_phase_mode = 'laden_met_zonne_energie'
        c._active_charge_phase_end = now - timedelta(minutes=5)
        return c

    def test_full_coordinator_plateau_refresh_json_restart_and_measured_completion(self):
        now = datetime.fromisoformat('2026-11-16T16:00:00+01:00')
        c = self.solar_cycle(now)
        for minute in (0, 1, 2):
            current = now + timedelta(minutes=minute)
            slots, result = completion_plan(c, current)
            self.assertEqual(result.battery_strategy, 'laden_van_net')
            trace = energy_trace(current, slots, result.planned_battery_mode_windows,
                                 initial=9.7, max_charge=2.5)
            self.assertAlmostEqual(max(e for _, e, _ in trace), 10., places=5)
            self.assertGreaterEqual(min(e for _, e, _ in trace), 5.995)
            self.assertTrue(any(w['mode'].startswith('ontladen')
                                for w in result.planned_battery_mode_windows))
            self.assertTrue(c._charge_session_started, 'unchanged measured SOC is not completion')
            if minute == 1:
                c.hass = SimpleNamespace(data={})
                c._store_battery_cycle_state_snapshot(current)
                data = json.loads(json.dumps(c.hass.data))
                restarted = coordinator()
                restarted._charge_session_started = False
                restarted._battery_cycle_state_initialized = False
                restarted.hass = SimpleNamespace(data=data)
                restarted.config_entry = c.config_entry
                with patch.object(c._restore_recent_battery_cycle_state.__globals__['dt_util'],
                                  'now', return_value=current):
                    restarted._restore_recent_battery_cycle_state()
                c = restarted
                self.assertTrue(c._charge_session_started)
                self.assertTrue(c._battery_cycle_state_restored_recent)
        slots, result = completion_plan(c, now + timedelta(minutes=10), soc=100.)
        self.assertFalse(c._charge_session_started)
        self.assertTrue(any(w['mode'].startswith('ontladen') for w in result.planned_battery_mode_windows))
        self.assertFalse(result.planned_grid_charge_windows)

    def test_new_imports_still_need_margin_demand_and_known_purchase_price(self):
        now = datetime.fromisoformat('2026-11-16T16:00:00+01:00')
        for kwargs in ({'price': .49}, {'demand': .1}, {'known': False}):
            with self.subTest(**kwargs):
                c = self.solar_cycle(now)
                c._battery_grid_charge_price = .15
                _, result = completion_plan(c, now, **kwargs)
                self.assertFalse(result.planned_grid_charge_windows)
                self.assertFalse(any(w['mode'].startswith('ontladen')
                                     for w in result.planned_battery_mode_windows))
                self.assertTrue(c._charge_session_started)

    def test_completion_must_precede_peak_not_cheapest_late_import(self):
        now = datetime.fromisoformat('2026-11-16T16:00:00+01:00')
        slots = [slot(now, 0, .20, .01, solar=.05),
                 slot(now, 1, .50, .01, demand=2.),
                 slot(now, 2, .50, .01, demand=2.),
                 slot(now, 3, .10, .01)]
        c = coordinator()
        c._active_charge_phase_mode = 'laden_met_zonne_energie'
        c._active_charge_phase_end = now - timedelta(minutes=5)
        solar, grid = c._plan_bound_charge_windows(
            slots=slots, now=now, usable_capacity_kwh=8.,
            current_remaining_capacity_kwh=.3, max_charge_kw=2.5,
            max_discharge_kw=3., battery_min_profit=.08,
            no_charge_reserve_kwh=4.)
        self.assertTrue(grid)
        self.assertTrue(all(datetime.fromisoformat(w['end']) <= now + timedelta(hours=1)
                            for w in grid), grid)
        self.assertAlmostEqual(sum(w['charge_kwh'] for w in solar + grid), .3, places=5)

    def test_expired_solar_fill_gets_economic_completion_above_95_percent(self):
        c = coordinator()
        c._active_charge_phase_mode = 'laden_met_zonne_energie'
        c._active_charge_phase_end = datetime.fromisoformat('2026-10-05T15:30:00+02:00')
        now, slots, result = replay_full_plan(
            '2026_10_05_reduced_solar', '2026-10-05T15:39:59.411439+02:00',
            soc=97, reserve=60, max_charge=2.5, instance=c,
            isolated_cycles=True, snapshot_exports=True)
        windows = result.planned_battery_mode_windows
        self.assertTrue(result.planned_grid_charge_windows)
        trace = energy_trace(now, slots, windows, initial=9.7, max_charge=2.5)
        self.assertGreaterEqual(max(e for _, e, _ in trace), 9.995)
        self.assertLessEqual(max(e for _, e, _ in trace), 10.005)
        self.assertTrue(any(w['mode'].startswith('ontladen') for w in windows))
        self.assertGreaterEqual(min(e for _, e, _ in trace), 5.995)


if __name__ == '__main__':
    unittest.main()
