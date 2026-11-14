"""Solar surplus must remain usable inside an unfinished discharge cycle."""
from datetime import datetime, timedelta
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from test_battery_energy_accounting import coordinator, energy_trace


def slot(now, index, price, export, *, solar=0., demand=0.):
    start = now + timedelta(hours=index)
    return dict(start=start, end=start + timedelta(hours=1), hours=1.,
                import_price=price, export_price=export, price_known=True,
                solar_kwh=solar, demand_kwh=demand, net_solar_kwh=solar-demand)
from custom_components.smart_energy_planner.price_models import PlannerWindow
from custom_components.smart_energy_planner.battery_models import SolarWindow
from custom_components.smart_energy_planner.battery_forecast import build_energy_balance_slots


def replay():
    a = json.loads((Path(__file__).parent / 'fixtures/battery_2026_11_12_night_solar.json').read_text())
    now = datetime.fromisoformat(a['planned_battery_mode_windows'][0]['start'])
    def prices(key):
        return [PlannerWindow(datetime.fromisoformat(w['start']), datetime.fromisoformat(w['end']),
                              w['price'], w.get('price_known', True)) for w in a[key]]
    imports, exports = prices('upcoming_energy_price_windows'), prices('upcoming_export_price_windows')
    solar = [SolarWindow(datetime.fromisoformat(w['start']), datetime.fromisoformat(w['end']),
                         w['estimated_kwh'], None, None) for w in a['estimated_hourly_solar_forecast']]
    demand = a['estimated_hourly_home_demand']
    slots = build_energy_balance_slots(price_windows=imports, export_price_windows=exports,
        solar_windows=solar, hourly_demand=demand, horizon_start=now, demand_safety_margin=0.)
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
        result = c._build_plan(planner_kind='battery', windows=imports, all_windows=imports,
            export_windows=exports, all_export_windows=exports, battery_switch_windows=imports,
            price_average=None, export_price_average=None, current_price=a['current_price'],
            solar_forecast_kwh=0., solar_windows=solar, all_solar_windows=solar,
            solcast_confidence=None, lookback_average_kwh=20., total_energy_daily_average_kwh=20.,
            non_heating_daily_average_kwh=20., hourly_demand_table={}, demand_adjustment_factor=1.,
            battery_soc_percent=a['battery_soc_percent'], price_resolution='quarter_hourly',
            source_status={'price_sensor': 'ok'}, source_errors=[])
    return now, slots, result, a


class SolarTopupTest(unittest.TestCase):
    def build(self, slots, *, initial=4., reserve=0., cost=None):
        c = coordinator()
        c._charge_session_started = False
        c._discharge_session_started = True
        c._battery_cycle_state_initialized = True
        c._battery_grid_charge_price = cost
        windows, _ = c._build_mode_windows_from_hourly_plan(
            slots=slots, now=slots[0]['start'], planned_solar_charge_windows=[],
            planned_grid_charge_windows=[], initial_usable_energy_kwh=initial,
            usable_capacity_kwh=8., average_price=.4, average_export_price=.1,
            max_charge_kw=2.5, max_discharge_kw=3., no_charge_reserve_kwh=reserve,
            battery_min_profit=.1)
        return c, windows

    def test_partial_solar_cannot_reverse_an_unfinished_discharge(self):
        now = datetime.fromisoformat('2026-11-13T12:00:00+01:00')
        slots = [slot(now, 0, .2, .05, solar=1.), slot(now, 1, .6, .1, demand=2.)]
        _, windows = self.build(slots, initial=1.)
        self.assertEqual(windows[0]['mode'], 'accu_uit')
        self.assertFalse(any(w['mode'].startswith('laden_') for w in windows))
        self.assertTrue(any(w['mode'] == 'ontladen' for w in windows))
        self.assertLess(max(e for _, e, _ in energy_trace(now, slots, windows, initial=3.)), 10.)

    def test_small_topup_does_not_release_no_charge_reserve(self):
        now = datetime.fromisoformat('2026-11-13T11:00:00+01:00')
        slots = [slot(now, 0, .5, .1, demand=3.), slot(now, 1, .2, .05, solar=.2),
                 slot(now, 2, .6, .1, demand=3.)]
        _, windows = self.build(slots, initial=4.5, reserve=4.)
        trace = energy_trace(now, slots, windows, initial=6.5)
        self.assertGreaterEqual(min(e for _, e, _ in trace), 5.995)

    def test_unprofitable_or_unknown_solar_is_not_new_purchase_authority(self):
        now = datetime.fromisoformat('2026-11-13T12:00:00+01:00')
        for known, export in ((True, .7), (False, .05)):
            with self.subTest(known=known, export=export):
                slots = [slot(now, 0, .2, export, solar=1.), slot(now, 1, .6, .1, demand=2.)]
                slots[0]['price_known'] = known
                _, windows = self.build(slots)
                self.assertFalse(any(w['mode'].startswith('laden_') for w in windows))

    def test_live_replay_uses_tomorrow_solar_within_physical_limits(self):
        now, slots, result, a = replay()
        windows = result.planned_battery_mode_windows + result.estimated_battery_mode_windows
        self.assertTrue(any(w['mode'] == 'laden_met_zonne_energie' for w in windows))
        trace = energy_trace(now, slots, windows, initial=a['battery_total_energy_kwh'], max_charge=2.5)
        self.assertGreaterEqual(min(e for _, e, _ in trace), 1.995)
        self.assertLessEqual(max(e for _, e, _ in trace), 10.005)
        self.assertTrue(any(w['mode'] == 'ontladen' and datetime.fromisoformat(w['start']).hour >= 16
                            for w in windows))


if __name__ == '__main__':
    unittest.main()
