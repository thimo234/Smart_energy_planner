"""Nov 15 cheap-hour investigation; do not confuse later inputs with history.

The sanitized numerical capture is versioned in tests/fixtures. Historical
recorder rows contain summary attributes, not full forecast arrays or the
contemporaneous persisted margin state. A backdated replay below
is explicitly a counterfactual using the *later* diagnostic forecast.
"""
import json
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from test_battery_energy_accounting import coordinator
from custom_components.smart_energy_planner.battery_models import SolarWindow
from custom_components.smart_energy_planner.price_models import PlannerWindow
from custom_components.smart_energy_planner.battery_planner import (
    order_discharge_price_bands, plan_segment_discharge_kwh,
)


class November15PriceRankingTest(unittest.TestCase):
    def test_one_cent_bands_cannot_promote_253_above_376_or_462(self):
        now = datetime.fromisoformat('2026-11-15T13:12:42.665548+01:00')
        # Prices are observed summary values; these equal-energy slots are a
        # synthetic unit scenario, not the missing historical demand forecast.
        slots = [dict(start=now + timedelta(hours=i), price=p)
                 for i, p in enumerate((.253, .262, .271, .280, .289,
                                        .298, .307, .316, .325, .334,
                                        .343, .352, .361, .370, .376, .462))]
        ranked = order_discharge_price_bands(slots)
        self.assertEqual(ranked[0]['price'], .462)
        self.assertLess(ranked.index(slots[-2]), ranked.index(slots[0]))
        self.assertEqual(ranked[-1]['price'], .253)

    def test_full_eight_kwh_budget_still_reserves_for_expensive_demand(self):
        now = datetime.fromisoformat('2026-11-15T13:12:42.665548+01:00')
        slots = [dict(start=now, hours=.0381484589, import_price=.253,
                      net_solar_kwh=-.127)]
        # Full battery does not mean enough energy to cover all later demand.
        # No selected intermediate refill exists in this synthetic scenario.
        slots += [dict(start=now + timedelta(hours=i+1), hours=1.,
                       import_price=.376, net_solar_kwh=-2.) for i in range(5)]
        for budget in (4., 8.):
            with self.subTest(budget=budget):
                assigned = plan_segment_discharge_kwh(
                    slots=slots, available_energy_kwh=budget, max_discharge_kw=3.)
                self.assertNotIn(now, assigned)
                self.assertAlmostEqual(sum(assigned.values()), budget)

    def test_rejected_refill_must_not_split_expensive_demand_horizon(self):
        """Minimal synthetic regression; no optional capture files required."""
        now = datetime.fromisoformat('2026-11-15T13:12:42.665548+01:00')
        refill = now.replace(minute=15, second=0, microsecond=0)
        starts = [now, refill, now.replace(hour=17, minute=0, second=0, microsecond=0)]
        ends = [refill, starts[2], starts[2] + timedelta(hours=4)]
        prices = [.253, .255, .376]
        deficits = [.127, 0., 8.]
        slots = [dict(start=s, end=e, hours=(e-s).total_seconds()/3600,
                      import_price=p, export_price=p-.12, price_known=True,
                      net_solar_kwh=-d, demand_kwh=d, solar_kwh=0.)
                 for s, e, p, d in zip(starts, ends, prices, deficits)]
        candidate = dict(start=refill.isoformat(),
                         end=(refill+timedelta(minutes=3)).isoformat(),
                         price=.255, usable_hours=.05, charge_kwh=.114445)
        c = coordinator()
        c._charge_session_started = False
        c._discharge_session_started = True
        c._battery_cycle_discharge_floor_kwh = 4.
        c._battery_rank_sunk_stock = False
        kwargs = dict(slots=slots, now=now, planned_solar_charge_windows=[],
                      planned_grid_charge_windows=[candidate],
                      initial_usable_energy_kwh=8., usable_capacity_kwh=8.,
                      battery_soc_percent=100., no_charge_reserve_kwh=4.,
                      battery_min_profit=.1, average_price=.30,
                      average_export_price=.18, max_charge_kw=2.5, max_discharge_kw=3.)
        windows, mode = c._build_mode_windows_from_hourly_plan(**kwargs)
        self.assertFalse(any(w['mode'] == 'laden_van_net' for w in windows))
        self.assertEqual(mode, 'accu_uit', windows)
        c._discharge_session_started = False
        windows, mode = c._build_mode_windows_from_hourly_plan(**kwargs)
        self.assertEqual(mode, 'accu_uit', windows)

    def test_imminent_refill_cannot_count_suppressed_export_as_reachable(self):
        now = datetime.fromisoformat('2026-11-15T13:00:00+01:00')
        refill = now + timedelta(minutes=15)
        later = now + timedelta(hours=4)
        slots = [dict(start=s, end=e, hours=(e-s).total_seconds()/3600,
                      import_price=p, export_price=.01, price_known=True,
                      net_solar_kwh=-d, demand_kwh=d, solar_kwh=0.)
                 for s, e, p, d in ((now, refill, .253, .01),
                                    (refill, later, .255, 0.),
                                    (later, later+timedelta(hours=4), .60, 8.))]
        candidate = dict(start=refill.isoformat(),
                         end=(refill+timedelta(minutes=3)).isoformat(),
                         price=.255, usable_hours=.05, charge_kwh=.125)
        outcomes = []
        for proposals in ([], [candidate]):
            c = coordinator()
            c._cycle_export_planning = True
            c._charge_session_started = False
            c._discharge_session_started = True
            c._battery_cycle_discharge_floor_kwh = 4.
            windows, mode = c._build_mode_windows_from_hourly_plan(
                slots=slots, now=now, planned_solar_charge_windows=[],
                planned_grid_charge_windows=proposals,
                initial_usable_energy_kwh=4.5, usable_capacity_kwh=8.,
                battery_soc_percent=65., no_charge_reserve_kwh=4.,
                battery_min_profit=.10, average_price=.35,
                average_export_price=.01, max_charge_kw=2.5, max_discharge_kw=3.)
            self.assertFalse(any(w['mode'] == 'laden_van_net' for w in windows))
            outcomes.append(mode)
        self.assertEqual(outcomes, ['accu_uit', 'accu_uit'])


class November15LocalCaptureReplayTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.capture = json.loads((Path(__file__).parent / 'fixtures' /
                                  'battery_2026_11_15_outlook.json').read_text())
        cls.history = cls.capture['history']

    def replay(self, timestamp=None, soc=None, optimizer_v2=False):
        p = self.capture
        a = p['plan']
        def items(key):
            wrapped = a[key]
            self.assertFalse(wrapped['truncated'], key)
            self.assertEqual(wrapped['total'], len(wrapped['items']), key)
            return wrapped['items']
        parse = datetime.fromisoformat
        def prices(key):
            return [PlannerWindow(parse(w['start']), parse(w['end']), w['price'], w['price_known'])
                    for w in items(key)]
        imports = prices('upcoming_energy_price_windows')
        exports = prices('upcoming_export_price_windows')
        solar = [SolarWindow(parse(w['start']), parse(w['end']), w['estimated_kwh'], None, None)
                 for w in items('estimated_hourly_solar_forecast')]
        demand = items('estimated_hourly_home_demand')
        now = parse(timestamp or items('planned_battery_mode_windows')[0]['start'])
        c = coordinator()
        c.config_entry = SimpleNamespace(entry_id='nov15-investigation', options={},
                                        data={**p['effective_config'], 'battery_optimizer_v2_full': optimizer_v2})
        for key, value in p['coordinator_state'].items():
            if key == 'battery_margin_state':
                # The diagnostic state was captured after planning, not before
                # the historical update. Never backdate its lots/actions.
                continue
            if key == 'active_charge_phase_end' and value:
                value = parse(value)
            setattr(c, '_' + key, value)
        # Fresh margin ledger is a replay assumption, not recovered history.
        namespace = c._build_plan_for_horizon.__globals__
        original_slots = namespace['build_energy_balance_slots']
        captured_slots = []
        def record_slots(**kwargs):
            slots = original_slots(**kwargs)
            captured_slots.append(slots)
            return slots
        current = next(w.price for w in imports if w.start <= now < w.end)
        with patch.object(namespace['dt_util'], 'now', return_value=now), patch.dict(
                namespace, build_energy_balance_slots=record_slots):
            result = c._build_plan(
                external_hourly_demand=demand, planner_kind='battery',
                windows=imports, all_windows=imports, export_windows=exports,
                all_export_windows=exports, battery_switch_windows=imports,
                price_average=None, export_price_average=None, current_price=current,
                solar_forecast_kwh=0., solar_windows=solar, all_solar_windows=solar,
                solcast_confidence=None, lookback_average_kwh=20.,
                total_energy_daily_average_kwh=20., non_heating_daily_average_kwh=20.,
                hourly_demand_table={}, demand_adjustment_factor=1.,
                battery_soc_percent=a['battery_soc_percent'] if soc is None else soc,
                price_resolution='quarter_hourly', source_status={'price_sensor': 'ok'},
                source_errors=[])
        self.replay_slots = max(captured_slots, key=len)
        self.replay_now = now
        return result

    def test_recorded_transition_does_not_contain_full_historical_inputs(self):
        idle, discharge = self.history[0]
        self.assertEqual((idle['state'], discharge['state']), ('accu_uit', 'ontladen'))
        before, after = idle['attributes'], discharge['attributes']
        self.assertEqual(before['battery_soc_percent'], 100.)
        self.assertEqual(after['battery_soc_percent'], 100.)
        self.assertEqual(after['current_price'], .253)
        self.assertEqual(before['next_high_price_window_price'], .376)
        self.assertEqual(after['next_high_price_window_price'], .462)
        self.assertIsNone(before['next_charge_window_start'])
        self.assertEqual(after['next_charge_opportunity_start'], '2026-11-15T13:15:00+01:00')
        self.assertEqual(after['next_charge_window_start'], '2026-11-15T23:45:00+01:00')
        for row in (before, after):
            self.assertNotIn('upcoming_energy_price_windows', row)
            self.assertNotIn('estimated_hourly_home_demand', row)
            self.assertNotIn('planned_battery_mode_windows', row)

    def test_later_diagnostics_with_fresh_ledger_wait_for_expensive_hours(self):
        result = self.replay()
        self.assertEqual(result.battery_strategy, 'accu_uit')
        first = next(w for w in result.planned_battery_mode_windows
                     if w['mode'].startswith('ontladen'))
        self.assertGreaterEqual(first['start'], '2026-11-15T14:00:00+01:00')

    def test_backdated_full_battery_must_not_discharge_before_rejected_refill(self):
        result = self.replay('2026-11-15T13:12:42.665548+01:00', soc=100.)
        self.assertEqual(result.battery_strategy, 'accu_uit')
        first = next(w for w in result.planned_battery_mode_windows
                     if w['mode'].startswith('ontladen'))
        self.assertGreaterEqual(first['start'], '2026-11-15T14:00:00+01:00')


    def test_captured_outlook_rejects_refill_and_completes_existing_full_cycle(self):
        from test_battery_energy_accounting import energy_trace
        result = self.replay()
        windows = result.planned_battery_mode_windows + result.estimated_battery_mode_windows
        trace = energy_trace(self.replay_now, self.replay_slots, windows,
                             initial=9.7, max_charge=2.5)
        tomorrow = [(t, e, mode) for t, e, mode in trace if t.startswith('2026-11-16')]
        self.assertTrue(tomorrow)
        # Strict acquisition economics reject the former cheap-hour refill.
        # Existing stock can still finish its committed full cycle, but each
        # selected household slot must cover the captured acquisition + margin.
        minimum_price = (self.capture['coordinator_state']['battery_grid_charge_price']
                         + self.capture['effective_config']['battery_min_profit_per_kwh'])
        discharge = [w for w in windows if w['mode'].startswith('ontladen')]
        for slot in self.replay_slots:
            if any(datetime.fromisoformat(w['start']) < slot['end']
                   and datetime.fromisoformat(w['end']) > slot['start']
                   for w in discharge):
                self.assertGreaterEqual(slot['import_price'] + 1e-9, minimum_price)
        self.assertFalse(result.planned_grid_charge_windows)
        self.assertFalse(result.planned_solar_charge_windows)
        self.assertFalse(any(mode.startswith('laden') for _, _, mode in trace))
        self.assertTrue(any(mode.startswith('ontladen') for _, _, mode in tomorrow))
        self.assertAlmostEqual(max(e for _, e, _ in trace), 9.7, delta=.005)
        self.assertAlmostEqual(tomorrow[-1][1], 2., delta=.005)
        self.assertGreaterEqual(min(e for _, e, _ in trace), 2.-.005)


if __name__ == '__main__':
    unittest.main()
