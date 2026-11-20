"""Approved early SOLAR reversal: full, profitable, without inventory amnesty."""
import unittest
from datetime import datetime, timedelta, timezone

from test_battery_energy_accounting import coordinator, energy_trace


class FullSolarRefillTest(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 11, 20, 10, tzinfo=timezone.utc)
        self.c = coordinator()
        self.c._charge_session_started = False
        self.c._discharge_session_started = True
        self.c._battery_cycle_discharge_floor_kwh = 4.
        self.c._battery_grid_charge_price = .271

    def slots(self, solar=4., demand=4., price=.44):
        return [dict(start=self.now + timedelta(hours=i),
                     end=self.now + timedelta(hours=i+1), hours=1.,
                     net_solar_kwh=net, import_price=p, export_price=.263,
                     price_known=True)
                for i, (net, p) in enumerate([(solar/2, .383), (solar/2, .383),
                                             (-demand/2, price), (-demand/2, price)])]

    def plan(self, slots, energy=4.8):
        solar, grid = self.c._plan_charge_windows_for_horizon(
            slots=slots, now=self.now, usable_capacity_kwh=8.,
            current_remaining_capacity_kwh=8.-energy, max_charge_kw=2.5,
            max_discharge_kw=3., battery_min_profit=.1, no_charge_reserve_kwh=4.)
        windows, mode = self.c._build_mode_windows_from_hourly_plan(
            slots=slots, now=self.now, planned_solar_charge_windows=solar,
            planned_grid_charge_windows=grid, initial_usable_energy_kwh=energy,
            usable_capacity_kwh=8., average_price=.4, average_export_price=.263,
            max_charge_kw=2.5, max_discharge_kw=3., battery_min_profit=.1,
            no_charge_reserve_kwh=4.)
        return solar, grid, windows, mode

    def test_later_fragment_of_same_solar_fill_does_not_hide_evening(self):
        slots = self.slots()
        slots.insert(2, dict(slots[1], start=self.now+timedelta(hours=2),
            end=self.now+timedelta(hours=3), net_solar_kwh=.2))
        for i, row in enumerate(slots):
            row.update(start=self.now+timedelta(hours=i), end=self.now+timedelta(hours=i+1))
        solar, _, windows, mode = self.plan(slots)
        self.assertTrue(solar)
        self.assertEqual(mode, 'laden_met_zonne_energie')

    def test_profitable_partial_solar_reverses_under_superseding_policy(self):
        # Approved partial PV replaces the historical full-solar-only condition.
        _, _, windows, mode = self.plan(self.slots(solar=1.))
        self.assertEqual(mode, 'laden_met_zonne_energie')
        self.assertTrue(any(w['mode'] == 'laden_met_zonne_energie' for w in windows))
        self.assertTrue(self.c._charge_session_started)
        self.assertFalse(self.c._discharge_session_started)

    def test_new_solar_minimum_margin_is_not_funded_by_old_stock(self):
        _, _, windows, _ = self.plan(self.slots(price=.35))
        self.assertFalse(any(w['mode'].startswith('laden_') for w in windows))
        self.assertFalse(self.c._charge_session_started)

    def test_existing_inventory_competes_for_scarce_later_demand(self):
        # 3.3 kWh can now finance the selected partial PV plus existing stock;
        # use genuinely insufficient demand under the approved partial policy.
        _, _, windows, _ = self.plan(self.slots(demand=1.))
        self.assertFalse(any(w['mode'].startswith('laden_') for w in windows))
        self.assertFalse(self.c._charge_session_started)

    def test_historical_acquisition_guard_is_not_waived(self):
        self.c._battery_grid_charge_price = .6
        _, _, windows, _ = self.plan(self.slots())
        self.assertFalse(any(w['mode'].startswith(('laden_', 'ontladen')) for w in windows))
        self.assertEqual(self.c._battery_grid_charge_price, .6)

    def test_early_grid_remains_rejected(self):
        slots = self.slots(solar=0.)
        for row in slots[:2]:
            row.update(import_price=.05, export_price=.01)
        solar, grid, windows, _ = self.plan(slots)
        self.assertTrue(grid)
        self.assertFalse(solar)
        self.assertFalse(any(w['mode'].startswith('laden_') for w in windows))
        self.assertFalse(self.c._charge_session_started)

    def test_rejected_partial_solar_does_not_spend_evening_stock_on_cheaper_early_demand(self):
        slots = self.slots(solar=1., demand=.8, price=.8)
        for row in slots:
            row['start'] += timedelta(hours=1)
            row['end'] += timedelta(hours=1)
        slots.insert(0, dict(slots[0], start=self.now, end=self.now+timedelta(hours=1),
                             net_solar_kwh=-.5, import_price=.4))
        _, _, windows, _ = self.plan(slots)
        trace = energy_trace(self.now, slots, windows, initial=4.8, max_charge=2.5)
        early = [r for r in trace if datetime.fromisoformat(r[0]) <= self.now+timedelta(hours=1)]
        self.assertFalse(any(r[2].startswith('ontladen') for r in early))
        self.assertTrue(any(r[2] == 'ontladen' for r in trace))
        self.assertGreaterEqual(min(r[1] for r in trace), 4.-.005)

    def test_unknown_later_prices_cannot_justify_early_reversal(self):
        slots = self.slots()
        for row in slots[2:]:
            row['price_known'] = False
        windows, _ = self.c._build_mode_windows_from_hourly_plan(
            slots=slots, now=self.now, planned_solar_charge_windows=[dict(
                start=self.now.isoformat(), end=(self.now+timedelta(hours=2)).isoformat(),
                price=.263, charge_kwh=3.2, usable_hours=2.)],
            planned_grid_charge_windows=[], initial_usable_energy_kwh=4.8,
            usable_capacity_kwh=8., average_price=.4, average_export_price=.263,
            max_charge_kw=2.5, max_discharge_kw=3., battery_min_profit=.1,
            no_charge_reserve_kwh=4.)
        self.assertFalse(any(w['mode'].startswith('laden_') for w in windows))

    def test_early_permission_cannot_authorize_overlapping_grid(self):
        slots = self.slots()
        window = dict(start=self.now.isoformat(), end=(self.now+timedelta(hours=2)).isoformat(),
                      price=.263, charge_kwh=3.2, usable_hours=2.)
        windows, _ = self.c._build_mode_windows_from_hourly_plan(
            slots=slots, now=self.now, planned_solar_charge_windows=[window],
            planned_grid_charge_windows=[dict(window, price=.01)],
            initial_usable_energy_kwh=4.8, usable_capacity_kwh=8., average_price=.4,
            average_export_price=.263, max_charge_kw=2.5, max_discharge_kw=3.,
            battery_min_profit=.1, no_charge_reserve_kwh=4.)
        self.assertFalse(any(w['mode'] == 'laden_van_net' for w in windows))

    def test_repeated_unmeasured_plans_and_json_restore_preserve_latch_and_ledger(self):
        import json
        from types import SimpleNamespace
        from unittest.mock import patch
        self.c._battery_margin_state = {'at': self.now.isoformat(), 'energy': 4.8, 'reserve': 0.,
                'lots': [[4.8, .271]], 'credit': -.2, 'actions': []}
        first = self.plan(self.slots())[2]
        for _ in range(3):
            self.assertEqual(self.plan(self.slots())[2], first)
            self.assertTrue(self.c._charge_session_started)
            self.assertEqual(self.c._battery_margin_state['lots'], [[4.8, .271]])
            self.assertEqual(self.c._battery_margin_state['credit'], -.2)
            self.assertEqual(self.c._battery_margin_state['energy'], 4.8)
        self.c.hass = SimpleNamespace(data={})
        self.c.config_entry = SimpleNamespace(entry_id='solar-refill')
        self.c._store_battery_cycle_state_snapshot(self.now)
        saved = json.loads(json.dumps(self.c.hass.data))
        restored = coordinator()
        restored._charge_session_started = False
        restored._discharge_session_started = False
        restored._battery_cycle_state_initialized = False
        restored.hass = SimpleNamespace(data=saved)
        restored.config_entry = self.c.config_entry
        clock = restored._restore_recent_battery_cycle_state.__globals__['dt_util']
        with patch.object(clock, 'now', return_value=self.now):
            restored._restore_recent_battery_cycle_state()
        self.c = restored
        self.assertTrue(restored._charge_session_started)
        self.assertFalse(restored._discharge_session_started)
        self.assertEqual(self.plan(self.slots())[2], first)
        self.assertTrue(restored._charge_session_started, 'forecast full is not measured full')
        self.plan(self.slots(), energy=6.)
        self.assertTrue(restored._charge_session_started)
        self.plan(self.slots(), energy=8.)
        self.assertFalse(restored._charge_session_started)

    def test_complete_coordinator_uses_exception_and_observed_full_latch(self):
        from unittest.mock import patch
        from test_battery_energy_accounting import replay_full_plan
        slots = self.slots()
        for row in slots:
            row.update(solar_kwh=max(0., row['net_solar_kwh']),
                       demand_kwh=max(0., -row['net_solar_kwh']))
        namespace = self.c._build_plan_for_horizon.__globals__
        def replan(soc):
            with patch('test_battery_energy_accounting.feedback_slots', return_value=(self.now, slots)), \
                 patch.dict(namespace, {'build_energy_balance_slots': lambda **kw: slots}):
                return replay_full_plan(instance=self.c, soc=soc, reserve=60., max_charge=2.5,
                    profit=.1, tax_deduction=.12)[2]
        result = replan(68.)
        self.assertEqual(result.battery_strategy, 'laden_met_zonne_energie')
        self.assertTrue(self.c._charge_session_started)
        trace = energy_trace(self.now, slots, result.planned_battery_mode_windows,
                             initial=4.8, max_charge=2.5)
        self.assertAlmostEqual(max(r[1] for r in trace), 8., delta=.005)
        replan(68.)
        self.assertTrue(self.c._charge_session_started)
        replan(80.)
        self.assertTrue(self.c._charge_session_started)
        replan(100.)
        self.assertFalse(self.c._charge_session_started)

    def test_profitable_full_solar_refill_above_committed_reserve(self):
        slots = self.slots()
        solar, grid, windows, mode = self.plan(slots)
        self.assertTrue(solar, 'source selector must propose profitable solar')
        self.assertFalse(grid)
        self.assertEqual(mode, 'laden_met_zonne_energie')
        trace = energy_trace(self.now, slots, windows, initial=4.8, max_charge=2.5)
        self.assertAlmostEqual(max(row[1] for row in trace), 8., places=5)
        self.assertTrue(self.c._charge_session_started)
        self.assertFalse(self.c._discharge_session_started)
        self.assertEqual(self.c._battery_cycle_discharge_floor_kwh, 4.)


if __name__ == '__main__':
    unittest.main()
