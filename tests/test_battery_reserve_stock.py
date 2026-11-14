"""Reserve stays stock at cost and cannot consume a profitable refill's demand."""
from datetime import datetime, timedelta
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from test_battery_energy_accounting import coordinator, energy_trace, replay_full_plan
from custom_components.smart_energy_planner.battery_profit import grid_supplement_profit


class ReserveStockTest(unittest.TestCase):
    def evaluate(self, *, home=1., export=.32, price=.44, existing=0., reserve=4.,
                 imported=8., budget=4., purchase_prices=(.26, .26)):
        now = datetime(2026, 10, 26, 12)
        slots = [dict(start=now+timedelta(hours=i), end=now+timedelta(hours=i+1),
                      hours=1., net_solar_kwh=0., import_price=p, export_price=p-.12)
                 for i, p in enumerate(purchase_prices)]
        selected = [dict(kind='grid', start=s['start'], charge_kwh=imported/2,
                         profit_cost=s['import_price']) for s in slots]
        slots += [dict(start=now+timedelta(hours=i), end=now+timedelta(hours=i+1),
                       hours=1., net_solar_kwh=-home, import_price=price,
                       export_price=export) for i in range(2, 6)]
        return grid_supplement_profit(selected=selected, slots=slots,
            after=now+timedelta(hours=2), before=slots[-1]['end'],
            max_charge_kw=4, max_discharge_kw=1, minimum_profit=.10,
            existing_energy=existing, retained_reserve_kwh=reserve,
            discharge_budget_kwh=budget, allow_export=True)

    def test_retained_imports_stay_at_weighted_cost_without_fictitious_profit(self):
        result = self.evaluate(price=.35, purchase_prices=(.20, .30))
        self.assertTrue(result['profitable'])
        self.assertAlmostEqual(result['average_profit'], .10)
        self.assertAlmostEqual(result['covered_kwh'], 4.)
        self.assertAlmostEqual(result['retained_kwh'], 4.)
        self.assertAlmostEqual(result['discharged_import_cost'], 1.)
        self.assertAlmostEqual(result['retained_import_cost'], 1.)
        self.assertAlmostEqual(result['import_cost'], 2.)
        self.assertFalse(self.evaluate(price=.349, purchase_prices=(.20, .30))['profitable'])

    def test_existing_reserve_does_not_take_the_new_imports_profitable_demand(self):
        result = self.evaluate(imported=4., existing=4.)
        self.assertTrue(result['profitable'])
        self.assertAlmostEqual(result['covered_kwh'], 4.)
        self.assertAlmostEqual(result['retained_kwh'], 0.)
        # Without a reserved part, existing stock must still be used first.
        self.assertFalse(self.evaluate(imported=4., existing=4., reserve=0.)['profitable'])
        self.assertAlmostEqual(self.evaluate(imported=4., existing=5.)['covered_kwh'], 3.)

    def test_home_savings_and_export_keep_separate_prices(self):
        self.assertTrue(self.evaluate()['profitable'])
        self.assertFalse(self.evaluate(home=0.)['profitable'])
        self.assertTrue(self.evaluate(home=0., export=.36)['profitable'])

    def test_reserve_and_tiny_peak_alone_cannot_justify_a_full_purchase(self):
        self.assertFalse(self.evaluate(home=0., export=0.)['profitable'])
        self.assertFalse(self.evaluate(home=.01, export=0.)['profitable'])
        self.assertFalse(self.evaluate(budget=0.)['profitable'])
        self.assertFalse(self.evaluate(imported=0.)['profitable'])
        # With all capacity released, full demand remains necessary.
        self.assertFalse(self.evaluate(budget=8., reserve=0.)['profitable'])

    def test_unavoidable_sixty_percent_remainder_can_be_filled_to_full(self):
        now = datetime(2026, 10, 26, 10)
        c = coordinator()
        c._charge_session_started, c._discharge_session_started = False, True
        c._battery_grid_charge_price = .30
        c._cycle_export_planning = True
        slots = [dict(start=now+timedelta(hours=i), end=now+timedelta(hours=i+1),
                      hours=1., import_price=.10 if i < 4 else .40,
                      export_price=-.02 if i < 4 else .28, price_known=True,
                      demand_kwh=0. if i < 4 else 1., solar_kwh=0.,
                      net_solar_kwh=0. if i < 4 else -1.) for i in range(8)]
        solar, grid = c._plan_charge_windows_for_horizon(slots=slots, now=now,
            usable_capacity_kwh=8, current_remaining_capacity_kwh=4,
            max_charge_kw=2.5, max_discharge_kw=3, battery_min_profit=.10,
            no_charge_reserve_kwh=4)
        self.assertTrue(grid)
        windows, mode = c._build_mode_windows_from_hourly_plan(slots=slots, now=now,
            planned_solar_charge_windows=solar, planned_grid_charge_windows=grid,
            initial_usable_energy_kwh=4, usable_capacity_kwh=8, battery_soc_percent=60,
            average_price=.25, average_export_price=.13, max_charge_kw=2.5,
            max_discharge_kw=3, no_charge_reserve_kwh=4, battery_min_profit=.10)
        self.assertEqual(mode, 'laden_van_net')
        trace = energy_trace(now, slots, windows, initial=6., max_charge=2.5)
        self.assertAlmostEqual(max(e for _, e, _ in trace), 10.)
        self.assertAlmostEqual(min(e for _, e, _ in trace), 6.)
        self.assertAlmostEqual(trace[-1][1], 6.)
        self.assertFalse(any(w['mode'] == 'ontladen_naar_net' for w in windows))
        imported = sum((datetime.fromisoformat(w['end'])-datetime.fromisoformat(w['start'])).total_seconds()
                       /3600*2.5 for w in windows if w['mode'] == 'laden_van_net')
        self.assertAlmostEqual(imported, 4.)

    def replay(self, now, energy=6., instance=None):
        return replay_full_plan('2026_10_25', now.isoformat(), soc=energy*10,
            discharging=True, reserve=60, max_charge=2.5, profit=.1,
            snapshot_exports=True, isolated_cycles=True, instance=instance)

    def test_supplied_snapshot_admits_full_refill_and_releases_reserve(self):
        now = datetime.fromisoformat('2026-10-25T17:49:43.736008+01:00')
        _, slots, result = self.replay(now)
        self.assertEqual(result.battery_strategy, 'ontladen')
        self.assertTrue(result.planned_grid_charge_windows)
        self.assertTrue(result.battery_no_charge_reserve_active)
        windows = result.planned_battery_mode_windows + result.estimated_battery_mode_windows
        trace = energy_trace(now, slots, windows, initial=6., max_charge=2.5)
        # A physically complete profitable new refill, not a continuation
        # waiver, releases reserve before it starts.
        self.assertAlmostEqual(min(e for _, e, _ in trace), 2., delta=.005)
        self.assertAlmostEqual(max(e for _, e, _ in trace), 10., delta=.005)
        self.assertAlmostEqual(trace[-1][1], 2., delta=.005)
        from test_battery_partial_cycle_economics import imported_margin
        ledger = imported_margin(slots, windows, initial_usable=4., reserve_usable=0.)
        self.assertGreaterEqual(ledger['margin'], .1, ledger)
        for window in windows:
            if window['mode'] == 'ontladen_naar_net':
                # Check the realized FIFO prefix, not the average acquisition
                # cost of different lots sold later in the outlook.
                prefix = imported_margin(slots, [w for w in windows if w['end'] <= window['end']],
                                         initial_usable=4., reserve_usable=0.)
                if prefix['sold'] > 1e-6:
                    self.assertGreaterEqual(prefix['margin'], .1 - 1e-9, prefix)
        # The outlook retains four physical kWh, including solar. Retained
        # imports remain stock, not fictitious sale revenue.
        self.assertGreater(ledger['imported'], 4.)
        self.assertGreater(ledger['imported'] - ledger['sold'], 0.)
        self.assertLessEqual(ledger['imported'] - ledger['sold'], 4.005)
        self.assertGreaterEqual(ledger['value']-ledger['sold']*ledger['average_cost'],
                                ledger['sold']*.10)

    def test_fixed_cadence_feedback_releases_completed_fill(self):
        self._assert_fixed_cadence_feedback()

    def test_fixed_cadence_restart_during_fill_reconciles_completion(self):
        self._assert_fixed_cadence_feedback(restart=True)

    def _assert_fixed_cadence_feedback(self, restart=False):
        c = coordinator()
        c._charge_session_started, c._discharge_session_started = False, True
        now = datetime.fromisoformat('2026-10-25T17:49:43.736008+01:00')
        energy, peak = 6., 6.
        for index in range(52):
            _, slots, result = self.replay(now, energy, c)
            if index == 41:
                self.assertFalse(c._charge_session_started)
                self.assertTrue(c._discharge_session_started)
            if restart and index == 40:
                self.assertTrue(c._charge_session_started)
                c.hass = SimpleNamespace(data={})
                c.config_entry.entry_id = 'fixed-reserve-fill'
                c._store_battery_cycle_state_snapshot(now)
                restored = coordinator()
                restored.hass, restored.config_entry = c.hass, c.config_entry
                with patch.object(c._restore_recent_battery_cycle_state.__globals__['dt_util'],
                                  'now', return_value=now):
                    restored._restore_recent_battery_cycle_state()
                self.assertTrue(restored._charge_session_started)
                self.assertEqual(c._battery_margin_state, restored._battery_margin_state)
                c = restored
                del c.hass
            until = now + timedelta(minutes=15)
            commands = [{**w, 'end': min(datetime.fromisoformat(w['end']), until).isoformat()}
                        for w in result.planned_battery_mode_windows
                        if datetime.fromisoformat(w['start']) < until]
            trace = energy_trace(now, slots, commands, initial=energy, max_charge=2.5)
            self.assertTrue(trace)
            self.assertGreaterEqual(min(e for _, e, _ in trace), 1.995)
            self.assertLessEqual(max(e for _, e, _ in trace), 10.005)
            peak = max(peak, max(e for _, e, _ in trace))
            energy = trace[-1][1]
            now = until
        self.assertAlmostEqual(peak, 10., delta=.005)
        self.assertFalse(c._charge_session_started, (now, energy, result.battery_strategy))
        self.assertTrue(c._discharge_session_started)
        self.assertTrue(result.battery_strategy.startswith('ontladen'))
        self.assertLess(energy, 9.763553326620002)  # Continued measured discharge, no topup loop.

    def test_boundary_aligned_feedback_and_restart_keep_refill_after_releasing_reserve(self):
        c = coordinator()
        c._charge_session_started, c._discharge_session_started = False, True
        now = datetime.fromisoformat('2026-10-25T17:49:43.736008+01:00')
        energy, minimum, peak, saw_grid = 6., 6., 6., False
        # Releasing another four kWh of reserve needs a longer real refill.
        for index in range(52):
            _, slots, result = self.replay(now, energy, c)
            if index == 12:
                c.hass = SimpleNamespace(data={})
                c.config_entry.entry_id = 'reserve-stock'
                c._store_battery_cycle_state_snapshot(now)
                restored = coordinator()
                restored.hass, restored.config_entry = c.hass, c.config_entry
                with patch.object(c._restore_recent_battery_cycle_state.__globals__['dt_util'],
                                  'now', return_value=now):
                    restored._restore_recent_battery_cycle_state()
                c = restored
                del c.hass
                _, slots, again = self.replay(now, energy, c)
                self.assertEqual(result.planned_battery_mode_windows, again.planned_battery_mode_windows)
            until = min([now + timedelta(minutes=15)] + [
                datetime.fromisoformat(w['end'])
                for w in result.planned_battery_mode_windows
                if datetime.fromisoformat(w['start']) <= now < datetime.fromisoformat(w['end'])])
            commands = [{**w, 'end': min(datetime.fromisoformat(w['end']), until).isoformat()}
                        for w in result.planned_battery_mode_windows
                        if datetime.fromisoformat(w['start']) < until]
            trace = energy_trace(now, slots, commands, initial=energy, max_charge=2.5)
            self.assertTrue(trace)
            self.assertGreaterEqual(min(e for _, e, _ in trace), 1.995)
            self.assertLessEqual(max(e for _, e, _ in trace), 10.005)
            saw_grid |= any(w['mode'] == 'laden_van_net' for w in commands)
            energy = trace[-1][1]
            minimum = min(minimum, min(e for _, e, _ in trace))
            peak = max(peak, max(e for _, e, _ in trace))
            now = until
        self.assertTrue(saw_grid)
        self.assertAlmostEqual(minimum, 2., delta=.005)
        # Only a genuinely started fill gets continuation treatment. This
        # admitted refill now completes to maximum, then releases discharge.
        self.assertAlmostEqual(peak, 10., delta=.005)
        self.assertFalse(c._charge_session_started)
        self.assertTrue(c._discharge_session_started)
        future = energy_trace(now, slots,
            [{**w, 'start': max(now, datetime.fromisoformat(w['start'])).isoformat()}
             for w in result.planned_battery_mode_windows
             if datetime.fromisoformat(w['end']) > now],
            initial=energy, max_charge=2.5)
        self.assertAlmostEqual(max(e for _, e, _ in future), 10., delta=.005)
