"""New purchases need executable per-slot outlets, not averaged-away losses."""
from datetime import datetime, timedelta
from unittest.mock import Mock, patch
import unittest

from test_battery_energy_accounting import coordinator, replay_full_plan, energy_trace
from custom_components.smart_energy_planner.battery_profit import grid_supplement_profit


class StrictAdmissionTest(unittest.TestCase):
    def evaluate(self, purchases=(.15,), sales=(.30, .20), *, known=True,
                 home=1., export=0., historical=None):
        now = datetime(2026, 10, 10, 12)
        slots = [dict(start=now+timedelta(hours=i), end=now+timedelta(hours=i+1),
                      hours=1., net_solar_kwh=0., import_price=price, export_price=0.)
                 for i, price in enumerate(purchases)]
        selected = [dict(kind='grid', start=slot['start'], charge_kwh=2./len(purchases),
                         profit_cost=slot['import_price']) for slot in slots]
        after = slots[-1]['end']
        slots += [dict(start=after+timedelta(hours=i), end=after+timedelta(hours=i+1),
                       hours=1., net_solar_kwh=-home, import_price=price,
                       export_price=export, price_known=known)
                  for i, price in enumerate(sales)]
        return grid_supplement_profit(selected=selected, slots=slots, after=after,
            before=slots[-1]['end'], max_charge_kw=2., max_discharge_kw=1.,
            minimum_profit=.10, existing_energy=1. if historical is not None else 0.,
            historical_cost=historical, allow_export=True)

    def test_peak_does_not_subsidize_an_inexecutable_household_outlet(self):
        result = self.evaluate()
        self.assertFalse(result['profitable'])
        self.assertAlmostEqual(result['covered_kwh'], 1.)
        self.assertAlmostEqual(result['retained_kwh'], 1.)
        self.assertAlmostEqual(result['discharge_value'], .30)

    def test_maximum_purchase_cost_not_weighted_cost_sets_outlet_floor(self):
        result = self.evaluate(purchases=(.10, .20), sales=(.26, .26))
        self.assertFalse(result['profitable'])
        self.assertEqual(result['covered_kwh'], 0.)
        self.assertTrue(self.evaluate(purchases=(.10, .20), sales=(.30, .30))['profitable'])

    def test_estimated_home_outlets_allowed_but_estimated_export_is_not(self):
        self.assertTrue(self.evaluate(sales=(.25, .25), known=False)['profitable'])
        result = self.evaluate(sales=(.25, .25), known=False, home=0., export=.40)
        self.assertFalse(result['profitable'])
        self.assertEqual(result['covered_kwh'], 0.)

    def test_export_cannot_bypass_simultaneous_unprofitable_household_use(self):
        result = self.evaluate(sales=(.20, .20, .20, .20), home=.5, export=.40)
        self.assertFalse(result['profitable'])
        self.assertEqual(result['covered_kwh'], 0.)
        self.assertTrue(self.evaluate(sales=(.25, .25, .25, .25),
                                     home=.5, export=.40)['profitable'])
        self.assertTrue(self.evaluate(sales=(.20, .20), home=0., export=.40)['profitable'])

    def test_historical_stock_cannot_be_averaged_away(self):
        result = self.evaluate(sales=(.35, .35), historical=.30)
        self.assertFalse(result['profitable'])
        self.assertEqual(result['covered_kwh'], 0.)

    def test_supplied_snapshot_does_not_buy_energy_without_executable_outlets(self):
        now, slots, result = replay_full_plan('2026_10_10_short_cycles',
            '2026-10-10T09:15:10.729241+02:00', soc=20, reserve=60,
            profit=.10, max_charge=2.5, snapshot_exports=True, isolated_cycles=True)
        self.assertFalse(result.planned_grid_charge_windows)
        windows = result.planned_battery_mode_windows + result.estimated_battery_mode_windows
        self.assertFalse(any(w['mode'] == 'laden_van_net' for w in windows))
        trace = energy_trace(now, slots, windows, initial=2., max_charge=2.5)
        self.assertTrue(trace)
        self.assertGreater(max(e for _, e, _ in trace), 2.)  # Real PV remains useful.
        self.assertGreaterEqual(min(e for _, e, _ in trace), 1.995)
        self.assertLessEqual(max(e for _, e, _ in trace), 10.005)

    def slots(self, prices, *, demand=1.):
        now = datetime(2026, 10, 10, 12)
        return now, [dict(start=now+timedelta(hours=i), end=now+timedelta(hours=i+1),
            hours=1., net_solar_kwh=-demand, solar_kwh=0., demand_kwh=demand,
            import_price=price, export_price=-.02, price_known=True)
            for i, price in enumerate(prices)]

    def test_planner_rejects_full_purchase_with_only_weighted_margin(self):
        now, slots = self.slots((.15, .15, .30, .20))
        c = coordinator()
        c._charge_session_started = False
        solar, grid = c._plan_charge_windows_for_horizon(slots=slots, now=now,
            usable_capacity_kwh=2., current_remaining_capacity_kwh=2.,
            max_charge_kw=1., max_discharge_kw=1., battery_min_profit=.10)
        self.assertFalse(solar)
        self.assertFalse(grid)

    def test_persisted_expensive_lot_blocks_cheap_top_up(self):
        now, slots = self.slots((.15, .15, .30, .30))
        c = coordinator()
        c._charge_session_started = False
        c._battery_grid_charge_price = .15
        c._battery_margin_state = dict(at=now.isoformat(), energy=1., reserve=0.,
            lots=[[1., .50]], credit=0., actions=[])
        _, grid = c._plan_charge_windows_for_horizon(slots=slots, now=now,
            usable_capacity_kwh=2., current_remaining_capacity_kwh=1.,
            max_charge_kw=1., max_discharge_kw=1., battery_min_profit=.10)
        self.assertFalse(grid)

    def test_consumed_persisted_lot_does_not_leave_a_stale_admission_floor(self):
        now, slots = self.slots((.15, .30, .30))
        c = coordinator()
        c._charge_session_started = False
        c._battery_grid_charge_price = .15
        c._battery_margin_state = dict(at=(now-timedelta(hours=1)).isoformat(),
            energy=2., reserve=0., lots=[[1., .50], [1., .15]], credit=0., actions=[])
        evaluate = Mock(wraps=grid_supplement_profit)
        # Discovery imports other coordinator stubs; patch this instance's globals.
        target = c._plan_charge_windows_for_horizon.__func__.__globals__
        with patch.dict(target, grid_supplement_profit=evaluate):
            _, grid = c._plan_charge_windows_for_horizon(slots=slots, now=now,
                usable_capacity_kwh=2., current_remaining_capacity_kwh=1.,
                max_charge_kw=1., max_discharge_kw=1., battery_min_profit=.10)
        self.assertAlmostEqual(sum(w['charge_kwh'] for w in grid), 1.)
        self.assertAlmostEqual(evaluate.call_args_list[0].kwargs['historical_cost'], .15)
        self.assertEqual(c._battery_margin_state['lots'], [[1., .50], [1., .15]])

    def test_projection_preserves_expensive_stock_during_cheap_household_slots(self):
        now, slots = self.slots((.20, .15, .60, .60))
        c = coordinator()
        c._charge_session_started = False
        c._battery_grid_charge_price = .50
        evaluate = Mock(wraps=grid_supplement_profit)
        # Discovery imports other coordinator stubs; patch this instance's globals.
        target = c._plan_charge_windows_for_horizon.__func__.__globals__
        with patch.dict(target, grid_supplement_profit=evaluate):
            _, grid = c._plan_charge_windows_for_horizon(slots=slots, now=now,
                usable_capacity_kwh=2., current_remaining_capacity_kwh=1.,
                max_charge_kw=1., max_discharge_kw=1., battery_min_profit=.10)
        self.assertTrue(grid)
        self.assertEqual(grid[0]['start'], slots[1]['start'].isoformat())
        self.assertAlmostEqual(sum(w['charge_kwh'] for w in grid), 1.)
        self.assertAlmostEqual(evaluate.call_args_list[0].kwargs['existing_energy'], 1.)

    def test_observed_partial_fill_finishes_with_outlets_for_existing_and_new_stock(self):
        now, slots = self.slots((.15, .30, .30))
        c = coordinator()
        c._charge_session_started = True
        c._battery_cycle_state_initialized = True
        c._battery_grid_charge_price = .15
        c._active_charge_phase_mode = 'laden_van_net'
        c._active_charge_phase_end = now+timedelta(hours=1)
        _, grid = c._plan_charge_windows_for_horizon(slots=slots, now=now,
            usable_capacity_kwh=2., current_remaining_capacity_kwh=1.,
            max_charge_kw=1., max_discharge_kw=1., battery_min_profit=.10)
        self.assertAlmostEqual(sum(w['charge_kwh'] for w in grid), 1.)

    def test_observed_partial_fill_rejects_import_when_existing_stock_uses_the_outlet(self):
        now, slots = self.slots((.15, .30))
        c = coordinator()
        c._charge_session_started = True
        c._battery_cycle_state_initialized = True
        c._battery_grid_charge_price = .15
        c._active_charge_phase_mode = 'laden_van_net'
        c._active_charge_phase_end = now+timedelta(hours=1)
        _, grid = c._plan_charge_windows_for_horizon(slots=slots, now=now,
            usable_capacity_kwh=2., current_remaining_capacity_kwh=1.,
            max_charge_kw=1., max_discharge_kw=1., battery_min_profit=.10)
        self.assertFalse(grid)

    def test_rejected_bundled_candidate_cannot_restore_an_invalid_original(self):
        now, slots = self.slots((.15, .30, .30))
        c = coordinator()
        c._charge_session_started = True
        c._battery_cycle_state_initialized = True
        c._battery_grid_charge_price = .15
        c._active_charge_phase_mode = 'laden_van_net'
        c._active_charge_phase_end = now+timedelta(hours=1)
        evaluations = []

        def reject_final_candidates(**kwargs):
            result = grid_supplement_profit(**kwargs)
            evaluations.append(kwargs)
            # Let the provisional selection pass. Both final candidates must
            # independently pass validation; a fallback is not pre-approved.
            if len(evaluations) > 1:
                result = {**result, 'profitable': False}
            return result

        target = c._plan_charge_windows_for_horizon.__func__.__globals__
        with patch.dict(target, grid_supplement_profit=reject_final_candidates):
            _, grid = c._plan_charge_windows_for_horizon(slots=slots, now=now,
                usable_capacity_kwh=2., current_remaining_capacity_kwh=1.,
                max_charge_kw=1., max_discharge_kw=1., battery_min_profit=.10)
        self.assertGreaterEqual(len(evaluations), 3)
        self.assertFalse(grid)

    def test_bundling_revalidates_even_a_running_fill(self):
        now, slots = self.slots((.15, .15, .30, .30))
        c = coordinator()
        c._charge_session_started = True
        c._battery_cycle_state_initialized = True
        c._battery_grid_charge_price = .15
        c._active_charge_phase_mode = 'laden_van_net'
        c._active_charge_phase_end = now+timedelta(hours=1)
        evaluate = Mock(wraps=grid_supplement_profit)
        # Discovery imports other coordinator stubs; patch this instance's globals.
        target = c._plan_charge_windows_for_horizon.__func__.__globals__
        with patch.dict(target, grid_supplement_profit=evaluate):
            _, grid = c._plan_charge_windows_for_horizon(slots=slots, now=now,
                usable_capacity_kwh=2., current_remaining_capacity_kwh=1.,
                max_charge_kw=1., max_discharge_kw=1., battery_min_profit=.10)
        self.assertTrue(grid)
        self.assertGreaterEqual(evaluate.call_count, 2)
        self.assertEqual(evaluate.call_args_list[0].kwargs['minimum_profit'], .10)
        self.assertEqual(evaluate.call_args_list[1].kwargs['minimum_profit'], .10)


if __name__ == '__main__':
    unittest.main()
