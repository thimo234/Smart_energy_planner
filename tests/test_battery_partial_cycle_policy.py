"""Superseded partial trades now respect the selected cycle direction."""
from datetime import datetime, timedelta
import unittest
from test_battery_energy_accounting import coordinator, energy_trace
from test_battery_solar_topup_regression import slot


class PartialCyclePolicyTest(unittest.TestCase):
    def setUp(self):
        self.now = datetime.fromisoformat('2026-11-13T12:00:00+01:00')

    def plan(self, slots, initial=4.5, reserve=4., charging=False, restored=False):
        c = coordinator()
        c._charge_session_started = charging
        c._discharge_session_started = not charging
        c._battery_cycle_state_restored_recent = restored
        solar, grid = c._plan_charge_windows_for_horizon(
            slots=slots, now=self.now, usable_capacity_kwh=8.,
            current_remaining_capacity_kwh=8.-initial, max_charge_kw=2.5,
            max_discharge_kw=3., battery_min_profit=.1, no_charge_reserve_kwh=reserve)
        windows, _ = c._build_mode_windows_from_hourly_plan(
            slots=slots, now=self.now, planned_solar_charge_windows=solar,
            planned_grid_charge_windows=grid, initial_usable_energy_kwh=initial,
            usable_capacity_kwh=8., average_price=.4, average_export_price=.1,
            max_charge_kw=2.5, max_discharge_kw=3., no_charge_reserve_kwh=reserve,
            battery_min_profit=.1)
        return c, solar, grid, windows

    def test_current_partial_solar_respects_unfinished_cycle(self):
        slots = [slot(self.now, 0, .2, .02, solar=1.),
                 slot(self.now, 1, .25, .02),
                 slot(self.now, 2, .65, .05, demand=1.4)]
        for charging in (False, True):
            for restored in (False, True):
                with self.subTest(charging=charging, restored=restored):
                    _, solar, grid, windows = self.plan(slots, charging=charging, restored=restored)
                    self.assertTrue(solar)
                    self.assertEqual(windows[0]['mode'], 'laden_met_zonne_energie' if charging else 'accu_uit')
                    trace = energy_trace(self.now, slots, windows, initial=6.5, max_charge=2.5)
                    self.assertGreaterEqual(min(e for _, e, _ in trace), 5.995)
                    self.assertLessEqual(max(e for _, e, _ in trace), 10.005)
                    if charging:
                        self.assertFalse(any(w['mode'].startswith('ontladen') for w in windows))
                    else:
                        self.assertTrue(any(w['mode'] == 'ontladen' for w in windows))

    def test_charge_latch_preserves_incomplete_charge_through_restore(self):
        slots = [slot(self.now, i, .6, .05, demand=2.) for i in range(3)]
        for restored in (False, True):
            _, _, _, windows = self.plan(slots, initial=6., charging=True, restored=restored)
            trace = energy_trace(self.now, slots, windows, initial=8., max_charge=2.5)
            self.assertAlmostEqual(trace[-1][1], 8., delta=.005)
            self.assertFalse(any(w['mode'].startswith('ontladen') for w in windows))

    def test_cheaper_useful_grid_still_beats_more_expensive_solar(self):
        # Four cheap hours can physically fill eight usable kWh; the old
        # single 2.5-kWh slot could never authorize a new complete grid fill.
        slots = [*[slot(self.now, i, .10, .05) for i in range(4)],
                 slot(self.now, 4, .50, .30, solar=1.),
                 *[slot(self.now, i, .80, .05, demand=2.) for i in range(5, 9)]]
        _, solar, grid, modes = self.plan(slots, initial=0.)
        self.assertTrue(grid)
        self.assertEqual(grid[0]['start'], self.now.isoformat())
        self.assertEqual(modes[0]['mode'], 'laden_van_net')
        self.assertEqual(solar, [])
        self.assertAlmostEqual(sum(w['charge_kwh'] for w in grid), 8.)
        from bound_cycle_assertions import assert_bound_plan
        extrema, final = assert_bound_plan(
            self, self.now, slots, modes, initial=2., floor=6., max_charge=2.5)
        self.assertAlmostEqual(max(extrema), 10., delta=.005)
        self.assertAlmostEqual(final, 6., delta=.005)
        from test_battery_partial_cycle_economics import imported_margin
        ledger = imported_margin(slots, modes, initial_usable=0., reserve_usable=4.)
        self.assertAlmostEqual(ledger['sold'], 4., delta=.005)
        self.assertGreaterEqual(ledger['margin'], .1)

    def test_partial_grid_still_requires_minimum_profit(self):
        slots = [slot(self.now, 0, .50, .05),
                 slot(self.now, 1, .55, .05, demand=3.)]
        for charging in (False, True):
            with self.subTest(charging=charging):
                _, _, grid, modes = self.plan(slots, initial=0., charging=charging)
                self.assertEqual(grid, [])
                self.assertFalse(any(w['mode'] == 'laden_van_net' for w in modes))

    def test_profitable_partial_grid_fill_is_rejected_without_full_target(self):
        # Four kWh of room, but only one 2.5-kWh charge hour: a profitable
        # isolated sale is not executable after an unfinished new refill.
        slots = [slot(self.now, 0, .1, .01), slot(self.now, 1, .5, .01, demand=1.)]
        _, _, grid, windows = self.plan(slots, initial=4.)
        self.assertEqual(grid, [])
        self.assertFalse(any(w['mode'] == 'laden_van_net' for w in windows))
        trace = energy_trace(self.now, slots, windows, initial=6., max_charge=2.5)
        self.assertTrue(all(abs(e - 6.) < .005 for _, e, _ in trace))


if __name__ == '__main__':
    unittest.main()
