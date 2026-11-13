"""Partial-cycle policy: physical energy and reserve, not full/empty latches."""
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

    def test_current_partial_solar_beats_dearer_grid_despite_unfinished_cycle(self):
        slots = [slot(self.now, 0, .2, .02, solar=1.),
                 slot(self.now, 1, .25, .02),
                 slot(self.now, 2, .65, .05, demand=1.4)]
        for charging in (False, True):
            for restored in (False, True):
                with self.subTest(charging=charging, restored=restored):
                    _, solar, grid, windows = self.plan(slots, charging=charging, restored=restored)
                    self.assertTrue(solar)
                    self.assertEqual(windows[0]['mode'], 'laden_met_zonne_energie')
                    trace = energy_trace(self.now, slots, windows, initial=6.5, max_charge=2.5)
                    self.assertGreaterEqual(min(e for _, e, _ in trace), 5.995)
                    self.assertLessEqual(max(e for _, e, _ in trace), 10.005)
                    self.assertTrue(any(w['mode'] == 'ontladen' for w in windows))

    def test_stale_charge_latch_cannot_block_home_discharge_to_reserve(self):
        slots = [slot(self.now, i, .6, .05, demand=2.) for i in range(3)]
        for restored in (False, True):
            _, _, _, windows = self.plan(slots, initial=6., charging=True, restored=restored)
            trace = energy_trace(self.now, slots, windows, initial=8., max_charge=2.5)
            self.assertAlmostEqual(trace[-1][1], 6., delta=.005)

    def test_cheaper_useful_grid_still_beats_more_expensive_solar(self):
        slots = [slot(self.now, 0, .10, .05),
                 slot(self.now, 1, .50, .30, solar=1.),
                 slot(self.now, 2, .80, .05, demand=4.)]
        _, solar, grid, modes = self.plan(slots, initial=0.)
        self.assertTrue(grid)
        self.assertEqual(grid[0]['start'], self.now.isoformat())
        self.assertEqual(modes[0]['mode'], 'laden_van_net')
        self.assertEqual(solar, [])
        trace = energy_trace(self.now, slots, modes, initial=2., max_charge=2.5)
        self.assertGreaterEqual(min(e for _, e, _ in trace), 1.995)
        self.assertLessEqual(max(e for _, e, _ in trace), 10.005)

    def test_partial_grid_still_requires_minimum_profit(self):
        slots = [slot(self.now, 0, .50, .05),
                 slot(self.now, 1, .55, .05, demand=3.)]
        for charging in (False, True):
            with self.subTest(charging=charging):
                _, _, grid, modes = self.plan(slots, initial=0., charging=charging)
                self.assertEqual(grid, [])
                self.assertFalse(any(w['mode'] == 'laden_van_net' for w in modes))

    def test_profitable_partial_grid_fill_is_allowed_without_full_target(self):
        slots = [slot(self.now, 0, .1, .01), slot(self.now, 1, .5, .01, demand=1.)]
        _, _, grid, windows = self.plan(slots, initial=4.)
        self.assertTrue(grid)
        trace = energy_trace(self.now, slots, windows, initial=6., max_charge=2.5)
        self.assertGreater(max(e for _, e, _ in trace), 6.)
        self.assertLess(max(e for _, e, _ in trace), 10.)
        self.assertGreaterEqual(min(e for _, e, _ in trace), 5.995)


if __name__ == '__main__':
    unittest.main()
