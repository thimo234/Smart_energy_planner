"""Price-based source choice and reserve release, including real commands."""
from datetime import datetime, timedelta
import unittest

from test_battery_energy_accounting import coordinator, energy_trace
from custom_components.smart_energy_planner.battery_planner import reserve_without_charge_opportunity


def scenario(grid_price=.10, solar_price=.40, simultaneous_solar=False):
    now = datetime(2026, 9, 25, 6)
    slots = []
    for i in range(16):
        grid = 2 <= i < 6
        solar = 6 <= i < 10 or (grid and simultaneous_solar)
        price = grid_price if grid else solar_price if solar else .50
        slots.append(dict(start=now + timedelta(hours=i), end=now + timedelta(hours=i+1),
                          hours=1., import_price=price, export_price=price-.11, price_known=True,
                          net_solar_kwh=2. if solar else -2., solar_kwh=3. if solar else 0.,
                          demand_kwh=1. if solar else 2.))
    return now, slots


class PricePriorityTest(unittest.TestCase):
    def plan(self, grid_price=.10, solar_price=.40, simultaneous_solar=False):
        now, slots = scenario(grid_price, solar_price, simultaneous_solar)
        c = coordinator()
        c._charge_session_started = False
        c._discharge_session_started = True
        solar, grid = c._plan_charge_windows_for_horizon(
            slots=slots, now=now, usable_capacity_kwh=8, current_remaining_capacity_kwh=4,
            max_charge_kw=2, max_discharge_kw=2, battery_min_profit=.08,
        )
        windows, mode = c._build_mode_windows_from_hourly_plan(
            slots=slots, now=now, planned_solar_charge_windows=solar, planned_grid_charge_windows=grid,
            initial_usable_energy_kwh=4, usable_capacity_kwh=8, battery_soc_percent=60,
            average_price=.30, average_export_price=.19, max_charge_kw=2, max_discharge_kw=2,
            no_charge_reserve_kwh=4, battery_min_profit=.08,
        )
        return now, slots, solar, grid, windows, mode

    def test_cheaper_grid_beats_abundant_more_expensive_solar(self):
        now, slots, solar, grid, windows, mode = self.plan()
        self.assertEqual(solar, [])
        self.assertAlmostEqual(sum(w['charge_kwh'] for w in grid), 8)
        self.assertEqual(grid[0]['start'], (now+timedelta(hours=2)).isoformat())
        self.assertEqual(mode, 'ontladen')
        before = [w for w in windows if w['end'] <= grid[0]['start']]
        trace = energy_trace(now, slots, before, initial=6, max_charge=2)
        self.assertAlmostEqual(trace[-1][1], 2, delta=.001)
        self.assertEqual(reserve_without_charge_opportunity(
            slots=slots, charge_windows=grid, after=now, reserve_kwh=4), 0)

    def test_eleven_cent_advantage_makes_solar_cheaper(self):
        _, _, solar, grid, _, _ = self.plan(grid_price=.30, solar_price=.40)
        self.assertEqual(grid, [])
        self.assertAlmostEqual(sum(w['charge_kwh'] for w in solar), 8)
        self.assertAlmostEqual(solar[0]['price'], .29)

    def test_negative_grid_price_uses_grid_even_with_solar(self):
        now, slots, solar, grid, windows, _ = self.plan(grid_price=-.02, simultaneous_solar=True)
        self.assertEqual(solar, [])
        self.assertAlmostEqual(sum(w['charge_kwh'] for w in grid), 8)
        trace = energy_trace(now, slots, windows, initial=6, max_charge=2)
        self.assertGreaterEqual(min(row[1] for row in trace), 2-.001)
        self.assertLessEqual(max(row[1] for row in trace), 10+.001)
        for a, b in zip(windows, windows[1:]):
            self.assertLessEqual(a['end'], b['start'])

    def test_unselected_sunshine_does_not_release_extra_reserve(self):
        now, slots = scenario()
        self.assertEqual(reserve_without_charge_opportunity(
            slots=slots, charge_windows=[], after=now, reserve_kwh=4), 4)

    def test_tiny_solar_surplus_cannot_block_full_cheap_grid_charge(self):
        now, slots = scenario()
        for slot in slots[2:6]:
            slot.update(net_solar_kwh=.1, solar_kwh=1.1, demand_kwh=1.)
        solar, grid = coordinator()._plan_charge_windows_for_horizon(
            slots=slots, now=now, usable_capacity_kwh=8, current_remaining_capacity_kwh=8,
            max_charge_kw=2, max_discharge_kw=2, battery_min_profit=.08,
        )
        self.assertEqual(solar, [])
        self.assertAlmostEqual(sum(w['charge_kwh'] for w in grid), 8)
        self.assertEqual(grid[0]['start'], (now+timedelta(hours=2)).isoformat())
        self.assertEqual(grid[-1]['end'], (now+timedelta(hours=6)).isoformat())
