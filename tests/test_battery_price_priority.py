"""Price-based source choice and reserve release, including real commands."""
from datetime import datetime, timedelta
import unittest

from test_battery_energy_accounting import coordinator, energy_trace, replay_full_plan
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
    def test_mixed_charge_repeated_updates_respect_capacity_and_complete(self):
        c = coordinator()
        now = datetime.fromisoformat('2026-09-26T13:30:18.607268+02:00')
        energy = 2.9
        for update in range(50):
            _, slots, result = replay_full_plan(
                '2026_09_26_drifting', now.isoformat(), soc=energy*10,
                reserve=60, tax_deduction=.11, isolated_cycles=True, instance=c,
            )
            if update == 0:
                self.assertEqual(result.battery_strategy, 'laden_van_net')
            until = now + timedelta(minutes=5)
            executed = [{**w, 'end': min(datetime.fromisoformat(w['end']), until).isoformat()}
                        for w in result.planned_battery_mode_windows
                        if datetime.fromisoformat(w['start']) < until]
            for previous, following in zip(executed, executed[1:]):
                self.assertLessEqual(previous['end'], following['start'])
            trace = energy_trace(now, slots, executed, initial=energy)
            self.assertTrue(trace)
            self.assertGreaterEqual(min(e for _, e, _ in trace), 2-1e-5)
            # Forecast/window attributes round kWh; allow the same 5 Wh
            # accounting tolerance as the other feedback replays.
            self.assertLessEqual(max(e for _, e, _ in trace), 10+.005)
            energy = trace[-1][1]
            now = until
            if energy >= 9.95:
                break
        self.assertGreaterEqual(energy, 9.95)

    def mixed_plan(self, later_solar_cost=.182, peak=.5):
        now = datetime(2026, 9, 26, 14)
        slots = []
        for i in range(6):
            price = .144 if i == 0 else later_solar_cost + .11 if i == 1 else peak
            surplus = 2.0 if i == 0 else 1.0 if i == 1 else -1.0
            slots.append(dict(start=now+timedelta(hours=i), end=now+timedelta(hours=i+1),
                              hours=1., import_price=price, export_price=price-.11,
                              price_known=True, net_solar_kwh=surplus,
                              solar_kwh=max(0, surplus+1), demand_kwh=1.))
        c = coordinator()
        c._cycle_export_planning = True
        solar, grid = c._plan_charge_windows_for_horizon(
            slots=slots, now=now, usable_capacity_kwh=3, current_remaining_capacity_kwh=3,
            max_charge_kw=3, max_discharge_kw=3, battery_min_profit=.08,
        )
        return now, solar, grid

    def test_grid_only_tops_up_solar_in_cheaper_early_slot(self):
        now, solar, grid = self.mixed_plan()
        # 2 kWh sun at 3.4 ct plus 1 kWh grid at 14.4 ct costs 21.2 ct.
        # Waiting for another solar kWh at 18.2 ct would cost 25 ct.
        self.assertEqual(solar, [])
        self.assertEqual(grid[0]['start'], now.isoformat())
        self.assertAlmostEqual(sum(w['charge_kwh'] for w in grid), 3.)
        self.assertEqual(grid[0]['price'], .144)
        self.assertEqual(grid[-1]['end'], (now+timedelta(hours=1)).isoformat())

    def test_cheaper_later_solar_still_beats_grid_topup(self):
        _, solar, grid = self.mixed_plan(later_solar_cost=.08)
        self.assertEqual(grid, [])
        self.assertAlmostEqual(sum(w['charge_kwh'] for w in solar), 3.)

    def test_solar_discount_cannot_hide_unprofitable_grid_topup(self):
        _, solar, grid = self.mixed_plan(later_solar_cost=.08, peak=.20)
        # Grid margin is only 5.6 ct, even though the blended price is lower.
        self.assertEqual(grid, [])
        self.assertTrue(solar)

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
