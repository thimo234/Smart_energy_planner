"""Fixed cycle boundaries: full cycles first, reserve cycles as fallback."""
import unittest
from datetime import datetime, timedelta, timezone

from test_battery_planner import SmartEnergyPlannerCoordinator
from test_battery_energy_accounting import energy_trace


class BoundCyclePolicyTest(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 11, 14, 0, tzinfo=timezone.utc)
        self.c = SmartEnergyPlannerCoordinator.__new__(SmartEnergyPlannerCoordinator)
        self.c._active_charge_phase_end = None
        self.c._active_charge_phase_mode = 'accu_uit'
        self.c._charge_session_started = False
        self.c._discharge_session_started = True
        self.c._battery_cycle_state_initialized = True

    def slots(self, count=12, home=.5, price=.60):
        return [dict(start=self.now+timedelta(hours=i), end=self.now+timedelta(hours=i+1),
                     hours=1., import_price=price, export_price=.01, price_known=True,
                     solar_kwh=0., demand_kwh=home, net_solar_kwh=-home)
                for i in range(count)]

    def charge(self, hour, duration, energy):
        return dict(start=(self.now+timedelta(hours=hour)).isoformat(),
                    end=(self.now+timedelta(hours=hour+duration)).isoformat(),
                    charge_kwh=energy, usable_hours=duration, price=.10)

    def build(self, slots, initial, charges=()):
        return self.c._build_mode_windows_from_hourly_plan(
            slots=slots, now=self.now, planned_solar_charge_windows=[],
            planned_grid_charge_windows=list(charges), initial_usable_energy_kwh=initial,
            usable_capacity_kwh=8., no_charge_reserve_kwh=4., battery_min_profit=.10,
            average_price=.35, average_export_price=.01, max_charge_kw=2.5,
            max_discharge_kw=3.)[0]

    def test_partial_observed_stock_is_not_a_completed_full_cycle(self):
        windows = self.build(self.slots(home=1.), 3.5)
        trace = energy_trace(self.now, self.slots(home=1.), windows, initial=5.5)
        self.assertEqual(self.c._battery_cycle_discharge_floor_kwh, 4.)
        self.assertAlmostEqual(min(e for _, e, _ in trace), 5.5, delta=.005)

    def test_reserve_cycle_cannot_reverse_above_its_fixed_floor(self):
        self.c._battery_cycle_discharge_floor_kwh = 4.
        slots = self.slots(home=.25)
        slots[1]['import_price'] = .10
        windows = self.build(slots, 6., [self.charge(1, 1, 2.5)])
        self.assertFalse(any(w['mode'].startswith('laden') for w in windows), windows)
        trace = energy_trace(self.now, slots, windows, initial=8., max_charge=2.5)
        self.assertAlmostEqual(min(e for _, e, _ in trace), 6., delta=.005)

    def test_reserve_reached_allows_a_complete_refill(self):
        self.c._battery_cycle_discharge_floor_kwh = 4.
        slots = self.slots(home=1.)
        for i in (4, 5):
            slots[i]['import_price'] = .10
        windows = self.build(slots, 6., [self.charge(4, 2, 5.)])
        charges = [w for w in windows if w['mode'].startswith('laden')]
        self.assertTrue(charges)
        trace = energy_trace(self.now, slots, windows, initial=8., max_charge=2.5)
        self.assertAlmostEqual(max(e for _, e, _ in trace), 10., delta=.005)
        self.assertGreaterEqual(min(e for _, e, _ in trace), 6.-.005)

    def test_opportunistic_solar_after_reserve_cannot_discharge_an_incomplete_refill(self):
        self.c._battery_cycle_discharge_floor_kwh = 4.
        slots = self.slots(count=6, home=2.)
        slots[2].update(solar_kwh=1., demand_kwh=0., net_solar_kwh=1.)
        windows = self.build(slots, 8.)
        from bound_cycle_assertions import assert_bound_plan
        assert_bound_plan(self, self.now, slots, windows, initial=10., floor=6., charging=False)

    def test_partial_charge_waits_until_full_before_discharging(self):
        self.c._charge_session_started = True
        self.c._discharge_session_started = False
        self.c._battery_cycle_discharge_floor_kwh = 4.
        slots = self.slots(home=1.)
        windows = self.build(slots, 4.5, [self.charge(0, .25, .625), self.charge(4, 1.15, 2.875)])
        before_full = [w for w in windows if datetime.fromisoformat(w['start']) < self.now+timedelta(hours=5.15)]
        self.assertFalse(any(w['mode'].startswith('ontladen') for w in before_full), windows)
        self.assertTrue(any(w['mode'].startswith('ontladen') for w in windows))

    def test_full_cycle_is_preferred_when_profitable_outlets_are_sufficient(self):
        windows = self.build(self.slots(home=1.), 8.)
        trace = energy_trace(self.now, self.slots(home=1.), windows, initial=10., max_charge=2.5)
        self.assertAlmostEqual(min(e for _, e, _ in trace), 2., delta=.005)
        self.assertEqual(self.c._battery_cycle_discharge_floor_kwh, 0.)

    def test_new_cycle_uses_reserve_when_full_discharge_is_not_feasible(self):
        slots = self.slots(count=6, home=1.)
        windows = self.build(slots, 8.)
        trace = energy_trace(self.now, slots, windows, initial=10., max_charge=2.5)
        self.assertAlmostEqual(min(e for _, e, _ in trace), 6., delta=.005)
        self.assertEqual(self.c._battery_cycle_discharge_floor_kwh, 4.)

    def test_committed_reserve_does_not_become_full_on_a_new_forecast(self):
        self.c._battery_cycle_discharge_floor_kwh = 4.
        slots = self.slots(home=1.)
        windows = self.build(slots, 7.)
        trace = energy_trace(self.now, slots, windows, initial=9., max_charge=2.5)
        self.assertAlmostEqual(min(e for _, e, _ in trace), 6., delta=.005)
        self.assertEqual(self.c._battery_cycle_discharge_floor_kwh, 4.)

    def test_near_minimum_without_profitable_outlet_does_not_complete_discharge(self):
        self.c._charge_session_started = False
        self.c._discharge_session_started = True
        self.c._battery_cycle_state_initialized = True
        self.c._battery_cycle_discharge_floor_kwh = 0.
        slots = self.slots(count=4, home=0.)
        self.build(slots, .037)
        self.assertTrue(self.c._discharge_session_started)
        self.assertFalse(self.c._charge_session_started)

    def test_near_full_is_not_maximum_and_cannot_discharge(self):
        self.c._charge_session_started = True
        self.c._discharge_session_started = False
        self.c._battery_cycle_discharge_floor_kwh = 0.
        slots = self.slots(count=4, home=1.)
        windows = self.build(slots, 7.97)
        self.assertFalse(any(w['mode'].startswith('ontladen') for w in windows))
        self.assertTrue(self.c._charge_session_started)

    def test_committed_full_cycle_does_not_become_reserve_on_a_new_forecast(self):
        self.c._battery_cycle_discharge_floor_kwh = 0.
        slots = self.slots(count=6, home=1.)
        windows = self.build(slots, 7.)
        trace = energy_trace(self.now, slots, windows, initial=9., max_charge=2.5)
        self.assertAlmostEqual(min(e for _, e, _ in trace), 3., delta=.005)
        self.assertEqual(self.c._battery_cycle_discharge_floor_kwh, 0.)


if __name__ == '__main__':
    unittest.main()
