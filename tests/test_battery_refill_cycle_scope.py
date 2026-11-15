"""A completed fill ends the old cycle's permission to use the lower reserve."""
from datetime import datetime, timedelta
import unittest
import test_battery_bound_cycle_policy as policy
from test_battery_energy_accounting import energy_trace


class RefillCycleScopeTest(unittest.TestCase):
    setUp = policy.BoundCyclePolicyTest.setUp
    slots = policy.BoundCyclePolicyTest.slots
    charge = policy.BoundCyclePolicyTest.charge
    build = policy.BoundCyclePolicyTest.build

    def scenario(self, next_refill=False):
        slots = self.slots(count=24 if next_refill else 16, home=2.)
        charges = [self.charge(4, 3.2, 8.)]
        for s in slots[4:8]:
            s.update(import_price=.10, demand_kwh=0., net_solar_kwh=0.)
        if next_refill:
            charges.append(self.charge(12, 3.2, 8.))
            for s in slots[12:16]:
                s.update(import_price=.10, demand_kwh=0., net_solar_kwh=0.)
        return slots, charges

    def test_completed_refill_does_not_authorize_its_own_new_full_discharge(self):
        self.c._battery_cycle_discharge_floor_kwh = 0.
        slots, charges = self.scenario()
        windows = self.build(slots, 8., charges)
        trace = energy_trace(self.now, slots, windows, initial=10., max_charge=2.5)
        after = [e for t, e, _ in trace if datetime.fromisoformat(t) >= self.now+timedelta(hours=8)]
        self.assertAlmostEqual(min(after), 6., delta=.005)
        self.assertEqual(self.c._battery_cycle_discharge_floor_kwh, 0.)

    def test_true_next_full_refill_allows_full_discharge_of_new_cycle(self):
        self.c._battery_cycle_discharge_floor_kwh = 0.
        slots, charges = self.scenario(next_refill=True)
        windows = self.build(slots, 8., charges)
        trace = energy_trace(self.now, slots, windows, initial=10., max_charge=2.5)
        between = [e for t, e, _ in trace
                   if self.now+timedelta(hours=8) <= datetime.fromisoformat(t) <= self.now+timedelta(hours=12)]
        self.assertAlmostEqual(min(between), 2., delta=.005)
        after = [e for t, e, _ in trace if datetime.fromisoformat(t) >= self.now+timedelta(hours=16)]
        self.assertAlmostEqual(min(after), 6., delta=.005)

    def test_outlook_carry_retains_permission_with_a_true_next_full_refill(self):
        self.c._battery_cycle_discharge_floor_kwh = 0.
        slots, charges = self.scenario(next_refill=True)
        actual = self.build(slots, 8., charges)
        for hour, initial in ((8, 10.), (10, 6.)):
            with self.subTest(hour=hour):
                estimated = self.c._estimate_unknown_price_plan(
                    slots=slots, now=self.now, actual_windows=actual,
                    initial_energy=8., capacity=8., max_charge=2.5,
                    max_discharge=3., reserve=4., min_profit=.10,
                    include_known_prices=True, preview_start=self.now+timedelta(hours=hour))
                trace = energy_trace(self.now+timedelta(hours=hour), slots, estimated,
                                     initial=initial, max_charge=2.5)
                before_refill = [e for t, e, _ in trace
                                 if datetime.fromisoformat(t) <= self.now+timedelta(hours=12)]
                self.assertAlmostEqual(min(before_refill), 2., delta=.005)

    def test_measured_fill_completion_releases_old_floor(self):
        for initial in (6., 8.):
            with self.subTest(initial=initial):
                self.c._battery_cycle_discharge_floor_kwh = 0.
                self.c._charge_session_started = True
                self.c._discharge_session_started = False
                slots = self.slots(count=12, home=2.)
                charges = [self.charge(0, .8, 2.)] if initial < 8 else []
                if charges:
                    slots[0].update(import_price=.10, demand_kwh=0., net_solar_kwh=0.)
                windows = self.build(slots, initial, charges)
                trace = energy_trace(self.now, slots, windows, initial=initial+2., max_charge=2.5)
                self.assertAlmostEqual(min(e for _, e, _ in trace), 6., delta=.005)

    def test_outlook_carry_resets_old_floor_on_completed_refill(self):
        self.c._battery_cycle_discharge_floor_kwh = 0.
        slots, charges = self.scenario()
        actual = self.build(slots, 8., charges)
        for hour in (6, 8):
            with self.subTest(hour=hour):
                estimated = self.c._estimate_unknown_price_plan(
                    slots=slots, now=self.now, actual_windows=actual,
                    initial_energy=8., capacity=8., max_charge=2.5,
                    max_discharge=3., reserve=4., min_profit=.10,
                    include_known_prices=True, preview_start=self.now+timedelta(hours=hour))
                initial = 5. if hour == 6 else 8.
                trace = energy_trace(self.now+timedelta(hours=hour), slots, estimated,
                                     initial=initial+2., max_charge=2.5)
                self.assertAlmostEqual(min(e for _, e, _ in trace), 6., delta=.005)
