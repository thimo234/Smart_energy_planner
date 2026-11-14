"""Explicitly approved loss-taking only for a committed reserve cycle."""
import unittest
import test_battery_bound_cycle_policy as helpers
from test_battery_energy_accounting import energy_trace


class ReserveLossCompletionTest(unittest.TestCase):
    setUp = helpers.BoundCyclePolicyTest.setUp
    slots = helpers.BoundCyclePolicyTest.slots
    charge = helpers.BoundCyclePolicyTest.charge
    build = helpers.BoundCyclePolicyTest.build

    def prepare(self):
        self.c._battery_cycle_discharge_floor_kwh = 4.
        self.c._battery_grid_charge_price = .80
        self.c._battery_margin_state = dict(at=self.now.isoformat(), energy=8.,
            reserve=0., lots=[[8., .80]], credit=0., actions=[])

    def test_loss_making_household_use_reaches_reserve(self):
        self.prepare()
        slots = self.slots(count=8, home=1., price=.10)
        windows = self.build(slots, 8.)
        trace = energy_trace(self.now, slots, windows, initial=10.)
        self.assertAlmostEqual(min(e for _, e, _ in trace), 6., delta=.005)
        self.assertFalse(any(w['mode'] == 'ontladen_naar_net' for w in windows))

    def test_loss_making_export_reaches_reserve_without_home_demand(self):
        self.prepare()
        self.c._cycle_export_planning = True
        slots = self.slots(count=8, home=0., price=.10)
        for slot in slots:
            slot['export_price'] = -.05
        windows = self.build(slots, 8.)
        trace = energy_trace(self.now, slots, windows, initial=10.)
        self.assertTrue(any(w['mode'] == 'ontladen_naar_net' for w in windows))
        self.assertAlmostEqual(min(e for _, e, _ in trace), 6., delta=.005)

    def test_full_cycle_still_rejects_loss_making_export(self):
        self.prepare()
        self.c._battery_cycle_discharge_floor_kwh = 0.
        self.c._cycle_export_planning = True
        slots = self.slots(count=8, home=0., price=.10)
        windows = self.build(slots, 8.)
        self.assertFalse(any(w['mode'].startswith('ontladen') for w in windows))

    def test_incomplete_charge_still_cannot_discharge_at_loss(self):
        self.prepare()
        self.c._charge_session_started = True
        self.c._discharge_session_started = False
        slots = self.slots(count=8, home=1., price=.10)
        windows = self.build(slots, 6.)
        self.assertFalse(any(w['mode'].startswith('ontladen') for w in windows))

    def test_unknown_prices_do_not_authorize_loss_export(self):
        self.prepare()
        self.c._cycle_export_planning = True
        slots = self.slots(count=8, home=0., price=.10)
        for slot in slots:
            slot['price_known'] = False
        windows = self.build(slots, 8.)
        self.assertFalse(any(w['mode'].startswith('ontladen') for w in windows))

    def test_mixed_home_and_export_stop_exactly_at_reserve(self):
        self.prepare()
        self.c._cycle_export_planning = True
        slots = self.slots(count=8, home=.25, price=.10)
        windows = self.build(slots, 8.)
        trace = energy_trace(self.now, slots, windows, initial=10.)
        self.assertAlmostEqual(min(e for _, e, _ in trace), 6., delta=.005)
        self.assertTrue(any(w['mode'] == 'ontladen_naar_net' for w in windows))

    def test_no_loss_discharge_below_reserve(self):
        self.prepare()
        slots = self.slots(count=8, home=1., price=.10)
        windows = self.build(slots, 3.5)
        self.assertFalse(any(w['mode'].startswith('ontladen') for w in windows))
