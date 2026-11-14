"""Profit protection above reserve; authorized full-cycle losses below it."""
import unittest
from datetime import datetime
import test_battery_bound_cycle_policy as helpers
from test_battery_energy_accounting import energy_trace


class ReserveLossCompletionTest(unittest.TestCase):
    setUp = helpers.BoundCyclePolicyTest.setUp
    slots = helpers.BoundCyclePolicyTest.slots
    charge = helpers.BoundCyclePolicyTest.charge
    build = helpers.BoundCyclePolicyTest.build

    def prepare(self, floor=0., energy=8.):
        self.c._battery_cycle_discharge_floor_kwh = floor
        self.c._battery_grid_charge_price = .80
        self.c._battery_margin_state = dict(at=self.now.isoformat(), energy=energy,
            reserve=0., lots=[[energy, .80]], credit=0., actions=[])

    def test_loss_above_reserve_is_rejected(self):
        self.prepare(floor=4.)
        self.c._cycle_export_planning = True
        windows = self.build(self.slots(count=8, home=1., price=.10), 8.)
        self.assertFalse(any(w['mode'].startswith('ontladen') for w in windows))

    def test_committed_full_cycle_rejects_loss_above_reserve_too(self):
        self.prepare()
        self.c._cycle_export_planning = True
        windows = self.build(self.slots(count=8, home=0., price=.10), 8.)
        self.assertFalse(any(w['mode'].startswith('ontladen') for w in windows))

    def test_committed_full_cycle_can_complete_at_loss_below_reserve(self):
        self.prepare(energy=4.)
        self.c._cycle_export_planning = True
        slots = self.slots(count=8, home=0., price=.10)
        for slot in slots:
            slot['export_price'] = -.05
        windows = self.build(slots, 4.)
        trace = energy_trace(self.now, slots, windows, initial=6.)
        self.assertTrue(any(w['mode'] == 'ontladen_naar_net' for w in windows))
        self.assertAlmostEqual(min(e for _, e, _ in trace), 2., delta=.005)

    def test_loss_making_household_use_below_reserve_reaches_minimum(self):
        self.prepare(energy=3.5)
        slots = self.slots(count=8, home=1., price=.10)
        windows = self.build(slots, 3.5)
        trace = energy_trace(self.now, slots, windows, initial=5.5)
        self.assertAlmostEqual(min(e for _, e, _ in trace), 2., delta=.005)
        self.assertFalse(any(w['mode'] == 'ontladen_naar_net' for w in windows))

    def test_profitable_prefix_then_loss_completion_crosses_reserve_safely(self):
        self.prepare()
        self.c._cycle_export_planning = True
        slots = self.slots(count=8, home=1., price=.10)
        for slot in slots[:4]:
            slot['import_price'] = 1.
        windows = self.build(slots, 8.)
        trace = energy_trace(self.now, slots, windows, initial=10.)
        for at, energy, mode in trace:
            if mode.startswith('ontladen') and datetime.fromisoformat(at) > slots[4]['start']:
                self.assertLessEqual(energy, 6.005)
        self.assertAlmostEqual(min(e for _, e, _ in trace), 2., delta=.005)

    def test_profitable_crossing_slot_does_not_idle_at_reserve(self):
        self.prepare(energy=4.5)
        slots = self.slots(count=2, home=3., price=1.)
        windows = self.build(slots, 4.5)
        trace = energy_trace(self.now, slots, windows, initial=6.5)
        self.assertAlmostEqual(trace[-1][1], 2., delta=.005)
        self.assertEqual(windows[0]['mode'], 'ontladen')
        self.assertEqual(windows[0]['end'], slots[1]['start'].replace(minute=30).isoformat())

    def test_below_reserve_future_profit_cannot_fund_above_reserve_loss(self):
        self.prepare(energy=5.)
        self.c._battery_grid_charge_price = None
        slots = self.slots(count=5, home=1., price=2.)
        slots[0]['import_price'] = .10
        windows = self.build(slots, 5.)
        self.assertEqual(windows[0]['mode'], 'accu_uit')

    def test_incomplete_charge_still_cannot_discharge_at_loss(self):
        self.prepare(energy=3.)
        self.c._charge_session_started = True
        self.c._discharge_session_started = False
        windows = self.build(self.slots(count=8, home=1., price=.10), 3.)
        self.assertFalse(any(w['mode'].startswith('ontladen') for w in windows))

    def test_unknown_prices_do_not_authorize_loss_export_below_reserve(self):
        self.prepare(energy=4.)
        self.c._cycle_export_planning = True
        slots = self.slots(count=8, home=0., price=.10)
        for slot in slots:
            slot['price_known'] = False
        windows = self.build(slots, 4.)
        self.assertFalse(any(w['mode'].startswith('ontladen') for w in windows))

    def test_mixed_home_and_export_stop_exactly_at_minimum(self):
        self.prepare(energy=4.)
        self.c._cycle_export_planning = True
        slots = self.slots(count=8, home=.25, price=.10)
        windows = self.build(slots, 4.)
        trace = energy_trace(self.now, slots, windows, initial=6.)
        self.assertAlmostEqual(min(e for _, e, _ in trace), 2., delta=.005)
        self.assertTrue(any(w['mode'] == 'ontladen_naar_net' for w in windows))

    def test_a_committed_reserve_cycle_does_not_cross_its_chosen_floor(self):
        self.prepare(floor=4., energy=3.5)
        windows = self.build(self.slots(count=8, home=1., price=.10), 3.5)
        self.assertFalse(any(w['mode'].startswith('ontladen') for w in windows))

    def test_known_historical_cost_without_margin_state_is_not_free_stock(self):
        self.prepare(floor=4.)
        self.c._battery_margin_state = None
        windows = self.build(self.slots(count=8, home=1., price=.10), 8.)
        self.assertFalse(any(w['mode'].startswith('ontladen') for w in windows))
