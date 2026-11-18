"""Household discharge must respect the configured acquisition margin."""
import unittest

import test_battery_bound_cycle_policy as bound_policy
from test_battery_energy_accounting import energy_trace


class HouseholdProfitTest(unittest.TestCase):
    def fixture(self, *, cost=.232, credit=0., lots=None):
        h = bound_policy.BoundCyclePolicyTest()
        h.setUp()
        h.c._battery_cycle_discharge_floor_kwh = 4.
        h.c._battery_grid_charge_price = cost
        h.c._battery_margin_state = dict(at=h.now.isoformat(), energy=8.,
            reserve=0., lots=lots if lots is not None else [[8., cost]],
            credit=credit, pending_actions=[], actions=[])
        return h

    def assert_no_discharge(self, h, slots):
        windows = h.build(slots, 8.)
        self.assertFalse(any(w['mode'].startswith('ontladen') for w in windows), windows)
        self.assertAlmostEqual(energy_trace(h.now, slots, windows, initial=10.)[-1][1], 10.)
        return windows

    def test_morning_reserve_discharge_waits_for_minimum_profit(self):
        h = self.fixture()
        self.assert_no_discharge(h, h.slots(count=4, home=1., price=.311))

    def test_exact_minimum_profit_is_allowed(self):
        h = self.fixture()
        windows = h.build(h.slots(count=4, home=1., price=.332), 8.)
        self.assertTrue(any(w['mode'] == 'ontladen' for w in windows), windows)
        self.assertLess(energy_trace(h.now, h.slots(count=4, home=1., price=.332),
            windows, initial=10.)[-1][1], 10.)

    def test_just_below_minimum_is_blocked(self):
        h = self.fixture()
        self.assert_no_discharge(h, h.slots(count=4, home=1., price=.33199))

    def test_realized_credit_does_not_waive_the_slot_floor(self):
        h = self.fixture(credit=100.)
        self.assert_no_discharge(h, h.slots(count=4, home=1., price=.311))

    def test_expensive_remaining_lot_cannot_be_hidden_by_historical_anchor(self):
        h = self.fixture(cost=.10, lots=[[7., .10], [1., .50]])
        self.assert_no_discharge(h, h.slots(count=4, home=1., price=.40))

    def test_profitable_evening_cannot_subsidize_morning(self):
        h = self.fixture()
        slots = h.slots(count=8, home=1., price=.311)
        for slot in slots[4:]:
            slot['import_price'] = .60
        windows = h.build(slots, 8.)
        morning_end = slots[4]['start'].isoformat()
        self.assertFalse(any(w['mode'].startswith('ontladen') and
            w['start'] < morning_end for w in windows), windows)
        self.assertTrue(any(w['mode'] == 'ontladen' and
            w['start'] >= morning_end for w in windows), windows)

    def test_export_cannot_disguise_unprofitable_household_leg(self):
        h = self.fixture()
        slots = h.slots(count=4, home=1., price=.311)
        for slot in slots:
            slot['export_price'] = .80
        self.assert_no_discharge(h, slots)

    def test_estimated_profitable_household_forecast_remains_eligible(self):
        h = self.fixture()
        slots = h.slots(count=4, home=1., price=.40)
        for slot in slots:
            slot['price_known'] = False
        windows = h.build(slots, 8.)
        self.assertTrue(any(w['mode'] == 'ontladen' for w in windows), windows)

    def test_legacy_historical_anchor_alone_protects_existing_stock(self):
        h = self.fixture()
        h.c._battery_margin_state = None
        self.assert_no_discharge(h, h.slots(count=4, home=1., price=.311))
