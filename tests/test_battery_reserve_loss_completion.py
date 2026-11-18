"""Strict household profit; authorized loss export only below reserve."""
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

    def test_loss_above_reserve_cannot_serve_home_or_export(self):
        self.prepare(floor=4.)
        self.c._cycle_export_planning = True
        slots = self.slots(count=8, home=1., price=.10)
        windows = self.build(slots, 8.)
        trace = energy_trace(self.now, slots, windows, initial=10.)
        self.assertAlmostEqual(trace[-1][1], 10., delta=.005)
        self.assertFalse(any(w['mode'].startswith('ontladen') for w in windows))

    def test_committed_full_cycle_rejects_loss_above_reserve_too(self):
        self.prepare()
        self.c._cycle_export_planning = True
        windows = self.build(self.slots(count=8, home=0., price=.10), 8.)
        self.assertFalse(any(w['mode'].startswith('ontladen') for w in windows))

    def test_positive_household_margin_below_minimum_is_rejected_for_known_costs(self):
        for energy in (8., 3.5):
            for cost_source in ('lots', 'historical'):
                with self.subTest(energy=energy, cost_source=cost_source):
                    self.setUp()
                    self.prepare(energy=energy)
                    if cost_source == 'lots':
                        self.c._battery_grid_charge_price = None
                    else:
                        self.c._battery_margin_state = None
                    # .05 gain covers acquisition but not the .10 minimum.
                    slots = self.slots(count=8, home=1., price=.85)
                    windows = self.build(slots, energy)
                    self.assertFalse(any(w['mode'].startswith('ontladen') for w in windows))
                    trace = energy_trace(self.now, slots, windows, initial=energy+2.)
                    self.assertAlmostEqual(min(e for _, e, _ in trace), energy+2., delta=.005)

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

    def test_loss_making_household_use_below_reserve_preserves_stock(self):
        self.prepare(energy=3.5)
        slots = self.slots(count=8, home=1., price=.10)
        windows = self.build(slots, 3.5)
        trace = energy_trace(self.now, slots, windows, initial=5.5)
        self.assertAlmostEqual(min(e for _, e, _ in trace), 5.5, delta=.005)
        self.assertFalse(any(w['mode'] == 'ontladen' for w in windows))
        self.assertFalse(any(w['mode'] == 'ontladen_naar_net' for w in windows))

    def test_profitable_prefix_cannot_authorize_later_loss_making_household_use(self):
        self.prepare()
        self.c._cycle_export_planning = True
        slots = self.slots(count=8, home=1., price=.10)
        for slot in slots[:4]:
            slot['import_price'] = 1.
        windows = self.build(slots, 8.)
        trace = energy_trace(self.now, slots, windows, initial=10.)
        self.assertTrue(any(w['mode'] == 'ontladen' for w in windows))
        self.assertFalse(any(w['mode'].startswith('ontladen')
                             and datetime.fromisoformat(w['end']) > slots[4]['start']
                             for w in windows))
        self.assertAlmostEqual(min(e for _, e, _ in trace), 6., delta=.005)

    def test_profitable_crossing_slot_does_not_idle_at_reserve(self):
        self.prepare(energy=4.5)
        slots = self.slots(count=2, home=3., price=1.)
        windows = self.build(slots, 4.5)
        trace = energy_trace(self.now, slots, windows, initial=6.5)
        self.assertAlmostEqual(trace[-1][1], 2., delta=.005)
        self.assertEqual(windows[0]['mode'], 'ontladen')
        self.assertEqual(windows[0]['end'], slots[1]['start'].replace(minute=30).isoformat())

    def test_known_lot_cost_blocks_loss_despite_future_profitable_home(self):
        self.prepare(energy=5.)
        self.c._battery_grid_charge_price = None
        slots = self.slots(count=5, home=1., price=2.)
        slots[0]['import_price'] = .10
        windows = self.build(slots, 5.)
        self.assertEqual(windows[0]['mode'], 'accu_uit')
        self.assertFalse(any(w['mode'].startswith('ontladen')
                             and datetime.fromisoformat(w['start']) < slots[1]['start']
                             for w in windows))
        self.assertTrue(any(w['mode'] == 'ontladen' for w in windows))
        self.assertEqual(self.c._battery_margin_state['credit'], 0.)

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
        # Household use is profitable; the .01 export still loses.
        slots = self.slots(count=8, home=.25, price=1.)
        windows = self.build(slots, 4.)
        trace = energy_trace(self.now, slots, windows, initial=6.)
        self.assertAlmostEqual(min(e for _, e, _ in trace), 2., delta=.005)
        self.assertTrue(any(w['mode'] == 'ontladen_naar_net' for w in windows))

    def test_mixed_loss_export_cannot_serve_unprofitable_home_below_reserve(self):
        self.prepare(energy=4.)
        self.c._cycle_export_planning = True
        slots = self.slots(count=8, home=.25, price=.10)
        windows = self.build(slots, 4.)
        self.assertFalse(any(w['mode'].startswith('ontladen') for w in windows))
        trace = energy_trace(self.now, slots, windows, initial=6.)
        self.assertAlmostEqual(min(e for _, e, _ in trace), 6., delta=.005)

    def test_a_committed_reserve_cycle_does_not_cross_its_chosen_floor(self):
        self.prepare(floor=4., energy=3.5)
        windows = self.build(self.slots(count=8, home=1., price=.10), 3.5)
        self.assertFalse(any(w['mode'].startswith('ontladen') for w in windows))

    def test_known_historical_cost_without_margin_state_blocks_loss_fallback(self):
        self.prepare(floor=4.)
        self.c._battery_margin_state = None
        windows = self.build(self.slots(count=8, home=1., price=.10), 8.)
        self.assertFalse(any(w['mode'].startswith('ontladen') for w in windows))
        trace = energy_trace(self.now, self.slots(count=8, home=1., price=.10),
                             windows, initial=10.)
        self.assertAlmostEqual(min(e for _, e, _ in trace), 10., delta=.005)
        self.assertEqual(self.c._battery_margin_state['lots'], [[8., .80]])
        self.assertEqual(self.c._battery_margin_state['credit'], 0.)
        self.assertEqual(self.c._battery_grid_charge_price, .80)
