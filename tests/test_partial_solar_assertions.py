"""Adversarial checks for the independent command oracle."""
import unittest
from datetime import datetime, timedelta, timezone
from partial_solar_assertions import assert_executable_plan


class PartialSolarAssertionsTest(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 11, 20, tzinfo=timezone.utc)
        self.slot = dict(start=self.now, end=self.now+timedelta(hours=1),
                         hours=1., net_solar_kwh=-.1,
                         import_price=.3, export_price=.8, price_known=True)
        self.window = dict(start=self.now.isoformat(), end=self.slot['end'].isoformat(),
                           mode='ontladen')

    def test_highest_remaining_cost_not_only_consumed_fifo_head(self):
        with self.assertRaises(AssertionError):
            assert_executable_plan(self, [self.slot], [self.window], initial=7., reserve=6.,
                state={'floor': 6., 'grid_hold': False, 'lots': [[4., .1], [1., .6]]})

    def test_historical_price_also_guards_known_cheaper_lots(self):
        with self.assertRaises(AssertionError):
            assert_executable_plan(self, [self.slot], [self.window], initial=7., reserve=6.,
                initial_cost=.6,
                state={'floor': 6., 'grid_hold': False, 'lots': [[5., .1]]})

    def test_mixed_export_must_pass_home_margin_independently(self):
        with self.assertRaises(AssertionError):
            assert_executable_plan(self, [self.slot], [dict(self.window, mode='ontladen_naar_net',
                end=(self.now+timedelta(minutes=10)).isoformat())],
                initial=7., reserve=6., initial_cost=.4)

    def test_idle_compatibility_is_not_verified_new_pv_profit(self):
        checked = assert_executable_plan(self, [self.slot], [], initial=7., reserve=6.)
        self.assertFalse(checked.get('verified_new_pv_profit', True))

    def test_pv_opportunity_uses_selected_weighted_basis_not_grid_lot_policy(self):
        slots = [dict(self.slot, start=self.now+timedelta(hours=i),
                      end=self.now+timedelta(hours=i+1),
                      net_solar_kwh=net, export_price=export, import_price=.35)
                 for i, (net, export) in enumerate(((1., .2), (.1, .4), (-1.1, .1)))]
        windows = [dict(start=slots[0]['start'].isoformat(), end=slots[1]['end'].isoformat(),
                        mode='laden_met_zonne_energie'),
                   dict(start=slots[2]['start'].isoformat(), end=slots[2]['end'].isoformat(),
                        mode='ontladen')]
        checked = assert_executable_plan(self, slots, windows, initial=6., reserve=6.,
                                        initial_cost=.1, require_new_pv_profit=True)
        self.assertTrue(checked['verified_new_pv_profit'])

    def test_required_new_pv_profit_rejects_no_sale(self):
        slot = dict(self.slot, net_solar_kwh=.5)
        with self.assertRaises(AssertionError):
            assert_executable_plan(self, [slot], [dict(self.window, mode='laden_met_zonne_energie')],
                initial=7., reserve=6., require_new_pv_profit=True)
