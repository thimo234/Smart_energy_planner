"""Derived export tariffs stay consistent with solar opportunity cost."""
from datetime import datetime, timedelta
import unittest

from test_battery_energy_accounting import coordinator
from test_battery_price_priority import scenario
from custom_components.smart_energy_planner.price_helpers import deduct_export_prices, export_price_deduction
from custom_components.smart_energy_planner.price_models import PlannerWindow


class ExportPriceDeductionTest(unittest.TestCase):
    def test_empty_same_and_distinct_sensor(self):
        config = {'price_sensor': 'sensor.nordpool', 'export_price_tax_deduction': .1234}
        self.assertEqual(export_price_deduction(config), .1234)
        self.assertEqual(export_price_deduction({**config, 'export_price_sensor': 'sensor.nordpool'}), .1234)
        self.assertEqual(export_price_deduction({**config, 'export_price_sensor': 'sensor.export'}), 0)
        self.assertEqual(export_price_deduction({}), .11)
        self.assertEqual(export_price_deduction({**config, 'export_price_tax_deduction': 0}), 0)

    def test_all_prices_adjusted_once_without_mutating_import(self):
        now = datetime(2026, 9, 25, 12)
        raw = [PlannerWindow(now, now+timedelta(hours=1), .30),
               PlannerWindow(now+timedelta(hours=1), now+timedelta(hours=2), .05, False)]
        current, average, future, history = deduct_export_prices(.30, .25, raw, raw, .11)
        self.assertAlmostEqual(current, .19)
        self.assertAlmostEqual(average, .14)
        self.assertAlmostEqual(future[1].price, -.06)
        self.assertEqual(future, history)
        self.assertFalse(future[1].price_known)
        self.assertEqual(raw[0].price, .30)
        self.assertEqual(raw[1].price, .05)
        self.assertEqual(deduct_export_prices(None, None, [], [], .11), (None, None, [], []))

    def test_configured_export_value_changes_solar_choice(self):
        now, slots = scenario(grid_price=.30, solar_price=.40)
        for deduction, expected_source in ((0, 'grid'), (.15, 'solar')):
            for slot in slots:
                slot['export_price'] = slot['import_price'] - deduction
            solar, grid = coordinator()._plan_charge_windows_for_horizon(
                slots=slots, now=now, usable_capacity_kwh=8, current_remaining_capacity_kwh=8,
                max_charge_kw=2, max_discharge_kw=2, battery_min_profit=.08,
            )
            self.assertTrue(grid if expected_source == 'grid' else solar)
            self.assertFalse(solar if expected_source == 'grid' else grid)
            if solar:
                self.assertAlmostEqual(solar[0]['price'], .25)  # No second deduction.
