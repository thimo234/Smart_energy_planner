"""Ledger migration must not erase acquisition debt when enabling full cycles."""
from datetime import datetime, timezone
import unittest

from test_battery_energy_accounting import coordinator  # HA/module harness
from custom_components.smart_energy_planner.battery_margin import observed_stock


class FullCycleStockMigrationTest(unittest.TestCase):
    def test_legacy_reserved_stock_retains_purchased_lots_and_realized_debt(self):
        now = datetime(2026, 11, 14, tzinfo=timezone.utc)
        previous = dict(at=now.isoformat(), energy=8., reserve=4.,
                        lots=[[2., .60], [2., .40]], credit=-.25, actions=[])
        stock = observed_stock(previous, now, 8., 0., .10)
        self.assertEqual(stock.credit, -.25)
        self.assertIn([2., .60], stock.lots)
        self.assertIn([2., .40], stock.lots)
        self.assertAlmostEqual(sum(q for q, _ in stock.lots), 8.)
        self.assertLess(stock.available(.30, .10), .001,
                        'migration may not turn costly stored energy into free stock')
        self.assertEqual(previous['lots'], [[2., .60], [2., .40]])


if __name__ == '__main__':
    unittest.main()
