"""Physical slot integration preserves intratariff forecast changes."""
from datetime import datetime, timedelta
from types import SimpleNamespace
import unittest
from test_battery_energy_accounting import coordinator  # bootstrap HA stubs
from custom_components.smart_energy_planner.battery_forecast import build_energy_balance_slots


class ForecastIntervalBoundsTest(unittest.TestCase):
    def test_demand_boundaries_are_not_averaged_into_tariff(self):
        now = datetime(2026, 11, 13, 12)
        prices = [SimpleNamespace(start=now, end=now+timedelta(hours=1), price=.4, price_known=True)]
        demand = [dict(start=now.isoformat(), end=(now+timedelta(minutes=30)).isoformat(), estimated_kwh=.1),
                  dict(start=(now+timedelta(minutes=30)).isoformat(), end=prices[0].end.isoformat(), estimated_kwh=1.)]
        slots = build_energy_balance_slots(price_windows=prices, export_price_windows=prices,
            solar_windows=[], hourly_demand=demand, horizon_start=now, demand_safety_margin=0.)
        self.assertEqual(len(slots), 2)
        self.assertEqual([s['hours'] for s in slots], [.5, .5])
        self.assertEqual([s['demand_kwh'] for s in slots], [.1, 1.])
        self.assertAlmostEqual(sum(s['net_solar_kwh'] for s in slots), -1.1)
