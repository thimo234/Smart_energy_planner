"""Weekly habits and price shifts must coexist without changing elapsed demand."""
from datetime import datetime, timedelta
import unittest
from test_support import install_package_stub
install_package_stub()
from custom_components.smart_energy_planner.battery_forecast import (
    align_price_responsive_demand_to_cheap_hours, build_expected_hourly_demand_table,
    update_expected_hourly_demand_stats,
)
from custom_components.smart_energy_planner.price_models import PlannerWindow


class DemandRoutinesTest(unittest.TestCase):
    def scenario(self):
        day = datetime.fromisoformat('2026-10-20T00:00:00+02:00')
        demand, prices = [], []
        for hour in range(24):
            start = day + timedelta(hours=hour)
            demand.append(dict(start=start.isoformat(), end=(start+timedelta(hours=1)).isoformat(),
                               estimated_kwh=3.0 if hour == 12 else 0.5,
                               routine_kwh=2.5 if hour == 12 else 0.0))
            prices.append(PlannerWindow(start, start+timedelta(hours=1),
                                        .05 if hour == 3 else .10 if hour == 15 else .4))
        return day, demand, prices

    def test_routine_preserved_and_flexible_energy_conserved(self):
        day, demand, prices = self.scenario()
        demand[20]['estimated_kwh'] = 2
        result = align_price_responsive_demand_to_cheap_hours(demand, prices, now=day)
        self.assertGreaterEqual(result[12]['estimated_kwh'], 2.5)
        self.assertEqual(result[3], demand[3])
        self.assertGreater(result[15]['estimated_kwh'], demand[15]['estimated_kwh'])
        self.assertAlmostEqual(sum(s['estimated_kwh'] for s in result),
                               sum(s['estimated_kwh'] for s in demand), places=3)
        self.assertEqual(demand[12]['estimated_kwh'], 3)

    def test_past_and_current_hour_unchanged(self):
        day, demand, prices = self.scenario()
        result = align_price_responsive_demand_to_cheap_hours(
            demand, prices, now=day+timedelta(hours=12, minutes=30))
        self.assertEqual(result[:13], demand[:13])

    def test_flat_prices_do_not_move_even_a_peak(self):
        _, demand, prices = self.scenario()
        prices = [PlannerWindow(p.start, p.end, .2) for p in prices]
        self.assertEqual(align_price_responsive_demand_to_cheap_hours(demand, prices), demand)

    def test_missing_or_estimated_cheap_hour_is_not_a_target(self):
        _, demand, prices = self.scenario()
        for replacement in (PlannerWindow(prices[15].start, prices[15].end, .1, False),
                            PlannerWindow(prices[15].start, prices[15].start+timedelta(minutes=15), .1)):
            modified = prices[:15]+[replacement]+prices[16:]
            result = align_price_responsive_demand_to_cheap_hours(demand, modified)
            self.assertEqual(result, demand)

    def test_repeated_weekly_peak_outweighs_workday_average(self):
        stats = {}
        for week in range(4):
            for weekday in range(5):
                stats = update_expected_hourly_demand_stats(stats, slot_key=str(weekday*24+12),
                                                           measured_kwh=2 if weekday == 1 else .4)
        table = build_expected_hourly_demand_table(stats, daily_average_kwh=12)
        self.assertGreater(table['36'], 1.5)
        self.assertLess(table['12'], 1)
        self.assertEqual(len(stats['36']['recent_kwh']), 4)

    def test_one_off_peak_is_not_a_confirmed_routine(self):
        stats = {'daytype:weekday:12': {'count': 20, 'mean_kwh': .4}}
        stats = update_expected_hourly_demand_stats(stats, slot_key='36', measured_kwh=3)
        table = build_expected_hourly_demand_table(stats, daily_average_kwh=12)
        self.assertLess(table['36'], 2)


if __name__ == '__main__':
    unittest.main()
