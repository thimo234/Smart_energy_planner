"""Weighted import margin, independent of PV and shared discharge power."""
from datetime import datetime, timedelta
import unittest

from test_battery_energy_accounting import coordinator, replay_full_plan, energy_trace
from custom_components.smart_energy_planner.battery_profit import grid_supplement_profit


class AverageProfitTest(unittest.TestCase):
    def evaluate(self, prices=(.30, .20), demand=(1., 1.), solar=0., existing=0., export=.0):
        now = datetime(2026, 10, 10, 12)
        slots = [dict(start=now, end=now+timedelta(hours=1), hours=1.,
                      net_solar_kwh=solar, import_price=.15, export_price=.01)]
        for i, (price, amount) in enumerate(zip(prices, demand), 1):
            slots.append(dict(start=now+timedelta(hours=i), end=now+timedelta(hours=i+1),
                hours=1., net_solar_kwh=-amount, import_price=price, export_price=export))
        return grid_supplement_profit(selected=[dict(kind='grid', start=now, charge_kwh=2., profit_cost=.15)],
            slots=slots, after=now+timedelta(hours=1), before=slots[-1]['end'],
            max_charge_kw=2., max_discharge_kw=1., minimum_profit=.10,
            existing_energy=existing, allow_export=True)

    def test_weighted_margin_passes_even_when_one_sale_is_below_ten_cents(self):
        result = self.evaluate()
        self.assertTrue(result['profitable'])
        self.assertAlmostEqual(result['average_profit'], .10)
        self.assertFalse(self.evaluate(prices=(.30, .199))['profitable'])

    def test_pv_share_has_no_import_cost_and_cannot_subsidize_grid(self):
        result = self.evaluate(prices=(.25, .25), solar=1.)
        self.assertEqual(result['net_kwh'], 1.)
        self.assertEqual(result['import_cost'], .15)
        self.assertTrue(result['profitable'])
        self.assertFalse(self.evaluate(prices=(.24, .24), solar=1.)['profitable'])
        self.assertFalse(self.evaluate(prices=(.02, .02), solar=2.)['profitable'])

    def test_demand_and_power_are_not_double_counted(self):
        self.assertFalse(self.evaluate(existing=1.)['profitable'])
        self.assertFalse(self.evaluate(prices=(.40,), demand=(1.,), export=.40)['profitable'])
        self.assertFalse(self.evaluate(prices=(.40,), demand=(0.,), export=.24)['profitable'])

    def test_supplied_snapshot_reaches_full_and_retains_next_day_reserve(self):
        now, slots, result = replay_full_plan('2026_10_10_short_cycles',
            '2026-10-10T09:15:10.729241+02:00', soc=20, reserve=60,
            profit=.10, max_charge=2.5, snapshot_exports=True, isolated_cycles=True)
        self.assertTrue(result.planned_grid_charge_windows)
        self.assertTrue(any('T12:' in w['start'] for w in result.planned_grid_charge_windows))
        windows = result.planned_battery_mode_windows + result.estimated_battery_mode_windows
        trace = energy_trace(now, slots, windows, initial=2., max_charge=2.5)
        self.assertAlmostEqual(max(e for _, e, _ in trace), 10., delta=.01)
        self.assertGreaterEqual(min(e for _, e, _ in trace), 1.995)
        self.assertAlmostEqual(trace[-1][1], 6., delta=.01)
        imported = cost = avoided = 0.
        for window in windows:
            for slot in slots:
                hours = max(0., (min(datetime.fromisoformat(window['end']), slot['end'])
                    - max(datetime.fromisoformat(window['start']), slot['start'])).total_seconds()/3600)
                net_kw = slot['net_solar_kwh']/slot['hours']
                if window['mode'] == 'laden_van_net':
                    amount = max(0., 2.5-max(0., net_kw))*hours
                    imported += amount
                    cost += amount*slot['import_price']
                elif window['mode'] == 'ontladen':
                    avoided += min(3., max(0., -net_kw))*hours*slot['import_price']
        # Even with stock retained at 60% valued at zero, this emitted plan
        # earns the configured mean margin over the complete grid purchase.
        self.assertGreater(imported, 4.)
        self.assertGreaterEqual(avoided-cost, imported*.10)

    def test_average_profitable_fill_survives_soc_feedback(self):
        now = datetime.fromisoformat('2026-10-10T09:15:10.729241+02:00')
        c = coordinator()
        energy = peak = 2.
        saw_grid = False
        for _ in range(34):
            _, slots, result = replay_full_plan('2026_10_10_short_cycles', now.isoformat(),
                soc=energy*10, instance=c, reserve=60, profit=.10, max_charge=2.5,
                snapshot_exports=True, isolated_cycles=True)
            until = now+timedelta(minutes=15)
            commands = [{**w, 'end': min(until, datetime.fromisoformat(w['end'])).isoformat()}
                        for w in result.planned_battery_mode_windows
                        if datetime.fromisoformat(w['start']) < until]
            saw_grid |= any(w['mode'] == 'laden_van_net' for w in commands)
            trace = energy_trace(now, slots, commands, initial=energy, max_charge=2.5)
            self.assertTrue(trace)
            self.assertGreaterEqual(min(e for _, e, _ in trace), 1.995)
            self.assertLessEqual(max(e for _, e, _ in trace), 10.005)
            energy = trace[-1][1]
            peak = max(peak, *(e for _, e, _ in trace))
            now = until
        self.assertTrue(saw_grid)
        self.assertGreaterEqual(peak, 9.95)


if __name__ == '__main__':
    unittest.main()
