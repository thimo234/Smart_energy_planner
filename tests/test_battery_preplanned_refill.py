"""Select the refill before releasing reserve; never sell retained stock on paper."""
import json
from datetime import datetime, timedelta
from pathlib import Path
import unittest
from unittest.mock import patch

from test_battery_energy_accounting import coordinator, energy_trace, replay_full_plan
from custom_components.smart_energy_planner.battery_profit import grid_supplement_profit


class PreplannedRefillTest(unittest.TestCase):
    def setUp(self):
        self.data = json.loads((Path(__file__).parent / 'fixtures/battery_2026_10_22.json').read_text())

    def replay(self, data, at='2026-10-22T06:00:00+02:00', energy=9., instance=None):
        original = Path.read_text
        with patch.object(Path, 'read_text', lambda path, *args, **kwargs:
                          json.dumps(data) if path.name == 'battery_2026_10_22.json'
                          else original(path, *args, **kwargs)):
            return replay_full_plan('2026_10_22', at, soc=energy*10, discharging=True,
                reserve=60, max_charge=2.5, profit=.1, isolated_cycles=True,
                snapshot_exports=True, external_demand=data['estimated_hourly_home_demand'],
                instance=instance)

    def profitable_scenario(self):
        # Explicit synthetic variant: higher evening home prices, unchanged
        # export tariff, demand, sunshine and estimated tomorrow prices.
        data = json.loads(json.dumps(self.data))
        for window in data['upcoming_energy_price_windows']:
            if '2026-10-22T16' <= window['start'] < '2026-10-23':
                window['price'] *= 2
        return data

    def test_supplied_day_does_not_buy_stock_that_must_remain_reserved(self):
        for at in ('2026-10-22T06:00:00+02:00', '2026-10-22T11:00:23.157288+02:00'):
            with self.subTest(at=at):
                now, slots, result = self.replay(self.data, at)
                self.assertFalse(result.planned_grid_charge_windows)
                self.assertTrue(result.battery_no_charge_reserve_active)
                trace = energy_trace(now, slots, result.planned_battery_mode_windows,
                                     initial=9, max_charge=2.5)
                self.assertGreaterEqual(min(e for _, e, _ in trace), 5.995)

    def test_profitable_full_refill_is_planned_before_household_depletion(self):
        now, slots, result = self.replay(self.profitable_scenario())
        self.assertTrue(result.planned_grid_charge_windows)
        self.assertLess(result.next_charge_window_start, '2026-10-22T12:00:00+02:00')
        self.assertFalse(result.battery_no_charge_reserve_active)
        self.assertTrue(any(w['mode'].startswith('ontladen')
                            for w in result.planned_battery_mode_windows))
        trace = energy_trace(now, slots, result.planned_battery_mode_windows,
                             initial=9, max_charge=2.5)
        self.assertGreaterEqual(min(e for _, e, _ in trace), 1.995)
        self.assertLessEqual(min(e for _, e, _ in trace), 2.005)
        self.assertAlmostEqual(trace[-1][1], 10, delta=.005)
        imported = cost = saved = 0.
        for window in result.planned_battery_mode_windows + result.estimated_battery_mode_windows:
            for slot in slots:
                hours = max(0., (min(datetime.fromisoformat(window['end']), slot['end'])
                    - max(datetime.fromisoformat(window['start']), slot['start'])).total_seconds()/3600)
                net_kw = slot['net_solar_kwh'] / slot['hours']
                if window['mode'] == 'laden_van_net':
                    amount = max(0., 2.5-max(0., net_kw))*hours
                    imported += amount
                    cost += amount*slot['import_price']
                elif window['mode'] == 'ontladen' and window['start'] >= '2026-10-22T16':
                    saved += min(3., max(0., -net_kw))*hours*slot['import_price']
        # Do not include morning exports or put a value on retained stock.
        self.assertGreaterEqual(saved-cost, imported*.10)

    def test_older_full_purchase_examples_cannot_sell_the_sixty_percent_reserve(self):
        for snapshot, timestamp, soc in (
            ('2026_09_19_morning', '2026-09-19T08:00:10.907547+02:00', 44),
            ('2026_09_27_late_refill', '2026-09-27T16:54:07.006343+02:00', 100),
        ):
            with self.subTest(snapshot=snapshot):
                _, _, result = replay_full_plan(snapshot, timestamp, soc=soc,
                    discharging=True, reserve=60, max_charge=2.5, tax_deduction=.11,
                    isolated_cycles=True)
                self.assertFalse(result.planned_grid_charge_windows)
                self.assertTrue(result.battery_no_charge_reserve_active)

    def test_repeated_updates_discharge_then_fill_without_losing_window(self):
        data = self.profitable_scenario()
        c = coordinator()
        c._charge_session_started = False
        c._discharge_session_started = True
        now = datetime.fromisoformat('2026-10-22T06:00:00+02:00')
        energy, peak, saw_grid = 9., 9., False
        for _ in range(42):
            _, slots, result = self.replay(data, now.isoformat(), energy, c)
            until = now + timedelta(minutes=15)
            commands = [{**w, 'end': min(datetime.fromisoformat(w['end']), until).isoformat()}
                        for w in result.planned_battery_mode_windows
                        if datetime.fromisoformat(w['start']) < until]
            trace = energy_trace(now, slots, commands, initial=energy, max_charge=2.5)
            self.assertTrue(trace)
            self.assertGreaterEqual(min(e for _, e, _ in trace), 1.995)
            self.assertLessEqual(max(e for _, e, _ in trace), 10.005)
            saw_grid |= any(w['mode'] == 'laden_van_net' for w in commands)
            energy = trace[-1][1]
            peak = max(peak, energy)
            now = until
        self.assertTrue(saw_grid)
        self.assertGreaterEqual(peak, 9.95)

    def test_estimated_day_mean_can_support_margin_within_discharge_budget(self):
        now = datetime(2026, 10, 22, 12)
        slots = [dict(start=now, end=now+timedelta(hours=1), hours=1,
                      net_solar_kwh=0, import_price=.20, export_price=.09)]
        slots += [dict(start=now+timedelta(hours=i), end=now+timedelta(hours=i+1),
                       hours=1, net_solar_kwh=-1, import_price=price,
                       export_price=0, price_known=i == 1)
                  for i, price in enumerate((.40, .32, .32, .32), 1)]
        args = dict(selected=[dict(kind='grid', start=now, charge_kwh=3, profit_cost=.20)],
                    slots=slots, after=slots[1]['start'], before=slots[-1]['end'],
                    max_charge_kw=3, max_discharge_kw=3, minimum_profit=.10)
        self.assertTrue(grid_supplement_profit(**args, discharge_budget_kwh=3)['profitable'])
        self.assertFalse(grid_supplement_profit(**args, discharge_budget_kwh=2)['profitable'])
