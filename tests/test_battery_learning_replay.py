"""Deterministic controls for the offline learning experiment, not field data."""
import json
import unittest
from datetime import datetime, timedelta, timezone
from typing import Any

from scripts import replay_battery_learning as replay


def sample(day, *, morning: float = 2, afternoon: float = .5) -> dict[str, Any]:
    start = datetime(2026, 1, 1, 11, tzinfo=timezone.utc) + timedelta(days=day-1)
    slots = []
    for i, (solar, actual, demand, price) in enumerate([
        (2, morning, 0, .2), (2, afternoon, 0, .2), (0, 0, 2, .6),
    ]):
        slots.append(dict(start=(start+timedelta(hours=i)).isoformat(),
                          end=(start+timedelta(hours=i+1)).isoformat(),
                          solar_kwh=solar, measured_solar_kwh=actual,
                          demand_kwh=demand, measured_demand_kwh=demand,
                          import_price=price, export_price=.05))

    def window(i, mode):
        return dict(start=slots[i]['start'], end=slots[i]['end'], mode=mode)

    return dict(now=start.isoformat(), initial=0, capacity=2, max_charge=2,
                max_discharge=2, slots=slots, baseline=[], candidates=[
                    dict(id='late', kind='solar', windows=[window(1, replay.SOLAR), window(2, replay.HOME)]),
                    dict(id='early', kind='solar', windows=[window(0, replay.SOLAR), window(2, replay.HOME)]),
                ])


class LearningReplayTests(unittest.TestCase):
    def test_no_history_reproduces_fixed_planner(self):
        row, = replay.backtest([sample(1)])
        self.assertFalse(row['calibrated'])
        self.assertEqual(row['fixed'], row['learned'])
        self.assertEqual(row['fixed']['plan'], 'late')

    def test_past_cloudy_tails_change_choice_and_reduce_peak_import(self):
        rows = replay.backtest([sample(i) for i in range(1, 5)])
        self.assertFalse(rows[2]['calibrated'])
        row = rows[3]
        self.assertTrue(row['calibrated'])
        self.assertEqual(row['fixed']['plan'], 'late')
        self.assertEqual(row['learned']['plan'], 'early')
        self.assertAlmostEqual(row['fixed']['expensive_import_kwh'], 1.5)
        self.assertAlmostEqual(row['learned']['expensive_import_kwh'], 0)
        self.assertAlmostEqual(row['fixed']['cost_eur'] - row['learned']['cost_eur'], .825)
        self.assertEqual(row['fixed']['switches'], row['learned']['switches'])
        self.assertEqual(row['fixed']['end_kwh'], row['learned']['end_kwh'])

    def test_current_outcome_changes_bill_but_not_its_own_selection(self):
        train = [sample(i) for i in range(1, 4)]
        first = replay.backtest(train + [sample(4)])[3]
        second = replay.backtest(train + [sample(4, morning=.5, afternoon=2)])[3]
        self.assertEqual(first['learned']['plan'], second['learned']['plan'])
        self.assertNotEqual(first['learned']['cost_eur'], second['learned']['cost_eur'])

    def test_planning_snapshot_is_not_accepted_as_measurement(self):
        case = sample(1)
        del case['slots'][0]['measured_solar_kwh']
        with self.assertRaisesRegex(ValueError, 'measured_solar_kwh'):
            replay.backtest([case])

    def test_duplicate_delivery_days_cannot_inflate_training(self):
        with self.assertRaisesRegex(ValueError, 'day'):
            replay.backtest([sample(1), sample(1)])

    def test_old_measurements_expire(self):
        row = replay.backtest([sample(1), sample(2), sample(3), sample(32)])[-1]
        self.assertFalse(row['calibrated'])
        self.assertEqual(row['fixed'], row['learned'])

    def test_json_lines_input(self):
        self.assertEqual(replay.read_cases([json.dumps(sample(1))])[0]['now'], sample(1)['now'])
        with self.assertRaisesRegex(ValueError, 'line 2'):
            replay.read_cases(['\n', '{broken'])
