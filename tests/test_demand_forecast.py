"""Independent device learning, schedules, accounting and external contracts."""
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from zoneinfo import ZoneInfo
import unittest

from test_support import install_package_stub
install_package_stub()
from custom_components.smart_energy_planner.demand_forecast import (
    build_demand_forecast, component_forecast, meter_hours, read_forecast, statistics_hours,
)
from custom_components.smart_energy_planner.price_models import PlannerWindow


def fixtures():
    now = datetime(2026, 10, 18, tzinfo=ZoneInfo('Europe/Amsterdam'))
    day = now-timedelta(days=14)
    profiles = {'dish': {11: 1, 12: .3}, 'pump': {13: 2, 14: 1, 15: .5},
                'cook': {17: 1.2, 18: .6}}
    histories = {name: [] for name in [*profiles, 'house']}
    counters = dict.fromkeys(histories, 100.)
    for i in range(14*24+1):
        at = day+timedelta(hours=i)
        for name in histories:
            histories[name].append(SimpleNamespace(state=str(counters[name]), last_updated=at,
                attributes={'unit_of_measurement': 'kWh', 'state_class': 'total'}))
        values = {name: profile.get(at.hour, 0) for name, profile in profiles.items()}
        values['house'] = sum(values.values())+.3
        for name, value in values.items():
            counters[name] += value
    prices = [PlannerWindow(now+timedelta(hours=i), now+timedelta(hours=i+1),
                            .1 if 11 <= i%24 < 16 else .4) for i in range(72)]
    devices = [dict(entity_id='dish', timing='cheap', offset_hours=0),
               dict(entity_id='pump', timing='cheap', offset_hours=2),
               dict(entity_id='cook', timing='fixed', start_time='17:30:00')]
    return now, histories, prices, devices


class DemandForecastTest(unittest.TestCase):
    def test_hourly_statistics_restore_idle_appliance_history_without_double_counting(self):
        now, histories, prices, devices = fixtures()
        statistics = {}
        for entity, states in histories.items():
            hours = meter_hours(states, now=now)
            counter=0
            rows=[dict(start=(min(hours)-timedelta(hours=1)).timestamp(),sum=counter)]
            for at,value in sorted(hours.items()):
                counter+=value
                rows.append(dict(start=at.timestamp(),sum=counter))
            statistics[entity]=rows
        sparse={e:[rows[0],rows[-1]] for e,rows in histories.items()}
        with self.assertRaisesRegex(ValueError,'insufficient_history'):
            build_demand_forecast(sparse,devices,total_entity='house',now=now,prices=prices)
        expected=build_demand_forecast(histories,devices,total_entity='house',now=now,prices=prices)
        for raw in (sparse,histories):
            actual=build_demand_forecast(raw,devices,total_entity='house',now=now,prices=prices,hourly_statistics=statistics)
            self.assertEqual(actual,expected)

    def test_statistics_gaps_corrections_and_unfinished_hours_are_not_zero(self):
        now, _, _, _=fixtures()
        start=now-timedelta(hours=8)
        rows=[dict(start=(start+timedelta(hours=h)).timestamp(),sum=v)
              for h,v in [(0,10),(1,10),(2,11),(4,15),(5,14),(6,None),(7,20),(8,21)]]
        result=statistics_hours(rows,now=now)
        self.assertEqual(result,{start+timedelta(hours=1):0,start+timedelta(hours=2):1})

    def test_sum_residual_and_scheduled_peaks(self):
        now, histories, prices, devices = fixtures()
        forecast, components, notes = build_demand_forecast(histories, devices,
            total_entity='house', now=now, prices=prices)
        self.assertEqual(len(forecast), 288)
        self.assertAlmostEqual(sum(w['estimated_kwh'] for w in forecast[:96]), 13.8, places=5)
        self.assertAlmostEqual(sum(w['estimated_kwh'] for w in components['remaining_home'][:96]), 7.2, places=5)
        for entity, hour, minute in [('dish',11,0), ('pump',13,0), ('cook',17,30)]:
            first = next(w for w in components[entity] if w['estimated_kwh'] > 0)
            start = datetime.fromisoformat(first['start'])
            self.assertEqual((start.hour,start.minute), (hour,minute))
        for i, row in enumerate(forecast):
            self.assertAlmostEqual(row['estimated_kwh'], sum(v[i]['estimated_kwh'] for v in components.values()), places=5)

    def test_duplicate_or_overlapping_sources_rejected(self):
        now, histories, prices, devices = fixtures()
        for bad in (devices+[devices[0]], devices+[dict(entity_id='house')]):
            with self.assertRaisesRegex(ValueError, 'duplicate'):
                build_demand_forecast(histories, bad, total_entity='house', now=now, prices=prices)
        histories['house'] = histories['dish']
        with self.assertRaisesRegex(ValueError, 'exceeds_total'):
            build_demand_forecast(histories, devices, total_entity='house', now=now, prices=prices)

    def test_unknown_history_does_not_mean_zero(self):
        now, histories, prices, devices = fixtures()
        histories['pump'] = []
        with self.assertRaisesRegex(ValueError, 'insufficient'):
            build_demand_forecast(histories, devices, total_entity='house', now=now, prices=prices)

    def test_missing_future_prices_use_explicit_historical_fallback(self):
        now, histories, prices, devices = fixtures()
        forecast, _, notes = build_demand_forecast(histories, devices, total_entity='house', now=now, prices=[])
        self.assertTrue(notes['pump'])
        self.assertEqual(len(forecast), 288)
        self.assertAlmostEqual(sum(w['estimated_kwh'] for w in forecast[:96]), 13.8, places=5)

    def test_recalculation_does_not_move_started_cheap_block(self):
        now, histories, prices, devices = fixtures()
        a, _, _ = build_demand_forecast(histories, devices, total_entity='house', now=now, prices=prices)
        b, _, _ = build_demand_forecast(histories, devices, total_entity='house', now=now+timedelta(hours=12), prices=prices)
        self.assertEqual(a,b)

    def test_contract_rejects_stale_gap_overlap_negative_and_nan(self):
        now, histories, prices, devices = fixtures()
        forecast, _, _ = build_demand_forecast(histories, devices, total_entity='house', now=now, prices=prices)
        attrs = dict(forecast_version=1, unit_of_measurement='kWh',
                     forecast_generated_at=now.isoformat(), estimated_hourly_home_demand=forecast)
        state = SimpleNamespace(state='13.8', attributes=attrs)
        self.assertEqual(read_forecast(state, now=now, horizon_end=now+timedelta(days=2)), forecast)
        for rows in (forecast[1:], forecast+[forecast[0]],
                     [{**forecast[0], 'estimated_kwh': -1}]+forecast[1:],
                     [{**forecast[0], 'estimated_kwh': float('nan')}]+forecast[1:]):
            state.attributes = {**attrs, 'estimated_hourly_home_demand': rows}
            with self.assertRaises(ValueError):
                read_forecast(state, now=now, horizon_end=now+timedelta(days=2))
        state.attributes = attrs
        with self.assertRaisesRegex(ValueError, 'stale'):
            read_forecast(state, now=now+timedelta(hours=3), horizon_end=now+timedelta(days=2))

    def test_signed_corrections_and_unit_conversion(self):
        now = datetime(2026,10,18,tzinfo=timezone.utc)
        states = [SimpleNamespace(state=v, last_updated=now+timedelta(minutes=m),
                  attributes=dict(unit_of_measurement='Wh', state_class='total'))
                  for m,v in [(0,'1000'),(15,'1300'),(30,'9000'),(30,'1200'),(60,'1500')]]
        hours = meter_hours(states, now=now+timedelta(hours=1))
        self.assertAlmostEqual(hours[now], .5)
        states[2].state = 'unavailable'
        states[3].state = 'unavailable'
        self.assertFalse(meter_hours(states, now=now+timedelta(hours=1)))

    def test_dst_horizon_has_no_gap_or_duplicate_interval(self):
        now, histories, prices, devices = fixtures()
        # Replay the same history a week later, across the 25-hour autumn day.
        later = now+timedelta(days=7)
        forecast, _, _ = build_demand_forecast(histories, [], total_entity='house', now=later, prices=[])
        self.assertEqual(len(forecast), 292)
        for a,b in zip(forecast,forecast[1:]):
            self.assertEqual(datetime.fromisoformat(a['end']),datetime.fromisoformat(b['start']))
        state=SimpleNamespace(state='13.8',attributes=dict(forecast_version=1,unit_of_measurement='kWh',
            forecast_generated_at=later.isoformat(),estimated_hourly_home_demand=forecast))
        self.assertTrue(read_forecast(state,now=later,horizon_end=later+timedelta(days=2)))

    def test_reset_counter_does_not_drop_post_reset_usage(self):
        now=datetime(2026,10,18,tzinfo=timezone.utc)
        states=[SimpleNamespace(state=v,last_updated=now+timedelta(hours=i),
                attributes=dict(unit_of_measurement='kWh',state_class='total_increasing'))
                for i,v in enumerate(['10','10.5','0.2'])]
        hours=meter_hours(states,now=now+timedelta(hours=2))
        self.assertAlmostEqual(sum(hours.values()),.7)


if __name__ == '__main__':
    unittest.main()
