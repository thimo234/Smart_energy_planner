"""Maximum ON duration ends early when the peak returns to its normal band."""
import json
from datetime import datetime, timedelta
from pathlib import Path
import unittest

from test_price_peaks import plan, prices, NOW, PlannerWindow, plan_price_peaks


class EarlyEndTest(unittest.TestCase):
    def test_large_evening_peak_wins_over_small_afternoon_peak(self):
        raw=json.loads((Path(__file__).parent/'fixtures/price_peaks_2026_10_18.json').read_text())
        windows=[PlannerWindow(datetime.fromisoformat(w['start']),datetime.fromisoformat(w['end']),w['price']) for w in raw]
        stop='2026-10-18T10:00:00+02:00'
        result=plan_price_peaks(windows,now=datetime.fromisoformat(stop),duration_hours=5,
            whole_hour_start=False,minimum_gap_hours=5,
            saved={'last_end':stop,'last_finished_end':stop})
        self.assertEqual([(w['start'][11:16],w['end'][11:16]) for w in result['windows']],
                         [('17:15','22:15')])
        self.assertEqual(result['windows'][0]['peak_price'],.503)
        restored=plan_price_peaks(windows,now=datetime.fromisoformat('2026-10-18T13:00:00+02:00'),
            duration_hours=5,whole_hour_start=False,minimum_gap_hours=5,saved=result)
        self.assertEqual(restored['windows'],result['windows'])

    def test_returns_to_valley_plus_percentage_not_daily_average(self):
        raw = prices((.1, .3, .6, .3, .15, .1, .1, .1))
        block = plan(raw, duration_hours=5)['windows'][0]
        self.assertEqual(block['start'], NOW.isoformat())
        self.assertEqual(block['end'], (NOW+timedelta(hours=4)).isoformat())
        self.assertAlmostEqual(block['normal_price_threshold'], .15)
        self.assertAlmostEqual(block['average_price'], .325)
        self.assertEqual(block['duration_hours'], 4)

    def test_shortened_end_survives_restart_missing_prices_and_higher_duration(self):
        raw = prices((.1, .3, .6, .3, .15, .1, .1, .1))
        saved = plan(raw, duration_hours=5)
        end = saved['windows'][0]['end']
        for hour in (1, 3):
            saved = plan([], now=NOW+timedelta(hours=hour), duration_hours=7,
                         saved=json.loads(json.dumps(saved)))
            self.assertEqual(saved['windows'][0]['end'], end)
        finished = plan(raw, now=NOW+timedelta(hours=4), duration_hours=5, saved=saved)
        self.assertEqual(finished['windows'], [])
        self.assertEqual(finished['last_finished_end'], end)

    def test_active_legacy_block_stops_now_without_retroactive_end(self):
        raw = prices((.1, .3, .6, .3, .15, .1, .1, .1))
        saved = plan(raw, duration_hours=5)
        saved['windows'][0]['end'] = (NOW+timedelta(hours=5)).isoformat()
        saved['last_end'] = saved['windows'][0]['end']
        now = NOW+timedelta(hours=4, minutes=30)
        result = plan(raw, now=now, duration_hours=5, saved=saved)
        self.assertEqual(result['windows'], [])
        self.assertEqual(result['last_finished_end'], now.isoformat())
        self.assertEqual(result['last_end'], now.isoformat())

    def test_gap_starts_at_actual_early_end_and_maximum_is_preserved(self):
        raw = prices((.1,.3,.6,.1,.1,.1,.1,.1,.1,.4,.7,.3,.1,.1,.1,.1))
        result = plan(raw,duration_hours=5,minimum_gap_hours=1)['windows']
        self.assertGreaterEqual(len(result),2)
        for block in result:
            self.assertLessEqual(block['duration_hours'],5)
        for left,right in zip(result,result[1:]):
            self.assertGreaterEqual(datetime.fromisoformat(right['start'])-
                                    datetime.fromisoformat(left['end']),timedelta(hours=1))

    def test_finished_legacy_block_keeps_actual_historical_end(self):
        raw = prices((.1, .3, .6, .3, .15, .1, .1, .1))
        saved = plan(raw,duration_hours=5)
        end = (NOW+timedelta(hours=5)).isoformat()
        saved['windows'][0]['end'] = end
        saved['last_end'] = end
        result = plan(raw,now=NOW+timedelta(hours=6),duration_hours=5,saved=saved)
        self.assertEqual(result['last_finished_end'],end)

    def test_supplied_day_ends_morning_block_when_price_normalizes(self):
        raw = json.loads((Path(__file__).parent/'fixtures/price_peaks_2026_10_18.json').read_text())
        windows = [PlannerWindow(datetime.fromisoformat(w['start']),datetime.fromisoformat(w['end']),w['price']) for w in raw]
        result = plan_price_peaks(windows,now=windows[0].start,duration_hours=5,
                                 whole_hour_start=False)['windows']
        morning = next(w for w in result if w['peak_at'][11:16]=='10:07')
        self.assertEqual(morning['end'][11:16],'10:30')
        self.assertAlmostEqual(morning['normal_price_threshold'],.359)

    def test_missing_tariffs_do_not_shorten_saved_block(self):
        raw = prices((.1, .3, .6, .3, .15, .1, .1, .1))
        saved = plan(raw,duration_hours=5)
        saved['windows'][0]['end']=(NOW+timedelta(hours=5)).isoformat()
        saved['windows'][0]['duration_hours']=5
        saved['last_end']=saved['windows'][0]['end']
        result = plan(raw[:3]+raw[4:],duration_hours=5,saved=saved)
        self.assertEqual(result['windows'][0]['end'],saved['windows'][0]['end'])
