"""Short price windows retain the summit rather than the cheap rising flank."""
import json
from datetime import datetime, timedelta
from pathlib import Path
import unittest

from test_price_peaks import PlannerWindow, plan_price_peaks, plan, prices, NOW


class RisingFlankTest(unittest.TestCase):
    def setUp(self):
        raw = json.loads((Path(__file__).parent/'fixtures/price_peaks_2026_10_22.json').read_text())
        self.windows = [PlannerWindow(datetime.fromisoformat(w['start']),
            datetime.fromisoformat(w['end']), w['price'], w['price_known']) for w in raw]
        self.args = dict(now=datetime.fromisoformat('2026-10-22T14:46:54+02:00'),
            duration_hours=5, whole_hour_start=False, minimum_gap_hours=5,
            minimum_prominence_percent=50)

    def test_morning_group_includes_later_lower_summits(self):
        result = plan_price_peaks(self.windows, **self.args)
        self.assertEqual([(w['start'][11:16], w['end'][11:16]) for w in result['windows']],
                         [('17:15', '21:45'), ('06:30', '10:15')])
        morning = result['windows'][1]
        self.assertEqual(morning['duration_hours'], 3.75)
        self.assertAlmostEqual(morning['normal_price_threshold'], .3185)
        self.assertLessEqual(morning['start'], morning['peak_at'])
        self.assertLess(morning['peak_at'], morning['end'])

    def test_saved_future_start_migrates_once_and_survives_refresh_restart(self):
        saved = plan_price_peaks(self.windows, **self.args)
        saved['windows'][1].update(start='2026-10-23T05:45:00+02:00', duration_hours=2.75, peak_group_version=0)
        result = plan_price_peaks(self.windows, saved=saved, **self.args)
        self.assertEqual(result['windows'][1]['start'], '2026-10-23T06:30:00+02:00')
        for restart in (False, True):
            result2 = plan_price_peaks(self.windows, saved=json.loads(json.dumps(result)),
                                      reselect_pending=restart, **self.args)
            self.assertEqual(result2['windows'], result['windows'])

    def test_active_legacy_start_is_not_rewritten(self):
        saved = plan_price_peaks(self.windows, **self.args)
        saved['windows'][1].update(start='2026-10-23T05:45:00+02:00', duration_hours=2.75, peak_group_version=0)
        result = plan_price_peaks(self.windows, saved=saved,
            **{**self.args, 'now': datetime.fromisoformat('2026-10-23T06:00:00+02:00')})
        self.assertEqual(result['windows'][0]['start'], '2026-10-23T05:45:00+02:00')

    def test_missing_prices_do_not_move_saved_start(self):
        saved = plan_price_peaks(self.windows, **self.args)
        saved['windows'][1].update(start='2026-10-23T05:45:00+02:00', duration_hours=2.75, peak_group_version=0)
        result = plan_price_peaks([], saved=saved, **self.args)
        self.assertEqual(result['windows'][1]['start'], '2026-10-23T05:45:00+02:00')

    def test_hundred_percent_retains_complete_flat_summit(self):
        result = plan(prices((.1,.2,.5,.5,.2,.1,.1,.1), step=.25),
                      duration_hours=1.5, whole_hour_start=False,
                      minimum_prominence_percent=100, minimum_duration_minutes=0)['windows'][0]
        self.assertEqual(result['start'], (NOW+timedelta(minutes=30)).isoformat())
        self.assertEqual(result['end'], (NOW+timedelta(hours=1)).isoformat())

    def test_whole_hour_start_rounds_back_to_keep_peak(self):
        result = plan(prices((.1,.2,.5,.2,.1,.1,.1,.1), step=.25),
                      duration_hours=1.5, whole_hour_start=True,
                      minimum_prominence_percent=100, minimum_duration_minutes=0)['windows'][0]
        self.assertEqual(result['start'], NOW.isoformat())
        self.assertLess(result['peak_at'], result['end'])

    def test_higher_minimum_removes_future_short_group_only(self):
        saved = plan_price_peaks(self.windows, **self.args)
        result = plan_price_peaks(self.windows, saved=saved,
                                  minimum_duration_minutes=240, **self.args)
        self.assertEqual(len(result['windows']), 1)
        self.assertEqual(result['windows'][0]['start'][11:16], '17:15')
        self.assertEqual(result['windows'][0]['end'][11:16], '21:45')
