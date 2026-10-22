"""Grouped shoulders and minimum-duration filtering for heating schedules."""
from datetime import timedelta
import unittest

from test_price_peaks import plan, prices, NOW


class PeakGroupTest(unittest.TestCase):
    def test_later_lower_summit_bridges_dip_but_stops_before_next_basin(self):
        raw = prices((.1, .3, .6, .34, .42, .2, .1, .4, .1), step=.25)
        result = plan(raw, duration_hours=1.5, whole_hour_start=False,
                      minimum_prominence_percent=50, minimum_duration_minutes=0)['windows'][0]
        self.assertEqual(result['start'], (NOW+timedelta(minutes=30)).isoformat())
        self.assertEqual(result['end'], (NOW+timedelta(minutes=75)).isoformat())
        self.assertAlmostEqual(result['normal_price_threshold'], .35)

    def test_minimum_skips_short_window_without_extending_it(self):
        raw = prices((.1, .2, .6, .2, .1, .1, .1, .1), step=.25)
        self.assertEqual(plan(raw, duration_hours=1.5, whole_hour_start=False)['windows'], [])
        accepted = plan(raw, duration_hours=1.5, whole_hour_start=False,
                        minimum_duration_minutes=30)['windows']
        self.assertEqual(accepted[0]['duration_hours'], .75)

    def test_exact_minimum_and_disabled_filter(self):
        raw = prices((.1, .6, .6, .6, .6, .1, .1, .1), step=.25)
        block = plan(raw, duration_hours=1.5, whole_hour_start=False)['windows'][0]
        self.assertEqual(block['duration_hours'], 1)
        self.assertEqual(plan(raw, duration_hours=1.5, whole_hour_start=False,
                              minimum_duration_minutes=75)['windows'], [])
        self.assertTrue(plan(raw, duration_hours=.5, whole_hour_start=False,
                             minimum_duration_minutes=0)['windows'])

    def test_minimum_does_not_cancel_active_block(self):
        raw = prices((.1, .2, .6, .2, .1, .1, .1, .1), step=.25)
        saved = plan(raw, duration_hours=1.5, whole_hour_start=False,
                     minimum_duration_minutes=0)
        active = plan(raw, duration_hours=1.5, whole_hour_start=False,
                      now=NOW+timedelta(minutes=35), saved=saved)
        self.assertEqual(active['windows'], saved['windows'])

    def test_minimum_over_maximum_produces_no_new_blocks(self):
        self.assertEqual(plan(duration_hours=2, minimum_duration_minutes=180)['windows'], [])
