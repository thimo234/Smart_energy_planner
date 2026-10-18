"""Filter small fluctuations without limiting the number of daily peaks."""
from datetime import datetime, timedelta
import json
from pathlib import Path
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from test_price_peaks import plan_price_peaks, PlannerWindow, prices, NOW
from test_battery_energy_accounting import coordinator
from custom_components.smart_energy_planner.const import RUNTIME_STATE


def supplied_prices():
    data = json.loads((Path(__file__).parent/'fixtures/battery_2026_10_10_short_cycles.json').read_text())
    return [PlannerWindow(datetime.fromisoformat(w['start']), datetime.fromisoformat(w['end']), w['price'])
            for w in data['upcoming_energy_price_windows'] if w['price_known']]


def replay(windows=None, **kwargs):
    raw = supplied_prices() if windows is None else windows
    return plan_price_peaks(raw, **{
        'now': supplied_prices()[0].start-timedelta(hours=1), 'duration_hours': 3,
        'whole_hour_start': True, 'minimum_gap_hours': 2, **kwargs,
    })


def legacy_state():
    return json.loads((Path(__file__).parent/'fixtures/price_peaks_legacy_2026_10_10.json').read_text())


class PeakProminenceTest(unittest.TestCase):
    def test_supplied_prices_have_two_broad_peaks_with_default_ten_percent(self):
        result = replay()['windows']
        self.assertEqual([(w['start'][11:16],w['end'][11:16]) for w in result],
                         [('07:00','10:00'),('18:00','21:00')])
        self.assertEqual([w['peak_prominence'] for w in result], [.061,.072])

    def test_percentage_is_configurable_and_zero_restores_small_peaks(self):
        self.assertEqual(len(replay(minimum_prominence_percent=0)['windows']), 6)
        self.assertEqual(len(replay(minimum_prominence_percent=10)['windows']), 2)
        self.assertEqual(len(replay(minimum_prominence_percent=50)['windows']), 0)

    def test_relative_height_is_invariant_to_scale_and_negative_price_offset(self):
        baseline = replay()['windows']
        transformed = [PlannerWindow(w.start,w.end,3*w.price-2) for w in supplied_prices()]
        result = replay(transformed)['windows']
        self.assertEqual([(w['start'],w['end']) for w in result],
                         [(w['start'],w['end']) for w in baseline])

    def test_does_not_limit_each_day_to_two_peaks(self):
        raw = prices((.1,.5,.1,.1,.6,.1,.1,.7,.1))
        result = plan_price_peaks(raw, now=NOW, duration_hours=1, whole_hour_start=True,
                                  minimum_prominence_percent=10)['windows']
        self.assertEqual(len(result),3)

    def test_tomorrow_price_range_does_not_change_today_threshold(self):
        raw = supplied_prices()
        tomorrow = [PlannerWindow(w.start+timedelta(days=1), w.end+timedelta(days=1), w.price*20+5) for w in raw]
        result = replay(raw+tomorrow)['windows']
        self.assertEqual([(w['start'],w['end']) for w in result[:2]],
                         [(w['start'],w['end']) for w in replay()['windows']])

    def test_cleanup_keeps_large_block_times_and_can_be_disabled(self):
        saved = legacy_state()
        filtered = replay(saved=saved)['windows']
        self.assertEqual(len(filtered),2)
        self.assertEqual(filtered, [saved['windows'][1],saved['windows'][3]])
        self.assertEqual(replay(saved=saved, remove_small_pending=False)['windows'], saved['windows'])

    def test_cleanup_preserves_active_block_and_never_extends_it(self):
        saved = legacy_state()
        now = datetime.fromisoformat(saved['windows'][0]['start'])+timedelta(minutes=30)
        result = replay(saved=saved,now=now)['windows']
        self.assertEqual(len(result),3)
        self.assertEqual(result[0],saved['windows'][0])

    def test_incomplete_or_missing_prices_do_not_cancel_pending_blocks(self):
        saved = legacy_state()
        self.assertEqual(replay([],saved=saved)['windows'],saved['windows'])
        self.assertEqual(replay(supplied_prices()[1:],saved=saved)['windows'],saved['windows'])

    def test_cancelled_future_tail_does_not_block_new_valid_peaks(self):
        saved = legacy_state()
        cleared = replay(saved=saved, minimum_prominence_percent=50)
        self.assertFalse(cleared['windows'])
        self.assertEqual(len(replay(saved=cleared)['windows']),2)

    def test_finished_block_preserves_minimum_pause_after_cleanup(self):
        saved = legacy_state()
        now = datetime.fromisoformat(saved['windows'][1]['end'])
        cleared = replay(saved=saved, now=now, minimum_prominence_percent=50)
        self.assertEqual(datetime.fromisoformat(cleared['last_finished_end']),now)
        self.assertEqual(datetime.fromisoformat(cleared['last_end']),now)


class PeakSettingsTest(unittest.IsolatedAsyncioTestCase):
    async def test_saved_ui_options_reach_planner_including_zero_and_false(self):
        raw = supplied_prices()
        now = raw[0].start-timedelta(hours=1)
        source = SimpleNamespace(state='.15', attributes={'raw_today': [
            {'start': w.start.isoformat(), 'end': w.end.isoformat(), 'value': w.price} for w in raw
        ]})
        old = legacy_state()
        for percentage, cleanup, expected in [(0,True,4),(10,False,4),(10,True,2),(50,True,0)]:
            instance = coordinator()
            instance.config_entry = SimpleNamespace(entry_id='peaks', data={
                'planner_kind':'price_window', 'price_sensor':'sensor.prices',
                'price_window_type':'most_expensive', 'price_window_duration_hours':3,
                'price_resolution':'quarter_hourly',
                'price_peak_min_gap_hours':2,
            }, options={'price_peak_min_prominence_percent':percentage,
                        'price_peak_remove_small_pending':cleanup})
            instance.hass = SimpleNamespace(data={RUNTIME_STATE:{'peaks':{'price_peak_state':old}}},
                states=SimpleNamespace(get=lambda key:source if key=='sensor.prices' else None,
                                       async_all=lambda:[]))
            instance._schedule_source_error_retry=Mock()
            instance._cancel_source_error_retry=Mock()
            instance._async_persist_runtime_state=AsyncMock()
            with patch.object(instance._async_update_data.__globals__['dt_util'],'now',return_value=now):
                result=await instance._async_update_data()
            self.assertEqual(len(result.planned_price_peak_windows),expected,result.source_errors)
