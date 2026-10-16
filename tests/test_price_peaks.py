"""Successive peaks, locked blocks, strict maximum ON time and configurable OFF time."""
import ast
from datetime import datetime, timedelta
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

from test_battery_energy_accounting import coordinator
from test_planner_types import load_function, ROOT
from custom_components.smart_energy_planner import const
from custom_components.smart_energy_planner.price_models import PlannerWindow
from custom_components.smart_energy_planner.price_peaks import plan_price_peaks, STATE_KEY

NOW = datetime.fromisoformat("2026-10-16T00:00:00+02:00")


def prices(values=(.1, .2, .5, .4, .1, .1, .2, .6, .3, .1), step=1):
    return [PlannerWindow(NOW+timedelta(hours=i*step), NOW+timedelta(hours=(i+1)*step), p)
            for i, p in enumerate(values)]


def plan(windows=None, **kwargs):
    return plan_price_peaks(prices() if windows is None else windows,
                           **{"now": NOW, "duration_hours": 2, "whole_hour_start": True, **kwargs})


class PeakPlanningTest(unittest.TestCase):
    def test_selects_best_block_around_each_peak(self):
        result = plan()["windows"]
        self.assertEqual([(w["start"][11:16], w["end"][11:16]) for w in result],
                         [("02:00", "04:00"), ("07:00", "09:00")])
        self.assertEqual([w["average_price"] for w in result], [.45, .45])

    def test_configurable_gap_skips_peak_that_cannot_fit(self):
        self.assertEqual(len(plan(minimum_gap_hours=4)["windows"]), 1)
        self.assertEqual(len(plan(minimum_gap_hours=3)["windows"]), 2)

    def test_no_joined_blocks_even_if_zero_gap_is_supplied(self):
        result = plan(prices((.1, .5, .3, .6, .2, .1)), minimum_gap_hours=0)["windows"]
        for left, right in zip(result, result[1:]):
            self.assertGreaterEqual(datetime.fromisoformat(right["start"])-datetime.fromisoformat(left["end"]),
                                    timedelta(minutes=15))
        self.assertTrue(all(w["duration_hours"] <= 2 for w in result))

    def test_plateau_is_one_peak_and_flat_or_edge_prices_are_not_peaks(self):
        self.assertEqual(len(plan(prices((.1, .5, .5, .1)))["windows"]), 1)
        for values in ((.1, .1, .1), (.1, .2, .3), (.3, .2, .1)):
            self.assertEqual(plan(prices(values))["windows"], [])

    def test_negative_prices_and_quarter_hour_contract(self):
        result = plan(prices((-.5, -.4, -.1, -.2, -.4, -.5), step=.25),
                      duration_hours=.5, whole_hour_start=False)["windows"]
        self.assertEqual(result[0]["start"], (NOW+timedelta(minutes=30)).isoformat())
        self.assertAlmostEqual(result[0]["average_price"], -.15)

    def test_daylight_saving_change_keeps_exact_elapsed_duration(self):
        from zoneinfo import ZoneInfo
        from datetime import timezone
        zone = ZoneInfo('Europe/Amsterdam')
        origin = datetime(2026, 10, 24, 22, tzinfo=timezone.utc)
        raw = [PlannerWindow((origin+timedelta(hours=i)).astimezone(zone),
                             (origin+timedelta(hours=i+1)).astimezone(zone), price)
               for i, price in enumerate((.1,.2,.6,.5,.3,.1))]
        result = plan_price_peaks(raw, now=origin.astimezone(zone), duration_hours=3,
                                  whole_hour_start=True)['windows']
        self.assertEqual(len(result), 1)
        start, end = (datetime.fromisoformat(result[0][key]) for key in ('start','end'))
        self.assertEqual(end.timestamp()-start.timestamp(), 3*3600)

    def test_missing_unknown_and_overlapping_prices_do_not_create_blocks(self):
        raw = prices((.1, .2, .5, .4, .1))
        self.assertEqual(plan(raw[:2]+raw[3:])["windows"], [])
        raw[2].price_known = False
        self.assertEqual(plan(raw)["windows"], [])
        raw[2].price_known = True
        self.assertEqual(plan(raw+[raw[2]])["windows"], [])

    def test_refresh_and_restart_keep_times_and_never_extend(self):
        saved = plan()
        revised = prices((.1, .3, .2, .9, .1, .1, .2, .3, .8, .1))
        for hour in (1, 2, 3):
            saved = plan(revised, now=NOW+timedelta(hours=hour), saved=json.loads(json.dumps(saved)))
            self.assertEqual(saved["windows"][0]["end"], (NOW+timedelta(hours=4)).isoformat())
        original = list(saved["windows"])
        self.assertEqual(plan([], saved=saved)["windows"], original)
        self.assertEqual(plan(duration_hours=3, saved=saved)["windows"], original)
        shortened = plan(duration_hours=1, saved=saved)["windows"]
        self.assertTrue(all(w["duration_hours"] <= 1 for w in shortened))

    def test_completed_blocks_are_not_found_again_and_new_day_is_added(self):
        saved = plan()
        later = [PlannerWindow(w.start+timedelta(days=1), w.end+timedelta(days=1), w.price) for w in prices()]
        updated = plan(prices()+later, now=NOW+timedelta(hours=10), saved=saved)
        self.assertEqual(len(updated["windows"]), 2)
        self.assertTrue(all(w["start"].startswith("2026-10-17") for w in updated["windows"]))


class PeakPersistenceTest(unittest.IsolatedAsyncioTestCase):
    async def test_real_refresh_preserves_peaks_when_source_disappears(self):
        instance = coordinator()
        instance.config_entry = SimpleNamespace(entry_id='peaks', options={}, data={
            'planner_kind': 'price_window', 'price_sensor': 'sensor.prices',
            'price_window_type': 'most_expensive', 'price_window_duration_hours': 2,
            'price_peak_min_gap_hours': 1,
        })
        source = SimpleNamespace(state='.1', attributes={'raw_today': [
            {'start': w.start.isoformat(), 'end': w.end.isoformat(), 'value': w.price} for w in prices()
        ]})
        instance.hass = SimpleNamespace(data={}, states=SimpleNamespace(
            get=lambda entity_id: source if entity_id == 'sensor.prices' else None, async_all=lambda: [],
        ))
        instance._schedule_source_error_retry = Mock()
        instance._cancel_source_error_retry = Mock()
        instance._async_persist_runtime_state = AsyncMock()
        with patch.object(instance._async_update_data.__globals__['dt_util'], 'now', return_value=NOW):
            result = await instance._async_update_data()
            self.assertEqual(len(result.planned_price_peak_windows), 2, result.source_errors)
            instance.hass.states.get = lambda entity_id: None
            missing = await instance._async_update_data()
        self.assertEqual(missing.planned_price_peak_windows, result.planned_price_peak_windows)
        self.assertEqual(instance._async_persist_runtime_state.await_count, 2)

    async def test_production_storage_and_setup_restore_committed_windows(self):
        saved = plan()
        instance = coordinator()
        instance.config_entry = SimpleNamespace(entry_id="peaks")
        instance.hass = SimpleNamespace(data={})
        stored = {}
        class Store:
            def __class_getitem__(cls, key): return cls
            def __init__(self, *args): pass
            async def async_load(self): return stored
            async def async_save(self, data): stored.update(json.loads(json.dumps(data)))
        with patch.dict(instance._async_persist_runtime_state.__globals__, Store=Store):
            await instance._async_persist_runtime_state({STATE_KEY: saved})
        tree = ast.parse((ROOT/'__init__.py').read_text(encoding='utf-8-sig'))
        mapping = next(n for n in ast.walk(tree) if isinstance(n, ast.Dict)
                       and any(isinstance(k, ast.Constant) and k.value == STATE_KEY for k in n.keys))
        expression = mapping.values[next(i for i,k in enumerate(mapping.keys) if isinstance(k,ast.Constant) and k.value==STATE_KEY)]
        restored = eval(compile(ast.Expression(expression), '<setup>', 'eval'), {'persisted_state': stored['peaks']})
        self.assertEqual(plan([], saved=restored)["windows"], saved["windows"])


class PeakBitTest(unittest.TestCase):
    def test_boundary_callbacks_turn_off_at_end_without_coordinator_refresh(self):
        class Base:
            def __class_getitem__(cls, key): return cls
            def __init__(self, coordinator): self.coordinator = coordinator
            def async_write_ha_state(self): self.writes.append(self.is_on)
        tree = ast.parse((ROOT/'binary_sensor.py').read_text())
        nodes = [n for n in tree.body if isinstance(n,(ast.ClassDef,ast.FunctionDef))]
        clock = SimpleNamespace(now=lambda: NOW)
        scheduled = []
        def track(hass, callback, at):
            scheduled.append((at, callback))
            return Mock()
        scope = {**vars(const), 'CoordinatorEntity': Base, 'BinarySensorEntity': object,
                 'SmartEnergyPlannerCoordinator': object, 'callback': lambda f:f,
                 'datetime': datetime, 'dt_util': clock, 'async_track_point_in_time': track}
        future = ast.ImportFrom(module='__future__', names=[ast.alias(name='annotations')],level=0)
        exec(compile(ast.fix_missing_locations(ast.Module(body=[future,*nodes],type_ignores=[])), '<binary>', 'exec'), scope)
        data = SimpleNamespace(price_window_type='most_expensive', planned_price_peak_windows=plan()['windows'])
        entity = scope['PriceWindowBinarySensor'](SimpleNamespace(data=data), SimpleNamespace(title='Peaks', entry_id='peaks'),
                                                 key='selected_price_window', name='High', icon='')
        entity.hass = SimpleNamespace(); entity.writes = []
        entity._schedule_boundary()
        for hour, expected in [(2,True),(4,False),(7,True),(9,False)]:
            at, callback = scheduled[-1]
            self.assertEqual(at, NOW+timedelta(hours=hour))
            clock.now = lambda at=at: at
            callback(at)
            self.assertEqual(entity.writes[-1], expected)
        self.assertIsNone(entity._boundary_unsub)
