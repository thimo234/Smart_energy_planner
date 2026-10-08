"""A real storage reload and unavailable startup SOC must preserve cycles."""
import ast
import json
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch
import unittest

from test_battery_energy_accounting import coordinator, energy_trace, replay_full_plan

NOW = datetime.fromisoformat("2026-10-01T08:34:19.167332+02:00")


def replay(instance, soc):
    return replay_full_plan("2026_10_01_restart", NOW.isoformat(), soc=soc,
                           instance=instance, reserve=60, max_charge=2.5,
                           tax_deduction=.11, isolated_cycles=True)


def cold_coordinator():
    c = coordinator()
    c._charge_session_started = False
    c._discharge_session_started = False
    c._battery_cycle_state_initialized = False
    return c


class RestartTest(unittest.IsolatedAsyncioTestCase):
    def test_repeated_unknown_soc_does_not_create_charge_cycle(self):
        c = cold_coordinator()
        for _ in range(3):
            _, _, result = replay(c, None)
            self.assertEqual(result.battery_strategy, "accu_uit")
            self.assertFalse(result.planned_battery_mode_windows)
            self.assertFalse(c._battery_cycle_state_initialized)
            self.assertFalse(c._charge_session_started)
        now, slots, result = replay(c, 39)
        # 8 October: no full profitable refill in this forecast, and the
        # measured 39% is already below the 60% no-charge reserve.
        self.assertEqual(result.battery_strategy, "accu_uit")
        self.assertFalse(result.planned_grid_charge_windows)
        trace = energy_trace(now, slots, result.planned_battery_mode_windows, initial=3.9, max_charge=2.5)
        self.assertAlmostEqual(trace[-1][1], 3.9, delta=.005)

    def test_unknown_soc_preserves_both_existing_directions(self):
        for charging in (False, True):
            c = coordinator()
            c._charge_session_started = charging
            c._discharge_session_started = not charging
            c._battery_cycle_state_restored_recent = True
            c._battery_grid_charge_price = .25
            c._active_charge_phase_end = NOW + timedelta(hours=1) if charging else None
            before = vars(c).copy()
            for _ in range(3):
                replay(c, None)
            for name in before:
                self.assertEqual(getattr(c, name), before[name], name)

    async def test_disk_roundtrip_setup_mapping_and_startup_updates_match_uninterrupted_plan(self):
        c = coordinator()
        c._charge_session_started = False
        c._discharge_session_started = True
        c._battery_grid_charge_price = .25
        _, _, baseline = replay(c, 39)
        c.hass = SimpleNamespace(data={})
        c.config_entry.entry_id = "battery"
        c._store_battery_cycle_state_snapshot(NOW)
        namespace = c._async_persist_runtime_state.__globals__
        runtime_key = namespace["RUNTIME_STATE"]
        with TemporaryDirectory() as folder:
            file = Path(folder) / "runtime.json"
            class DiskStore:
                def __init__(self, *args):
                    pass
                def __class_getitem__(cls, _):
                    return cls
                async def async_load(self):
                    return json.loads(file.read_text()) if file.exists() else None
                async def async_save(self, data):
                    file.write_text(json.dumps(data))
            with patch.dict(namespace, Store=DiskStore):
                await c._async_persist_runtime_state(c.hass.data[runtime_key]["battery"])
            persisted = json.loads(file.read_text())["battery"]
        # Evaluate the production setup mapping, so an omitted stored field
        # cannot pass by simply sharing the previous hass.data dictionary.
        source = Path('custom_components/smart_energy_planner/__init__.py').read_text(encoding='utf-8-sig')
        setup = next(n for n in ast.parse(source).body if isinstance(n, ast.AsyncFunctionDef) and n.name == 'async_setup_entry')
        mapping = next(n.value for n in ast.walk(setup) if isinstance(n, ast.Assign)
                       and isinstance(n.value, ast.Dict) and any(isinstance(k, ast.Constant) and k.value == 'battery_cycle_state' for k in n.value.keys))
        restored = {key.value: eval(compile(ast.Expression(value), '<setup>', 'eval'), {'persisted_state': persisted})
                    for key, value in zip(mapping.keys, mapping.values)
                    if isinstance(key, ast.Constant) and key.value in ('battery_cycle_state', 'battery_grid_charge_price')}
        restarted = cold_coordinator()
        restarted.hass = SimpleNamespace(data={runtime_key: {"battery": restored}})
        restarted.config_entry = c.config_entry
        with patch.object(namespace['dt_util'], 'now', return_value=NOW):
            restarted._restore_recent_battery_cycle_state()
        self.assertEqual(restarted._battery_grid_charge_price, .25)
        saved = json.dumps(restored, sort_keys=True)
        del restarted.hass
        for _ in range(3):
            replay(restarted, None)
        self.assertEqual(json.dumps(restored, sort_keys=True), saved)
        _, _, after = replay(restarted, 39)
        self.assertEqual(after.battery_strategy, baseline.battery_strategy)
        self.assertEqual(after.planned_battery_mode_windows, baseline.planned_battery_mode_windows)
        self.assertEqual(after.estimated_battery_mode_windows, baseline.estimated_battery_mode_windows)
