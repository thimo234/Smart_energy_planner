"""Removed planner entries must never start another controller by accident."""
import ast
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from datetime import datetime

from test_battery_energy_accounting import coordinator, replay_full_plan
from custom_components.smart_energy_planner import const

ROOT = Path(__file__).resolve().parents[1] / "custom_components/smart_energy_planner"


def load_function(filename, name, **namespace):
    tree = ast.parse((ROOT / filename).read_text(encoding="utf-8-sig"))
    function = next(node for node in ast.walk(tree)
                    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name)
    scope = {**vars(const), **namespace}
    module = ast.Module(body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), function], type_ignores=[])
    exec(compile(ast.fix_missing_locations(module), filename, "exec"), scope)
    return scope[name]


class PlannerTypesTest(unittest.IsolatedAsyncioTestCase):
    async def test_missing_price_sensor_refresh_does_not_use_removed_controller(self):
        for kind in ("battery", "price_window"):
            instance = coordinator()
            instance.config_entry = SimpleNamespace(
                data={"planner_kind": kind, "price_sensor": "sensor.prices"},
                options={}, entry_id="test",
            )
            instance.hass = SimpleNamespace(data={}, states=SimpleNamespace(
                get=lambda entity_id: None, async_all=lambda: [],
            ))
            instance._schedule_source_error_retry = Mock()
            instance._cancel_source_error_retry = Mock()
            clock = instance._async_update_data.__globals__["dt_util"]
            with patch.object(clock, "now", return_value=datetime.fromisoformat("2026-10-16T10:00:00+02:00")):
                result = await instance._async_update_data()
            self.assertNotEqual(result.status, "planner_runtime_error", result.source_errors)
            self.assertEqual(result.planner_kind, kind)
            self.assertNotIn("room_temperature_sensor", result.source_status)

    async def test_removed_entry_is_rejected_before_resources_or_control_start(self):
        setup = load_function("__init__.py", "async_setup_entry", _LOGGER=Mock())
        for data, options in [({"planner_kind": "thermostat"}, {}),
                              ({"planner_kind": "battery"}, {"planner_kind": "thermostat"})]:
            # No HA methods are available: rejection must happen before any side effects.
            self.assertFalse(await setup(SimpleNamespace(), SimpleNamespace(data=data, options=options)))

    async def test_removed_flow_and_options_abort_without_battery_fallback(self):
        flow = SimpleNamespace(context={"planner_kind": "thermostat"},
                               config_entry=SimpleNamespace(data={"planner_kind": "thermostat"}, options={}),
                               async_abort=lambda **kw: kw)
        for name in ("async_step_user", "async_step_configure", "async_step_init"):
            method = load_function("config_flow.py", name)
            result = await method(flow, {"planner_kind": "thermostat"})
            self.assertEqual(result, {"reason": "unsupported_planner_kind"})

    def test_price_window_planning_survives_without_temperature_inputs(self):
        instance = coordinator()
        original = instance._build_plan_for_horizon

        def price_window_plan(**inputs):
            return original(**{**inputs, "planner_kind": "price_window"})

        instance._build_plan = price_window_plan
        _, _, result = replay_full_plan(instance=instance, isolated_cycles=True)
        self.assertEqual(result.planner_kind, "price_window")
        self.assertIsNotNone(result.selected_price_window)
        self.assertFalse(result.planned_battery_mode_windows)
        self.assertFalse(hasattr(result, "thermostat_setpoint_c"))
