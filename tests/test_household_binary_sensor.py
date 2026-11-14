"""Exercise real price-window entity lifecycle with HA boundary doubles."""
from datetime import datetime, timedelta, timezone
import importlib.util
import sys
from types import SimpleNamespace as NS
import unittest
from unittest.mock import Mock, patch

from test_household import entry, sub
from test_household_flows import ROOT, module
from custom_components.smart_energy_planner.household import async_setup_platform
from custom_components.smart_energy_planner.const import DOMAIN


class CoordinatorEntityDouble:
    def __class_getitem__(cls, item):
        return cls

    def __init__(self, coordinator):
        self.coordinator = coordinator
        self.removers = []
        self.async_write_ha_state = Mock()

    async def async_added_to_hass(self):
        self.coordinator.listener = self._handle_coordinator_update

    def async_on_remove(self, remove):
        self.removers.append(remove)

    @property
    def available(self):
        return self.coordinator.last_update_success

    def _handle_coordinator_update(self):
        self.async_write_ha_state()


class HouseholdBinarySensorTest(unittest.IsolatedAsyncioTestCase):
    async def test_unready_child_registers_then_recovers_boundary_scheduling(self):
        now = datetime(2026, 11, 14, 12, tzinfo=timezone.utc)
        track = Mock(side_effect=lambda *args: Mock())
        doubles = {
            'homeassistant.components.binary_sensor': module('binary_sensor', BinarySensorEntity=object),
            'homeassistant.config_entries': module('entries', ConfigEntry=object),
            'homeassistant.core': module('core', HomeAssistant=object, callback=lambda f: f),
            'homeassistant.helpers.event': module('event', async_track_point_in_time=track),
            'homeassistant.helpers.entity_platform': module('platform', AddEntitiesCallback=object),
            'homeassistant.helpers.update_coordinator': module('update', CoordinatorEntity=CoordinatorEntityDouble),
            'homeassistant.util': module('util', dt=NS(now=lambda: now)),
            'custom_components.smart_energy_planner.coordinator': module('coordinator', SmartEnergyPlannerCoordinator=object),
        }
        spec = importlib.util.spec_from_file_location(
            'custom_components.smart_energy_planner._binary_lifecycle_test', ROOT / 'binary_sensor.py')
        assert spec is not None and spec.loader is not None
        sensor_module = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, doubles):
            spec.loader.exec_module(sensor_module)
        home = entry()
        home.subentries = {'price': sub('price', 'price_window')}
        coordinator = NS(data=None, last_update_success=False, _config={'planner_kind': 'price_window'})
        hass = NS(data={DOMAIN: {'home': {'price': coordinator}, 'price': coordinator}})
        add = Mock()
        await async_setup_platform(hass, home, add, sensor_module._async_setup_planner_binary_sensors)
        add.assert_called_once()
        self.assertEqual(add.call_args.kwargs, {'config_subentry_id': 'price'})
        sensor = add.call_args.args[0][0]
        sensor.hass = hass
        await sensor.async_added_to_hass()
        self.assertFalse(sensor.available)
        self.assertFalse(sensor.is_on)
        self.assertEqual(sensor.extra_state_attributes, {})
        track.assert_not_called()
        start, end = now + timedelta(minutes=10), now + timedelta(minutes=30)
        coordinator.data = NS(price_window_type='cheapest', planner_kind='price_window',
            selected_price_window={'start': start, 'end': end}, source_status={}, source_errors=[])
        coordinator.last_update_success = True
        coordinator.listener()
        self.assertTrue(sensor.available)
        self.assertEqual(track.call_args.args, (hass, sensor._boundary_reached, start))
        first_unsub = sensor._boundary_unsub
        coordinator.listener()
        first_unsub.assert_called_once_with()
        now = start
        sensor._boundary_reached(now)
        self.assertTrue(sensor.is_on)
        self.assertEqual(track.call_args.args[2], end)
        self.assertEqual(sensor.async_write_ha_state.call_count, 3)
        last_unsub = sensor._boundary_unsub
        for remove in sensor.removers:
            remove()
        last_unsub.assert_called_once_with()
        self.assertIsNone(sensor._boundary_unsub)
