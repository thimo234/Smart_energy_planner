"""Adoption/removal regressions against HA-shaped doubles, not installed HA."""
import asyncio
from copy import deepcopy
import json
from pathlib import Path
import sys
from types import SimpleNamespace as NS
import unittest
from unittest.mock import AsyncMock, Mock, patch

import test_household as household_tests
from test_household import entry, sub
from test_household_flows import load_runtime, ROOT
from custom_components.smart_energy_planner.household import async_adopt_legacy, PlannerBinding
from custom_components.smart_energy_planner.const import DOMAIN, RUNTIME_STATE


# Ownership-related statements copied from HA Core 2026.11.1:
# homeassistant/helpers/entity_registry.py::_validate_item / async_device_modified.
# Keep these checks independent of the adoption implementation. The device event
# carries OLD ownership and is delivered later, including after an exception.
# https://github.com/home-assistant/core/blob/2026.11.1/homeassistant/helpers/entity_registry.py
_UNDEFINED = object()


def ha_validate_ownership(hass, *, config_entry_id=None, config_subentry_id=None,
                          device_id=None, old_config_subentry_id=None):
    if config_entry_id and config_entry_id is not _UNDEFINED:
        if not hass.config_entries.async_get_entry(config_entry_id):
            raise ValueError(
                f"Can't link entity to unknown config entry {config_entry_id}"
            )
    if (
        config_entry_id
        and config_entry_id is not _UNDEFINED
        and old_config_subentry_id
        and config_subentry_id is _UNDEFINED
    ):
        raise ValueError("Can't change config entry without changing subentry")
    if (
        config_entry_id
        and config_entry_id is not _UNDEFINED
        and config_subentry_id
        and config_subentry_id is not _UNDEFINED
    ):
        if (
            not (config_entry := hass.config_entries.async_get_entry(config_entry_id))
            or config_subentry_id not in config_entry.subentries
        ):
            raise ValueError(
                f"Config entry {config_entry_id} has no subentry {config_subentry_id}"
            )
    if device_id and device_id is not _UNDEFINED:
        if hass.devices.async_get(device_id, include_composite_devices=False) is None:
            raise ValueError(f"Device {device_id} does not exist")


def ha_device_ownership_event(registry, event):
    """HA's update ownership-cleanup branch (names adapted, conditions intact)."""
    device = registry.hass.devices.async_get(event.data['device_id'])
    if not device:
        return
    changes = event.data['changes']
    if 'config_entry_id' in changes:
        old_config_entry_id = changes['config_entry_id']
        entities = [entity for entity in registry.entities.values()
                    if entity.device_id == event.data['device_id']]
        for entity in entities:
            if (
                entity.config_entry_id == old_config_entry_id
                and entity.config_entry_id != device.config_entry_id
            ):
                registry.async_remove(entity.entity_id)
    if 'config_subentry_id' in changes:
        old_config_subentry_id = changes['config_subentry_id']
        entities = [entity for entity in registry.entities.values()
                    if entity.device_id == event.data['device_id']]
        for entity in entities:
            if (
                entity.config_entry_id == device.config_entry_id
                and entity.config_subentry_id == old_config_subentry_id
                and entity.config_subentry_id != device.config_subentry_id
            ):
                registry.async_remove(entity.entity_id)


class LabelsTest(unittest.TestCase):
    def test_menu_uses_official_subentry_translation_keys(self):
        for filename in ('strings.json', 'translations/en.json', 'translations/nl.json'):
            document = json.loads((ROOT / filename).read_text())
            for kind in ('battery', 'price_window', 'demand'):
                with self.subTest(filename=filename, kind=kind):
                    translation = document['config_subentries'][kind]
                    self.assertTrue(translation.get('entry_type'))
                    self.assertTrue(translation.get('initiate_flow', {}).get('user'))


class AdoptionLifecycleTest(unittest.IsolatedAsyncioTestCase):
    def fixture(self):
        hass, home, old, child, registry = household_tests.AdoptionTest().fixture()
        hass.data = {}
        hass.config_entries.async_entries = lambda domain: [home, old]
        device = NS(id='battery-device', config_entry_id='legacy', config_subentry_id=None)
        def move(identity, **kwargs):
            self.assertEqual(identity, device.id)
            device.config_entry_id = kwargs['new_config_entry_id']
            device.config_subentry_id = kwargs['new_config_subentry_id']
        devices = NS(devices={device.id: device}, async_update_device=Mock(side_effect=move))
        return hass, home, old, child, registry, devices, device

    async def test_devices_move_before_archive_can_be_deleted_and_resume(self):
        hass, home, old, child, registry, devices, device = self.fixture()
        await async_adopt_legacy(hass, home, child, registry, devices)
        self.assertEqual((device.config_entry_id, device.config_subentry_id), ('home', 'child'))
        self.assertEqual(registry.entities['sensor.strategy'].unique_id, 'legacy_battery_strategy')
        await async_adopt_legacy(hass, home, child, registry, devices)
        devices.async_update_device.assert_called_once()
        hass.config_entries.async_unload.assert_awaited_once()

    async def test_configured_claim_blocks_missing_archive_duplicate_even_if_owner_not_loaded(self):
        hass, home, old, child, registry, devices, device = self.fixture()
        duplicate = sub('duplicate', _legacy_entry_id='legacy')
        other = entry('other')
        other.subentries = {'duplicate': duplicate}
        hass.config_entries.async_entries = lambda domain: [home, other]
        hass.config_entries.async_get_entry = lambda identity: {'home': home, 'other': other}.get(identity)
        with self.assertRaisesRegex(ValueError, 'claimed'):
            await async_adopt_legacy(hass, home, child, registry, devices)
        hass.config_entries.async_unload.assert_not_awaited()
        hass.config_entries.async_update_entry.assert_not_called()
        devices.async_update_device.assert_not_called()

    async def test_active_marker_owner_is_not_silently_stolen(self):
        hass, home, old, child, registry, devices, device = self.fixture()
        owner = entry('other')
        owner.subentries = {'existing': sub('existing')}
        old.data['_adopted_by'] = {'entry_id': 'other', 'subentry_id': 'existing'}
        lookup = hass.config_entries.async_get_entry
        hass.config_entries.async_get_entry = lambda identity: owner if identity == 'other' else lookup(identity)
        with self.assertRaisesRegex(ValueError, 'already adopted'):
            await async_adopt_legacy(hass, home, child, registry, devices)
        devices.async_update_device.assert_not_called()

    async def test_removed_child_archive_can_be_recovered_without_restart_or_state_reset(self):
        hass, home, old, child, registry, devices, device = self.fixture()
        old.data['_adopted_by'] = {'entry_id': 'home', 'subentry_id': 'removed'}
        await async_adopt_legacy(hass, home, child, registry, devices)
        self.assertEqual(old.data['_adopted_by'], {'entry_id': 'home', 'subentry_id': 'child'})
        hass.config_entries.async_unload.assert_not_awaited()
        self.assertEqual(PlannerBinding(home, child).entry_id, 'legacy')
        registry.async_update_entity.assert_called_once_with('sensor.strategy', config_entry_id='home',
                                                             config_subentry_id='child')

    async def test_interrupted_device_move_resumes_with_archive_marker_intact(self):
        hass, home, old, child, registry, devices, device = self.fixture()
        move = devices.async_update_device.side_effect
        devices.async_update_device.side_effect = RuntimeError('interrupted')
        with self.assertRaises(RuntimeError):
            await async_adopt_legacy(hass, home, child, registry, devices)
        self.assertIn('_adopted_by', old.data)
        devices.async_update_device.side_effect = move
        await async_adopt_legacy(hass, home, child, registry, devices)
        self.assertEqual(device.config_entry_id, 'home')
        hass.config_entries.async_unload.assert_awaited_once()

    async def test_missing_archive_does_not_allow_a_second_live_controller(self):
        hass, home, old, child, registry, devices, _ = self.fixture()
        hass.config_entries.async_get_entry = lambda identity: None
        live = object()
        hass.data = {DOMAIN: {'legacy': live}}
        with self.assertRaises(ValueError):
            await async_adopt_legacy(hass, home, child, registry, devices)
        self.assertIs(hass.data[DOMAIN]['legacy'], live)


class InterruptedOwnershipTransferTest(unittest.IsolatedAsyncioTestCase):
    def fixture(self):
        hass, home, old, child, registry = household_tests.AdoptionTest().fixture()
        hass.data[RUNTIME_STATE] = {'legacy': {
            'cycle': 'discharge', 'started_at': '2026-11-14T17:00:00+01:00',
            'remaining_kwh': 3.25, 'history': [{'strategy': 'charge'}]}}
        devices = NS(devices={})
        for identity in ('battery-device', 'aux-device'):
            devices.devices[identity] = NS(
                id=identity, config_entry_id='legacy', config_subentry_id=None,
                disabled=False, disabled_by=None)
        devices.async_get = lambda identity, **kwargs: devices.devices.get(identity)
        hass.devices = devices
        hass.config_entries.async_get_entry = {'home': home, 'legacy': old}.get
        registry.hass = hass
        registry.event_errors = []
        registry.entities.clear()
        for index, device_id in enumerate(('battery-device', 'battery-device', 'aux-device')):
            identity = f'sensor.custom_{index}'
            registry.entities[identity] = NS(
                entity_id=identity, unique_id=f'legacy_strategy_{index}',
                config_entry_id='legacy', config_subentry_id=None, device_id=device_id,
                name=f'My strategy {index}', icon='mdi:battery', area_id='garage',
                labels={'energy'}, disabled_by='user' if index == 1 else None,
                options={'sensor': {'display_precision': 2}})
        registry.async_remove = Mock(side_effect=registry.entities.pop)

        def update_entity(identity, **kwargs):
            record = registry.entities[identity]
            ha_validate_ownership(
                hass, config_entry_id=kwargs.get('config_entry_id', _UNDEFINED),
                config_subentry_id=kwargs.get('config_subentry_id', _UNDEFINED),
                device_id=kwargs.get('device_id', _UNDEFINED),
                old_config_subentry_id=record.config_subentry_id)
            for key, value in kwargs.items():
                setattr(record, key, value)

        def deliver_device_event(event):
            try:
                ha_device_ownership_event(registry, event)
            except Exception as err:
                # call_soon exceptions otherwise only get logged, not failed.
                registry.event_errors.append(err)

        def update_device(identity, **kwargs):
            device = devices.devices[identity]
            changes = {}
            for key in ('config_entry_id', 'config_subentry_id'):
                value = kwargs[f'new_{key}']
                if getattr(device, key) != value:
                    changes[key] = getattr(device, key)
                    setattr(device, key, value)
            asyncio.get_running_loop().call_soon(
                deliver_device_event,
                NS(data={'action': 'update', 'device_id': identity, 'changes': changes}))

        registry.async_update_entity = Mock(side_effect=update_entity)
        devices.async_update_device = Mock(side_effect=update_device)
        return hass, home, old, child, registry, devices

    async def test_partial_transfer_keeps_original_records_through_event_cleanup_and_retry(self):
        # Includes an error AFTER mutation: successful writes cannot be assumed
        # rolled back merely because an update call raised. Disabled entries count.
        for kind in ('entity', 'device'):
            for fail_at, after_write in ((1, False), (2, False), (1, True), (2, True)):
                with self.subTest(kind=kind, fail_at=fail_at, after_write=after_write):
                    hass, home, old, child, registry, devices = self.fixture()
                    records = deepcopy({key: vars(value) for key, value in registry.entities.items()})
                    runtime = deepcopy(hass.data[RUNTIME_STATE])
                    updater = (registry.async_update_entity if kind == 'entity'
                               else devices.async_update_device)
                    original = updater.side_effect
                    calls = 0

                    def interrupt(*args, **kwargs):
                        nonlocal calls
                        calls += 1
                        if calls == fail_at and not after_write:
                            raise RuntimeError('interrupted transfer')
                        original(*args, **kwargs)
                        if calls == fail_at:
                            raise RuntimeError('interrupted transfer')

                    updater.side_effect = interrupt
                    with self.assertRaisesRegex(RuntimeError, 'interrupted transfer'):
                        await async_adopt_legacy(hass, home, child, registry, devices)
                    # Let the real event-loop deliver queued HA cleanup BEFORE
                    # retrying: a deleted record cannot recover customizations.
                    await asyncio.sleep(0)
                    self.assertEqual(registry.event_errors, [])
                    registry.async_remove.assert_not_called()
                    self.assertEqual(set(registry.entities), set(records))
                    self.assertEqual(old.data['_adopted_by'],
                                     {'entry_id': 'home', 'subentry_id': 'child'})
                    self.assertEqual(hass.data[RUNTIME_STATE], runtime)
                    updater.side_effect = original
                    await async_adopt_legacy(hass, home, child, registry, devices)
                    await asyncio.sleep(0)
                    # A second completed retry must be a no-op, too.
                    moves = (registry.async_update_entity.call_count,
                             devices.async_update_device.call_count)
                    await async_adopt_legacy(hass, home, child, registry, devices)
                    await asyncio.sleep(0)
                    self.assertEqual(moves, (registry.async_update_entity.call_count,
                                             devices.async_update_device.call_count))
                    self.assertEqual(registry.event_errors, [])
                    registry.async_remove.assert_not_called()
                    for identity, before in records.items():
                        self.assertEqual(vars(registry.entities[identity]),
                                         {**before, 'config_entry_id': 'home',
                                          'config_subentry_id': 'child'})
                    for device in devices.devices.values():
                        self.assertEqual((device.config_entry_id, device.config_subentry_id),
                                         ('home', 'child'))
                    self.assertEqual(hass.data[RUNTIME_STATE], runtime)
                    self.assertEqual(PlannerBinding(home, child).entry_id, 'legacy')
                    hass.config_entries.async_unload.assert_awaited_once_with('legacy')


class RecoveryFlowTest(unittest.IsolatedAsyncioTestCase):
    async def test_orphan_archive_requires_confirmation_and_keeps_stable_identity(self):
        from test_household_flows import load_flow
        flow = load_flow().PlannerSubentryFlow()
        home = entry(price_sensor='sensor.price')
        old = entry('legacy', 'battery', price_sensor='sensor.price',
                    _adopted_by={'entry_id': 'missing', 'subentry_id': 'removed'})
        entries = {'home': home, 'legacy': old}
        flow.hass = NS(config_entries=NS(async_entries=lambda domain: list(entries.values()),
                       async_get_entry=entries.get))
        flow.handler, flow.context = ('home', 'battery'), {}
        result = await flow.async_step_user({'legacy_entry_id': 'legacy'})
        self.assertEqual(result['step_id'], 'adopt')
        result = await flow.async_step_adopt({'confirm': True})
        self.assertEqual(result['data']['_legacy_entry_id'], 'legacy')
        self.assertNotIn('_adopted_by', result['data'])
        # An archive still owned by a configured child must never be offered.
        home.subentries['active'] = sub('active', _legacy_entry_id='legacy')
        self.assertEqual(flow._legacy_choices(), [])


class UnloadLifecycleTest(unittest.IsolatedAsyncioTestCase):
    async def test_removed_child_unload_clears_runtime_ownership_not_persisted_state(self):
        for household in (False, True):
            with self.subTest(household=household):
                runtime, _, _ = load_runtime()
                runtime._runtime_store = Mock()
                home = entry() if household else entry('legacy', 'battery')
                # HA removes the subentry before the update-listener reload.
                home.subentries = {}
                coordinator = NS(planner_entry=NS(entry_id='legacy'),
                    _cancel_source_error_retry=Mock(), async_shutdown=AsyncMock())
                planners = {'legacy': coordinator}
                if household:
                    planners['home'] = {'child': coordinator}
                hass = NS(data={DOMAIN: planners, RUNTIME_STATE: {'legacy': {'cycle': 'discharge'}}},
                    config_entries=NS(async_unload_platforms=AsyncMock(return_value=True)))
                self.assertTrue(await runtime.async_unload_entry(hass, home))
                self.assertEqual(hass.data[DOMAIN], {})
                self.assertEqual(hass.data[RUNTIME_STATE], {})
                runtime._runtime_store.assert_not_called()
                # HA's DataUpdateCoordinator registers shutdown with async_on_unload;
                # the component must not invent a subentry removal hook or erase disk.

    async def test_failed_unload_keeps_ownership_and_does_not_shutdown(self):
        runtime, _, _ = load_runtime()
        coordinator = NS(async_shutdown=AsyncMock())
        hass = NS(data={DOMAIN: {'home': {'child': coordinator}, 'legacy': coordinator}},
            config_entries=NS(async_unload_platforms=AsyncMock(return_value=False)))
        self.assertFalse(await runtime.async_unload_entry(hass, entry()))
        self.assertIs(hass.data[DOMAIN]['legacy'], coordinator)
        coordinator.async_shutdown.assert_not_awaited()

    async def test_removed_child_archive_stays_inert_and_can_be_deleted(self):
        runtime, _, _ = load_runtime()
        archive = entry('legacy', 'battery', _adopted_by={'entry_id': 'home', 'subentry_id': 'gone'})
        hass = NS(data={})
        self.assertTrue(await runtime.async_setup_entry(hass, archive))
        self.assertTrue(await runtime.async_unload_entry(hass, archive))
        self.assertEqual(hass.data, {})
        runtime._async_load_runtime_state.assert_not_awaited()
