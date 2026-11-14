"""Household composition and adoption contracts, without a Home Assistant install."""
import asyncio
from copy import deepcopy
import json
from types import SimpleNamespace as NS
import unittest
from unittest.mock import AsyncMock, Mock

from test_support import install_package_stub
install_package_stub()
from custom_components.smart_energy_planner.household import (
    PlannerBinding, SharedSources, adoption_conflicts, async_adopt_legacy,
    child_data, planner_bindings,
)
from custom_components.smart_energy_planner.const import DOMAIN, SHARED_SOURCE_KEYS


def entry(identity='home', kind='household', **data):
    return NS(entry_id=identity, title=identity, data={'planner_kind': kind, **data},
              options={}, subentries={}, async_on_unload=Mock(), disabled_by=None)


def sub(identity='child', kind='battery', **data):
    return NS(subentry_id=identity, subentry_type=kind, title=identity, data=data)


class CompositionTest(unittest.TestCase):
    def test_explicit_zero_tax_and_untyped_legacy_battery_are_not_erased(self):
        home = entry(export_price_tax_deduction=.11)
        old = entry('legacy', 'battery', export_price_tax_deduction=0)
        self.assertIn('export_price_tax_deduction', adoption_conflicts(home, old))
        old.data.pop('planner_kind')
        self.assertIn('export_price_tax_deduction', adoption_conflicts(home, old))

    def test_parent_sources_authoritative_and_child_policy_independent(self):
        home = entry(price_sensor='sensor.price', total_energy_sensor='sensor.house')
        home.options = {'price_sensor': 'sensor.changed'}
        home.subentries = {'a': sub('a', battery_capacity_kwh=10, price_sensor='stale'),
                           'b': sub('b', 'price_window', price_window_duration_hours=5)}
        a, b = planner_bindings(home)
        self.assertEqual(a.data['price_sensor'], 'sensor.changed')
        self.assertEqual(b.data['price_sensor'], 'sensor.changed')
        self.assertEqual(a.data['battery_capacity_kwh'], 10)
        self.assertEqual(b.data['price_window_duration_hours'], 5)
        self.assertNotIn('price_sensor', child_data(a.data))
        self.assertEqual(a.parent, home)

    def test_legacy_identity_survives_json_restart_and_removal(self):
        home = entry(price_sensor='sensor.price')
        payload = json.loads(json.dumps({'_legacy_entry_id': 'old-battery', 'battery_capacity_kwh': 10}))
        home.subentries = {'a': sub('a', **payload), 'b': sub('b', 'demand')}
        a, b = planner_bindings(home)
        self.assertEqual(a.entry_id, 'old-battery')
        self.assertEqual(a.subentry_id, 'a')
        self.assertEqual(b.entry_id, 'b')
        del home.subentries['a']
        self.assertEqual([p.entry_id for p in planner_bindings(home)], ['b'])
        self.assertEqual(home.data['price_sensor'], 'sensor.price')

    def test_adoption_reports_conflicts_instead_of_overwriting_sources(self):
        home = entry(price_sensor='sensor.a', total_energy_sensor='sensor.house')
        old = entry('legacy', 'battery', price_sensor={'entity_id': 'sensor.a'},
                    total_energy_sensor='sensor.other', solcast_today_sensor='sensor.sun')
        self.assertEqual(set(adoption_conflicts(home, old)), {'total_energy_sensor', 'solcast_today_sensor'})
        self.assertEqual(old.data['total_energy_sensor'], 'sensor.other')

    def test_conflict_defaults_match_runtime_export_tariff(self):
        home = entry(price_sensor='sensor.a', export_price_tax_deduction=.11)
        old = entry('legacy', 'price_window', price_sensor='sensor.a')
        self.assertEqual(adoption_conflicts(home, old), {})


class SharedSourcesTest(unittest.IsolatedAsyncioTestCase):
    async def test_identical_source_work_is_coalesced_and_errors_not_cached(self):
        shared = SharedSources()
        calls = []
        async def load():
            calls.append(1)
            await asyncio.sleep(0)
            return {'energy': [1]}
        first, second = await asyncio.gather(shared.async_cached('history', load),
                                             shared.async_cached('history', load))
        first['energy'].append(2)
        self.assertEqual(second, {'energy': [1]})
        self.assertEqual(len(calls), 1)
        shared.invalidate()
        await shared.async_cached('history', load)
        self.assertEqual(len(calls), 2)
        bad = AsyncMock(side_effect=ValueError('broken'))
        for _ in range(2):
            with self.assertRaises(ValueError):
                await shared.async_cached('bad', bad)
        self.assertEqual(bad.await_count, 2)


class AdoptionTest(unittest.IsolatedAsyncioTestCase):
    async def test_interrupted_registry_move_resumes_without_unloading_twice(self):
        hass, home, old, child, registry = self.fixture()
        update = registry.async_update_entity.side_effect
        registry.async_update_entity.side_effect = RuntimeError('interrupted')
        with self.assertRaises(RuntimeError):
            await async_adopt_legacy(hass, home, child, registry, NS(devices={}))
        self.assertIn('_adopted_by', old.data)
        self.assertEqual(registry.entities['sensor.strategy'].config_entry_id, 'legacy')
        registry.async_update_entity.side_effect = update
        await async_adopt_legacy(hass, home, child, registry, NS(devices={}))
        hass.config_entries.async_unload.assert_awaited_once_with('legacy')
        self.assertEqual(registry.entities['sensor.strategy'].config_entry_id, 'home')

    def fixture(self):
        home = entry(price_sensor='sensor.price')
        old = entry('legacy', 'battery', price_sensor='sensor.price')
        child = sub('child', _legacy_entry_id='legacy')
        home.subentries = {'child': child}
        entities = {'sensor.strategy': NS(entity_id='sensor.strategy', config_entry_id='legacy',
                         config_subentry_id=None, unique_id='legacy_battery_strategy')}
        def update(entity_id, **kwargs):
            for key, value in kwargs.items():
                setattr(entities[entity_id], key, value)
        registry = NS(entities=entities, async_update_entity=Mock(side_effect=update))
        def update_entry(target, **kwargs):
            for key, value in kwargs.items():
                setattr(target, key, value)
        manager = NS(async_get_entry=lambda identity: old if identity == 'legacy' else home,
                     async_entries=lambda domain: [home, old],
                     async_unload=AsyncMock(return_value=True),
                     async_update_entry=Mock(side_effect=update_entry))
        hass = NS(config_entries=manager, data={})
        return hass, home, old, child, registry

    async def test_adoption_moves_registry_not_unique_ids_or_persistent_state(self):
        hass, home, old, child, registry = self.fixture()
        await async_adopt_legacy(hass, home, child, registry, NS(devices={}))
        record = registry.entities['sensor.strategy']
        self.assertEqual(record.config_entry_id, 'home')
        self.assertEqual(record.config_subentry_id, 'child')
        self.assertEqual(record.unique_id, 'legacy_battery_strategy')
        self.assertEqual(old.data['_adopted_by'], {'entry_id': 'home', 'subentry_id': 'child'})
        self.assertEqual(old.data['price_sensor'], 'sensor.price')
        self.assertEqual(PlannerBinding(home, child).entry_id, 'legacy')
        await async_adopt_legacy(hass, home, child, registry, NS(devices={}))
        self.assertEqual(registry.async_update_entity.call_count, 1)

    async def test_failed_unload_does_not_move_or_mark_legacy(self):
        hass, home, old, child, registry = self.fixture()
        hass.config_entries.async_unload.return_value = False
        with self.assertRaises(ValueError):
            await async_adopt_legacy(hass, home, child, registry, NS(devices={}))
        registry.async_update_entity.assert_not_called()
        self.assertNotIn('_adopted_by', old.data)

    async def test_sources_changed_after_confirmation_are_rechecked(self):
        hass, home, old, child, registry = self.fixture()
        home.options = {'price_sensor': 'different'}
        with self.assertRaises(ValueError):
            await async_adopt_legacy(hass, home, child, registry, NS(devices={}))
        hass.config_entries.async_unload.assert_not_called()

    async def test_adopted_source_changes_and_serialized_restart_are_idempotent(self):
        hass, home, old, child, registry = self.fixture()
        await async_adopt_legacy(hass, home, child, registry, NS(devices={}))
        old.data = json.loads(json.dumps(old.data))
        home.options = {'price_sensor': 'new-price'}
        await async_adopt_legacy(hass, home, child, registry, NS(devices={}))
        self.assertEqual(PlannerBinding(home, child).data['price_sensor'], 'new-price')

    async def test_same_legacy_cannot_be_claimed_by_another_household(self):
        hass, home, old, child, registry = self.fixture()
        other = entry('other')
        other.subentries = {'different': sub('different')}
        entries = {'home': home, 'legacy': old, 'other': other}
        hass.config_entries.async_get_entry = entries.get
        hass.config_entries.async_entries = lambda domain: list(entries.values())
        old.data['_adopted_by'] = {'entry_id': 'other', 'subentry_id': 'different'}
        with self.assertRaises(ValueError):
            await async_adopt_legacy(hass, home, child, registry, NS(devices={}))
        registry.async_update_entity.assert_not_called()
