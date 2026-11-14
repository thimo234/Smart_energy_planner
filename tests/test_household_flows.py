"""Execute full flow/lifecycle modules against explicit HA 2026.10-shaped doubles.

These are contract tests, not a replacement for an installed-HA integration run.
"""
import importlib.util
from enum import StrEnum
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace as NS
import unittest
from unittest.mock import AsyncMock, Mock, patch

from test_household import entry, sub
from test_support import install_package_stub
install_package_stub()
from custom_components.smart_energy_planner.const import DOMAIN, SHARED_SOURCE_KEYS
from custom_components.smart_energy_planner.const import RUNTIME_STATE
from custom_components.smart_energy_planner.household import async_setup_platform

ROOT = Path(__file__).resolve().parents[1] / 'custom_components/smart_energy_planner'

class SensorClass(StrEnum):
    ENERGY = 'energy'
    BATTERY = 'battery'

class EnergyUnit(StrEnum):
    KILO_WATT_HOUR = 'kWh'


class Marker:
    def __init__(self, schema, default=None):
        self.schema, self.default = schema, default


class Schema:
    def __init__(self, schema):
        self.schema = schema


class Selector:
    def __init__(self, config=None, **kwargs):
        self.config = config if config is not None else kwargs


class Flow:
    def __init_subclass__(cls, **kwargs):
        pass

    @property
    def _subentry_type(self):
        return self.handler[1]

    def _get_entry(self):
        return self.hass.config_entries.async_get_entry(self.handler[0])

    def _get_reconfigure_subentry(self):
        return self._get_entry().subentries[self.context['subentry_id']]

    def async_show_form(self, **kwargs):
        return {'type': 'form', **kwargs}

    def async_abort(self, **kwargs):
        return {'type': 'abort', **kwargs}

    def async_create_entry(self, **kwargs):
        return {'type': 'create_entry', **kwargs}

    def async_update_and_abort(self, parent, child, **kwargs):
        for key, value in kwargs.items():
            setattr(child, key, value)
        return {'type': 'abort', 'reason': 'reconfigure_successful'}


def module(name, **attrs):
    obj = ModuleType(name)
    obj.__dict__.update(attrs)
    return obj


def load_flow():
    selectors = module('homeassistant.helpers.selector')
    selectors.__getattr__ = lambda name: Selector
    selectors.NumberSelectorMode = NS(BOX='box', SLIDER='slider')
    selectors.SelectSelectorMode = NS(DROPDOWN='dropdown')
    doubles = {
        'voluptuous': module('voluptuous', Schema=Schema, Required=Marker, Optional=Marker),
        'homeassistant.components.sensor': module('sensor', SensorDeviceClass=SensorClass),
        'homeassistant.config_entries': module('entries', ConfigFlow=Flow, OptionsFlow=Flow,
                                                ConfigFlowResult=dict, ConfigSubentryFlow=Flow),
        'homeassistant.const': module('const', UnitOfEnergy=EnergyUnit),
        'homeassistant.core': module('core', HomeAssistant=object),
        'homeassistant.helpers': module('helpers', selector=selectors),
        'homeassistant.helpers.selector': selectors,
    }
    with patch.dict(sys.modules, doubles):
        spec = importlib.util.spec_from_file_location(
            'custom_components.smart_energy_planner._flow_contract_test', ROOT / 'config_flow.py')
        obj = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(obj)
    return obj


class FlowContractTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.module = load_flow()
        self.home = entry(price_sensor='sensor.price', total_energy_sensor='sensor.house',
                          solcast_today_sensor='sensor.sun')
        self.home.add_update_listener = Mock()
        self.old = entry('old', 'battery', price_sensor='sensor.price',
                         total_energy_sensor='sensor.house', solcast_today_sensor='sensor.sun',
                         battery_capacity_kwh=10, battery_soc_sensor='sensor.soc')
        states = [NS(entity_id='sensor.house', state='100', attributes={
            'unit_of_measurement': 'kWh', 'state_class': 'total_increasing', 'device_class': 'energy'})]
        self.hass = NS(states=NS(async_all=lambda *a: states,
            get=lambda key: next((s for s in states if s.entity_id == key), None)),
            config_entries=NS(async_entries=lambda domain: [self.home, self.old],
                async_get_entry=lambda identity: self.home if identity == 'home' else self.old,
                async_update_entry=Mock()))

    def flow(self, kind='battery'):
        flow = self.module.PlannerSubentryFlow()
        flow.hass, flow.handler, flow.context = self.hass, ('home', kind), {}
        return flow

    async def test_household_create_and_options_share_the_same_source_schema(self):
        flow = self.module.SmartEnergyPlannerConfigFlow()
        flow.hass = self.hass
        form = await flow.async_step_user()
        keys = {key.schema for key in form['data_schema'].schema}
        self.assertEqual(keys, set(SHARED_SOURCE_KEYS) | {'planner_name'})
        result = await flow.async_step_user({'planner_name': 'House', 'price_sensor': 'sensor.price'})
        self.assertEqual(result['data']['planner_kind'], 'household')
        self.assertEqual(set(flow.async_get_supported_subentry_types(self.home)), {'battery','price_window','demand'})
        self.assertEqual(flow.async_get_supported_subentry_types(self.old), {})
        options = self.module.SmartEnergyPlannerOptionsFlow()
        options.hass, options.config_entry = self.hass, self.home
        form = await options.async_step_init()
        self.assertEqual(keys, {key.schema for key in form['data_schema'].schema})
        saved = await options.async_step_init({'planner_name': 'Renamed', 'price_sensor': 'sensor.other'})
        self.assertEqual(saved['data']['price_sensor'], 'sensor.other')

    async def test_household_options_can_clear_optional_sources_from_initial_data(self):
        options = self.module.SmartEnergyPlannerOptionsFlow()
        options.hass, options.config_entry = self.hass, self.home
        result = await options.async_step_init({'planner_name': 'House', 'price_sensor': 'sensor.price'})
        self.home.options = result['data']
        from custom_components.smart_energy_planner.household import merged_data
        self.assertIsNone(merged_data(self.home)['solcast_today_sensor'])

    async def test_all_child_forms_exclude_shared_sources(self):
        for kind in ('battery','price_window','demand'):
            flow = self.flow(kind)
            result = await flow.async_step_user({'legacy_entry_id': 'new'})
            keys = {key.schema for key in result['data_schema'].schema}
            self.assertFalse(keys & SHARED_SOURCE_KEYS, keys)
            self.assertIn('planner_name', keys)

    async def test_new_price_child_and_edit_preserve_policy_without_shared_copies(self):
        flow = self.flow('price_window')
        result = await flow.async_step_configure({'planner_name': 'Five hours',
            'price_window_duration_hours': 5, 'price_sensor': 'stale'})
        self.assertEqual(result['type'], 'create_entry')
        self.assertEqual(result['data']['price_window_duration_hours'], 5)
        self.assertNotIn('price_sensor', result['data'])
        self.home.subentries['a'] = sub('a', 'price_window', **result['data'])
        flow = self.flow('price_window')
        flow.context = {'subentry_id': 'a'}
        await flow.async_step_reconfigure()
        result = await flow.async_step_configure({'planner_name': 'Three hours', 'price_window_duration_hours': 3})
        self.assertEqual(result['reason'], 'reconfigure_successful')
        self.assertEqual(self.home.subentries['a'].data['price_window_duration_hours'], 3)

    async def test_adoption_requires_explicit_confirmation_and_blocks_source_conflicts(self):
        flow = self.flow()
        result = await flow.async_step_user({'legacy_entry_id': 'old'})
        self.assertEqual(result['step_id'], 'adopt')
        self.assertEqual((await flow.async_step_adopt({'confirm': False}))['type'], 'form')
        self.old.options['price_sensor'] = 'sensor.other'
        result = await flow.async_step_adopt({'confirm': True})
        self.assertEqual(result['reason'], 'shared_source_conflict')
        self.assertIn('price_sensor', result['description_placeholders']['sources'])
        self.old.options.clear()
        result = await flow.async_step_adopt({'confirm': True})
        self.assertEqual(result['data']['_legacy_entry_id'], 'old')
        self.assertNotIn('price_sensor', result['data'])
        self.hass.config_entries.async_update_entry.assert_not_called()

    async def test_demand_child_uses_shared_total_and_retains_device_wizard(self):
        flow = self.flow('demand')
        result = await flow.async_step_configure({'planner_name': 'Consumption', 'demand_energy_sensors': []})
        self.assertEqual(result['type'], 'create_entry')
        self.assertEqual(result['data']['demand_devices'], [])
        self.assertNotIn('total_energy_sensor', result['data'])

    async def test_edited_battery_keeps_adoption_identity_and_rejects_self_forecast(self):
        child = sub('a', _legacy_entry_id='old', battery_capacity_kwh=10)
        self.home.subentries['a'] = child
        flow = self.flow()
        flow.context = {'subentry_id': 'a'}
        await flow.async_step_reconfigure()
        record = NS(unique_id='old_battery_strategy')
        registry = module('homeassistant.helpers.entity_registry', async_get=lambda hass: NS(async_get=lambda entity: record))
        helpers = module('homeassistant.helpers', entity_registry=registry)
        with patch.dict(sys.modules, {'homeassistant.helpers': helpers, 'homeassistant.helpers.entity_registry': registry}):
            rejected = await flow.async_step_configure({'planner_name': 'Battery', 'demand_forecast_sensor': 'sensor.own'})
        self.assertEqual(rejected['errors']['base'], 'self_forecast')
        result = await flow.async_step_configure({'planner_name': 'Battery', 'battery_capacity_kwh': 12})
        self.assertEqual(result['reason'], 'reconfigure_successful')
        self.assertEqual(child.data['_legacy_entry_id'], 'old')
        self.assertEqual(child.data['battery_capacity_kwh'], 12)


class PlatformContractTest(unittest.IsolatedAsyncioTestCase):
    async def test_parent_cannot_register_another_coordinators_identity(self):
        home = entry()
        home.subentries = {'a': sub('a')}
        hass = NS(data={DOMAIN: {'home': {'a': object()}, 'a': object()}})
        setup, add = AsyncMock(), Mock()
        await async_setup_platform(hass, home, add, setup)
        setup.assert_not_awaited()
        add.assert_not_called()

    async def test_one_child_platform_failure_does_not_skip_other_children(self):
        home = entry()
        home.subentries = {'a': sub('a'), 'b': sub('b')}
        children = {'a': object(), 'b': object()}
        hass = NS(data={DOMAIN: {'home': children, **children}})
        add = Mock()
        async def setup(hass, binding, add):
            if binding.entry_id == 'a':
                raise ValueError('invalid entity')
            add(['b'])
        with self.assertLogs('custom_components.smart_energy_planner.household', 'ERROR'):
            await async_setup_platform(hass, home, add, setup)
        add.assert_called_once_with(['b'], config_subentry_id='b')

    async def test_add_registration_and_remove_keep_subentry_ownership(self):
        home = entry()
        home.subentries = {'a': sub('a'), 'b': sub('b', 'price_window'), 'bad': sub('bad')}
        children = {'a': object(), 'b': object()}
        hass = NS(data={DOMAIN: {'home': children, **children}})
        add = Mock()
        async def setup(hass, binding, register):
            register([binding.entry_id])
        await async_setup_platform(hass, home, add, setup)
        self.assertEqual([c.kwargs for c in add.call_args_list],
                         [{'config_subentry_id': 'a'}, {'config_subentry_id': 'b'}])
        del home.subentries['a']
        add.reset_mock()
        await async_setup_platform(hass, home, add, setup)
        add.assert_called_once_with(['b'], config_subentry_id='b')


class CoordinatorDouble:
    def __init__(self, hass, binding):
        if binding.entry_id == 'broken':
            raise ValueError('one child has invalid configuration')
        self.config_entry = getattr(binding, 'parent', binding)
        self.planner_entry = binding
        self.data = None
        self.async_request_refresh = AsyncMock()
        self._cancel_source_error_retry = Mock()
        self.async_shutdown = AsyncMock()

    async def async_refresh(self):
        if self.planner_entry.entry_id != 'unready':
            self.data = NS(source_errors=[])


def load_runtime():
    events = module('events', async_call_later=Mock(return_value=Mock()),
        async_track_state_change_event=Mock(return_value=Mock()),
        async_track_time_interval=Mock(return_value=Mock()))
    dr = module('dr', async_get=lambda hass: NS(async_get_or_create=Mock()))
    er = module('er', async_get=lambda hass: NS(entities={}))
    helpers = module('helpers', device_registry=dr, entity_registry=er)
    doubles = {
        'homeassistant.components.http': module('http', StaticPathConfig=Mock()),
        'homeassistant.config_entries': module('entries', ConfigEntry=object, ConfigEntryNotReady=RuntimeError),
        'homeassistant.const': module('const', EVENT_HOMEASSISTANT_STARTED='started',
                                     Platform=NS(SENSOR='sensor', BINARY_SENSOR='binary_sensor')),
        'homeassistant.core': module('core', CoreState=NS(running='running'), Event=object,
            EventStateChangedData=object, HomeAssistant=object, callback=lambda func: func),
        'homeassistant.helpers': helpers,
        'homeassistant.helpers.event': events,
        'homeassistant.helpers.storage': module('storage', Store=Mock()),
        'custom_components.smart_energy_planner.coordinator': module('coordinator',
            SmartEnergyPlannerCoordinator=CoordinatorDouble),
    }
    spec = importlib.util.spec_from_file_location(
        'custom_components.smart_energy_planner._runtime_contract_test', ROOT / '__init__.py', submodule_search_locations=None)
    spec.submodule_search_locations = None
    obj = importlib.util.module_from_spec(spec)
    obj.__package__ = 'custom_components.smart_energy_planner'
    with patch.dict(sys.modules, doubles):
        spec.loader.exec_module(obj)
    obj._async_register_frontend = AsyncMock()
    obj._async_load_runtime_state = AsyncMock(return_value={
        'battery_profit_total_eur': 44, 'battery_cycle_state': {'cycles': 7},
        'expected_hourly_demand_stats': {'00': {'count': 11}}, 'demand_devices_state': {'meter': 123}})
    return obj, doubles, events


class RuntimeContractTest(unittest.IsolatedAsyncioTestCase):
    async def test_failed_adoption_preserves_live_legacy_identity_and_runtime(self):
        for reason in ('source_conflict', 'unload_failed'):
            with self.subTest(reason=reason):
                runtime, doubles, _ = load_runtime()
                home = entry(price_sensor='sensor.price')
                home.subentries = {'a': sub('a', _legacy_entry_id='old')}
                home.async_on_unload = Mock()
                home.add_update_listener = Mock()
                old = entry('old', 'battery', price_sensor=(
                    'sensor.other' if reason == 'source_conflict' else 'sensor.price'))
                old_data = dict(old.data)
                live = NS(data=NS(source_errors=[]),
                          _cancel_source_error_retry=Mock(), async_shutdown=AsyncMock())
                state = {'battery_profit_total_eur': 44, 'battery_cycle_state': {'cycles': 7}}
                hass = NS(data={DOMAIN: {'old': live}, RUNTIME_STATE: {'old': state}},
                    config_entries=NS(async_get_entry=lambda identity: old,
                        async_unload=AsyncMock(return_value=False), async_update_entry=Mock(),
                        async_forward_entry_setups=AsyncMock()))
                with patch.dict(sys.modules, doubles), self.assertLogs(runtime._LOGGER, 'ERROR'):
                    self.assertTrue(await runtime.async_setup_entry(hass, home))
                self.assertIs(hass.data[DOMAIN].get('old'), live)
                self.assertIs(hass.data[RUNTIME_STATE].get('old'), state)
                self.assertEqual(state, {'battery_profit_total_eur': 44,
                                         'battery_cycle_state': {'cycles': 7}})
                self.assertEqual(old.data, old_data)
                self.assertEqual(hass.data[DOMAIN]['home'], {})
                live._cancel_source_error_retry.assert_not_called()
                live.async_shutdown.assert_not_awaited()
                runtime._async_load_runtime_state.assert_not_awaited()
                hass.config_entries.async_update_entry.assert_not_called()
                # Both entity platforms must skip the unadopted live identity.
                for platform in ('sensor', 'binary_sensor'):
                    with self.subTest(platform=platform):
                        setup, add = AsyncMock(), Mock()
                        await async_setup_platform(hass, home, add, setup)
                        setup.assert_not_awaited()
                        add.assert_not_called()
                if reason == 'source_conflict':
                    hass.config_entries.async_unload.assert_not_awaited()
                else:
                    hass.config_entries.async_unload.assert_awaited_once_with('old')

    async def test_late_child_setup_failure_cleans_only_new_child(self):
        runtime, doubles, events = load_runtime()
        home = entry()
        home.subentries = {'a': sub('a')}
        home.async_on_unload = Mock()
        home.add_update_listener = Mock()
        live = NS(_cancel_source_error_retry=Mock(), async_shutdown=AsyncMock())
        state = {'battery_profit_total_eur': 99}
        hass = NS(data={DOMAIN: {'standalone': live}, RUNTIME_STATE: {'standalone': state}},
            state='starting', bus=NS(async_listen_once=Mock(return_value=Mock())),
            config_entries=NS(async_forward_entry_setups=AsyncMock()))
        failed = CoordinatorDouble(hass, self.module_binding(home))
        events.async_track_time_interval.side_effect = ValueError('late setup failure')
        with patch.dict(sys.modules, doubles), \
             patch.object(runtime, 'SmartEnergyPlannerCoordinator', return_value=failed), \
             self.assertLogs(runtime._LOGGER, 'ERROR'):
            self.assertTrue(await runtime.async_setup_entry(hass, home))
        self.assertNotIn('a', hass.data[DOMAIN])
        self.assertNotIn('a', hass.data[RUNTIME_STATE])
        self.assertEqual(hass.data[DOMAIN]['home'], {})
        failed._cancel_source_error_retry.assert_called_once_with()
        failed.async_shutdown.assert_awaited_once_with()
        self.assertIs(hass.data[DOMAIN]['standalone'], live)
        self.assertIs(hass.data[RUNTIME_STATE]['standalone'], state)
        live._cancel_source_error_retry.assert_not_called()
        live.async_shutdown.assert_not_awaited()

    async def test_failed_initial_refresh_shuts_down_only_that_coordinator(self):
        runtime, _, _ = load_runtime()
        home = entry()
        binding = self.module_binding(home)
        failed = NS(async_refresh=AsyncMock(side_effect=ValueError('refresh failed')),
                    async_shutdown=AsyncMock())
        hass = NS(data={})
        with patch.object(runtime, 'SmartEnergyPlannerCoordinator', return_value=failed):
            with self.assertRaisesRegex(ValueError, 'refresh failed'):
                await runtime._async_setup_planner(hass, binding, object())
        failed.async_shutdown.assert_awaited_once()
        self.assertNotIn('a', hass.data.get(DOMAIN, {}))

    @staticmethod
    def module_binding(home):
        from custom_components.smart_energy_planner.household import PlannerBinding
        return PlannerBinding(home, sub('a'))

    async def test_unloading_archive_does_not_remove_active_adopted_child(self):
        runtime, _, _ = load_runtime()
        archive = entry('old', 'battery', _adopted_by={'entry_id': 'home', 'subentry_id': 'a'})
        child = object()
        hass = NS(data={DOMAIN: {'old': child}, RUNTIME_STATE: {'old': {'profit': 44}}},
                  config_entries=NS(async_unload_platforms=AsyncMock(return_value=True)))
        self.assertTrue(await runtime.async_unload_entry(hass, archive))
        self.assertIs(hass.data[DOMAIN]['old'], child)
        self.assertEqual(hass.data[RUNTIME_STATE]['old'], {'profit': 44})
        hass.config_entries.async_unload_platforms.assert_not_awaited()

    async def test_six_children_isolate_unready_source_and_setup_failure_then_reload_remove(self):
        runtime, doubles, events = load_runtime()
        home = entry(price_sensor='sensor.price', total_energy_sensor='sensor.house')
        home.subentries = {'battery': sub('battery', battery_soc_sensor='sensor.soc'),
            'three': sub('three','price_window', price_window_duration_hours=3),
            'one': sub('one','price_window', price_window_duration_hours=1),
            'expensive': sub('expensive','price_window', price_window_mode='expensive'),
            'unready': sub('unready','price_window', price_window_duration_hours=5),
            'demand': sub('demand','demand'), 'broken': sub('broken','battery')}
        cleanups = []
        home.async_on_unload = cleanups.append
        home.add_update_listener = Mock(return_value=Mock())
        tasks = []
        hass = NS(data={}, state='starting', bus=NS(async_listen_once=Mock(return_value=Mock())),
            async_create_task=tasks.append, config_entries=NS(
                async_get_entry=lambda identity: None,
                async_forward_entry_setups=AsyncMock(), async_unload_platforms=AsyncMock(return_value=True),
                async_reload=AsyncMock()))
        with patch.dict(sys.modules, doubles), self.assertLogs(runtime._LOGGER, 'ERROR'):
            self.assertTrue(await runtime.async_setup_entry(hass, home))
        self.assertEqual(set(hass.data[DOMAIN]['home']), set(home.subentries) - {'broken'})
        self.assertIsNone(hass.data[DOMAIN]['unready'].data)
        self.assertEqual(hass.data[RUNTIME_STATE]['battery']['battery_profit_total_eur'], 44)
        self.assertEqual(hass.data[RUNTIME_STATE]['demand']['demand_devices_state'], {'meter': 123})
        self.assertEqual(runtime._async_load_runtime_state.await_count, 7)
        # A single parent subscription dispatches only to interested children.
        self.assertEqual(events.async_track_state_change_event.call_count, 1)
        source_changed = events.async_track_state_change_event.call_args.args[2]
        source_changed(NS(data={'entity_id': 'sensor.soc'}))
        self.assertEqual(len(tasks), 1)
        await tasks.pop()
        source_changed(NS(data={'entity_id': 'sensor.price'}))
        self.assertEqual(len(tasks), 6)
        for task in tasks:
            await task
        tasks.clear()
        await runtime._async_update_listener(hass, home)
        hass.config_entries.async_reload.assert_awaited_once_with('home')
        self.assertTrue(await runtime.async_unload_entry(hass, home))
        self.assertEqual(hass.data[DOMAIN], {})
        self.assertEqual(hass.data[RUNTIME_STATE], {})
        # Actual reload after removing a child and changing shared sources.
        del home.subentries['three']
        del home.subentries['broken']
        home.options['price_sensor'] = 'sensor.new_price'
        with patch.dict(sys.modules, doubles):
            self.assertTrue(await runtime.async_setup_entry(hass, home))
        self.assertNotIn('three', hass.data[DOMAIN])
        for coordinator in hass.data[DOMAIN]['home'].values():
            self.assertIn('sensor.new_price', coordinator.source_entities)
            self.assertNotIn('sensor.price', coordinator.source_entities)
        self.assertTrue(cleanups)

    async def test_archived_entry_sets_up_inert_without_duplicate_coordinator(self):
        runtime, _, _ = load_runtime()
        archive = entry('old', 'battery', _adopted_by='home')
        hass = NS(data={})
        self.assertTrue(await runtime.async_setup_entry(hass, archive))
        self.assertEqual(hass.data, {})
        runtime._async_load_runtime_state.assert_not_awaited()
