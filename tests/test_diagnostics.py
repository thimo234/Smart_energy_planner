"""Diagnostics contracts with real-shaped doubles, not an HA integration test."""
from copy import deepcopy
from datetime import datetime, timezone
import json
from types import SimpleNamespace as NS
import unittest
from unittest.mock import AsyncMock

from test_support import install_package_stub
install_package_stub()
from custom_components.smart_energy_planner.const import DOMAIN, RUNTIME_STATE
from custom_components.smart_energy_planner.diagnostics import async_get_config_entry_diagnostics


def entry(identity='home', kind='household', **data):
    return NS(entry_id=identity, data={'planner_kind': kind, **data}, options={}, subentries={})


def child(identity='child', **data):
    return NS(subentry_id=identity, subentry_type='battery', title='PRIVATE TITLE', data=data)


def margin():
    return {'at': '2026-11-14T10:00:00+01:00', 'energy': 7.5, 'reserve': 2.,
            'credit': -.42, 'lots': [[3.2, .31], [2.3, None]],
            'actions': [{'start': '2026-11-14T10:00:00+01:00',
                         'end': '2026-11-14T11:00:00+01:00', 'mode': 'ontladen',
                         'solar_kw': 0., 'grid_kw': 0., 'power': 1.,
                         'sale_price': .26, 'import_price': .29, 'token': 'SECRET'}],
            'password': 'SECRET'}


def coordinator():
    return NS(data=NS(status='ready', battery_strategy='idle', battery_soc_percent=75.,
                     battery_charge_cycle_active=False, battery_discharge_cycle_active=True,
                     planned_battery_mode_windows=[{'start': datetime(2026, 11, 14, tzinfo=timezone.utc),
                         'end': '2026-11-14T11:00:00+01:00', 'mode': 'accu_uit',
                         'expected_demand_kwh': 1.5, 'token': 'SECRET'}],
                     next_discharge_window_start=None, current_price=.29,
                     current_export_price=.18, source_errors=['SECRET sensor.private'],
                     secret='SECRET'), last_update_success=True,
              _charge_session_started=False, _discharge_session_started=True,
              _battery_cycle_discharge_floor_kwh=1.25,
              _battery_cycle_state_initialized=True, _battery_cycle_state_restored_recent=True,
              _battery_grid_charge_price=.31, _battery_margin_state=margin(),
              _active_charge_phase_end=datetime(2026, 11, 14, tzinfo=timezone.utc),
              _active_charge_phase_mode='accu_uit', async_refresh=AsyncMock())


class DiagnosticsTest(unittest.IsolatedAsyncioTestCase):
    def fixture(self):
        home = entry(price_sensor='sensor.private', export_price_tax_deduction=.11,
                     password='SECRET')
        home.options = {'export_price_tax_deduction': 0.}
        home.subentries = {'child': child(_legacy_entry_id='legacy', battery_capacity_kwh=10.,
                                  battery_min_profit_per_kwh=.08, export_price_tax_deduction=.99)}
        coord = coordinator()
        runtime = {'battery_cycle_state': {'initialized': True, 'discharge_floor_kwh': 2.,
                       'charge_session_started': False, 'discharge_session_started': True,
                       'updated_at': '2026-11-14T10:00:00+01:00', 'token': 'SECRET'},
                   'battery_grid_charge_price': .33, 'battery_margin_state': margin(),
                   'expected_hourly_demand_stats': {'private': 'SECRET'}, 'token': 'SECRET'}
        hass = NS(data={DOMAIN: {'home': {'child': coord}, 'legacy': coord,
                                'unrelated': coordinator()},
                        RUNTIME_STATE: {'legacy': runtime, 'child': {'battery_grid_charge_price': 999.}},
                        'unrelated': {'secret': 'SECRET'}})
        return hass, home, coord, runtime

    async def test_household_uses_stable_identity_and_parent_effective_policy(self):
        hass, home, coord, runtime = self.fixture()
        result = await async_get_config_entry_diagnostics(hass, home)
        self.assertEqual(result['entry_id'], 'home')
        self.assertEqual(len(result['planners']), 1)
        report = result['planners'][0]
        self.assertEqual(report['planner_entry_id'], 'legacy')
        self.assertEqual(report['subentry_id'], 'child')
        self.assertEqual(report['effective_config']['battery_capacity_kwh'], 10.)
        self.assertEqual(report['effective_config']['battery_min_profit_per_kwh'], .08)
        self.assertEqual(report['effective_config']['export_price_tax_deduction'], 0.)
        self.assertEqual(report['runtime_state']['battery_grid_charge_price'], .33)
        self.assertEqual(report['runtime_state']['battery_margin_state']['lots']['items'], [[3.2, .31], [2.3, None]])
        self.assertEqual(report['runtime_state']['battery_margin_state']['credit'], -.42)
        self.assertEqual(report['runtime_state']['battery_cycle_state']['discharge_floor_kwh'], 2.)
        self.assertEqual(report['coordinator_state']['battery_cycle_discharge_floor_kwh'], 1.25)
        self.assertTrue(report['coordinator_state']['discharge_session_started'])
        self.assertEqual(report['coordinator_state']['battery_grid_charge_price'], .31)
        self.assertEqual(report['coordinator_state']['battery_margin_state']['actions']['items'][0]['sale_price'], .26)
        self.assertIsNone(report['plan']['next_discharge_window_start'])
        self.assertEqual(report['plan']['planned_battery_mode_windows']['items'][0]['mode'], 'accu_uit')
        self.assertEqual(report['plan']['current_export_price'], .18)
        self.assertTrue(report['plan']['battery_discharge_cycle_active'])

    async def test_standalone_options_and_untyped_legacy_default(self):
        coord = coordinator()
        old = entry('legacy', 'battery', battery_min_soc_percent=15.)
        old.data.pop('planner_kind')
        old.options = {'battery_capacity_kwh': 12., 'battery_min_profit_per_kwh': .12}
        hass = NS(data={DOMAIN: {'legacy': coord}})
        report = (await async_get_config_entry_diagnostics(hass, old))['planners'][0]
        self.assertEqual(report['planner_entry_id'], 'legacy')
        self.assertIsNone(report['subentry_id'])
        self.assertEqual(report['effective_config']['planner_kind'], 'battery')
        self.assertEqual(report['effective_config']['battery_capacity_kwh'], 12.)
        self.assertEqual(report['effective_config']['battery_min_profit_per_kwh'], .12)
        self.assertEqual(report['effective_config']['battery_no_charge_min_soc_percent'], 15.)
        self.assertIsNone(report['runtime_state'])

    async def test_failed_child_does_not_export_live_standalone_alias_or_state(self):
        hass, home, coord, runtime = self.fixture()
        hass.data[DOMAIN]['home'].clear()  # adoption failed; legacy owner remains live
        report = (await async_get_config_entry_diagnostics(hass, home))['planners'][0]
        self.assertFalse(report['coordinator_available'])
        self.assertIsNone(report['coordinator_state'])
        self.assertIsNone(report['runtime_state'])
        self.assertIsNone(report['plan'])

    async def test_mismatched_parent_and_stable_alias_is_not_owned(self):
        hass, home, coord, runtime = self.fixture()
        hass.data[DOMAIN]['legacy'] = coordinator()
        report = (await async_get_config_entry_diagnostics(hass, home))['planners'][0]
        self.assertFalse(report['coordinator_available'])
        self.assertIsNone(report['runtime_state'])

    async def test_safe_detached_json_and_no_refresh_or_state_mutation(self):
        hass, home, coord, runtime = self.fixture()
        before_data, before_options = deepcopy(home.data), deepcopy(home.options)
        before_runtime, before_margin = deepcopy(runtime), deepcopy(coord._battery_margin_state)
        report = await async_get_config_entry_diagnostics(hass, home)
        encoded = json.dumps(report, allow_nan=False)
        for forbidden in ('SECRET', 'sensor.private', 'PRIVATE TITLE', 'unrelated', 'expected_hourly_demand_stats'):
            self.assertNotIn(forbidden, encoded)
        self.assertEqual(home.data, before_data)
        self.assertEqual(home.options, before_options)
        self.assertEqual(runtime, before_runtime)
        self.assertEqual(coord._battery_margin_state, before_margin)
        report['planners'][0]['runtime_state']['battery_margin_state']['lots']['items'][0][0] = 999
        report['planners'][0]['coordinator_state']['battery_margin_state']['actions']['items'][0]['sale_price'] = 999
        self.assertEqual(runtime, before_runtime)
        self.assertEqual(coord._battery_margin_state, before_margin)
        coord.async_refresh.assert_not_awaited()
        self.assertIn('2026-11-14T00:00:00+00:00', encoded)

    async def test_real_slotted_window_and_grid_charge_cost_projection(self):
        from custom_components.smart_energy_planner.price_models import PlannerWindow
        hass, home, coord, runtime = self.fixture()
        coord.data.upcoming_energy_price_windows = [PlannerWindow(
            datetime(2026, 11, 14, tzinfo=timezone.utc),
            datetime(2026, 11, 15, tzinfo=timezone.utc), .29, False)]
        coord.data.planned_grid_charge_windows = [{'start': '2026-11-14T11:00:00+01:00',
            'end': '2026-11-14T12:00:00+01:00', 'average_price': .314}]
        report = (await async_get_config_entry_diagnostics(hass, home))['planners'][0]
        prices = report['plan']['upcoming_energy_price_windows']['items']
        self.assertEqual(prices[0]['price'], .29)
        self.assertFalse(prices[0]['price_known'])
        self.assertEqual(prices[0]['start'], '2026-11-14T00:00:00+00:00')
        charges = report['plan']['planned_grid_charge_windows']['items']
        self.assertEqual(charges[0]['average_price'], .314)
        json.dumps(report, allow_nan=False)

    async def test_sibling_failure_and_nonlegacy_child_identity_are_isolated(self):
        hass, home, coord, runtime = self.fixture()
        home.subentries['new-child'] = child('new-child', battery_capacity_kwh=22.)
        home.subentries['failed-child'] = child('failed-child', battery_capacity_kwh=7.)
        second = coordinator()
        hass.data[DOMAIN]['home']['new-child'] = second
        hass.data[DOMAIN]['new-child'] = second
        hass.data[RUNTIME_STATE]['new-child'] = {'battery_grid_charge_price': .44}
        reports = (await async_get_config_entry_diagnostics(hass, home))['planners']
        self.assertEqual(len(reports), 3)
        by_subentry = {report['subentry_id']: report for report in reports}
        self.assertEqual(by_subentry['child']['planner_entry_id'], 'legacy')
        self.assertEqual(by_subentry['new-child']['planner_entry_id'], 'new-child')
        self.assertEqual(by_subentry['new-child']['effective_config']['battery_capacity_kwh'], 22.)
        self.assertEqual(by_subentry['new-child']['runtime_state']['battery_grid_charge_price'], .44)
        self.assertFalse(by_subentry['failed-child']['coordinator_available'])
        self.assertIsNone(by_subentry['failed-child']['runtime_state'])

    async def test_optional_weather_source_presence_is_redacted(self):
        for weather, sensor in [('weather.private', {'entity_id':'sensor.private'}), (None, None)]:
            old = entry('demand', 'demand', weather_entity=weather,
                        outdoor_temperature_sensor=sensor)
            result = await async_get_config_entry_diagnostics(NS(data={}), old)
            config = result['planners'][0]['effective_config']
            self.assertEqual(config['weather_entity'], '**REDACTED**' if weather else None)
            self.assertEqual(config['outdoor_temperature_sensor'], '**REDACTED**' if sensor else None)
            self.assertNotIn('private', json.dumps(result))

    async def test_missing_data_is_read_only(self):
        hass, old = NS(data={}), entry('legacy', 'battery')
        report = await async_get_config_entry_diagnostics(hass, old)
        self.assertFalse(report['planners'][0]['coordinator_available'])
        self.assertEqual(hass.data, {})
        json.dumps(report, allow_nan=False)

    async def test_bounded_projection_and_non_json_values_never_stringified(self):
        hass, home, coord, runtime = self.fixture()
        coord._battery_margin_state['actions'] *= 600
        coord.data.current_price = float('nan')
        coord.data.battery_soc_percent = object()
        home.subentries['child'].data['battery_capacity_kwh'] = {'token': 'SECRET'}
        result = await async_get_config_entry_diagnostics(hass, home)
        report = result['planners'][0]
        actions = report['coordinator_state']['battery_margin_state']['actions']
        self.assertTrue(actions['truncated'])
        self.assertEqual(actions['total'], 600)
        self.assertLess(len(actions['items']), 600)
        self.assertIsNone(report['plan']['current_price'])
        self.assertIsNone(report['plan']['battery_soc_percent'])
        self.assertIsNone(report['effective_config']['battery_capacity_kwh'])
        self.assertNotIn('SECRET', json.dumps(result, allow_nan=False))
