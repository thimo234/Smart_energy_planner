"""The standalone predictor feeds battery planning without learning twice."""
import asyncio
from dataclasses import asdict
from datetime import timedelta
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
import sys
from unittest.mock import patch, AsyncMock, Mock

from test_demand_forecast import fixtures
from test_battery_energy_accounting import replay_full_plan, coordinator
from custom_components.smart_energy_planner.demand_forecast import build_demand_forecast
from custom_components.smart_energy_planner.demand_coordinator import async_demand_result
from custom_components.smart_energy_planner import demand_coordinator
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from visualize_battery import plot_data


def demand_plan(*, inconsistent_meters=False):
    now, histories, prices, devices = fixtures()
    if inconsistent_meters:
        for entity, states in histories.items():
            if entity != 'house':
                for state in states:
                    state.state = str(float(state.state)*2)
    forecast, _, _ = build_demand_forecast(histories, devices, total_entity='house', now=now, prices=prices)
    data = dict(estimated_hourly_home_demand=forecast, upcoming_energy_price_windows=[
        dict(start=p.start.isoformat(), end=p.end.isoformat(), price=p.price, price_known=True) for p in prices],
        estimated_hourly_solar_forecast=[dict(start=p.start.isoformat(), end=p.end.isoformat(), estimated_kwh=0)
                                       for p in prices])
    with patch.object(Path, 'read_text', return_value=json.dumps(data)):
        _, _, result = replay_full_plan(timestamp=now.isoformat(), soc=60, discharging=True,
                                       isolated_cycles=True, external_demand=forecast, demand_margin=20)
    return now, forecast, result


class DemandIntegrationTest(unittest.TestCase):
    def setUp(self):
        # A real registry always implements both async_get entry points.
        # Keep this module runnable independently of test_sensor's HA stubs.
        import types
        sys.modules.setdefault('homeassistant.helpers.entity_registry',
                               types.ModuleType('homeassistant.helpers.entity_registry'))
        from homeassistant.helpers import entity_registry
        registry = SimpleNamespace(async_get=lambda entity_id: None)
        lookup = patch.object(entity_registry, 'async_get', return_value=registry, create=True)
        lookup.start()
        self.addCleanup(lookup.stop)

    def test_waiting_forecast_preserves_other_live_sources_without_commands(self):
        now, _, prices, _=fixtures()
        c=coordinator()
        c.config_entry=SimpleNamespace(entry_id='battery',options={},data=dict(
            planner_kind='battery',demand_forecast_sensor='forecast',price_sensor='prices',
            solcast_today_sensor='sun',battery_soc_sensor='soc',battery_capacity_kwh=10,
            battery_min_soc_percent=20))
        states={'forecast':SimpleNamespace(state='unknown',attributes={}),
                'soc':SimpleNamespace(state='65',attributes={}),
                'sun':SimpleNamespace(state='2',attributes={'detailedHourly':[
                    dict(period_start=(now+timedelta(hours=12)).isoformat(),pv_estimate=2)]}),
                'prices':SimpleNamespace(state='.4',attributes={'raw_today':[
                    dict(start=p.start.isoformat(),end=p.end.isoformat(),value=p.price) for p in prices]})}
        c.hass=SimpleNamespace(states=SimpleNamespace(get=states.get,async_all=lambda:[]))
        c._schedule_source_error_retry=Mock()
        c._cancel_source_error_retry=Mock()
        c._async_get_average_daily_usage=AsyncMock(side_effect=AssertionError('must not learn on wait'))
        with patch.object(c._async_update_data.__globals__['dt_util'],'now',return_value=now):
            result=asyncio.run(c._async_update_data())
        self.assertEqual(result.status,'waiting_for_demand_forecast',result.source_errors)
        self.assertEqual(result.battery_soc_percent,65)
        self.assertEqual(result.battery_total_energy_kwh,6.5)
        self.assertEqual(result.current_price,.4)
        self.assertTrue(result.upcoming_energy_price_windows)
        self.assertTrue(result.estimated_hourly_solar_forecast)
        self.assertFalse(result.estimated_hourly_home_demand)
        self.assertFalse(result.planned_battery_mode_windows)
        c._async_get_average_daily_usage.assert_not_called()

    def test_recorder_statistics_query_requests_hourly_reset_adjusted_kwh(self):
        now, _, _, _=fixtures()
        query=Mock(return_value={'sensor.meter':[]})
        with patch.dict(sys.modules, {'homeassistant.components.recorder.statistics':
                                     SimpleNamespace(statistics_during_period=query)}):
            result=demand_coordinator.load_hourly_statistics('hass',['sensor.meter'],now)
        query.assert_called_once_with('hass',now-timedelta(days=28,hours=1),now,
                                      {'sensor.meter'},'hour',{'energy':'kWh'},{'sum'})
        self.assertEqual(result,{'sensor.meter':[]})

    def test_selected_forecast_skips_internal_learning_in_coordinator(self):
        now, histories, prices, devices = fixtures()
        forecast, _, _ = build_demand_forecast(histories, devices, total_entity='house', now=now, prices=prices)
        c=coordinator()
        c.config_entry=SimpleNamespace(entry_id='battery', options={}, data=dict(
            planner_kind='battery', price_sensor='prices', battery_soc_sensor='soc',
            demand_forecast_sensor='forecast', total_energy_sensor='house', battery_enabled=True))
        states={'forecast':SimpleNamespace(state='13.8',attributes=dict(forecast_version=1,
            forecast_generated_at=now.isoformat(), unit_of_measurement='kWh', estimated_hourly_home_demand=forecast)),
            'soc':SimpleNamespace(state='60',attributes={}),
            'prices':SimpleNamespace(state='.4',attributes={'raw_today':[
                dict(start=p.start.isoformat(),end=p.end.isoformat(),value=p.price) for p in prices]})}
        c.hass=SimpleNamespace(data={},states=SimpleNamespace(get=states.get,async_all=lambda:[]))
        c._schedule_source_error_retry=Mock()
        c._cancel_source_error_retry=Mock()
        c._async_persist_runtime_state=AsyncMock()
        c._async_update_battery_profit_tracking=AsyncMock()
        c._async_get_average_daily_usage=AsyncMock(side_effect=AssertionError('internal learning called'))
        ns=c._async_update_data.__globals__
        with patch.object(ns['dt_util'],'now',return_value=now), patch.dict(ns, {
            'build_hourly_home_demand_forecast':Mock(side_effect=AssertionError('internal forecast called')),
            'align_price_responsive_demand_to_cheap_hours':Mock(side_effect=AssertionError('demand shifted twice'))}):
            result=asyncio.run(c._async_update_data())
        self.assertNotEqual(result.status,'planner_runtime_error',result.source_errors)
        self.assertEqual(result.source_status['demand_forecast_sensor'],'ok')
        c._async_get_average_daily_usage.assert_not_called()

    def test_external_intervals_retained_with_margin_once(self):
        now, forecast, result = demand_plan()
        self.assertEqual(result.source_status['demand_forecast_sensor'], 'ok')
        for source, actual in zip(forecast, result.estimated_hourly_home_demand):
            self.assertEqual(source['start'], actual['start'])
            self.assertEqual(source['end'], actual['end'])
            self.assertAlmostEqual(actual['estimated_kwh'], source['estimated_kwh']*1.2, places=3)
        snapshot = asdict(result)
        snapshot.update(battery_capacity_kwh=10, battery_min_soc_percent=20)
        trace = plot_data(snapshot, 3, 3, 'Independent forecast')['soc']
        self.assertGreaterEqual(min(v for _,v in trace),19.95)
        self.assertLessEqual(max(v for _,v in trace),100.05)

    def test_missing_selected_forecast_waits_instead_of_zero_demand(self):
        c = coordinator()
        c.config_entry = SimpleNamespace(entry_id='battery', data={'demand_forecast_sensor':'sensor.missing'}, options={})
        c.hass = SimpleNamespace(states=SimpleNamespace(get=lambda entity: None, async_all=lambda: []))
        c._schedule_source_error_retry=Mock()
        c._cancel_source_error_retry=Mock()
        with patch.object(c._async_update_data.__globals__['dt_util'],'now',return_value=fixtures()[0]):
            result = asyncio.run(c._async_update_data())
        self.assertEqual(result.status, 'waiting_for_demand_forecast')
        self.assertFalse(result.planned_battery_mode_windows)
        self.assertTrue(any('forecast_unavailable' in e for e in result.source_errors))

    def test_recorder_refreshes_on_completed_hour_boundary(self):
        now,histories,_,devices=fixtures()
        now=now+timedelta(minutes=50)
        c=coordinator()
        c.config_entry=SimpleNamespace(data={'demand_devices':devices,'total_energy_sensor':'house'},options={})
        c.hass=SimpleNamespace(states=SimpleNamespace(get=lambda entity:histories[entity][-1]))
        async def load(job):
            return job()
        with patch.object(demand_coordinator,'get_instance',return_value=SimpleNamespace(async_add_executor_job=load)), \
             patch.object(demand_coordinator,'load_hourly_statistics',return_value={}), \
             patch.object(demand_coordinator.history,'get_significant_states',return_value=histories,create=True) as query:
            asyncio.run(async_demand_result(c,now))
            asyncio.run(async_demand_result(c,now+timedelta(minutes=15)))
            self.assertEqual(query.call_count,2)

    def test_completed_observations_override_overlapping_provider_forecast(self):
        now = fixtures()[0].replace(hour=10, minute=30)
        start = now.replace(hour=0, minute=0)
        observed = {start+timedelta(hours=h): 5 for h in range(10)}
        c = coordinator()
        c.config_entry = SimpleNamespace(data={'weather_entity':'weather.test',
            'outdoor_temperature_sensor':'sensor.temp'}, options={})
        states = {'weather.test':SimpleNamespace(state='sunny', attributes={
            'supported_features':2, 'temperature_unit':'°C', 'temperature':8}),
            'sensor.temp':SimpleNamespace(state='50', attributes={'unit_of_measurement':'°F'})}
        service = AsyncMock(return_value={'weather.test':{'forecast':[
            {'datetime':(start+timedelta(hours=h)).isoformat(),'temperature':20}
            for h in range(24) if h != 10]}})
        c.hass = SimpleNamespace(states=SimpleNamespace(get=states.get),
                                 services=SimpleNamespace(async_call=service))
        c._demand_temperature_cache = ('sensor.temp', int(now.timestamp()//3600), observed)
        history, combined, sources = asyncio.run(demand_coordinator.async_weather_inputs(c,now))
        self.assertEqual(history, observed)
        self.assertEqual(combined[start], 5)
        self.assertEqual(combined[start+timedelta(hours=10)], 10)
        self.assertEqual(len(combined), 24)
        self.assertNotIn(start+timedelta(hours=10), history)
        self.assertEqual(sources['current_hour'], 'outdoor_temperature_sensor')
        # Invalid sensor can use the weather entity's explicitly Celsius value.
        states['sensor.temp'].state = 'unavailable'
        _, combined, sources = asyncio.run(demand_coordinator.async_weather_inputs(c,now))
        self.assertEqual(combined[start+timedelta(hours=10)], 8)
        self.assertEqual(sources['current_hour'], 'weather_entity')
        # A missing completed hour must not be fabricated from current weather.
        del observed[start+timedelta(hours=9)]
        c._demand_weather_cache = ('weather.test', int(now.timestamp()//3600), {
            start+timedelta(hours=h):20 for h in range(11,24)})
        _, combined, _ = asyncio.run(demand_coordinator.async_weather_inputs(c,now))
        self.assertNotIn(start+timedelta(hours=9), combined)

    def test_optional_weather_read_only_service_and_failure_fallback(self):
        now,histories,_,devices=fixtures()
        c=coordinator()
        c.config_entry=SimpleNamespace(data={'demand_devices':devices,'total_energy_sensor':'house',
            'weather_entity':'weather.test','outdoor_temperature_sensor':'sensor.temp'},options={})
        states={e:v[-1] for e,v in histories.items()}
        states['weather.test']=SimpleNamespace(state='cloudy',attributes={'temperature_unit':'°F','supported_features':3})
        states['sensor.temp']=SimpleNamespace(state='50',attributes={'unit_of_measurement':'°F'})
        service=AsyncMock(return_value={'weather.test':{'forecast':[{'datetime':now.isoformat(),'temperature':50}]}})
        c.hass=SimpleNamespace(states=SimpleNamespace(get=states.get),services=SimpleNamespace(async_call=service))
        async def load(job):
            return job()
        with patch.object(demand_coordinator,'get_instance',return_value=SimpleNamespace(async_add_executor_job=load)), \
             patch.object(demand_coordinator,'load_hourly_statistics',return_value={}), \
             patch.object(demand_coordinator.history,'get_significant_states',return_value=histories,create=True):
            result=asyncio.run(async_demand_result(c,now))
            service.assert_awaited_once_with('weather','get_forecasts',
                {'entity_id':'weather.test','type':'hourly'},blocking=True,return_response=True)
            self.assertEqual(result.demand_weather_sources['forecast'],'ok')
            self.assertTrue(result.demand_accuracy)
            service.side_effect=RuntimeError('provider down')
            fallback=asyncio.run(async_demand_result(c,now+timedelta(hours=1)))
            self.assertTrue(fallback.estimated_hourly_home_demand)
            self.assertEqual(fallback.demand_weather_sources['forecast'],'unavailable: RuntimeError')

    def test_optional_weather_bad_sources_do_not_block_ready_forecast(self):
        now,histories,_,devices=fixtures()
        for state,attrs,expected in [('unknown',{},'unavailable'),
                ('sunny',{'supported_features':None},'hourly_not_supported'),
                ('sunny',{'supported_features':1},'hourly_not_supported'),
                ('sunny',{'supported_features':2,'temperature_unit':'K'},'invalid_or_empty')]:
            with self.subTest(expected=expected):
                c=coordinator()
                c.config_entry=SimpleNamespace(data={'demand_devices':devices,'total_energy_sensor':'house',
                    'weather_entity':'weather.bad'},options={})
                states={e:v[-1] for e,v in histories.items()}
                states['weather.bad']=SimpleNamespace(state=state,attributes=attrs)
                service=AsyncMock(return_value={'weather.bad':{'forecast':[{'datetime':now.isoformat(),'temperature':10}]}})
                c.hass=SimpleNamespace(states=SimpleNamespace(get=states.get),services=SimpleNamespace(async_call=service))
                async def load(job):
                    return job()
                with patch.object(demand_coordinator,'get_instance',return_value=SimpleNamespace(async_add_executor_job=load)), \
                     patch.object(demand_coordinator,'load_hourly_statistics',return_value={}), \
                     patch.object(demand_coordinator.history,'get_significant_states',return_value=histories,create=True):
                    result=asyncio.run(async_demand_result(c,now))
                self.assertTrue(result.estimated_hourly_home_demand)
                self.assertEqual(result.demand_weather_sources['forecast'],expected)
                self.assertEqual(result.status, 'ready_with_warnings')
                self.assertEqual(result.source_status['weather_entity'], expected)
                self.assertTrue(any('weather_entity' in e for e in result.source_errors))
                if expected != 'invalid_or_empty':
                    service.assert_not_awaited()

    def test_optional_recorder_error_cause_survives_same_hour_cache(self):
        now = fixtures()[0]
        c = coordinator()
        c.config_entry = SimpleNamespace(data={'outdoor_temperature_sensor':'sensor.temp'}, options={})
        c.hass = SimpleNamespace(states=SimpleNamespace(get=lambda entity: None))
        async def load(job):
            return job()
        with patch.object(demand_coordinator, 'get_instance', return_value=SimpleNamespace(async_add_executor_job=load)), \
             patch.object(demand_coordinator.history, 'get_significant_states', side_effect=RuntimeError('private recorder cause'), create=True), \
             patch.object(demand_coordinator, 'load_temperature_statistics', side_effect=ValueError('private stats cause')):
            first = asyncio.run(demand_coordinator.async_weather_inputs(c, now))[2]
            again = asyncio.run(demand_coordinator.async_weather_inputs(c, now+timedelta(minutes=1)))[2]
        self.assertEqual(first['history'], 'unavailable: RuntimeError')
        self.assertEqual(again['history'], first['history'])
        self.assertEqual(again['statistics'], 'unavailable: ValueError')
        self.assertNotIn('private', json.dumps(again))

    def test_statistics_temperature_fallback_survives_purged_state_history(self):
        now,histories,_,devices=fixtures()
        c=coordinator()
        c.config_entry=SimpleNamespace(data={'demand_devices':devices,'total_energy_sensor':'house',
            'outdoor_temperature_sensor':'sensor.temp'},options={})
        states={e:v[-1] for e,v in histories.items()}
        c.hass=SimpleNamespace(states=SimpleNamespace(get=states.get))
        async def load(job):
            return job()
        rows={'sensor.temp':[{'start':(now-timedelta(hours=h)).replace(minute=0,second=0,microsecond=0).timestamp(),
                              'mean':9.0} for h in range(1,25)]}
        with patch.object(demand_coordinator,'get_instance',return_value=SimpleNamespace(async_add_executor_job=load)), \
             patch.object(demand_coordinator,'load_hourly_statistics',return_value={}), \
             patch.object(demand_coordinator,'load_temperature_statistics',return_value=rows) as stats, \
             patch.object(demand_coordinator.history,'get_significant_states',return_value=histories,create=True):
            result=asyncio.run(async_demand_result(c,now))
        stats.assert_called_once_with(c.hass,'sensor.temp',now)
        self.assertEqual(result.demand_weather_sources['history'],'ok')
        self.assertEqual(result.demand_weather_sources['temperature_history_hours'],24)
        self.assertTrue(result.estimated_hourly_home_demand)

    def test_predictor_uses_recorder_and_caches_history(self):
        now, histories, prices, devices = fixtures()
        c = coordinator()
        c.config_entry = SimpleNamespace(data={'demand_devices':devices, 'total_energy_sensor':'house'}, options={})
        c.hass = SimpleNamespace(states=SimpleNamespace(get=lambda entity: histories[entity][-1]))
        async def load(job):
            return job()
        with patch.object(demand_coordinator, 'get_instance', return_value=SimpleNamespace(async_add_executor_job=load)), \
             patch.object(demand_coordinator, 'load_hourly_statistics', return_value={}), \
             patch.object(demand_coordinator.history, 'get_significant_states', return_value=histories, create=True) as query:
            first = asyncio.run(async_demand_result(c, now))
            again = asyncio.run(async_demand_result(c, now+timedelta(minutes=15)))
            self.assertEqual(query.call_count,1)
        self.assertEqual(first.status, 'ready_with_warnings')
        self.assertEqual(first.estimated_total_home_demand_kwh,13.8)
        self.assertEqual(len(again.estimated_hourly_home_demand),288)
