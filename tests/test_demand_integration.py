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


def demand_plan():
    now, histories, prices, devices = fixtures()
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
        c.config_entry = SimpleNamespace(data={'demand_forecast_sensor':'sensor.missing'}, options={})
        c.hass = SimpleNamespace(states=SimpleNamespace(get=lambda entity: None))
        result = asyncio.run(c._async_update_data())
        self.assertEqual(result.status, 'waiting_for_demand_forecast')
        self.assertFalse(result.planned_battery_mode_windows)
        self.assertIn('forecast_unavailable', result.source_errors[0])

    def test_predictor_uses_recorder_and_caches_history(self):
        now, histories, prices, devices = fixtures()
        c = coordinator()
        c.config_entry = SimpleNamespace(data={'demand_devices':devices, 'total_energy_sensor':'house'}, options={})
        c.hass = SimpleNamespace(states=SimpleNamespace(get=lambda entity: histories[entity][-1]))
        async def load(job):
            return job()
        with patch.object(demand_coordinator, 'get_instance', return_value=SimpleNamespace(async_add_executor_job=load)), \
             patch.object(demand_coordinator.history, 'get_significant_states', return_value=histories, create=True) as query:
            first = asyncio.run(async_demand_result(c, now))
            again = asyncio.run(async_demand_result(c, now+timedelta(minutes=15)))
            self.assertEqual(query.call_count,1)
        self.assertEqual(first.status, 'ready_with_warnings')
        self.assertEqual(first.estimated_total_home_demand_kwh,13.8)
        self.assertEqual(len(again.estimated_hourly_home_demand),288)
