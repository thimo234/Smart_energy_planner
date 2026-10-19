import json
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import Mock, patch
from datetime import datetime

from test_support import install_package_stub

install_package_stub()


def _install_homeassistant_stubs() -> None:
    sys.modules.setdefault("homeassistant", types.ModuleType("homeassistant"))
    sys.modules.setdefault("homeassistant.components", types.ModuleType("homeassistant.components"))
    sensor = sys.modules.setdefault(
        "homeassistant.components.sensor",
        types.ModuleType("homeassistant.components.sensor"),
    )
    sensor.SensorDeviceClass = types.SimpleNamespace(TIMESTAMP="timestamp", MONETARY="monetary")
    sensor.SensorEntity = object

    config_entries = sys.modules.setdefault(
        "homeassistant.config_entries",
        types.ModuleType("homeassistant.config_entries"),
    )
    config_entries.ConfigEntry = object

    const = sys.modules.setdefault("homeassistant.const", types.ModuleType("homeassistant.const"))
    const.UnitOfEnergy = types.SimpleNamespace(KILO_WATT_HOUR="kWh")

    core = sys.modules.setdefault("homeassistant.core", types.ModuleType("homeassistant.core"))
    core.HomeAssistant = object
    core.callback = lambda function: function

    sys.modules.setdefault("homeassistant.helpers", types.ModuleType("homeassistant.helpers"))
    sys.modules.setdefault("homeassistant.helpers.entity_registry", types.ModuleType("homeassistant.helpers.entity_registry"))
    entity_platform = sys.modules.setdefault(
        "homeassistant.helpers.entity_platform",
        types.ModuleType("homeassistant.helpers.entity_platform"),
    )
    entity_platform.AddEntitiesCallback = object
    update_coordinator = sys.modules.setdefault(
        "homeassistant.helpers.update_coordinator",
        types.ModuleType("homeassistant.helpers.update_coordinator"),
    )

    class CoordinatorEntity:
        def __init__(self, coordinator):
            self.coordinator = coordinator

        @classmethod
        def __class_getitem__(cls, item):
            return cls

    update_coordinator.CoordinatorEntity = CoordinatorEntity

    util = sys.modules.setdefault("homeassistant.util", types.ModuleType("homeassistant.util"))
    dt_module = sys.modules.setdefault("homeassistant.util.dt", types.ModuleType("homeassistant.util.dt"))
    util.dt = dt_module

    coordinator_module = types.ModuleType("custom_components.smart_energy_planner.coordinator")
    coordinator_module.SmartEnergyPlannerCoordinator = object
    sys.modules["custom_components.smart_energy_planner.coordinator"] = coordinator_module


_install_homeassistant_stubs()

from custom_components.smart_energy_planner.sensor import (
    BatteryPlannerSensor,
    BatteryStrategySensor,
    CheapestPriceWindowStartSensor,
    HomeDemandSensor,
)


class BatteryRecorderAttributesTest(unittest.TestCase):
    def test_large_forecasts_remain_live_but_fit_recorder_after_exclusions(self):
        forecast = json.loads(
            (Path(__file__).parent / "fixtures/battery_2026_09_24_cheap_solar.json")
            .read_text(encoding="utf-8")
        )
        data = types.SimpleNamespace(
            **forecast,
            planner_kind="battery", score=70, source_status={}, source_errors=[],
            current_price=0.30, price_spread=0.20,
            next_window_start=None, next_window_end=None,
            battery_min_profit_per_kwh=0.08, price_resolution="quarter_hourly",
            rationale="Test forecast", battery_strategy="ontladen",
            battery_energy_available_kwh=4.0, battery_remaining_capacity_kwh=4.0,
            estimated_total_home_demand_kwh=12.0,
            upcoming_export_price_windows=forecast["upcoming_energy_price_windows"],
            estimated_battery_mode_windows=forecast["planned_battery_mode_windows"],
            planned_battery_mode_schedule=[
                {"at": window["start"], "mode": window["mode"]}
                for window in forecast["planned_battery_mode_windows"]
            ],
        )
        data.status='ready'
        source=types.SimpleNamespace(state='12',attributes={
            'status':'ready','forecast_generated_at':'2026-09-24T08:00:00+02:00',
            'source_status':{'sensor.dishwasher':'ok'},'source_errors':[]})
        coordinator = types.SimpleNamespace(data=data,
            _config={'demand_forecast_sensor':'sensor.forecast',
                     'solcast_tomorrow_sensor':{'entity_id':'sensor.sun_tomorrow'}},
            hass=types.SimpleNamespace(states=types.SimpleNamespace(get=lambda entity:source)))
        entry = types.SimpleNamespace(entry_id="battery-entry", title="Battery")
        entities = [
            BatteryStrategySensor(coordinator, entry),
        ]
        for entity in entities:
            with self.subTest(sensor=type(entity).__name__):
                live = entity.extra_state_attributes
                self.assertEqual(live['status'],'ready')
                self.assertEqual(live['source_entities']['solcast_tomorrow_sensor'],'sensor.sun_tomorrow')
                self.assertEqual(live['demand_forecast_source']['status'],'ready')
                for key in ('upcoming_energy_price_windows','estimated_hourly_home_demand','estimated_hourly_solar_forecast'):
                    self.assertEqual(live[key],forecast[key])
                source.state='unknown'
                source.attributes.update(status='waiting_for_demand_sources',source_errors=['dishwasher: insufficient_history'])
                self.assertEqual(entity.extra_state_attributes['demand_forecast_source']['source_errors'],
                                 ['dishwasher: insufficient_history'])
                self.assertGreater(len(json.dumps(live).encode("utf-8")), 16384)
                # Mirror Recorder's attribute-name exclusion, without changing
                # the live state consumed by Lovelace and automations.
                recorded = {
                    key: value for key, value in live.items()
                    if key not in entity._unrecorded_attributes
                }
                self.assertLess(len(json.dumps(recorded).encode("utf-8")), 16384)
                self.assertEqual(recorded[entity._value_key], entity.native_value)
                self.assertEqual(recorded["current_price"], 0.30)
                for key in entity._unrecorded_attributes:
                    self.assertEqual(live[key], getattr(data, key))



class CheapestPriceWindowStartSensorTest(unittest.TestCase):
    def test_following_block_is_after_selected_block_or_empty(self):
        current = dict(start="2026-10-18T18:30:00+02:00",end="2026-10-18T21:30:00+02:00")
        following = dict(start="2026-10-19T07:00:00+02:00",end="2026-10-19T09:00:00+02:00")
        data = types.SimpleNamespace(planner_kind="price_window",source_status={},source_errors=[],
            price_window_type="most_expensive",selected_price_window=current,
            planned_price_peak_windows=[following,current])
        entity = CheapestPriceWindowStartSensor(types.SimpleNamespace(data=data),
            types.SimpleNamespace(entry_id="p",title="Prices"))
        self.assertEqual(entity.extra_state_attributes['next_price_window_start'],following['start'])
        self.assertEqual(entity.extra_state_attributes['next_price_window_end'],following['end'])
        data.planned_price_peak_windows=[current]
        self.assertIsNone(entity.extra_state_attributes['next_price_window_start'])
        data.price_window_type='cheapest'
        data.tomorrow_cheapest_price_window=following
        self.assertEqual(entity.extra_state_attributes['next_price_window_end'],following['end'])

    def test_exposes_cheapest_block_start_as_timestamp(self):
        start = "2026-08-07T02:00:00+02:00"
        data = types.SimpleNamespace(
            selected_price_window={
                "start": start,
                "end": "2026-08-07T05:00:00+02:00",
                "average_price": 0.12,
                "duration_hours": 3.0,
            },
            planner_kind="price_window",
            source_status={},
            source_errors=[],
        )
        coordinator = types.SimpleNamespace(data=data)
        entry = types.SimpleNamespace(entry_id="entry-1", title="Energy Prices")

        entity = CheapestPriceWindowStartSensor(coordinator, entry)

        self.assertEqual(entity.native_value, datetime.fromisoformat(start))
        self.assertEqual(entity._attr_device_class, "timestamp")
        self.assertEqual(entity.extra_state_attributes["end"], "2026-08-07T05:00:00+02:00")

    def test_expensive_selection_and_updates_follow_the_selected_block(self):
        cheap = dict(start="2026-10-16T11:30:00+02:00", end="2026-10-16T16:30:00+02:00", average_price=.3449, duration_hours=5)
        peak = dict(start="2026-10-16T16:45:00+02:00", end="2026-10-16T21:45:00+02:00", average_price=.4305, duration_hours=5)
        data = types.SimpleNamespace(planner_kind="price_window", source_status={}, source_errors=[],
                                     price_window_type="most_expensive", cheapest_price_window=cheap,
                                     selected_price_window=peak)
        entity = CheapestPriceWindowStartSensor(types.SimpleNamespace(data=data),
                                               types.SimpleNamespace(entry_id="entry-1", title="Prices"))
        for block in (peak, cheap, None):
            data.selected_price_window = block
            self.assertEqual(entity.native_value, datetime.fromisoformat(block["start"]) if block else None)
            attrs = entity.extra_state_attributes
            for key in ("end", "average_price", "duration_hours"):
                self.assertEqual(attrs[key], block[key] if block else None)
            self.assertEqual(attrs["selected_price_window_start"], block["start"] if block else None)
        self.assertEqual(entity._attr_unique_id, "entry-1_cheapest_price_window_start")


class HomeDemandDetailsTest(unittest.TestCase):
    def test_only_today_demand_and_its_source_are_exposed(self):
        from custom_components.smart_energy_planner import sensor
        now=datetime.fromisoformat('2026-10-18T12:00:00+02:00')
        today=dict(start='2026-10-18T14:00:00+02:00',estimated_kwh=1.2)
        tomorrow=dict(start='2026-10-19T14:00:00+02:00',estimated_kwh=1.5)
        data=types.SimpleNamespace(estimated_total_home_demand_kwh=12,
            estimated_hourly_home_demand=[today,tomorrow],battery_soc_percent=80,
            source_status={'total_energy_sensor':'ok','battery_soc_sensor':'ok'},
            source_errors=['price_sensor: missing','total_energy_sensor: warning'])
        entity=HomeDemandSensor(types.SimpleNamespace(data=data),types.SimpleNamespace(entry_id='a',title='Accu'))
        with patch.object(sensor.dt_util,'now',return_value=now,create=True), patch.object(sensor.dt_util,'as_local',side_effect=lambda x:x,create=True):
            attrs=entity.extra_state_attributes
        self.assertEqual(entity.native_value,12)
        self.assertEqual(set(attrs),{'estimated_total_home_demand_kwh','forecast_date','estimated_hourly_home_demand','source_status','source_errors'})
        self.assertEqual(attrs['estimated_hourly_home_demand'],[today])
        self.assertEqual(attrs['source_status'],{'total_energy_sensor':'ok'})
        self.assertEqual(attrs['source_errors'],['total_energy_sensor: warning'])
        self.assertIn('estimated_hourly_home_demand',entity._unrecorded_attributes)


class IndependentDemandSensorTest(unittest.TestCase):
    def test_full_horizon_components_stay_live_but_unrecorded(self):
        from custom_components.smart_energy_planner.sensor import DemandForecastSensor
        rows = [{'start': '2026-10-19T12:00:00+02:00', 'end': '2026-10-19T12:15:00+02:00', 'estimated_kwh': .3}]
        data = types.SimpleNamespace(status='ready', estimated_total_home_demand_kwh=12,
            estimated_hourly_home_demand=rows, demand_components={'pump':rows},
            forecast_generated_at='2026-10-18T12:00:00+02:00', source_status={},source_errors=[])
        entity = DemandForecastSensor(types.SimpleNamespace(data=data),types.SimpleNamespace(entry_id='a',title='Vraag'))
        self.assertTrue(entity.available)
        self.assertEqual(entity.native_value,12)
        self.assertEqual(entity.extra_state_attributes['estimated_hourly_home_demand'],rows)
        self.assertEqual(entity.extra_state_attributes['forecast_version'],1)
        self.assertIn('demand_components',entity._unrecorded_attributes)
        data.status='waiting_for_demand_sources'
        data.source_errors=['pump: insufficient_history']
        self.assertTrue(entity.available)
        self.assertIsNone(entity.native_value)
        attrs=entity.extra_state_attributes
        self.assertEqual(attrs['status'],'waiting_for_demand_sources')
        self.assertEqual(attrs['source_errors'],['pump: insufficient_history'])
        self.assertEqual(attrs['estimated_hourly_home_demand'],[])
        self.assertIsNone(attrs['forecast_generated_at'])
        entity.coordinator.last_update_success=False
        self.assertFalse(entity.available)
        self.assertIsNone(entity.native_value)

    def test_missing_external_demand_is_unavailable_in_battery_diagnostic(self):
        from custom_components.smart_energy_planner.sensor import HomeDemandSensor
        entity=HomeDemandSensor(types.SimpleNamespace(data=types.SimpleNamespace(status='waiting_for_demand_forecast')),
                                types.SimpleNamespace(entry_id='a',title='Accu'))
        self.assertFalse(entity.available)


class ScoreRemovalTest(unittest.IsolatedAsyncioTestCase):
    async def test_setup_removes_only_this_entries_score_and_does_not_recreate_it(self):
        from custom_components.smart_energy_planner import sensor
        registry=Mock()
        registry.async_get_entity_id.return_value='sensor.accu_planner_score'
        entry=types.SimpleNamespace(entry_id='a',title='Accu')
        coordinator=types.SimpleNamespace(data=types.SimpleNamespace(planner_kind='battery'))
        hass=types.SimpleNamespace(data={sensor.DOMAIN:{'a':coordinator}})
        add=Mock()
        with patch.object(sensor.er,'async_get',return_value=registry,create=True):
            await sensor.async_setup_entry(hass,entry,add)
        registry.async_get_entity_id.assert_called_once_with('sensor',sensor.DOMAIN,'a_score')
        registry.async_remove.assert_called_once_with('sensor.accu_planner_score')
        self.assertEqual([type(x).__name__ for x in add.call_args.args[0]],
                         ['BatteryStrategySensor','BatteryProfitSensor','HomeDemandSensor'])


if __name__ == "__main__":
    unittest.main()
