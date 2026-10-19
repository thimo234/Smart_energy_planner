"""Device configuration steps preserve selections and reject double counting."""
from types import SimpleNamespace
from unittest.mock import AsyncMock
import unittest
import math
from test_planner_types import load_function


class DemandConfigTest(unittest.IsolatedAsyncioTestCase):
    def make_flow(self, data):
        marker = lambda key, **kw: key
        factory = lambda *args, **kw: kw
        vol = SimpleNamespace(Required=marker, Schema=lambda obj: obj)
        selector = SimpleNamespace(SelectSelector=factory, SelectSelectorConfig=factory,
            SelectOptionDict=factory, SelectSelectorMode=SimpleNamespace(DROPDOWN='dropdown'),
            TimeSelector=factory, NumberSelector=factory, NumberSelectorConfig=factory,
            NumberSelectorMode=SimpleNamespace(BOX='box'))
        flow = SimpleNamespace(hass=None, async_show_form=lambda **kwargs: kwargs,
                               async_step_configure=AsyncMock(side_effect=lambda d: d))
        step = load_function('config_flow.py','async_step_device',vol=vol,selector=selector)
        start = load_function('config_flow.py','_start_demand_devices',
                              _build_demand_schema=lambda hass,data: data,
                              _demand_meter_ids=lambda hass: ['house','dish','pump','cook'])
        flow.async_step_device = lambda value=None: step(flow,value)
        return flow, lambda: start(flow,data,'configure')

    async def test_multiple_devices_keep_independent_rules(self):
        flow, start = self.make_flow(dict(total_energy_sensor='house', demand_energy_sensors=['dish','pump'],price_sensor='prices'))
        form = await start()
        self.assertEqual(form['description_placeholders']['entity'],'dish')
        form = await flow.async_step_device(dict(timing='cheap',offset_hours=0,start_time='17:30:00',weekdays=['0','1']))
        self.assertEqual(form['description_placeholders']['entity'],'pump')
        saved = await flow.async_step_device(dict(timing='cheap',offset_hours=2,start_time='17:30:00',weekdays=['0','1']))
        self.assertEqual([r['offset_hours'] for r in saved['demand_devices']],[0,2])
        self.assertEqual(saved['demand_devices'][1]['weekdays'],[0,1])

    async def test_total_and_appliance_cannot_be_same_meter(self):
        _, start = self.make_flow(dict(total_energy_sensor='house',demand_energy_sensors=['house']))
        result = await start()
        self.assertEqual(result['errors']['base'],'duplicate_energy_source')

    async def test_removing_devices_keeps_total_only_forecast(self):
        _, start = self.make_flow(dict(total_energy_sensor='house', demand_energy_sensors=[],
                                       demand_devices=[dict(entity_id='old')]))
        result = await start()
        self.assertEqual(result['demand_devices'],[])

    async def test_price_schedule_requires_price_source(self):
        flow,start = self.make_flow(dict(demand_energy_sensors=['pump']))
        await start()
        result = await flow.async_step_device(dict(timing='cheap',offset_hours=2,start_time='17:30:00',weekdays=['1']))
        self.assertEqual(result['errors']['base'],'price_sensor_required')
        self.assertEqual(flow._demand_queue,['pump'])

    async def test_invalid_saved_or_submitted_meter_is_rejected(self):
        for finish in ('configure', 'init'):
            for data in (dict(demand_energy_sensors=['power']),
                         dict(total_energy_sensor='power', demand_energy_sensors=[])):
                flow, _ = self.make_flow(data)
                start = load_function('config_flow.py','_start_demand_devices',
                    _build_demand_schema=lambda hass,data: data,
                    _demand_meter_ids=lambda hass: ['house','pump'])
                result = await start(flow,data,finish)
                self.assertEqual(result['errors']['base'],'unsupported_energy_meter')
                self.assertEqual(result['step_id'],finish)

    def test_meter_filter_accepts_supplied_meters_and_rejects_wrong_semantics(self):
        states=[]
        for entity,unit,kind,value in [
            ('sensor.buiten_kerstboom_opsomming_afgeleverd','kWh','total_increasing','72.68'),
            ('sensor.warmtepomp_verbruik_energie','kWh','total_increasing','2720.77'),
            ('sensor.wh','Wh','total','123'),
            ('sensor.power','W','measurement','100'),
            ('sensor.forecast','kWh',None,'10'),
            ('sensor.measurement','kWh','measurement','10'),
            ('sensor.mwh','MWh','total','1'),
            ('sensor.missing','kWh','total','unavailable'),
            ('sensor.nan','kWh','total','NaN')]:
            states.append(SimpleNamespace(entity_id=entity,state=value,
                attributes=dict(unit_of_measurement=unit,state_class=kind)))
        filter_ids=load_function('config_flow.py','_demand_meter_ids',
                                math=math,_sensor_options=lambda hass: states)
        self.assertEqual(filter_ids(None),[s.entity_id for s in states[:3]])

    def test_empty_meter_selector_cannot_fall_back_to_all_entities(self):
        factory=lambda **kwargs:kwargs
        selectors=SimpleNamespace(SelectSelector=lambda config:config,SelectSelectorConfig=factory,
            SelectOptionDict=factory,SelectSelectorMode=SimpleNamespace(DROPDOWN='dropdown'))
        build=load_function('config_flow.py','_demand_meter_selector',selector=selectors,
                            _demand_meter_ids=lambda hass:[])
        for multiple in (True,False):
            result=build(None,multiple=multiple)
            self.assertEqual(result['options'],[])
            self.assertFalse(result['custom_value'])
            self.assertEqual(result['multiple'],multiple)
