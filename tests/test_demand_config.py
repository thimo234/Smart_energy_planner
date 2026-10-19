"""Device configuration steps preserve selections and reject double counting."""
from types import SimpleNamespace
from unittest.mock import AsyncMock
import unittest
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
                              _build_demand_schema=lambda hass,data: data)
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
