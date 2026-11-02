"""Preserve morning demand and allow daytime grid fills independently of PV."""
from datetime import datetime, timedelta
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from test_battery_energy_accounting import replay_full_plan, energy_trace, coordinator


class MorningValueTest(unittest.TestCase):
    def replay(self, at='2026-11-02T04:00:00+01:00', energy=3.6, solar='actual', instance=None, profit=.1):
        original = Path.read_text
        data = json.loads(original(Path(__file__).parent/'fixtures/battery_2026_11_02.json'))
        if solar != 'actual':
            for w in data['estimated_hourly_solar_forecast']:
                w['estimated_kwh'] = 0.
                if solar == 'tiny' and w['start'].startswith('2026-11-02T12:00'):
                    a,b = map(datetime.fromisoformat,(w['start'],w['end']))
                    for d in data['estimated_hourly_home_demand']:
                        c,e = map(datetime.fromisoformat,(d['start'],d['end']))
                        hours = max(0.,(min(b,e)-max(a,c)).total_seconds()/3600)
                        w['estimated_kwh'] += d['estimated_kwh']*hours/((e-c).total_seconds()/3600)
                    w['estimated_kwh'] += .001
        with patch.object(Path,'read_text',lambda p,*a,**k:json.dumps(data)
                if p.name=='battery_2026_11_02.json' else original(p,*a,**k)):
            return replay_full_plan('2026_11_02',at,soc=energy*10,discharging=True,
                reserve=60,max_charge=2.5,profit=profit,isolated_cycles=True,
                snapshot_exports=True,instance=instance,external_demand=data['estimated_hourly_home_demand'])

    def test_morning_value_survives_with_sun_without_sun_and_tiny_surplus(self):
        for solar in ('actual','zero','tiny'):
            with self.subTest(solar=solar):
                now, slots, result = self.replay(solar=solar)
                windows = result.planned_battery_mode_windows + result.estimated_battery_mode_windows
                self.assertTrue(result.planned_grid_charge_windows)
                charges = [w for w in windows if w['mode'].startswith('laden_')]
                start=min(datetime.fromisoformat(w['start']) for w in charges)
                end=max(datetime.fromisoformat(w['end']) for w in charges)
                self.assertGreaterEqual(start.hour,9)
                self.assertLessEqual(end.hour,15)
                self.assertFalse(any(w['mode']=='ontladen_naar_net' for w in windows))
                self.assertTrue(any(w['mode']=='ontladen' and 'T08:' in w['start'] for w in windows))
                trace=energy_trace(now,slots,windows,initial=3.6,max_charge=2.5)
                self.assertAlmostEqual(min(e for _,e,_ in trace),2.,delta=.005)
                self.assertAlmostEqual(max(e for _,e,_ in trace),10.,delta=.005)
                self.assertAlmostEqual(trace[-1][1],6.,delta=.005)
                if solar=='zero':self.assertFalse(result.planned_solar_charge_windows)
                imported=cost=used=value=0.
                for w in windows:
                    for s in slots:
                        a=max(datetime.fromisoformat(w['start']),s['start']);b=min(datetime.fromisoformat(w['end']),s['end'])
                        hours=max(0.,(b-a).total_seconds()/3600);net=s['net_solar_kwh']/s['hours']
                        if w['mode']=='laden_van_net':
                            amount=max(0.,2.5-max(0.,net))*hours;imported+=amount;cost+=amount*s['import_price']
                        elif w['mode']=='ontladen' and a>=end:
                            amount=min(3.,max(0.,-net))*hours;used+=amount;value+=amount*s['import_price']
                self.assertGreaterEqual(value/used-cost/imported,.1)

    def test_no_sun_does_not_bypass_profit(self):
        _,_,r=self.replay(solar='zero',profit=.5)
        self.assertFalse(r.planned_grid_charge_windows)
        self.assertTrue(r.battery_no_charge_reserve_active)

    def test_feedback_and_restart_preserve_morning_energy_and_complete_grid_fill(self):
        c=coordinator();c._charge_session_started=False;c._discharge_session_started=True
        now=datetime.fromisoformat('2026-11-02T04:00:00+01:00');energy=3.6
        for index in range(44):
            _,slots,r=self.replay(now.isoformat(),energy,solar='zero',instance=c)
            if index==12:
                c.hass=SimpleNamespace(data={});c.config_entry.entry_id='morning-value'
                c._store_battery_cycle_state_snapshot(now)
                restored=coordinator();restored.hass,restored.config_entry=c.hass,c.config_entry
                with patch.object(c._restore_recent_battery_cycle_state.__globals__['dt_util'],'now',return_value=now):
                    restored._restore_recent_battery_cycle_state()
                c=restored;del c.hass
                _,slots,again=self.replay(now.isoformat(),energy,solar='zero',instance=c)
                self.assertEqual(r.planned_battery_mode_windows,again.planned_battery_mode_windows)
            until=now+timedelta(minutes=15)
            commands=[{**w,'end':min(datetime.fromisoformat(w['end']),until).isoformat()}
                for w in r.planned_battery_mode_windows if datetime.fromisoformat(w['start'])<until]
            t=energy_trace(now,slots,commands,initial=energy,max_charge=2.5)
            self.assertTrue(t);self.assertGreaterEqual(min(e for _,e,_ in t),1.995);self.assertLessEqual(max(e for _,e,_ in t),10.005)
            energy=t[-1][1]
            if now.hour<7:self.assertAlmostEqual(energy,3.6,delta=.005)
            now=until
        self.assertGreaterEqual(energy,9.95)
