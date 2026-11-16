"""Weather correction uses past observations, never future targets for fitting."""
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
import unittest
from test_support import install_package_stub
install_package_stub()
from custom_components.smart_energy_planner.demand_weather import (
    temperature_c, temperature_hours, forecast_temperatures, correct_component,
)


class DemandWeatherTest(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 11, 10, 10, tzinfo=timezone.utc)
        start = self.now.replace(hour=0)-timedelta(days=28)
        self.hours, self.temps = {}, {}
        for d in range(28):
            temp = 3+(d*7 % 17)
            for h in range(24):
                at = start+timedelta(days=d, hours=h)
                self.temps[at] = temp
                self.hours[at] = .1+.05*max(0, 18-temp)
        self.profile = {self.now.replace(hour=0)+timedelta(minutes=15*i): .1 for i in range(288)}
        self.future = {self.now.replace(hour=0)+timedelta(hours=i): 0 for i in range(72)}

    def test_build_exposes_component_metrics_and_preserves_sum(self):
        from test_demand_forecast import fixtures
        from custom_components.smart_energy_planner.demand_forecast import build_demand_forecast
        now, histories, prices, devices=fixtures()
        diagnostics={}
        forecast,components,_=build_demand_forecast(histories,devices,total_entity='house',now=now,
            prices=prices,temperature_history={},temperature_forecast={},diagnostics=diagnostics)
        self.assertEqual(set(diagnostics),set(components))
        for i,row in enumerate(forecast):
            self.assertAlmostEqual(row['estimated_kwh'],sum(v[i]['estimated_kwh'] for v in components.values()),places=5)

    def test_baseline_accuracy_available_without_weather(self):
        _,metrics=correct_component(self.profile,self.hours,{}, {}, {'timing':'cheap'},now=self.now)
        self.assertGreaterEqual(metrics['baseline_history']['validation_days'],7)
        self.assertIn('mae_kwh',metrics['baseline_history'])
        self.assertEqual(metrics['baseline_history']['scope'],'complete_day_energy_chronological_no_intraday_validation')

    def test_dst_history_skips_transition_day_and_keeps_physical_hours(self):
        from zoneinfo import ZoneInfo
        zone=ZoneInfo('Europe/Amsterdam')
        start=datetime(2026,10,24,22,tzinfo=timezone.utc)
        states=[SimpleNamespace(last_updated=start+timedelta(hours=h),state='10',
                attributes={'unit_of_measurement':'°C'}) for h in range(26)]
        hours=temperature_hours(states,now=start+timedelta(hours=25))
        self.assertEqual(len(hours),25)
        from custom_components.smart_energy_planner.demand_forecast import _days
        self.assertEqual(_days(hours,zone),{})

    def test_recorder_temperature_statistics_are_completed_celsius_hours(self):
        from custom_components.smart_energy_planner.demand_weather import temperature_statistics
        start=self.now.replace(hour=0)
        rows=[{'start':(start+timedelta(hours=h)).timestamp(),'mean':10+h} for h in range(13)]
        rows += [{'start':start.timestamp()+3600,'mean':None}, {'start':start.timestamp()+7200,'mean':float('nan')}]
        hours=temperature_statistics(rows,now=self.now)
        self.assertEqual(len(hours),10)
        self.assertEqual(hours[start],10)
        self.assertTrue(all(t+timedelta(hours=1)<=self.now for t in hours))

    def test_conversion_and_invalid_values(self):
        self.assertEqual(temperature_c(50, '°F'), 10)
        self.assertEqual(temperature_c(10, '°C'), 10)
        for value, unit in [('unknown','°C'), ('nan','°C'), (200,'°C'), (10,'K')]:
            self.assertIsNone(temperature_c(value, unit))

    def test_hourly_history_requires_coverage_and_does_not_bridge_invalid(self):
        at = self.now-timedelta(hours=3)
        states = [SimpleNamespace(last_updated=at+timedelta(minutes=m), state=v,
                    attributes={'unit_of_measurement':'°F'}) for m,v in [(0,'50'),(60,'50'),(90,'unavailable'),(120,'50'),(180,'50')]]
        self.assertEqual(temperature_hours(states, now=self.now), {at:10, at+timedelta(hours=2):10})

    def test_forecast_is_timezone_aware_hourly_only(self):
        rows=[{'datetime':self.now.isoformat(),'temperature':50},
              {'datetime':self.now.replace(tzinfo=None).isoformat(),'temperature':100},
              {'datetime':(self.now+timedelta(hours=1)).isoformat(),'temperature':'nan'}]
        self.assertEqual(forecast_temperatures(rows, '°F'), {self.now:10})

    def test_heating_component_activated_by_chronological_holdout(self):
        result, metrics = correct_component(self.profile, self.hours, self.temps, self.future,
                                             {'timing':'cheap'}, now=self.now)
        self.assertEqual(metrics['temperature_status'], 'active')
        self.assertGreaterEqual(metrics['validation_days'], 7)
        self.assertLess(metrics['weather_mae_kwh'], metrics['baseline_mae_kwh'])
        self.assertEqual(metrics['validation_weather'], 'observed_temperature_proxy_not_archived_forecast')
        self.assertGreater(sum(result.values()), sum(self.profile.values()))
        self.assertLessEqual(max(result.values()), .15+1e-9)
        self.assertEqual(result, correct_component(self.profile, self.hours, self.temps, self.future,
                                                  {'timing':'cheap'}, now=self.now)[0])

    def test_weather_preserves_mixed_price_shape_and_normal_hour_heating_base(self):
        from test_demand_forecast import PlannerWindow
        from custom_components.smart_energy_planner.demand_forecast import component_forecast
        start = self.now.replace(hour=0)
        energy = {t: value*(1.4 if 6 <= t.hour < 18 else .6) for t, value in self.hours.items()}
        prices = [PlannerWindow(start+timedelta(hours=i), start+timedelta(hours=i+1),
                  .1 if i % 24 >= 21 else .4) for i in range(72)]
        fraction = .3
        rule = {'timing':'cheap', 'shiftable_fraction':fraction, 'offset_hours':2.25}
        clock, _ = component_forecast(energy, {}, now=self.now, prices=prices)
        mixed, _ = component_forecast(energy, rule, now=self.now, prices=prices)
        corrected, metrics = correct_component(mixed, energy, self.temps, self.future, rule, now=self.now)
        self.assertEqual(metrics['temperature_status'], 'active')
        self.assertNotEqual(mixed, clock)
        for day in range(3):
            ticks = [t for t in mixed if t.date() == (start+timedelta(days=day)).date()]
            self.assertAlmostEqual(sum(mixed[t] for t in ticks), sum(clock[t] for t in ticks))
            factor = sum(corrected[t] for t in ticks)/sum(mixed[t] for t in ticks)
            for t in ticks:
                self.assertAlmostEqual(corrected[t], factor*mixed[t])
                self.assertGreaterEqual(corrected[t]+1e-12, factor*(1-fraction)*clock[t])
        self.assertEqual(metrics['intraday_factor'], 1)

    def test_constant_dishwasher_and_missing_history_unchanged(self):
        for energy, temps in [({t:.2 for t in self.hours}, self.temps), (self.hours,{})]:
            result, metrics = correct_component(self.profile, energy, temps, self.future,
                                                 {'timing':'cheap'}, now=self.now)
            self.assertEqual(result,self.profile)
            self.assertNotEqual(metrics['temperature_status'],'active')

    def test_missing_forecast_coverage_falls_back_not_zero(self):
        result, metrics = correct_component(self.profile,self.hours,self.temps,{}, {'timing':'cheap'},now=self.now)
        self.assertEqual(result,self.profile)
        self.assertEqual(metrics['forecast_days_corrected'],0)

    def test_validated_model_reports_actual_per_day_activation(self):
        result, metrics = correct_component(self.profile, self.hours, self.temps, {},
                                             {'timing':'cheap'}, now=self.now)
        self.assertEqual(result, self.profile)
        self.assertEqual(metrics['temperature_model_status'], 'validated')
        self.assertEqual(metrics['temperature_status'], 'missing_forecast_coverage')
        self.assertEqual(set(metrics['temperature_days'].values()), {'missing_forecast_coverage'})
        future = {t:v for t,v in self.future.items() if t.date() != self.now.date()}
        result, metrics = correct_component(self.profile, self.hours, self.temps, future,
                                             {'timing':'cheap'}, now=self.now)
        self.assertEqual(metrics['temperature_status'], 'partially_active')
        self.assertEqual(metrics['temperature_days'][self.now.date().isoformat()], 'missing_forecast_coverage')
        self.assertEqual(metrics['forecast_days_corrected'], 2)

    def test_intraday_only_completed_hours_and_future_today_bounded(self):
        energy=dict(self.hours)
        day=self.now.replace(hour=0)
        energy.update({day+timedelta(hours=h):.8 for h in range(11)})
        result, metrics=correct_component(self.profile,energy,{}, {}, {'timing':'learned'},now=self.now)
        self.assertEqual(metrics['intraday_factor'],1.2)
        for at, value in result.items():
            self.assertAlmostEqual(value,.12 if self.now <= at < day+timedelta(days=1) else .1)
        unchanged,_=correct_component(self.profile,energy,{}, {}, {'timing':'cheap'},now=self.now)
        self.assertEqual(unchanged,self.profile)

    def test_future_observations_cannot_change_fit(self):
        first=correct_component(self.profile,self.hours,self.temps,self.future,{'timing':'cheap'},now=self.now)
        hours={**self.hours, **{t:1000 for t in self.future}}
        temps={**self.temps, **{t:-50 for t in self.future}}
        self.assertEqual(first,correct_component(self.profile,hours,temps,self.future,{'timing':'cheap'},now=self.now))


if __name__ == '__main__':
    unittest.main()
