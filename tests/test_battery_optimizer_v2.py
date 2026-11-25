"""Opt-in optimizer: score command windows, not proposed energy."""
from datetime import datetime, timedelta
import json
from pathlib import Path
import unittest
from unittest.mock import Mock, patch
from typing import Any

from partial_solar_assertions import assert_executable_plan
from test_battery_energy_accounting import replay_full_plan
from test_battery_evening_peak_regression import replay_evening_gap
import test_battery_nov15_brief_discharge
from test_support import install_package_stub

install_package_stub()

from custom_components.smart_energy_planner.battery_optimizer import evaluate_solar_plan, optimize_solar_plan


class OptimizerScoreTest(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 11, 25, 12)
        self.slots = [self.slot(0, solar=3, demand=1, tariff=.25),
                      self.slot(1, solar=0, demand=2, tariff=.70)]

    def slot(self, hour, *, solar, demand, tariff) -> dict[str, Any]:
        start = self.now + timedelta(hours=hour)
        return dict(start=start, end=start + timedelta(hours=1), hours=1.,
                    solar_kwh=solar, demand_kwh=demand, net_solar_kwh=solar-demand,
                    import_price=tariff, export_price=.10, price_known=True)

    def window(self, hour, mode):
        slot = self.slots[hour]
        return dict(start=slot['start'].isoformat(), end=slot['end'].isoformat(), mode=mode)

    def score(self, windows, factor=1.):
        return evaluate_solar_plan(self.slots, windows, now=self.now, initial=0.,
                                   capacity=2., max_charge=2., max_discharge=2.,
                                   solar_factor=factor)

    def test_cloud_risk_uses_gross_generation_and_limits_discharge(self):
        windows = [self.window(0, 'laden_met_zonne_energie'), self.window(1, 'ontladen')]
        clear = self.score(windows)
        clouds = self.score(windows, .75)
        self.assertAlmostEqual(clear.cost, -1.20)
        self.assertAlmostEqual(clouds.cost, -.75)
        self.assertAlmostEqual(clear.peak, 2.)
        self.assertAlmostEqual(clouds.peak, 1.25)
        self.assertEqual(clear.switches, 1)

    def test_rejects_unknown_tariff_overlap_and_non_solar_actions(self):
        charge = self.window(0, 'laden_met_zonne_energie')
        self.assertIsNone(self.score([charge, charge]))
        self.assertIsNone(self.score([self.window(1, 'ontladen_naar_net')]))
        self.assertIsNone(self.score([self.window(1, 'laden_van_net')]))
        self.slots[0]['price_known'] = False
        self.assertIsNone(self.score([charge]))

    def test_solar_transition_cannot_be_scheduled_to_charge(self):
        self.slots[0]['solar_transition'] = 'lead'
        self.assertIsNone(self.score([self.window(0, 'laden_met_zonne_energie')]))

    def test_deadline_keeps_baseline_even_if_candidate_would_be_better(self):
        result = optimize_solar_plan(baseline=[], candidates=[[1]],
            simulate=lambda source: ([self.window(0, 'laden_met_zonne_energie'),
                                      self.window(1, 'ontladen')] if source else []),
            settings=dict(slots=self.slots, now=self.now, initial=0., capacity=2.,
                          max_charge=2., max_discharge=2.),
            clock=iter([0., .01, .02, .16]).__next__)
        self.assertEqual(result, (None, 'time_budget'))


def solar_scenario():
    now = datetime.fromisoformat('2026-11-25T09:00:00+01:00')

    def point(i):
        return dict(start=(now + timedelta(hours=i)).isoformat(),
                    end=(now + timedelta(hours=i+1)).isoformat())

    def prices(values):
        return [dict(**point(i), price=price, price_known=True) for i, price in enumerate(values)]

    data = dict(
        upcoming_energy_price_windows=prices([.6, .4, .6, .9, .9, .4, .9, .25, .4, .4, .25, .6, .4, .4, .9]),
        upcoming_export_price_windows=prices([.02]*2 + [.35, .15, .46, .02, .05] + [.03]*8),
        estimated_hourly_home_demand=[dict(**point(i), estimated_kwh=kwh) for i, kwh in
            enumerate([0., 2., .5, 2., 1., .5, 2., 1., 0., 1., .5, 0., 1., .5, 1.])],
        estimated_hourly_solar_forecast=[dict(**point(i), estimated_kwh=kwh) for i, kwh in
                                        enumerate([4., 0., 3., 4., 0.], start=2)],
    )
    return dict(timestamp=now.isoformat(), fixture_data=data, soc=80., discharging=True,
                reserve=60., snapshot_exports=True, isolated_cycles=True)


class OptimizerIntegrationTest(unittest.TestCase):
    def test_opt_in_selects_cheaper_full_refill_and_remains_executable(self):
        args = solar_scenario()
        now, _, v1 = replay_full_plan(**args)
        _, slots, v2 = replay_full_plan(**args, optimizer_v2=True)
        self.assertEqual((v1.battery_planner_engine, v1.battery_planner_engine_reason), ('v1', 'disabled'))
        self.assertEqual((v2.battery_planner_engine, v2.battery_planner_engine_reason),
                         ('v2', 'solar_cost_and_cloud_risk'))
        self.assertNotEqual(v2.planned_solar_charge_windows, v1.planned_solar_charge_windows)
        self.assertFalse(v2.planned_grid_charge_windows)
        trace = assert_executable_plan(self, slots, v2.planned_battery_mode_windows, initial=8., reserve=6.)
        self.assertTrue(trace['saw_full'])
        settings = dict(now=now, slots=slots, initial=6., capacity=8., max_charge=3., max_discharge=3.)
        # Normalize replay slots to the same recorded tariff the integration received.
        export = {row['start']: row['price'] for row in args['fixture_data']['upcoming_export_price_windows']}
        for slot in slots:
            slot['export_price'] = export[slot['start'].isoformat()]
        for factor in (1., .75):
            with self.subTest(solar_factor=factor):
                baseline = evaluate_solar_plan(windows=v1.planned_battery_mode_windows, solar_factor=factor, **settings)
                optimized = evaluate_solar_plan(windows=v2.planned_battery_mode_windows, solar_factor=factor, **settings)
                self.assertLess(optimized.cost, baseline.cost)

    def test_unexpected_error_retains_exact_v1_commands(self):
        args = solar_scenario()
        _, _, v1 = replay_full_plan(**args)
        from test_battery_energy_accounting import coordinator

        namespace = coordinator()._build_plan_for_horizon.__globals__
        broken_optimizer = Mock(side_effect=ValueError('bad input'))
        with patch.dict(namespace, optimize_solar_plan=broken_optimizer, _LOGGER=Mock()):
            _, _, v2 = replay_full_plan(**args, optimizer_v2=True)
        broken_optimizer.assert_called_once()
        self.assertEqual((v2.battery_planner_engine, v2.battery_planner_engine_reason), ('v1', 'optimizer_error'))
        self.assertEqual(v2.planned_battery_mode_windows, v1.planned_battery_mode_windows)

    def test_replay_all_saved_battery_snapshots(self):
        snapshots = sorted(Path(__file__).with_name('fixtures').glob('battery_*.json'))
        self.assertGreaterEqual(len(snapshots), 36)
        for path in snapshots:
            with self.subTest(snapshot=path.stem):
                raw = json.loads(path.read_text())
                if path.stem == 'battery_2026_11_15_outlook':
                    replay = test_battery_nov15_brief_discharge.November15LocalCaptureReplayTest()
                    replay.capture = raw
                    v1, v2 = replay.replay(), replay.replay(optimizer_v2=True)
                    slots = replay.replay_slots
                    soc = raw['plan']['battery_soc_percent']
                    config = raw['effective_config']
                    reserve, minimum = config['battery_no_charge_min_soc_percent'], config['battery_min_soc_percent']
                    max_charge, max_discharge = config['battery_max_charge_kw'], config['battery_max_discharge_kw']
                elif 'import_prices' in raw:
                    now, slots, v1 = replay_evening_gap()
                    _, _, v2 = replay_evening_gap(optimizer_v2=True)
                    soc, reserve, minimum, max_charge, max_discharge = raw['soc'], 60., 24., 2.5, 3.
                else:
                    data = raw.get('attributes', raw.get('plan', raw))
                    prices = data['upcoming_energy_price_windows']
                    timestamp = data.get('forecast_generated_at') or raw.get('last_updated') or prices[0]['start']
                    if datetime.fromisoformat(timestamp) >= datetime.fromisoformat(prices[-1]['end']):
                        timestamp = prices[0]['start']
                    # Older exports record the forecast only. Use an explicit
                    # synthetic SOC rather than reporting a historical one.
                    soc = data.get('battery_soc_percent', 40.)
                    reserve, minimum, max_charge, max_discharge = 60., 20., 3., 3.
                    args = dict(timestamp=timestamp, fixture_data=data, soc=soc, discharging=True,
                                reserve=reserve, isolated_cycles=True,
                                snapshot_exports=bool(data.get('upcoming_export_price_windows')))
                    now, slots, v1 = replay_full_plan(**args)
                    _, _, v2 = replay_full_plan(**args, optimizer_v2=True)
                self.assertNotIn(v2.battery_planner_engine_reason, ('disabled', 'optimizer_error'))
                if v2.battery_planner_engine == 'v1':
                    self.assertEqual(v2.planned_battery_mode_windows, v1.planned_battery_mode_windows)
                    self.assertEqual(v2.planned_grid_charge_windows, v1.planned_grid_charge_windows)
                    self.assertEqual(v2.planned_solar_charge_windows, v1.planned_solar_charge_windows)
                else:
                    self.assertFalse(v2.planned_grid_charge_windows)
                    assert_executable_plan(self, slots, v2.planned_battery_mode_windows,
                        initial=soc/10, reserve=reserve/10, minimum=minimum/10,
                        max_charge=max_charge, max_discharge=max_discharge)
