"""Opt-in optimizer: score command windows, not proposed energy."""
from copy import copy
from datetime import datetime, timedelta
import json
from pathlib import Path
import unittest
from unittest.mock import Mock, patch
from typing import Any

from partial_solar_assertions import assert_executable_plan
from test_battery_energy_accounting import coordinator, replay_full_plan
from test_battery_evening_peak_regression import replay_evening_gap
import test_battery_nov15_brief_discharge
from test_support import install_package_stub

install_package_stub()

from custom_components.smart_energy_planner.battery_optimizer import evaluate_battery_plan, optimize_battery_plan


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
        return evaluate_battery_plan(self.slots, windows, now=self.now, initial=0.,
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

    def test_prices_grid_import_and_separates_home_from_export(self):
        windows = [self.window(0, 'laden_van_net'), self.window(1, 'ontladen_naar_net')]
        self.slots[1].update(demand_kwh=1., net_solar_kwh=-1.)
        self.slots[0].update(solar_kwh=2., net_solar_kwh=1.)
        self.assertAlmostEqual(self.score(windows).cost, .25 + .10 - .70 - .10)
        self.assertAlmostEqual(self.score(windows, .75).cost, 1.5*.25 + .5*.10 - .70 - .10)

    def test_rejects_unknown_tariff_and_overlap(self):
        charge = self.window(0, 'laden_met_zonne_energie')
        self.assertIsNone(self.score([charge, charge]))
        self.slots[0]['price_known'] = False
        self.assertIsNone(self.score([charge]))

    def test_solar_transition_cannot_be_scheduled_to_charge(self):
        self.slots[0]['solar_transition'] = 'lead'
        self.assertIsNone(self.score([self.window(0, 'laden_met_zonne_energie')]))

    def test_cheaper_but_incomplete_grid_fill_cannot_win(self):
        self.slots[0].update(import_price=-.50, solar_kwh=0., net_solar_kwh=-1.)
        charge = dict(self.window(0, 'laden_van_net'), end=(self.now + timedelta(minutes=30)).isoformat())
        self.assertTrue(self.score([charge]).incomplete_grid_fill)
        result = optimize_battery_plan(baseline=('reference', None), candidates=[('grid', 1)],
            simulate=lambda variant: [charge] if variant[0] == 'grid' else [],
            settings=dict(slots=self.slots, now=self.now, initial=0., capacity=2.,
                          max_charge=2., max_discharge=2.))
        self.assertEqual(result, (None, 'no_better_battery_plan'))

    def test_starting_full_does_not_count_as_solar_refill(self):
        self.slots[0].update(demand_kwh=1., solar_kwh=0., net_solar_kwh=-1.)
        self.slots[1].update(solar_kwh=.5, demand_kwh=0., net_solar_kwh=.5, export_price=-.50)
        discharge = self.window(0, 'ontladen')
        refill = self.window(1, 'laden_met_zonne_energie')
        result = optimize_battery_plan(baseline=('reference', None), candidates=[('solar', 1)],
            simulate=lambda variant: [discharge, refill] if variant[0] == 'solar' else [discharge],
            settings=dict(slots=self.slots, now=self.now, initial=2., capacity=2.,
                          max_charge=2., max_discharge=2.))
        self.assertEqual(result, (None, 'no_better_battery_plan'))

    def test_deadline_keeps_baseline_even_if_candidate_would_be_better(self):
        result = optimize_battery_plan(baseline=[], candidates=[("solar", 1)],
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
    def test_solar_candidate_starts_before_reference_cycle_admission(self):
        now = datetime.fromisoformat(solar_scenario()['timestamp'])
        reference = coordinator()
        reference._charge_session_started = False
        reference._battery_cycle_discharge_floor_kwh = 4.
        seed = copy(reference)
        reference._battery_cycle_discharge_floor_kwh = 0.
        reference._full_arbitrage_intervals = [(now, now + timedelta(hours=1))]
        solar = [{'start': now.isoformat(), 'end': (now + timedelta(hours=1)).isoformat()}]
        observed = []

        def build(trial, **plan):
            observed.append((trial._battery_cycle_discharge_floor_kwh,
                             getattr(trial, '_full_arbitrage_intervals', [])))
            return [{**solar[0], 'mode': 'laden_met_zonne_energie'}], None

        def compare(*, baseline, candidates, simulate, settings):
            simulate(baseline)
            simulate(next(candidates))
            return None, 'no_better_battery_plan'

        with (patch.object(type(reference), '_build_mode_windows_from_hourly_plan', build),
              patch.object(type(reference), '_full_solar_refill_blocks', side_effect=lambda *_: iter([solar])),
              patch.dict(reference._try_battery_optimizer.__func__.__globals__, optimize_battery_plan=compare)):
            result = reference._try_battery_optimizer(
                baseline=solar, grid=[], source_seed=seed, source_args={}, full=True,
                slots=[{'start': now, 'end': now + timedelta(hours=1), 'import_price': .2}],
                now=now, usable_capacity_kwh=8., initial_usable_energy_kwh=8.,
                no_charge_reserve_kwh=4., max_charge_kw=3., max_discharge_kw=3., battery_min_profit=.1)
        self.assertEqual(result, (None, 'no_better_battery_plan'))
        self.assertEqual(observed, [(0., reference._full_arbitrage_intervals), (4., [])])
        self.assertEqual(reference._battery_cycle_discharge_floor_kwh, 0.)

    def test_opt_in_jointly_selects_grid_solar_home_and_export(self):
        args = solar_scenario()
        args['soc'] = 60.
        data = args['fixture_data']
        for series, field, values in (
            ('upcoming_energy_price_windows', 'price', [.20,.65,.09,.20,.48,.20,.48,.65,.65,.04,.48,.04,.48,.85,.04]),
            ('upcoming_export_price_windows', 'price', [.05,.25,.01,.12,.33,.01,.33,.57,.25,.01,.40,.01,.40,.70,.01]),
            ('estimated_hourly_home_demand', 'estimated_kwh', [.5,2.,1.,3.,3.,0.,2.,0.,.5,.5,.5,1.,3.,.5,3.]),
            ('estimated_hourly_solar_forecast', 'estimated_kwh', [0.,0.,0.,.5,1.]),
        ):
            for row, value in zip(data[series], values, strict=True):
                row[field] = value
        now, slots, v1 = replay_full_plan(**args)
        _, _, v2 = replay_full_plan(**args, optimizer_v2=True)
        self.assertEqual(v2.battery_planner_engine, 'v2', v2.battery_planner_engine_reason)
        self.assertFalse(v1.planned_grid_charge_windows)
        self.assertTrue(v2.planned_grid_charge_windows)
        _, _, solar_only = replay_full_plan(**args, optimizer_solar_only=True)
        self.assertFalse(solar_only.planned_grid_charge_windows)
        self.assertTrue({'laden_van_net', 'laden_met_zonne_energie', 'ontladen', 'ontladen_naar_net'}
                        <= {w['mode'] for w in v2.planned_battery_mode_windows})
        exports = {row['start']: row['price'] for row in data['upcoming_export_price_windows']}
        for slot in slots:
            slot['export_price'] = exports[slot['start'].isoformat()]
        trace = assert_executable_plan(self, slots, v2.planned_battery_mode_windows, initial=6., reserve=6.)
        self.assertTrue(trace['saw_full'])
        for factor in (1., .75):
            settings = dict(now=now, slots=slots, initial=4., capacity=8., max_charge=3.,
                            max_discharge=3., solar_factor=factor)
            before, after = (evaluate_battery_plan(windows=p.planned_battery_mode_windows, **settings)
                             for p in (v1, v2))
            self.assertLess(after.cost, before.cost)
            self.assertGreaterEqual(after.end + .005, before.end)

    def test_opt_in_selects_cheaper_full_refill_and_remains_executable(self):
        args = solar_scenario()
        now, _, v1 = replay_full_plan(**args)
        _, slots, v2 = replay_full_plan(**args, optimizer_v2=True)
        self.assertEqual((v1.battery_planner_engine, v1.battery_planner_engine_reason), ('v1', 'disabled'))
        self.assertEqual((v2.battery_planner_engine, v2.battery_planner_engine_reason),
                         ('v2', 'battery_cost_and_cloud_risk'))
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
                baseline = evaluate_battery_plan(windows=v1.planned_battery_mode_windows, solar_factor=factor, **settings)
                optimized = evaluate_battery_plan(windows=v2.planned_battery_mode_windows, solar_factor=factor, **settings)
                self.assertLess(optimized.cost, baseline.cost)

    def test_unexpected_error_retains_exact_v1_commands(self):
        args = solar_scenario()
        _, _, v1 = replay_full_plan(**args)
        namespace = coordinator()._build_plan_for_horizon.__globals__
        broken_optimizer = Mock(side_effect=ValueError('bad input'))
        with patch.dict(namespace, optimize_battery_plan=broken_optimizer, _LOGGER=Mock()):
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
                    assert_executable_plan(self, slots, v2.planned_battery_mode_windows,
                        initial=soc/10, reserve=reserve/10, minimum=minimum/10,
                        max_charge=max_charge, max_discharge=max_discharge)
