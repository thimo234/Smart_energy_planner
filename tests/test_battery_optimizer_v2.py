"""V2 scores executable command windows, not proposed energy."""
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

from custom_components.smart_energy_planner.battery_optimizer import BatteryScore, evaluate_battery_plan, optimize_battery_plan


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

    def test_publication_requires_minimum_profit_on_new_grid_energy(self):
        windows = [self.window(0, 'laden_van_net'), self.window(1, 'ontladen')]
        self.slots[0].update(solar_kwh=0., net_solar_kwh=-1., import_price=.50)
        settings = dict(slots=self.slots, now=self.now, initial=0., capacity=2.,
                        max_charge=2., max_discharge=2., minimum_profit=.08)
        for tariff in (.0, .55, .58):
            with self.subTest(tariff=tariff):
                self.slots[1]['import_price'] = tariff
                reasons = []
                result = optimize_battery_plan(candidates=[('grid', None)], simulate=lambda _: windows,
                    clock=lambda: 0., settings=settings)
                if tariff < .58:
                    self.assertIsNone(evaluate_battery_plan(windows=windows, on_reject=reasons.append, **settings))
                    self.assertEqual(reasons, ['grid_minimum_profit'])
                    self.assertEqual(result, (None, 'no_valid_plan'))
                else:
                    self.assertEqual(result, (('grid', None), 'battery_cost_and_cloud_risk'))

    def test_night_topup_cannot_be_preferred_without_profitable_use_of_grid_energy(self):
        self.slots[0].update(solar_kwh=0., net_solar_kwh=-1., import_price=.20)
        self.slots[1].update(solar_kwh=1., net_solar_kwh=1., export_price=.10)
        grid = self.window(0, 'laden_van_net')
        grid['end'] = (self.now + timedelta(minutes=30)).isoformat()
        solar = self.window(1, 'laden_met_zonne_energie')
        options = [('night_topup', None), ('reserve_only', None)]
        reasons = set()
        result = optimize_battery_plan(candidates=options,
            simulate=dict(zip(options, ([grid, solar], []))).__getitem__, clock=lambda: 0.,
            settings=dict(slots=self.slots, now=self.now, initial=0., capacity=2.,
                          max_charge=2., max_discharge=2., minimum_profit=.08),
            topup_target=(self.slots[1]['start'], self.slots[1]['end'], 1.),
            rejection_reasons=reasons)
        self.assertEqual(result[0], options[1])
        self.assertIn('night_topup_unproven_profit', reasons)

    def test_truncated_grid_plan_requires_profitable_outlook(self):
        self.slots[0].update(solar_kwh=0., net_solar_kwh=-1., import_price=.50)
        grid = self.window(0, 'laden_van_net')
        discharge = self.window(1, 'ontladen')
        option = ('grid', None)
        settings = dict(slots=self.slots, now=self.now, initial=0., capacity=2.,
                        max_charge=2., max_discharge=2., minimum_profit=.08)
        for tariff, expected in ((.00, None), (.55, None), (.58, option)):
            with self.subTest(tariff=tariff):
                self.slots[1]['import_price'] = tariff
                result, _ = optimize_battery_plan(candidates=[option], simulate=lambda _: [grid],
                    clock=lambda: 0., settings=settings,
                    candidate_settings=lambda _: {'profit_outlook': [grid, discharge]})
                self.assertEqual(result, expected)
        # No downstream use, or use based on a different published charge,
        # cannot prove profitability for the actual command.
        shifted_grid = dict(grid, start=(self.now + timedelta(minutes=30)).isoformat())
        for outlook in ([grid], [shifted_grid, discharge]):
            with self.subTest(outlook=outlook):
                result, reason = optimize_battery_plan(candidates=[option], simulate=lambda _: [grid],
                    clock=lambda: 0., settings=settings,
                    candidate_settings=lambda _: {'profit_outlook': outlook})
                self.assertIsNone(result)
                self.assertEqual(reason, 'no_valid_plan')

    def test_weighted_grid_margin_does_not_count_retained_stock_as_profit(self):
        self.slots = [self.slot(0, solar=0, demand=0, tariff=.10),
                      self.slot(1, solar=0, demand=0, tariff=.30),
                      self.slot(2, solar=0, demand=1, tariff=.27)]
        windows = [self.window(0, 'laden_van_net'), self.window(1, 'laden_van_net'),
                   self.window(2, 'ontladen')]
        settings = dict(now=self.now, initial=0., capacity=2., max_charge=1.,
                        max_discharge=1., minimum_profit=.08)
        self.assertIsNone(evaluate_battery_plan(self.slots, windows, **settings))
        self.slots[2]['import_price'] = .28
        score = evaluate_battery_plan(self.slots, windows, **settings)
        self.assertAlmostEqual(score.unspent_grid_kwh, 1.)
        self.assertAlmostEqual(score.spent_grid_kwh, 1.)

    def test_mixed_discharge_keeps_separate_export_margin(self):
        windows = [self.window(0, 'laden_van_net'), self.window(1, 'ontladen_naar_net')]
        self.slots[0].update(solar_kwh=0., net_solar_kwh=-1., import_price=.20)
        self.slots[1].update(demand_kwh=1., net_solar_kwh=-1.)
        for home, export, accepted in ((.40, .25, False), (.15, .40, False),
                                       (.15, .55, False), (.25, .40, False), (.40, .28, True)):
            with self.subTest(home=home, export=export):
                self.slots[1].update(import_price=home, export_price=export)
                score = evaluate_battery_plan(self.slots, windows, now=self.now, initial=0.,
                    capacity=2., max_charge=2., max_discharge=2., minimum_profit=.08)
                self.assertEqual(score is not None, accepted)

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
        result = optimize_battery_plan(candidates=[('grid', 1)], simulate=lambda _: [charge],
            settings=dict(slots=self.slots, now=self.now, initial=0., capacity=2.,
                          max_charge=2., max_discharge=2.))
        self.assertEqual(result, (None, 'no_valid_plan'))

    def test_starting_full_does_not_count_as_solar_refill(self):
        self.slots[0].update(demand_kwh=1., solar_kwh=0., net_solar_kwh=-1.)
        self.slots[1].update(solar_kwh=.5, demand_kwh=0., net_solar_kwh=.5, export_price=-.50)
        discharge = self.window(0, 'ontladen')
        refill = self.window(1, 'laden_met_zonne_energie')
        result = optimize_battery_plan(candidates=[('solar', 1)], simulate=lambda _: [discharge, refill],
            settings=dict(slots=self.slots, now=self.now, initial=2., capacity=2.,
                          max_charge=2., max_discharge=2.))
        self.assertEqual(result, (None, 'no_valid_plan'))

    def test_active_grid_fill_cannot_discharge_before_it_reaches_full(self):
        self.slots = [self.slot(0, solar=0, demand=1, tariff=.50),
                      self.slot(1, solar=1, demand=0, tariff=.20),
                      self.slot(2, solar=0, demand=1, tariff=.50)]
        option = ('continuation', None)
        settings = dict(slots=self.slots, now=self.now, initial=1., capacity=2.,
                        max_charge=1., max_discharge=1., fallback_reserve=0.,
                        minimum_profit=.08)
        premature_discharge = self.window(0, 'ontladen')
        solar = self.window(1, 'laden_met_zonne_energie')
        later_discharge = self.window(2, 'ontladen')
        reasons = set()
        self.assertEqual(optimize_battery_plan(candidates=[option],
            simulate=lambda _: [premature_discharge, solar], clock=lambda: 0.,
            settings=settings, rejection_reasons=reasons), (None, 'no_valid_plan'))
        self.assertIn('unfinished_grid_fill_discharge', reasons)
        self.assertEqual(optimize_battery_plan(candidates=[option],
            simulate=lambda _: [solar, later_discharge], clock=lambda: 0., settings=settings)[0], option)

    def test_deadline_keeps_validated_candidate(self):
        result = optimize_battery_plan(candidates=[('solar', 1), ('solar', 2)],
            simulate=lambda source: [self.window(0, 'laden_met_zonne_energie'),
                                     self.window(1, 'ontladen')],
            settings=dict(slots=self.slots, now=self.now, initial=0., capacity=2.,
                          max_charge=2., max_discharge=2.),
            clock=iter([0., .01, .16]).__next__)
        self.assertEqual(result, (('solar', 1), 'time_budget'))

    def test_deadline_without_a_valid_candidate_issues_no_plan(self):
        result = optimize_battery_plan(candidates=[('grid', 1)], simulate=lambda _: None,
            settings=dict(slots=self.slots, now=self.now, initial=0., capacity=2.,
                          max_charge=2., max_discharge=2.),
            clock=iter([0., .01, .16]).__next__)
        self.assertEqual(result, (None, 'time_budget'))

    def test_publication_check_rejects_export_and_tries_next_candidate(self):
        options = [('grid', 1), ('grid', 2)]
        invalid = [self.window(0, 'laden_met_zonne_energie'), self.window(1, 'ontladen_naar_net')]
        valid = [self.window(0, 'laden_met_zonne_energie'), self.window(1, 'ontladen')]
        result = optimize_battery_plan(candidates=options,
            simulate=dict(zip(options, (invalid, valid))).__getitem__, clock=lambda: 0.,
            settings=dict(slots=self.slots, now=self.now, initial=0., capacity=2.,
                          max_charge=2., max_discharge=2., allow_export=False))
        self.assertEqual(result[0], options[1])

    def test_reserve_cannot_be_released_by_a_future_refill(self):
        windows = [self.window(0, 'ontladen_naar_net'), self.window(1, 'accu_uit')]
        reasons = []
        result = evaluate_battery_plan(self.slots, windows, now=self.now, initial=2., capacity=3.,
            max_charge=2., max_discharge=2., discharge_floor=0., fallback_reserve=1.,
            on_reject=reasons.append)
        self.assertIsNone(result)
        self.assertEqual(reasons, ['discharge_reserve'])

    def test_new_full_cycle_requires_its_refill_in_the_executable_prefix(self):
        self.slots[0].update(solar_kwh=0., net_solar_kwh=-1.5, demand_kwh=1.5)
        discharge = self.window(0, 'ontladen')
        refill = self.window(1, 'laden_van_net')
        settings = dict(now=self.now, initial=1.5, capacity=2., max_charge=2.,
                        max_discharge=2., fallback_reserve=1.)
        self.assertIsNone(evaluate_battery_plan(self.slots, [discharge, refill], **settings))
        settings['allow_refill_backed_discharge'] = True
        self.assertIsNone(evaluate_battery_plan(self.slots, [discharge], **settings))
        self.assertIsNotNone(evaluate_battery_plan(self.slots, [discharge, refill], **settings))

    def test_manual_rejection_tries_different_commands_even_if_original_scores_better(self):
        options = [('grid', 1), ('reserve_only', None)]
        original = [self.window(0, 'laden_met_zonne_energie'), self.window(1, 'ontladen')]
        settings = dict(slots=self.slots, now=self.now, initial=0., capacity=2.,
                        max_charge=2., max_discharge=2.)
        feedback = [{'vote': 'rejected', 'plan': {'planned_battery_mode_windows': original}}]
        result = optimize_battery_plan(candidates=options,
            simulate=dict(zip(options, (original, []))).__getitem__, clock=lambda: 0.,
            settings=settings, feedback=feedback)
        self.assertEqual(result[0], options[1])
        self.assertEqual(optimize_battery_plan(candidates=options[:1], simulate=lambda _: original,
            clock=lambda: 0., settings=settings, feedback=feedback), (None, 'no_feedback_alternative'))

        ticks = iter((0., 0., 1.))
        self.assertEqual(optimize_battery_plan(candidates=options, simulate=lambda _: original,
            clock=lambda: next(ticks), settings=settings, feedback=feedback), (None, 'feedback_search_timeout'))

    def test_idle_candidate_reports_no_commands(self):
        for standby in (False, True):
            with self.subTest(standby=standby):
                idle = self.window(0, 'accu_uit')
                windows = [{**idle, 'standby': standby}]
                self.assertEqual(optimize_battery_plan(candidates=[('reserve_only', None)],
                    simulate=lambda _: windows, clock=lambda: 0., settings=dict(
                        slots=self.slots, now=self.now, initial=0., capacity=2.,
                        max_charge=2., max_discharge=2.)), (('reserve_only', None), 'no_battery_action'))

    def test_standby_discharge_window_is_still_a_command(self):
        windows = [{**self.window(0, 'ontladen'), 'standby': True}]
        with patch.dict(optimize_battery_plan.__globals__, evaluate_battery_plan=Mock(
                return_value=BatteryScore(cost=0., peak=0., end=1., switches=1,
                                          incomplete_grid_fill=False, completed_solar_fill=False))):
            self.assertEqual(optimize_battery_plan(candidates=[('reserve_only', None)],
                simulate=lambda _: windows, clock=lambda: 0., settings={}),
                (('reserve_only', None), 'battery_cost_and_cloud_risk'))

    def test_old_feedback_breaks_ties_without_skipping_the_hard_checks(self):
        options = [('grid', 1), ('grid', 2)]
        pv_home = [self.window(0, 'laden_met_zonne_energie'), self.window(1, 'ontladen')]
        pv_export = [self.window(0, 'laden_met_zonne_energie'), self.window(1, 'ontladen_naar_net')]
        old = [{**w, **{k: (datetime.fromisoformat(w[k]) - timedelta(days=1)).isoformat()
                        for k in ('start', 'end')}} for w in pv_export]
        settings = dict(slots=self.slots, now=self.now, initial=0., capacity=2.,
                        max_charge=2., max_discharge=2.)
        for slot in self.slots:
            slot['export_price'] = slot['import_price']
        options_map = dict(zip(options, (pv_home, pv_export)))
        feedback = [{'vote': 'approved', 'plan': {'planned_battery_mode_windows': old}}]
        self.assertEqual(optimize_battery_plan(candidates=options, simulate=options_map.__getitem__,
            clock=lambda: 0., settings=settings)[0], options[0])
        self.assertEqual(optimize_battery_plan(candidates=options, simulate=options_map.__getitem__,
            clock=lambda: 0., settings=settings, feedback=feedback)[0], options[1])
        self.assertEqual(optimize_battery_plan(candidates=options, simulate=options_map.__getitem__,
            clock=lambda: 0., settings={**settings, 'allow_export': False}, feedback=feedback)[0], options[0])

    def test_night_grid_leaves_room_for_solar_and_full_charge(self):
        self.now = self.now.replace(hour=3)
        self.slots = [self.slot(0, solar=0, demand=0, tariff=.30),
                      self.slot(8, solar=1, demand=0, tariff=.35),
                      self.slot(15, solar=0, demand=2, tariff=.70)]
        grid = dict(self.window(0, 'laden_van_net'), end=(self.now + timedelta(minutes=30)).isoformat())
        solar = self.window(1, 'laden_met_zonne_energie')
        discharge = self.window(2, 'ontladen')
        options = [('grid', 1), ('night_topup', 2)]
        result = optimize_battery_plan(candidates=options,
            simulate=dict(zip(options, ([self.window(0, 'laden_van_net'), discharge],
                                        [grid, solar, discharge]))).__getitem__, clock=lambda: 0.,
            settings=dict(slots=self.slots, now=self.now, initial=0., capacity=2.,
                          max_charge=2., max_discharge=2.),
            topup_target=(self.slots[1]['start'], self.slots[1]['end'], 1.))
        self.assertEqual(result[0], options[1])

    def test_cheaper_clear_weather_cannot_hide_cloud_cost_or_lower_stock(self):
        incumbent = ('grid', 1)
        candidate = ('grid', 2)
        def score(cost, stock=1.):
            return BatteryScore(cost=cost, peak=2., end=stock, switches=2,
                                incomplete_grid_fill=False, completed_solar_fill=False)
        for clear, cloudy in ((score(-.9), score(-.1)), (score(-.9, .5), score(-.5)),
                              (score(-.9), score(-.5, .5))):
            with self.subTest(clear=clear, cloudy=cloudy):
                namespace = optimize_battery_plan.__globals__
                with patch.dict(namespace, evaluate_battery_plan=Mock(side_effect=[score(-.4), score(-.4), clear, cloudy])):
                    result = optimize_battery_plan(candidates=[incumbent, candidate],
                        simulate=lambda _: [self.window(0, 'ontladen')], settings={}, clock=lambda: 0.)
                self.assertEqual(result, (incumbent, 'battery_cost_and_cloud_risk'))


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
    def test_night_topup_is_planned_only_when_it_can_pay_and_leave_room_for_pv(self):
        now = datetime.fromisoformat('2026-11-25T01:00:00+01:00')

        def point(i):
            return dict(start=(now + timedelta(hours=i)).isoformat(),
                        end=(now + timedelta(hours=i + 1)).isoformat())

        values = [.35, .10, .10] + [.35] * 11 + [.85] * 5 + [.35] * 5
        for profitable, tight_budget in ((True, False), (False, False), (True, True)):
            with self.subTest(profitable=profitable, tight_budget=tight_budget):
                data = dict(
                    upcoming_energy_price_windows=[dict(point(i), price=v if profitable else .35, price_known=True)
                                                   for i, v in enumerate(values)],
                    upcoming_export_price_windows=[dict(point(i), price=max(.01, v - .10) if profitable else .25,
                                                         price_known=True) for i, v in enumerate(values)],
                    estimated_hourly_home_demand=[dict(point(i), estimated_kwh=2. if 14 <= i < 19 else 0.)
                                                  for i in range(len(values))],
                    estimated_hourly_solar_forecast=[dict(point(i), estimated_kwh=2.) for i in (10, 11)],
                )
                namespace = coordinator()._build_plan_for_horizon.__globals__
                optimizer = namespace['optimize_battery_plan']

                def search(**kwargs):
                    if tight_budget:
                        # Exactly one trial can finish before the deadline.
                        ticks = iter((0., 0.))
                        kwargs['clock'] = lambda: next(ticks, 60.)
                    return optimizer(**kwargs)

                with patch.dict(namespace, optimize_battery_plan=search):
                    _, slots, result = replay_full_plan(timestamp=now.isoformat(), fixture_data=data,
                        soc=30., discharging=True, reserve=20., isolated_cycles=True, snapshot_exports=True)
                if profitable:
                    # A fully checked top-up remains executable when the
                    # live search budget expires before all trials finish.
                    self.assertIn(result.battery_planner_engine_reason, ('night_topup_with_solar', 'time_budget'))
                    self.assertTrue(result.planned_grid_charge_windows)
                    self.assertTrue(assert_executable_plan(self, slots, result.planned_battery_mode_windows,
                                                           initial=3., reserve=2.)['saw_full'])
                else:
                    self.assertFalse(result.planned_grid_charge_windows)

    def test_rejected_published_plan_is_replaced_with_valid_reserve_plan(self):
        args = solar_scenario()
        _, _, initial = replay_full_plan(**args)
        self.assertTrue(initial.planned_solar_charge_windows)
        instance = coordinator()
        instance._charge_session_started = False
        instance._discharge_session_started = True
        instance._battery_plan_feedback = [{'vote': 'rejected', 'plan': {
            'planned_battery_mode_windows': initial.planned_battery_mode_windows}}]
        _, slots, alternative = replay_full_plan(instance=instance, **args)
        self.assertNotEqual(alternative.planned_battery_mode_windows, initial.planned_battery_mode_windows)
        self.assertNotIn(alternative.battery_planner_engine_reason,
                         ('no_feedback_alternative', 'publication_mismatch', 'optimizer_error'))
        assert_executable_plan(self, slots, alternative.planned_battery_mode_windows, initial=8., reserve=6.)

    def test_final_replay_mismatch_never_reaches_the_schedule(self):
        namespace = coordinator()._build_plan_for_horizon.__globals__
        with patch.dict(namespace, _LOGGER=Mock()):
            planner_class = type(coordinator())
            clip = planner_class._executable_battery_windows

            def replay(self, windows, now):
                checked = clip(self, windows, now)
                return checked if not getattr(self, '_test_alter_replay', False) else []

            select = planner_class._select_battery_plan

            def select_then_alter(self, **kwargs):
                result = select(self, **kwargs)
                self._test_alter_replay = True
                return result

            with patch.object(planner_class, '_executable_battery_windows', replay), \
                    patch.object(planner_class, '_select_battery_plan', select_then_alter):
                _, _, result = replay_full_plan(**solar_scenario())
        self.assertEqual(result.battery_planner_engine_reason, 'publication_mismatch')
        self.assertFalse(result.planned_battery_mode_windows)
        self.assertEqual(result.battery_strategy, 'accu_uit')

    def test_v2_jointly_selects_grid_solar_home_and_export(self):
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
        now, slots, v2 = replay_full_plan(**args)
        self.assertEqual(v2.battery_planner_engine, 'v2', v2.battery_planner_engine_reason)
        self.assertTrue(v2.planned_grid_charge_windows)
        self.assertTrue({'laden_van_net', 'laden_met_zonne_energie', 'ontladen', 'ontladen_naar_net'}
                        <= {w['mode'] for w in v2.planned_battery_mode_windows})
        exports = {row['start']: row['price'] for row in data['upcoming_export_price_windows']}
        for slot in slots:
            slot['export_price'] = exports[slot['start'].isoformat()]
        trace = assert_executable_plan(self, slots, v2.planned_battery_mode_windows, initial=6., reserve=6.)
        self.assertTrue(trace['saw_full'])
    def test_v2_selects_executable_solar_refill_with_or_without_old_options(self):
        args = solar_scenario()
        _, slots, default = replay_full_plan(**args)
        self.assertEqual(default.battery_planner_engine, 'v2')
        self.assertFalse(default.planned_grid_charge_windows)
        trace = assert_executable_plan(self, slots, default.planned_battery_mode_windows, initial=8., reserve=6.)
        self.assertTrue(trace['saw_full'])
        for flags in ({'optimizer_v2': True}, {'optimizer_solar_only': True}):
            with self.subTest(flags=flags):
                _, _, legacy = replay_full_plan(**args, **flags)
                self.assertEqual(legacy.battery_planner_engine, 'v2')
                self.assertEqual(legacy.planned_battery_mode_windows, default.planned_battery_mode_windows)

    def test_unexpected_error_produces_no_new_commands(self):
        namespace = coordinator()._build_plan_for_horizon.__globals__
        broken_optimizer = Mock(side_effect=ValueError('bad input'))
        with patch.dict(namespace, optimize_battery_plan=broken_optimizer, _LOGGER=Mock()):
            _, _, result = replay_full_plan(**solar_scenario())
        broken_optimizer.assert_called()
        self.assertEqual((result.battery_planner_engine, result.battery_planner_engine_reason), ('v2', 'optimizer_error'))
        self.assertEqual(result.battery_strategy, 'accu_uit')
        self.assertFalse(result.planned_battery_mode_windows)
        self.assertFalse(result.planned_grid_charge_windows)
        self.assertFalse(result.planned_solar_charge_windows)

    def test_replay_all_saved_battery_snapshots(self):
        snapshots = sorted(Path(__file__).with_name('fixtures').glob('battery_*.json'))
        self.assertGreaterEqual(len(snapshots), 36)
        for path in snapshots:
            with self.subTest(snapshot=path.stem):
                raw = json.loads(path.read_text())
                committed_full = False
                if path.stem == 'battery_2026_11_15_outlook':
                    replay = test_battery_nov15_brief_discharge.November15LocalCaptureReplayTest()
                    replay.capture = raw
                    v2 = replay.replay()
                    slots = replay.replay_slots
                    soc = raw['plan']['battery_soc_percent']
                    config = raw['effective_config']
                    reserve, minimum = config['battery_no_charge_min_soc_percent'], config['battery_min_soc_percent']
                    max_charge, max_discharge = config['battery_max_charge_kw'], config['battery_max_discharge_kw']
                    state = raw['coordinator_state']
                    committed_full = (state['battery_cycle_discharge_floor_kwh'] == 0.
                                      and state['discharge_session_started']
                                      and not state['charge_session_started']
                                      and not state.get('solar_reserve_relaxation', False))
                elif 'import_prices' in raw:
                    now, slots, v2 = replay_evening_gap()
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
                    now, slots, v2 = replay_full_plan(**args)
                self.assertEqual(v2.battery_planner_engine, 'v2')
                self.assertNotIn(v2.battery_planner_engine_reason, ('disabled', 'optimizer_error'))
                assert_executable_plan(self, slots, v2.planned_battery_mode_windows,
                    initial=soc/10, reserve=reserve/10, minimum=minimum/10,
                    max_charge=max_charge, max_discharge=max_discharge, committed_full=committed_full,
                    allow_refill_backed_discharge=True)
