"""Profitable NEW PV may refill partially without releasing the committed floor."""
import unittest
from datetime import datetime
import test_battery_full_solar_refill as full_solar
from test_battery_energy_accounting import energy_trace


class PartialSolarRefillTest(unittest.TestCase):
    setUp = full_solar.FullSolarRefillTest.setUp
    slots = full_solar.FullSolarRefillTest.slots
    plan = full_solar.FullSolarRefillTest.plan

    def test_observed_grid_phase_replaces_previous_solar_source(self):
        from datetime import timedelta
        from types import SimpleNamespace
        self.c.config_entry = SimpleNamespace(data={}, options={
            'battery_capacity_kwh': 10., 'battery_min_soc_percent': 20.})
        self.c._battery_charge_source = 'laden_met_zonne_energie'
        previous = dict(at=self.now.isoformat(), energy=4.8, actions=[dict(
            start=self.now.isoformat(), end=(self.now+timedelta(hours=1)).isoformat(),
            mode='laden_van_net', solar_kw=0., grid_kw=1.)])
        self.c._reconcile_elapsed_battery_phase(previous, self.now+timedelta(hours=1), 78.)
        self.assertEqual(self.c._battery_charge_source, 'laden_van_net')
        self.c._release_solar_charge_hold()
        self.assertTrue(self.c._charge_session_started)

    def test_partial_grid_and_legacy_unknown_source_keep_full_hold(self):
        for source in ('laden_van_net', None):
            with self.subTest(source=source):
                self.setUp()
                self.c._battery_charge_source = source
                self.c._charge_session_started = True
                self.c._discharge_session_started = False
                _, _, windows, _ = self.plan(self.slots(solar=0.), energy=5.8)
                self.assertFalse(any(w['mode'].startswith('ontladen') for w in windows))

    def test_solar_tail_does_not_release_incomplete_grid_phase(self):
        from datetime import timedelta
        from types import SimpleNamespace
        self.c.config_entry = SimpleNamespace(data={}, options={
            'battery_capacity_kwh': 10., 'battery_min_soc_percent': 20.})
        self.c._battery_charge_source = 'laden_van_net'
        self.c._charge_session_started = True
        previous = dict(at=self.now.isoformat(), energy=4.8, actions=[dict(
            start=self.now.isoformat(), end=(self.now+timedelta(hours=1)).isoformat(),
            mode='laden_met_zonne_energie', solar_kw=1., grid_kw=0.)])
        self.c._reconcile_elapsed_battery_phase(previous, self.now+timedelta(hours=1), 78.)
        self.c._release_solar_charge_hold()
        self.assertEqual(self.c._battery_charge_source, 'laden_van_net')
        self.assertTrue(self.c._charge_session_started)

    def test_partial_grid_target_remains_rejected(self):
        slots = self.slots(solar=0.)
        slots[0]['import_price'] = .05
        _, _, windows, _ = self.plan(slots, energy=4.)
        self.assertFalse(any(w['mode'] == 'laden_van_net' for w in windows))

    def test_partial_pv_is_not_exported_below_its_opportunity_margin(self):
        from datetime import timedelta
        self.c._battery_grid_charge_price = .01
        self.c._battery_margin_state = dict(at=self.now.isoformat(), energy=4.8,
            reserve=4., lots=[[4.8, .01]], credit=0., actions=[])
        slots = self.slots(solar=1., demand=4.)
        slots[1].update(export_price=.3, import_price=.1, net_solar_kwh=0.)
        slots.insert(2, dict(slots[0], start=self.now+timedelta(hours=3),
                             end=self.now+timedelta(hours=4)))
        slots[3]['start'] = self.now+timedelta(hours=4)
        slots[3]['end'] = self.now+timedelta(hours=5)
        _, _, windows, _ = self.plan(slots)
        self.assertTrue(any(w['mode'] == 'laden_met_zonne_energie' for w in windows))
        self.assertFalse(any(w['mode'] == 'ontladen_naar_net' and
                             datetime.fromisoformat(w['start']) < self.now+timedelta(hours=4)
                             for w in windows))

    def test_partial_solar_at_reserve_also_allows_guarded_discharge(self):
        slots = self.slots(solar=1.)
        _, grid, windows, mode = self.plan(slots, energy=4.)
        self.assertFalse(grid)
        self.assertEqual(mode, 'laden_met_zonne_energie')
        self.assertTrue(any(w['mode'] == 'ontladen' for w in windows))
        trace = energy_trace(self.now, slots, windows, initial=4., max_charge=2.5)
        self.assertGreaterEqual(min(r[1] for r in trace), 4.-.005)
        self.assertLessEqual(max(r[1] for r in trace), 5.+.005)

    def test_opportunistic_partial_solar_does_not_invent_full_boundary(self):
        from datetime import timedelta
        slots = self.slots(solar=1.)
        for row in slots:
            row['start'] += timedelta(hours=1)
            row['end'] += timedelta(hours=1)
        slots.insert(0, dict(slots[0], start=self.now, end=self.now+timedelta(hours=1),
                             net_solar_kwh=-.8, import_price=.8))
        windows, mode = self.c._build_mode_windows_from_hourly_plan(
            slots=slots, now=self.now, planned_solar_charge_windows=[],
            planned_grid_charge_windows=[], initial_usable_energy_kwh=4.8,
            usable_capacity_kwh=8., average_price=.4, average_export_price=.263,
            max_charge_kw=2.5, max_discharge_kw=3., battery_min_profit=.1,
            no_charge_reserve_kwh=4.)
        self.assertEqual(mode, 'ontladen')
        self.assertTrue(any(w['mode'] == 'laden_met_zonne_energie' for w in windows))
        self.assertTrue(any(w['mode'] == 'ontladen' and
                            datetime.fromisoformat(w['start']) >= self.now+timedelta(hours=3)
                            for w in windows))
        self.assertEqual(self.c._battery_cycle_discharge_floor_kwh, 4.)
        trace = energy_trace(self.now, slots, windows, initial=4.8, max_charge=2.5)
        self.assertGreaterEqual(min(r[1] for r in trace), 4.-.005)
        self.assertLessEqual(max(r[1] for r in trace), 5.+.005)

    def test_partial_pv_export_only_outlets_use_shared_known_allowed_power(self):
        for allowed, known, value in ((True, True, .5), (False, True, .5),
                                      (True, False, .5), (True, True, .3)):
            with self.subTest(allowed=allowed, known=known, value=value):
                self.setUp()
                self.c._cycle_export_planning = allowed
                slots = self.slots(solar=1., demand=0.)
                for row in slots[2:]:
                    row.update(import_price=.1, export_price=value, price_known=known)
                _, _, windows, _ = self.plan(slots)
                admitted = allowed and known and value >= .263+.1
                self.assertEqual(any(w['mode'] == 'laden_met_zonne_energie'
                                     for w in windows), admitted)
                if admitted:
                    from partial_solar_assertions import assert_executable_plan
                    checked = assert_executable_plan(self, slots, windows,
                        initial=6.8, reserve=6., max_charge=2.5, margin=.1,
                        initial_cost=.271)
                    self.assertGreater(checked['sold'], 0.)
                    self.assertFalse(checked['saw_full'])

    def test_partial_solar_economics_and_competing_stock_controls(self):
        for case in ('unprofitable', 'scarce', 'unknown'):
            with self.subTest(case=case):
                self.setUp()
                slots = self.slots(solar=1., demand=1. if case == 'scarce' else 4.,
                                   price=.35 if case == 'unprofitable' else .44)
                if case == 'unknown':
                    for row in slots[2:]:
                        row['price_known'] = False
                _, _, windows, _ = self.plan(slots)
                self.assertFalse(any(w['mode'].startswith('laden_') for w in windows))

    def test_forecast_shrink_measured_partial_idle_and_json_restore(self):
        import json
        from types import SimpleNamespace
        from unittest.mock import patch
        from datetime import timedelta
        from test_battery_energy_accounting import replay_full_plan
        slots = self.slots()
        self.c._battery_margin_state = dict(at=self.now.isoformat(), energy=4.8,
            reserve=0., lots=[[4.8, .271]], credit=-.2, actions=[])
        def replan(soc):
            for row in slots:
                row.update(solar_kwh=max(0., row['net_solar_kwh']),
                           demand_kwh=max(0., -row['net_solar_kwh']))
            namespace = self.c._build_plan_for_horizon.__globals__
            with patch('test_battery_energy_accounting.feedback_slots', return_value=(self.now, slots)), \
                 patch.dict(namespace, {'build_energy_balance_slots': lambda **kw: slots}), \
                 patch.object(self.c, '_store_battery_cycle_state_snapshot'):
                return replay_full_plan(instance=self.c, soc=soc, reserve=60., max_charge=2.5,
                    profit=.1, tax_deduction=.12)[2]
        self.assertEqual(replan(68.).battery_strategy, 'laden_met_zonne_energie')
        self.assertTrue(self.c._charge_session_started)
        # Observed partial gain, no remaining PV, idle before the profitable evening.
        self.now += timedelta(hours=2)
        slots = [dict(slots[2], start=self.now, end=self.now+timedelta(hours=1),
                      net_solar_kwh=0., import_price=.3),
                 dict(slots[2], start=self.now+timedelta(hours=1),
                      end=self.now+timedelta(hours=2))]
        self.c.hass = SimpleNamespace(data={})
        self.c.config_entry = SimpleNamespace(entry_id='partial-solar', data={}, options={})
        self.c._store_battery_cycle_state_snapshot(self.now)
        saved = json.loads(json.dumps(self.c.hass.data))
        from test_battery_energy_accounting import coordinator
        restored = coordinator()
        restored._charge_session_started = False
        restored._discharge_session_started = False
        restored._battery_cycle_state_initialized = False
        restored.hass = SimpleNamespace(data=saved)
        restored.config_entry = self.c.config_entry
        with patch.object(restored._restore_recent_battery_cycle_state.__globals__['dt_util'],
                          'now', return_value=self.now):
            restored._restore_recent_battery_cycle_state()
        self.c = restored
        result = replan(78.)
        self.assertEqual(result.battery_strategy, 'accu_uit')
        self.assertTrue(any(w['mode'] == 'ontladen' for w in result.planned_battery_mode_windows))
        self.assertFalse(self.c._charge_session_started)
        self.assertEqual(self.c._battery_cycle_discharge_floor_kwh, 4.)
        self.assertEqual(self.c._battery_margin_state['credit'], -.2)
        for _ in range(2):
            result = replan(78.)
            self.assertTrue(any(w['mode'] == 'ontladen' for w in result.planned_battery_mode_windows))
            self.assertEqual(self.c._battery_cycle_discharge_floor_kwh, 4.)

    def test_profitable_partial_solar_emits_charge_then_guarded_discharge(self):
        slots = self.slots(solar=1.)
        solar, grid, windows, mode = self.plan(slots)
        self.assertTrue(solar)
        self.assertFalse(grid)
        self.assertEqual(mode, 'laden_met_zonne_energie')
        trace = energy_trace(self.now, slots, windows, initial=4.8, max_charge=2.5)
        self.assertAlmostEqual(max(r[1] for r in trace), 5.8, delta=.005)
        self.assertGreaterEqual(min(r[1] for r in trace), 4.-.005)
        self.assertTrue(any(r[2] == 'ontladen' for r in trace))
        self.assertEqual(self.c._battery_cycle_discharge_floor_kwh, 4.)
        for w in windows:
            if w['mode'].startswith('ontladen'):
                start = datetime.fromisoformat(w['start'])
                slot = next(s for s in slots if s['start'] <= start < s['end'])
                self.assertGreaterEqual(slot['import_price'], .271 + .1)


if __name__ == '__main__':
    unittest.main()
