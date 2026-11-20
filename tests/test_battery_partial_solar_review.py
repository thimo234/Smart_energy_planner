"""Full coordinator regressions for partial-PV review blockers."""
import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import test_battery_full_solar_refill as fixture
from partial_solar_assertions import assert_executable_plan
from test_battery_energy_accounting import coordinator, replay_full_plan, energy_trace


class PartialSolarReviewTest(unittest.TestCase):
    def setUp(self):
        self.t = fixture.FullSolarRefillTest()
        self.t.setUp()

    def full_plan(self, slots):
        t = self.t
        for row in slots:
            row.update(solar_kwh=max(0., row['net_solar_kwh']),
                       demand_kwh=max(0., -row['net_solar_kwh']))
        namespace = t.c._build_plan_for_horizon.__globals__
        with patch('test_battery_energy_accounting.feedback_slots', return_value=(t.now, slots)), \
             patch.object(t.c, '_store_battery_cycle_state_snapshot', return_value=None), \
             patch.dict(namespace, {'build_energy_balance_slots': lambda **kw: slots}):
            return replay_full_plan(instance=t.c, soc=68., reserve=60., max_charge=2.5,
                profit=.1, tax_deduction=.12)[2].planned_battery_mode_windows

    def restore(self):
        t = self.t
        t.c.hass = SimpleNamespace(data={})
        t.c.config_entry = SimpleNamespace(entry_id='partial-review')
        t.c._store_battery_cycle_state_snapshot(t.now)
        saved = json.loads(json.dumps(t.c.hass.data))
        c = coordinator()
        c._charge_session_started = c._discharge_session_started = False
        c._battery_cycle_state_initialized = False
        c.hass = SimpleNamespace(data=saved)
        c.config_entry = t.c.config_entry
        with patch.object(c._restore_recent_battery_cycle_state.__globals__['dt_util'],
                          'now', return_value=t.now):
            c._restore_recent_battery_cycle_state()
        t.c = c

    def test_unfinished_grid_hold_survives_partial_solar_tail_refresh_and_json(self):
        t = self.t
        t.c._charge_session_started = True
        t.c._discharge_session_started = False
        t.c._battery_charge_source = 'laden_van_net'
        for stage in ('first', 'refresh', 'json'):
            if stage == 'json':
                self.restore()
            with self.subTest(stage=stage):
                windows = self.full_plan(t.slots(solar=1.))
                self.assertTrue(any(w['mode'].startswith('laden') for w in windows))
                self.assertFalse(any(w['mode'].startswith('ontladen') for w in windows), windows)
                self.assertTrue(t.c._charge_session_started)
                self.assertEqual(t.c._battery_charge_source, 'laden_van_net')

    def test_grid_hold_releases_only_after_integrated_full_solar_tail(self):
        t = self.t
        t.c._charge_session_started = True
        t.c._discharge_session_started = False
        t.c._battery_charge_source = 'laden_van_net'
        slots = t.slots(solar=4.)
        windows = self.full_plan(slots)
        trace = energy_trace(t.now, slots, windows, initial=4.8, max_charge=2.5)
        self.assertAlmostEqual(max(r[1] for r in trace), 8., delta=.005)
        self.assertTrue(any(w['mode'].startswith('ontladen') for w in windows))
        self.assertTrue(t.c._charge_session_started, 'forecast full is not observed full')

    def test_mixed_export_cannot_finance_pv_when_household_leg_fails(self):
        t = self.t
        t.c._cycle_export_planning = True
        slots = t.slots(solar=1., demand=.2)
        for row in slots[2:]:
            row.update(import_price=.1, export_price=.5)
        for stage in ('first', 'refresh', 'json'):
            if stage == 'json':
                self.restore()
            with self.subTest(stage=stage):
                windows = self.full_plan(slots)
                self.assertFalse(any(w['mode'].startswith('laden') for w in windows), windows)
                self.assertFalse(any(w['mode'].startswith('ontladen') for w in windows),
                                 'household guard must not be waived')

    def test_export_only_and_profitable_mixed_outlets_still_finance_partial_pv(self):
        for demand, home_price in ((0., .1), (.2, .4)):
            with self.subTest(demand=demand):
                self.setUp()
                self.t.c._cycle_export_planning = True
                slots = self.t.slots(solar=1., demand=demand)
                for row in slots[2:]:
                    row.update(import_price=home_price, export_price=.5)
                windows = self.full_plan(slots)
                self.assertTrue(any(w['mode'].startswith('laden') for w in windows), windows)
                self.assertTrue(any(w['mode'] == 'ontladen_naar_net' for w in windows), windows)
                assert_executable_plan(self, slots, windows, initial=6.8, reserve=6.,
                    max_charge=2.5, margin=.1, initial_cost=.271, require_new_pv_profit=True)

    def test_grid_hold_survives_solar_window_starting_inside_slot(self):
        t = self.t
        t.c._charge_session_started = True
        t.c._discharge_session_started = False
        t.c._battery_charge_source = 'laden_van_net'
        slots = t.slots(solar=1.)
        solar, grid = t.c._plan_charge_windows_for_horizon(slots=slots, now=t.now,
            usable_capacity_kwh=8., current_remaining_capacity_kwh=3.2,
            max_charge_kw=2.5, max_discharge_kw=3., battery_min_profit=.1,
            no_charge_reserve_kwh=4.)
        self.assertTrue(solar)
        from datetime import timedelta
        solar[0]['start'] = (t.now+timedelta(minutes=15)).isoformat()
        windows, _ = t.c._build_mode_windows_from_hourly_plan(slots=slots, now=t.now,
            planned_solar_charge_windows=solar, planned_grid_charge_windows=grid,
            initial_usable_energy_kwh=4.8, usable_capacity_kwh=8., average_price=.4,
            average_export_price=.263, max_charge_kw=2.5, max_discharge_kw=3.,
            battery_min_profit=.1, no_charge_reserve_kwh=4.)
        self.assertTrue(any(w['mode'].startswith('laden') for w in windows))
        self.assertFalse(any(w['mode'].startswith('ontladen') for w in windows), windows)

    def test_solar_only_partial_still_discharges(self):
        slots = self.t.slots(solar=1.)
        windows = self.full_plan(slots)
        self.assertTrue(any(w['mode'].startswith('laden') for w in windows))
        self.assertTrue(any(w['mode'].startswith('ontladen') for w in windows))
        trace = energy_trace(self.t.now, slots, windows, initial=4.8, max_charge=2.5)
        self.assertLess(max(r[1] for r in trace), 8.-.005)
        self.assertGreaterEqual(min(r[1] for r in trace), 4.-.005)
        assert_executable_plan(self, slots, windows, initial=6.8, reserve=6.,
            max_charge=2.5, margin=.1, initial_cost=.271, require_new_pv_profit=True)
