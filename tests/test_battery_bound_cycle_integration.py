"""Independent physical checks through the complete production coordinator."""
from datetime import datetime, timedelta, timezone
import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from test_battery_energy_accounting import coordinator, energy_trace, replay_full_plan

NOW = datetime(2026, 11, 14, 16, tzinfo=timezone.utc)


def scenario(count=16, demand_hours=12, charging=False, unknown_from=None):
    rows = []
    for i in range(count):
        home = 1. if i < demand_hours and not charging else 0.
        if charging and 4 <= i < 12:
            home = 1.
        rows.append(dict(start=NOW + timedelta(hours=i), end=NOW + timedelta(hours=i+1),
            hours=1., import_price=.10 if charging and i < 4 else .80,
            export_price=.01, price_known=unknown_from is None or i < unknown_from,
            solar_kwh=0., demand_kwh=home, net_solar_kwh=-home))
    return rows


def replay(c, slots, soc=100., reserve=70., now=NOW):
    with patch('test_battery_energy_accounting.feedback_slots', return_value=(now, slots)):
        return replay_full_plan('2026_09_19_morning', now.isoformat(), soc=soc,
            reserve=reserve, profit=.10, max_charge=2.5,
            tax_deduction=.79, external_demand=[dict(start=s['start'].isoformat(),
                end=s['end'].isoformat(), estimated_kwh=s['demand_kwh']) for s in slots],
            instance=c, isolated_cycles=True)[2]


class BoundCycleIntegrationTest(unittest.TestCase):
    def new(self, charging=False):
        c = coordinator()
        c._charge_session_started = charging
        c._discharge_session_started = not charging
        c._battery_cycle_discharge_floor_kwh = None
        return c

    def test_full_pipeline_prefers_complete_cycle_and_preserves_observed_phase(self):
        c, slots = self.new(charging=True), scenario(charging=True)
        result = replay(c, slots, soc=20.)
        trace = energy_trace(NOW, slots, result.planned_battery_mode_windows, initial=2., max_charge=2.5)
        self.assertTrue(result.planned_grid_charge_windows)
        self.assertAlmostEqual(max(e for _, e, _ in trace), 10., delta=.005)
        self.assertAlmostEqual(trace[-1][1], 2., delta=.005)
        self.assertEqual(c._battery_cycle_discharge_floor_kwh, 0.)
        self.assertTrue(c._charge_session_started, 'future simulated completion is not observed full')
        self.assertFalse(c._discharge_session_started)
        first_discharge = next(datetime.fromisoformat(w['start']) for w in result.planned_battery_mode_windows
                               if w['mode'].startswith('ontladen'))
        self.assertGreaterEqual(first_discharge, NOW+timedelta(hours=4))

    def test_near_full_live_measurement_keeps_charging_phase(self):
        c = self.new(charging=True)
        replay(c, scenario(charging=True), soc=99.7)
        self.assertTrue(c._charge_session_started)
        self.assertFalse(c._discharge_session_started)

    def test_source_selection_does_not_release_boundary_above_mode_tolerance(self):
        c = self.new()
        c._battery_cycle_discharge_floor_kwh = 5.
        result = replay(c, scenario(charging=True), soc=70.07)
        self.assertTrue(c._discharge_session_started)
        self.assertFalse(c._charge_session_started)
        self.assertEqual(c._battery_cycle_discharge_floor_kwh, 5.)
        self.assertFalse(result.battery_strategy.startswith('laden_'))

    def test_full_and_configured_70_percent_reserve_survive_forecast_updates_and_json_restore(self):
        for complete in (False, True):
            with self.subTest(complete=complete):
                c = self.new()
                slots = scenario(demand_hours=12 if complete else 4)
                baseline = replay(c, slots)
                floor = 0. if complete else 5.
                self.assertEqual(c._battery_cycle_discharge_floor_kwh, floor)
                trace = energy_trace(NOW, slots, baseline.planned_battery_mode_windows, initial=10., max_charge=2.5)
                self.assertAlmostEqual(min(e for _, e, _ in trace), floor+2., delta=.005)
                # Observed partial progress, followed by the opposite forecast.
                changed = scenario(demand_hours=4 if complete else 12)
                replay(c, changed, soc=90.)
                self.assertEqual(c._battery_cycle_discharge_floor_kwh, floor)
                self.assertTrue(c._discharge_session_started)
                c.hass = SimpleNamespace(data={})
                c.config_entry.entry_id = 'bound-integration'
                c._store_battery_cycle_state_snapshot(NOW)
                saved = json.loads(json.dumps(c.hass.data))
                restarted = self.new()
                restarted.hass = SimpleNamespace(data=saved)
                restarted.config_entry = SimpleNamespace(entry_id='bound-integration')
                clock = restarted._restore_recent_battery_cycle_state.__globals__['dt_util']
                with patch.object(clock, 'now', return_value=NOW):
                    restarted._restore_recent_battery_cycle_state()
                del restarted.hass
                del c.hass
                self.assertEqual(restarted._battery_cycle_discharge_floor_kwh, floor)
                actual = replay(restarted, changed, soc=90.)
                expected = replay(c, changed, soc=90.)
                self.assertEqual(actual.planned_battery_mode_windows, expected.planned_battery_mode_windows)
                self.assertEqual(actual.estimated_battery_mode_windows, expected.estimated_battery_mode_windows)

    def restore_after_downtime(self, c, minutes):
        c.hass = SimpleNamespace(data={})
        c.config_entry.entry_id = 'bound-downtime'
        c._store_battery_cycle_state_snapshot(NOW)
        saved = json.loads(json.dumps(c.hass.data))
        del c.hass
        restarted = self.new()
        # Match constructor defaults, not this suite's active-cycle fixture.
        restarted._charge_session_started = False
        restarted._discharge_session_started = False
        restarted._battery_cycle_state_initialized = False
        restarted.hass = SimpleNamespace(data=saved)
        restarted.config_entry = SimpleNamespace(entry_id='bound-downtime')
        clock = restarted._restore_recent_battery_cycle_state.__globals__['dt_util']
        with patch.object(clock, 'now', return_value=NOW+timedelta(minutes=minutes)):
            restarted._restore_recent_battery_cycle_state()
        del restarted.hass
        return restarted

    def test_committed_discharge_survives_long_downtime_without_early_refill(self):
        for minutes in (31, 24*60, 7*24*60):
            for complete in (True, False):
                with self.subTest(minutes=minutes, complete=complete):
                    c = self.new()
                    replay(c, scenario(demand_hours=12 if complete else 4))
                    floor = 0. if complete else 5.
                    restored = self.restore_after_downtime(c, minutes)
                    self.assertEqual(restored._battery_cycle_discharge_floor_kwh, floor)
                    self.assertTrue(restored._discharge_session_started)
                    self.assertFalse(restored._charge_session_started)
                    now = NOW+timedelta(minutes=minutes)
                    slots = scenario(charging=True)
                    for slot in slots:
                        slot['start'] += now-NOW
                        slot['end'] += now-NOW
                    for soc in (90., 70.07) if not complete else (90., 70.):
                        result = replay(restored, slots, soc=soc, now=now)
                        self.assertEqual(restored._battery_cycle_discharge_floor_kwh, floor)
                        self.assertTrue(restored._discharge_session_started)
                        self.assertFalse(restored._charge_session_started)
                        self.assertFalse(result.battery_strategy.startswith('laden_'))
                    # Actual completion during downtime, not elapsed time, may
                    # release the old discharge and admit a new fill.
                    replay(restored, slots, soc=20. if complete else 70., now=now)
                    self.assertFalse(restored._discharge_session_started)
                    self.assertTrue(restored._charge_session_started)

    def test_committed_charge_survives_long_downtime_until_measured_full(self):
        for minutes in (31, 24*60, 7*24*60):
            with self.subTest(minutes=minutes):
                c = self.new(charging=True)
                replay(c, scenario(charging=True), soc=20.)
                restored = self.restore_after_downtime(c, minutes)
                self.assertEqual(restored._battery_cycle_discharge_floor_kwh, 0.)
                self.assertTrue(restored._charge_session_started)
                self.assertFalse(restored._discharge_session_started)
                now = NOW+timedelta(minutes=minutes)
                slots = scenario(demand_hours=4)
                for slot in slots:
                    slot['start'] += now-NOW
                    slot['end'] += now-NOW
                result = replay(restored, slots, soc=90., now=now)
                self.assertTrue(restored._charge_session_started)
                self.assertFalse(restored._discharge_session_started)
                self.assertFalse(any(w['mode'].startswith('ontladen')
                                     for w in result.planned_battery_mode_windows))
                replay(restored, slots, soc=100., now=now)
                self.assertFalse(restored._charge_session_started)
                self.assertTrue(restored._discharge_session_started)

    def test_executable_outlook_seam_preserves_each_selected_boundary(self):
        for complete in (False, True):
            with self.subTest(complete=complete):
                c = self.new()
                slots = scenario(demand_hours=12, unknown_from=8)
                if not complete:
                    for s in slots[3:8]:
                        s.update(demand_kwh=0., net_solar_kwh=0.)
                result = replay(c, slots)
                self.assertTrue(result.planned_battery_mode_windows)
                self.assertTrue(result.estimated_battery_mode_windows)
                self.assertEqual(result.planned_battery_mode_windows[-1]['end'],
                                 result.estimated_battery_mode_windows[0]['start'])
                floor = 0. if complete else 5.
                self.assertEqual(c._battery_cycle_discharge_floor_kwh, floor)
                trace = energy_trace(NOW, slots,
                    result.planned_battery_mode_windows + result.estimated_battery_mode_windows,
                    initial=10., max_charge=2.5)
                self.assertGreaterEqual(min(e for _, e, _ in trace), floor+2.-.005)
                self.assertLessEqual(max(e for _, e, _ in trace), 10.005)


if __name__ == '__main__':
    unittest.main()
