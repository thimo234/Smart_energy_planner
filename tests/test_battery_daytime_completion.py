"""Unfinished fills need executable outlets before grid imports may continue."""
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch
import unittest

from test_battery_energy_accounting import coordinator, energy_trace, replay_full_plan


class DaytimeCompletionTest(unittest.TestCase):
    def evening_coordinator(self):
        c = coordinator()
        c._battery_grid_charge_price = .211  # Explicit historical-cost assumption.
        c._active_charge_phase_mode = 'laden_van_net'
        c._active_charge_phase_end = datetime.fromisoformat('2026-10-06T01:45:00+02:00')
        return c

    def assert_wait_for_solar_completion(self, now, slots, result, initial):
        """No executable grid outlet: hold the fill until real PV can finish it."""
        commands = result.planned_battery_mode_windows
        preview = result.estimated_battery_mode_windows
        windows = commands + preview
        self.assertFalse(result.planned_grid_charge_windows)
        self.assertFalse(any(w['mode'] == 'laden_van_net' for w in windows))
        self.assertEqual(result.battery_strategy, 'accu_uit')
        for before, after in zip(windows, windows[1:]):
            self.assertLessEqual(datetime.fromisoformat(before['end']),
                                 datetime.fromisoformat(after['start']))
        solar = [w for w in windows if w['mode'] == 'laden_met_zonne_energie']
        self.assertTrue(solar, 'rejecting grid must preserve the usable solar completion')
        self.assertTrue(all(w['start'].startswith('2026-10-06') for w in solar))
        self.assertTrue(all(w['mode'] == 'accu_uit' for w in windows
                            if w['start'] < solar[0]['start']))
        first_discharge = next(w for w in windows if w['mode'].startswith('ontladen'))
        self.assertLessEqual(solar[-1]['end'], first_discharge['start'])
        fill_trace = energy_trace(now, slots,
            [w for w in windows if w['end'] <= solar[-1]['end']],
            initial=initial, max_charge=2.5)
        self.assertAlmostEqual(fill_trace[-1][1], 10., delta=.005)
        trace = energy_trace(now, slots, windows, initial=initial, max_charge=2.5)
        self.assertGreaterEqual(min(e for _, e, _ in trace), 5.995)
        self.assertLessEqual(max(e for _, e, _ in trace), 10.005)
        self.assertAlmostEqual(trace[-1][1], 6., delta=.005)
        self.assertTrue(preview)
        self.assertTrue(all(w['mode'] == 'accu_uit' for w in preview))

    def test_running_fill_rejects_unsafe_grid_and_waits_for_solar(self):
        c = self.evening_coordinator()
        now, slots, result = self.replay('2026-10-05T16:02:41.391142+02:00', 93, c,
                                       snapshot='2026_10_05_evening_discharge')
        # An observed running grid fill is not permission to buy imports whose
        # outlets the acquisition-plus-minimum execution guard would refuse.
        self.assert_wait_for_solar_completion(now, slots, result, initial=9.3)
        self.assertTrue(c._charge_session_started)

    def test_rejected_evening_fill_waits_through_updates_and_restart(self):
        c = self.evening_coordinator()
        now = datetime.fromisoformat('2026-10-05T16:02:41.391142+02:00')
        end = datetime.fromisoformat('2026-10-06T09:00:00+02:00')
        energy = peak = 9.3
        discharged = False
        for index in range(44):
            _, slots, result = self.replay(now.isoformat(), energy*10, c,
                                          snapshot='2026_10_05_evening_discharge')
            if index == 9:
                c.hass = SimpleNamespace(data={})
                c.config_entry.entry_id = 'battery'
                c._store_battery_cycle_state_snapshot(now)
                restarted = coordinator()
                restarted.hass, restarted.config_entry = c.hass, c.config_entry
                with patch.object(c._restore_recent_battery_cycle_state.__globals__['dt_util'],
                                  'now', return_value=now):
                    restarted._restore_recent_battery_cycle_state()
                c = restarted
                del c.hass
                _, _, result = self.replay(now.isoformat(), energy*10, c,
                                           snapshot='2026_10_05_evening_discharge')
            self.assertFalse(result.planned_grid_charge_windows)
            self.assertFalse(any(w['mode'] == 'laden_van_net' for w in
                result.planned_battery_mode_windows + result.estimated_battery_mode_windows))
            self.assertEqual(result.battery_strategy, 'accu_uit')
            self.assertTrue(c._charge_session_started)
            discharged |= result.battery_strategy.startswith('ontladen')
            until = min(end, now + timedelta(minutes=5 if index < 8 else 30))
            commands = [{**w, 'end': min(datetime.fromisoformat(w['end']), until).isoformat()}
                        for w in result.planned_battery_mode_windows
                        if datetime.fromisoformat(w['start']) < until]
            for before, after in zip(commands, commands[1:]):
                self.assertLessEqual(before['end'], after['start'])
            trace = energy_trace(now, slots, commands, initial=energy, max_charge=2.5)
            self.assertTrue(trace)
            self.assertGreaterEqual(min(e for _, e, _ in trace), 2-.005)
            self.assertLessEqual(max(e for _, e, _ in trace), 10+.005)
            if result.battery_no_charge_reserve_active:
                # 8 October: if the next refill becomes physically unavailable
                # on an update, stop at the reserve (or hold an existing lower SOC).
                self.assertGreaterEqual(min(e for _, e, _ in trace), min(energy, 6)-.005)
            peak = max(peak, max(e for _, e, _ in trace))
            energy = trace[-1][1]
            now = until
            if now == end:
                break
        # The unfinished fill cannot discharge before an admissible completion,
        # even after the old grid window expires and runtime state is restored.
        self.assertFalse(discharged)
        self.assertAlmostEqual(peak, 9.3, delta=.005)
        self.assertEqual(now, end)
        self.assertAlmostEqual(energy, 9.3, delta=.005)

    def test_grid_led_cycle_can_cross_midnight(self):
        now = datetime.fromisoformat('2026-10-05T22:00:00+02:00')
        slots = [dict(start=now+timedelta(hours=i), end=now+timedelta(hours=i+1),
                      hours=1., price_known=True, import_price=.10 if i < 3 else .50,
                      export_price=0 if i < 3 else .40, solar_kwh=0.,
                      demand_kwh=1., net_solar_kwh=-1.) for i in range(7)]
        c = coordinator()
        solar, grid = c._plan_charge_windows_for_horizon(
            slots=slots, now=now, usable_capacity_kwh=3., current_remaining_capacity_kwh=3.,
            max_charge_kw=1., max_discharge_kw=1., battery_min_profit=.08,
        )
        self.assertFalse(solar)
        self.assertAlmostEqual(sum(w['charge_kwh'] for w in grid), 3.)
        self.assertEqual(grid[0]['start'], now.isoformat())
        self.assertEqual(grid[-1]['end'], (now+timedelta(hours=3)).isoformat())

    def test_continuing_grid_fill_requires_demand_for_existing_and_new_stock(self):
        now = datetime.fromisoformat('2026-10-05T16:00:00+02:00')
        for demand, expected_import in ((.5, 0.), (1., 1.)):
            with self.subTest(hourly_household_demand=demand):
                slots = [dict(start=now+timedelta(hours=i),
                    end=now+timedelta(hours=i+1), hours=1., price_known=True,
                    import_price=price, export_price=-.02, solar_kwh=0.,
                    demand_kwh=demand, net_solar_kwh=-demand)
                    for i, price in enumerate((.15, .30, .30))]
                c = coordinator()
                c._battery_cycle_state_initialized = True
                c._battery_grid_charge_price = .15
                c._active_charge_phase_mode = 'laden_van_net'
                c._active_charge_phase_end = now+timedelta(hours=1)
                self.assertTrue(c._charge_session_started)
                solar, grid = c._plan_charge_windows_for_horizon(
                    slots=slots, now=now, usable_capacity_kwh=2.,
                    current_remaining_capacity_kwh=1., max_charge_kw=1.,
                    max_discharge_kw=1., battery_min_profit=.08)
                self.assertFalse(solar)
                # Both demand tariffs clear .15 + .08. Only the larger real
                # demand can consume the existing 1 kWh AND the new 1 kWh;
                # neither a profitable export nor a weighted peak is needed.
                self.assertAlmostEqual(sum(w['charge_kwh'] for w in grid), expected_import)
                if expected_import:
                    self.assertEqual(grid[0]['start'], now.isoformat())
                    self.assertEqual(grid[-1]['end'], (now+timedelta(hours=1)).isoformat())

    def test_overnight_grid_fill_precedes_cheaper_next_solar_day(self):
        now = datetime.fromisoformat('2026-10-05T22:00:00+02:00')
        slots = [dict(start=now+timedelta(hours=i), end=now+timedelta(hours=i+1),
                      hours=1., price_known=True,
                      import_price=.10 if i < 3 else (.50 if i < 7 else .05),
                      export_price=0 if i < 3 else (.40 if i < 7 else .02),
                      solar_kwh=0. if i < 7 else 2., demand_kwh=1.,
                      net_solar_kwh=-1. if i < 7 else 1.) for i in range(11)]
        c = coordinator()
        _, grid = c._plan_charge_windows_for_horizon(
            slots=slots, now=now, usable_capacity_kwh=3., current_remaining_capacity_kwh=3.,
            max_charge_kw=1., max_discharge_kw=1., battery_min_profit=.08,
        )
        self.assertAlmostEqual(sum(w['charge_kwh'] for w in grid), 3.)
        self.assertEqual(grid[0]['start'], now.isoformat())
        self.assertEqual(grid[-1]['end'], (now+timedelta(hours=3)).isoformat())

    def replay(self, now='2026-10-05T14:32:43.274255+02:00', soc=83, instance=None,
               snapshot='2026_10_05_incomplete_charge', profit=.08):
        return replay_full_plan(
            snapshot, now, soc=soc, instance=instance, profit=profit,
            reserve=60, max_charge=2.5, isolated_cycles=True, snapshot_exports=True,
        )

    def test_reduced_solar_rejects_grid_completion_for_all_historical_costs(self):
        for cost in (None, .19, .211):
            with self.subTest(historical_cost=cost):
                c = coordinator()
                c._battery_grid_charge_price = cost
                now, slots, result = self.replay('2026-10-05T15:39:59.411439+02:00', 92, c,
                                               snapshot='2026_10_05_reduced_solar')
                self.assert_wait_for_solar_completion(now, slots, result, initial=9.2)
                self.assertTrue(c._charge_session_started)

    def test_reduced_solar_still_respects_minimum_profit(self):
        _, _, result = self.replay('2026-10-05T15:39:59.411439+02:00', 92,
                                  snapshot='2026_10_05_reduced_solar', profit=.50)
        self.assertFalse(result.planned_grid_charge_windows)

    def test_latest_tomorrow_solar_is_executable_not_an_unsafe_grid_preview(self):
        now, slots, result = self.replay('2026-10-05T15:52:01.632559+02:00', 92,
                                       snapshot='2026_10_05_tomorrow_preview')
        self.assert_wait_for_solar_completion(now, slots, result, initial=9.2)

    def test_forecast_drop_updates_and_restart_do_not_force_grid_completion(self):
        c = coordinator()
        self.replay(instance=c)  # Start from the earlier, sunnier plan.
        now = datetime.fromisoformat('2026-10-05T15:39:59.411439+02:00')
        energy = peak = 9.2  # Use measured SOC, not an old projection.
        for index in range(16):
            _, slots, result = self.replay(now.isoformat(), energy*10, c,
                                          snapshot='2026_10_05_reduced_solar')
            if index == 3:
                c.hass = SimpleNamespace(data={})
                c.config_entry.entry_id = 'battery'
                c._store_battery_cycle_state_snapshot(now)
                restarted = coordinator()
                restarted.hass = c.hass
                restarted.config_entry = c.config_entry
                with patch.object(c._restore_recent_battery_cycle_state.__globals__['dt_util'],
                                  'now', return_value=now):
                    restarted._restore_recent_battery_cycle_state()
                c = restarted
                del c.hass
                _, slots, restored = self.replay(now.isoformat(), energy*10, c,
                                                snapshot='2026_10_05_reduced_solar')
                self.assertEqual(
                    [w for w in result.planned_battery_mode_windows if w['mode'].startswith('laden_')],
                    [w for w in restored.planned_battery_mode_windows if w['mode'].startswith('laden_')],
                )
                result = restored
            self.assertTrue(result.battery_no_charge_reserve_active)
            windows = result.planned_battery_mode_windows
            self.assertFalse(result.planned_grid_charge_windows)
            self.assertFalse(any(w['mode'] == 'laden_van_net' for w in
                windows + result.estimated_battery_mode_windows))
            self.assertEqual(result.battery_strategy, 'accu_uit')
            self.assertTrue(c._charge_session_started)
            for before, after in zip(windows, windows[1:]):
                self.assertLessEqual(datetime.fromisoformat(before['end']), datetime.fromisoformat(after['start']))
            until = now + timedelta(minutes=5)
            commands = [{**w, 'end': min(datetime.fromisoformat(w['end']), until).isoformat()}
                        for w in windows if datetime.fromisoformat(w['start']) < until]
            trace = energy_trace(now, slots, commands, initial=energy, max_charge=2.5)
            self.assertTrue(trace)
            self.assertGreaterEqual(min(e for _, e, _ in trace), 2-.005)
            self.assertLessEqual(max(e for _, e, _ in trace), 10+.005)
            peak = max(peak, *(e for _, e, _ in trace))
            energy = trace[-1][1]
            now = until
            if energy >= 9.95:
                break
        self.assertAlmostEqual(peak, 9.2, delta=.005)
        self.assertAlmostEqual(energy, 9.2, delta=.005)
        self.assertLess(now, now.replace(hour=17, minute=0, second=0, microsecond=0))

    def test_supplied_forecast_fills_today_before_evening_discharge(self):
        now, slots, result = self.replay()
        windows = result.planned_battery_mode_windows
        for previous, following in zip(windows, windows[1:]):
            self.assertLessEqual(datetime.fromisoformat(previous['end']),
                                 datetime.fromisoformat(following['start']))
        charge = [w for w in windows if w['mode'].startswith('laden_')]
        self.assertTrue(result.planned_grid_charge_windows)
        self.assertEqual(result.battery_strategy, 'laden_van_net')
        self.assertTrue(all(w['start'].startswith('2026-10-05') for w in charge))
        trace = energy_trace(now, slots, charge, initial=8.3, max_charge=2.5)
        self.assertGreater(trace[-1][1], 8.3)
        self.assertLessEqual(trace[-1][1], 10.005)
        first_discharge = next(w for w in windows if w['mode'].startswith('ontladen'))
        self.assertGreaterEqual(first_discharge['start'], charge[-1]['end'])
        full_trace = energy_trace(now, slots, windows, initial=8.3, max_charge=2.5)
        self.assertGreaterEqual(min(e for _, e, _ in full_trace), 2-.005)
        self.assertLessEqual(max(e for _, e, _ in full_trace), 10+.005)
        self.assertTrue(any(w['mode'].startswith('laden_')
                            for w in result.estimated_battery_mode_windows))
        for slot in slots:
            if any(w['mode'] == 'laden_van_net' and slot['start'] < datetime.fromisoformat(w['end'])
                   and slot['end'] > datetime.fromisoformat(w['start']) for w in windows):
                later = [s['import_price'] for s in slots if s['start'] >= slot['end'] and s['price_known']]
                self.assertGreaterEqual(max(later)-slot['import_price']+1e-9, .08)

    def test_updates_and_restart_allow_safe_partial_discharge_without_overfilling(self):
        c = coordinator()
        now = datetime.fromisoformat('2026-10-05T14:32:43.274255+02:00')
        energy = peak = 8.3
        paused = False
        for index in range(32):
            _, slots, result = self.replay(now.isoformat(), energy*10, c)
            if index == 4:
                # Restart during a tariff pause: persist the cycle, not a
                # short-lived active charging window.
                c.hass = SimpleNamespace(data={})
                c.config_entry.entry_id = 'battery'
                c._store_battery_cycle_state_snapshot(now)
                restarted = coordinator()
                restarted.hass = c.hass
                restarted.config_entry = c.config_entry
                with patch.object(c._restore_recent_battery_cycle_state.__globals__['dt_util'],
                                  'now', return_value=now):
                    restarted._restore_recent_battery_cycle_state()
                c = restarted
                del c.hass
                _, slots, result = self.replay(now.isoformat(), energy*10, c)
            self.assertTrue(result.battery_no_charge_reserve_active)
            paused |= result.battery_strategy == 'accu_uit'
            until = now + timedelta(minutes=5)
            commands = [{**w, 'end': min(datetime.fromisoformat(w['end']), until).isoformat()}
                        for w in result.planned_battery_mode_windows
                        if datetime.fromisoformat(w['start']) < until]
            trace = energy_trace(now, slots, commands, initial=energy, max_charge=2.5)
            self.assertTrue(trace)
            self.assertGreaterEqual(min(e for _, e, _ in trace), 2-.005)
            self.assertLessEqual(max(e for _, e, _ in trace), 10+.005)
            peak = max(peak, *(e for _, e, _ in trace))
            energy = trace[-1][1]
            now = until
            if energy >= 9.95:
                break
        self.assertTrue(paused, 'idle gaps must preserve the full-charge latch')
        self.assertGreaterEqual(peak, 8.3)
        self.assertNotAlmostEqual(energy, 8.3, delta=.005)  # Useful physical progress.
        self.assertGreaterEqual(energy, 5.995)
