"""Tomorrow's solar must not substitute an unfinished daytime recharge."""
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch
import unittest

from test_battery_energy_accounting import coordinator, energy_trace, replay_full_plan


class DaytimeCompletionTest(unittest.TestCase):
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

    def test_reduced_solar_finishes_grid_cycle_before_evening(self):
        for cost in (None, .19, .211):
            with self.subTest(historical_cost=cost):
                c = coordinator()
                c._battery_grid_charge_price = cost
                now, slots, result = self.replay('2026-10-05T15:39:59.411439+02:00', 92, c,
                                               snapshot='2026_10_05_reduced_solar')
                charge = [w for w in result.planned_battery_mode_windows if w['mode'].startswith('laden_')]
                self.assertTrue(charge)
                self.assertTrue(all(w['start'].startswith('2026-10-05') for w in charge))
                self.assertTrue(all(w['mode'] == 'laden_van_net' for w in charge))
                trace = energy_trace(now, slots, charge, initial=9.2, max_charge=2.5)
                self.assertAlmostEqual(trace[-1][1], 10., delta=.005)
                first_discharge = next(w for w in result.planned_battery_mode_windows
                                       if w['mode'].startswith('ontladen'))
                self.assertLessEqual(charge[-1]['end'], first_discharge['start'])

    def test_reduced_solar_still_respects_minimum_profit(self):
        _, _, result = self.replay('2026-10-05T15:39:59.411439+02:00', 92,
                                  snapshot='2026_10_05_reduced_solar', profit=.50)
        self.assertFalse(result.planned_grid_charge_windows)

    def test_latest_tomorrow_preview_uses_solar_instead_of_late_grid_fill(self):
        now, slots, result = self.replay('2026-10-05T15:52:01.632559+02:00', 92,
                                       snapshot='2026_10_05_tomorrow_preview')
        preview = result.estimated_battery_mode_windows
        solar = [w for w in preview if w['mode'] == 'laden_met_zonne_energie']
        grid = [w for w in preview if w['mode'] == 'laden_van_net']
        self.assertEqual(solar[0]['start'], '2026-10-06T09:00:00+02:00')
        self.assertEqual(len(grid), 1)
        self.assertEqual(grid[0]['start'], '2026-10-06T13:30:00+02:00')
        self.assertEqual(grid[0]['end'], '2026-10-06T14:00:00+02:00')
        windows = result.planned_battery_mode_windows + preview
        for before, after in zip(windows, windows[1:]):
            self.assertLessEqual(datetime.fromisoformat(before['end']), datetime.fromisoformat(after['start']))
        trace = energy_trace(now, slots, windows, initial=9.2, max_charge=2.5)
        self.assertGreaterEqual(min(e for _, e, _ in trace), 2-.005)
        self.assertLessEqual(max(e for _, e, _ in trace), 10+.005)
        tomorrow = [(t, e) for t, e, _ in trace if t.startswith('2026-10-06')]
        self.assertAlmostEqual(max(e for _, e in tomorrow), 10, delta=.005)

    def test_forecast_drop_updates_and_restart_complete_real_commands(self):
        c = coordinator()
        self.replay(instance=c)  # Start from the earlier, sunnier plan.
        now = datetime.fromisoformat('2026-10-05T15:39:59.411439+02:00')
        energy = 9.2  # New measured SOC, rather than the old projected SOC.
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
            self.assertFalse(result.battery_strategy.startswith('ontladen'))
            windows = result.planned_battery_mode_windows
            for before, after in zip(windows, windows[1:]):
                self.assertLessEqual(datetime.fromisoformat(before['end']), datetime.fromisoformat(after['start']))
            until = now + timedelta(minutes=5)
            commands = [{**w, 'end': min(datetime.fromisoformat(w['end']), until).isoformat()}
                        for w in windows if datetime.fromisoformat(w['start']) < until]
            trace = energy_trace(now, slots, commands, initial=energy, max_charge=2.5)
            self.assertTrue(trace)
            self.assertGreaterEqual(min(e for _, e, _ in trace), 2-.005)
            self.assertLessEqual(max(e for _, e, _ in trace), 10+.005)
            energy = trace[-1][1]
            now = until
            if energy >= 9.95:
                break
        self.assertGreaterEqual(energy, 9.95)
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
        self.assertAlmostEqual(trace[-1][1], 10, delta=.005)
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

    def test_updates_and_restart_finish_without_reversing_or_overfilling(self):
        c = coordinator()
        now = datetime.fromisoformat('2026-10-05T14:32:43.274255+02:00')
        energy = 8.3
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
            self.assertFalse(result.battery_strategy.startswith('ontladen'))
            paused |= result.battery_strategy == 'accu_uit'
            until = now + timedelta(minutes=5)
            commands = [{**w, 'end': min(datetime.fromisoformat(w['end']), until).isoformat()}
                        for w in result.planned_battery_mode_windows
                        if datetime.fromisoformat(w['start']) < until]
            trace = energy_trace(now, slots, commands, initial=energy, max_charge=2.5)
            self.assertTrue(trace)
            self.assertGreaterEqual(min(e for _, e, _ in trace), 2-.005)
            self.assertLessEqual(max(e for _, e, _ in trace), 10+.005)
            energy = trace[-1][1]
            now = until
            if energy >= 9.95:
                break
        self.assertTrue(paused)
        self.assertGreaterEqual(energy, 9.95)
