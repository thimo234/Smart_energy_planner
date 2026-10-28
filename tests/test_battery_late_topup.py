"""A late cheap top-up must not hide a profitable daytime completion."""
from datetime import datetime, timedelta
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from test_battery_energy_accounting import replay_full_plan, energy_trace, coordinator


class LateTopupTest(unittest.TestCase):
    def replay(self, now=None, energy=10., instance=None, profit=.1):
        return replay_full_plan('2026_10_28',
            (now or datetime.fromisoformat('2026-10-28T15:57:23.390647+01:00')).isoformat(),
            soc=energy*10, discharging=True, reserve=60, max_charge=2.5,
            profit=profit, snapshot_exports=True, isolated_cycles=True, instance=instance)

    def test_daytime_fill_preserves_evening_value_and_safe_full_cycle(self):
        now, slots, result = self.replay()
        self.assertTrue(result.planned_grid_charge_windows)
        self.assertTrue(result.planned_solar_charge_windows)
        self.assertFalse(result.battery_no_charge_reserve_active)
        windows = result.planned_battery_mode_windows + result.estimated_battery_mode_windows
        charge = [w for w in windows if w['mode'].startswith('laden_')]
        start = min(datetime.fromisoformat(w['start']) for w in charge)
        end = max(datetime.fromisoformat(w['end']) for w in charge)
        self.assertEqual(start.date().isoformat(), '2026-10-29')
        self.assertLessEqual(end.hour, 16)
        trace = energy_trace(now, slots, windows, initial=10., max_charge=2.5)
        self.assertAlmostEqual(min(e for _, e, _ in trace), 2., delta=.005)
        self.assertAlmostEqual(max(e for t, e, _ in trace if start.isoformat() <= t <= end.isoformat()), 10., delta=.005)
        self.assertAlmostEqual(trace[-1][1], 6., delta=.005)
        self.assertFalse(any(w['mode'] == 'ontladen_naar_net' for w in windows))
        imported = cost = used = value = 0.
        for w in windows:
            for s in slots:
                a = max(datetime.fromisoformat(w['start']), s['start'])
                b = min(datetime.fromisoformat(w['end']), s['end'])
                hours = max(0., (b-a).total_seconds()/3600)
                net = s['net_solar_kwh']/s['hours']
                if w['mode'] == 'laden_van_net':
                    amount = max(0., 2.5-max(0., net))*hours
                    imported += amount
                    cost += amount*s['import_price']
                elif w['mode'] == 'ontladen' and a >= end:
                    amount = min(3., max(0., -net))*hours
                    used += amount
                    value += amount*s['import_price']
        self.assertGreater(imported, 2.)
        self.assertLess(imported, 4.)
        self.assertGreaterEqual(value/used-cost/imported, .1)

    def test_unprofitable_fill_still_keeps_reserve(self):
        _, _, result = self.replay(profit=1.)
        self.assertFalse(result.planned_grid_charge_windows)
        self.assertFalse(result.planned_solar_charge_windows)
        self.assertTrue(result.battery_no_charge_reserve_active)

    def test_feedback_keeps_daytime_completion(self):
        now = datetime.fromisoformat('2026-10-28T16:00:00+01:00')
        c = coordinator()
        c._charge_session_started, c._discharge_session_started = False, True
        energy = 10.
        minimum, peak = 10., 0.
        for index in range(96):
            _, slots, result = self.replay(now, energy, c)
            if index == 80:
                c.hass = SimpleNamespace(data={})
                c.config_entry.entry_id = 'late-topup'
                c._store_battery_cycle_state_snapshot(now)
                restored = coordinator()
                restored.hass, restored.config_entry = c.hass, c.config_entry
                with patch.object(c._restore_recent_battery_cycle_state.__globals__['dt_util'],
                                  'now', return_value=now):
                    restored._restore_recent_battery_cycle_state()
                c = restored
                del c.hass
                _, slots, again = self.replay(now, energy, c)
                self.assertEqual(result.planned_battery_mode_windows, again.planned_battery_mode_windows)
            until = now + timedelta(minutes=15)
            commands = [{**w, 'end': min(datetime.fromisoformat(w['end']), until).isoformat()}
                        for w in result.planned_battery_mode_windows if datetime.fromisoformat(w['start']) < until]
            trace = energy_trace(now, slots, commands, initial=energy, max_charge=2.5)
            self.assertTrue(trace)
            self.assertGreaterEqual(min(e for _, e, _ in trace), 1.995)
            self.assertLessEqual(max(e for _, e, _ in trace), 10.005)
            energy = trace[-1][1]
            minimum = min(minimum, energy)
            if now.date().isoformat() == '2026-10-29' and now.hour >= 9:
                peak = max(peak, energy)
            now = until
        self.assertLessEqual(minimum, 2.05)
        self.assertGreaterEqual(peak, 9.95)
