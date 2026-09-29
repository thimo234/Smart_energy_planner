"""Night charging must leave room for the cheaper following daytime cycle."""
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch
import unittest

from test_battery_energy_accounting import coordinator, energy_trace, replay_full_plan


class PartialNightCycleTest(unittest.TestCase):
    def replay(self, now, soc, instance=None):
        return replay_full_plan('2026_09_29_night_refill', now.isoformat(), soc=soc,
                               instance=instance, discharging=soc > 50, reserve=60,
                               max_charge=2.5, tax_deduction=.11, isolated_cycles=True)

    def test_existing_energy_avoids_export_and_rebuy_and_keeps_daytime_fill(self):
        now = datetime.fromisoformat('2026-09-29T18:59:37.810870+02:00')
        _, slots, result = self.replay(now, 68)
        windows = result.planned_battery_mode_windows
        self.assertFalse(any(w['mode'] == 'ontladen_naar_net' for w in windows))
        charge = [w for w in windows if w['mode'].startswith('laden_')]
        self.assertTrue(charge)
        self.assertGreaterEqual(datetime.fromisoformat(charge[0]['start']).hour, 9)
        trace = energy_trace(now, slots, windows, initial=6.8, max_charge=2.5)
        self.assertGreaterEqual(min(e for _, e, _ in trace), 2-.005)
        self.assertLessEqual(max(e for _, e, _ in trace), 10+.005)

    def test_empty_battery_buys_only_profitable_morning_energy(self):
        now = datetime.fromisoformat('2026-09-30T00:00:00+02:00')
        _, slots, result = self.replay(now, 20)
        windows = result.planned_battery_mode_windows
        charge = [w for w in windows if w['mode'] == 'laden_van_net']
        self.assertTrue(charge)
        energy = sum(energy_trace(now, slots, [w], initial=0, max_charge=2.5)[-1][1] for w in charge)
        self.assertGreater(energy, .5)
        self.assertLess(energy, 1.5)
        for w in windows:
            if w['mode'] == 'ontladen':
                for slot in slots:
                    if slot['start'] < datetime.fromisoformat(w['end']) and slot['end'] > datetime.fromisoformat(w['start']):
                        self.assertGreaterEqual(slot['import_price']+.000001, .243+.08)
        self.assertTrue(any(w['mode'].startswith('laden_') for w in result.estimated_battery_mode_windows))

    def test_five_minute_updates_and_restart_finish_before_daytime_refill(self):
        c = coordinator()
        now = datetime.fromisoformat('2026-09-30T00:00:00+02:00')
        energy = 2.
        morning_modes = set()
        daytime_charge = False
        for i in range(121):
            _, slots, result = self.replay(now, energy*10, c)
            if i == 50:
                # Restore the actual runtime snapshot, including the partial
                # cycle boundary, rather than reusing the coordinator object.
                c.hass = SimpleNamespace(data={})
                c.config_entry.entry_id = 'battery'
                c._store_battery_cycle_state_snapshot(now)
                restarted = coordinator()
                restarted.hass = c.hass
                restarted.config_entry = c.config_entry
                with patch.object(c._restore_recent_battery_cycle_state.__globals__['dt_util'], 'now', return_value=now):
                    restarted._restore_recent_battery_cycle_state()
                self.assertEqual(restarted._partial_grid_cycle, c._partial_grid_cycle)
                c = restarted
                del c.hass  # replay helper uses config entries without IDs
            if 6 <= now.hour < 9:
                morning_modes.add(result.battery_strategy)
            if now.hour >= 9 and result.battery_strategy.startswith('laden_'):
                daytime_charge = True
            until = now+timedelta(minutes=5)
            windows = [{**w, 'end': min(datetime.fromisoformat(w['end']), until).isoformat()}
                       for w in result.planned_battery_mode_windows if datetime.fromisoformat(w['start']) < until]
            trace = energy_trace(now, slots, windows, initial=energy, max_charge=2.5)
            energy = trace[-1][1]
            self.assertGreaterEqual(energy, 2-.005)
            self.assertLessEqual(energy, 10+.005)
            if now.hour < 6:
                self.assertLess(energy, 3.5)
            now = until
        self.assertIn('ontladen', morning_modes)
        self.assertTrue(daytime_charge)
