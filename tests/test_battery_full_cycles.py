"""Full charge/discharge cycles replace the former partial-night exception."""
import json
from pathlib import Path
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch
import unittest

from test_battery_energy_accounting import coordinator, energy_trace, replay_full_plan


class FullNightCycleTest(unittest.TestCase):
    def replay(self, now, soc, instance=None, snapshot='2026_09_29_night_refill'):
        return replay_full_plan(snapshot, now.isoformat(), soc=soc,
                               instance=instance, discharging=soc > 50, reserve=60,
                               max_charge=2.5, tax_deduction=.11, isolated_cycles=True)

    def assert_cycles(self, now, slots, windows, initial):
        energy = initial
        direction = 'discharge' if initial > 2.05 else None
        for window in windows:
            mode = window['mode']
            next_direction = ('charge' if mode.startswith('laden_') else
                              'discharge' if mode.startswith('ontladen') else None)
            if next_direction and next_direction != direction:
                if next_direction == 'charge':
                    self.assertLessEqual(energy, 2.05)
                elif direction == 'charge':
                    self.assertGreaterEqual(energy, 9.95)
                direction = next_direction
            trace = energy_trace(now, slots, [window], initial=energy, max_charge=2.5)
            if trace:
                energy = trace[-1][1]
                self.assertGreaterEqual(min(e for _, e, _ in trace), 2-.005)
                self.assertLessEqual(max(e for _, e, _ in trace), 10+.005)
        return energy

    def test_insufficient_morning_profit_skips_night_cycle_and_keeps_daytime_fill(self):
        now = datetime.fromisoformat('2026-09-29T18:59:37.810870+02:00')
        _, slots, result = self.replay(now, 68)
        self.assertFalse(any(w['mode'].startswith('laden_') and datetime.fromisoformat(w['start']).hour < 9
                             for w in result.planned_battery_mode_windows))
        end_energy = self.assert_cycles(now, slots, result.planned_battery_mode_windows, 6.8)
        self.assertAlmostEqual(end_energy, 10, delta=.005)
        self.assert_cycles(now, slots,
                           result.planned_battery_mode_windows + result.estimated_battery_mode_windows, 6.8)

    def test_empty_battery_selects_full_day_cycle_instead_of_small_night_purchases(self):
        now = datetime.fromisoformat('2026-09-30T00:00:00+02:00')
        _, slots, result = self.replay(now, 20)
        windows = result.planned_battery_mode_windows
        charge = [w for w in windows if w['mode'].startswith('laden_')]
        self.assertTrue(charge)
        self.assertGreaterEqual(datetime.fromisoformat(charge[0]['start']).hour, 9)
        self.assert_cycles(now, slots, windows, 2.)

    def test_full_profitable_export_cycle_then_daytime_refill(self):
        now = datetime.fromisoformat('2026-09-30T00:00:00+02:00')
        _, slots, result = self.replay(now, 20, snapshot='full_cycles_synthetic')
        windows = result.planned_battery_mode_windows
        self.assertTrue(any(w['mode'] == 'ontladen_naar_net' for w in windows))
        end_energy = self.assert_cycles(now, slots, windows, 2.)
        self.assertAlmostEqual(end_energy, 2., delta=.005)
        self.assertTrue(any(w['mode'].startswith('laden_') for w in result.estimated_battery_mode_windows))
        self.assert_cycles(now, slots, windows + result.estimated_battery_mode_windows, 2.)
        max_buy = max(float(slot['import_price']) for slot in slots
            if any(w['mode'] == 'laden_van_net' and slot['start'] < datetime.fromisoformat(w['end'])
                   and slot['end'] > datetime.fromisoformat(w['start']) for w in windows))
        for slot in slots:
            if any(w['mode'] == 'ontladen_naar_net' and slot['start'] < datetime.fromisoformat(w['end'])
                   and slot['end'] > datetime.fromisoformat(w['start']) for w in windows):
                # replay input uses import minus 0.11; feedback slots use 0.10.
                self.assertGreaterEqual(slot['import_price'] - .11 + 1e-9, max_buy + .08)

    def test_single_expensive_quarter_does_not_justify_full_night_charge(self):
        data = json.loads((Path(__file__).parent / 'fixtures/battery_2026_09_29_night_refill.json').read_text())
        for window in data['upcoming_energy_price_windows']:
            if window['start'] == '2026-09-30T07:00:00+02:00':
                window['price'] = .90
        with patch.object(Path, 'read_text', return_value=json.dumps(data)):
            now = datetime.fromisoformat('2026-09-30T00:00:00+02:00')
            _, _, result = self.replay(now, 20)
        self.assertFalse(any(w['mode'].startswith('laden_') and datetime.fromisoformat(w['start']).hour < 9
                             for w in result.planned_battery_mode_windows))

    def test_day_evening_night_morning_day_full_cycles(self):
        now = datetime.fromisoformat('2026-09-29T10:00:00+02:00')
        data = {'upcoming_energy_price_windows': [], 'estimated_hourly_home_demand': [],
                'estimated_hourly_solar_forecast': []}
        for i in range(38):
            start = now + timedelta(hours=i)
            end = start + timedelta(hours=1)
            price = .60 if 5 <= start.hour < 9 or 17 <= start.hour < 22 else .10
            base = {'start': start.isoformat(), 'end': end.isoformat()}
            data['upcoming_energy_price_windows'].append({**base, 'price': price, 'price_known': True})
            data['estimated_hourly_home_demand'].append({**base, 'estimated_kwh': .2})
            data['estimated_hourly_solar_forecast'].append({**base, 'estimated_kwh': 3 if 10 <= start.hour < 16 else 0})
        with patch.object(Path, 'read_text', return_value=json.dumps(data)):
            _, slots, result = self.replay(now, 20)
        windows = result.planned_battery_mode_windows + result.estimated_battery_mode_windows
        families = []
        for window in windows:
            family = ('charge' if window['mode'].startswith('laden_') else
                      'discharge' if window['mode'].startswith('ontladen') else None)
            if family is not None and (not families or family != families[-1]):
                families.append(family)
        self.assertGreaterEqual(len(families), 5)
        self.assert_cycles(now, slots, windows, 2.)

    def test_updates_and_restart_keep_full_cycle_and_ignore_legacy_partial_boundary(self):
        c = coordinator()
        now = datetime.fromisoformat('2026-09-30T00:00:00+02:00')
        energy = 2.
        # Obsolete partial-cycle data must not re-enable the old exception.
        c._partial_grid_cycle = (now + timedelta(minutes=20), now + timedelta(hours=9))
        maximum = energy
        direction = None
        saw_discharge = False
        daytime_refill = False
        for i in range(193):
            _, slots, result = self.replay(now, energy*10, c, snapshot='full_cycles_synthetic')
            if i == 12:
                c.hass = SimpleNamespace(data={})
                c.config_entry.entry_id = 'battery'
                c._store_battery_cycle_state_snapshot(now)
                state = c.hass.data
                state[c._store_battery_cycle_state_snapshot.__globals__['RUNTIME_STATE']]['battery'][c._store_battery_cycle_state_snapshot.__globals__['_BATTERY_CYCLE_STATE_KEY']]['partial_grid_cycle'] = [
                    now.isoformat(), (now+timedelta(hours=8)).isoformat()]
                restarted = coordinator()
                restarted.hass = c.hass
                restarted.config_entry = c.config_entry
                with patch.object(c._restore_recent_battery_cycle_state.__globals__['dt_util'], 'now', return_value=now):
                    restarted._restore_recent_battery_cycle_state()
                c = restarted
                del c.hass
            until = now+timedelta(minutes=5)
            windows = [{**w, 'end': min(datetime.fromisoformat(w['end']), until).isoformat()}
                       for w in result.planned_battery_mode_windows if datetime.fromisoformat(w['start']) < until]
            for w in windows:
                mode = w['mode']
                if mode.startswith('laden_'):
                    if saw_discharge and now.hour >= 9:
                        daytime_refill = True
                    if direction == 'discharge':
                        self.assertLessEqual(energy, 2.05)
                    direction = 'charge'
                elif mode.startswith('ontladen'):
                    if direction == 'charge':
                        self.assertGreaterEqual(energy, 9.95)
                    direction = 'discharge'
                    saw_discharge = True
                trace = energy_trace(now, slots, [w], initial=energy, max_charge=2.5)
                if trace:
                    energy = trace[-1][1]
                self.assertGreaterEqual(energy, 2-.005)
                self.assertLessEqual(energy, 10+.005)
                maximum = max(maximum, energy)
            now = until
        self.assertGreaterEqual(maximum, 9.95)
        self.assertTrue(saw_discharge)
        self.assertTrue(daytime_refill)
        self.assertGreaterEqual(energy, 9.95)
