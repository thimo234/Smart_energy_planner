"""Assess a refill using the supply at its actual selected start."""

from datetime import datetime, timedelta
import unittest

from test_battery_energy_accounting import coordinator, energy_trace, replay_full_plan


class RefillPriceAnchorTest(unittest.TestCase):
    def replay(self, timestamp='2026-09-27T16:54:07.006343+02:00', soc=100, instance=None):
        return replay_full_plan(
            '2026_09_27_late_refill', timestamp, soc=soc, discharging=True,
            reserve=60, max_charge=2.5, tax_deduction=.11,
            isolated_cycles=True, instance=instance,
        )

    def test_actual_refill_uses_midday_prices_and_safe_energy(self):
        now, slots, result = self.replay()
        windows = result.planned_battery_mode_windows
        charge = [w for w in windows if w['mode'].startswith('laden_')]
        self.assertEqual(charge[0]['start'], '2026-09-28T11:30:00+02:00')
        self.assertEqual(charge[-1]['end'], '2026-09-28T15:12:00+02:00')
        for window in charge:
            for slot in slots:
                if slot['start'] < datetime.fromisoformat(window['end']) and slot['end'] > datetime.fromisoformat(window['start']):
                    self.assertLessEqual(slot['import_price'], .315)
        trace = energy_trace(now, slots, windows, initial=10, max_charge=2.5)
        self.assertGreaterEqual(min(e for _, e, _ in trace), 2-.005)
        self.assertLessEqual(max(e for _, e, _ in trace), 10+.005)
        self.assertAlmostEqual(trace[-1][1], 10, delta=.005)
        before = [e for t, e, _ in trace if t <= charge[0]['start']]
        self.assertAlmostEqual(before[-1], 2, delta=.005)

    def test_repeated_updates_keep_refill_in_cheap_hours(self):
        c = coordinator()
        c._charge_session_started = False
        c._discharge_session_started = True
        now = datetime.fromisoformat('2026-09-27T16:54:07.006343+02:00')
        energy = 10.
        for _ in range(12):
            _, slots, result = self.replay(now.isoformat(), energy*10, c)
            self.assertLess(datetime.fromisoformat(result.next_charge_window_start),
                            datetime.fromisoformat('2026-09-28T13:00:00+02:00'))
            until = now + timedelta(minutes=5)
            commands = [{**w, 'end': min(datetime.fromisoformat(w['end']), until).isoformat()}
                        for w in result.planned_battery_mode_windows
                        if datetime.fromisoformat(w['start']) < until]
            trace = energy_trace(now, slots, commands, initial=energy, max_charge=2.5)
            self.assertGreaterEqual(min(e for _, e, _ in trace), 2-.005)
            self.assertLessEqual(max(e for _, e, _ in trace), 10+.005)
            energy = trace[-1][1]
            now = until
