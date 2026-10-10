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

    def test_actual_refill_passes_average_profit_for_the_full_purchase(self):
        # October 10: weighted margin, rather than every kWh individually,
        # supports this complete mixed fill after the actual depletion.
        now, slots, result = self.replay()
        self.assertTrue(result.planned_grid_charge_windows)
        self.assertFalse(result.battery_no_charge_reserve_active)
        trace = energy_trace(now, slots, result.planned_battery_mode_windows, initial=10, max_charge=2.5)
        self.assertGreaterEqual(min(e for _, e, _ in trace), 2-.005)
        self.assertAlmostEqual(trace[-1][1], 10, delta=.005)

    def test_repeated_updates_keep_the_full_average_profitable_refill(self):
        c = coordinator()
        c._charge_session_started = False
        c._discharge_session_started = True
        now = datetime.fromisoformat('2026-09-27T16:54:07.006343+02:00')
        energy = 10.
        for _ in range(12):
            _, slots, result = self.replay(now.isoformat(), energy*10, c)
            self.assertEqual(result.next_charge_window_start, '2026-09-28T11:30:00+02:00')
            self.assertFalse(result.battery_no_charge_reserve_active)
            until = now + timedelta(minutes=5)
            commands = [{**w, 'end': min(datetime.fromisoformat(w['end']), until).isoformat()}
                        for w in result.planned_battery_mode_windows
                        if datetime.fromisoformat(w['start']) < until]
            trace = energy_trace(now, slots, commands, initial=energy, max_charge=2.5)
            self.assertGreaterEqual(min(e for _, e, _ in trace), 2-.005)
            self.assertLessEqual(max(e for _, e, _ in trace), 10+.005)
            energy = trace[-1][1]
            now = until
