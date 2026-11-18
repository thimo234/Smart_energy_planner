"""Assess a refill using the supply at its actual selected start."""

from datetime import datetime, timedelta
import unittest

from test_battery_energy_accounting import coordinator, energy_trace, replay_full_plan


class RefillPriceAnchorTest(unittest.TestCase):
    def replay(self, timestamp='2026-09-27T16:54:07.006343+02:00', soc=100, instance=None):
        return replay_full_plan(
            '2026_09_27_late_refill', timestamp, soc=soc, discharging=True,
            # Even without the extra reserve, weighted profit is insufficient
            # when household outlets fail the per-slot acquisition floor.
            reserve=20, max_charge=2.5, tax_deduction=.11,
            isolated_cycles=True, instance=instance,
        )

    def test_actual_refill_rejects_weighted_profit_without_strict_coverage(self):
        now, slots, result = self.replay()
        self.assertEqual(result.planned_grid_charge_windows, [])
        self.assertIsNone(result.next_charge_window_start)
        self.assertFalse(any(w['mode'] == 'laden_van_net' for w in
            result.planned_battery_mode_windows + result.estimated_battery_mode_windows))
        self.assertFalse(result.battery_no_charge_reserve_active)
        trace = energy_trace(now, slots, result.planned_battery_mode_windows, initial=10, max_charge=2.5)
        self.assertGreaterEqual(min(e for _, e, _ in trace), 2-.005)
        self.assertLessEqual(max(e for _, e, _ in trace), 10+.005)
        self.assertAlmostEqual(trace[-1][1], 2, delta=.005)

    def test_repeated_updates_do_not_restore_the_weighted_only_refill(self):
        c = coordinator()
        c._charge_session_started = False
        c._discharge_session_started = True
        now = datetime.fromisoformat('2026-09-27T16:54:07.006343+02:00')
        energy = 10.
        for _ in range(12):
            _, slots, result = self.replay(now.isoformat(), energy*10, c)
            self.assertEqual(result.planned_grid_charge_windows, [])
            self.assertIsNone(result.next_charge_window_start)
            full_trace = energy_trace(now, slots, result.planned_battery_mode_windows,
                                      initial=energy, max_charge=2.5)
            self.assertGreaterEqual(min(e for _, e, _ in full_trace), 2-.005)
            self.assertLessEqual(max(e for _, e, _ in full_trace), 10+.005)
            self.assertAlmostEqual(full_trace[-1][1], 2., delta=.005)
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
