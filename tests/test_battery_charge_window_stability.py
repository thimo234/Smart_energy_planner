"""Charging must execute inside a quarter, not chase the next quarter forever."""

from datetime import datetime, timedelta
import unittest

from test_battery_energy_accounting import coordinator, energy_trace, replay_full_plan


class ChargeWindowStabilityTest(unittest.TestCase):
    def plan(self, instance, now, soc=20, snapshot="2026_09_26_drifting"):
        return replay_full_plan(
            snapshot=snapshot, timestamp=now.isoformat(), soc=soc, reserve=60,
            tax_deduction=.11, instance=instance, isolated_cycles=True,
        )

    def test_empty_battery_starts_in_current_quarter_even_after_restart(self):
        for restored in (False, True):
            with self.subTest(restored=restored):
                instance = coordinator()
                instance._charge_session_started = False
                instance._discharge_session_started = True
                instance._battery_cycle_state_restored_recent = restored
                now = datetime.fromisoformat("2026-09-26T12:04:06.122+02:00")
                _, _, result = self.plan(instance, now)
                self.assertEqual(result.battery_strategy, "laden_met_zonne_energie")
                self.assertEqual(datetime.fromisoformat(result.next_charge_window_start), now)
                self.assertTrue(instance._charge_session_started)
                self.assertFalse(instance._discharge_session_started)

    def test_repeated_non_quarter_updates_finish_selected_solar_cycle(self):
        instance = coordinator()
        instance._charge_session_started = False
        instance._discharge_session_started = True
        instance._battery_cycle_state_restored_recent = True
        now = datetime.fromisoformat("2026-09-26T12:04:06.122+02:00")
        energy = 2.0
        # Execute real emitted commands, feeding their resulting SOC back into
        # the next refresh. Keeping SOC fixed would invent missing energy.
        for _ in range(52):
            _, slots, result = self.plan(instance, now, energy * 10)
            self.assertEqual(result.battery_strategy, "laden_met_zonne_energie")
            until = now + timedelta(minutes=5)
            windows = [{**w, "end": min(datetime.fromisoformat(w["end"]), until).isoformat()}
                       for w in result.planned_battery_mode_windows
                       if datetime.fromisoformat(w["start"]) < until]
            trace = energy_trace(now, slots, windows, initial=energy)
            self.assertTrue(trace)
            self.assertGreaterEqual(min(e for _, e, _ in trace), 2 - 1e-6)
            self.assertLessEqual(max(e for _, e, _ in trace), 10 + 1e-6)
            energy = trace[-1][1]
            now = until
            if energy >= 9.95:
                break
        self.assertGreaterEqual(energy, 9.95, "the cheap cycle must actually fill the battery")
        self.assertLess(now.hour, 17, "do not postpone into the expensive evening")

    def test_partial_discharge_still_blocks_recharging(self):
        for restored in (False, True):
            instance = coordinator()
            instance._charge_session_started = False
            instance._discharge_session_started = True
            instance._battery_cycle_state_restored_recent = restored
            for minute in (4, 5, 14, 16):
                now = datetime.fromisoformat(f"2026-09-26T12:{minute:02d}:06+02:00")
                _, _, result = self.plan(instance, now, soc=60)
                self.assertFalse(result.battery_strategy.startswith("laden_"))
                self.assertTrue(instance._discharge_session_started)

    def test_grid_charge_executes_between_quarter_boundaries(self):
        instance = coordinator()
        now = datetime.fromisoformat("2026-09-19T13:16:06+02:00")
        energy = 2.0
        for _ in range(6):
            _, slots, result = self.plan(instance, now, energy * 10, "2026_09_19_morning")
            self.assertEqual(result.battery_strategy, "laden_van_net")
            until = now + timedelta(minutes=5)
            windows = [{**w, "end": min(datetime.fromisoformat(w["end"]), until).isoformat()}
                       for w in result.planned_battery_mode_windows
                       if datetime.fromisoformat(w["start"]) < until]
            energy = energy_trace(now, slots, windows, initial=energy)[-1][1]
            now = until
        self.assertAlmostEqual(energy, 3.5)
