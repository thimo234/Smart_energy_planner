"""Stateful regression: scarce profitable demand must not end a discharge cycle."""
from __future__ import annotations

import unittest
from datetime import datetime

from test_battery_energy_accounting import coordinator, replay_full_plan, energy_trace


class LiveDischargeCycleTest(unittest.TestCase):
    def replay(self, cost=.395):
        instance = coordinator()
        instance._charge_session_started = False
        instance._discharge_session_started = True
        # The entity does not expose acquisition cost. This explicit test input
        # reproduces the captured executable prefix; it is not a measured cost.
        instance._battery_grid_charge_price = cost
        return replay_full_plan(
            '2026_11_12_discharge_live', '2026-11-12T17:03:27.415493+01:00',
            soc=55, instance=instance, reserve=60, profit=.1,
            snapshot_exports=True, isolated_cycles=True,
        )

    def test_scarce_profitable_demand_cannot_restart_running_discharge(self):
        now, slots, result = self.replay()
        windows = result.planned_battery_mode_windows + result.estimated_battery_mode_windows
        energy = energy_trace(now, slots, windows, initial=5.5)
        before = 5.5
        charging = False
        for _, after, mode in energy:
            if mode.startswith('laden_'):
                if not charging:
                    self.assertLessEqual(before, 2.051)
                charging = True
            elif mode.startswith('ontladen'):
                charging = False
            before = after
        self.assertTrue(any(mode.startswith('ontladen') for _, _, mode in energy))
        # Only a small amount of profitable demand is available. Wait for
        # later profitable slots instead of ending the cycle to allow refill.
        self.assertFalse(any(w['mode'].startswith('laden_') for w in windows))
        self.assertLess(min(e for _, e, _ in energy), 5.5)
        self.assertGreater(min(e for _, e, _ in energy), 2.05)

    def test_no_margin_waits_without_refill_or_forced_export(self):
        now, slots, result = self.replay(cost=1.0)
        windows = result.planned_battery_mode_windows + result.estimated_battery_mode_windows
        first_refill = min((datetime.fromisoformat(w['start']) for w in windows
                            if w['mode'].startswith('laden')), default=slots[-1]['end'])
        before_refill = [w for w in windows if datetime.fromisoformat(w['start']) < first_refill]
        rows = energy_trace(now, slots, before_refill, initial=5.5)
        self.assertFalse(any(w['mode'].startswith('laden') for w in windows))
        self.assertFalse(any(mode == 'ontladen_naar_net' for _, _, mode in rows))
        self.assertAlmostEqual(rows[-1][1], 5.5, delta=.005)


if __name__ == '__main__':
    unittest.main()
