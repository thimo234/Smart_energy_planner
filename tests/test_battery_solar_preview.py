"""Solar-only energy must not authorize an unfinished grid cycle."""
from datetime import datetime, timedelta
import unittest

from test_battery_energy_accounting import coordinator, energy_trace, replay_full_plan


class SolarPreviewTest(unittest.TestCase):
    def replay(self, timestamp, soc, instance=None):
        return replay_full_plan('2026_10_10_short_cycles', timestamp, soc=soc,
            # At 10 ct this snapshot now passes the agreed average-margin
            # test. Use 15 ct to keep exercising rejection and reserve safety.
            reserve=60, profit=.15, max_charge=2.5, snapshot_exports=True,
            isolated_cycles=True, instance=instance)

    def test_preview_does_not_reintroduce_rejected_grid_supplement(self):
        now, slots, result = self.replay('2026-10-10T09:15:10.729241+02:00', 20)
        windows = result.planned_battery_mode_windows + result.estimated_battery_mode_windows
        self.assertFalse(any(w['mode'] == 'laden_van_net' for w in windows))
        trace = energy_trace(now, slots, windows, initial=2., max_charge=2.5)
        self.assertGreaterEqual(min(e for _, e, _ in trace), 1.995)
        self.assertLessEqual(max(e for _, e, _ in trace), 10.005)
        # New policy: reject the entire incomplete cycle, including its PV.
        self.assertTrue(all(w['mode'] == 'accu_uit' for w in windows))
        self.assertAlmostEqual(trace[-1][1], 2.)
        self.assertTrue(result.battery_no_charge_reserve_active)

    def test_skipped_cycle_preserves_reserve_across_updates(self):
        c = coordinator()
        c._charge_session_started = False
        c._discharge_session_started = True
        for timestamp in ['2026-10-10T09:15:10+02:00', '2026-10-10T09:20:10+02:00']:
            now, slots, result = self.replay(timestamp, 60, c)
            self.assertTrue(result.battery_no_charge_reserve_active)
            self.assertFalse(result.planned_grid_charge_windows)
            self.assertFalse(result.planned_solar_charge_windows)
            trace = energy_trace(now, slots, result.planned_battery_mode_windows,
                                 initial=6., max_charge=2.5)
            self.assertTrue(all(e >= 5.995 for _, e, _ in trace))

    def test_skipping_partial_solar_keeps_next_full_solar_opportunity(self):
        now = datetime(2026, 10, 10)
        slots = []
        for hour in range(48):
            surplus = 1. if hour == 12 else 2. if 34 <= hour < 38 else -.5
            slots.append(dict(start=now+timedelta(hours=hour), end=now+timedelta(hours=hour+1),
                hours=1., import_price=.4, export_price=.28, price_known=True,
                solar_kwh=max(0., surplus), demand_kwh=max(0., -surplus), net_solar_kwh=surplus))
        c = coordinator()
        c._charge_session_started = False
        c._cycle_export_planning = True
        solar, grid = c._plan_charge_windows_for_horizon(slots=slots, now=now,
            usable_capacity_kwh=8, current_remaining_capacity_kwh=8,
            max_charge_kw=2.5, max_discharge_kw=3, battery_min_profit=.10)
        self.assertFalse(grid)
        self.assertAlmostEqual(sum(w['charge_kwh'] for w in solar), 8.)
        self.assertTrue(all(w['start'].startswith('2026-10-11') for w in solar))

    def test_solar_feedback_does_not_become_a_running_grid_cycle(self):
        c = coordinator()
        c._active_charge_phase_mode = 'laden_met_zonne_energie'
        c._active_charge_phase_end = datetime.fromisoformat('2026-10-10T17:00:00+02:00')
        for timestamp, soc in [('2026-10-10T10:00:00+02:00', 22),
                               ('2026-10-10T10:05:00+02:00', 22.2)]:
            _, _, result = self.replay(timestamp, soc, c)
            self.assertFalse(result.planned_grid_charge_windows)
            self.assertFalse(any(w['mode'] == 'laden_van_net'
                                 for w in result.estimated_battery_mode_windows))
            self.assertTrue(result.planned_solar_charge_windows)


if __name__ == '__main__':
    unittest.main()
