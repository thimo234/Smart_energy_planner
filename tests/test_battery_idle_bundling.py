"""One-cent fluctuations should not scatter the available discharge energy."""
from datetime import datetime, timedelta
import unittest

from test_battery_energy_accounting import coordinator, energy_trace, replay_full_plan
from custom_components.smart_energy_planner.battery_planner import plan_segment_discharge_kwh


class IdleBundlingTest(unittest.TestCase):
    def test_future_grid_cycle_bundles_small_price_gaps(self):
        now = datetime(2026, 10, 1, 12)
        slots = [dict(start=now+timedelta(hours=i), end=now+timedelta(hours=i+1),
                      hours=1., price_known=True, import_price=p, export_price=p-.11,
                      net_solar_kwh=-1., solar_kwh=0., demand_kwh=1.)
                 for i, p in enumerate((.4, .1, .109, .1, .1, .4, .4, .4))]
        c = coordinator()
        c._charge_session_started = False
        solar, grid = c._plan_charge_windows_for_horizon(slots=slots, now=now,
            usable_capacity_kwh=3, current_remaining_capacity_kwh=3,
            max_charge_kw=1, max_discharge_kw=1, battery_min_profit=.08)
        self.assertFalse(solar)
        self.assertEqual(len(grid), 1)
        self.assertEqual(grid[0]['start'], (now+timedelta(hours=1)).isoformat())
        self.assertEqual(grid[0]['end'], (now+timedelta(hours=4)).isoformat())
        self.assertAlmostEqual(grid[0]['charge_kwh'], 3.)

    def test_discharge_groups_idle_at_end_of_price_band(self):
        now = datetime(2026, 10, 1, 18)
        slots = [dict(start=now+timedelta(minutes=15*i), hours=.25,
                      import_price=p, net_solar_kwh=-.25)
                 for i, p in enumerate((.40, .41, .402, .409, .404, .41))]
        plan = plan_segment_discharge_kwh(slots=slots, available_energy_kwh=.75,
                                          max_discharge_kw=1)
        self.assertEqual(list(plan), [s['start'] for s in slots[:3]])
        self.assertAlmostEqual(sum(plan.values()), .75)
        # Repeated replanning with the remaining energy keeps the same block.
        for i in range(3):
            plan = plan_segment_discharge_kwh(slots=slots[i:],
                available_energy_kwh=.75-i*.25, max_discharge_kw=1)
            self.assertEqual(list(plan), [s['start'] for s in slots[i:3]])

    def test_band_does_not_chain_or_cross_large_price_difference(self):
        now = datetime(2026, 10, 1)
        slots = [dict(start=now+timedelta(hours=i), hours=1.,
                      import_price=p, net_solar_kwh=-1.)
                 for i, p in enumerate((.38, .389, .398, .407))]
        plan = plan_segment_discharge_kwh(slots=slots, available_energy_kwh=1.,
                                          max_discharge_kw=1)
        self.assertEqual(list(plan), [slots[2]['start']])

    def test_real_snapshot_below_reserve_does_not_buy_an_unprofitable_full_cycle(self):
        c = coordinator()
        c._charge_session_started = False
        c._discharge_session_started = True
        now = datetime.fromisoformat('2026-10-01T08:34:19.167332+02:00')
        energy = 3.9
        for _ in range(96):
            _, slots, result = replay_full_plan('2026_10_01_restart', now.isoformat(),
                soc=energy*10, instance=c, reserve=60, max_charge=2.5,
                tax_deduction=.11, isolated_cycles=True)
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
        self.assertAlmostEqual(energy, 3.9, delta=.005)  # 8 October: no profitable full refill.
