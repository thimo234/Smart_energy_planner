"""Existing energy can serve the home; new imports and exports retain profit checks."""
from datetime import datetime, timedelta
import unittest

from test_battery_cycle_safety import modes, slots_at
from test_battery_energy_accounting import coordinator, energy_trace, replay_full_plan


class ExistingEnergyTest(unittest.TestCase):
    def instance(self, cost=.322):
        c = coordinator()
        c._charge_session_started = False
        c._discharge_session_started = True
        c._battery_grid_charge_price = cost
        return c

    def test_home_consumption_completes_cycle_before_refill(self):
        now = datetime(2026, 9, 29, 10)
        slots = slots_at(now)
        for i, slot in enumerate(slots):
            slot.update(net_solar_kwh=-1., solar_kwh=0.,
                        import_price=.20 if i < 4 else .80)
        window = dict(start=(now+timedelta(hours=3)).isoformat(),
                      end=(now+timedelta(hours=4)).isoformat(), charge_kwh=2., usable_hours=1.)
        windows, _ = modes(self.instance(.5), now, slots, 2., [], [window])
        self.assertTrue(any(w['mode'] == 'laden_van_net' for w in windows))
        self.assertFalse(any(w['mode'] == 'ontladen_naar_net' for w in windows))

    def test_feedback_uses_home_energy_then_refills_unavoidable_remainder(self):
        now, slots, result = replay_full_plan(
            '2026_09_29_blocked_cycle', '2026-09-29T09:02:57.501039+02:00',
            soc=75, instance=self.instance(), reserve=60, max_charge=2.5,
            tax_deduction=.11, isolated_cycles=True,
        )
        windows = result.planned_battery_mode_windows
        self.assertEqual(result.battery_strategy, 'ontladen')
        # Explicit October 2 exception: once PV covers all further demand
        # and export cannot earn its historical margin, retain the refill.
        self.assertEqual(result.next_charge_window_start, '2026-09-29T11:30:00+02:00')
        self.assertFalse(result.battery_no_charge_reserve_active)
        self.assertFalse(any(w['mode'] == 'ontladen_naar_net' for w in windows))
        trace = energy_trace(now, slots, windows, initial=7.5, max_charge=2.5)
        self.assertGreaterEqual(min(e for _, e, _ in trace), 6-.005)
        self.assertAlmostEqual(trace[-1][1], 10, delta=.005)

    def test_repeated_updates_keep_home_discharge_and_historical_export_cost(self):
        c = self.instance()
        now = datetime.fromisoformat('2026-09-29T09:02:57.501039+02:00')
        energy = 7.5
        for _ in range(8):
            _, slots, result = replay_full_plan(
                '2026_09_29_blocked_cycle', now.isoformat(), soc=energy*10,
                instance=c, reserve=60, max_charge=2.5,
                tax_deduction=.11, isolated_cycles=True,
            )
            self.assertEqual(result.battery_strategy, 'ontladen')
            self.assertEqual(c._battery_grid_charge_price, .322)
            until = now + timedelta(minutes=5)
            commands = [{**w, 'end': min(datetime.fromisoformat(w['end']), until).isoformat()}
                        for w in result.planned_battery_mode_windows if datetime.fromisoformat(w['start']) < until]
            trace = energy_trace(now, slots, commands, initial=energy, max_charge=2.5)
            energy = trace[-1][1]
            self.assertGreaterEqual(energy, 6-.005)
            now = until
