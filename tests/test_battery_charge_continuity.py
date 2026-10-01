"""Small price differences must not fragment an active charging cycle."""

from datetime import datetime, timedelta
import unittest

from test_battery_energy_accounting import coordinator, energy_trace, replay_full_plan


class ChargeContinuityTest(unittest.TestCase):
    def test_feedback_charges_continuously_with_same_energy(self):
        now, slots, result = replay_full_plan(
            '2026_09_27_pauses', '2026-09-27T12:38:26.339665+02:00',
            soc=53, reserve=60, tax_deduction=.11, isolated_cycles=True,
        )
        charge = [w for w in result.planned_battery_mode_windows if w['mode'].startswith('laden_')]
        self.assertEqual(len(charge), 1)
        self.assertEqual(charge[0]['mode'], 'laden_met_zonne_energie')
        self.assertEqual(datetime.fromisoformat(charge[0]['start']), now)
        self.assertEqual(datetime.fromisoformat(charge[0]['end']).strftime('%H:%M'), '15:42')
        trace = energy_trace(now, slots, charge, initial=5.3)
        self.assertAlmostEqual(trace[-1][1], 10, delta=.005)
        self.assertEqual(result.next_discharge_window_start, '2026-09-27T18:00:00+02:00')

    def test_sequential_updates_continue_until_full(self):
        instance = coordinator()
        now = datetime.fromisoformat('2026-09-27T12:38:26.339665+02:00')
        energy = 5.3
        for _ in range(42):
            _, slots, result = replay_full_plan(
                '2026_09_27_pauses', now.isoformat(), soc=energy*10,
                reserve=60, tax_deduction=.11, isolated_cycles=True, instance=instance,
            )
            self.assertEqual(result.battery_strategy, 'laden_met_zonne_energie')
            until = now + timedelta(minutes=5)
            commands = [{**w, 'end': min(datetime.fromisoformat(w['end']), until).isoformat()}
                        for w in result.planned_battery_mode_windows
                        if datetime.fromisoformat(w['start']) < until]
            trace = energy_trace(now, slots, commands, initial=energy)
            self.assertTrue(trace)
            self.assertLessEqual(max(e for _, e, _ in trace), 10+.005)
            self.assertGreaterEqual(min(e for _, e, _ in trace), 2-.005)
            energy = trace[-1][1]
            now = until
            if energy >= 9.95:
                break
        self.assertGreaterEqual(energy, 9.95)

    def test_price_band_respects_large_differences_and_profit(self):
        now = datetime(2026, 9, 27, 12)
        for kind in ('solar', 'grid'):
            for expensive, peak in ((.104, .4), (.11, .4), (.111, .4), (.109, .181)):
                with self.subTest(kind=kind, expensive=expensive, peak=peak):
                    prices = [.1, expensive, .1, .1, peak, peak]
                    slots = [dict(start=now+timedelta(hours=i), end=now+timedelta(hours=i+1),
                                  hours=1., price_known=True,
                                  import_price=p if kind == 'grid' else p+.11,
                                  export_price=p-.11 if kind == 'grid' else p,
                                  net_solar_kwh=1. if kind == 'solar' and i<4 else -1.,
                                  solar_kwh=2. if kind == 'solar' and i<4 else 0.,
                                  demand_kwh=1.) for i,p in enumerate(prices)]
                    c = coordinator()
                    solar, grid = c._plan_charge_windows_for_horizon(
                        slots=slots, now=now, usable_capacity_kwh=3,
                        current_remaining_capacity_kwh=3, max_charge_kw=1,
                        max_discharge_kw=1, battery_min_profit=.08,
                    )
                    selected = solar if kind == 'solar' else grid
                    uses_second = any(datetime.fromisoformat(w['start']) <= now+timedelta(hours=1)
                                      < datetime.fromisoformat(w['end']) for w in selected)
                    allowed = expensive <= .11 and (kind == 'solar' or peak-expensive >= .08)
                    self.assertEqual(uses_second, allowed)
                    total = sum(w['charge_kwh'] for w in selected)
                    self.assertGreater(total, 0)
                    self.assertLessEqual(total, 3.)
                    if peak == .4:
                        self.assertAlmostEqual(total, 3.)
