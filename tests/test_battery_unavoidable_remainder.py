"""A small stranded remainder must not cancel a useful solar refill."""
from datetime import datetime, timedelta
import unittest

from test_battery_energy_accounting import coordinator, energy_trace, replay_full_plan


NOW = datetime.fromisoformat('2026-10-02T08:11:36.720573+02:00')


def instance_with_cost(cost=.30):
    c = coordinator()
    c._charge_session_started = False
    c._discharge_session_started = True
    c._battery_grid_charge_price = cost
    return c


def replay(now=NOW, soc=26, instance=None):
    return replay_full_plan('2026_10_02_missing_charge', now.isoformat(), soc=soc,
        instance=instance if instance is not None else instance_with_cost(),
        reserve=60, max_charge=2.5, tax_deduction=.12, isolated_cycles=True)


class UnavoidableRemainderTest(unittest.TestCase):
    def test_solar_refill_survives_stranded_remainder(self):
        c = instance_with_cost()
        now, slots, result = replay(instance=c)
        self.assertFalse(c._charge_session_started)
        self.assertTrue(c._discharge_session_started)
        self.assertEqual(c._battery_grid_charge_price, .30)
        charge = [w for w in result.planned_battery_mode_windows if w['mode'].startswith('laden_')]
        self.assertTrue(charge)
        self.assertEqual(charge[0]['start'], '2026-10-02T11:45:00+02:00')
        trace = energy_trace(now, slots, result.planned_battery_mode_windows, initial=2.6, max_charge=2.5)
        self.assertAlmostEqual(trace[-1][1], 10, delta=.005)
        self.assertGreaterEqual(min(e for _, e, _ in trace), 2-.005)
        self.assertLessEqual(max(e for _, e, _ in trace), 10+.005)
        self.assertFalse(any(w['mode'] == 'ontladen_naar_net' for w in result.planned_battery_mode_windows))

    def test_profitable_export_still_empties_battery_before_refill(self):
        now, slots, result = replay(instance=instance_with_cost(.20))
        windows = result.planned_battery_mode_windows
        self.assertTrue(any(w['mode'] == 'ontladen_naar_net' for w in windows))
        charge_start = result.next_charge_window_start
        self.assertIsNotNone(charge_start)
        trace = energy_trace(now, slots, [w for w in windows if w['end'] <= charge_start],
                             initial=2.6, max_charge=2.5)
        self.assertAlmostEqual(trace[-1][1], 2, delta=.005)
        for w in windows:
            if w['mode'] == 'ontladen_naar_net':
                for s in slots:
                    if s['start'] < datetime.fromisoformat(w['end']) and s['end'] > datetime.fromisoformat(w['start']):
                        self.assertGreaterEqual(s['import_price']-.12+.000001, .20+.08)

    def test_known_profit_or_home_demand_prevents_remainder_exception(self):
        c = instance_with_cost()
        c._cycle_export_planning = True
        slot = dict(start=NOW, end=NOW+timedelta(hours=1), net_solar_kwh=1.,
                    export_price=.379, price_known=True)
        def possible():
            return c._can_discharge_in_interval(slots=[slot], start=NOW,
                end=slot['end'], cost=.30, minimum_profit=.08)
        self.assertFalse(possible())
        slot['export_price'] = .38
        self.assertTrue(possible())
        slot['price_known'] = False
        self.assertFalse(possible())
        slot['net_solar_kwh'] = -.1
        self.assertTrue(possible())

    def test_restored_discharge_state_does_not_delay_current_solar_charge(self):
        c = instance_with_cost()
        c._battery_cycle_state_restored_recent = True
        now = datetime.fromisoformat('2026-10-02T11:51:36.720573+02:00')
        _, _, result = replay(now, 24.87095933, c)
        self.assertEqual(result.battery_strategy, 'laden_met_zonne_energie')
        self.assertEqual(result.next_charge_window_start, now.isoformat())

    def test_sequential_updates_keep_daytime_refill(self):
        c = instance_with_cost()
        now, energy = NOW, 2.6
        charged = False
        for _ in range(100):
            _, slots, result = replay(now, energy*10, c)
            if energy < 9.95:
                self.assertIsNotNone(result.next_charge_window_start)
            until = now+timedelta(minutes=5)
            commands = [{**w, 'end': min(datetime.fromisoformat(w['end']), until).isoformat()}
                for w in result.planned_battery_mode_windows if datetime.fromisoformat(w['start']) < until]
            trace = energy_trace(now, slots, commands, initial=energy, max_charge=2.5)
            self.assertTrue(trace)
            self.assertGreaterEqual(min(e for _, e, _ in trace), 2-.005)
            self.assertLessEqual(max(e for _, e, _ in trace), 10+.005)
            energy = trace[-1][1]
            charged |= result.battery_strategy.startswith('laden_')
            now = until
            if energy >= 9.95:
                break
        self.assertTrue(charged)
        self.assertGreaterEqual(energy, 9.95)
