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
    def test_refill_projection_only_counts_committed_below_reserve_loss_export(self):
        from test_battery_solar_topup_regression import slot

        cases = [
            # A committed loss export empties 0.6 kWh before the solar fill.
            (4., 0., True, 0., False, 8.),
            # No higher reserve, or stock above it: normal profit protection.
            (0., 0., True, 0., False, 7.4),
            (.5, 0., True, 0., False, 7.4),
            # Loss completion cannot bypass household minimum profit.
            (4., .1, True, 0., False, 7.4),
            # Unknown export prices, uncommitted cycles and strict cycles
            # cannot be assumed to dispose of the carried stock either.
            (4., 0., False, 0., False, 7.4),
            (4., 0., True, None, False, 7.4),
            (4., 0., True, 0., True, 7.4),
        ]
        for reserve, home, known, floor, strict, expected in cases:
            with self.subTest(reserve=reserve, home=home, known=known,
                              floor=floor, strict=strict):
                c = instance_with_cost()
                c._cycle_export_planning = True
                c._battery_cycle_discharge_floor_kwh = floor
                c._battery_require_profitable_cycle = strict
                slots = [slot(NOW, 0, .35, .20, demand=home)]
                slots[0]['price_known'] = known
                slots += [slot(NOW, i, .5, .02, solar=2.5) for i in range(1, 5)]
                slots += [slot(NOW, i, .8, .05, demand=2.) for i in range(5, 9)]
                solar, grid = c._plan_charge_windows_for_horizon(
                    slots=slots, now=NOW, usable_capacity_kwh=8.,
                    current_remaining_capacity_kwh=7.4, max_charge_kw=2.5,
                    max_discharge_kw=3., battery_min_profit=.1,
                    no_charge_reserve_kwh=0., loss_completion_reserve_kwh=reserve)
                self.assertEqual(grid, [])
                self.assertAlmostEqual(sum(w['charge_kwh'] for w in solar), expected)

    def test_solar_refill_survives_stranded_remainder(self):
        c = instance_with_cost()
        # Loss export below reserve requires an explicitly committed full
        # cycle, not merely a discharge latch with an unselected boundary.
        c._battery_cycle_discharge_floor_kwh = 0.
        now, slots, result = replay(instance=c)
        # A future useful refill is not an executing partial fill yet.
        self.assertFalse(c._charge_session_started)
        self.assertTrue(c._discharge_session_started)
        self.assertEqual(c._battery_grid_charge_price, .30)
        charge = [w for w in result.planned_battery_mode_windows if w['mode'].startswith('laden_')]
        self.assertTrue(charge)
        self.assertEqual(charge[0]['start'], '2026-10-02T11:45:00+02:00')
        trace = energy_trace(now, slots, result.planned_battery_mode_windows, initial=2.6, max_charge=2.5)
        self.assertAlmostEqual(trace[-1][1], 10, delta=.005)
        self.assertGreater(max(e for _, e, _ in trace), 6.)
        self.assertGreaterEqual(min(e for _, e, _ in trace), 2-.005)
        self.assertLessEqual(max(e for _, e, _ in trace), 10+.005)
        # The profitable full refill releases the remainder before charging.
        for w in result.planned_battery_mode_windows:
            if w['mode'] == 'ontladen_naar_net':
                self.assertLessEqual(w['end'], charge[0]['start'])

    def test_profitable_full_refill_releases_initial_energy_below_reserve(self):
        now, slots, result = replay(instance=instance_with_cost(.20))
        windows = result.planned_battery_mode_windows
        charge_start = result.next_charge_window_start
        self.assertIsNotNone(charge_start)
        trace = energy_trace(now, slots, [w for w in windows if w['end'] <= charge_start],
                             initial=2.6, max_charge=2.5)
        self.assertAlmostEqual(trace[-1][1], 2., delta=.005)
        for w in windows:
            if w['mode'] == 'ontladen_naar_net':
                self.assertLessEqual(w['end'], charge_start)
        self.assertGreaterEqual(min(e for _, e, _ in energy_trace(
            now, slots, windows, initial=2.6, max_charge=2.5)), 2-.005)

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

    def test_sequential_updates_keep_profitable_daytime_pv_and_original_reserve(self):
        c = instance_with_cost()
        now, energy = NOW, 2.6
        from partial_solar_assertions import assert_executable_plan
        ledger = None
        bought = 0.
        charged = False
        for _ in range(100):
            _, slots, result = replay(now, energy*10, c)

            until = now+timedelta(minutes=5)
            commands = [{**w, 'end': min(datetime.fromisoformat(w['end']), until).isoformat()}
                for w in result.planned_battery_mode_windows if datetime.fromisoformat(w['start']) < until]
            trace = energy_trace(now, slots, commands, initial=energy, max_charge=2.5)
            self.assertTrue(trace)
            self.assertGreaterEqual(min(e for _, e, _ in trace), 2-.005)
            self.assertLessEqual(max(e for _, e, _ in trace), 10+.005)
            ledger = assert_executable_plan(self, slots, commands, initial=energy,
                reserve=6., max_charge=2.5, state=ledger)
            bought += ledger['bought']
            energy = trace[-1][1]
            charged |= result.battery_strategy.startswith('laden_')
            now = until
            if energy >= 9.95:
                break
        self.assertTrue(charged)
        self.assertGreater(bought, 4.)
        self.assertGreaterEqual(energy, 5.995)
