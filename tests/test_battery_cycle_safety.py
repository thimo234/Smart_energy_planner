"""Cycle hysteresis, an expired-charge escape, and forecast uncertainty."""
from datetime import datetime, timedelta
import unittest

from test_battery_energy_accounting import coordinator, replay_full_plan, energy_trace


def slots_at(now):
    return [dict(start=now+timedelta(hours=i), end=now+timedelta(hours=i+1), hours=1.,
                 import_price=.20 if i < 2 else .60, export_price=.09 if i < 2 else .49,
                 net_solar_kwh=1. if i < 2 else -1.,
                 solar_kwh=2. if i < 2 else 0., demand_kwh=1., price_known=True)
            for i in range(8)]


def modes(c, now, slots, energy, solar, grid):
    return c._build_mode_windows_from_hourly_plan(
        slots=slots, now=now, planned_solar_charge_windows=solar, planned_grid_charge_windows=grid,
        initial_usable_energy_kwh=energy, usable_capacity_kwh=8, battery_soc_percent=20+10*energy,
        average_price=.30, average_export_price=.19, max_charge_kw=2, max_discharge_kw=2,
        no_charge_reserve_kwh=0, battery_min_profit=.08,
    )


class CycleSafetyTest(unittest.TestCase):
    def test_small_soc_changes_keep_direction_across_replans(self):
        now = datetime(2026, 9, 25, 10)
        c = coordinator()
        window = dict(start=now.replace(hour=14).isoformat(), end=now.replace(hour=16).isoformat(),
                      charge_kwh=4., usable_hours=2.)
        for minute, energy in ((0, 1.), (5, 1.1), (10, 1.2)):
            current = now + timedelta(minutes=minute)
            slots = slots_at(current)
            for s in slots:
                s.update(net_solar_kwh=-1., solar_kwh=0., import_price=.60)
            _, mode = modes(c, current, slots, energy, [], [window])
            self.assertEqual(mode, 'accu_uit')
            self.assertTrue(c._charge_session_started)
        c._charge_session_started = False
        c._discharge_session_started = True
        for minute, energy in ((15, 2.), (20, 1.9), (25, 1.8)):
            current = now + timedelta(minutes=minute)
            cheap = dict(start=current.isoformat(), end=(current+timedelta(hours=1)).isoformat(),
                         charge_kwh=2., usable_hours=1.)
            _, mode = modes(c, current, slots_at(current), energy, [], [cheap])
            self.assertNotEqual(mode, 'laden_van_net')
            self.assertTrue(c._discharge_session_started)

    def test_lower_solar_forecast_starts_evening_discharge_earlier(self):
        now = datetime(2026, 9, 25, 18)
        outcomes = []
        for solar_factor in (1., .8):
            slots = slots_at(now)
            for i, s in enumerate(slots):
                solar = 1.1 * solar_factor if i == 0 else 0.
                s.update(solar_kwh=solar, net_solar_kwh=solar-1.,
                         import_price=.80 if i == 0 else .60)
            _, mode = modes(coordinator(), now, slots, 2., [], [])
            outcomes.append(mode)
        self.assertEqual(outcomes, ['accu_uit', 'ontladen'])

    def test_future_cheap_refill_cannot_reverse_unfinished_simulated_discharge(self):
        now = datetime(2026, 9, 25, 10)
        c = coordinator()
        c._charge_session_started = False
        c._discharge_session_started = True
        c._battery_grid_charge_price = .50
        slots = slots_at(now)
        for i, s in enumerate(slots):
            s.update(net_solar_kwh=-1., solar_kwh=0., import_price=.20 if i < 4 else .80)
        window = dict(start=(now+timedelta(hours=3)).isoformat(),
                      end=(now+timedelta(hours=4)).isoformat(), charge_kwh=2., usable_hours=1.)
        windows, _ = modes(c, now, slots, 2., [], [window])
        self.assertFalse(any(w['mode'] == 'laden_van_net' for w in windows))
        self.assertTrue(any(w['mode'] == 'ontladen' for w in windows))

    def test_discharge_latch_rejects_solar_and_grid_until_minimum(self):
        now = datetime(2026, 9, 25, 12)
        for source in ('solar', 'grid'):
            for energy in (7.5, 4, .10):
                with self.subTest(source=source, energy=energy):
                    c = coordinator()
                    c._charge_session_started = False
                    c._discharge_session_started = True
                    window = dict(start=now.isoformat(), end=(now+timedelta(hours=1)).isoformat(),
                                  charge_kwh=1., usable_hours=1.)
                    _, mode = modes(c, now, slots_at(now), energy,
                                    [window] if source == 'solar' else [],
                                    [window] if source == 'grid' else [])
                    self.assertNotIn(mode, ('laden_met_zonne_energie', 'laden_van_net'))
                    self.assertTrue(c._discharge_session_started)
                    self.assertFalse(c._charge_session_started)

    def test_depletion_releases_discharge_latch_for_next_charge(self):
        now = datetime(2026, 9, 25, 12)
        c = coordinator()
        c._charge_session_started = False
        c._discharge_session_started = True
        window = dict(start=now.isoformat(), end=(now+timedelta(hours=1)).isoformat(),
                      charge_kwh=1., usable_hours=1.)
        _, mode = modes(c, now, slots_at(now), .02, [window], [])
        self.assertEqual(mode, 'laden_met_zonne_energie')
        self.assertTrue(c._charge_session_started)

    def test_partial_charge_waits_for_remaining_charge_instead_of_reversing(self):
        now = datetime(2026, 9, 25, 10)
        c = coordinator()  # Charging started earlier; only a small amount stored.
        slots = slots_at(now)
        for s in slots:
            s.update(net_solar_kwh=-1., solar_kwh=0., import_price=.60)
        window = dict(start=(now+timedelta(hours=4)).isoformat(),
                      end=(now+timedelta(hours=6)).isoformat(), charge_kwh=4., usable_hours=2.)
        _, mode = modes(c, now, slots, 1., [], [window])
        self.assertEqual(mode, 'accu_uit')
        self.assertTrue(c._charge_session_started)
        self.assertFalse(c._discharge_session_started)

    def test_last_charge_window_over_allows_evening_discharge_when_not_full(self):
        now = datetime(2026, 9, 25, 18)
        c = coordinator()
        slots = slots_at(now)
        for s in slots:
            s.update(net_solar_kwh=-1., solar_kwh=0., import_price=.60)
        _, mode = modes(c, now, slots, 2., [], [])
        self.assertEqual(mode, 'ontladen')
        self.assertTrue(c._discharge_session_started)
        self.assertFalse(c._charge_session_started)

    def test_twenty_percent_margin_reduces_solar_in_actual_planner(self):
        args = ('2026_09_24_cheap_solar', '2026-09-24T20:04:29.361358+02:00')
        _, _, baseline = replay_full_plan(*args, soc=28, discharging=True, reserve=60, max_charge=2.5)
        now, slots, guarded = replay_full_plan(*args, soc=28, discharging=True, reserve=60,
                                         max_charge=2.5, solar_margin=20)
        for raw, safe in zip(baseline.estimated_hourly_solar_forecast, guarded.estimated_hourly_solar_forecast):
            self.assertAlmostEqual(safe['estimated_kwh'], .8*raw['estimated_kwh'], delta=.000001)
        self.assertLessEqual(guarded.next_charge_window_start, baseline.next_charge_window_start)
        safe_slots = [{**s, 'solar_kwh': .8*s['solar_kwh'],
                       'net_solar_kwh': .8*s['solar_kwh']-s['demand_kwh']} for s in slots]
        trace = energy_trace(now, safe_slots, guarded.planned_battery_mode_windows,
                             initial=2.8, max_charge=2.5)
        self.assertGreaterEqual(min(row[1] for row in trace), 2-.005)
        self.assertLessEqual(max(row[1] for row in trace), 10+.005)
