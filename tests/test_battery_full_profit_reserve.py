"""A marginal next fill must not release the no-charge reserve."""
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch
import unittest

from test_battery_energy_accounting import coordinator, energy_trace, replay_full_plan


class FullProfitReserveTest(unittest.TestCase):
    def test_rejected_charge_cannot_release_reserve_or_leave_stale_sensor_summary(self):
        now = datetime.fromisoformat('2026-10-08T19:30:10.369777+02:00')
        c = coordinator()
        c._charge_session_started = False
        c._discharge_session_started = True
        # A candidate whose actual discharge cannot finish before its start.
        c._battery_grid_charge_price = .40  # Prevent uneconomic early export.
        rejected = [dict(start='2026-10-09T07:00:00+02:00', end='2026-10-09T07:15:00+02:00',
                         price=.10, usable_hours=.25, charge_kwh=.625)]
        with patch.object(c, '_plan_charge_windows_for_horizon', return_value=([], rejected)):
            _, slots, result = self.replay(now, 90, c)
        self.assertFalse(result.planned_grid_charge_windows)
        self.assertTrue(result.battery_no_charge_reserve_active)
        self.assertEqual(result.battery_reserved_energy_kwh, 4.)
        self.assertIsNone(result.next_charge_opportunity_start)
        trace = energy_trace(now, slots, result.planned_battery_mode_windows, initial=9., max_charge=2.5)
        self.assertGreaterEqual(min(e for _, e, _ in trace), 6-.005)

    def test_full_profitable_refill_still_releases_reserve(self):
        now = datetime(2026, 10, 8)
        slots = [dict(start=now+timedelta(hours=i), end=now+timedelta(hours=i+1),
            hours=1., import_price=.10 if 6 <= i < 10 else .40,
            export_price=-.02 if 6 <= i < 10 else .28, price_known=True,
            demand_kwh=1., solar_kwh=0., net_solar_kwh=-1.) for i in range(18)]
        c = coordinator()
        c._charge_session_started = False
        c._discharge_session_started = True
        c._cycle_export_planning = True
        solar, grid = c._plan_charge_windows_for_horizon(slots=slots, now=now,
            usable_capacity_kwh=8, current_remaining_capacity_kwh=2,
            max_charge_kw=2, max_discharge_kw=3, battery_min_profit=.10)
        self.assertAlmostEqual(sum(w['charge_kwh'] for w in grid), 8.)
        windows, _ = c._build_mode_windows_from_hourly_plan(slots=slots, now=now,
            planned_solar_charge_windows=solar, planned_grid_charge_windows=grid,
            initial_usable_energy_kwh=6, usable_capacity_kwh=8, battery_soc_percent=80,
            average_price=.30, average_export_price=.18, max_charge_kw=2,
            max_discharge_kw=3, no_charge_reserve_kwh=4, battery_min_profit=.10)
        trace = energy_trace(now, slots, windows, initial=8, max_charge=2)
        self.assertAlmostEqual(min(e for _, e, _ in trace), 2., delta=.005)
        self.assertAlmostEqual(max(e for _, e, _ in trace), 10., delta=.005)

    def replay(self, now, soc, instance=None, reserve=60, profit=.10):
        return replay_full_plan('2026_10_08_low_profit', now.isoformat(), soc=soc,
            discharging=True, reserve=reserve, profit=profit, max_charge=2.5,
            snapshot_exports=True, isolated_cycles=True, instance=instance)

    def test_supplied_prices_do_not_justify_full_refill_at_eight_or_ten_cents(self):
        now = datetime.fromisoformat('2026-10-08T19:30:10.369777+02:00')
        for profit in (.08, .10):
            _, slots, result = self.replay(now, 90, profit=profit)
            self.assertFalse(result.planned_grid_charge_windows)
            self.assertTrue(result.battery_no_charge_reserve_active)
            self.assertAlmostEqual(result.battery_reserved_energy_kwh, 4.)
            windows = result.planned_battery_mode_windows + result.estimated_battery_mode_windows
            self.assertFalse(any(w['mode'].startswith('laden_') for w in windows))
            trace = energy_trace(now, slots, windows, initial=9., max_charge=2.5)
            self.assertGreaterEqual(min(e for _, e, _ in trace), 6-.005)
            self.assertAlmostEqual(trace[-1][1], 6., delta=.005)

    def test_existing_discharge_and_restart_respect_configured_reserve(self):
        c = coordinator()
        c._charge_session_started = False
        c._discharge_session_started = True
        now = datetime.fromisoformat('2026-10-08T19:30:10.369777+02:00')
        now = now.replace(hour=20, minute=30)
        energy = 6.1
        for index in range(8):
            _, slots, result = self.replay(now, energy*10, c)
            if index == 3:
                c.hass = SimpleNamespace(data={})
                c.config_entry.entry_id = 'battery'
                c._store_battery_cycle_state_snapshot(now)
                restarted = coordinator()
                restarted.hass, restarted.config_entry = c.hass, c.config_entry
                with patch.object(c._restore_recent_battery_cycle_state.__globals__['dt_util'],
                                  'now', return_value=now):
                    restarted._restore_recent_battery_cycle_state()
                c = restarted
                del c.hass
                _, slots, result = self.replay(now, energy*10, c)
            until = now + timedelta(minutes=5)
            windows = [{**w, 'end': min(until, datetime.fromisoformat(w['end'])).isoformat()}
                       for w in result.planned_battery_mode_windows
                       if datetime.fromisoformat(w['start']) < until]
            trace = energy_trace(now, slots, windows, initial=energy, max_charge=2.5)
            self.assertGreaterEqual(min(e for _, e, _ in trace), 6-.005)
            energy = trace[-1][1]
            now = until
        self.assertAlmostEqual(energy, 6., delta=.005)
        for soc, reserve in ((59, 60), (60, 60), (75, 75)):
            _, _, result = self.replay(now, soc, c, reserve=reserve)
            self.assertEqual(result.battery_strategy, 'accu_uit')
            self.assertFalse(result.planned_grid_charge_windows)

    def test_estimated_sale_prices_support_only_a_fully_profitable_known_purchase(self):
        now = datetime(2026, 10, 8, 12)
        for later_price, later_hours, charge_known, allowed in (
                (.35, 4, True, True), (.25, 4, True, False),
                (.35, 1, True, False), (.35, 4, False, False)):
            slots = [dict(start=now+timedelta(hours=i), end=now+timedelta(hours=i+1),
                hours=1., import_price=.20 if i < 2 else later_price,
                export_price=.08 if i < 2 else later_price-.12,
                price_known=charge_known and i < 2, demand_kwh=1., solar_kwh=0.,
                net_solar_kwh=-1.) for i in range(2+later_hours)]
            c = coordinator()
            c._charge_session_started = False
            _, grid = c._plan_charge_windows_for_horizon(slots=slots, now=now,
                usable_capacity_kwh=2., current_remaining_capacity_kwh=2.,
                max_charge_kw=1., max_discharge_kw=1., battery_min_profit=.10)
            self.assertEqual(bool(grid), allowed)
            if allowed:
                self.assertAlmostEqual(sum(w['charge_kwh'] for w in grid), 2.)
                self.assertTrue(all(datetime.fromisoformat(w['end']) <= now+timedelta(hours=2)
                                    for w in grid))
