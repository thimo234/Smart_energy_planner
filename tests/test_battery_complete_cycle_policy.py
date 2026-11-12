"""Complete cycles, with home-only fallback down to the configured reserve."""
from datetime import datetime, timedelta
import unittest

from test_battery_energy_accounting import coordinator, energy_trace


class CompleteCyclePolicyTest(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 11, 11, 12)
        self.c = coordinator()
        self.c._charge_session_started = False
        self.c._cycle_export_planning = True
        self.c._battery_grid_charge_price = .30

    def slots(self, count=8, home=1., price=.20):
        return [dict(start=self.now + timedelta(hours=i),
                     end=self.now + timedelta(hours=i+1), hours=1.,
                     import_price=price, export_price=price-.12,
                     price_known=True, demand_kwh=home, solar_kwh=0.,
                     net_solar_kwh=-home) for i in range(count)]

    def build(self, slots, energy, grid=()):
        return self.c._build_mode_windows_from_hourly_plan(
            slots=slots, now=self.now, planned_solar_charge_windows=[],
            planned_grid_charge_windows=list(grid), initial_usable_energy_kwh=energy,
            usable_capacity_kwh=8., battery_soc_percent=(energy+2)*10,
            average_price=.20, average_export_price=.08, max_charge_kw=2.5,
            max_discharge_kw=3., no_charge_reserve_kwh=4., battery_min_profit=.10)[0]

    def charge(self, start, end, amount):
        return dict(start=(self.now+timedelta(hours=start)).isoformat(),
                    end=(self.now+timedelta(hours=end)).isoformat(),
                    charge_kwh=amount, price=.20)

    def test_partial_charge_must_not_switch_to_discharge(self):
        self.c._charge_session_started = True
        windows = self.build(self.slots(), 5.)
        self.assertFalse(any(w['mode'].startswith('ontladen') for w in windows))

    def test_future_partial_charge_must_not_switch_to_discharge(self):
        self.c._charge_session_started = True
        slots = self.slots()
        windows = self.build(slots, 0., [self.charge(0, 1, 2.5)])
        self.assertTrue(any(w['mode'].startswith('laden_') for w in windows))
        self.assertFalse(any(w['mode'].startswith('ontladen') for w in windows))

    def test_recharge_above_fallback_reserve_is_rejected(self):
        self.c._discharge_session_started = True
        windows = self.build(self.slots(home=0.), 6., [self.charge(1, 2, 2.)])
        self.assertFalse(any(w['mode'].startswith('laden_') for w in windows))

    def test_current_refill_cannot_clear_an_unfinished_discharge_latch(self):
        for start in (0, -.25):
            with self.subTest(start=start):
                self.setUp()
                self.c._discharge_session_started = True
                windows = self.build(self.slots(home=0.), 6., [self.charge(start, 1, 2.)])
                self.assertFalse(any(w['mode'].startswith('laden_') for w in windows))
                self.assertTrue(self.c._discharge_session_started)
                self.assertFalse(self.c._charge_session_started)
                self.assertIsNone(self.c._active_charge_phase_end)

    def test_legitimate_running_fill_is_preserved(self):
        for start in (0, -.25):
            with self.subTest(start=start):
                self.setUp()
                self.c._charge_session_started = True
                windows = self.build(self.slots(home=0.), 6., [self.charge(start, 1, 2.)])
                self.assertTrue(any(w['mode'] == 'laden_van_net' for w in windows))
                self.assertTrue(self.c._charge_session_started)
                self.assertFalse(self.c._discharge_session_started)

    def test_mixed_tariffs_preserve_reserve_after_profitable_opportunity(self):
        slots = self.slots()
        slots[0]['import_price'] = .60
        for slot in slots:
            slot['export_price'] = .08
        windows = self.build(slots, 8., [self.charge(6, 8, 4.)])
        before_refill = [w for w in windows if datetime.fromisoformat(w['start']) < self.now+timedelta(hours=6)]
        trace = energy_trace(self.now, slots, before_refill, initial=10., max_charge=2.5)
        self.assertAlmostEqual(trace[0][1], 9., delta=.005)
        self.assertAlmostEqual(min(e for _, e, _ in trace), 6., delta=.005)
        self.assertFalse(any(w['mode'] == 'ontladen_naar_net' for w in before_refill))

    def test_later_grid_window_does_not_hide_current_solar_refill(self):
        self.c._discharge_session_started = True
        slots = self.slots(count=8, price=.31)
        for slot in slots[:2]:
            slot['net_solar_kwh'] = 2.5
        solar = dict(self.charge(0, 2, 4.), mode='laden_met_zonne_energie')
        later_grid = self.charge(5, 7, 4.)
        windows, _ = self.c._build_mode_windows_from_hourly_plan(
            slots=slots, now=self.now, planned_solar_charge_windows=[solar],
            planned_grid_charge_windows=[later_grid], initial_usable_energy_kwh=4.,
            usable_capacity_kwh=8., battery_soc_percent=60., average_price=.30,
            average_export_price=.20, max_charge_kw=2.5, max_discharge_kw=3.,
            no_charge_reserve_kwh=4., battery_min_profit=.10,
        )
        self.assertEqual(windows[0]['mode'], 'laden_met_zonne_energie')
        trace = energy_trace(self.now, slots, windows, initial=6., max_charge=2.5)
        self.assertAlmostEqual(max(e for _, e, _ in trace), 10., delta=.005)

    def test_refill_at_reserve_ignores_outlet_after_full_completion(self):
        self.c._discharge_session_started = True
        slots = self.slots(count=6, price=.31)
        for slot in slots[:2]:
            slot['net_solar_kwh'] = 2.5
        slots[2]['import_price'] = .8
        proposed = self.charge(0, 3, 7.5)
        windows = self.build(slots, 4., [proposed])
        fills = [w for w in windows if w['mode'].startswith('laden_')]
        self.assertTrue(fills, 'An outlet after reaching full must not block solar refill at reserve')
        self.assertLess(datetime.fromisoformat(fills[0]['end']), slots[2]['start'])
        trace = energy_trace(self.now, slots, windows, initial=6., max_charge=2.5)
        self.assertAlmostEqual(max(e for _, e, _ in trace), 10., delta=.005)

    def test_profitable_use_below_reserve_is_not_restored_by_cheap_slots(self):
        slots = self.slots(count=10)
        for slot in slots:
            slot['export_price'] = .08
        for slot in slots[:2]:
            slot.update(import_price=.60, demand_kwh=3., net_solar_kwh=-3.)
        windows = self.build(slots, 8., [self.charge(6, 9, 6.)])
        before_refill = [w for w in windows if datetime.fromisoformat(w['start']) < self.now+timedelta(hours=6)]
        trace = energy_trace(self.now, slots, before_refill, initial=10., max_charge=2.5)
        self.assertAlmostEqual(trace[-1][1], 4., delta=.005)
        self.assertTrue(all(w['mode'] == 'accu_uit' for w in before_refill
                            if datetime.fromisoformat(w['start']) >= self.now+timedelta(hours=2)))
        full_trace = energy_trace(self.now, slots, windows, initial=10., max_charge=2.5)
        charge_end = next(e for end, e, _ in full_trace
                          if datetime.fromisoformat(end) == self.now+timedelta(hours=8.4))
        self.assertAlmostEqual(charge_end, 10., delta=.005)
        self.assertGreaterEqual(min(e for _, e, _ in full_trace), 2.-.005)
        self.assertLessEqual(max(e for _, e, _ in full_trace), 10.+.005)

    def test_charge_pause_preserves_fill_and_only_discharges_after_full(self):
        self.c._charge_session_started = True
        slots = self.slots(count=12, price=.60)
        windows = self.build(slots, 3., [self.charge(0, 1, 2.5), self.charge(3, 4, 2.5)])
        trace = energy_trace(self.now, slots, windows, initial=5., max_charge=2.5)
        discharge = next(w for w in windows if w['mode'].startswith('ontladen'))
        self.assertGreaterEqual(datetime.fromisoformat(discharge['start']), self.now+timedelta(hours=4))
        self.assertAlmostEqual(max(e for _, e, _ in trace), 10., delta=.005)
        self.assertTrue(any(w['mode'] == 'laden_van_net'
                            and datetime.fromisoformat(w['start']) == self.now+timedelta(hours=3)
                            for w in windows))

    def test_partial_refill_at_fallback_reserve_is_rejected(self):
        self.c._discharge_session_started = True
        windows = self.build(self.slots(home=0.), 4., [self.charge(1, 2, 2.5)])
        self.assertFalse(any(w['mode'].startswith('laden_') for w in windows))

    def test_unprofitable_home_fallback_stops_at_reserve_before_future_refill(self):
        self.c._discharge_session_started = True
        slots = self.slots()
        windows = self.build(slots, 8., [self.charge(6, 8, 4.)])
        before_refill = [w for w in windows if datetime.fromisoformat(w['start']) < self.now+timedelta(hours=6)]
        trace = energy_trace(self.now, slots, before_refill, initial=10., max_charge=2.5)
        self.assertAlmostEqual(min(e for _, e, _ in trace), 6., delta=.005)
        self.assertTrue(any(w['mode'] == 'ontladen' for w in before_refill))
        self.assertFalse(any(w['mode'] == 'ontladen_naar_net' for w in before_refill))

    def test_unprofitable_home_without_refill_stops_at_reserve(self):
        slots = self.slots()
        windows = self.build(slots, 8.)
        trace = energy_trace(self.now, slots, windows, initial=10., max_charge=2.5)
        self.assertAlmostEqual(trace[-1][1], 6., delta=.005)
        self.assertFalse(any(w['mode'] == 'ontladen_naar_net' for w in windows))

    def test_preview_does_not_treat_ended_partial_charge_as_full(self):
        slots = self.slots(price=.60)
        actual = [dict(start=self.now.isoformat(),
                       end=(self.now+timedelta(hours=1)).isoformat(),
                       mode='laden_van_net', price=.20)]
        preview = self.c._estimate_unknown_price_plan(
            slots=slots, now=self.now, actual_windows=actual,
            initial_energy=4.5, capacity=8., max_charge=2.5, max_discharge=3.,
            reserve=4., min_profit=.10, include_known_prices=True,
            preview_start=self.now+timedelta(hours=1))
        self.assertFalse(any(w['mode'].startswith('ontladen') for w in preview))

    def test_full_charge_can_discharge_to_minimum_when_profitable(self):
        slots = self.slots(price=.60)
        windows = self.build(slots, 8., [self.charge(6, 8, 5.)])
        before_refill = [w for w in windows if datetime.fromisoformat(w['start']) < self.now+timedelta(hours=6)]
        trace = energy_trace(self.now, slots, before_refill, initial=10., max_charge=2.5)
        self.assertAlmostEqual(min(e for _, e, _ in trace), 2., delta=.005)


if __name__ == '__main__':
    unittest.main()
