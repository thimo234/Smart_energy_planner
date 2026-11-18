"""Executable allocation and acquisition-cost export regressions (synthetic)."""
from datetime import timedelta
import unittest

import test_battery_bound_cycle_policy as helpers
from test_battery_energy_accounting import energy_trace
from custom_components.smart_energy_planner import battery_planner


class ApprovedExportGuardTest(unittest.TestCase):
    setUp = helpers.BoundCyclePolicyTest.setUp
    slots = helpers.BoundCyclePolicyTest.slots
    build = helpers.BoundCyclePolicyTest.build

    def prepare(self, home=0., price=.283, floor=4., energy=8.):
        self.c._cycle_export_planning = True
        self.c._battery_cycle_discharge_floor_kwh = floor
        self.c._battery_grid_charge_price = .268
        self.c._battery_margin_state = dict(at=self.now.isoformat(), energy=energy,
            reserve=0., lots=[[energy, .268]], credit=-2., actions=[])
        slots = self.slots(count=4, home=home, price=.50)
        for slot in slots:
            slot['export_price'] = price  # Already net of export tax.
        return slots

    def test_user_example_rejects_export_above_reserve(self):
        slots = self.prepare()
        windows = self.build(slots, 8.)
        self.assertFalse(any(w['mode'] == 'ontladen_naar_net' for w in windows))
        self.assertEqual(self.c._battery_margin_state['lots'], [[8., .268]])
        self.assertEqual(self.c._battery_margin_state['credit'], -2.)

    def test_net_export_at_exact_margin_is_not_taxed_twice(self):
        slots = self.prepare(price=.368)
        windows = self.build(slots, 8.)
        self.assertTrue(any(w['mode'] == 'ontladen_naar_net' for w in windows))
        self.assertAlmostEqual(energy_trace(self.now, slots, windows, initial=10.)[-1][1], 6., delta=.005)

    def test_rejected_export_does_not_block_household_avoidance(self):
        slots = self.prepare(home=1.)
        windows = self.build(slots, 8.)
        self.assertFalse(any(w['mode'] == 'ontladen_naar_net' for w in windows))
        self.assertAlmostEqual(energy_trace(self.now, slots, windows, initial=10.)[-1][1], 6., delta=.005)

    def test_committed_full_cycle_below_reserve_can_export_at_loss(self):
        slots = self.prepare(price=-.05, floor=0., energy=4.)
        windows = self.build(slots, 4.)
        self.assertAlmostEqual(energy_trace(self.now, slots, windows, initial=6.)[-1][1], 2., delta=.005)

    def test_committed_full_cycle_does_not_waive_guard_above_reserve(self):
        slots = self.prepare(home=1., floor=0., energy=6.)
        windows = self.build(slots, 6.)
        exports = [w for w in windows if w['mode'] == 'ontladen_naar_net']
        self.assertTrue(exports)
        self.assertGreaterEqual(exports[0]['start'], slots[2]['start'].isoformat())
        trace = energy_trace(self.now, slots, windows, initial=8.)
        self.assertAlmostEqual(trace[-1][1], 2., delta=.005)

    def test_fifo_cost_is_protected_without_legacy_grid_price(self):
        slots = self.prepare()
        self.c._battery_grid_charge_price = None
        windows = self.build(slots, 8.)
        self.assertFalse(any(w['mode'] == 'ontladen_naar_net' for w in windows))


class ApprovedAllocationBundlingTest(unittest.TestCase):
    def test_grid_charge_commands_really_close_ten_minute_gap(self):
        from test_battery_energy_accounting import coordinator
        from datetime import datetime
        now = datetime(2026, 11, 15)
        slots = [dict(start=now+timedelta(minutes=10*i),
                      end=now+timedelta(minutes=10*(i+1)), hours=1/6,
                      price_known=True, import_price=p, export_price=p-.11,
                      net_solar_kwh=-1/6, solar_kwh=0., demand_kwh=1/6)
                 for i, p in enumerate([.4, .1, .2, .1, .4, .4, .4, .4])]
        c = coordinator()
        c._charge_session_started = False
        solar, grid = c._plan_charge_windows_for_horizon(slots=slots, now=now,
            usable_capacity_kwh=1/3, current_remaining_capacity_kwh=1/3,
            max_charge_kw=1., max_discharge_kw=1., battery_min_profit=.08)
        self.assertFalse(solar)
        self.assertEqual(len(grid), 1)
        self.assertEqual(grid[0]['start'], slots[1]['start'].isoformat())
        self.assertAlmostEqual(sum(w['charge_kwh'] for w in grid), 1/3, places=5)
        self.assertLessEqual(datetime.fromisoformat(grid[0]['end']), slots[3]['start'])

    def test_household_commands_really_close_ten_minute_gap(self):
        h = helpers.BoundCyclePolicyTest()
        h.setUp()
        h.c._battery_cycle_discharge_floor_kwh = 4.
        slots = [dict(start=h.now+timedelta(minutes=10*i),
                      end=h.now+timedelta(minutes=10*(i+1)), hours=1/6,
                      price_known=True, import_price=p, export_price=.01,
                      net_solar_kwh=-.5, solar_kwh=0., demand_kwh=.5)
                 for i, p in enumerate([.4, .2, .5])]
        windows = h.build(slots, 5.)
        active = [w for w in windows if w['mode'] == 'ontladen']
        self.assertEqual(len(active), 1)
        self.assertEqual(active[0]['start'], h.now.isoformat())
        self.assertEqual(active[0]['end'], slots[2]['start'].isoformat())
        trace = energy_trace(h.now, slots, windows, initial=7.)
        self.assertAlmostEqual(trace[-1][1], 6., delta=.005)

    def test_continuing_grid_cycle_keeps_profitable_original_when_bundle_loses_margin(self):
        from test_battery_energy_accounting import coordinator
        from datetime import datetime
        now = datetime(2026, 11, 15)
        prices = [.10, .20, .10] + [.20] * 8 + [.30]
        slots = [dict(start=now+timedelta(minutes=10*i),
                      end=now+timedelta(minutes=10*(i+1)), hours=1/6,
                      price_known=True, import_price=p, export_price=.001,
                      net_solar_kwh=-(.01 if i == len(prices)-1 else 1/6),
                      solar_kwh=0., demand_kwh=.01 if i == len(prices)-1 else 1/6)
                 for i, p in enumerate(prices)]
        for export_planning in (False, True):
            with self.subTest(cycle_export_planning=export_planning):
                c = coordinator()
                c._charge_session_started = True
                c._battery_grid_charge_price = .10
                c._cycle_export_planning = export_planning
                solar, grid = c._plan_charge_windows_for_horizon(
                    slots=slots, now=now, usable_capacity_kwh=.5,
                    current_remaining_capacity_kwh=1/3, max_charge_kw=1.,
                    max_discharge_kw=1., battery_min_profit=.08)
                self.assertFalse(solar)
                self.assertAlmostEqual(sum(w['charge_kwh'] for w in grid), 1/3, places=5)
                self.assertEqual(len(grid), 2, grid)
                self.assertEqual([w['start'] for w in grid],
                                 [slots[0]['start'].isoformat(), slots[2]['start'].isoformat()])
                self.assertTrue(all(w['price'] == .10 for w in grid), grid)

    def test_committed_full_below_reserve_commands_close_ten_minute_gap(self):
        h = helpers.BoundCyclePolicyTest()
        h.setUp()
        h.c._battery_cycle_discharge_floor_kwh = 0.
        slots = [dict(start=h.now+timedelta(minutes=10*i),
                      end=h.now+timedelta(minutes=10*(i+1)), hours=1/6,
                      price_known=True, import_price=p, export_price=.01,
                      net_solar_kwh=-.5, solar_kwh=0., demand_kwh=.5)
                 for i, p in enumerate([.4, .2, .5])]
        windows = h.build(slots, 1.)
        active = [w for w in windows if w['mode'] == 'ontladen']
        self.assertEqual(len(active), 1, windows)
        self.assertEqual(active[0]['start'], h.now.isoformat())
        self.assertEqual(active[0]['end'], slots[2]['start'].isoformat())
        trace = energy_trace(h.now, slots, windows, initial=3.)
        self.assertAlmostEqual(trace[-1][1], 2., delta=.005)
        self.assertGreaterEqual(min(e for _, e, _ in trace), 2.-.005)

    def loss_export_fixture(self, home=0.):
        h = helpers.BoundCyclePolicyTest()
        h.setUp()
        h.c._battery_cycle_discharge_floor_kwh = 0.
        h.c._cycle_export_planning = True
        h.c._battery_grid_charge_price = 1.
        slots = [dict(start=h.now+timedelta(minutes=10*i),
                      end=h.now+timedelta(minutes=10*(i+1)), hours=1/6,
                      price_known=True, import_price=.6, export_price=p,
                      net_solar_kwh=-home, solar_kwh=0., demand_kwh=home)
                 for i, p in enumerate([.4, .2, .5])]
        return h, slots

    def assert_loss_export_bundle(self, h, slots, energy=1.):
        windows = h.build(slots, energy)
        active = [w for w in windows if w['mode'] == 'ontladen_naar_net']
        self.assertEqual(len(active), 1, windows)
        self.assertEqual(active[0]['start'], h.now.isoformat())
        self.assertEqual(active[0]['end'], slots[2]['start'].isoformat())
        trace = energy_trace(h.now, slots, windows, initial=energy+2.)
        self.assertAlmostEqual(trace[-1][1], 2., delta=.005)
        self.assertGreaterEqual(min(e for _, e, _ in trace), 2.-.005)
        return windows

    def test_below_reserve_loss_exports_close_ten_minute_gap(self):
        h, slots = self.loss_export_fixture()
        self.assert_loss_export_bundle(h, slots)

    def test_below_reserve_loss_export_bundle_survives_refresh_and_json_restart(self):
        import json
        from types import SimpleNamespace
        from unittest.mock import patch
        h, slots = self.loss_export_fixture()
        baseline = self.assert_loss_export_bundle(h, slots)
        self.assertEqual(self.assert_loss_export_bundle(h, slots), baseline)
        h.c.hass = SimpleNamespace(data={})
        h.c.config_entry = SimpleNamespace(entry_id='loss-export-bundle')
        h.c._store_battery_cycle_state_snapshot(h.now)
        saved = json.loads(json.dumps(h.c.hass.data))
        restarted, _ = self.loss_export_fixture()
        restarted.c._battery_cycle_discharge_floor_kwh = None
        restarted.c._discharge_session_started = False
        restarted.c._battery_cycle_state_initialized = False
        restarted.c.hass = SimpleNamespace(data=saved)
        restarted.c.config_entry = SimpleNamespace(entry_id='loss-export-bundle')
        clock = restarted.c._restore_recent_battery_cycle_state.__globals__['dt_util']
        with patch.object(clock, 'now', return_value=h.now):
            restarted.c._restore_recent_battery_cycle_state()
        self.assertEqual(restarted.c._battery_cycle_discharge_floor_kwh, 0.)
        self.assertTrue(restarted.c._discharge_session_started)
        self.assertEqual(self.assert_loss_export_bundle(restarted, slots), baseline)

    def test_below_reserve_loss_export_respects_home_and_physical_power(self):
        h, slots = self.loss_export_fixture(home=.1)
        # Mixed lossful export still requires profitable household discharge.
        for slot in slots:
            slot['import_price'] = 1.10
        windows = h.build(slots, 1.1)
        # Household needs .3 kWh; exports may use only .8 kWh and .4 per slot.
        # A bridge needing two full .4 kWh export slots remains executable.
        active = [w for w in windows if w['mode'] == 'ontladen_naar_net']
        self.assertEqual(len(active), 1, windows)
        self.assertEqual(active[0]['start'], h.now.isoformat())
        self.assertEqual(active[0]['end'], slots[2]['start'].isoformat())
        trace = energy_trace(h.now, slots, windows, initial=3.1)
        self.assertAlmostEqual(trace[-1][1], 2., delta=.005)
        self.assertGreaterEqual(min(e for _, e, _ in trace), 2.-.005)

    def test_below_reserve_loss_export_does_not_bridge_fifteen_minutes(self):
        h, slots = self.loss_export_fixture()
        for i, slot in enumerate(slots):
            slot.update(start=h.now+timedelta(minutes=15*i),
                        end=h.now+timedelta(minutes=15*(i+1)), hours=.25)
        windows = h.build(slots, 1.5)
        active = [w for w in windows if w['mode'] == 'ontladen_naar_net']
        self.assertEqual(len(active), 2, windows)
        self.assertEqual(active[0]['end'], slots[1]['start'].isoformat())
        self.assertEqual(active[1]['start'], slots[2]['start'].isoformat())

    def test_below_reserve_loss_export_unknown_price_is_hard_boundary(self):
        h, slots = self.loss_export_fixture()
        slots[1]['price_known'] = False
        windows = h.build(slots, 1.)
        active = [w for w in windows if w['mode'] == 'ontladen_naar_net']
        self.assertEqual(len(active), 2, windows)
        self.assertEqual(active[0]['end'], slots[1]['start'].isoformat())
        self.assertEqual(active[1]['start'], slots[2]['start'].isoformat())
        self.assertAlmostEqual(energy_trace(h.now, slots, windows, initial=3.)[-1][1], 2., delta=.005)

    def test_below_reserve_loss_export_one_cent_band_is_not_transitive(self):
        h, slots = self.loss_export_fixture()
        for i, (slot, price) in enumerate(zip(slots, [.400, .409, .418])):
            slot.update(start=h.now+timedelta(minutes=30*i),
                        end=h.now+timedelta(minutes=30*(i+1)), hours=.5, export_price=price)
        windows = h.build(slots, 3.)
        active = [w for w in windows if w['mode'] == 'ontladen_naar_net']
        self.assertEqual(len(active), 2, windows)
        self.assertEqual(active[0]['start'], slots[0]['start'].isoformat())
        self.assertEqual(active[0]['end'], slots[1]['start'].isoformat())
        self.assertEqual(active[1]['start'], slots[2]['start'].isoformat())
        self.assertAlmostEqual(energy_trace(h.now, slots, windows, initial=5.)[-1][1], 2., delta=.005)

    def candidates(self, prices, minutes=10):
        from datetime import datetime
        now = datetime(2026, 11, 15)
        return [dict(start=now+timedelta(minutes=minutes*i),
                     end=now+timedelta(minutes=minutes*(i+1)),
                     price=p, capacity_kwh=minutes/60.) for i, p in enumerate(prices)]

    def bundle(self, slots, amounts):
        return battery_planner.bundle_slot_allocations(slots, {
            s['start']: a for s, a in zip(slots, amounts) if a > 0})

    def test_short_gap_redistributes_energy_even_outside_price_band(self):
        slots = self.candidates([.4, .2, .5])
        result = self.bundle(slots, [1/6, 0., 1/6])
        self.assertAlmostEqual(result.get(slots[1]['start'], 0.), 1/6)
        self.assertAlmostEqual(sum(result.values()), 1/3)
        self.assertNotIn(slots[2]['start'], result)

    def test_fifteen_minute_gap_is_not_bridged(self):
        slots = self.candidates([.4, .2, .5], minutes=15)
        self.assertEqual(self.bundle(slots, [.25, 0., .25]),
                         {slots[0]['start']: .25, slots[2]['start']: .25})

    def test_total_price_span_not_transitive_chain(self):
        slots = self.candidates([.400, .409, .418], minutes=30)
        result = self.bundle(slots, [0., .5, .5])
        self.assertAlmostEqual(result.get(slots[0]['start'], 0.), .5)
        self.assertAlmostEqual(result.get(slots[2]['start'], 0.), .5)
        self.assertNotIn(slots[1]['start'], result)

    def test_missing_ineligible_slot_is_a_hard_boundary(self):
        slots = self.candidates([.4, .4, .4])
        result = self.bundle([slots[0], slots[2]], [1/12, 1/6])
        self.assertAlmostEqual(result[slots[0]['start']], 1/12)
        self.assertAlmostEqual(result[slots[2]['start']], 1/6)

    def test_insufficient_energy_keeps_gap_instead_of_faking_continuity(self):
        slots = self.candidates([.4, .2, .5])
        result = self.bundle(slots, [1/6, 0., .01])
        self.assertAlmostEqual(result[slots[2]['start']], .01)
        self.assertNotIn(slots[1]['start'], result)
