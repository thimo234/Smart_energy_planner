"""Measured SOC, not an idle demand forecast, must drive stock reconciliation."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any
import unittest

from test_battery_energy_accounting import coordinator  # HA module harness
from custom_components.smart_energy_planner.battery_margin import observed_stock


class ObservedAccountingTest(unittest.TestCase):
    def setUp(self):
        self.start = datetime(2026, 11, 14, 17, tzinfo=timezone.utc)
        self.end = self.start + timedelta(minutes=15)

    def state(self, mode='accu_uit', *, power=1., energy=6.8) -> dict[str, Any]:
        return dict(at=self.start.isoformat(), energy=energy, reserve=0.,
                    lots=[[energy, .377]], credit=0., actions=[dict(
                        start=self.start.isoformat(), end=self.end.isoformat(),
                        mode=mode, power=power, sale_price=.441,
                        solar_kw=0., grid_kw=0., import_price=.441)])

    def test_idle_unchanged_soc_cannot_create_realized_debt(self):
        previous = self.state()
        before = deepcopy(previous)
        stock = observed_stock(previous, self.end, 6.8, 0., .10)
        self.assertEqual(stock.lots, [[6.8, .377]])
        self.assertEqual(stock.credit, 0.)
        self.assertEqual(previous, before)

    def test_repeated_idle_updates_do_not_poison_acquisition_lots(self):
        previous = self.state()
        for _ in range(120):
            now = datetime.fromisoformat(previous['at']) + timedelta(seconds=10)
            stock = observed_stock(previous, now, 6.8, 0., .10)
            self.assertEqual(stock.lots, [[6.8, .377]])
            self.assertEqual(stock.credit, 0.)
            previous = dict(at=now.isoformat(), energy=6.8, reserve=0.,
                            lots=stock.lots, credit=stock.credit, actions=previous['actions'])

    def test_discharge_forecast_without_soc_drop_is_not_a_realized_sale(self):
        stock = observed_stock(self.state('ontladen'), self.end, 6.8, 0., .10)
        self.assertEqual(stock.lots, [[6.8, .377]])
        self.assertEqual(stock.credit, 0.)

    def test_discharge_sale_is_limited_to_measured_soc_drop(self):
        stock = observed_stock(self.state('ontladen'), self.end, 6.7, 0., .10)
        self.assertAlmostEqual(sum(q for q, _ in stock.lots), 6.7)
        self.assertAlmostEqual(stock.credit, .1 * (.441 - .377 - .10))
        self.assertEqual(len(stock.lots), 1)

    def carry(self, previous, now, stock):
        import json
        return json.loads(json.dumps(dict(
            at=now.isoformat(), energy=stock.energy, reserve=stock.reserve,
            lots=stock.lots, credit=stock.credit,
            pending_actions=getattr(stock, 'pending_actions', []),
            actions=previous['actions'])))

    def test_quantized_soc_confirms_accumulated_discharge_after_restart(self):
        previous = self.state('ontladen')
        previous['lots'] = [[6.8, .4]]
        previous['actions'][0]['sale_price'] = .2
        for step in range(1, 36):
            now = self.start + timedelta(seconds=step * 10)
            stock = observed_stock(previous, now, 6.8, 0., .1)
            self.assertEqual(stock.credit, 0.)
            self.assertEqual(stock.lots, [[6.8, .4]])
            previous = self.carry(previous, now, stock)
        stock = observed_stock(previous, self.start + timedelta(seconds=360), 6.7, 0., .1)
        self.assertAlmostEqual(stock.credit, -.03)
        self.assertAlmostEqual(sum(q for q, _ in stock.lots), 6.7)

    def test_confirmation_uses_proportional_historical_sale_prices_once(self):
        previous = self.state('ontladen')
        previous['lots'] = [[6.8, .4]]
        previous['credit'] = -.5
        # 0.1 kWh at .2 followed by 0.3 kWh at .6; only .1 kWh
        # is measured. Attribute .025/.075 rather than latest-price/FIFO time.
        previous['actions'][0]['sale_price'] = .2
        now = self.start + timedelta(seconds=360)
        stock = observed_stock(previous, now, 6.8, 0., .1)
        previous = self.carry(previous, now, stock)
        previous['actions'][0].update(start=now.isoformat(), power=3., sale_price=.6)
        now += timedelta(seconds=360)
        stock = observed_stock(previous, now, 6.7, 0., .1)
        self.assertAlmostEqual(stock.credit, -.5 + .025 * (-.3) + .075 * .1)
        self.assertEqual(stock.pending_actions, [])
        previous = self.carry(previous, now, stock)
        previous['actions'] = []
        # No remainder of the .4 kWh forecast may become later debt.
        later = observed_stock(previous, now + timedelta(seconds=10), 6.6, 0., .1)
        self.assertAlmostEqual(later.credit, stock.credit)

    def test_quantized_mixed_charge_retains_original_grid_price_and_solar_share(self):
        previous = self.state('laden_van_net')
        previous['actions'][0].update(solar_kw=.25, grid_kw=.75, import_price=.4)
        now = self.start + timedelta(seconds=360)
        stock = observed_stock(previous, now, 6.8, 0., .1)
        self.assertEqual(stock.lots, [[6.8, .377]])
        previous = self.carry(previous, now, stock)
        previous['actions'][0].update(start=now.isoformat(), solar_kw=.75,
                                      grid_kw=.25, import_price=.8)
        now += timedelta(seconds=360)
        stock = observed_stock(previous, now, 6.9, 0., .1)
        self.assertEqual(len(stock.lots), 5)
        for lot, quantity, cost in zip(stock.lots[1:], [.0125, .0375, .0375, .0125],
                                       [None, .4, None, .8]):
            self.assertAlmostEqual(lot[0], quantity)
            self.assertEqual(lot[1], cost)
        self.assertEqual(stock.credit, 0.)
        self.assertEqual(stock.pending_actions, [])

    def test_unconfirmed_evidence_is_bounded_without_changing_real_debt(self):
        previous = self.state('ontladen')
        previous['credit'] = -.5
        previous['actions'][0]['end'] = (self.start + timedelta(hours=3)).isoformat()
        for step in range(1, 751):
            now = self.start + timedelta(seconds=step * 10)
            stock = observed_stock(previous, now, 6.8, 0., .1)
            self.assertEqual(stock.credit, -.5)
            self.assertEqual(stock.lots, [[6.8, .377]])
            self.assertLessEqual(len(stock.pending_actions), 360)
            self.assertTrue(all(datetime.fromisoformat(a['start']) >= now - timedelta(hours=1)
                                for a in stock.pending_actions))
            previous = self.carry(previous, now, stock)
        previous['actions'] = []
        stock = observed_stock(previous, now + timedelta(hours=2), 6.7, 0., .1)
        self.assertEqual(stock.pending_actions, [])
        self.assertEqual(stock.credit, -.5)

    def test_actual_discharge_still_records_realized_debt(self):
        stock = observed_stock(self.state('ontladen'), self.end, 6.55, 0., .10)
        self.assertAlmostEqual(stock.credit, .25 * (.441 - .377 - .10))
        self.assertAlmostEqual(sum(q for q, _ in stock.lots), 6.55)

    def test_idle_preserves_existing_realized_debt_and_restart_state(self):
        import json
        previous = self.state()
        previous['credit'] = -.7485
        previous = json.loads(json.dumps(previous))
        stock = observed_stock(previous, self.end, 6.8, 0., .10)
        self.assertEqual(stock.credit, -.7485)
        self.assertEqual(stock.lots, [[6.8, .377]])

    def test_planned_grid_charge_without_soc_gain_does_not_reprice_stock(self):
        previous = self.state('laden_van_net')
        previous['actions'][0]['grid_kw'] = 1.
        stock = observed_stock(previous, self.end, 6.8, 0., .10)
        self.assertEqual(stock.lots, [[6.8, .377]])
        self.assertEqual(stock.credit, 0.)

    def test_planned_charge_is_limited_to_measured_soc_gain(self):
        previous = self.state('laden_van_net')
        previous['actions'][0]['grid_kw'] = 1.
        stock = observed_stock(previous, self.end, 6.9, 0., .10)
        self.assertEqual(stock.lots[0], [6.8, .377])
        self.assertAlmostEqual(stock.lots[1][0], .1)
        self.assertEqual(stock.lots[1][1], .441)
        self.assertEqual(stock.credit, 0.)

    def test_known_solar_charge_is_not_recorded_as_grid_purchase(self):
        # None marks solar/initial stock exempt from imported-energy margin;
        # export opportunity cost is accounted for by the charge planner.
        previous = self.state('laden_met_zonne_energie')
        previous['actions'][0]['solar_kw'] = 1.
        stock = observed_stock(previous, self.end, 6.9, 0., .10)
        self.assertEqual(stock.lots[0], [6.8, .377])
        self.assertAlmostEqual(stock.lots[1][0], .1)
        self.assertIsNone(stock.lots[1][1])
        self.assertEqual(stock.credit, 0.)

    def test_solar_above_forecast_does_not_inherit_legacy_grid_cost(self):
        previous = self.state('laden_met_zonne_energie')
        previous['credit'] = -.5
        previous['actions'][0]['solar_kw'] = .04
        # Forecast .01 kWh, measured .1 kWh: source is still the solar command.
        stock = observed_stock(previous, self.end, 6.9, 0., .10)
        self.assertEqual(stock.lots[0], [6.8, .377])
        self.assertAlmostEqual(stock.lots[1][0], .1)
        self.assertIsNone(stock.lots[1][1])
        self.assertAlmostEqual(sum(q for q, _ in stock.lots), 6.9)
        self.assertEqual(stock.credit, -.5)

    def test_solar_command_with_zero_forecast_keeps_measured_solar_source(self):
        previous = self.state('laden_met_zonne_energie')
        previous['actions'][0]['solar_kw'] = 0.
        stock = observed_stock(previous, self.end, 6.9, 0., .10)
        self.assertEqual(stock.lots[0], [6.8, .377])
        self.assertAlmostEqual(stock.lots[1][0], .1)
        self.assertIsNone(stock.lots[1][1])

    def test_mixed_source_excess_does_not_become_free_solar(self):
        previous = self.state('laden_van_net')
        previous['actions'][0].update(solar_kw=.02, grid_kw=.02, import_price=.4)
        stock = observed_stock(previous, self.end, 6.9, 0., .10)
        self.assertAlmostEqual(sum(q for q, c in stock.lots if c is None), .005)
        self.assertAlmostEqual(sum(q for q, c in stock.lots if c is not None), 6.895)
        self.assertEqual(stock.credit, 0.)

    def test_unknown_gain_is_not_assumed_to_be_free_solar(self):
        stock = observed_stock(self.state(), self.end, 6.9, 0., .10)
        self.assertAlmostEqual(sum(q for q, _ in stock.lots), 6.9)
        self.assertEqual(stock.lots[0][1], .377)
        self.assertEqual(stock.credit, 0.)

    def test_unexplained_loss_does_not_erase_existing_debt(self):
        previous = self.state()
        previous['credit'] = -.7485
        stock = observed_stock(previous, self.end, 6.7, 0., .10)
        self.assertEqual(stock.credit, -.7485)

    def test_unexplained_idle_drop_removes_stock_without_inventing_sale(self):
        stock = observed_stock(self.state(), self.end, 6.7, 0., .10)
        self.assertAlmostEqual(sum(q for q, _ in stock.lots), 6.7)
        self.assertEqual(stock.credit, 0.)


class ObservedPersistenceTest(unittest.IsolatedAsyncioTestCase):
    async def test_replan_save_restore_keeps_unconfirmed_execution(self):
        import json
        from types import SimpleNamespace
        from unittest.mock import patch
        from test_battery_energy_accounting import replay_full_plan

        now = datetime.fromisoformat('2026-09-17T12:33:32+02:00')
        start = now - timedelta(seconds=360)
        c = coordinator()
        c._battery_margin_state = dict(
            at=start.isoformat(), energy=4.8, reserve=0., lots=[[4.8, .4]], credit=-.5,
            actions=[dict(start=start.isoformat(), end=(now + timedelta(minutes=15)).isoformat(),
                          mode='ontladen', power=1., sale_price=.2,
                          solar_kw=0., grid_kw=0., import_price=.2)])
        replay_full_plan(instance=c, profit=.1)
        self.assertTrue(c._battery_margin_state.get('pending_actions'))
        self.assertEqual(c._battery_margin_state['credit'], -.5)
        c.hass = SimpleNamespace(data={})
        c.config_entry.entry_id = 'observed'
        c._store_battery_cycle_state_snapshot(now)
        namespace = c._async_persist_runtime_state.__globals__
        stored = {}

        class JsonStore:
            def __init__(self, *args):
                pass
            def __class_getitem__(cls, _):
                return cls
            async def async_load(self):
                return json.loads(json.dumps(stored))
            async def async_save(self, data):
                stored.update(json.loads(json.dumps(data)))

        with patch.dict(namespace, Store=JsonStore):
            await c._async_persist_runtime_state(c.hass.data[namespace['RUNTIME_STATE']]['observed'])
        restarted = coordinator()
        restarted.config_entry = c.config_entry
        restarted.hass = SimpleNamespace(data={namespace['RUNTIME_STATE']: stored})
        with patch.object(namespace['dt_util'], 'now', return_value=now):
            restarted._restore_recent_battery_cycle_state()
        self.assertEqual(restarted._battery_margin_state, c._battery_margin_state)
        stock = observed_stock(restarted._battery_margin_state, now, 4.7, 0., .1)
        self.assertAlmostEqual(stock.credit, -.53)
        self.assertEqual(stock.pending_actions, [])


if __name__ == '__main__':
    unittest.main()
