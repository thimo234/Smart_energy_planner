"""Independent accounting of actual emitted partial-cycle commands.

No planner profit helper or projected/clamped SOC is used in these checks.
Reserve stock is valued at acquisition cost, never at a future sale price.
"""
from datetime import datetime, timedelta
import unittest
from test_battery_energy_accounting import replay_full_plan, energy_trace


def imported_margin(slots, windows, *, initial_usable, reserve_usable, charge_kw=2.5):
    from collections import deque
    imported = sold = cost = value = 0.
    reserve_deficit = max(0., reserve_usable-initial_usable)
    lots = deque([[max(0., initial_usable-reserve_usable), None]])

    def add(amount, price):
        nonlocal reserve_deficit, imported
        if price is not None:
            imported += amount
        held = min(reserve_deficit, amount)
        reserve_deficit -= held
        if amount > held+1e-9:
            lots.append([amount-held, price])

    # FIFO keeps pre-existing stock's earlier household use out of the
    # subsequent grid purchase's realized margin. Never retroactively assign
    # that already-consumed stock to higher-price later sales.
    for window in sorted(windows, key=lambda w: w['start']):
        start, end = (datetime.fromisoformat(window[k]) for k in ('start', 'end'))
        for slot in slots:
            hours = max(0., (min(end, slot['end']) - max(start, slot['start'])).total_seconds()/3600)
            net = float(slot['net_solar_kwh'])/float(slot['hours'])
            mode = window['mode']
            if mode == 'laden_van_net':
                solar_kw = min(charge_kw, max(0., net))
                add(solar_kw*hours, None)
                add((charge_kw-solar_kw)*hours, slot['import_price'])
            elif mode == 'laden_met_zonne_energie':
                add(min(charge_kw, max(0., net))*hours, None)
            elif mode.startswith('ontladen') and hours > 0:
                home_kw = min(3., max(0., -net))
                power = 3. if mode == 'ontladen_naar_net' else home_kw
                remaining = power*hours
                sale_price = ((home_kw*slot['import_price'] +
                    (power-home_kw)*slot['export_price'])/power) if power else 0.
                while remaining > 1e-7 and lots:
                    amount, acquisition = lots[0]
                    used = min(remaining, amount)
                    if acquisition is not None:
                        sold += used
                        value += used*sale_price
                        cost += used*acquisition
                    remaining -= used
                    lots[0][0] -= used
                    if lots[0][0] <= 1e-7:
                        lots.popleft()
                if remaining > 1e-5:
                    raise AssertionError(f'Unfunded commanded discharge: {remaining} kWh')
    return dict(imported=imported, sold=sold, value=value,
                average_cost=cost/sold if sold else 0.,
                margin=(value-cost)/sold if sold > 1e-6 else None)


class PartialCycleEconomicsTest(unittest.TestCase):
    def test_current_command_cannot_borrow_from_unreachable_fifo_tail(self):
        from custom_components.smart_energy_planner.battery_margin import MarginStock

        stock = MarginStock(0., 0.)
        stock.charge(1., .40)
        stock.charge(1., .00)
        # Selling all stock at .30 would average the required .10 margin,
        # but a .20-kWh household command cannot reach the cheap second lot.
        # No later demand is available to fund the expensive FIFO prefix.
        self.assertGreater(stock.available(.30, .10), 1.9)
        self.assertLess(stock.available(.30, .10, requested=.20), 1e-6)

    def test_grid_margin_survives_actual_quarter_hour_replanning(self):
        self.quarter_hour_replay()

    def test_grid_margin_survives_restart_during_quarter_hour_replanning(self):
        self.quarter_hour_replay(restart=True)

    def quarter_hour_replay(self, restart=False):
        from test_battery_energy_accounting import coordinator
        from types import SimpleNamespace
        from unittest.mock import patch
        import json

        instance = coordinator()
        now = datetime.fromisoformat('2026-09-18T19:24:56.329382+02:00')
        energy = 6.
        all_slots, executed = [], []
        minimum_energy = maximum_energy = energy
        for update in range(114):
            if restart and update == 57:
                # Exercise production snapshot, JSON persistence and restore.
                instance.hass = SimpleNamespace(data={})
                instance.config_entry.entry_id = 'margin-restart'
                instance._store_battery_cycle_state_snapshot(now)
                restored = coordinator()
                restored.hass = SimpleNamespace(data=json.loads(json.dumps(instance.hass.data)))
                restored.config_entry = SimpleNamespace(entry_id='margin-restart')
                clock = restored._restore_recent_battery_cycle_state.__globals__['dt_util']
                with patch.object(clock, 'now', return_value=now):
                    restored._restore_recent_battery_cycle_state()
                self.assertEqual(restored._battery_margin_state, instance._battery_margin_state)
                # Replay injects fixture config without an HA entry id.
                del restored.hass
                instance = restored
            _, slots, result = replay_full_plan(
                '2026_09_18_evening', now.isoformat(), soc=energy*10,
                reserve=40, profit=.15, max_charge=2.5, tax_deduction=.11,
                isolated_cycles=True, instance=instance)
            until = now + timedelta(minutes=15)
            commands = [{**w, 'start': max(now, datetime.fromisoformat(w['start'])).isoformat(),
                         'end': min(until, datetime.fromisoformat(w['end'])).isoformat()}
                        for w in result.planned_battery_mode_windows
                        if datetime.fromisoformat(w['start']) < until
                        and datetime.fromisoformat(w['end']) > now]
            # Clip the forecasts too: never count a later replan's overlapping
            # forecast interval twice in the independent realized FIFO ledger.
            for slot in slots:
                start, end = max(now, slot['start']), min(until, slot['end'])
                if end > start:
                    hours = (end-start).total_seconds()/3600
                    all_slots.append({**slot, 'start': start, 'end': end, 'hours': hours,
                        'net_solar_kwh': slot['net_solar_kwh']*hours/slot['hours']})
            trace = energy_trace(now, slots, commands, initial=energy, max_charge=2.5)
            self.assertTrue(trace)
            self.assertGreaterEqual(min(e for _, e, _ in trace), 4.-1e-6)
            self.assertLessEqual(max(e for _, e, _ in trace), 10.+1e-6)
            minimum_energy = min(minimum_energy, *(e for _, e, _ in trace))
            maximum_energy = max(maximum_energy, *(e for _, e, _ in trace))
            energy = trace[-1][1]
            executed.extend(commands)
            now = until
        ledger = imported_margin(all_slots, executed, initial_usable=4., reserve_usable=2.)
        self.assertGreater(ledger['imported'], .1, ledger)
        self.assertGreater(ledger['sold'], .1, ledger)
        self.assertGreaterEqual(ledger['margin'] + 1e-6, .15, ledger)
        self.replan_metrics = {**ledger, 'minimum_energy_kwh': minimum_energy,
            'maximum_energy_kwh': maximum_energy, 'final_energy_kwh': energy,
            'updates': 114, 'restart': restart}

    def test_reviewed_single_plan_grid_margin(self):
        _, slots, result = replay_full_plan(
            '2026_09_18_evening', '2026-09-18T19:24:56.329382+02:00',
            soc=60, reserve=40, profit=.15, max_charge=2.5,
            tax_deduction=.11, isolated_cycles=True)
        ledger = imported_margin(slots,
            result.planned_battery_mode_windows + result.estimated_battery_mode_windows,
            initial_usable=4., reserve_usable=2.)
        self.assertGreater(ledger['sold'], .1, ledger)
        self.assertGreaterEqual(ledger['margin'] + 1e-6, .15, ledger)

    def test_emitted_grid_imports_keep_margin_without_pv_subsidy(self):
        cases = [
            ('2026_10_10_short_cycles', '2026-10-10T09:15:10.729241+02:00', 20., .15),
            ('2026_10_08_low_profit', '2026-10-08T19:30:10.369777+02:00', 90., .1),
            ('2026_09_18_evening', '2026-09-18T19:24:56.329382+02:00', 64., .1),
        ]
        for snapshot, at, soc, minimum in cases:
            with self.subTest(snapshot=snapshot):
                now, slots, result = replay_full_plan(snapshot, at, soc=soc, reserve=60,
                    profit=minimum, max_charge=2.5, snapshot_exports=snapshot.startswith('2026_10'),
                    tax_deduction=.11, isolated_cycles=True)
                windows = result.planned_battery_mode_windows + result.estimated_battery_mode_windows
                trace = energy_trace(now, slots, windows, initial=soc/10., max_charge=2.5)
                self.assertGreaterEqual(min(e for _, e, _ in trace), min(soc/10., 6.)-.005)
                self.assertLessEqual(max(e for _, e, _ in trace), 10.005)
                ledger = imported_margin(slots, windows, initial_usable=soc/10.-2., reserve_usable=4.)
                if ledger['imported'] > .01:
                    self.assertIsNotNone(ledger['margin'], ledger)
                    self.assertGreaterEqual(ledger['margin'] + .001, minimum, ledger)
