"""Replay sensor feedback and independently integrate the emitted commands."""

import json
from datetime import datetime, timedelta
from pathlib import Path
import unittest
from unittest.mock import patch
from types import SimpleNamespace

from test_battery_planner import SmartEnergyPlannerCoordinator
from custom_components.smart_energy_planner.battery_forecast import build_energy_balance_slots
from custom_components.smart_energy_planner.battery_models import SolarWindow
from custom_components.smart_energy_planner.price_models import PlannerWindow
from custom_components.smart_energy_planner.price_helpers import extend_price_window_tail


def feedback_slots(snapshot="2026_09_17", timestamp="2026-09-17T12:33:32+02:00"):
    data = json.loads((Path(__file__).parent / f"fixtures/battery_{snapshot}.json").read_text())
    parse = datetime.fromisoformat
    now = parse(timestamp)
    prices = [PlannerWindow(parse(w["start"]), parse(w["end"]), w["price"], w["price_known"])
              for w in data["upcoming_energy_price_windows"]]
    solar = [SolarWindow(parse(w["start"]), parse(w["end"]), w["estimated_kwh"], None, None)
             for w in data["estimated_hourly_solar_forecast"]]
    # Export tariff is absent from the supplied snapshot. Use import minus 0.10
    # as an explicit replay assumption; it does not affect today's net deficit.
    exports = [PlannerWindow(w.start, w.end, w.price - 0.10, w.price_known) for w in prices]
    slots = build_energy_balance_slots(
        price_windows=prices, export_price_windows=exports, solar_windows=solar,
        hourly_demand=data["estimated_hourly_home_demand"], horizon_start=now, demand_safety_margin=0,
    )
    return now, slots


def coordinator():
    c = SmartEnergyPlannerCoordinator.__new__(SmartEnergyPlannerCoordinator)
    c._active_charge_phase_end = None
    c._active_charge_phase_mode = "accu_uit"
    c._charge_session_started = True
    c._discharge_session_started = False
    c._battery_cycle_state_initialized = True
    return c


def replay():
    now, slots = feedback_slots()
    c = coordinator()
    solar, grid = c._plan_charge_windows_for_horizon(
        slots=slots, now=now, usable_capacity_kwh=8, current_remaining_capacity_kwh=3.2,
        max_charge_kw=3, max_discharge_kw=3, battery_min_profit=.08,
    )
    windows, mode = c._build_mode_windows_from_hourly_plan(
        slots=slots, now=now, planned_solar_charge_windows=solar, planned_grid_charge_windows=grid,
        initial_usable_energy_kwh=4.8, usable_capacity_kwh=8, battery_soc_percent=68,
        average_price=.30, average_export_price=.20, max_charge_kw=3, max_discharge_kw=3,
    )
    return now, slots, solar, grid, windows, mode


def replay_full_plan(snapshot="2026_09_17", timestamp="2026-09-17T12:33:32+02:00", soc=68,
                     discharging=False, reserve=20, max_charge=3, profit=.08, instance=None,
                     solar_margin=0, tax_deduction=.10, estimate_tomorrow=False):
    """Exercise final sensor scheduling with the already computed demand input."""
    now, slots = feedback_slots(snapshot, timestamp)
    data = json.loads((Path(__file__).parent / f"fixtures/battery_{snapshot}.json").read_text())
    prices = [PlannerWindow(s["start"], s["end"], s["import_price"], s["price_known"]) for s in slots]
    if estimate_tomorrow:
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        # Include the full known day, including prices before the replay time.
        prices = [PlannerWindow(datetime.fromisoformat(w['start']), datetime.fromisoformat(w['end']),
                                w['price'], w['price_known']) for w in data['upcoming_energy_price_windows']
                  if datetime.fromisoformat(w['start']) < midnight]
        prices = extend_price_window_tail(windows=prices, horizon_end=midnight+timedelta(days=1), fallback_price=None)
    exports = [PlannerWindow(w.start, w.end, w.price-tax_deduction, w.price_known) for w in prices]
    solar = [SolarWindow(datetime.fromisoformat(w["start"]), datetime.fromisoformat(w["end"]),
                         w["estimated_kwh"], None, None) for w in data["estimated_hourly_solar_forecast"]]
    if estimate_tomorrow:
        slots = build_energy_balance_slots(price_windows=prices, export_price_windows=exports,
                                          solar_windows=solar, hourly_demand=data['estimated_hourly_home_demand'],
                                          horizon_start=now, demand_safety_margin=0)
    c = instance if instance is not None else coordinator()
    if instance is None:
        c._charge_session_started = not discharging
        c._discharge_session_started = discharging
    c.config_entry = SimpleNamespace(data={"battery_enabled": True, "battery_capacity_kwh": 10, "battery_min_soc_percent": 20,
                                          "battery_max_charge_kw": max_charge, "battery_max_discharge_kw": 3,
                                          "battery_min_profit_per_kwh": profit,
                                          "battery_demand_safety_margin": 0,
                                          "battery_charge_safety_margin": solar_margin,
                                          "battery_no_charge_min_soc_percent": reserve}, options={})
    c._locked_eco_window = c._locked_preheat_end = c._preheat_expired_at = None
    namespace = c._build_plan.__globals__
    with patch.object(namespace["dt_util"], "now", return_value=now), patch.dict(namespace, {
        "build_hourly_home_demand_forecast": lambda **kwargs: data["estimated_hourly_home_demand"],
        "align_price_responsive_demand_to_cheap_hours": lambda demand, prices: demand,
    }):
        result = c._build_plan(
            planner_kind="battery", windows=[p for p in prices if p.price_known], all_windows=prices,
            export_windows=exports, all_export_windows=exports, battery_switch_windows=prices,
            price_average=None, export_price_average=None, current_price=prices[0].price,
            solar_forecast_kwh=sum(w.forecast_kwh for w in solar if w.start.date() == now.date()),
            solar_windows=solar, all_solar_windows=solar, solcast_confidence=None,
            heating_estimate_kwh=0, lookback_average_kwh=20, total_energy_daily_average_kwh=20,
            non_heating_daily_average_kwh=20, hourly_demand_table={}, demand_adjustment_factor=1,
            room_temperature_c=None, thermostat_setpoint_c=None, thermostat_cool_setpoint_c=None,
            thermostat_preheat_setpoint_c=None, thermostat_eco_setpoint_c=None,
            room_cooling_hours_to_eco=None, room_cooling_rate_c_per_hour=None,
            cooling_reference_outdoor_temp_c=None, battery_soc_percent=soc,
            price_resolution="quarter_hourly", source_status={"price_sensor": "ok"}, source_errors=[],
        )
    return now, slots, result


def energy_trace(now, slots, windows, initial=6.8, max_charge=3):
    """Integrate mode duration and forecast power without clamping to capacity."""
    energy = initial
    trace = []
    for window in windows:
        for slot in slots:
            start = max(now, datetime.fromisoformat(window["start"]), slot["start"])
            end = min(datetime.fromisoformat(window["end"]), slot["end"])
            hours = max(0, (end - start).total_seconds() / 3600)
            net_kw = slot["net_solar_kwh"] / slot["hours"]
            rate = {"laden_van_net": max_charge, "laden_met_zonne_energie": min(max_charge, max(0, net_kw)),
                    "ontladen": -min(3, max(0, -net_kw)), "ontladen_naar_net": -3,
                    "accu_uit": 0}[window["mode"]]
            energy += hours * rate
            if hours:
                trace.append((end.isoformat(), energy, window["mode"]))
    return trace


class EnergyAccountingTest(unittest.TestCase):
    def test_solar_moves_to_cheapest_sufficient_daytime_slots(self):
        now, slots, result = replay_full_plan(
            "2026_09_24_cheap_solar", "2026-09-24T20:04:29.361358+02:00",
            soc=28, discharging=True, reserve=60, max_charge=2.5,
        )
        solar = result.planned_solar_charge_windows
        self.assertEqual(solar[0]["start"], "2026-09-25T11:45:00+02:00")
        self.assertAlmostEqual(sum(w["charge_kwh"] for w in solar), 8)
        self.assertEqual(result.planned_grid_charge_windows, [])
        # All selected energy fits in slots <=22 ct; the earlier morning
        # surplus has a higher opportunity cost and must remain unselected.
        for window in solar:
            for slot in slots:
                if slot["end"] > datetime.fromisoformat(window["start"]) and slot["start"] < datetime.fromisoformat(window["end"]):
                    self.assertLessEqual(slot["import_price"], .22)
        trace = energy_trace(now, slots, result.planned_battery_mode_windows, initial=2.8, max_charge=2.5)
        self.assertAlmostEqual(max(row[1] for row in trace), 10, delta=.005)

    def test_cheap_solar_waits_in_morning_then_completes_across_updates(self):
        _, _, result = replay_full_plan(
            "2026_09_24_cheap_solar", "2026-09-25T08:00:00+02:00",
            soc=20, reserve=60, max_charge=2.5,
        )
        self.assertEqual(result.battery_strategy, "accu_uit")
        _, history = self.run_feedback_updates(
            "2026_09_24_cheap_solar", "2026-09-25T11:45:00+02:00", 20, 48,
        )
        self.assertEqual(history[0][1].battery_strategy, "laden_met_zonne_energie")
        self.assertGreaterEqual(max(energy for _, _, energy in history), 9.95)

    def run_feedback_updates(self, snapshot, timestamp, soc, count, minutes=5):
        """Execute successive commands, update SOC, and reuse persisted state."""
        c = coordinator()
        now = datetime.fromisoformat(timestamp)
        energy = soc / 10
        history = []
        for _ in range(count):
            _, slots, result = replay_full_plan(
                snapshot, now.isoformat(), soc=energy * 10, reserve=60,
                max_charge=2.5, instance=c,
            )
            until = now + timedelta(minutes=minutes)
            executed = [{**w, "end": min(datetime.fromisoformat(w["end"]), until).isoformat()}
                        for w in result.planned_battery_mode_windows
                        if datetime.fromisoformat(w["start"]) < until]
            trace = energy_trace(now, slots, executed, initial=energy, max_charge=2.5)
            self.assertTrue(trace)
            self.assertGreaterEqual(min(row[1] for row in trace), 2 - .005)
            self.assertLessEqual(max(row[1] for row in trace), 10 + .005)
            energy = trace[-1][1]
            history.append((now, result, energy))
            now = until
        return c, history

    def test_september_23_cheap_charging_runs_across_updates(self):
        _, history = self.run_feedback_updates(
            "2026_09_23", "2026-09-23T13:55:04.999681+02:00", 81, 24,
        )
        self.assertTrue(all(r.battery_strategy in ("laden_met_zonne_energie", "laden_van_net") for _, r, _ in history))
        self.assertGreater(history[-1][2], 8.4)
        _, first, _ = history[0]
        self.assertTrue(any(w["mode"] == "ontladen" and w["start"].startswith("2026-09-23")
                            for w in first.planned_battery_mode_windows))
        self.assertTrue(first.planned_grid_charge_windows)
        self.assertTrue(all(w["price"] <= .186 for w in first.planned_grid_charge_windows))

    def test_september_24_stale_charge_latch_does_not_defer_discharge(self):
        c, history = self.run_feedback_updates(
            "2026_09_24", "2026-09-24T19:53:04.854187+02:00", 30, 12,
        )
        self.assertTrue(all(r.battery_strategy == "ontladen" for _, r, _ in history))
        self.assertTrue(c._discharge_session_started)
        self.assertLess(history[-1][2], 2.6)

    def test_incomplete_discharge_does_not_reverse_for_next_day_solar(self):
        _, history = self.run_feedback_updates(
            "2026_09_23", "2026-09-23T13:55:04.999681+02:00", 81, 112, minutes=15,
        )
        self.assertTrue(any(now.day == 23 and r.battery_strategy == "ontladen"
                            for now, r, _ in history))
        tomorrow = [(r, energy) for now, r, energy in history if now.day == 24]
        # This forecast never consumes the remaining energy at profitable
        # prices. Cheap sunshine alone must not reverse that discharge cycle.
        self.assertGreater(min(energy for _, energy in tomorrow), 2.05)
        self.assertTrue(any(r.battery_strategy == "ontladen" for r, _ in tomorrow))
        self.assertFalse(any(r.battery_strategy in ("laden_met_zonne_energie", "laden_van_net")
                             for r, _ in tomorrow))
        self.assertLess(tomorrow[-1][1], tomorrow[0][1])

    def test_committed_grid_refill_does_not_cancel_itself_halfway(self):
        _, history = self.run_feedback_updates(
            "2026_09_19_morning", "2026-09-19T12:30:00+02:00", 20, 60,
        )
        self.assertTrue(all(r.battery_strategy == "laden_van_net" for _, r, _ in history[2:36]))
        # The integration treats >=99.5% as full to avoid SOC sensor chatter.
        self.assertGreaterEqual(history[-1][2], 9.95)

    def test_active_grid_cycle_still_rechecks_profit(self):
        c, history = self.run_feedback_updates(
            "2026_09_19_morning", "2026-09-19T12:30:00+02:00", 20, 15,
        )
        now, _, energy = history[-1]
        _, _, result = replay_full_plan(
            "2026_09_19_morning", (now + timedelta(minutes=5)).isoformat(),
            soc=energy * 10, reserve=60, max_charge=2.5, profit=.50, instance=c,
        )
        self.assertEqual(result.planned_grid_charge_windows, [])
        self.assertNotEqual(result.battery_strategy, "laden_van_net")

    def test_full_recharge_from_floor_then_evening_discharge(self):
        now, slots, result = replay_full_plan(
            "2026_09_18_full_cycle", "2026-09-18T21:58:35+02:00",
            soc=78, discharging=True, reserve=60, max_charge=2.5,
        )
        grid = result.planned_grid_charge_windows
        self.assertEqual(len(grid), 1)
        self.assertAlmostEqual(grid[0]["charge_kwh"], 8)
        self.assertAlmostEqual((datetime.fromisoformat(grid[0]["end"]) -
                                datetime.fromisoformat(grid[0]["start"])).total_seconds() / 3600, 3.2)
        trace = energy_trace(now, slots, result.planned_battery_mode_windows, initial=7.8, max_charge=2.5)
        self.assertAlmostEqual(min(row[1] for row in trace), 2, delta=.001)
        self.assertAlmostEqual(max(row[1] for row in trace), 10, delta=.001)
        self.assertTrue(any(w["mode"] == "ontladen" and w["start"] > grid[0]["end"]
                            for w in result.planned_battery_mode_windows))
        self.assertGreaterEqual(trace[-1][1], 6 - .001)

    def test_small_solar_window_gets_grid_topup_and_later_solar_respects_capacity(self):
        now, slots, result = replay_full_plan(
            "2026_09_19_morning", "2026-09-19T08:00:10.907547+02:00",
            soc=44, discharging=True, reserve=60, max_charge=2.5,
        )
        grid = result.planned_grid_charge_windows
        self.assertEqual(len(grid), 1)
        self.assertEqual(grid[0]["start"], "2026-09-19T12:36:27.360000+02:00")
        self.assertAlmostEqual(grid[0]["charge_kwh"], 7.856)
        self.assertAlmostEqual(result.planned_solar_charge_windows[0]["charge_kwh"], .144)
        self.assertTrue(all(w["start"].startswith("2026-09-19") for w in result.planned_solar_charge_windows))
        windows = result.planned_battery_mode_windows
        trace = energy_trace(now, slots, windows, initial=4.4, max_charge=2.5)
        self.assertAlmostEqual(min(row[1] for row in trace), 2, delta=.001)
        self.assertAlmostEqual(max(row[1] for row in trace), 10, delta=.001)
        for previous, following in zip(windows, windows[1:]):
            self.assertEqual(previous["end"], following["start"])
        for window in windows:
            self.assertLess(window["start"], window["end"])
            if window["mode"] == "ontladen" and window["start"] > grid[0]["end"]:
                for slot in slots:
                    if slot["end"] > datetime.fromisoformat(window["start"]) and slot["start"] < datetime.fromisoformat(window["end"]):
                        self.assertTrue(slot["price_known"])
                        self.assertGreaterEqual(slot["import_price"] - .131 + 1e-9, .08)
        self.assertIn("2026-09-19T18:30:00+02:00", [w["start"] for w in windows if w["mode"] == "ontladen"])
        for window in [*grid, *result.planned_solar_charge_windows]:
            mode = "laden_van_net" if window in grid else "laden_met_zonne_energie"
            measured = energy_trace(now, slots, [{**window, "mode": mode}], initial=0, max_charge=2.5)
            self.assertAlmostEqual(measured[-1][1], window["charge_kwh"], delta=.001)

    def test_small_solar_window_does_not_override_minimum_grid_profit(self):
        _, _, result = replay_full_plan(
            "2026_09_19_morning", "2026-09-19T08:00:10.907547+02:00",
            soc=44, discharging=True, reserve=60, max_charge=2.5, profit=.50,
        )
        self.assertEqual(result.planned_grid_charge_windows, [])

    def test_grid_opportunity_releases_reserve_in_sensor_and_before_charge(self):
        now, slots, result = replay_full_plan(
            "2026_09_18_reserve", "2026-09-18T21:24:16.994088+02:00",
            soc=81, discharging=True, reserve=60, max_charge=2.5,
        )
        self.assertTrue(result.planned_grid_charge_windows)
        self.assertFalse(result.battery_no_charge_reserve_active)
        self.assertEqual(result.battery_reserved_energy_kwh, 0)
        self.assertAlmostEqual(result.battery_energy_available_for_discharge_kwh, 6.1)
        first_charge = result.next_charge_window_start
        before = [w for w in result.planned_battery_mode_windows if w["end"] <= first_charge]
        trace = energy_trace(now, slots, before, initial=8.1, max_charge=2.5)
        self.assertLess(min(row[1] for row in trace), 6.0)
        self.assertGreaterEqual(min(row[1] for row in trace), 2.0 - .005)

    def test_same_snapshot_without_profitable_grid_opportunity_keeps_reserve(self):
        now, slots, result = replay_full_plan(
            "2026_09_18_reserve", "2026-09-18T21:24:16.994088+02:00",
            soc=81, discharging=True, reserve=60, max_charge=2.5, profit=.50,
        )
        self.assertEqual(result.planned_grid_charge_windows, [])
        self.assertTrue(result.battery_no_charge_reserve_active)
        self.assertEqual(result.battery_reserved_energy_kwh, 4)
        self.assertAlmostEqual(result.battery_energy_available_for_discharge_kwh, 2.1)
        trace = energy_trace(now, slots, result.planned_battery_mode_windows, initial=8.1, max_charge=2.5)
        self.assertGreaterEqual(min(row[1] for row in trace), 6.0 - .005)

    def test_latest_feedback_one_charge_block_and_only_profitable_discharge(self):
        now, slots, result = replay_full_plan(
            "2026_09_18_evening", "2026-09-18T19:24:56.329382+02:00",
            soc=95, discharging=True, reserve=20, max_charge=2.5,
        )
        grid = result.planned_grid_charge_windows
        self.assertEqual(len(grid), 1)
        self.assertEqual(grid[0]["start"], "2026-09-19T12:03:00+02:00")
        self.assertAlmostEqual(grid[0]["charge_kwh"], 8.0)
        end = datetime.fromisoformat(grid[0]["end"])
        delivered = 0.0
        for window in result.planned_battery_mode_windows:
            if window["mode"] != "ontladen" or datetime.fromisoformat(window["start"]) < end:
                continue
            for slot in slots:
                hours = max(0, (min(slot["end"], datetime.fromisoformat(window["end"]))
                                - max(slot["start"], datetime.fromisoformat(window["start"]))).total_seconds()/3600)
                if hours:
                    self.assertGreaterEqual(slot["import_price"] - .131 + 1e-9, .08)
                    delivered += hours * max(0, -slot["net_solar_kwh"]) / slot["hours"]
        self.assertGreater(delivered, 0)
        self.assertLessEqual(delivered, grid[0]["charge_kwh"] + .005)
        trace = energy_trace(now, slots, result.planned_battery_mode_windows, initial=9.5, max_charge=2.5)
        self.assertGreaterEqual(min(row[1] for row in trace), 2.0 - .005)
        self.assertLessEqual(max(row[1] for row in trace), 10.0 + .005)

    def test_higher_configured_profit_rejects_latest_grid_cycle(self):
        _, _, result = replay_full_plan(
            "2026_09_18_evening", "2026-09-18T19:24:56.329382+02:00",
            soc=95, discharging=True, reserve=60, max_charge=2.5, profit=.20,
        )
        self.assertEqual(result.planned_grid_charge_windows, [])
        self.assertTrue(result.battery_no_charge_reserve_active)

    def test_equal_price_topup_adjoins_the_cheapest_block(self):
        now = datetime(2026, 9, 19, 12)
        slots = [dict(start=now+timedelta(minutes=15*i), end=now+timedelta(minutes=15*(i+1)),
                      hours=.25, import_price=.131 if i < 4 else .13 if i < 6 else .4,
                      net_solar_kwh=0 if i < 6 else -.625) for i in range(10)]
        _, grid = coordinator()._plan_charge_windows_for_horizon(
            slots=slots, now=now, usable_capacity_kwh=2.5, current_remaining_capacity_kwh=2.5,
            max_charge_kw=2, max_discharge_kw=3, battery_min_profit=.08,
        )
        self.assertEqual(len(grid), 1)
        self.assertEqual(grid[0]["start"], (now+timedelta(minutes=15)).isoformat())
        self.assertAlmostEqual(grid[0]["charge_kwh"], 2.5)

    def test_replanning_keeps_acquisition_price_floor_during_discharge(self):
        now = datetime(2026, 9, 19, 16)
        c = coordinator()
        c._battery_grid_charge_price = .13
        c._charge_session_started = False
        c._discharge_session_started = True
        slots = [dict(start=now+timedelta(hours=i), end=now+timedelta(hours=i+1), hours=1,
                      import_price=.132 if i == 0 else .21, export_price=.05,
                      net_solar_kwh=-1, demand_kwh=1, solar_kwh=0) for i in range(3)]
        windows, mode = c._build_mode_windows_from_hourly_plan(
            slots=slots, now=now, planned_solar_charge_windows=[], planned_grid_charge_windows=[],
            initial_usable_energy_kwh=2, usable_capacity_kwh=8, battery_soc_percent=40,
            average_price=.2, average_export_price=.1, max_charge_kw=3, max_discharge_kw=3,
            battery_min_profit=.08,
        )
        self.assertEqual(mode, "accu_uit")
        self.assertTrue(any(w["mode"] == "ontladen" for w in windows))
        self.assertTrue(all(datetime.fromisoformat(w["start"]) >= now+timedelta(hours=1)
                            for w in windows if w["mode"] == "ontladen"))

    def test_grid_purchase_floor_survives_restart_even_when_cycle_latch_expires(self):
        now = datetime.now()
        c = coordinator()
        c.hass = SimpleNamespace(data={})
        c.config_entry = SimpleNamespace(entry_id="battery")
        c._battery_grid_charge_price = .131
        c._store_battery_cycle_state_snapshot(now - timedelta(days=1))
        restarted = coordinator()
        restarted.hass = c.hass
        restarted.config_entry = c.config_entry
        restarted._restore_recent_battery_cycle_state()
        self.assertEqual(restarted._battery_grid_charge_price, .131)

    def test_live_grid_charge_records_cost_before_next_refresh(self):
        now = datetime(2026, 9, 19, 13)
        c = coordinator()
        c.hass = SimpleNamespace(data={})
        c.config_entry = SimpleNamespace(entry_id="battery")
        slots = [dict(start=now, end=now+timedelta(hours=1), hours=1,
                      import_price=.131, export_price=.03, net_solar_kwh=-1)]
        _, mode = c._build_mode_windows_from_hourly_plan(
            slots=slots, now=now, planned_solar_charge_windows=[],
            planned_grid_charge_windows=[dict(start=now.isoformat(), end=(now+timedelta(minutes=30)).isoformat(),
                                              charge_kwh=1.5, usable_hours=.5)],
            initial_usable_energy_kwh=4, usable_capacity_kwh=8, battery_soc_percent=60,
            average_price=.2, average_export_price=.1, max_charge_kw=3, max_discharge_kw=3,
        )
        self.assertEqual(mode, "laden_van_net")
        self.assertEqual(c._battery_grid_charge_price, .131)
        runtime_key = c._store_battery_cycle_state_snapshot.__globals__["RUNTIME_STATE"]
        self.assertEqual(c.hass.data[runtime_key]["battery"]["battery_grid_charge_price"], .131)

    def test_september_18_discharge_does_not_hide_tomorrow_grid_charge(self):
        now, slots, result = replay_full_plan(
            "2026_09_18", "2026-09-18T19:15:10.454352+02:00", soc=96, discharging=True, reserve=60,
        )
        self.assertEqual(result.battery_strategy, "ontladen")
        self.assertTrue(result.planned_grid_charge_windows)
        self.assertTrue(any(w["mode"] == "laden_van_net" for w in result.planned_battery_mode_windows))
        self.assertFalse(result.battery_no_charge_reserve_active)
        self.assertTrue(all(datetime.fromisoformat(w["start"]).date() > now.date()
                            for w in result.planned_grid_charge_windows))
        self.assertGreater(sum(w["charge_kwh"] for w in result.planned_grid_charge_windows), .4)
        trace = energy_trace(now, slots, result.planned_battery_mode_windows, initial=9.6)
        self.assertGreaterEqual(min(row[1] for row in trace), 2.0 - .005)
        self.assertLessEqual(max(row[1] for row in trace), 10.0 + .005)

    def test_full_sensor_plan_preserves_energy_limited_stop_times(self):
        now, slots, result = replay_full_plan("2026_09_18", "2026-09-18T19:15:10+02:00", soc=96, discharging=True)
        windows = result.planned_battery_mode_windows
        trace = energy_trace(now, slots, windows, initial=9.6)
        self.assertGreaterEqual(min(row[1] for row in trace), 2.0 - 0.005)
        self.assertLessEqual(max(row[1] for row in trace), 10.0 + 0.005)
        for previous, following in zip(windows, windows[1:]):
            self.assertEqual(previous["end"], following["start"])
        grid_end = next(w["end"] for w in windows if w["mode"] == "laden_van_net")
        self.assertIn({"at": grid_end, "mode": "accu_uit"}, result.planned_battery_mode_schedule)

    def test_small_charge_does_not_create_full_battery(self):
        now = datetime(2026, 9, 17, 12)
        _, energy, _ = coordinator()._append_charge_window_mode(
            hourly_modes=[], slot={"start": now, "import_price": .16},
            charge_window={"end": now + timedelta(minutes=10), "charge_kwh": .5},
            mode="laden_van_net", price_key="import_price", now=now, current_mode="accu_uit",
            sim_usable_energy_kwh=1, usable_capacity_kwh=8, max_charge_kw=3,
        )
        self.assertEqual(energy, 1.5)

    def test_partial_charge_cycle_only_discharges_the_energy_actually_stored(self):
        now = datetime(2026, 9, 17, 12)
        slots = [dict(start=now+timedelta(hours=i), end=now+timedelta(hours=i+1), hours=1,
                      import_price=.16 if i == 0 else .42, export_price=.10,
                      net_solar_kwh=-1, demand_kwh=1, solar_kwh=0) for i in range(5)]
        windows, _ = coordinator()._build_mode_windows_from_hourly_plan(
            slots=slots, now=now, planned_solar_charge_windows=[],
            planned_grid_charge_windows=[dict(start=now.isoformat(), end=(now+timedelta(minutes=10)).isoformat(),
                                              usable_hours=1/6, charge_kwh=.5)],
            initial_usable_energy_kwh=1, usable_capacity_kwh=8, battery_soc_percent=30,
            average_price=.30, average_export_price=.20, max_charge_kw=3, max_discharge_kw=3,
        )
        trace = energy_trace(now, slots, windows, initial=3)
        self.assertGreaterEqual(min(row[1] for row in trace), 2.0 - 1e-5)
        self.assertAlmostEqual(trace[-1][1], 2.0)

    def test_grid_topup_does_not_reuse_the_same_cheap_slot(self):
        now = datetime(2026, 9, 17, 12)
        slots = [dict(start=now+timedelta(minutes=15*i), end=now+timedelta(minutes=15*(i+1)),
                      hours=.25, import_price=.16 if i == 0 else .42, net_solar_kwh=-.25)
                 for i in range(3)]
        _, grid = coordinator()._plan_charge_windows_for_horizon(
            slots=slots, now=now, usable_capacity_kwh=8, current_remaining_capacity_kwh=8,
            max_charge_kw=3, max_discharge_kw=3, battery_min_profit=.08,
        )
        self.assertAlmostEqual(sum(w["charge_kwh"] for w in grid), .75)

    def test_unknown_future_prices_cannot_justify_grid_arbitrage(self):
        now = datetime(2026, 9, 17, 12)
        for unknown_index in (0, 1):
            slots = [dict(start=now+timedelta(hours=i), end=now+timedelta(hours=i+1),
                          hours=1, import_price=.16 if i == 0 else .42, net_solar_kwh=-1,
                          price_known=i != unknown_index) for i in range(2)]
            _, grid = coordinator()._plan_charge_windows_for_horizon(
                slots=slots, now=now, usable_capacity_kwh=8, current_remaining_capacity_kwh=3.2,
                max_charge_kw=3, max_discharge_kw=3, battery_min_profit=.08,
            )
            self.assertEqual(grid, [])

    def test_feedback_commands_respect_physical_capacity(self):
        now, slots, _, _, windows, mode = replay()
        self.assertEqual(mode, "accu_uit")
        trace = energy_trace(now, slots, windows)
        self.assertGreaterEqual(min(row[1] for row in trace), 2.0 - 0.005)
        self.assertLessEqual(max(row[1] for row in trace), 10.0 + 0.005)


if __name__ == "__main__":
    now, slots, result = replay_full_plan()
    trace = energy_trace(now, slots, result.planned_battery_mode_windows)
    print(result.battery_strategy, "energy range", min(row[1] for row in trace), max(row[1] for row in trace))
    for window in result.planned_battery_mode_windows:
        print(window)
