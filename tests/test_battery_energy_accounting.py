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


def feedback_slots():
    data = json.loads((Path(__file__).parent / "fixtures/battery_2026_09_17.json").read_text())
    parse = datetime.fromisoformat
    now = parse("2026-09-17T12:33:32+02:00")
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


def replay_full_plan():
    """Exercise final sensor scheduling with the already computed demand input."""
    now, slots = feedback_slots()
    data = json.loads((Path(__file__).parent / "fixtures/battery_2026_09_17.json").read_text())
    prices = [PlannerWindow(s["start"], s["end"], s["import_price"], s["price_known"]) for s in slots]
    exports = [PlannerWindow(s["start"], s["end"], s["export_price"], s["price_known"]) for s in slots]
    solar = [SolarWindow(datetime.fromisoformat(w["start"]), datetime.fromisoformat(w["end"]),
                         w["estimated_kwh"], None, None) for w in data["estimated_hourly_solar_forecast"]]
    c = coordinator()
    c.config_entry = SimpleNamespace(data={"battery_enabled": True, "battery_capacity_kwh": 10, "battery_min_soc_percent": 20,
                                          "battery_max_charge_kw": 3, "battery_max_discharge_kw": 3,
                                          "battery_demand_safety_margin": 0}, options={})
    c._locked_eco_window = c._locked_preheat_end = c._preheat_expired_at = None
    namespace = c._build_plan.__globals__
    with patch.object(namespace["dt_util"], "now", return_value=now), patch.dict(namespace, {
        "build_hourly_home_demand_forecast": lambda **kwargs: data["estimated_hourly_home_demand"],
        "align_price_responsive_demand_to_cheap_hours": lambda demand, prices: demand,
    }):
        result = c._build_plan(
            planner_kind="battery", windows=[p for p in prices if p.price_known], all_windows=prices,
            export_windows=exports, all_export_windows=exports, battery_switch_windows=prices,
            price_average=None, export_price_average=None, current_price=.159,
            solar_forecast_kwh=sum(w.forecast_kwh for w in solar if w.start.date() == now.date()),
            solar_windows=solar, all_solar_windows=solar, solcast_confidence=None,
            heating_estimate_kwh=0, lookback_average_kwh=20, total_energy_daily_average_kwh=20,
            non_heating_daily_average_kwh=20, hourly_demand_table={}, demand_adjustment_factor=1,
            room_temperature_c=None, thermostat_setpoint_c=None, thermostat_cool_setpoint_c=None,
            thermostat_preheat_setpoint_c=None, thermostat_eco_setpoint_c=None,
            room_cooling_hours_to_eco=None, room_cooling_rate_c_per_hour=None,
            cooling_reference_outdoor_temp_c=None, battery_soc_percent=68,
            price_resolution="quarter_hourly", source_status={"price_sensor": "ok"}, source_errors=[],
        )
    return now, slots, result


def energy_trace(now, slots, windows, initial=6.8):
    """Integrate mode duration and forecast power without clamping to capacity."""
    energy = initial
    trace = []
    for window in windows:
        for slot in slots:
            start = max(now, datetime.fromisoformat(window["start"]), slot["start"])
            end = min(datetime.fromisoformat(window["end"]), slot["end"])
            hours = max(0, (end - start).total_seconds() / 3600)
            net_kw = slot["net_solar_kwh"] / slot["hours"]
            rate = {"laden_van_net": 3, "laden_met_zonne_energie": min(3, max(0, net_kw)),
                    "ontladen": -min(3, max(0, -net_kw)), "ontladen_naar_net": -3,
                    "accu_uit": 0}[window["mode"]]
            energy += hours * rate
            if hours:
                trace.append((end.isoformat(), energy, window["mode"]))
    return trace


class EnergyAccountingTest(unittest.TestCase):
    def test_full_sensor_plan_preserves_energy_limited_stop_times(self):
        now, slots, result = replay_full_plan()
        self.assertEqual(result.battery_strategy, "laden_van_net")
        windows = result.planned_battery_mode_windows
        trace = energy_trace(now, slots, windows)
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
            slots=slots, now=now, usable_capacity_kwh=8, current_remaining_capacity_kwh=3.2,
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
        self.assertEqual(mode, "laden_van_net")
        trace = energy_trace(now, slots, windows)
        self.assertGreaterEqual(min(row[1] for row in trace), 2.0 - 0.005)
        self.assertLessEqual(max(row[1] for row in trace), 10.0 + 0.005)


if __name__ == "__main__":
    now, slots, result = replay_full_plan()
    trace = energy_trace(now, slots, result.planned_battery_mode_windows)
    print(result.battery_strategy, "energy range", min(row[1] for row in trace), max(row[1] for row in trace))
    for window in result.planned_battery_mode_windows:
        print(window)
