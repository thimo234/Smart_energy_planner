"""Preview continuity: full cycles use peaks; partial fills must first finish."""

from datetime import datetime, timedelta
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from types import SimpleNamespace

from test_battery_energy_accounting import coordinator, energy_trace
from custom_components.smart_energy_planner.price_models import PlannerWindow
from custom_components.smart_energy_planner.battery_models import SolarWindow
from custom_components.smart_energy_planner.battery_forecast import build_energy_balance_slots


def replay_evening_gap(*, soc=None, instance=None, charge_started=False, discharge_started=False, at=None):
    data = json.loads((Path(__file__).parent / "fixtures" /
                       "battery_2026_11_11_evening_gap.json").read_text())
    now = datetime.fromisoformat(data["captured_at"])
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    prices = data["import_prices"] + [data["tomorrow_price"]] * 96
    windows = [PlannerWindow(midnight + timedelta(minutes=15*i),
                             midnight + timedelta(minutes=15*(i+1)), price, i < 96)
               for i, price in enumerate(prices)]
    demand = [{"start": (midnight + timedelta(minutes=15*i)).isoformat(),
               "end": (midnight + timedelta(minutes=15*(i+1))).isoformat(),
               "estimated_kwh": kwh} for i, kwh in enumerate(data["demand_kwh"])]
    solar = [SolarWindow(midnight + timedelta(hours=i),
                         midnight + timedelta(hours=i+1), kwh, None, None)
             for i, kwh in enumerate(data["solar_kwh"])]
    exports = [PlannerWindow(w.start, w.end, w.price-.12, w.price_known) for w in windows]
    slots = build_energy_balance_slots(price_windows=windows, export_price_windows=exports,
                                        solar_windows=solar, hourly_demand=demand,
                                        horizon_start=now, demand_safety_margin=0.)
    c = instance or coordinator()
    c.config_entry = SimpleNamespace(data={
        "battery_enabled": True, "battery_capacity_kwh": 10., "battery_min_soc_percent": 24.,
        "battery_no_charge_min_soc_percent": 60., "battery_max_charge_kw": 2.5,
        "battery_max_discharge_kw": 3., "battery_min_profit_per_kwh": .1,
        "battery_demand_safety_margin": 0., "battery_charge_safety_margin": 0.,
    }, options={})
    c._charge_session_started = charge_started
    c._discharge_session_started = discharge_started
    c._battery_cycle_state_initialized = True
    c._battery_cycle_state_restored_recent = False
    c._store_battery_cycle_state_snapshot = lambda now: None
    namespace = c._build_plan_for_horizon.__globals__
    with patch.object(namespace["dt_util"], "now", return_value=now), patch.dict(namespace, {
        "build_hourly_home_demand_forecast": lambda **kwargs: demand,
        "align_price_responsive_demand_to_cheap_hours": lambda demand, prices, **kwargs: demand,
    }):
        result = c._build_plan(
            planner_kind="battery", windows=windows[:96], all_windows=windows,
            export_windows=exports[:96], all_export_windows=exports, battery_switch_windows=windows,
            price_average=None, export_price_average=None, current_price=.541,
            solar_forecast_kwh=sum(data["solar_kwh"][:24]), solar_windows=solar,
            all_solar_windows=solar, solcast_confidence=None, lookback_average_kwh=20.,
            total_energy_daily_average_kwh=20., non_heating_daily_average_kwh=20.,
            hourly_demand_table={}, demand_adjustment_factor=1.,
            battery_soc_percent=data["soc"] if soc is None else soc,
            price_resolution="quarter_hourly", source_status={"price_sensor": "ok"}, source_errors=[])
    return now, slots, result


class EveningPeakRegressionTest(unittest.TestCase):
    def test_preview_retains_incomplete_solar_charge_through_evening_peak(self):
        now, slots, _ = replay_evening_gap()
        data = json.loads((Path(__file__).parent / "fixtures" /
                           "battery_2026_11_11_evening_gap.json").read_text())
        c = coordinator()
        preview = c._estimate_unknown_price_plan(
            slots=slots, now=now, actual_windows=data["actual_windows"],
            initial_energy=7.2, capacity=7.6, max_charge=2.5, max_discharge=3.,
            reserve=3.6, min_profit=.1, include_known_prices=True,
            preview_start=datetime.fromisoformat("2026-11-11T16:00:00+01:00"))
        peak = datetime.fromisoformat("2026-11-11T17:45:00+01:00")
        mode = next(w["mode"] for w in preview
                    if datetime.fromisoformat(w["start"]) <= peak < datetime.fromisoformat(w["end"]))
        self.assertEqual(mode, "accu_uit", 'partial solar refill cannot authorize discharge')
        trace = energy_trace(now, slots, data["actual_windows"] + preview,
                             initial=9.6, max_charge=2.5)
        self.assertGreaterEqual(min(e for _, e, _ in trace), 2.4 - 1e-6)
        self.assertLessEqual(max(e for _, e, _ in trace), 10. + 1e-6)

    def test_only_completed_charge_phase_releases_preview_latch(self):
        now, slots, _ = replay_evening_gap()
        data = json.loads((Path(__file__).parent / "fixtures" /
                           "battery_2026_11_11_evening_gap.json").read_text())
        for cutoff, expected_charging in [("15:30:00", True), ("16:00:00", True)]:
            with self.subTest(cutoff=cutoff):
                c = coordinator()
                c._charge_session_started = True
                c._discharge_session_started = False
                original_state = dict(vars(c))
                cls = type(c)
                original = cls._plan_charge_windows_for_horizon
                charging_states = []

                def observe(instance, **kwargs):
                    charging_states.append(instance._charge_session_started)
                    return original(instance, **kwargs)

                with patch.object(cls, "_plan_charge_windows_for_horizon", observe):
                    c._estimate_unknown_price_plan(
                        slots=slots, now=now, actual_windows=data["actual_windows"],
                        initial_energy=7.2, capacity=7.6, max_charge=2.5,
                        max_discharge=3., reserve=3.6, min_profit=.1,
                        include_known_prices=True,
                        preview_start=datetime.fromisoformat(f"2026-11-11T{cutoff}+01:00"))
                self.assertTrue(charging_states)
                self.assertTrue(all(state == expected_charging for state in charging_states))
                self.assertEqual(vars(c), original_state,
                                 "preview must not mutate live coordinator state")

    def test_observed_incomplete_solar_topup_waits_at_evening_peak(self):
        now, slots, result = replay_evening_gap()
        windows = result.planned_battery_mode_windows + result.estimated_battery_mode_windows
        peak = datetime.fromisoformat("2026-11-11T17:45:00+01:00")
        mode = next(w["mode"] for w in windows
                    if datetime.fromisoformat(w["start"]) <= peak < datetime.fromisoformat(w["end"]))
        self.assertEqual(mode, "accu_uit", 'high prices do not complete an unfinished refill')
        trace = energy_trace(now, slots, windows, initial=9.6, max_charge=2.5)
        self.assertGreaterEqual(min(e for _, e, _ in trace), 2. - 1e-6)
        self.assertLessEqual(max(e for _, e, _ in trace), 10. + 1e-6)


if __name__ == "__main__":
    unittest.main()
