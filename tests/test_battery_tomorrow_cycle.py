"""A nearly full afternoon battery must not erase tomorrow's charge cycle."""

from datetime import datetime
import unittest
from unittest.mock import patch

from test_battery_energy_accounting import coordinator, energy_trace, replay_full_plan


class TomorrowCycleTest(unittest.TestCase):
    def test_cycle_boundary_ignores_pauses_and_charging_source_changes(self):
        modes = ["laden_met_zonne_energie", "accu_uit", "laden_van_net",
                 "ontladen", "accu_uit", "ontladen_naar_net", "laden_van_net"]
        windows = [{"start": f"2026-09-25T{hour:02d}:00:00+02:00",
                    "end": f"2026-09-25T{hour+1:02d}:00:00+02:00", "mode": mode}
                   for hour, mode in enumerate(modes, start=10)]
        self.assertEqual(coordinator()._two_cycle_end(windows),
                         datetime.fromisoformat("2026-09-25T16:00:00+02:00"))

    def test_tomorrow_inputs_and_preview_cannot_change_live_plan_or_state(self):
        options = dict(snapshot="2026_09_25_afternoon",
                       timestamp="2026-09-25T15:17:12.932148+02:00",
                       soc=99, reserve=60, tax_deduction=.11, isolated_cycles=True)
        live = coordinator()
        _, _, baseline = replay_full_plan(**options, instance=live)
        cls = type(live)
        original = cls._estimate_unknown_price_plan
        def changed_preview(instance, **inputs):
            if inputs.get("include_known_prices"):
                cutoff = inputs["preview_start"]
                inputs["slots"] = [{**s, "import_price": -1.0, "export_price": -1.11,
                                     "solar_kwh": 0.0, "net_solar_kwh": -s["demand_kwh"]}
                                    if s["start"] >= cutoff else s for s in inputs["slots"]]
            return original(instance, **inputs)
        for preview_disabled in (False, True):
            other = coordinator()
            if preview_disabled:
                with patch.object(cls, "_estimate_unknown_price_plan", return_value=[]):
                    _, _, result = replay_full_plan(**options, instance=other)
            else:
                with patch.object(cls, "_estimate_unknown_price_plan", changed_preview):
                    _, _, result = replay_full_plan(**options, instance=other)
            with self.subTest(preview_disabled=preview_disabled):
                for key in ("battery_strategy", "planned_battery_mode_windows", "planned_battery_mode_schedule",
                            "battery_reserved_energy_kwh", "battery_no_charge_reserve_active",
                            "next_charge_window_start", "next_discharge_window_start", "current_price", "price_spread"):
                    self.assertEqual(getattr(baseline, key), getattr(result, key), key)
                for key in ("_charge_session_started", "_discharge_session_started",
                            "_active_charge_phase_end", "_active_charge_phase_mode", "_battery_grid_charge_price"):
                    self.assertEqual(getattr(live, key, None), getattr(other, key, None), key)
        self.assertTrue(all(w["planning_estimated"] and not w["price_estimated"]
                            for w in baseline.estimated_battery_mode_windows))
        self.assertEqual(baseline.planned_battery_mode_windows[-1]["end"],
                         baseline.estimated_battery_mode_windows[0]["start"])
        families = []
        for w in baseline.planned_battery_mode_windows:
            if w["mode"] == "accu_uit":
                continue
            family = "charge" if w["mode"].startswith("laden_") else "discharge"
            if not families or families[-1] != family:
                families.append(family)
        self.assertEqual(families, ["charge", "discharge"])
        self.assertGreater(baseline.planned_battery_mode_windows[-1]["end"],
                           "2026-09-26T00:00:00+02:00")

    def test_known_tomorrow_prices_keep_a_complete_cycle(self):
        for soc in (95, 99, 100):
            with self.subTest(soc=soc):
                now, slots, result = replay_full_plan(
                    snapshot="2026_09_25_afternoon",
                    timestamp="2026-09-25T15:17:12.932148+02:00",
                    soc=soc, reserve=60, tax_deduction=.11, isolated_cycles=True,
                )
                windows = result.planned_battery_mode_windows + result.estimated_battery_mode_windows
                tomorrow = [w for w in windows if w["start"].startswith("2026-09-26")]
                charge = [w for w in tomorrow if w["mode"] == "laden_met_zonne_energie"]
                self.assertTrue(charge)
                self.assertTrue(any(w["mode"] == "ontladen" and w["start"] >= charge[-1]["end"]
                                    for w in tomorrow))
                before = [w for w in windows if w["end"] <= charge[0]["start"]]
                before_trace = energy_trace(now, slots, before, initial=soc / 10)
                self.assertAlmostEqual(before_trace[-1][1], 2, delta=.05)
                trace = energy_trace(now, slots, windows, initial=soc / 10)
                self.assertGreaterEqual(min(e for _, e, _ in trace), 2 - 1e-6)
                self.assertLessEqual(max(e for _, e, _ in trace), 10 + 1e-6)
                self.assertAlmostEqual(max(e for t, e, _ in trace if t.startswith("2026-09-26")),
                                       10, delta=.05)
                # Surplus export must use executable slots within eight hours
                # before the next cycle, while still covering household demand.
                exports = [w for w in windows if w["mode"] == "ontladen_naar_net"]
                self.assertTrue(exports)
                for window in exports:
                    gap = (datetime.fromisoformat(charge[0]["start"])
                           - datetime.fromisoformat(window["start"])).total_seconds() / 3600
                    self.assertGreater(gap, 0)
                    self.assertLessEqual(gap, 8)
