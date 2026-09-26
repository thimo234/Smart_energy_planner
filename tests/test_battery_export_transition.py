"""Partial export must retain the scheduled supply to the house."""

from datetime import datetime, timedelta
import unittest

from test_battery_energy_accounting import coordinator, energy_trace, replay_full_plan


class ExportTransitionTest(unittest.TestCase):
    def test_feedback_export_returns_directly_to_home_discharge(self):
        now, slots, result = replay_full_plan(
            "2026_09_25_full", "2026-09-25T15:43:02+02:00", soc=100,
            reserve=60, tax_deduction=.11, isolated_cycles=True,
        )
        windows = result.planned_battery_mode_windows
        exports = [i for i, w in enumerate(windows) if w["mode"] == "ontladen_naar_net"]
        self.assertTrue(exports)
        for i in exports:
            self.assertEqual(windows[i + 1]["mode"], "ontladen")
            self.assertEqual(windows[i]["end"], windows[i + 1]["start"])
        trace = energy_trace(now, slots, windows, initial=10)
        self.assertGreaterEqual(min(e for _, e, _ in trace), 2 - 1e-6)
        self.assertLessEqual(max(e for _, e, _ in trace), 10 + 1e-6)
        self.assertAlmostEqual(trace[-1][1], 10, delta=.005)

    def test_partial_export_preserves_full_home_supply_and_energy_budget(self):
        for home_kw in (0, .5):
            with self.subTest(home_kw=home_kw):
                now = datetime.fromisoformat("2026-09-26T18:00:00+02:00")
                slots = [dict(start=now + timedelta(hours=i), end=now + timedelta(hours=i+1),
                              hours=1, import_price=.5 if i == 1 else .3,
                              export_price=.4 if i == 1 else .2, price_known=True,
                              demand_kwh=home_kw, solar_kwh=0, net_solar_kwh=-home_kw)
                         for i in range(4)]
                instance = coordinator()
                instance._charge_session_started = False
                instance._discharge_session_started = True
                instance._cycle_export_planning = True
                energy = 3 * home_kw + .5
                windows, _ = instance._build_mode_windows_from_hourly_plan(
                    slots=slots, now=now, planned_solar_charge_windows=[],
                    planned_grid_charge_windows=[dict(start=slots[3]["start"].isoformat(),
                        end=slots[3]["end"].isoformat(), usable_hours=1, charge_kwh=3)],
                    initial_usable_energy_kwh=energy, usable_capacity_kwh=8,
                    battery_soc_percent=20 + energy*10, average_price=.3,
                    average_export_price=.2, max_charge_kw=3, max_discharge_kw=3,
                )
                exports = [w for w in windows if w["mode"] == "ontladen_naar_net"]
                self.assertEqual(len(exports), 1)
                exported = exports[0]
                duration = (datetime.fromisoformat(exported["end"]) -
                            datetime.fromisoformat(exported["start"])).total_seconds()/3600
                self.assertAlmostEqual(duration * (3-home_kw), .5, places=6)
                following = windows[windows.index(exported)+1]
                self.assertEqual(following["mode"], "ontladen" if home_kw else "accu_uit")
                self.assertEqual(following["start"], exported["end"])
                before_charge = [w for w in windows if datetime.fromisoformat(w["start"]) < slots[3]["start"]]
                trace = energy_trace(now, slots, before_charge, initial=2+energy)
                self.assertAlmostEqual(trace[-1][1], 2, places=6)
                self.assertGreaterEqual(min(e for _, e, _ in trace), 2-1e-6)
