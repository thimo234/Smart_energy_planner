import unittest
from datetime import datetime, timedelta
import sys
import types

from test_support import install_package_stub

install_package_stub()
from custom_components.smart_energy_planner.battery_planner import (
    BATTERY_MODE_GRID_CHARGE,
    BATTERY_MODE_OFF,
    BATTERY_MODE_SOLAR_CHARGE,
    collapse_short_off_mode_windows,
    normalize_full_battery_charge_mode,
    normalize_full_battery_mode_windows,
    summarize_battery_cycles,
)


def _install_homeassistant_stubs() -> None:
    homeassistant = sys.modules.setdefault("homeassistant", types.ModuleType("homeassistant"))

    components = sys.modules.setdefault("homeassistant.components", types.ModuleType("homeassistant.components"))
    recorder = sys.modules.setdefault("homeassistant.components.recorder", types.ModuleType("homeassistant.components.recorder"))
    recorder.get_instance = lambda *args, **kwargs: None
    recorder.history = types.SimpleNamespace()
    components.recorder = recorder

    config_entries = sys.modules.setdefault(
        "homeassistant.config_entries",
        types.ModuleType("homeassistant.config_entries"),
    )
    config_entries.ConfigEntry = object

    const = sys.modules.setdefault("homeassistant.const", types.ModuleType("homeassistant.const"))
    const.STATE_OFF = "off"
    const.STATE_ON = "on"
    const.STATE_UNAVAILABLE = "unavailable"
    const.STATE_UNKNOWN = "unknown"

    core = sys.modules.setdefault("homeassistant.core", types.ModuleType("homeassistant.core"))
    core.HomeAssistant = object
    core.callback = lambda func: func

    helpers = sys.modules.setdefault("homeassistant.helpers", types.ModuleType("homeassistant.helpers"))
    event = sys.modules.setdefault("homeassistant.helpers.event", types.ModuleType("homeassistant.helpers.event"))
    event.async_track_time_interval = lambda *args, **kwargs: None
    storage = sys.modules.setdefault("homeassistant.helpers.storage", types.ModuleType("homeassistant.helpers.storage"))
    storage.Store = object
    update_coordinator = sys.modules.setdefault(
        "homeassistant.helpers.update_coordinator",
        types.ModuleType("homeassistant.helpers.update_coordinator"),
    )

    class DataUpdateCoordinator:
        @classmethod
        def __class_getitem__(cls, item):
            return cls

    update_coordinator.DataUpdateCoordinator = DataUpdateCoordinator
    helpers.event = event
    helpers.storage = storage
    helpers.update_coordinator = update_coordinator

    util = sys.modules.setdefault("homeassistant.util", types.ModuleType("homeassistant.util"))
    dt = sys.modules.setdefault("homeassistant.util.dt", types.ModuleType("homeassistant.util.dt"))
    dt.now = datetime.now
    dt.as_local = lambda value: value
    dt.parse_datetime = datetime.fromisoformat
    util.dt = dt
    homeassistant.components = components
    homeassistant.config_entries = config_entries
    homeassistant.const = const
    homeassistant.core = core
    homeassistant.helpers = helpers
    homeassistant.util = util


_install_homeassistant_stubs()
from custom_components.smart_energy_planner.coordinator import SmartEnergyPlannerCoordinator


class BatteryPlannerTest(unittest.TestCase):
    def _feedback_state_slots(self, start_at: datetime) -> list[dict[str, float | datetime]]:
        prices = {
            "2026-06-25T17:00:00": 0.259,
            "2026-06-25T18:00:00": 0.295,
            "2026-06-25T19:00:00": 0.37925,
            "2026-06-25T20:00:00": 0.4925,
            "2026-06-25T21:00:00": 0.4365,
            "2026-06-25T22:00:00": 0.36075,
            "2026-06-25T23:00:00": 0.32525,
            "2026-06-26T00:00:00": 0.30525,
            "2026-06-26T01:00:00": 0.2885,
            "2026-06-26T02:00:00": 0.278,
            "2026-06-26T03:00:00": 0.27725,
            "2026-06-26T04:00:00": 0.27875,
            "2026-06-26T05:00:00": 0.2875,
            "2026-06-26T06:00:00": 0.31025,
            "2026-06-26T07:00:00": 0.2985,
            "2026-06-26T08:00:00": 0.27475,
            "2026-06-26T09:00:00": 0.25975,
            "2026-06-26T10:00:00": 0.24475,
            "2026-06-26T11:00:00": 0.2155,
            "2026-06-26T12:00:00": 0.16675,
            "2026-06-26T13:00:00": 0.144,
            "2026-06-26T14:00:00": 0.173,
            "2026-06-26T15:00:00": 0.21925,
            "2026-06-26T16:00:00": 0.24525,
            "2026-06-26T17:00:00": 0.26725,
            "2026-06-26T18:00:00": 0.31425,
            "2026-06-26T19:00:00": 0.4285,
            "2026-06-26T20:00:00": 0.56275,
            "2026-06-26T21:00:00": 0.49425,
            "2026-06-26T22:00:00": 0.37725,
            "2026-06-26T23:00:00": 0.319,
        }
        demand = {
            "2026-06-25T17:00:00": 1.368,
            "2026-06-25T18:00:00": 0.572,
            "2026-06-25T19:00:00": 0.488,
            "2026-06-25T20:00:00": 0.398,
            "2026-06-25T21:00:00": 0.492,
            "2026-06-25T22:00:00": 0.692,
            "2026-06-25T23:00:00": 0.391,
            "2026-06-26T00:00:00": 0.511,
            "2026-06-26T01:00:00": 0.278,
            "2026-06-26T02:00:00": 0.281,
            "2026-06-26T03:00:00": 0.21,
            "2026-06-26T04:00:00": 0.332,
            "2026-06-26T05:00:00": 0.191,
            "2026-06-26T06:00:00": 1.796,
            "2026-06-26T07:00:00": 0.614,
            "2026-06-26T08:00:00": 1.588,
            "2026-06-26T09:00:00": 2.246,
            "2026-06-26T10:00:00": 2.246,
            "2026-06-26T11:00:00": 1.337,
            "2026-06-26T12:00:00": 1.574,
            "2026-06-26T13:00:00": 2.246,
            "2026-06-26T14:00:00": 2.246,
            "2026-06-26T15:00:00": 1.75,
            "2026-06-26T16:00:00": 1.572,
            "2026-06-26T17:00:00": 0.949,
            "2026-06-26T18:00:00": 0.761,
            "2026-06-26T19:00:00": 0.748,
            "2026-06-26T20:00:00": 0.661,
            "2026-06-26T21:00:00": 0.876,
            "2026-06-26T22:00:00": 0.65,
            "2026-06-26T23:00:00": 1.082,
        }
        solar = {
            "2026-06-26T07:00:00": 0.655,
            "2026-06-26T08:00:00": 1.637,
            "2026-06-26T09:00:00": 2.947,
            "2026-06-26T10:00:00": 4.257,
            "2026-06-26T11:00:00": 5.239,
            "2026-06-26T12:00:00": 5.567,
            "2026-06-26T13:00:00": 4.912,
            "2026-06-26T14:00:00": 3.602,
            "2026-06-26T15:00:00": 2.292,
            "2026-06-26T16:00:00": 1.31,
            "2026-06-26T17:00:00": 0.327,
        }
        slots = []
        for stamp, price in prices.items():
            slot_start = datetime.fromisoformat(stamp)
            if slot_start + timedelta(hours=1) <= start_at:
                continue
            demand_kwh = demand.get(stamp, 0.0)
            solar_kwh = solar.get(stamp, 0.0)
            slots.append(
                {
                    "start": slot_start,
                    "end": slot_start + timedelta(hours=1),
                    "import_price": price,
                    "export_price": price,
                    "hours": 1.0,
                    "net_solar_kwh": round(solar_kwh - demand_kwh, 6),
                    "demand_kwh": demand_kwh,
                    "solar_kwh": solar_kwh,
                }
            )
        return slots

    def test_feedback_state_keeps_tomorrow_solar_charge_window_during_discharge_latch(self):
        now = datetime(2026, 6, 25, 17, 0)
        slots = self._feedback_state_slots(now)
        tomorrow = datetime(2026, 6, 26)

        coordinator = SmartEnergyPlannerCoordinator.__new__(SmartEnergyPlannerCoordinator)
        coordinator.config_entry = types.SimpleNamespace(data={}, options={})
        coordinator._active_charge_phase_end = None
        coordinator._active_charge_phase_mode = "accu_uit"
        coordinator._discharge_session_started = True

        solar_windows, grid_windows = SmartEnergyPlannerCoordinator._plan_charge_windows_for_horizon(
            coordinator,
            slots=slots,
            now=now,
            usable_capacity_kwh=8.0,
            current_remaining_capacity_kwh=0.2,
            max_charge_kw=3.0,
            max_discharge_kw=3.0,
            battery_min_profit=0.08,
            charge_safety_margin=0.5,
        )
        mode_windows, current_mode = SmartEnergyPlannerCoordinator._build_mode_windows_from_hourly_plan(
            coordinator,
            slots=slots,
            now=now,
            planned_solar_charge_windows=solar_windows,
            planned_grid_charge_windows=grid_windows,
            initial_usable_energy_kwh=7.8,
            usable_capacity_kwh=8.0,
            battery_soc_percent=98.0,
            average_price=0.30,
            average_export_price=0.30,
            max_charge_kw=3.0,
            max_discharge_kw=3.0,
        )
        cycle_summary = summarize_battery_cycles(
            full_planned_mode_windows=mode_windows,
            energy_balance_slots=slots,
            now=now,
        )

        self.assertIn(current_mode, ("accu_uit", "ontladen", "ontladen_naar_net"))
        self.assertTrue(solar_windows)
        self.assertFalse(any(datetime.fromisoformat(window["start"]) < tomorrow for window in grid_windows))
        self.assertEqual(cycle_summary["next_charge_window_start"], "2026-06-26T11:00:00")
        self.assertEqual(cycle_summary["next_charge_window_end"], "2026-06-26T15:00:00")

    def test_feedback_state_near_full_battery_does_not_show_evening_grid_charge(self):
        now = datetime(2026, 6, 25, 18, 15)
        slots = self._feedback_state_slots(now)
        tomorrow = datetime(2026, 6, 26)

        coordinator = SmartEnergyPlannerCoordinator.__new__(SmartEnergyPlannerCoordinator)
        coordinator.config_entry = types.SimpleNamespace(data={}, options={})
        coordinator._active_charge_phase_end = None
        coordinator._active_charge_phase_mode = "accu_uit"
        coordinator._discharge_session_started = True

        solar_windows, grid_windows = SmartEnergyPlannerCoordinator._plan_charge_windows_for_horizon(
            coordinator,
            slots=slots,
            now=now,
            usable_capacity_kwh=8.0,
            current_remaining_capacity_kwh=0.2,
            max_charge_kw=3.0,
            max_discharge_kw=3.0,
            battery_min_profit=0.08,
            charge_safety_margin=0.5,
        )
        mode_windows, current_mode = SmartEnergyPlannerCoordinator._build_mode_windows_from_hourly_plan(
            coordinator,
            slots=slots,
            now=now,
            planned_solar_charge_windows=solar_windows,
            planned_grid_charge_windows=grid_windows,
            initial_usable_energy_kwh=7.8,
            usable_capacity_kwh=8.0,
            battery_soc_percent=98.0,
            average_price=0.30,
            average_export_price=0.30,
            max_charge_kw=3.0,
            max_discharge_kw=3.0,
        )
        cycle_summary = summarize_battery_cycles(
            full_planned_mode_windows=mode_windows,
            energy_balance_slots=slots,
            now=now,
        )

        self.assertNotEqual(current_mode, "laden_van_net")
        self.assertFalse(any(datetime.fromisoformat(window["start"]) < tomorrow for window in grid_windows))
        self.assertFalse(
            any(
                window["mode"] == "laden_van_net"
                and datetime.fromisoformat(str(window["start"])) < tomorrow
                for window in mode_windows
            )
        )
        self.assertEqual(cycle_summary["next_charge_window_start"], "2026-06-26T11:00:00")

    def test_feedback_state_96_percent_battery_does_not_grid_charge_before_peak(self):
        now = datetime(2026, 6, 25, 18, 30)
        slots = self._feedback_state_slots(now)
        tomorrow = datetime(2026, 6, 26)

        coordinator = SmartEnergyPlannerCoordinator.__new__(SmartEnergyPlannerCoordinator)
        coordinator.config_entry = types.SimpleNamespace(data={}, options={})
        coordinator._active_charge_phase_end = None
        coordinator._active_charge_phase_mode = "accu_uit"
        coordinator._discharge_session_started = False

        solar_windows, grid_windows = SmartEnergyPlannerCoordinator._plan_charge_windows_for_horizon(
            coordinator,
            slots=slots,
            now=now,
            usable_capacity_kwh=8.0,
            current_remaining_capacity_kwh=0.4,
            max_charge_kw=3.0,
            max_discharge_kw=3.0,
            battery_min_profit=0.08,
            charge_safety_margin=0.5,
        )
        mode_windows, current_mode = SmartEnergyPlannerCoordinator._build_mode_windows_from_hourly_plan(
            coordinator,
            slots=slots,
            now=now,
            planned_solar_charge_windows=solar_windows,
            planned_grid_charge_windows=grid_windows,
            initial_usable_energy_kwh=7.6,
            usable_capacity_kwh=8.0,
            battery_soc_percent=96.0,
            average_price=0.30,
            average_export_price=0.30,
            max_charge_kw=3.0,
            max_discharge_kw=3.0,
        )
        cycle_summary = summarize_battery_cycles(
            full_planned_mode_windows=mode_windows,
            energy_balance_slots=slots,
            now=now,
        )

        self.assertNotEqual(current_mode, "laden_van_net")
        self.assertFalse(any(datetime.fromisoformat(window["start"]) < tomorrow for window in grid_windows))
        self.assertFalse(
            any(
                window["mode"] == "laden_van_net"
                and datetime.fromisoformat(str(window["start"])) < tomorrow
                for window in mode_windows
            )
        )
        self.assertEqual(cycle_summary["next_charge_window_start"], "2026-06-26T11:00:00")
        self.assertEqual(cycle_summary["next_charge_window_end"], "2026-06-26T15:00:00")

    def test_full_battery_grid_charge_becomes_solar_hold(self):
        mode = normalize_full_battery_charge_mode(
            mode=BATTERY_MODE_GRID_CHARGE,
            usable_energy_kwh=8.0,
            usable_capacity_kwh=8.0,
        )

        self.assertEqual(mode, BATTERY_MODE_SOLAR_CHARGE)

    def test_full_battery_mode_windows_convert_grid_charge(self):
        windows = [
            {
                "start": "2026-05-31T10:00:00",
                "end": "2026-05-31T11:00:00",
                "mode": BATTERY_MODE_GRID_CHARGE,
                "price": 0.12,
                "usable_hours": 1.0,
            }
        ]

        normalized = normalize_full_battery_mode_windows(
            windows=windows,
            usable_energy_kwh=8.0,
            usable_capacity_kwh=8.0,
        )

        self.assertEqual(normalized[0]["mode"], BATTERY_MODE_SOLAR_CHARGE)
        self.assertEqual(windows[0]["mode"], BATTERY_MODE_GRID_CHARGE)

    def test_short_off_mode_window_collapses_into_next_active_mode(self):
        start = datetime(2026, 6, 14, 10, 0)
        windows = [
            {
                "start": start,
                "end": start + timedelta(minutes=15),
                "mode": BATTERY_MODE_OFF,
                "price": 0.10,
                "usable_hours": 0.25,
            },
            {
                "start": start + timedelta(minutes=15),
                "end": start + timedelta(hours=1),
                "mode": BATTERY_MODE_SOLAR_CHARGE,
                "price": 0.10,
                "usable_hours": 0.75,
            },
        ]

        collapsed = collapse_short_off_mode_windows(windows)

        self.assertEqual(len(collapsed), 1)
        self.assertEqual(collapsed[0]["start"], start)
        self.assertEqual(collapsed[0]["end"], start + timedelta(hours=1))
        self.assertEqual(collapsed[0]["mode"], BATTERY_MODE_SOLAR_CHARGE)

    def test_long_off_mode_window_is_kept(self):
        start = datetime(2026, 6, 14, 10, 0)
        windows = [
            {
                "start": start,
                "end": start + timedelta(hours=1),
                "mode": BATTERY_MODE_OFF,
                "price": 0.10,
                "usable_hours": 1.0,
            },
            {
                "start": start + timedelta(hours=1),
                "end": start + timedelta(hours=2),
                "mode": BATTERY_MODE_SOLAR_CHARGE,
                "price": 0.10,
                "usable_hours": 1.0,
            },
        ]

        collapsed = collapse_short_off_mode_windows(windows)

        self.assertEqual(len(collapsed), 2)
        self.assertEqual(collapsed[0]["mode"], BATTERY_MODE_OFF)

    def test_solar_charge_plan_does_not_extend_into_evening_peak(self):
        now = datetime(2026, 6, 22, 7, 45)
        slots = []
        for hour in range(8, 24):
            start = now.replace(hour=hour, minute=0)
            price = 0.12 if hour in (11, 12) else (0.46 if hour >= 20 else 0.30)
            net_solar_kwh = 1.0 if 11 <= hour < 18 else (-0.8 if hour >= 20 else -0.2)
            slots.append(
                {
                    "start": start,
                    "end": start + timedelta(hours=1),
                    "import_price": price,
                    "export_price": price,
                    "hours": 1.0,
                    "net_solar_kwh": net_solar_kwh,
                    "demand_kwh": max(0.0, -net_solar_kwh),
                    "solar_kwh": max(0.0, net_solar_kwh),
                }
            )
        coordinator = SmartEnergyPlannerCoordinator.__new__(SmartEnergyPlannerCoordinator)
        coordinator.config_entry = types.SimpleNamespace(data={}, options={})
        coordinator._active_charge_phase_end = None
        coordinator._discharge_session_started = False

        solar_windows, grid_windows = SmartEnergyPlannerCoordinator._plan_charge_windows_for_horizon(
            coordinator,
            slots=slots,
            now=now,
            usable_capacity_kwh=8.0,
            current_remaining_capacity_kwh=2.0,
            max_charge_kw=1.0,
            max_discharge_kw=3.0,
            battery_min_profit=0.08,
        )

        self.assertEqual(len(solar_windows), 1)
        self.assertEqual(solar_windows[0]["start"], now.replace(hour=11, minute=0).isoformat())
        self.assertLessEqual(datetime.fromisoformat(solar_windows[0]["end"]), now.replace(hour=20, minute=0))
        self.assertTrue(all(datetime.fromisoformat(window["start"]) >= now.replace(hour=10, minute=0) for window in grid_windows))

        coordinator._active_charge_phase_mode = "accu_uit"
        mode_windows, current_mode = SmartEnergyPlannerCoordinator._build_mode_windows_from_hourly_plan(
            coordinator,
            slots=slots,
            now=now,
            planned_solar_charge_windows=solar_windows,
            planned_grid_charge_windows=grid_windows,
            initial_usable_energy_kwh=6.0,
            usable_capacity_kwh=8.0,
            average_price=0.30,
            average_export_price=0.30,
            max_charge_kw=1.0,
            max_discharge_kw=3.0,
        )

        self.assertEqual(current_mode, "accu_uit")
        self.assertTrue(
            any(
                window["mode"] == "ontladen"
                and window["start"] <= now.replace(hour=20, minute=0).isoformat()
                and window["end"] >= now.replace(hour=21, minute=0).isoformat()
                for window in mode_windows
            )
        )

    def test_high_soc_grid_topup_allowed_before_discharge_started(self):
        now = datetime(2026, 6, 25, 13, 30)
        slots = []
        for hour in range(14, 21):
            start = now.replace(hour=hour, minute=0, second=0, microsecond=0)
            price = 0.50 if hour == 20 else 0.14
            net_solar_kwh = 0.2 if hour < 17 else -0.8
            slots.append(
                {
                    "start": start,
                    "end": start + timedelta(hours=1),
                    "import_price": price,
                    "export_price": price,
                    "hours": 1.0,
                    "net_solar_kwh": net_solar_kwh,
                    "demand_kwh": max(0.0, -net_solar_kwh),
                    "solar_kwh": max(0.0, net_solar_kwh),
                }
            )

        coordinator = SmartEnergyPlannerCoordinator.__new__(SmartEnergyPlannerCoordinator)
        coordinator.config_entry = types.SimpleNamespace(data={}, options={})
        coordinator._active_charge_phase_end = None
        coordinator._active_charge_phase_mode = "accu_uit"
        coordinator._discharge_session_started = False

        _, grid_windows = SmartEnergyPlannerCoordinator._plan_charge_windows_for_horizon(
            coordinator,
            slots=slots,
            now=now,
            usable_capacity_kwh=8.0,
            current_remaining_capacity_kwh=1.9,
            max_charge_kw=1.0,
            max_discharge_kw=3.0,
            battery_min_profit=0.08,
        )

        self.assertTrue(grid_windows)

    def test_full_battery_ends_active_charge_but_keeps_future_plan(self):
        now = datetime(2026, 6, 25, 15, 45)
        slots = []
        day = now.replace(hour=0, minute=0, second=0, microsecond=0)
        for hour in range(16, 24):
            start = day + timedelta(hours=hour)
            slots.append(
                {
                    "start": start,
                    "end": start + timedelta(hours=1),
                    "import_price": 0.50 if 20 <= hour < 22 else 0.24,
                    "export_price": 0.50 if 20 <= hour < 22 else 0.24,
                    "hours": 1.0,
                    "net_solar_kwh": 0.4 if hour in (16, 17) else -0.6,
                    "demand_kwh": 0.0 if hour in (16, 17) else 0.6,
                    "solar_kwh": 1.0 if hour in (16, 17) else 0.0,
                }
            )
        for hour in range(10, 16):
            start = now.replace(day=26, hour=hour, minute=0, second=0, microsecond=0)
            slots.append(
                {
                    "start": start,
                    "end": start + timedelta(hours=1),
                    "import_price": 0.18,
                    "export_price": 0.18,
                    "hours": 1.0,
                    "net_solar_kwh": 1.0,
                    "demand_kwh": 0.0,
                    "solar_kwh": 1.0,
                }
            )
        slots.sort(key=lambda slot: slot["start"])

        coordinator = SmartEnergyPlannerCoordinator.__new__(SmartEnergyPlannerCoordinator)
        coordinator.config_entry = types.SimpleNamespace(data={}, options={})
        coordinator._active_charge_phase_end = None
        coordinator._active_charge_phase_mode = "accu_uit"
        coordinator._discharge_session_started = False

        solar_windows, grid_windows = SmartEnergyPlannerCoordinator._plan_charge_windows_for_horizon(
            coordinator,
            slots=slots,
            now=now,
            usable_capacity_kwh=8.0,
            current_remaining_capacity_kwh=0.0,
            max_charge_kw=1.0,
            max_discharge_kw=3.0,
            battery_min_profit=0.08,
            charge_safety_margin=0.5,
        )

        self.assertTrue(solar_windows)
        self.assertEqual(grid_windows, [])

        mode_windows, current_mode = SmartEnergyPlannerCoordinator._build_mode_windows_from_hourly_plan(
            coordinator,
            slots=slots,
            now=now,
            planned_solar_charge_windows=[
                {
                    "start": now.replace(hour=15, minute=0).isoformat(),
                    "end": now.replace(hour=19, minute=45).isoformat(),
                    "price": 0.30,
                    "usable_hours": 4.75,
                },
                *solar_windows,
            ],
            planned_grid_charge_windows=grid_windows,
            initial_usable_energy_kwh=8.0,
            usable_capacity_kwh=8.0,
            battery_soc_percent=100.0,
            average_price=0.30,
            average_export_price=0.30,
            max_charge_kw=1.0,
            max_discharge_kw=3.0,
        )

        self.assertNotIn(
            ("laden_met_zonne_energie", now.replace(hour=15, minute=0).isoformat()),
            {(window["mode"], window["start"]) for window in mode_windows},
        )
        self.assertTrue(
            any(window["mode"] in ("ontladen", "ontladen_naar_net") for window in mode_windows)
        )
        self.assertNotEqual(current_mode, "laden_met_zonne_energie")

    def test_full_battery_keeps_later_charge_after_expected_depletion(self):
        now = datetime(2026, 6, 25, 15, 45)
        slots = []
        day = now.replace(hour=0, minute=0, second=0, microsecond=0)
        for offset in range(18):
            start = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=offset + 1)
            slots.append(
                {
                    "start": start,
                    "end": start + timedelta(hours=1),
                    "import_price": 0.45 if offset < 6 else 0.30,
                    "export_price": 0.45 if offset < 6 else 0.30,
                    "hours": 1.0,
                    "net_solar_kwh": -0.7,
                    "demand_kwh": 0.7,
                    "solar_kwh": 0.0,
                }
            )
        for hour in range(10, 16):
            start = day + timedelta(days=1, hours=hour)
            slots.append(
                {
                    "start": start,
                    "end": start + timedelta(hours=1),
                    "import_price": 0.18,
                    "export_price": 0.18,
                    "hours": 1.0,
                    "net_solar_kwh": 1.0,
                    "demand_kwh": 0.0,
                    "solar_kwh": 1.0,
                }
            )
        slots.sort(key=lambda slot: slot["start"])

        coordinator = SmartEnergyPlannerCoordinator.__new__(SmartEnergyPlannerCoordinator)
        coordinator.config_entry = types.SimpleNamespace(data={}, options={})
        coordinator._active_charge_phase_end = None
        coordinator._active_charge_phase_mode = "accu_uit"
        coordinator._discharge_session_started = False

        solar_windows, grid_windows = SmartEnergyPlannerCoordinator._plan_charge_windows_for_horizon(
            coordinator,
            slots=slots,
            now=now,
            usable_capacity_kwh=8.0,
            current_remaining_capacity_kwh=0.0,
            max_charge_kw=1.0,
            max_discharge_kw=3.0,
            battery_min_profit=0.08,
            charge_safety_margin=0.0,
        )

        self.assertTrue(solar_windows)
        self.assertGreaterEqual(datetime.fromisoformat(solar_windows[0]["start"]), day + timedelta(days=1, hours=10))

    def test_discharge_window_turns_off_cheap_hours_when_energy_is_short(self):
        now = datetime(2026, 6, 25, 18, 0)
        day = now.replace(hour=0, minute=0, second=0, microsecond=0)
        slots = []
        for hour, price in ((18, 0.20), (19, 0.22), (20, 0.50), (21, 0.48), (22, 0.18)):
            start = day + timedelta(hours=hour)
            slots.append(
                {
                    "start": start,
                    "end": start + timedelta(hours=1),
                    "import_price": price,
                    "export_price": price,
                    "hours": 1.0,
                    "net_solar_kwh": 2.0 if hour == 22 else -1.0,
                    "demand_kwh": 0.0 if hour == 22 else 1.0,
                    "solar_kwh": 2.0 if hour == 22 else 0.0,
                }
            )

        coordinator = SmartEnergyPlannerCoordinator.__new__(SmartEnergyPlannerCoordinator)
        coordinator.config_entry = types.SimpleNamespace(data={}, options={})
        coordinator._active_charge_phase_end = None
        coordinator._active_charge_phase_mode = "accu_uit"
        coordinator._discharge_session_started = True

        mode_windows, _ = SmartEnergyPlannerCoordinator._build_mode_windows_from_hourly_plan(
            coordinator,
            slots=slots,
            now=now,
            planned_solar_charge_windows=[
                {
                    "start": (day + timedelta(hours=22)).isoformat(),
                    "end": (day + timedelta(hours=23)).isoformat(),
                    "price": 0.08,
                    "usable_hours": 1.0,
                    "charge_kwh": 2.0,
                }
            ],
            planned_grid_charge_windows=[],
            initial_usable_energy_kwh=2.0,
            usable_capacity_kwh=8.0,
            battery_soc_percent=25.0,
            average_price=0.30,
            average_export_price=0.30,
            max_charge_kw=2.0,
            max_discharge_kw=1.0,
        )
        def _mode_at(hour: int) -> str:
            moment = day + timedelta(hours=hour)
            return next(
                str(window["mode"])
                for window in mode_windows
                if datetime.fromisoformat(str(window["start"])) <= moment < datetime.fromisoformat(str(window["end"]))
            )

        self.assertEqual(_mode_at(18), BATTERY_MODE_OFF)
        self.assertEqual(_mode_at(19), BATTERY_MODE_OFF)
        self.assertEqual(_mode_at(20), "ontladen")
        self.assertEqual(_mode_at(21), "ontladen")

    def test_discharge_uses_net_demand_after_solar_before_charge_window(self):
        now = datetime(2026, 6, 25, 10, 0)
        day = now.replace(hour=0, minute=0, second=0, microsecond=0)
        slots = []
        for hour in range(10, 22):
            start = day + timedelta(hours=hour)
            daytime_solar_covers_home = 10 <= hour < 16
            evening_peak = hour in (18, 19, 20, 21)
            slots.append(
                {
                    "start": start,
                    "end": start + timedelta(hours=1),
                    "import_price": 0.55 if evening_peak else 0.22,
                    "export_price": 0.55 if evening_peak else 0.22,
                    "hours": 1.0,
                    "net_solar_kwh": 1.0 if daytime_solar_covers_home else -1.0,
                    "demand_kwh": 1.0,
                    "solar_kwh": 2.0 if daytime_solar_covers_home else 0.0,
                }
            )
        charge_start = day + timedelta(hours=22)
        slots.append(
            {
                "start": charge_start,
                "end": charge_start + timedelta(hours=1),
                "import_price": 0.10,
                "export_price": 0.10,
                "hours": 1.0,
                "net_solar_kwh": 2.0,
                "demand_kwh": 0.0,
                "solar_kwh": 2.0,
            }
        )

        coordinator = SmartEnergyPlannerCoordinator.__new__(SmartEnergyPlannerCoordinator)
        coordinator.config_entry = types.SimpleNamespace(data={}, options={})
        coordinator._active_charge_phase_end = None
        coordinator._active_charge_phase_mode = "accu_uit"
        coordinator._discharge_session_started = True

        mode_windows, _ = SmartEnergyPlannerCoordinator._build_mode_windows_from_hourly_plan(
            coordinator,
            slots=slots,
            now=now,
            planned_solar_charge_windows=[
                {
                    "start": charge_start.isoformat(),
                    "end": (charge_start + timedelta(hours=1)).isoformat(),
                    "price": 0.10,
                    "usable_hours": 1.0,
                    "charge_kwh": 2.0,
                }
            ],
            planned_grid_charge_windows=[],
            initial_usable_energy_kwh=4.0,
            usable_capacity_kwh=8.0,
            battery_soc_percent=50.0,
            average_price=0.30,
            average_export_price=0.30,
            max_charge_kw=2.0,
            max_discharge_kw=1.0,
        )

        def _mode_at(hour: int) -> str:
            moment = day + timedelta(hours=hour)
            return next(
                str(window["mode"])
                for window in mode_windows
                if datetime.fromisoformat(str(window["start"])) <= moment < datetime.fromisoformat(str(window["end"]))
            )

        self.assertEqual(_mode_at(10), BATTERY_MODE_OFF)
        self.assertEqual(_mode_at(15), BATTERY_MODE_OFF)
        self.assertEqual(_mode_at(18), "ontladen")
        self.assertEqual(_mode_at(21), "ontladen")

    def test_safety_margin_does_not_create_high_soc_grid_charge(self):
        now = datetime(2026, 6, 25, 18, 15)
        slots = []
        day = now.replace(hour=0, minute=0, second=0, microsecond=0)
        for hour in range(18, 24):
            start = day + timedelta(hours=hour)
            slots.append(
                {
                    "start": start,
                    "end": start + timedelta(hours=1),
                    "import_price": 0.49 if hour in (20, 21) else 0.38,
                    "export_price": 0.49 if hour in (20, 21) else 0.38,
                    "hours": 1.0,
                    "net_solar_kwh": -0.5,
                    "demand_kwh": 0.5,
                    "solar_kwh": 0.0,
                }
            )
        for hour in range(10, 16):
            start = day + timedelta(days=1, hours=hour)
            slots.append(
                {
                    "start": start,
                    "end": start + timedelta(hours=1),
                    "import_price": 0.14 if hour == 13 else 0.22,
                    "export_price": 0.14 if hour == 13 else 0.22,
                    "hours": 1.0,
                    "net_solar_kwh": 3.0,
                    "demand_kwh": 0.0,
                    "solar_kwh": 3.0,
                }
            )

        coordinator = SmartEnergyPlannerCoordinator.__new__(SmartEnergyPlannerCoordinator)
        coordinator.config_entry = types.SimpleNamespace(data={}, options={})
        coordinator._active_charge_phase_end = None
        coordinator._active_charge_phase_mode = "accu_uit"
        coordinator._discharge_session_started = False

        solar_windows, grid_windows = SmartEnergyPlannerCoordinator._plan_charge_windows_for_horizon(
            coordinator,
            slots=slots,
            now=now,
            usable_capacity_kwh=8.0,
            current_remaining_capacity_kwh=0.2,
            max_charge_kw=3.0,
            max_discharge_kw=3.0,
            battery_min_profit=0.08,
            charge_safety_margin=0.5,
        )

        today_end = day + timedelta(days=1)
        self.assertTrue(solar_windows)
        self.assertFalse(any(datetime.fromisoformat(window["start"]) < today_end for window in grid_windows))

    def test_charge_planning_ignores_valley_without_minimum_profit(self):
        now = datetime(2026, 6, 25, 8, 0)
        slots = []
        day = now.replace(hour=0, minute=0, second=0, microsecond=0)
        for hour in range(9, 18):
            start = day + timedelta(hours=hour)
            is_solar_valley = hour == 11
            slots.append(
                {
                    "start": start,
                    "end": start + timedelta(hours=1),
                    "import_price": 0.30 if is_solar_valley else 0.27,
                    "export_price": 0.25 if is_solar_valley else 0.27,
                    "hours": 1.0,
                    "net_solar_kwh": 2.0 if is_solar_valley else -0.4,
                    "demand_kwh": 0.0 if is_solar_valley else 0.4,
                    "solar_kwh": 2.0 if is_solar_valley else 0.0,
                }
            )

        coordinator = SmartEnergyPlannerCoordinator.__new__(SmartEnergyPlannerCoordinator)
        coordinator.config_entry = types.SimpleNamespace(data={}, options={})
        coordinator._active_charge_phase_end = None
        coordinator._active_charge_phase_mode = "accu_uit"
        coordinator._discharge_session_started = False

        solar_windows, grid_windows = SmartEnergyPlannerCoordinator._plan_charge_windows_for_horizon(
            coordinator,
            slots=slots,
            now=now,
            usable_capacity_kwh=2.0,
            current_remaining_capacity_kwh=2.0,
            max_charge_kw=2.0,
            max_discharge_kw=2.0,
            battery_min_profit=0.08,
        )

        self.assertEqual(solar_windows, [])
        self.assertEqual(grid_windows, [])

    def test_charge_planning_locks_valley_before_next_cycle(self):
        now = datetime(2026, 6, 25, 8, 0)
        slots = []
        day = now.replace(hour=0, minute=0, second=0, microsecond=0)
        for hour in range(9, 18):
            start = day + timedelta(hours=hour)
            is_solar_valley = hour in (10, 11, 13, 14, 15)
            slots.append(
                {
                    "start": start,
                    "end": start + timedelta(hours=1),
                    "import_price": 0.50 if hour >= 16 else 0.24,
                    "export_price": 0.04 if is_solar_valley else 0.24,
                    "hours": 1.0,
                    "net_solar_kwh": 2.0 if is_solar_valley else -0.5,
                    "demand_kwh": 0.0 if is_solar_valley else 0.5,
                    "solar_kwh": 2.0 if is_solar_valley else 0.0,
                }
            )

        coordinator = SmartEnergyPlannerCoordinator.__new__(SmartEnergyPlannerCoordinator)
        coordinator.config_entry = types.SimpleNamespace(data={}, options={})
        coordinator._active_charge_phase_end = None
        coordinator._active_charge_phase_mode = "accu_uit"
        coordinator._discharge_session_started = False

        solar_windows, _ = SmartEnergyPlannerCoordinator._plan_charge_windows_for_horizon(
            coordinator,
            slots=slots,
            now=now,
            usable_capacity_kwh=4.0,
            current_remaining_capacity_kwh=4.0,
            max_charge_kw=2.0,
            max_discharge_kw=2.0,
            battery_min_profit=0.08,
        )

        self.assertEqual(solar_windows[0]["start"], (day + timedelta(hours=10)).isoformat())
        self.assertEqual(solar_windows[0]["end"], (day + timedelta(hours=12)).isoformat())
        self.assertFalse(any(window["start"] == (day + timedelta(hours=13)).isoformat() for window in solar_windows))

    def test_charge_planning_skips_grid_topup_when_solar_can_fill_battery(self):
        now = datetime(2026, 6, 26, 7, 30)
        slots = []
        day = now.replace(hour=0, minute=0, second=0, microsecond=0)
        for hour in range(8, 18):
            start = day + timedelta(hours=hour)
            has_solar = 8 <= hour < 16
            slots.append(
                {
                    "start": start,
                    "end": start + timedelta(hours=1),
                    "import_price": 0.319,
                    "export_price": 0.319,
                    "hours": 1.0,
                    "net_solar_kwh": 2.0 if has_solar else -0.5,
                    "demand_kwh": 0.0 if has_solar else 0.5,
                    "solar_kwh": 2.0 if has_solar else 0.0,
                }
            )

        coordinator = SmartEnergyPlannerCoordinator.__new__(SmartEnergyPlannerCoordinator)
        coordinator.config_entry = types.SimpleNamespace(data={}, options={})
        coordinator._active_charge_phase_end = None
        coordinator._active_charge_phase_mode = "accu_uit"
        coordinator._discharge_session_started = False

        solar_windows, grid_windows = SmartEnergyPlannerCoordinator._plan_charge_windows_for_horizon(
            coordinator,
            slots=slots,
            now=now,
            usable_capacity_kwh=8.0,
            current_remaining_capacity_kwh=8.0,
            max_charge_kw=2.0,
            max_discharge_kw=2.0,
            battery_min_profit=0.08,
        )

        self.assertTrue(solar_windows)
        self.assertEqual(grid_windows, [])

    def test_small_grid_topup_keeps_actual_charge_kwh_in_mode_window(self):
        now = datetime(2026, 6, 25, 18, 15)
        slots = [
            {
                "start": now.replace(minute=0),
                "end": now.replace(hour=19, minute=0),
                "import_price": 0.34,
                "export_price": 0.34,
                "hours": 1.0,
                "net_solar_kwh": -0.8,
                "demand_kwh": 0.8,
                "solar_kwh": 0.0,
            },
            {
                "start": now.replace(hour=19, minute=0),
                "end": now.replace(hour=20, minute=0),
                "import_price": 0.50,
                "export_price": 0.50,
                "hours": 1.0,
                "net_solar_kwh": -0.5,
                "demand_kwh": 0.5,
                "solar_kwh": 0.0,
            },
        ]

        coordinator = SmartEnergyPlannerCoordinator.__new__(SmartEnergyPlannerCoordinator)
        coordinator._active_charge_phase_end = None
        coordinator._active_charge_phase_mode = "accu_uit"
        coordinator._discharge_session_started = False

        mode_windows, current_mode = SmartEnergyPlannerCoordinator._build_mode_windows_from_hourly_plan(
            coordinator,
            slots=slots,
            now=now,
            planned_solar_charge_windows=[],
            planned_grid_charge_windows=[
                {
                    "start": now.isoformat(),
                    "end": now.replace(hour=19, minute=45).isoformat(),
                    "price": 0.34,
                    "usable_hours": 1.5,
                    "charge_kwh": 0.2,
                }
            ],
            initial_usable_energy_kwh=7.8,
            usable_capacity_kwh=8.0,
            battery_soc_percent=98.0,
            average_price=0.30,
            average_export_price=0.30,
            max_charge_kw=3.0,
            max_discharge_kw=3.0,
        )

        self.assertEqual(current_mode, "laden_van_net")
        charge_window = next(window for window in mode_windows if window["mode"] == "laden_van_net")
        self.assertEqual(charge_window["usable_hours"], 0.067)
        charge_duration_seconds = (
            datetime.fromisoformat(charge_window["end"]) - datetime.fromisoformat(charge_window["start"])
        ).total_seconds()
        self.assertAlmostEqual(charge_duration_seconds, 240.0, delta=0.01)
        self.assertIn("ontladen", {window["mode"] for window in mode_windows})

    def test_full_battery_exports_surplus_to_be_empty_before_charge_window(self):
        now = datetime(2026, 6, 25, 12, 0)
        charge_start = now + timedelta(hours=6)
        slots = []
        for offset in range(6):
            start = now + timedelta(hours=offset)
            slots.append(
                {
                    "start": start,
                    "end": start + timedelta(hours=1),
                    "import_price": 0.45 if offset in (1, 2) else 0.30,
                    "export_price": 0.50 if offset in (1, 2) else 0.20,
                    "hours": 1.0,
                    "net_solar_kwh": -0.4,
                    "demand_kwh": 0.4,
                    "solar_kwh": 0.0,
                }
            )
        for offset in range(4):
            start = charge_start + timedelta(hours=offset)
            slots.append(
                {
                    "start": start,
                    "end": start + timedelta(hours=1),
                    "import_price": 0.18,
                    "export_price": 0.18,
                    "hours": 1.0,
                    "net_solar_kwh": 2.0,
                    "demand_kwh": 0.0,
                    "solar_kwh": 2.0,
                }
            )

        coordinator = SmartEnergyPlannerCoordinator.__new__(SmartEnergyPlannerCoordinator)
        coordinator._active_charge_phase_end = None
        coordinator._active_charge_phase_mode = "accu_uit"
        coordinator._discharge_session_started = False

        mode_windows, _ = SmartEnergyPlannerCoordinator._build_mode_windows_from_hourly_plan(
            coordinator,
            slots=slots,
            now=now,
            planned_solar_charge_windows=[
                {
                    "start": charge_start.isoformat(),
                    "end": (charge_start + timedelta(hours=4)).isoformat(),
                    "price": 0.18,
                    "usable_hours": 4.0,
                }
            ],
            planned_grid_charge_windows=[],
            initial_usable_energy_kwh=8.0,
            usable_capacity_kwh=8.0,
            battery_soc_percent=100.0,
            average_price=0.30,
            average_export_price=0.30,
            max_charge_kw=2.0,
            max_discharge_kw=3.0,
        )

        self.assertIn("ontladen_naar_net", {window["mode"] for window in mode_windows})
        self.assertIn(
            ("laden_met_zonne_energie", charge_start.isoformat()),
            {(window["mode"], window["start"]) for window in mode_windows},
        )

    def test_discharge_latch_blocks_charge_until_net_demand_reaches_threshold(self):
        now = datetime(2026, 6, 25, 12, 0)
        charge_start = now + timedelta(hours=6)
        slots = []
        for offset in range(6):
            start = now + timedelta(hours=offset)
            slots.append(
                {
                    "start": start,
                    "end": start + timedelta(hours=1),
                    "import_price": 0.45,
                    "export_price": 0.45,
                    "hours": 1.0,
                    "net_solar_kwh": -0.4,
                    "demand_kwh": 0.4,
                    "solar_kwh": 0.0,
                }
            )
        for offset in range(4):
            start = charge_start + timedelta(hours=offset)
            slots.append(
                {
                    "start": start,
                    "end": start + timedelta(hours=1),
                    "import_price": 0.18,
                    "export_price": 0.18,
                    "hours": 1.0,
                    "net_solar_kwh": 2.0,
                    "demand_kwh": 0.0,
                    "solar_kwh": 2.0,
                }
            )

        coordinator = SmartEnergyPlannerCoordinator.__new__(SmartEnergyPlannerCoordinator)
        coordinator._active_charge_phase_end = None
        coordinator._active_charge_phase_mode = "accu_uit"
        coordinator._discharge_session_started = True

        mode_windows, _ = SmartEnergyPlannerCoordinator._build_mode_windows_from_hourly_plan(
            coordinator,
            slots=slots,
            now=now,
            planned_solar_charge_windows=[
                {
                    "start": charge_start.isoformat(),
                    "end": (charge_start + timedelta(hours=4)).isoformat(),
                    "price": 0.18,
                    "usable_hours": 4.0,
                }
            ],
            planned_grid_charge_windows=[],
            initial_usable_energy_kwh=8.0,
            usable_capacity_kwh=8.0,
            battery_soc_percent=100.0,
            average_price=0.30,
            average_export_price=0.30,
            max_charge_kw=2.0,
            max_discharge_kw=3.0,
        )

        self.assertNotIn("ontladen_naar_net", {window["mode"] for window in mode_windows})
        self.assertNotIn(
            ("laden_met_zonne_energie", charge_start.isoformat()),
            {(window["mode"], window["start"]) for window in mode_windows},
        )
        self.assertEqual(mode_windows[-1]["mode"], BATTERY_MODE_OFF)

    def test_high_soc_grid_topup_blocked_after_discharge_started(self):
        now = datetime(2026, 6, 25, 13, 30)
        slots = []
        for day_offset in range(2):
            day = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=day_offset)
            for hour in range(24):
                start = day + timedelta(hours=hour)
                if start <= now:
                    continue
                price = 0.50 if hour in (20, 21) else (0.14 if hour in (14, 15) else 0.28)
                if day_offset == 0:
                    net_solar_kwh = 1.4 if hour == 14 else (0.2 if hour == 15 else -0.8)
                else:
                    net_solar_kwh = 1.0 if 11 <= hour < 16 else -0.4
                slots.append(
                    {
                        "start": start,
                        "end": start + timedelta(hours=1),
                        "import_price": price,
                        "export_price": price,
                        "hours": 1.0,
                        "net_solar_kwh": net_solar_kwh,
                        "demand_kwh": max(0.0, -net_solar_kwh),
                        "solar_kwh": max(0.0, net_solar_kwh),
                    }
                )

        coordinator = SmartEnergyPlannerCoordinator.__new__(SmartEnergyPlannerCoordinator)
        coordinator.config_entry = types.SimpleNamespace(data={}, options={})
        coordinator._active_charge_phase_end = None
        coordinator._active_charge_phase_mode = "accu_uit"
        coordinator._discharge_session_started = True

        solar_windows, grid_windows = SmartEnergyPlannerCoordinator._plan_charge_windows_for_horizon(
            coordinator,
            slots=slots,
            now=now,
            usable_capacity_kwh=8.0,
            current_remaining_capacity_kwh=1.9,
            max_charge_kw=1.0,
            max_discharge_kw=3.0,
            battery_min_profit=0.08,
        )

        today_end = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        self.assertFalse(any(datetime.fromisoformat(window["start"]) < today_end for window in grid_windows))
        self.assertTrue(any(datetime.fromisoformat(window["start"]) >= today_end for window in solar_windows))

        mode_windows, _ = SmartEnergyPlannerCoordinator._build_mode_windows_from_hourly_plan(
            coordinator,
            slots=slots,
            now=now,
            planned_solar_charge_windows=solar_windows,
            planned_grid_charge_windows=grid_windows,
            initial_usable_energy_kwh=6.1,
            usable_capacity_kwh=8.0,
            average_price=0.28,
            average_export_price=0.28,
            max_charge_kw=1.0,
            max_discharge_kw=3.0,
            battery_soc_percent=76.25,
        )

        self.assertFalse(
            any(
                window["mode"] == BATTERY_MODE_SOLAR_CHARGE
                and datetime.fromisoformat(window["start"]) < today_end
                and datetime.fromisoformat(window["end"]) > today_end
                for window in mode_windows
            )
        )
        self.assertTrue(
            any(
                window["mode"] == "ontladen"
                and datetime.fromisoformat(window["start"]).date() == now.date()
                for window in mode_windows
            )
        )

    def test_export_waits_when_battery_can_cover_own_demand_earlier(self):
        now = datetime(2026, 6, 25, 6, 0)
        slots = []
        for hour in range(6, 10):
            start = now.replace(hour=hour, minute=0, second=0, microsecond=0)
            demand_kwh = 0.6 if hour == 7 else 1.0
            solar_kwh = 0.66 if hour == 7 else 0.0
            slots.append(
                {
                    "start": start,
                    "end": start + timedelta(hours=1),
                    "import_price": 0.50 if hour == 8 else 0.30,
                    "export_price": 0.70 if hour == 7 else 0.20,
                    "hours": 1.0,
                    "net_solar_kwh": round(solar_kwh - demand_kwh, 3),
                    "demand_kwh": demand_kwh,
                    "solar_kwh": solar_kwh,
                }
            )

        coordinator = SmartEnergyPlannerCoordinator.__new__(SmartEnergyPlannerCoordinator)
        coordinator._active_charge_phase_end = None
        coordinator._active_charge_phase_mode = "accu_uit"
        coordinator._discharge_session_started = False

        mode_windows, _ = SmartEnergyPlannerCoordinator._build_mode_windows_from_hourly_plan(
            coordinator,
            slots=slots,
            now=now,
            planned_solar_charge_windows=[
                {
                    "start": now.replace(hour=10).isoformat(),
                    "end": now.replace(hour=11).isoformat(),
                    "price": 0.20,
                    "usable_hours": 1.0,
                }
            ],
            planned_grid_charge_windows=[],
            initial_usable_energy_kwh=2.0,
            usable_capacity_kwh=8.0,
            battery_soc_percent=25.0,
            average_price=0.30,
            average_export_price=0.30,
            max_charge_kw=1.0,
            max_discharge_kw=1.0,
        )

        self.assertNotIn("ontladen_naar_net", {window["mode"] for window in mode_windows})
        self.assertTrue(
            any(
                window["mode"] == "ontladen"
                and window["start"] <= now.replace(hour=8).isoformat()
                and window["end"] >= now.replace(hour=9).isoformat()
                for window in mode_windows
            )
        )

    def test_active_discharge_session_blocks_charge_until_soc_threshold(self):
        now = datetime(2026, 6, 22, 17, 45)
        slots = []
        for hour in range(17, 21):
            start = now.replace(hour=hour, minute=0)
            slots.append(
                {
                    "start": start,
                    "end": start + timedelta(hours=1),
                    "import_price": 0.20 if hour == 18 else 0.45,
                    "export_price": 0.20 if hour == 18 else 0.45,
                    "hours": 1.0,
                    "net_solar_kwh": -1.0,
                    "demand_kwh": 1.0,
                    "solar_kwh": 0.0,
                }
            )

        coordinator = SmartEnergyPlannerCoordinator.__new__(SmartEnergyPlannerCoordinator)
        coordinator._active_charge_phase_end = now + timedelta(hours=2)
        coordinator._active_charge_phase_mode = "laden_van_net"
        coordinator._discharge_session_started = True

        mode_windows, current_mode = SmartEnergyPlannerCoordinator._build_mode_windows_from_hourly_plan(
            coordinator,
            slots=slots,
            now=now,
            planned_solar_charge_windows=[],
            planned_grid_charge_windows=[
                {
                    "start": now.replace(hour=18, minute=0).isoformat(),
                    "end": now.replace(hour=19, minute=0).isoformat(),
                    "price": 0.20,
                    "usable_hours": 1.0,
                }
            ],
            initial_usable_energy_kwh=4.0,
            usable_capacity_kwh=8.0,
            battery_soc_percent=50.0,
            average_price=0.30,
            average_export_price=0.30,
            max_charge_kw=2.0,
            max_discharge_kw=2.0,
        )

        self.assertEqual(current_mode, "ontladen")
        self.assertTrue(coordinator._discharge_session_started)
        self.assertNotIn("laden_van_net", {window["mode"] for window in mode_windows})
        self.assertNotIn("laden_met_zonne_energie", {window["mode"] for window in mode_windows})

    def test_active_discharge_session_allows_charge_when_soc_is_below_threshold(self):
        now = datetime(2026, 6, 22, 22, 30)
        cycle_end = datetime(2026, 6, 23, 0, 0)
        slots = []
        for hour in (22, 23):
            start = now.replace(hour=hour, minute=0)
            slots.append(
                {
                    "start": start,
                    "end": start + timedelta(hours=1),
                    "import_price": 0.45,
                    "export_price": 0.45,
                    "hours": 1.0,
                    "net_solar_kwh": -1.0,
                    "demand_kwh": 1.0,
                    "solar_kwh": 0.0,
                }
            )
        for hour in (0, 1):
            start = cycle_end.replace(hour=hour)
            slots.append(
                {
                    "start": start,
                    "end": start + timedelta(hours=1),
                    "import_price": 0.12,
                    "export_price": 0.12,
                    "hours": 1.0,
                    "net_solar_kwh": -0.2,
                    "demand_kwh": 0.2,
                    "solar_kwh": 0.0,
                }
            )

        coordinator = SmartEnergyPlannerCoordinator.__new__(SmartEnergyPlannerCoordinator)
        coordinator._active_charge_phase_end = now + timedelta(minutes=30)
        coordinator._active_charge_phase_mode = "laden_van_net"
        coordinator._discharge_session_started = True

        mode_windows, current_mode = SmartEnergyPlannerCoordinator._build_mode_windows_from_hourly_plan(
            coordinator,
            slots=slots,
            now=now,
            planned_solar_charge_windows=[],
            planned_grid_charge_windows=[
                {
                    "start": cycle_end.isoformat(),
                    "end": (cycle_end + timedelta(hours=1)).isoformat(),
                    "price": 0.12,
                    "usable_hours": 1.0,
                }
            ],
            initial_usable_energy_kwh=4.0,
            usable_capacity_kwh=8.0,
            battery_soc_percent=29.0,
            average_price=0.30,
            average_export_price=0.30,
            max_charge_kw=2.0,
            max_discharge_kw=2.0,
        )

        self.assertEqual(current_mode, "laden_van_net")
        self.assertIn("laden_van_net", {window["mode"] for window in mode_windows})

    def test_discharge_session_allows_charge_after_soc_dropped_below_threshold(self):
        now = datetime(2026, 6, 24, 5, 30)
        cycle_end = datetime(2026, 6, 25, 0, 0)
        slots = []
        for hour in range(5, 22):
            start = now.replace(hour=hour, minute=0)
            slots.append(
                {
                    "start": start,
                    "end": start + timedelta(hours=1),
                    "import_price": 0.20 if 11 <= hour < 14 else (0.80 if hour >= 20 else 0.35),
                    "export_price": 0.20 if 11 <= hour < 14 else (0.80 if hour >= 20 else 0.35),
                    "hours": 1.0,
                    "net_solar_kwh": 1.0 if 11 <= hour < 14 else (-0.3 if hour >= 20 else 0.0),
                    "demand_kwh": 0.0 if hour < 20 else 0.3,
                    "solar_kwh": 1.0 if 11 <= hour < 14 else 0.0,
                }
            )

        coordinator = SmartEnergyPlannerCoordinator.__new__(SmartEnergyPlannerCoordinator)
        coordinator._active_charge_phase_end = None
        coordinator._active_charge_phase_mode = "accu_uit"
        coordinator._discharge_session_started = True

        mode_windows, current_mode = SmartEnergyPlannerCoordinator._build_mode_windows_from_hourly_plan(
            coordinator,
            slots=slots,
            now=now,
            planned_solar_charge_windows=[
                {
                    "start": now.replace(hour=11, minute=0).isoformat(),
                    "end": now.replace(hour=14, minute=0).isoformat(),
                    "price": 0.20,
                    "usable_hours": 3.0,
                }
            ],
            planned_grid_charge_windows=[
                {
                    "start": now.replace(hour=10, minute=0).isoformat(),
                    "end": now.replace(hour=11, minute=0).isoformat(),
                    "price": 0.20,
                    "usable_hours": 1.0,
                }
            ],
            initial_usable_energy_kwh=1.7,
            usable_capacity_kwh=8.0,
            battery_soc_percent=29.0,
            average_price=0.40,
            average_export_price=0.40,
            max_charge_kw=1.0,
            max_discharge_kw=3.0,
        )

        self.assertIn(current_mode, ("ontladen", "ontladen_naar_net"))
        self.assertIn(
            ("laden_met_zonne_energie", now.replace(hour=11, minute=0).isoformat()),
            {(window["mode"], window["start"]) for window in mode_windows},
        )
        self.assertIn(
            ("laden_van_net", now.replace(hour=10, minute=0).isoformat()),
            {(window["mode"], window["start"]) for window in mode_windows},
        )

    def test_discharge_session_blocks_grid_charge_until_net_demand_reaches_threshold(self):
        now = datetime(2026, 6, 24, 20, 0)
        cycle_end = datetime(2026, 6, 25, 0, 0)
        slots = []
        for hour in range(20, 24):
            start = now.replace(hour=hour, minute=0)
            slots.append(
                {
                    "start": start,
                    "end": start + timedelta(hours=1),
                    "import_price": 0.20 if hour == 22 else 0.80,
                    "export_price": 0.20 if hour == 22 else 0.80,
                    "hours": 1.0,
                    "net_solar_kwh": -0.3,
                    "demand_kwh": 0.3,
                    "solar_kwh": 0.0,
                }
            )

        coordinator = SmartEnergyPlannerCoordinator.__new__(SmartEnergyPlannerCoordinator)
        coordinator._active_charge_phase_end = None
        coordinator._active_charge_phase_mode = "accu_uit"
        coordinator._discharge_session_started = True

        mode_windows, current_mode = SmartEnergyPlannerCoordinator._build_mode_windows_from_hourly_plan(
            coordinator,
            slots=slots,
            now=now,
            planned_solar_charge_windows=[],
            planned_grid_charge_windows=[
                {
                    "start": now.replace(hour=22, minute=0).isoformat(),
                    "end": now.replace(hour=23, minute=0).isoformat(),
                    "price": 0.20,
                    "usable_hours": 1.0,
                }
            ],
            initial_usable_energy_kwh=3.7,
            usable_capacity_kwh=8.0,
            battery_soc_percent=37.0,
            average_price=0.40,
            average_export_price=0.40,
            max_charge_kw=1.0,
            max_discharge_kw=3.0,
        )

        self.assertIn(current_mode, ("ontladen", "ontladen_naar_net"))
        self.assertNotIn("laden_van_net", {window["mode"] for window in mode_windows})
        self.assertTrue(
            any(
                window["mode"] in ("ontladen", "ontladen_naar_net")
                and window["start"] < now.replace(hour=22, minute=0).isoformat()
                for window in mode_windows
            )
        )

    def test_battery_drains_before_next_charge_window(self):
        now = datetime(2026, 6, 24, 5, 45)
        slots = []
        for hour in range(5, 15):
            start = now.replace(hour=hour, minute=0)
            slots.append(
                {
                    "start": start,
                    "end": start + timedelta(hours=1),
                    "import_price": 0.20 if 11 <= hour < 14 else 0.35,
                    "export_price": 0.20 if 11 <= hour < 14 else 0.35,
                    "hours": 1.0,
                    "net_solar_kwh": 1.0 if 11 <= hour < 14 else -0.5,
                    "demand_kwh": 0.0 if 11 <= hour < 14 else 0.5,
                    "solar_kwh": 1.0 if 11 <= hour < 14 else 0.0,
                }
            )

        coordinator = SmartEnergyPlannerCoordinator.__new__(SmartEnergyPlannerCoordinator)
        coordinator._active_charge_phase_end = None
        coordinator._active_charge_phase_mode = "accu_uit"
        coordinator._discharge_session_started = False

        mode_windows, current_mode = SmartEnergyPlannerCoordinator._build_mode_windows_from_hourly_plan(
            coordinator,
            slots=slots,
            now=now,
            planned_solar_charge_windows=[
                {
                    "start": now.replace(hour=11, minute=0).isoformat(),
                    "end": now.replace(hour=14, minute=0).isoformat(),
                    "price": 0.20,
                    "usable_hours": 3.0,
                }
            ],
            planned_grid_charge_windows=[],
            initial_usable_energy_kwh=1.7,
            usable_capacity_kwh=8.0,
            battery_soc_percent=37.0,
            average_price=0.30,
            average_export_price=0.30,
            max_charge_kw=1.0,
            max_discharge_kw=3.0,
        )

        self.assertIn(current_mode, ("ontladen", "ontladen_naar_net"))
        self.assertTrue(
            any(
                window["mode"] in ("ontladen", "ontladen_naar_net")
                and window["end"] <= now.replace(hour=11, minute=0).isoformat()
                for window in mode_windows
            )
        )
        self.assertIn(
            ("laden_met_zonne_energie", now.replace(hour=11, minute=0).isoformat()),
            {(window["mode"], window["start"]) for window in mode_windows},
        )

    def test_discharge_latch_keeps_charge_window_after_predrain(self):
        now = datetime(2026, 6, 24, 5, 45)
        slots = []
        for hour in range(5, 15):
            start = now.replace(hour=hour, minute=0)
            slots.append(
                {
                    "start": start,
                    "end": start + timedelta(hours=1),
                    "import_price": 0.20 if 11 <= hour < 14 else 0.35,
                    "export_price": 0.20 if 11 <= hour < 14 else 0.35,
                    "hours": 1.0,
                    "net_solar_kwh": 1.0 if 11 <= hour < 14 else -0.5,
                    "demand_kwh": 0.0 if 11 <= hour < 14 else 0.5,
                    "solar_kwh": 1.0 if 11 <= hour < 14 else 0.0,
                }
            )

        coordinator = SmartEnergyPlannerCoordinator.__new__(SmartEnergyPlannerCoordinator)
        coordinator._active_charge_phase_end = None
        coordinator._active_charge_phase_mode = "accu_uit"
        coordinator._discharge_session_started = True

        mode_windows, current_mode = SmartEnergyPlannerCoordinator._build_mode_windows_from_hourly_plan(
            coordinator,
            slots=slots,
            now=now,
            planned_solar_charge_windows=[
                {
                    "start": now.replace(hour=11, minute=0).isoformat(),
                    "end": now.replace(hour=14, minute=0).isoformat(),
                    "price": 0.20,
                    "usable_hours": 3.0,
                }
            ],
            planned_grid_charge_windows=[],
            initial_usable_energy_kwh=1.7,
            usable_capacity_kwh=8.0,
            battery_soc_percent=37.0,
            average_price=0.30,
            average_export_price=0.30,
            max_charge_kw=1.0,
            max_discharge_kw=3.0,
        )

        self.assertIn(current_mode, ("ontladen", "ontladen_naar_net"))
        self.assertIn("laden_met_zonne_energie", {window["mode"] for window in mode_windows})

    def test_charge_windows_less_than_three_hours_apart_form_one_phase(self):
        now = datetime(2026, 6, 24, 9, 0)
        slots = []
        for hour in range(9, 15):
            start = now.replace(hour=hour)
            slots.append(
                {
                    "start": start,
                    "end": start + timedelta(hours=1),
                    "import_price": 0.20 if hour in (10, 13) else 0.35,
                    "export_price": 0.20 if hour in (10, 13) else 0.35,
                    "hours": 1.0,
                    "net_solar_kwh": 1.0 if hour in (10, 13) else 0.0,
                    "demand_kwh": 0.0,
                    "solar_kwh": 1.0 if hour in (10, 13) else 0.0,
                }
            )

        coordinator = SmartEnergyPlannerCoordinator.__new__(SmartEnergyPlannerCoordinator)
        coordinator._active_charge_phase_end = None
        coordinator._active_charge_phase_mode = "accu_uit"
        coordinator._discharge_session_started = False

        mode_windows, _current_mode = SmartEnergyPlannerCoordinator._build_mode_windows_from_hourly_plan(
            coordinator,
            slots=slots,
            now=now,
            planned_solar_charge_windows=[
                {
                    "start": now.replace(hour=10).isoformat(),
                    "end": now.replace(hour=11).isoformat(),
                    "price": 0.20,
                    "usable_hours": 1.0,
                },
                {
                    "start": now.replace(hour=13).isoformat(),
                    "end": now.replace(hour=14).isoformat(),
                    "price": 0.20,
                    "usable_hours": 1.0,
                },
            ],
            planned_grid_charge_windows=[],
            initial_usable_energy_kwh=0.0,
            usable_capacity_kwh=8.0,
            battery_soc_percent=20.0,
            average_price=0.30,
            average_export_price=0.30,
            max_charge_kw=1.0,
            max_discharge_kw=3.0,
        )

        self.assertTrue(
            any(
                window["mode"] == "laden_met_zonne_energie"
                and window["start"] <= now.replace(hour=10).isoformat()
                and window["end"] >= now.replace(hour=14).isoformat()
                for window in mode_windows
            )
        )

