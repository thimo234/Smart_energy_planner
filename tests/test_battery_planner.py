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
            battery_min_profit=0.08,
        )

        self.assertEqual(grid_windows, [])
        self.assertEqual(len(solar_windows), 1)
        self.assertEqual(solar_windows[0]["start"], now.replace(hour=11, minute=0).isoformat())
        self.assertEqual(solar_windows[0]["end"], now.replace(hour=13, minute=0).isoformat())

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
            battery_min_profit=0.08,
        )

        self.assertTrue(grid_windows)

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
            battery_min_profit=0.08,
        )

        today_end = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        self.assertFalse(any(datetime.fromisoformat(window["start"]) < today_end for window in grid_windows))

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

    def test_discharge_session_blocks_charge_after_simulated_drain_above_soc_threshold(self):
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

    def test_discharge_latch_blocks_charge_window_after_predrain_above_soc_threshold(self):
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
        self.assertNotIn("laden_met_zonne_energie", {window["mode"] for window in mode_windows})

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

