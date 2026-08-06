import sys
import types
import unittest
from datetime import datetime

from test_support import install_package_stub

install_package_stub()


def _install_homeassistant_stubs() -> None:
    sys.modules.setdefault("homeassistant", types.ModuleType("homeassistant"))
    sys.modules.setdefault("homeassistant.components", types.ModuleType("homeassistant.components"))
    sensor = sys.modules.setdefault(
        "homeassistant.components.sensor",
        types.ModuleType("homeassistant.components.sensor"),
    )
    sensor.SensorDeviceClass = types.SimpleNamespace(TIMESTAMP="timestamp", MONETARY="monetary")
    sensor.SensorEntity = object

    config_entries = sys.modules.setdefault(
        "homeassistant.config_entries",
        types.ModuleType("homeassistant.config_entries"),
    )
    config_entries.ConfigEntry = object

    const = sys.modules.setdefault("homeassistant.const", types.ModuleType("homeassistant.const"))
    const.UnitOfEnergy = types.SimpleNamespace(KILO_WATT_HOUR="kWh")

    core = sys.modules.setdefault("homeassistant.core", types.ModuleType("homeassistant.core"))
    core.HomeAssistant = object
    core.callback = lambda function: function

    sys.modules.setdefault("homeassistant.helpers", types.ModuleType("homeassistant.helpers"))
    entity_platform = sys.modules.setdefault(
        "homeassistant.helpers.entity_platform",
        types.ModuleType("homeassistant.helpers.entity_platform"),
    )
    entity_platform.AddEntitiesCallback = object
    update_coordinator = sys.modules.setdefault(
        "homeassistant.helpers.update_coordinator",
        types.ModuleType("homeassistant.helpers.update_coordinator"),
    )

    class CoordinatorEntity:
        def __init__(self, coordinator):
            self.coordinator = coordinator

        @classmethod
        def __class_getitem__(cls, item):
            return cls

    update_coordinator.CoordinatorEntity = CoordinatorEntity

    util = sys.modules.setdefault("homeassistant.util", types.ModuleType("homeassistant.util"))
    dt_module = sys.modules.setdefault("homeassistant.util.dt", types.ModuleType("homeassistant.util.dt"))
    util.dt = dt_module

    coordinator_module = types.ModuleType("custom_components.smart_energy_planner.coordinator")
    coordinator_module.SmartEnergyPlannerCoordinator = object
    sys.modules["custom_components.smart_energy_planner.coordinator"] = coordinator_module


_install_homeassistant_stubs()

from custom_components.smart_energy_planner.sensor import CheapestPriceWindowStartSensor


class CheapestPriceWindowStartSensorTest(unittest.TestCase):
    def test_exposes_cheapest_block_start_as_timestamp(self):
        start = "2026-08-07T02:00:00+02:00"
        data = types.SimpleNamespace(
            cheapest_price_window={
                "start": start,
                "end": "2026-08-07T05:00:00+02:00",
                "average_price": 0.12,
                "duration_hours": 3.0,
            },
            planner_kind="price_window",
            source_status={},
            source_errors=[],
        )
        coordinator = types.SimpleNamespace(data=data)
        entry = types.SimpleNamespace(entry_id="entry-1", title="Energy Prices")

        entity = CheapestPriceWindowStartSensor(coordinator, entry)

        self.assertEqual(entity.native_value, datetime.fromisoformat(start))
        self.assertEqual(entity._attr_device_class, "timestamp")
        self.assertEqual(entity.extra_state_attributes["end"], "2026-08-07T05:00:00+02:00")


if __name__ == "__main__":
    unittest.main()
