import asyncio
import sys
import types
import unittest
from enum import IntFlag, StrEnum

from test_support import install_package_stub

install_package_stub()


class HVACMode(StrEnum):
    OFF = "off"
    HEAT = "heat"
    COOL = "cool"
    AUTO = "auto"


class HVACAction(StrEnum):
    OFF = "off"
    IDLE = "idle"
    HEATING = "heating"
    COOLING = "cooling"


class ClimateEntityFeature(IntFlag):
    TARGET_TEMPERATURE = 1
    TURN_ON = 2
    TURN_OFF = 4
    PRESET_MODE = 8


def _install_homeassistant_stubs() -> None:
    homeassistant = sys.modules.setdefault("homeassistant", types.ModuleType("homeassistant"))

    components = sys.modules.setdefault("homeassistant.components", types.ModuleType("homeassistant.components"))
    climate = sys.modules.setdefault("homeassistant.components.climate", types.ModuleType("homeassistant.components.climate"))
    climate_const = sys.modules.setdefault(
        "homeassistant.components.climate.const",
        types.ModuleType("homeassistant.components.climate.const"),
    )
    climate.ClimateEntity = object
    climate_const.ClimateEntityFeature = ClimateEntityFeature
    climate_const.HVACAction = HVACAction
    climate_const.HVACMode = HVACMode

    config_entries = sys.modules.setdefault(
        "homeassistant.config_entries",
        types.ModuleType("homeassistant.config_entries"),
    )
    config_entries.ConfigEntry = object

    const = sys.modules.setdefault("homeassistant.const", types.ModuleType("homeassistant.const"))
    const.UnitOfTemperature = types.SimpleNamespace(CELSIUS="C")

    core = sys.modules.setdefault("homeassistant.core", types.ModuleType("homeassistant.core"))
    core.HomeAssistant = object

    helpers = sys.modules.setdefault("homeassistant.helpers", types.ModuleType("homeassistant.helpers"))
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

        def async_write_ha_state(self):
            pass

    update_coordinator.CoordinatorEntity = CoordinatorEntity
    helpers.entity_platform = entity_platform
    helpers.update_coordinator = update_coordinator

    init_module = types.ModuleType("custom_components.smart_energy_planner.__init__")

    async def _async_call_turn_service(*args, **kwargs):
        return None

    async def _async_save_runtime_state(*args, **kwargs):
        return None

    init_module._async_call_turn_service = _async_call_turn_service
    init_module._async_save_runtime_state = _async_save_runtime_state
    sys.modules["custom_components.smart_energy_planner.__init__"] = init_module

    coordinator_module = types.ModuleType("custom_components.smart_energy_planner.coordinator")
    coordinator_module.SmartEnergyPlannerCoordinator = object
    sys.modules["custom_components.smart_energy_planner.coordinator"] = coordinator_module

    homeassistant.components = components
    homeassistant.config_entries = config_entries
    homeassistant.const = const
    homeassistant.core = core
    homeassistant.helpers = helpers


_install_homeassistant_stubs()
from custom_components.smart_energy_planner.climate import PlannerThermostatEntity
from custom_components.smart_energy_planner.const import CONF_COOLING_MODE_SWITCH_ENTITY, RUNTIME_STATE


class _States:
    def __init__(self, states):
        self._states = states

    def get(self, entity_id):
        state = self._states.get(entity_id)
        if state is None:
            return None
        return types.SimpleNamespace(state=state, attributes={})


class PlannerThermostatEntityTest(unittest.TestCase):
    def _entity(self, cooling_state):
        coordinator = types.SimpleNamespace(
            data=types.SimpleNamespace(
                room_temperature_c=22.0,
                thermostat_setpoint_c=20.0,
                thermostat_cool_setpoint_c=24.0,
                thermostat_preheat_setpoint_c=21.0,
                thermostat_eco_setpoint_c=18.0,
                heat_pump_strategy="normal",
            )
        )
        entry = types.SimpleNamespace(
            entry_id="entry-1",
            title="Planner",
            data={CONF_COOLING_MODE_SWITCH_ENTITY: "switch.cooling"},
            options={},
        )
        entity = PlannerThermostatEntity(coordinator, entry)
        entity.hass = types.SimpleNamespace(
            states=_States({"switch.cooling": cooling_state}),
            data={RUNTIME_STATE: {"entry-1": {"hvac_mode": HVACMode.HEAT}}},
        )
        return entity

    def test_hvac_modes_are_limited_to_off_and_cool_when_cooling_switch_is_on(self):
        entity = self._entity("on")

        self.assertEqual(entity.hvac_modes, [HVACMode.OFF, HVACMode.COOL])
        self.assertEqual(entity.hvac_mode, HVACMode.COOL)

    def test_heat_and_auto_are_ignored_while_cooling_switch_is_on(self):
        entity = self._entity("on")

        asyncio.run(entity.async_set_hvac_mode(HVACMode.HEAT))
        self.assertEqual(entity.hass.data[RUNTIME_STATE]["entry-1"]["hvac_mode"], HVACMode.HEAT)

    def test_all_modes_remain_available_when_cooling_switch_is_off(self):
        entity = self._entity("off")

        self.assertEqual(
            entity.hvac_modes,
            [HVACMode.OFF, HVACMode.HEAT, HVACMode.AUTO, HVACMode.COOL],
        )
