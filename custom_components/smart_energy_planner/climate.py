"""Climate platform for Smart Energy Planner."""

from __future__ import annotations

from homeassistant.components.climate import ClimateEntity
from homeassistant.components.climate.const import (
    ClimateEntityFeature,
    HVACAction,
    HVACMode,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import UnitOfTemperature
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import (
    CONF_PLANNER_KIND,
    CONF_THERMOSTAT_COLD_TOLERANCE,
    CONF_THERMOSTAT_CONTROL_CHECK_MINUTES,
    CONF_THERMOSTAT_HOT_TOLERANCE,
    CONF_THERMOSTAT_MAX_TEMP,
    CONF_THERMOSTAT_MIN_CYCLE_MINUTES,
    CONF_THERMOSTAT_MIN_TEMP,
    CONF_THERMOSTAT_PREHEAT_MINUTES,
    DEFAULT_THERMOSTAT_COLD_TOLERANCE,
    DEFAULT_THERMOSTAT_CONTROL_CHECK_MINUTES,
    DEFAULT_THERMOSTAT_HOT_TOLERANCE,
    DEFAULT_THERMOSTAT_MAX_TEMP,
    DEFAULT_THERMOSTAT_MIN_CYCLE_MINUTES,
    DEFAULT_THERMOSTAT_MIN_TEMP,
    DEFAULT_THERMOSTAT_PREHEAT_MINUTES,
    DOMAIN,
    CONF_COOLING_MODE_SWITCH_ENTITY,
    HVAC_MODE_SMART,
    PLANNER_KIND_THERMOSTAT,
    PRESET_ECO,
    PRESET_NORMAL,
    PRESET_PREHEAT,
    RUNTIME_STATE,
)
from .coordinator import SmartEnergyPlannerCoordinator
from .__init__ import _async_call_turn_service, _async_save_runtime_state


def _planner_thermostat_name(entry: ConfigEntry) -> str:
    """Build the visible thermostat name from the planner entry title."""
    return entry.title


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up climate entities for Smart Energy Planner."""
    coordinator: SmartEnergyPlannerCoordinator = hass.data[DOMAIN][entry.entry_id]
    planner_kind = coordinator.data.planner_kind
    if planner_kind != PLANNER_KIND_THERMOSTAT:
        return

    async_add_entities([PlannerThermostatEntity(coordinator, entry)])


class PlannerThermostatEntity(CoordinatorEntity[SmartEnergyPlannerCoordinator], ClimateEntity):
    """A planned thermostat that represents the integration target temperature."""

    _attr_hvac_mode = HVACMode.HEAT
    _attr_supported_features = (
        ClimateEntityFeature.TARGET_TEMPERATURE
        | ClimateEntityFeature.TURN_ON
        | ClimateEntityFeature.TURN_OFF
        | ClimateEntityFeature.PRESET_MODE
    )
    _attr_temperature_unit = UnitOfTemperature.CELSIUS
    _attr_has_entity_name = True
    _attr_target_temperature_step = 0.5
    _attr_precision = 0.1
    _attr_preset_modes = [PRESET_NORMAL, PRESET_PREHEAT, PRESET_ECO]

    def __init__(self, coordinator: SmartEnergyPlannerCoordinator, entry: ConfigEntry) -> None:
        super().__init__(coordinator)
        self._entry = entry
        self._attr_name = _planner_thermostat_name(entry)
        self._attr_unique_id = f"{entry.entry_id}_planner_thermostat"
        self._attr_icon = "mdi:thermostat-auto"

    @property
    def current_temperature(self) -> float | None:
        current = self.coordinator.data.room_temperature_c
        if current is not None:
            return current
        room_sensor = self._merged_config.get("room_temperature_sensor")
        state = self.hass.states.get(room_sensor) if room_sensor else None
        if state is None:
            return None
        try:
            return float(state.state)
        except (TypeError, ValueError):
            return None

    @property
    def current_humidity(self) -> float | None:
        room_sensor = self._merged_config.get("room_temperature_sensor")
        state = self.hass.states.get(room_sensor) if room_sensor else None
        if state is None:
            return None
        humidity = state.attributes.get("humidity")
        try:
            return float(humidity) if humidity is not None else None
        except (TypeError, ValueError):
            return None

    @property
    def target_temperature(self) -> float | None:
        if self.hvac_mode == HVACMode.OFF:
            return None
        runtime_state = self.hass.data.setdefault(RUNTIME_STATE, {}).setdefault(self._entry.entry_id, {})
        key = self._preset_temperature_key(self.preset_mode)
        fallback = self._preset_fallback_temperature(self.preset_mode)
        manual = runtime_state.get(key, fallback)
        if manual is None:
            manual = self._default_target_for_preset(self.preset_mode)
        return round(float(manual), 2)

    @property
    def target_temperature_step(self) -> float:
        return 0.5

    @property
    def hvac_mode(self) -> HVACMode | str:
        runtime_state = self.hass.data.setdefault(RUNTIME_STATE, {}).setdefault(self._entry.entry_id, {})
        if runtime_state.get("hvac_mode") == HVACMode.OFF:
            return HVACMode.OFF
        if self._cooling_mode_active:
            return HVACMode.COOL
        if runtime_state.get("hvac_mode") in {HVAC_MODE_SMART, "smart", HVACMode.AUTO}:
            return HVACMode.AUTO
        return HVACMode.HEAT

    @property
    def hvac_modes(self) -> list[HVACMode]:
        if self._cooling_mode_active:
            return [HVACMode.OFF, HVACMode.COOL]
        return [HVACMode.OFF, HVACMode.HEAT, HVACMode.AUTO]

    @property
    def preset_mode(self) -> str:
        if self.hvac_mode == HVACMode.OFF:
            return PRESET_NORMAL
        if self.hvac_mode == HVACMode.COOL:
            return PRESET_NORMAL
        runtime_state = self.hass.data.setdefault(RUNTIME_STATE, {}).setdefault(self._entry.entry_id, {})
        if self.hvac_mode == HVACMode.AUTO:
            if self.coordinator.data.heat_pump_strategy == "preheating":
                return PRESET_PREHEAT
            if self.coordinator.data.heat_pump_strategy == "energy_saving_on":
                return PRESET_ECO
            return PRESET_NORMAL
        manual_preset_mode = runtime_state.get("manual_preset_mode", PRESET_NORMAL)
        if manual_preset_mode in (PRESET_NORMAL, PRESET_PREHEAT, PRESET_ECO):
            return str(manual_preset_mode)
        return PRESET_NORMAL

    @property
    def hvac_action(self) -> HVACAction:
        if self.hvac_mode == HVACMode.OFF:
            return HVACAction.OFF
        current = self.current_temperature
        target = self.target_temperature
        if current is None or target is None:
            return HVACAction.OFF
        if self.hvac_mode == HVACMode.COOL:
            if current > target:
                return HVACAction.COOLING
            return HVACAction.IDLE
        if current < target:
            return HVACAction.HEATING
        return HVACAction.IDLE

    @property
    def min_temp(self) -> float:
        return float(self._merged_config.get(CONF_THERMOSTAT_MIN_TEMP, DEFAULT_THERMOSTAT_MIN_TEMP))

    @property
    def max_temp(self) -> float:
        return float(self._merged_config.get(CONF_THERMOSTAT_MAX_TEMP, DEFAULT_THERMOSTAT_MAX_TEMP))

    async def async_set_hvac_mode(self, hvac_mode: HVACMode | str) -> None:
        """Keep a simple heat-only thermostat interface."""
        if hvac_mode not in (HVACMode.HEAT, HVACMode.OFF, HVACMode.COOL, HVACMode.AUTO):
            return
        runtime_state = self.hass.data.setdefault(RUNTIME_STATE, {}).setdefault(self._entry.entry_id, {})
        if hvac_mode == HVACMode.COOL:
            if not self._cooling_mode_switch_entity:
                return
            await _async_call_turn_service(self.hass, self._cooling_mode_switch_entity, "turn_on")
            runtime_state["hvac_mode"] = HVACMode.COOL
        else:
            if self._cooling_mode_switch_entity and self._cooling_mode_active:
                await _async_call_turn_service(self.hass, self._cooling_mode_switch_entity, "turn_off")
            runtime_state["hvac_mode"] = HVAC_MODE_SMART if hvac_mode == HVACMode.AUTO else hvac_mode
        if hvac_mode == HVACMode.OFF:
            runtime_state["manual_preset_mode"] = PRESET_NORMAL
        await _async_save_runtime_state(self.hass, self._entry.entry_id, runtime_state)
        await self.coordinator.async_request_refresh()
        self.async_write_ha_state()

    @property
    def extra_state_attributes(self) -> dict[str, str | float | list | None]:
        data = self.coordinator.data
        return {
            "status": data.status,
            "planner_kind": data.planner_kind,
            "thermostat_setpoint_c": data.thermostat_setpoint_c,
            "thermostat_cool_setpoint_c": getattr(data, "thermostat_cool_setpoint_c", None),
            "thermostat_preheat_setpoint_c": getattr(data, "thermostat_preheat_setpoint_c", None),
            "thermostat_eco_setpoint_c": data.thermostat_eco_setpoint_c,
            "effective_target_temperature": self.target_temperature,
            "active_hvac_mode": self.hvac_mode,
            "active_preset_mode": self.preset_mode,
            "cooling_mode_switch_entity": self._cooling_mode_switch_entity,
            "cooling_mode_active": self._cooling_mode_active,
            "cold_tolerance": self._merged_config.get(
                CONF_THERMOSTAT_COLD_TOLERANCE, DEFAULT_THERMOSTAT_COLD_TOLERANCE
            ),
            "hot_tolerance": self._merged_config.get(
                CONF_THERMOSTAT_HOT_TOLERANCE, DEFAULT_THERMOSTAT_HOT_TOLERANCE
            ),
            "min_cycle_minutes": self._merged_config.get(
                CONF_THERMOSTAT_MIN_CYCLE_MINUTES, DEFAULT_THERMOSTAT_MIN_CYCLE_MINUTES
            ),
            "control_check_minutes": self._merged_config.get(
                CONF_THERMOSTAT_CONTROL_CHECK_MINUTES, DEFAULT_THERMOSTAT_CONTROL_CHECK_MINUTES
            ),
            "preheat_minutes": self._merged_config.get(
                CONF_THERMOSTAT_PREHEAT_MINUTES, DEFAULT_THERMOSTAT_PREHEAT_MINUTES
            ),
            "planned_preheat_window_start": getattr(data, "planned_preheat_window_start", None),
            "planned_preheat_window_end": getattr(data, "planned_preheat_window_end", None),
            "planned_preheat_windows": getattr(data, "planned_preheat_windows", []),
            "planned_eco_window_start": data.planned_eco_window_start,
            "planned_eco_window_end": data.planned_eco_window_end,
            "planned_eco_windows": getattr(data, "planned_eco_windows", []),
            "room_cooling_hours_to_eco": data.room_cooling_hours_to_eco,
            "rationale": data.rationale,
        }

    @property
    def _merged_config(self) -> dict:
        return {**self._entry.data, **self._entry.options}

    async def async_turn_on(self) -> None:
        """Turn the planner thermostat on."""
        await self.async_set_hvac_mode(HVACMode.HEAT)

    async def async_turn_off(self) -> None:
        """Turn the planner thermostat off."""
        await self.async_set_hvac_mode(HVACMode.OFF)

    async def async_set_preset_mode(self, preset_mode: str) -> None:
        """Allow Home Assistant to present normal and eco controls."""
        if preset_mode not in (PRESET_NORMAL, PRESET_PREHEAT, PRESET_ECO):
            return
        if self.hvac_mode == HVACMode.COOL:
            return
        runtime_state = self.hass.data.setdefault(RUNTIME_STATE, {}).setdefault(self._entry.entry_id, {})
        runtime_state["manual_preset_mode"] = preset_mode
        runtime_state["hvac_mode"] = HVACMode.HEAT
        await _async_save_runtime_state(self.hass, self._entry.entry_id, runtime_state)
        await self.coordinator.async_request_refresh()
        self.async_write_ha_state()

    async def async_set_temperature(self, **kwargs) -> None:
        """Store manual temperature changes on the planner thermostat."""
        temperature = kwargs.get("temperature")
        if temperature is None:
            return

        clamped_temperature = min(self.max_temp, max(self.min_temp, float(temperature)))
        runtime_state = self.hass.data.setdefault(RUNTIME_STATE, {}).setdefault(
            self._entry.entry_id, {}
        )
        key = self._preset_temperature_key(self.preset_mode)
        runtime_state[key] = round(clamped_temperature, 2)
        await _async_save_runtime_state(self.hass, self._entry.entry_id, runtime_state)
        await self.coordinator.async_request_refresh()

    def _preset_temperature_key(self, preset_mode: str) -> str:
        if self.hvac_mode == HVACMode.COOL:
            return "manual_cool_temperature"
        if preset_mode == PRESET_PREHEAT:
            return "manual_preheat_temperature"
        if preset_mode == PRESET_ECO:
            return "manual_eco_temperature"
        return "manual_temperature"

    def _preset_fallback_temperature(self, preset_mode: str) -> float | None:
        if self.hvac_mode == HVACMode.COOL:
            return getattr(self.coordinator.data, "thermostat_cool_setpoint_c", None)
        if preset_mode == PRESET_PREHEAT:
            return getattr(self.coordinator.data, "thermostat_preheat_setpoint_c", None)
        if preset_mode == PRESET_ECO:
            return self.coordinator.data.thermostat_eco_setpoint_c
        return self.coordinator.data.thermostat_setpoint_c

    def _default_target_for_preset(self, preset_mode: str) -> float:
        if self.hvac_mode == HVACMode.COOL:
            return min(self.max_temp, max(self.min_temp, 24.0))
        if preset_mode == PRESET_PREHEAT:
            return min(self.max_temp, max(self.min_temp, 21.0))
        if preset_mode == PRESET_ECO:
            return min(self.max_temp, max(self.min_temp, 18.0))
        return min(self.max_temp, max(self.min_temp, 20.0))

    @property
    def _cooling_mode_switch_entity(self) -> str | None:
        return self._merged_config.get(CONF_COOLING_MODE_SWITCH_ENTITY)

    @property
    def _cooling_mode_active(self) -> bool:
        cooling_switch = self._cooling_mode_switch_entity
        if not cooling_switch:
            return False
        state = self.hass.states.get(cooling_switch)
        if state is None:
            return False
        return str(state.state).lower() in {"on", "heat", "heating", "cool", "cooling"}
