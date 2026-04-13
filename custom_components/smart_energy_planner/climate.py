"""Climate platform for Smart Energy Planner."""

from __future__ import annotations

from homeassistant.components.climate import ClimateEntity
from homeassistant.components.climate.const import (
    ClimateEntityFeature,
    HVACAction,
    HVACMode,
    PRESET_ECO,
    PRESET_NONE,
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
    DEFAULT_THERMOSTAT_COLD_TOLERANCE,
    DEFAULT_THERMOSTAT_CONTROL_CHECK_MINUTES,
    DEFAULT_THERMOSTAT_HOT_TOLERANCE,
    DEFAULT_THERMOSTAT_MAX_TEMP,
    DEFAULT_THERMOSTAT_MIN_CYCLE_MINUTES,
    DEFAULT_THERMOSTAT_MIN_TEMP,
    DOMAIN,
    PLANNER_KIND_THERMOSTAT,
    RUNTIME_STATE,
)
from .coordinator import SmartEnergyPlannerCoordinator
from .__init__ import _async_save_runtime_state


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

    _attr_hvac_modes = [HVACMode.OFF, HVACMode.HEAT]
    _attr_hvac_mode = HVACMode.HEAT
    _attr_supported_features = (
        ClimateEntityFeature.TARGET_TEMPERATURE
        | ClimateEntityFeature.TURN_ON
        | ClimateEntityFeature.TURN_OFF
        | ClimateEntityFeature.PRESET_MODE
    )
    _attr_temperature_unit = UnitOfTemperature.CELSIUS
    _attr_has_entity_name = True
    _attr_name = "Planner Thermostat"
    _attr_target_temperature_step = 0.5
    _attr_precision = 0.1
    _attr_preset_modes = [PRESET_NONE, PRESET_ECO]

    def __init__(self, coordinator: SmartEnergyPlannerCoordinator, entry: ConfigEntry) -> None:
        super().__init__(coordinator)
        self._entry = entry
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
        target = self.coordinator.data.thermostat_setpoint_c
        if target is not None:
            return target
        runtime_state = self.hass.data.setdefault(RUNTIME_STATE, {}).setdefault(self._entry.entry_id, {})
        manual = runtime_state.get("manual_temperature")
        if manual is None:
            manual = min(self.max_temp, max(self.min_temp, 20.0))
        return round(float(manual), 2)

    @property
    def target_temperature_step(self) -> float:
        return 0.5

    @property
    def hvac_mode(self) -> HVACMode:
        runtime_state = self.hass.data.setdefault(RUNTIME_STATE, {}).setdefault(self._entry.entry_id, {})
        if runtime_state.get("hvac_mode") == HVACMode.OFF:
            return HVACMode.OFF
        return HVACMode.HEAT

    @property
    def preset_mode(self) -> str:
        runtime_state = self.hass.data.setdefault(RUNTIME_STATE, {}).setdefault(self._entry.entry_id, {})
        if runtime_state.get("manual_preset_mode") == PRESET_ECO:
            return PRESET_ECO
        return PRESET_ECO if self.coordinator.data.heat_pump_strategy == "energy_saving_on" else PRESET_NONE

    @property
    def hvac_action(self) -> HVACAction:
        if self.hvac_mode == HVACMode.OFF:
            return HVACAction.OFF
        if self.coordinator.data.heat_pump_strategy == "energy_saving_on":
            if self.current_temperature is not None and self.coordinator.data.thermostat_eco_setpoint_c is not None:
                if self.current_temperature <= self.coordinator.data.thermostat_eco_setpoint_c:
                    return HVACAction.HEATING
            return HVACAction.IDLE
        current = self.current_temperature
        target = self.target_temperature
        if current is None or target is None:
            return HVACAction.OFF
        if current < target:
            return HVACAction.HEATING
        return HVACAction.IDLE

    @property
    def min_temp(self) -> float:
        return float(self._merged_config.get(CONF_THERMOSTAT_MIN_TEMP, DEFAULT_THERMOSTAT_MIN_TEMP))

    @property
    def max_temp(self) -> float:
        return float(self._merged_config.get(CONF_THERMOSTAT_MAX_TEMP, DEFAULT_THERMOSTAT_MAX_TEMP))

    async def async_set_hvac_mode(self, hvac_mode: HVACMode) -> None:
        """Keep a simple heat-only thermostat interface."""
        if hvac_mode not in (HVACMode.HEAT, HVACMode.OFF):
            return
        runtime_state = self.hass.data.setdefault(RUNTIME_STATE, {}).setdefault(self._entry.entry_id, {})
        runtime_state["hvac_mode"] = hvac_mode
        await _async_save_runtime_state(self.hass, self._entry.entry_id, runtime_state)
        self.async_write_ha_state()

    @property
    def extra_state_attributes(self) -> dict[str, str | float | None]:
        data = self.coordinator.data
        return {
            "status": data.status,
            "planner_kind": data.planner_kind,
            "thermostat_setpoint_c": self.target_temperature,
            "thermostat_eco_setpoint_c": data.thermostat_eco_setpoint_c,
            "effective_target_temperature": data.thermostat_eco_setpoint_c
            if data.heat_pump_strategy == "energy_saving_on"
            else self.target_temperature,
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
            "planned_eco_window_start": data.planned_eco_window_start,
            "planned_eco_window_end": data.planned_eco_window_end,
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
        """Allow Home Assistant to present normal eco/none controls."""
        if preset_mode not in (PRESET_NONE, PRESET_ECO):
            return
        runtime_state = self.hass.data.setdefault(RUNTIME_STATE, {}).setdefault(self._entry.entry_id, {})
        runtime_state["manual_preset_mode"] = preset_mode
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
        runtime_state["manual_temperature"] = round(clamped_temperature, 2)
        await _async_save_runtime_state(self.hass, self._entry.entry_id, runtime_state)
        await self.coordinator.async_request_refresh()
