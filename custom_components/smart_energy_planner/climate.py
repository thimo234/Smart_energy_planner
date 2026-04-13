"""Climate platform for Smart Energy Planner."""

from __future__ import annotations

from homeassistant.components.climate import ClimateEntity
from homeassistant.components.climate.const import ClimateEntityFeature, HVACMode
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import UnitOfTemperature
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import (
    CONF_PLANNER_KIND,
    CONF_THERMOSTAT_ENTITY,
    DOMAIN,
    PLANNER_KIND_COMBINED,
    PLANNER_KIND_THERMOSTAT,
)
from .coordinator import SmartEnergyPlannerCoordinator


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up climate entities for Smart Energy Planner."""
    coordinator: SmartEnergyPlannerCoordinator = hass.data[DOMAIN][entry.entry_id]
    planner_kind = coordinator.data.planner_kind
    if planner_kind not in (PLANNER_KIND_COMBINED, PLANNER_KIND_THERMOSTAT):
        return

    async_add_entities([PlannerThermostatEntity(coordinator, entry)])


class PlannerThermostatEntity(CoordinatorEntity[SmartEnergyPlannerCoordinator], ClimateEntity):
    """A planned thermostat that represents the integration target temperature."""

    _attr_hvac_modes = [HVACMode.HEAT]
    _attr_hvac_mode = HVACMode.HEAT
    _attr_supported_features = ClimateEntityFeature.TARGET_TEMPERATURE
    _attr_temperature_unit = UnitOfTemperature.CELSIUS
    _attr_has_entity_name = True
    _attr_name = "Planner Thermostat"

    def __init__(self, coordinator: SmartEnergyPlannerCoordinator, entry: ConfigEntry) -> None:
        super().__init__(coordinator)
        self._entry = entry
        self._attr_unique_id = f"{entry.entry_id}_planner_thermostat"
        self._attr_icon = "mdi:thermostat-auto"

    @property
    def current_temperature(self) -> float | None:
        return self.coordinator.data.room_temperature_c

    @property
    def target_temperature(self) -> float | None:
        if self.coordinator.data.heat_pump_strategy == "energy_saving_on":
            return self.coordinator.data.thermostat_eco_setpoint_c
        return self.coordinator.data.thermostat_setpoint_c

    @property
    def min_temp(self) -> float:
        return 5.0

    @property
    def max_temp(self) -> float:
        return 30.0

    @property
    def extra_state_attributes(self) -> dict[str, str | float | None]:
        data = self.coordinator.data
        return {
            "status": data.status,
            "planner_kind": data.planner_kind,
            "underlying_thermostat_entity": self._merged_config.get(CONF_THERMOSTAT_ENTITY),
            "thermostat_setpoint_c": data.thermostat_setpoint_c,
            "thermostat_eco_setpoint_c": data.thermostat_eco_setpoint_c,
            "planned_eco_window_start": data.planned_eco_window_start,
            "planned_eco_window_end": data.planned_eco_window_end,
            "room_cooling_hours_to_eco": data.room_cooling_hours_to_eco,
            "rationale": data.rationale,
        }

    @property
    def _merged_config(self) -> dict:
        return {**self._entry.data, **self._entry.options}

    async def async_set_temperature(self, **kwargs) -> None:
        """Forward manual temperature changes to the underlying thermostat."""
        temperature = kwargs.get("temperature")
        thermostat_entity = self._merged_config.get(CONF_THERMOSTAT_ENTITY)
        if temperature is None or thermostat_entity is None:
            return

        await self.hass.services.async_call(
            "climate",
            "set_temperature",
            {"entity_id": thermostat_entity, "temperature": temperature},
            blocking=True,
        )
        await self.coordinator.async_request_refresh()
