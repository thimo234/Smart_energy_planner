"""Sensor platform for Smart Energy Planner."""

from __future__ import annotations

from homeassistant.components.sensor import SensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import UnitOfEnergy
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import PlannerResult, SmartEnergyPlannerCoordinator


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Smart Energy Planner sensors."""
    coordinator: SmartEnergyPlannerCoordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities(
        [
            PlannerSensor(coordinator, entry, "score", "Planner Score", "score"),
            PlannerSensor(coordinator, entry, "recommendation", "Planner Recommendation", "recommendation"),
            PlannerSensor(coordinator, entry, "battery_strategy", "Battery Strategy", "battery_strategy"),
            PlannerSensor(coordinator, entry, "heat_pump_strategy", "Heat Pump Strategy", "heat_pump_strategy"),
            PlannerSensor(
                coordinator,
                entry,
                "estimated_home_demand_today",
                "Estimated Home Demand Today",
                "estimated_total_home_demand_kwh",
                native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
            ),
            PlannerSensor(
                coordinator,
                entry,
                "heating_estimate",
                "Heating Estimate",
                "heating_estimate_kwh",
                native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
            ),
        ]
    )


class PlannerSensor(CoordinatorEntity[SmartEnergyPlannerCoordinator], SensorEntity):
    """Representation of a Smart Energy Planner sensor."""

    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: SmartEnergyPlannerCoordinator,
        entry: ConfigEntry,
        key: str,
        name: str,
        value_key: str,
        *,
        native_unit_of_measurement: str | None = None,
    ) -> None:
        super().__init__(coordinator)
        self._value_key = value_key
        self._attr_name = name
        self._attr_unique_id = f"{entry.entry_id}_{key}"
        self._attr_native_unit_of_measurement = native_unit_of_measurement
        self._attr_icon = "mdi:home-lightning-bolt-outline"

    @property
    def native_value(self):
        return getattr(self.coordinator.data, self._value_key)

    @property
    def extra_state_attributes(self) -> dict[str, str | float | int | None]:
        data: PlannerResult = self.coordinator.data
        return {
            "status": data.status,
            "source_status": data.source_status,
            "source_errors": data.source_errors,
            "current_price": data.current_price,
            "price_spread": data.price_spread,
            "next_cheap_window_start": data.next_window_start,
            "next_cheap_window_end": data.next_window_end,
            "next_cheap_window_price": data.next_window_price,
            "best_solar_window_start": data.best_solar_window_start,
            "best_solar_window_end": data.best_solar_window_end,
            "best_solar_window_kwh": data.best_solar_window_kwh,
            "solcast_confidence": data.solcast_confidence,
            "solar_forecast_kwh": data.solar_forecast_kwh,
            "heating_estimate_kwh": data.heating_estimate_kwh,
            "lookback_daily_average_kwh": data.lookback_daily_average_kwh,
            "total_energy_daily_average_kwh": data.total_energy_daily_average_kwh,
            "non_heating_daily_average_kwh": data.non_heating_daily_average_kwh,
            "estimated_total_home_demand_kwh": data.estimated_total_home_demand_kwh,
            "estimated_hourly_home_demand": data.estimated_hourly_home_demand,
            "price_resolution": data.price_resolution,
            "rationale": data.rationale,
        }
