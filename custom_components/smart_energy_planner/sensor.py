"""Sensor platform for Smart Energy Planner."""

from __future__ import annotations

from homeassistant.components.sensor import SensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import UnitOfEnergy
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN, PLANNER_KIND_BATTERY, PLANNER_KIND_THERMOSTAT
from .coordinator import PlannerResult, SmartEnergyPlannerCoordinator


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Smart Energy Planner sensors."""
    coordinator: SmartEnergyPlannerCoordinator = hass.data[DOMAIN][entry.entry_id]
    planner_kind = coordinator.data.planner_kind
    entities: list[PlannerSensor] = [
        PlannerSensor(coordinator, entry, "score", "Planner Score", "score"),
        PlannerSensor(coordinator, entry, "recommendation", "Planner Recommendation", "recommendation"),
    ]

    if planner_kind == PLANNER_KIND_BATTERY:
        entities.extend(
            [
                PlannerSensor(coordinator, entry, "battery_strategy", "Battery Strategy", "battery_strategy"),
                PlannerSensor(
                    coordinator,
                    entry,
                    "estimated_home_demand_today",
                    "Estimated Home Demand Today",
                    "estimated_total_home_demand_kwh",
                    native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
                ),
            ]
        )

    if planner_kind == PLANNER_KIND_THERMOSTAT:
        entities.extend(
            [
                PlannerSensor(
                    coordinator,
                    entry,
                    "room_cooling_hours_to_eco",
                    "Room Cooling Hours To Eco",
                    "room_cooling_hours_to_eco",
                    native_unit_of_measurement="h",
                ),
                PlannerSensor(
                    coordinator,
                    entry,
                    "thermostat_eco_start_time",
                    "Thermostat Eco Start Time",
                    "planned_eco_window_start",
                ),
            ]
        )

    async_add_entities(entities)


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
            "planner_kind": data.planner_kind,
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
            "lookback_daily_average_kwh": data.lookback_daily_average_kwh,
            "total_energy_daily_average_kwh": data.total_energy_daily_average_kwh,
            "self_used_energy_daily_average_kwh": data.non_heating_daily_average_kwh,
            "estimated_total_home_demand_kwh": data.estimated_total_home_demand_kwh,
            "estimated_hourly_home_demand": getattr(data, "estimated_hourly_home_demand", []),
            "projected_remaining_solar_until_sunset_kwh": getattr(
                data, "projected_remaining_solar_until_sunset_kwh", 0.0
            ),
            "projected_remaining_home_demand_until_sunset_kwh": getattr(
                data, "projected_remaining_home_demand_until_sunset_kwh", 0.0
            ),
            "projected_solar_surplus_until_sunset_kwh": getattr(
                data, "projected_solar_surplus_until_sunset_kwh", 0.0
            ),
            "grid_charge_needed_until_sunset_kwh": getattr(
                data, "grid_charge_needed_until_sunset_kwh", 0.0
            ),
            "battery_charge_hours_needed_until_sunset": getattr(
                data, "battery_charge_hours_needed_until_sunset", 0.0
            ),
            "target_battery_full_by_sunset": getattr(data, "target_battery_full_by_sunset", False),
            "planned_grid_charge_windows": getattr(data, "planned_grid_charge_windows", []),
            "room_temperature_c": getattr(data, "room_temperature_c", None),
            "thermostat_setpoint_c": getattr(data, "thermostat_setpoint_c", None),
            "thermostat_eco_setpoint_c": getattr(data, "thermostat_eco_setpoint_c", None),
            "room_cooling_hours_to_eco": getattr(data, "room_cooling_hours_to_eco", None),
            "room_cooling_rate_c_per_hour": getattr(data, "room_cooling_rate_c_per_hour", None),
            "cooling_reference_outdoor_temp_c": getattr(data, "cooling_reference_outdoor_temp_c", None),
            "planned_eco_window_start": getattr(data, "planned_eco_window_start", None),
            "planned_eco_window_end": getattr(data, "planned_eco_window_end", None),
            "battery_min_profit_per_kwh": data.battery_min_profit_per_kwh,
            "price_resolution": data.price_resolution,
            "rationale": data.rationale,
        }
