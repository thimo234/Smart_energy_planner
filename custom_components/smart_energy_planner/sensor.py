"""Sensor platform for Smart Energy Planner."""

from __future__ import annotations

from homeassistant.components.sensor import SensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import UnitOfEnergy
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN, PLANNER_KIND_BATTERY, PLANNER_KIND_THERMOSTAT, RUNTIME_STATE
from .coordinator import PlannerResult, SmartEnergyPlannerCoordinator


def _planner_entity_name(entry: ConfigEntry, suffix: str) -> str:
    """Build a readable entity name from the planner entry title."""
    return f"{entry.title} {suffix}".strip()


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Smart Energy Planner sensors."""
    coordinator: SmartEnergyPlannerCoordinator = hass.data[DOMAIN][entry.entry_id]
    planner_kind = coordinator.data.planner_kind
    entities: list[PlannerSensor] = []

    if planner_kind == PLANNER_KIND_BATTERY:
        entities.extend(
            [
                BatteryPlannerSensor(coordinator, entry, "score", "Planner Score", "score"),
                BatteryPlannerSensor(coordinator, entry, "battery_strategy", "Battery Strategy", "battery_strategy"),
                BatteryPlannerSensor(
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
                ThermostatPlannerSensor(
                    coordinator,
                    entry,
                    "score",
                    "Planner Score",
                    "score",
                ),
                ThermostatPlannerSensor(
                    coordinator,
                    entry,
                    "room_cooling_hours_to_eco",
                    "Room Cooling Hours To Eco",
                    "room_cooling_hours_to_eco",
                    native_unit_of_measurement="h",
                ),
                ThermostatPlannerSensor(
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
        self._entry_id = entry.entry_id
        self._attr_name = _planner_entity_name(entry, name)
        self._attr_unique_id = f"{entry.entry_id}_{key}"
        self._attr_native_unit_of_measurement = native_unit_of_measurement
        self._attr_icon = "mdi:home-lightning-bolt-outline"

    @property
    def native_value(self):
        return getattr(self.coordinator.data, self._value_key)

    @property
    def extra_state_attributes(self) -> dict[str, str | float | int | None]:
        return {
            "planner_kind": self.coordinator.data.planner_kind,
            "status": self.coordinator.data.status,
            "source_status": self.coordinator.data.source_status,
            "source_errors": self.coordinator.data.source_errors,
        }


class BatteryPlannerSensor(PlannerSensor):
    """Battery planner sensor with battery-only attributes."""

    @property
    def extra_state_attributes(self) -> dict[str, str | float | int | None]:
        data: PlannerResult = self.coordinator.data
        return super().extra_state_attributes | {
            "current_price": data.current_price,
            "price_spread": data.price_spread,
            "next_cheap_window_start": data.next_window_start,
            "next_cheap_window_end": data.next_window_end,
            "next_charge_opportunity_start": getattr(data, "next_charge_opportunity_start", None),
            "home_demand_until_next_charge_kwh": getattr(data, "home_demand_until_next_charge_kwh", 0.0),
            "battery_reserved_energy_kwh": getattr(data, "battery_reserved_energy_kwh", 0.0),
            "battery_energy_available_for_discharge_kwh": getattr(
                data, "battery_energy_available_for_discharge_kwh", 0.0
            ),
            "battery_room_needed_for_solar_kwh": getattr(data, "battery_room_needed_for_solar_kwh", 0.0),
            "next_high_price_window_start": getattr(data, "next_high_price_window_start", None),
            "next_high_price_window_price": getattr(data, "next_high_price_window_price", None),
            "battery_min_profit_per_kwh": data.battery_min_profit_per_kwh,
            "planned_battery_mode_schedule": getattr(data, "planned_battery_mode_schedule", []),
            "price_resolution": data.price_resolution,
            "rationale": data.rationale,
        }


class ThermostatPlannerSensor(PlannerSensor):
    """Thermostat planner sensor with thermostat-only attributes."""

    @property
    def extra_state_attributes(self) -> dict[str, str | float | int | None]:
        data: PlannerResult = self.coordinator.data
        runtime_state = self.coordinator.hass.data.get(RUNTIME_STATE, {}).get(self._entry_id, {})
        return super().extra_state_attributes | {
            "current_price": data.current_price,
            "price_spread": data.price_spread,
            "next_cheap_window_start": data.next_window_start,
            "next_cheap_window_end": data.next_window_end,
            "next_cheap_window_price": data.next_window_price,
            "room_temperature_c": getattr(data, "room_temperature_c", None),
            "thermostat_setpoint_c": getattr(data, "thermostat_setpoint_c", None),
            "thermostat_eco_setpoint_c": getattr(data, "thermostat_eco_setpoint_c", None),
            "room_cooling_hours_to_eco": getattr(data, "room_cooling_hours_to_eco", None),
            "room_cooling_rate_c_per_hour": getattr(data, "room_cooling_rate_c_per_hour", None),
            "cooling_reference_outdoor_temp_c": getattr(data, "cooling_reference_outdoor_temp_c", None),
            "planned_preheat_window_start": getattr(data, "planned_preheat_window_start", None),
            "planned_preheat_window_end": getattr(data, "planned_preheat_window_end", None),
            "planned_preheat_windows": getattr(data, "planned_preheat_windows", []),
            "planned_eco_window_start": getattr(data, "planned_eco_window_start", None),
            "planned_eco_window_end": getattr(data, "planned_eco_window_end", None),
            "planned_eco_windows": getattr(data, "planned_eco_windows", []),
            "cooling_model": runtime_state.get("cooling_model", {}),
            "price_resolution": data.price_resolution,
            "rationale": data.rationale,
        }
