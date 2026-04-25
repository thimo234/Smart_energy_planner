"""Sensor platform for Smart Energy Planner."""

from __future__ import annotations

from homeassistant.components.sensor import SensorDeviceClass, SensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import UnitOfEnergy
from homeassistant.core import HomeAssistant, callback
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
                BatteryProfitSensor(
                    coordinator,
                    entry,
                    "battery_profit_total_eur",
                    "Battery Profit Total",
                ),
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
            self._value_key: self.native_value,
            "source_status": self.coordinator.data.source_status,
            "source_errors": self.coordinator.data.source_errors,
        }


class BatteryPlannerSensor(PlannerSensor):
    """Battery planner sensor with battery-only attributes."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._last_written_mode: str | None = None

    @callback
    def _handle_coordinator_update(self) -> None:
        # Only write HA state (and fire state_changed) when the mode value
        # actually changes.  Every coordinator refresh updates attributes like
        # current_price and battery_soc, which would otherwise fire a
        # state_changed event every 15 minutes even while mode stays the same,
        # causing automations to trigger unnecessarily.
        current_mode = str(self.coordinator.data.battery_strategy or "")
        if current_mode != self._last_written_mode:
            self._last_written_mode = current_mode
            self.async_write_ha_state()

    @property
    def extra_state_attributes(self) -> dict[str, str | float | int | None]:
        data: PlannerResult = self.coordinator.data
        return super().extra_state_attributes | {
            "current_price": data.current_price,
            "price_spread": data.price_spread,
            "next_cheap_window_start": data.next_window_start,
            "next_cheap_window_end": data.next_window_end,
            "battery_soc_percent": getattr(data, "battery_soc_percent", None),
            "battery_total_energy_kwh": getattr(data, "battery_total_energy_kwh", 0.0),
            "battery_energy_available_kwh": getattr(data, "battery_energy_available_kwh", 0.0),
            "battery_remaining_capacity_kwh": getattr(data, "battery_remaining_capacity_kwh", 0.0),
            "next_charge_opportunity_start": getattr(data, "next_charge_opportunity_start", None),
            "next_charge_window_start": getattr(data, "next_charge_window_start", None),
            "next_charge_window_end": getattr(data, "next_charge_window_end", None),
            "next_charge_window_hours": getattr(data, "next_charge_window_hours", 0.0),
            "following_charge_window_start": getattr(data, "following_charge_window_start", None),
            "following_charge_window_end": getattr(data, "following_charge_window_end", None),
            "following_charge_window_hours": getattr(data, "following_charge_window_hours", 0.0),
            "next_discharge_window_start": getattr(data, "next_discharge_window_start", None),
            "next_discharge_window_end": getattr(data, "next_discharge_window_end", None),
            "next_discharge_window_hours": getattr(data, "next_discharge_window_hours", 0.0),
            "next_idle_window_start": getattr(data, "next_idle_window_start", None),
            "current_relevant_battery_window_start": getattr(
                data, "current_relevant_battery_window_start", None
            ),
            "current_relevant_battery_window_end": getattr(data, "current_relevant_battery_window_end", None),
            "current_relevant_battery_window_mode": getattr(data, "current_relevant_battery_window_mode", None),
            "current_relevant_battery_window_expected_demand_kwh": getattr(
                data, "current_relevant_battery_window_expected_demand_kwh", 0.0
            ),
            "current_relevant_battery_window_expected_solar_kwh": getattr(
                data, "current_relevant_battery_window_expected_solar_kwh", 0.0
            ),
            "home_demand_until_next_charge_kwh": getattr(data, "home_demand_until_next_charge_kwh", 0.0),
            "battery_reserved_energy_kwh": getattr(data, "battery_reserved_energy_kwh", 0.0),
            "battery_energy_available_for_discharge_kwh": getattr(
                data, "battery_energy_available_for_discharge_kwh", 0.0
            ),
            "battery_exportable_energy_kwh": getattr(data, "battery_exportable_energy_kwh", 0.0),
            "battery_room_needed_for_solar_kwh": getattr(data, "battery_room_needed_for_solar_kwh", 0.0),
            "battery_charge_hours_needed_total": getattr(data, "battery_charge_hours_needed_total", 0.0),
            "battery_full_discharge_hours": getattr(data, "battery_full_discharge_hours", 0.0),
            "next_high_price_window_start": getattr(data, "next_high_price_window_start", None),
            "next_high_price_window_price": getattr(data, "next_high_price_window_price", None),
            "battery_min_profit_per_kwh": data.battery_min_profit_per_kwh,
            "planned_battery_mode_schedule": getattr(data, "planned_battery_mode_schedule", []),
            "price_resolution": data.price_resolution,
            "rationale": data.rationale,
        }


class BatteryProfitSensor(PlannerSensor):
    """Cumulative realized battery profit sensor."""

    _attr_device_class = SensorDeviceClass.MONETARY

    def __init__(
        self,
        coordinator: SmartEnergyPlannerCoordinator,
        entry: ConfigEntry,
        key: str,
        name: str,
    ) -> None:
        super().__init__(
            coordinator,
            entry,
            key,
            name,
            value_key="score",
            native_unit_of_measurement="EUR",
        )
        self._attr_icon = "mdi:cash-multiple"

    @property
    def native_value(self):
        runtime_state = self.coordinator.hass.data.get(RUNTIME_STATE, {}).get(self._entry_id, {})
        value = runtime_state.get("battery_profit_total_eur", 0.0)
        try:
            return round(float(value), 4)
        except (TypeError, ValueError):
            return 0.0

    @property
    def extra_state_attributes(self) -> dict[str, str | float | int | None]:
        runtime_state = self.coordinator.hass.data.get(RUNTIME_STATE, {}).get(self._entry_id, {})
        tracked_energy = runtime_state.get("battery_profit_tracked_energy_kwh", 0.0)
        cost_basis = runtime_state.get("battery_profit_cost_basis_eur", 0.0)
        average_cost = None
        try:
            tracked_energy_float = float(tracked_energy)
            cost_basis_float = float(cost_basis)
            if tracked_energy_float > 0:
                average_cost = round(cost_basis_float / tracked_energy_float, 4)
        except (TypeError, ValueError, ZeroDivisionError):
            average_cost = None
        return super().extra_state_attributes | {
            "tracked_battery_energy_kwh": tracked_energy,
            "tracked_cost_basis_eur": cost_basis,
            "tracked_average_cost_per_kwh": average_cost,
            "last_tracked_battery_energy_kwh": runtime_state.get("battery_profit_last_energy_kwh"),
            "last_profit_update": runtime_state.get("battery_profit_last_updated"),
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
            "thermostat_cool_setpoint_c": getattr(data, "thermostat_cool_setpoint_c", None),
            "thermostat_preheat_setpoint_c": getattr(data, "thermostat_preheat_setpoint_c", None),
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
