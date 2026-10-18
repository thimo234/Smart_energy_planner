"""Sensor platform for Smart Energy Planner."""

from __future__ import annotations

from datetime import datetime

from homeassistant.components.sensor import SensorDeviceClass, SensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import UnitOfEnergy
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.update_coordinator import CoordinatorEntity
from homeassistant.util import dt as dt_util

from .battery_planner import normalize_full_battery_charge_mode
from .const import (
    DOMAIN,
    PLANNER_KIND_BATTERY,
    PLANNER_KIND_PRICE_WINDOW,
    RUNTIME_STATE,
)
from .coordinator import SmartEnergyPlannerCoordinator
from .planner_result import PlannerResult

_BATTERY_STRATEGY_OPTIONS = [
    "accu_uit",
    "laden_van_net",
    "laden_met_zonne_energie",
    "ontladen",
    "ontladen_naar_net",
]


_FULL_BATTERY_EARLY_DISCHARGE_LOOKAHEAD_HOURS = 3.0


def _planner_entity_name(entry: ConfigEntry, suffix: str) -> str:
    """Build a readable entity name from the planner entry title."""
    return f"{entry.title} {suffix}".strip()


def _parse_datetime(value) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.astimezone()


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
        registry = er.async_get(hass)
        old_score = registry.async_get_entity_id("sensor", DOMAIN, f"{entry.entry_id}_score")
        if old_score is not None:
            registry.async_remove(old_score)
        entities.extend(
            [
                BatteryStrategySensor(coordinator, entry),
                BatteryProfitSensor(
                    coordinator,
                    entry,
                    "battery_profit_total_eur",
                    "Battery Profit Total",
                ),
                HomeDemandSensor(coordinator, entry),
            ]
        )

    if planner_kind == PLANNER_KIND_PRICE_WINDOW:
        entities.append(CheapestPriceWindowStartSensor(coordinator, entry))

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


class CheapestPriceWindowStartSensor(PlannerSensor):
    """Timestamp at which the selected price block starts (legacy entity ID)."""

    _attr_device_class = SensorDeviceClass.TIMESTAMP

    def __init__(
        self,
        coordinator: SmartEnergyPlannerCoordinator,
        entry: ConfigEntry,
    ) -> None:
        super().__init__(
            coordinator,
            entry,
            "cheapest_price_window_start",
            "Price Window Start",
            "cheapest_price_window_start",
        )
        self._attr_icon = "mdi:clock-start"

    @property
    def native_value(self) -> datetime | None:
        return _parse_datetime(self._window.get("start"))

    @property
    def extra_state_attributes(self) -> dict:
        window = self._window
        following = self._following_window
        return super().extra_state_attributes | {
            "price_window_type": getattr(self.coordinator.data, "price_window_type", None),
            "selected_price_window_start": window.get("start"),
            "end": window.get("end"),
            "average_price": window.get("average_price"),
            "duration_hours": window.get("duration_hours"),
            "next_price_window_start": following.get("start"),
            "next_price_window_end": following.get("end"),
        }

    @property
    def _following_window(self) -> dict:
        end = _parse_datetime(self._window.get("end"))
        if end is None:
            return {}
        data = self.coordinator.data
        candidates = list(getattr(data, "planned_price_peak_windows", None) or [])
        if getattr(data, "price_window_type", None) != "most_expensive":
            candidates = [getattr(data, "tomorrow_cheapest_price_window", None)]
        following = [w for w in candidates if isinstance(w, dict)
                     and (start := _parse_datetime(w.get("start"))) is not None and start >= end]
        return min(following, key=lambda w: _parse_datetime(w["start"])) if following else {}

    @property
    def _window(self) -> dict:
        value = getattr(self.coordinator.data, "selected_price_window", None)
        return value if isinstance(value, dict) else {}


class BatteryPlannerSensor(PlannerSensor):
    """Battery planner sensor with battery-only attributes."""

    # Keep forecast series available to cards and automations, but do not
    # duplicate the entire changing forecast in Recorder on every update.
    _unrecorded_attributes = frozenset(
        {
            "upcoming_energy_price_windows",
            "upcoming_export_price_windows",
            "estimated_battery_mode_windows",
            "estimated_hourly_home_demand",
            "estimated_hourly_solar_forecast",
            "planned_battery_mode_schedule",
            "planned_battery_mode_windows",
        }
    )

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

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
            "battery_charge_cycle_active": getattr(data, "battery_charge_cycle_active", False),
            "battery_discharge_cycle_active": getattr(data, "battery_discharge_cycle_active", False),
            "home_demand_until_next_charge_kwh": getattr(data, "home_demand_until_next_charge_kwh", 0.0),
            "battery_reserved_energy_kwh": getattr(data, "battery_reserved_energy_kwh", 0.0),
            "battery_no_charge_min_soc_percent": getattr(data, "battery_no_charge_min_soc_percent", 20.0),
            "battery_no_charge_reserve_active": getattr(data, "battery_no_charge_reserve_active", False),
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
            "upcoming_energy_price_windows": getattr(data, "upcoming_energy_price_windows", []),
            "current_export_price": getattr(data, "current_export_price", None),
            "upcoming_export_price_windows": getattr(data, "upcoming_export_price_windows", None) or [],
            "estimated_battery_mode_windows": getattr(data, "estimated_battery_mode_windows", None) or [],
            "estimated_hourly_home_demand": getattr(data, "estimated_hourly_home_demand", []),
            "estimated_hourly_solar_forecast": getattr(data, "estimated_hourly_solar_forecast", []),
            "planned_battery_mode_schedule": getattr(data, "planned_battery_mode_schedule", []),
            "planned_battery_mode_windows": getattr(data, "planned_battery_mode_windows", []),
            "price_resolution": data.price_resolution,
            "rationale": data.rationale,
        }


class HomeDemandSensor(PlannerSensor):
    """Sensor exposing estimated total and hourly home demand for diagnostics."""

    _unrecorded_attributes = frozenset({"estimated_hourly_home_demand"})

    def __init__(
        self,
        coordinator: SmartEnergyPlannerCoordinator,
        entry: ConfigEntry,
    ) -> None:
        super().__init__(
            coordinator,
            entry,
            "estimated_home_demand_today",
            "Estimated Home Demand Today",
            "estimated_total_home_demand_kwh",
            native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        )

    @property
    def extra_state_attributes(self) -> dict:
        data = self.coordinator.data
        today = dt_util.now().date()
        return {
            "estimated_total_home_demand_kwh": self.native_value,
            "forecast_date": today.isoformat(),
            "estimated_hourly_home_demand": [w for w in data.estimated_hourly_home_demand
                if (start := _parse_datetime(w.get("start"))) is not None
                and dt_util.as_local(start).date() == today],
            "source_status": {k: v for k, v in data.source_status.items() if k == "total_energy_sensor"},
            "source_errors": [error for error in data.source_errors
                              if str(error).startswith("total_energy_sensor:")],
        }


class BatteryStrategySensor(BatteryPlannerSensor):
    """Battery strategy sensor with ENUM device class for automation state picker."""

    _attr_device_class = "enum"
    _attr_options = _BATTERY_STRATEGY_OPTIONS

    def __init__(
        self,
        coordinator: SmartEnergyPlannerCoordinator,
        entry: ConfigEntry,
    ) -> None:
        super().__init__(coordinator, entry, "battery_strategy", "Battery Strategy", "battery_strategy")

    @property
    def native_value(self):
        data: PlannerResult = self.coordinator.data
        usable_energy_kwh = getattr(data, "battery_energy_available_kwh", 0.0)
        usable_capacity_kwh = max(
            0.0,
            getattr(data, "battery_total_energy_kwh", 0.0)
            + getattr(data, "battery_remaining_capacity_kwh", 0.0)
            - (getattr(data, "battery_total_energy_kwh", 0.0) - usable_energy_kwh),
        )
        normalized_mode = normalize_full_battery_charge_mode(
            mode=data.battery_strategy,
            usable_energy_kwh=usable_energy_kwh,
            usable_capacity_kwh=usable_capacity_kwh,
        )
        if normalized_mode not in _BATTERY_STRATEGY_OPTIONS:
            return "accu_uit"

        battery_is_physically_full = getattr(data, "battery_remaining_capacity_kwh", 0.0) <= 0.05
        if battery_is_physically_full and normalized_mode in ("laden_van_net", "laden_met_zonne_energie"):
            now = dt_util.now()
            next_discharge_start = _parse_datetime(getattr(data, "next_discharge_window_start", None))
            next_discharge_end = _parse_datetime(getattr(data, "next_discharge_window_end", None))
            if next_discharge_start is not None and (next_discharge_end is None or now < next_discharge_end):
                hours_until_discharge = (next_discharge_start - now).total_seconds() / 3600
                if 0.0 <= hours_until_discharge <= _FULL_BATTERY_EARLY_DISCHARGE_LOOKAHEAD_HOURS:
                    return "ontladen"

        return normalized_mode


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
