"""Binary sensor platform for Smart Energy Planner."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from homeassistant.components.binary_sensor import BinarySensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity
from homeassistant.util import dt as dt_util

from .const import (
    CONF_PRICE_WINDOW_DURATION_HOURS,
    CONF_PRICE_WINDOW_WHOLE_HOUR_START,
    DEFAULT_PRICE_WINDOW_DURATION_HOURS,
    DEFAULT_PRICE_WINDOW_WHOLE_HOUR_START,
    DOMAIN,
    PLANNER_KIND_PRICE_WINDOW,
)
from .coordinator import SmartEnergyPlannerCoordinator


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Smart Energy Planner binary sensors."""
    coordinator: SmartEnergyPlannerCoordinator = hass.data[DOMAIN][entry.entry_id]
    if coordinator.data.planner_kind != PLANNER_KIND_PRICE_WINDOW:
        return

    async_add_entities(
        [
            PriceWindowBinarySensor(
                coordinator,
                entry,
                key="cheapest_price_window",
                name="Low Price Window Active",
                window_key="cheapest_price_window",
                icon="mdi:cash-clock",
            ),
            PriceWindowBinarySensor(
                coordinator,
                entry,
                key="most_expensive_price_window",
                name="High Price Window Active",
                window_key="most_expensive_price_window",
                icon="mdi:cash-alert",
            ),
        ]
    )


class PriceWindowBinarySensor(CoordinatorEntity[SmartEnergyPlannerCoordinator], BinarySensorEntity):
    """Binary sensor that is on while a selected price window is active."""

    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: SmartEnergyPlannerCoordinator,
        entry: ConfigEntry,
        *,
        key: str,
        name: str,
        window_key: str,
        icon: str,
    ) -> None:
        super().__init__(coordinator)
        self._entry = entry
        self._window_key = window_key
        self._attr_name = f"{entry.title} {name}".strip()
        self._attr_unique_id = f"{entry.entry_id}_{key}"
        self._attr_icon = icon

    @property
    def is_on(self) -> bool:
        window = self._window
        start = _parse_datetime(window.get("start") if window else None)
        end = _parse_datetime(window.get("end") if window else None)
        if start is None or end is None:
            return False
        now = dt_util.now()
        return start <= now < end

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        window = self._window or {}
        merged = {**self._entry.data, **self._entry.options}
        return {
            "planner_kind": self.coordinator.data.planner_kind,
            "start": window.get("start"),
            "end": window.get("end"),
            "average_price": window.get("average_price"),
            "duration_hours": window.get("duration_hours"),
            "configured_duration_hours": merged.get(
                CONF_PRICE_WINDOW_DURATION_HOURS,
                DEFAULT_PRICE_WINDOW_DURATION_HOURS,
            ),
            "whole_hour_start": merged.get(
                CONF_PRICE_WINDOW_WHOLE_HOUR_START,
                DEFAULT_PRICE_WINDOW_WHOLE_HOUR_START,
            ),
            "source_status": self.coordinator.data.source_status,
            "source_errors": self.coordinator.data.source_errors,
        }

    @property
    def _window(self) -> dict[str, str | float] | None:
        value = getattr(self.coordinator.data, self._window_key, None)
        return value if isinstance(value, dict) else None


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None
