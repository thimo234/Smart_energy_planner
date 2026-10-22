"""Binary sensor platform for Smart Energy Planner."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from homeassistant.components.binary_sensor import BinarySensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.event import async_track_point_in_time
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity
from homeassistant.util import dt as dt_util

from .const import (
    CONF_PRICE_WINDOW_TYPE,
    CONF_PRICE_PEAK_MIN_DURATION_MINUTES, DEFAULT_PRICE_PEAK_MIN_DURATION_MINUTES,
    CONF_PRICE_PEAK_MIN_GAP_HOURS,
    DEFAULT_PRICE_PEAK_MIN_GAP_HOURS,
    CONF_PRICE_PEAK_MIN_PROMINENCE_PERCENT,
    DEFAULT_PRICE_PEAK_MIN_PROMINENCE_PERCENT,
    CONF_PRICE_WINDOW_DURATION_HOURS,
    CONF_PRICE_WINDOW_WHOLE_HOUR_START,
    DEFAULT_PRICE_WINDOW_TYPE,
    DEFAULT_PRICE_WINDOW_DURATION_HOURS,
    DEFAULT_PRICE_WINDOW_WHOLE_HOUR_START,
    DOMAIN,
    PLANNER_KIND_PRICE_WINDOW,
    PRICE_WINDOW_TYPE_MOST_EXPENSIVE,
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

    merged = {**entry.data, **entry.options}
    price_window_type = str(merged.get(CONF_PRICE_WINDOW_TYPE, DEFAULT_PRICE_WINDOW_TYPE))
    is_high_window = price_window_type == PRICE_WINDOW_TYPE_MOST_EXPENSIVE
    async_add_entities(
        [
            PriceWindowBinarySensor(
                coordinator,
                entry,
                key="selected_price_window",
                name="High Price Window Active" if is_high_window else "Low Price Window Active",
                icon="mdi:cash-alert" if is_high_window else "mdi:cash-clock",
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
        icon: str,
    ) -> None:
        super().__init__(coordinator)
        self._entry = entry
        self._attr_name = f"{entry.title} {name}".strip()
        self._attr_unique_id = f"{entry.entry_id}_{key}"
        self._attr_icon = icon
        self._boundary_unsub = None

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        self.async_on_remove(self._cancel_boundary)
        self._schedule_boundary()

    @callback
    def _cancel_boundary(self) -> None:
        if self._boundary_unsub is not None:
            self._boundary_unsub()
            self._boundary_unsub = None

    @callback
    def _schedule_boundary(self) -> None:
        self._cancel_boundary()
        now = dt_util.now()
        windows = self._peak_windows if self._is_peak_planner else [self._window or {}]
        boundaries = [at for window in windows for key in ("start", "end")
                      if (at := _parse_datetime(window.get(key))) is not None and at > now]
        if boundaries:
            self._boundary_unsub = async_track_point_in_time(self.hass, self._boundary_reached, min(boundaries))

    @callback
    def _boundary_reached(self, _now) -> None:
        self._boundary_unsub = None
        self.async_write_ha_state()
        self._schedule_boundary()

    @callback
    def _handle_coordinator_update(self) -> None:
        self._schedule_boundary()
        super()._handle_coordinator_update()

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
        tomorrow_cheapest_window = self._tomorrow_cheapest_window or {}
        merged = {**self._entry.data, **self._entry.options}
        return {
            "planner_kind": self.coordinator.data.planner_kind,
            "price_window_type": self.coordinator.data.price_window_type,
            "start": window.get("start"),
            "end": window.get("end"),
            "average_price": window.get("average_price"),
            "duration_hours": window.get("duration_hours"),
            "planned_price_peak_windows": self._peak_windows,
            "minimum_peak_duration_minutes": merged.get(
                CONF_PRICE_PEAK_MIN_DURATION_MINUTES, DEFAULT_PRICE_PEAK_MIN_DURATION_MINUTES),
            "minimum_gap_hours": merged.get(CONF_PRICE_PEAK_MIN_GAP_HOURS, DEFAULT_PRICE_PEAK_MIN_GAP_HOURS),
            "minimum_peak_prominence_percent": merged.get(
                CONF_PRICE_PEAK_MIN_PROMINENCE_PERCENT, DEFAULT_PRICE_PEAK_MIN_PROMINENCE_PERCENT,
            ),
            "remove_small_pending_blocks": True,
            "tomorrow_cheapest_price_window": self._tomorrow_cheapest_window,
            "tomorrow_cheapest_price_window_start": tomorrow_cheapest_window.get("start"),
            "tomorrow_cheapest_price_window_end": tomorrow_cheapest_window.get("end"),
            "tomorrow_cheapest_price_window_average_price": tomorrow_cheapest_window.get("average_price"),
            "tomorrow_cheapest_price_window_duration_hours": tomorrow_cheapest_window.get("duration_hours"),
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
        if self._is_peak_planner:
            now = dt_util.now()
            return next((w for w in self._peak_windows
                         if (end := _parse_datetime(w.get("end"))) is not None and end > now), None)
        value = getattr(self.coordinator.data, "selected_price_window", None)
        return value if isinstance(value, dict) else None

    @property
    def _is_peak_planner(self) -> bool:
        return self.coordinator.data.price_window_type == PRICE_WINDOW_TYPE_MOST_EXPENSIVE

    @property
    def _peak_windows(self) -> list[dict]:
        return getattr(self.coordinator.data, "planned_price_peak_windows", None) or []

    @property
    def _tomorrow_cheapest_window(self) -> dict[str, str | float] | None:
        value = getattr(self.coordinator.data, "tomorrow_cheapest_price_window", None)
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
