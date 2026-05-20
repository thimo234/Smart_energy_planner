"""Thermostat planning helpers for Smart Energy Planner.

This module is intentionally small and independent so future thermostat planning
changes can be made without rewriting the large coordinator.py file.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any


HEAT_PUMP_STRATEGY_NORMAL = "normal"
HEAT_PUMP_STRATEGY_ECO = "energy_saving_on"
HEAT_PUMP_STRATEGY_PREHEAT = "preheating"
HEAT_PUMP_STRATEGY_NOT_APPLICABLE = "not_applicable"

THERMOSTAT_PRESET_NORMAL = "normal"
THERMOSTAT_PRESET_PREHEAT = "preheat"
THERMOSTAT_PRESET_ECO = "eco"


def clamp_temperature(
    value: float | None,
    *,
    minimum: float,
    maximum: float,
    fallback: float | None = None,
) -> float | None:
    """Clamp a thermostat temperature to the configured range."""

    if value is None:
        value = fallback
    if value is None:
        return None
    return round(min(maximum, max(minimum, float(value))), 2)


def eco_temperature_for_setpoint(
    eco_temperature: float | None,
    *,
    setpoint: float | None,
    minimum: float,
    maximum: float,
) -> float | None:
    """Clamp eco temperature and keep it at or below the normal setpoint."""

    if eco_temperature is None:
        return None
    if setpoint is not None:
        eco_temperature = min(float(eco_temperature), float(setpoint))
    return clamp_temperature(eco_temperature, minimum=minimum, maximum=maximum)


def is_window_active(
    window: dict[str, Any] | None,
    now: datetime,
) -> bool:
    """Return true when now is inside a planner window."""

    if window is None:
        return False
    start = window.get("start")
    end = window.get("end")
    return isinstance(start, datetime) and isinstance(end, datetime) and start <= now < end


def build_preheat_window(
    *,
    eco_start: datetime,
    minutes: int,
    earliest_start: datetime,
    previous_eco_end: datetime | None = None,
) -> dict[str, datetime] | None:
    """Build a preheat window ending at the eco start."""

    if minutes <= 0:
        return None
    start = eco_start - timedelta(minutes=minutes)
    if previous_eco_end is not None:
        start = max(start, previous_eco_end)
    start = max(start, earliest_start)
    if start >= eco_start:
        return None
    return {"start": start, "end": eco_start}


def room_reached_eco_temperature(
    *,
    room_temperature_c: float | None,
    eco_setpoint_c: float | None,
) -> bool:
    """Return true when the room is already at or below eco temperature."""

    return (
        room_temperature_c is not None
        and eco_setpoint_c is not None
        and room_temperature_c <= eco_setpoint_c
    )


def remaining_cooling_hours(
    *,
    room_temperature_c: float | None,
    eco_setpoint_c: float | None,
    cooling_rate_c_per_hour: float | None,
    fallback_hours: float,
) -> float:
    """Estimate remaining cooling time from current room temperature."""

    if room_temperature_c is None or eco_setpoint_c is None:
        return max(0.0, fallback_hours)
    if room_temperature_c <= eco_setpoint_c:
        return 0.0
    if cooling_rate_c_per_hour is None or cooling_rate_c_per_hour <= 0:
        return max(0.0, fallback_hours)
    return max(0.0, (room_temperature_c - eco_setpoint_c) / cooling_rate_c_per_hour)
