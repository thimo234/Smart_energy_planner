"""Battery planning helpers for Smart Energy Planner.

This module is intentionally small and independent so future battery planning
changes can be made without rewriting the large coordinator.py file.
"""

from __future__ import annotations

from typing import Any


BATTERY_MODE_OFF = "accu_uit"
BATTERY_MODE_GRID_CHARGE = "laden_van_net"
BATTERY_MODE_SOLAR_CHARGE = "laden_met_zonne_energie"
BATTERY_MODE_DISCHARGE = "ontladen"
BATTERY_MODE_EXPORT = "ontladen_naar_net"

CHARGE_MODES = {BATTERY_MODE_GRID_CHARGE, BATTERY_MODE_SOLAR_CHARGE}
DISCHARGE_MODES = {BATTERY_MODE_DISCHARGE, BATTERY_MODE_EXPORT}


def battery_mode_family(mode: str) -> str:
    """Return the broad mode family used for battery cycle summaries."""

    if mode in CHARGE_MODES:
        return "laden"
    if mode in DISCHARGE_MODES:
        return "ontladen"
    return BATTERY_MODE_OFF


def should_hold_full_battery_on_solar(
    *,
    mode: str,
    usable_energy_kwh: float,
    usable_capacity_kwh: float,
    tolerance_kwh: float = 0.01,
) -> bool:
    """Return true when a full battery should not be commanded to grid-charge.

    A full battery can stay in solar-charge mode so the inverter remains ready
    to accept PV passively, but it should not stay in grid-charge mode until the
    next discharge phase starts.
    """

    return (
        mode == BATTERY_MODE_GRID_CHARGE
        and usable_capacity_kwh > 0
        and usable_energy_kwh >= usable_capacity_kwh - max(0.0, tolerance_kwh)
    )


def normalize_full_battery_charge_mode(
    *,
    mode: str,
    usable_energy_kwh: float,
    usable_capacity_kwh: float,
) -> str:
    """Convert full-battery grid charge to solar hold mode."""

    if should_hold_full_battery_on_solar(
        mode=mode,
        usable_energy_kwh=usable_energy_kwh,
        usable_capacity_kwh=usable_capacity_kwh,
    ):
        return BATTERY_MODE_SOLAR_CHARGE
    return mode


def clamp_charge_safety_margin(value: Any, *, default: float = 0.0) -> float:
    """Convert a percentage safety margin to a 0.0-0.5 multiplier."""

    try:
        margin_percent = float(value)
    except (TypeError, ValueError):
        margin_percent = default
    return max(0.0, min(0.5, margin_percent / 100.0))
