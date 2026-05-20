"""Thermostat planning helpers for Smart Energy Planner.

This module is intentionally small and independent so future thermostat planning
changes can be made without rewriting the large coordinator.py file.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, cast


HEAT_PUMP_STRATEGY_NORMAL = "normal"
HEAT_PUMP_STRATEGY_ECO = "energy_saving_on"
HEAT_PUMP_STRATEGY_PREHEAT = "preheating"
HEAT_PUMP_STRATEGY_NOT_APPLICABLE = "not_applicable"

THERMOSTAT_PRESET_NORMAL = "normal"
THERMOSTAT_PRESET_PREHEAT = "preheat"
THERMOSTAT_PRESET_ECO = "eco"

THERMOSTAT_ECO_MERGE_GAP = timedelta(hours=1)
THERMOSTAT_FALLBACK_COOLING_FACTOR = 0.04
THERMOSTAT_MIN_FALLBACK_COOLDOWN_HOURS = 2.0
THERMOSTAT_MAX_COOLDOWN_HOURS = 18.0
THERMOSTAT_COOLING_LEARN_SAMPLES = 3


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


def estimate_cooling_profile_from_model(
    *,
    cooling_model: dict[str, Any],
    outdoor_temperature_c: float,
    room_temperature_c: float,
    cooldown_delta_c: float,
) -> tuple[float, float]:
    """Estimate room cooling rate and duration from fallback and learned model data."""

    learned_factor = _coerce_float(cooling_model.get("rolling_cooling_factor"))
    learned_samples = int(_coerce_float(cooling_model.get("eco_sample_count"), default=0.0) or 0)
    last_eco_duration_hours = _coerce_float(cooling_model.get("last_eco_duration_hours"))
    delta_temp = max(room_temperature_c - outdoor_temperature_c, 1.0)

    fallback_rate = max(0.03, delta_temp * THERMOSTAT_FALLBACK_COOLING_FACTOR)
    fallback_hours = min(
        THERMOSTAT_MAX_COOLDOWN_HOURS,
        max(THERMOSTAT_MIN_FALLBACK_COOLDOWN_HOURS, cooldown_delta_c / fallback_rate),
    )

    if learned_factor is not None and learned_samples > 0:
        delta_temp = max(room_temperature_c - outdoor_temperature_c, 0.5)
        clamped_factor = min(learned_factor, 10.0 * THERMOSTAT_FALLBACK_COOLING_FACTOR)
        learned_rate = max(0.03, min(2.0, clamped_factor * delta_temp))
        blend = min(1.0, learned_samples / float(THERMOSTAT_COOLING_LEARN_SAMPLES))
        estimated_rate = (fallback_rate * (1.0 - blend)) + (learned_rate * blend)
        estimated_hours = min(
            THERMOSTAT_MAX_COOLDOWN_HOURS,
            max(1.0, cooldown_delta_c / max(estimated_rate, 0.03)),
        )
        if last_eco_duration_hours is not None:
            estimated_hours = max(
                estimated_hours,
                min(THERMOSTAT_MAX_COOLDOWN_HOURS, last_eco_duration_hours),
            )
        return estimated_rate, estimated_hours

    return fallback_rate, fallback_hours


def find_next_valley_start(
    windows: list[Any],
    now: datetime,
    average_price: float,
) -> datetime | None:
    """Find the next cheap valley after at least one expensive window."""

    future = sorted(
        [window for window in windows if window.end > now],
        key=lambda window: window.start,
    )
    n = len(future)
    saw_expensive = False
    index = 0
    while index < n:
        if not saw_expensive:
            if future[index].price > average_price:
                saw_expensive = True
            index += 1
            continue

        price = future[index].price
        if price > average_price:
            index += 1
            continue

        prev_price = future[index - 1].price if index > 0 else float("inf")
        if prev_price <= price:
            index += 1
            continue

        plateau_end = index
        while plateau_end + 1 < n and future[plateau_end + 1].price == price:
            plateau_end += 1

        next_price = future[plateau_end + 1].price if plateau_end + 1 < n else float("inf")
        if next_price <= price:
            index = plateau_end + 1
            continue
        return future[index].start

    return None


def select_most_expensive_window_block(
    *,
    windows: list[Any],
    now: datetime,
    duration_hours: float,
) -> dict[str, datetime | float] | None:
    """Select the highest-average contiguous block for a fixed duration."""

    if duration_hours <= 0:
        return None

    eligible_windows = [window for window in windows if window.end > now]
    if not eligible_windows:
        return None

    best_block: dict[str, datetime | float] | None = None
    for start_index, start_window in enumerate(eligible_windows):
        block_start = max(start_window.start, now)
        accumulated_hours = 0.0
        weighted_price = 0.0
        block_end = block_start

        for window in eligible_windows[start_index:]:
            usable_start = max(window.start, block_end if block_end > window.start else window.start)
            usable_end = window.end
            usable_hours = (usable_end - usable_start).total_seconds() / 3600
            if usable_hours <= 0:
                continue
            take_hours = min(usable_hours, duration_hours - accumulated_hours)
            weighted_price += window.price * take_hours
            accumulated_hours += take_hours
            block_end = usable_start + timedelta(hours=take_hours)
            if accumulated_hours >= duration_hours:
                average_price = weighted_price / accumulated_hours
                candidate = {"start": block_start, "end": block_end, "average_price": average_price}
                if best_block is None or average_price > float(best_block["average_price"]):
                    best_block = candidate
                break

    return best_block


def select_expensive_peak_blocks(
    *,
    windows: list[Any],
    now: datetime,
    duration_hours: float,
    expensive_threshold: float,
) -> list[dict[str, datetime | float]]:
    """Select one fixed-duration block for each distinct expensive price peak."""

    eligible_windows = [
        window
        for window in windows
        if window.end > now and window.price >= expensive_threshold
    ]
    if not eligible_windows:
        return []

    grouped_windows = _group_contiguous_windows(eligible_windows)

    peak_blocks: list[dict[str, datetime | float]] = []
    for group in grouped_windows:
        group_start = max(group[0].start, now)
        group_end = group[-1].end
        group_hours = max((group_end - group_start).total_seconds() / 3600, 0.0)
        if group_hours <= 0:
            continue

        if duration_hours > 0 and group_hours > duration_hours:
            selected = select_most_expensive_window_block(
                windows=group,
                now=now,
                duration_hours=duration_hours,
            )
            if selected is not None:
                peak_blocks.append(selected)
            continue

        weighted_price = 0.0
        total_hours = 0.0
        for window in group:
            usable_start = max(window.start, now)
            usable_hours = max((window.end - usable_start).total_seconds() / 3600, 0.0)
            if usable_hours <= 0:
                continue
            weighted_price += window.price * usable_hours
            total_hours += usable_hours
        if total_hours <= 0:
            continue
        peak_blocks.append(
            {
                "start": group_start,
                "end": group_end,
                "average_price": weighted_price / total_hours,
            }
        )

    return sorted(peak_blocks, key=lambda item: item["start"])


def select_thermostat_peak_eco_windows(
    *,
    windows: list[Any],
    now: datetime,
    cooldown_hours: float,
    expensive_threshold: float,
) -> list[dict[str, datetime | float]]:
    """Plan stable eco windows around expensive peaks."""

    if cooldown_hours <= 0:
        return []

    lookback_start = now - timedelta(hours=cooldown_hours)
    eligible_windows = [
        window
        for window in windows
        if window.end > lookback_start and window.price >= expensive_threshold
    ]
    if not eligible_windows:
        return []

    grouped_windows = _group_contiguous_windows(eligible_windows)

    eco_windows: list[dict[str, datetime | float]] = []
    for group in grouped_windows:
        group_start = group[0].start
        group_end = group[-1].end
        if group_end <= now:
            continue

        group_hours = (group_end - group_start).total_seconds() / 3600
        if group_hours <= 0:
            continue

        if group_hours > cooldown_hours:
            best_block: dict[str, datetime | float] | None = None
            for start_index, start_window in enumerate(group):
                block_start = start_window.start
                accumulated_hours = 0.0
                weighted_price = 0.0
                block_end = block_start
                for window in group[start_index:]:
                    usable_start = max(window.start, block_end)
                    usable_hours = (window.end - usable_start).total_seconds() / 3600
                    if usable_hours <= 0:
                        continue
                    take_hours = min(usable_hours, cooldown_hours - accumulated_hours)
                    weighted_price += window.price * take_hours
                    accumulated_hours += take_hours
                    block_end = usable_start + timedelta(hours=take_hours)
                    if accumulated_hours >= cooldown_hours:
                        average_price = weighted_price / accumulated_hours
                        if best_block is None or average_price > float(best_block["average_price"]):
                            best_block = {
                                "start": block_start,
                                "end": block_end,
                                "average_price": average_price,
                            }
                        break
            if best_block is None:
                continue
            eco_start = cast(datetime, best_block["start"])
            eco_end = cast(datetime, best_block["end"])
            avg_price_val = float(best_block["average_price"])
        else:
            weighted_price = 0.0
            total_hours = 0.0
            for window in group:
                window_hours = (window.end - window.start).total_seconds() / 3600
                weighted_price += window.price * window_hours
                total_hours += window_hours
            if total_hours <= 0:
                continue
            eco_start = group_start
            eco_end = group_end
            avg_price_val = weighted_price / total_hours

        if eco_end <= now:
            continue

        eco_windows.append({"start": eco_start, "end": eco_end, "average_price": avg_price_val})

    if not eco_windows:
        return []

    merged_windows: list[dict[str, datetime | float]] = []
    for window in sorted(eco_windows, key=lambda item: item["start"]):
        if not merged_windows:
            merged_windows.append(window)
            continue
        previous = merged_windows[-1]
        if window["start"] <= previous["end"] + THERMOSTAT_ECO_MERGE_GAP:
            previous["end"] = max(previous["end"], window["end"])
            previous["average_price"] = max(
                float(previous["average_price"]),
                float(window["average_price"]),
            )
        else:
            merged_windows.append(window)

    return merged_windows


def _group_contiguous_windows(windows: list[Any]) -> list[list[Any]]:
    grouped_windows: list[list[Any]] = []
    current_group: list[Any] = []
    for window in windows:
        if not current_group:
            current_group = [window]
            continue
        if window.start <= current_group[-1].end:
            current_group.append(window)
        else:
            grouped_windows.append(current_group)
            current_group = [window]
    if current_group:
        grouped_windows.append(current_group)
    return grouped_windows


def _coerce_float(value: Any, default: float | None = None) -> float | None:
    if value in (None, "unknown", "unavailable", ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
