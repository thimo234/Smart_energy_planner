"""Battery planning helpers for Smart Energy Planner.

This module is intentionally small and independent so future battery planning
changes can be made without rewriting the large coordinator.py file.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, cast


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


def normalize_full_battery_mode_windows(
    *,
    windows: list[dict[str, str | float]],
    usable_energy_kwh: float,
    usable_capacity_kwh: float,
) -> list[dict[str, str | float]]:
    """Convert grid-charge mode windows to solar hold mode when the battery is full."""

    normalized_windows: list[dict[str, str | float]] = []
    for window in windows:
        normalized = dict(window)
        normalized["mode"] = normalize_full_battery_charge_mode(
            mode=str(window.get("mode", BATTERY_MODE_OFF)),
            usable_energy_kwh=usable_energy_kwh,
            usable_capacity_kwh=usable_capacity_kwh,
        )
        normalized_windows.append(normalized)
    return normalized_windows


def collapse_short_off_mode_windows(
    windows: list[dict[str, str | float]],
    *,
    max_duration: timedelta = timedelta(minutes=30),
) -> list[dict[str, str | float]]:
    """Fold tiny off gaps into the adjacent active battery mode.

    Replanning often creates a short idle sliver just before an upcoming active
    window.  The next refresh usually removes it anyway, so prefer the adjacent
    active mode immediately when the off period is small enough.
    """

    if not windows:
        return []

    collapsed: list[dict[str, str | float]] = []
    ordered_windows = sorted(windows, key=lambda item: str(item.get("start", "")))
    for index, window in enumerate(ordered_windows):
        collapsed_window = dict(window)
        mode = str(collapsed_window.get("mode", BATTERY_MODE_OFF))
        start = _parse_datetime(collapsed_window.get("start"))
        end = _parse_datetime(collapsed_window.get("end"))

        if mode == BATTERY_MODE_OFF and start is not None and end is not None and end > start:
            duration = end - start
            if duration <= max_duration:
                previous_mode = _neighbor_active_mode(ordered_windows, index - 1)
                next_mode = _neighbor_active_mode(ordered_windows, index + 1)
                replacement_mode = next_mode or previous_mode
                if replacement_mode is not None:
                    collapsed_window["mode"] = replacement_mode

        collapsed.append(collapsed_window)

    return merge_windows(collapsed, same_mode_only=True, pick_max_price=True)


def clamp_charge_safety_margin(value: Any, *, default: float = 0.0) -> float:
    """Convert a percentage safety margin to a 0.0-0.5 multiplier."""

    try:
        margin_percent = float(value)
    except (TypeError, ValueError):
        margin_percent = default
    return max(0.0, min(0.5, margin_percent / 100.0))


def calculate_next_battery_peak_price(
    slots: list[dict[str, Any]],
    after: datetime,
    *,
    price_key: str = "price",
) -> float | None:
    """Return the next local peak price after a charge candidate."""

    trailing_slots = [slot for slot in slots if slot["start"] >= after]
    if len(trailing_slots) < 2:
        return None

    prices = [float(slot[price_key]) for slot in trailing_slots]
    index = 0
    while index + 1 < len(prices) and prices[index + 1] <= prices[index]:
        index += 1

    peak_max = prices[index]
    while index + 1 < len(prices) and prices[index + 1] >= prices[index]:
        index += 1
        peak_max = max(peak_max, prices[index])

    return peak_max


def build_charge_window_lookup(
    windows: list[dict[str, str | float]],
    *,
    max_charge_kw: float,
) -> dict[datetime, dict[str, float | datetime]]:
    """Index planned charge windows by parsed start time."""

    lookup: dict[datetime, dict[str, float | datetime]] = {}
    for window in windows:
        parsed_start = _parse_datetime(window.get("start"))
        parsed_end = _parse_datetime(window.get("end"))
        if parsed_start is None or parsed_end is None:
            continue
        usable_hours = float(window.get("usable_hours", 0.0))
        charge_kwh = float(window.get("charge_kwh", usable_hours * max_charge_kw))
        if max_charge_kw > 0 and 0 < charge_kwh < usable_hours * max_charge_kw:
            usable_hours = charge_kwh / max_charge_kw
            parsed_end = parsed_start + timedelta(hours=usable_hours)
        lookup[parsed_start] = {
            "end": parsed_end,
            "usable_hours": round(usable_hours, 6),
            "charge_kwh": round(max(0.0, charge_kwh), 6),
        }
    return lookup


def select_contiguous_productive_solar_slot_starts(
    *,
    slots: list[dict[str, Any]],
    max_charge_kw: float,
    minimum_slots: int,
) -> set[datetime]:
    """Return solar slot starts that belong to productive contiguous runs."""

    if minimum_slots <= 1:
        return {
            cast(datetime, slot["start"])
            for slot in slots
            if min(max_charge_kw * float(slot["hours"]), max(0.0, float(slot["net_solar_kwh"]))) > 0
        }

    productive_starts: set[datetime] = set()
    current_run: list[dict[str, Any]] = []
    previous_end: datetime | None = None

    for slot in slots:
        charge_potential_kwh = min(
            max_charge_kw * float(slot["hours"]),
            max(0.0, float(slot["net_solar_kwh"])),
        )
        slot_start = cast(datetime, slot["start"])
        slot_end = cast(datetime, slot["end"])
        if charge_potential_kwh <= 0:
            if len(current_run) >= minimum_slots:
                productive_starts.update(cast(datetime, run_slot["start"]) for run_slot in current_run)
            current_run = []
            previous_end = None
            continue

        if previous_end is not None and slot_start != previous_end:
            if len(current_run) >= minimum_slots:
                productive_starts.update(cast(datetime, run_slot["start"]) for run_slot in current_run)
            current_run = []

        current_run.append(slot)
        previous_end = slot_end

    if len(current_run) >= minimum_slots:
        productive_starts.update(cast(datetime, run_slot["start"]) for run_slot in current_run)

    return productive_starts


def build_battery_cycle_windows(
    windows: list[dict[str, str | float]],
) -> list[dict[str, str | datetime | float]]:
    """Collapse adjacent mode windows into broader charge/discharge/idle cycles."""

    cycles: list[dict[str, str | datetime | float]] = []
    for window in sorted(windows, key=lambda item: str(item["start"])):
        start = _parse_datetime(window.get("start"))
        end = _parse_datetime(window.get("end"))
        if start is None or end is None:
            continue

        family = battery_mode_family(str(window.get("mode", BATTERY_MODE_OFF)))
        if cycles and cast(datetime, cycles[-1]["end"]) == start and str(cycles[-1]["family"]) == family:
            cycles[-1]["end"] = end
            cycles[-1]["usable_hours"] = round(
                float(cycles[-1].get("usable_hours", 0.0)) + float(window.get("usable_hours", 0.0)),
                3,
            )
            continue

        cycles.append(
            {
                "start": start,
                "end": end,
                "family": family,
                "usable_hours": round(float(window.get("usable_hours", 0.0)), 3),
            }
        )

    return cycles


def select_relevant_battery_cycle(
    *,
    cycle_windows: list[dict[str, str | datetime | float]],
    now: datetime,
) -> dict[str, str | datetime | float] | None:
    """Return the active or next non-idle battery cycle."""

    current_cycle = next(
        (
            cycle
            for cycle in cycle_windows
            if cast(datetime, cycle["start"]) <= now < cast(datetime, cycle["end"])
        ),
        None,
    )
    if current_cycle is not None and str(current_cycle["family"]) != BATTERY_MODE_OFF:
        return current_cycle

    next_relevant_cycle = next(
        (
            cycle
            for cycle in cycle_windows
            if cast(datetime, cycle["start"]) > now and str(cycle["family"]) != BATTERY_MODE_OFF
        ),
        None,
    )
    return next_relevant_cycle or current_cycle


def find_battery_cycle(
    *,
    cycle_windows: list[dict[str, str | datetime | float]],
    now: datetime,
    family: str,
) -> dict[str, str | datetime | float] | None:
    """Find the next cycle for a family."""

    return next(
        (
            cycle
            for cycle in cycle_windows
            if str(cycle["family"]) == family and cast(datetime, cycle["end"]) > now
        ),
        None,
    )


def find_next_idle_start(
    *,
    cycle_windows: list[dict[str, str | datetime | float]],
    now: datetime,
) -> datetime | None:
    """Return the next idle cycle start."""

    next_idle_cycle = next(
        (
            cycle
            for cycle in cycle_windows
            if str(cycle["family"]) == BATTERY_MODE_OFF and cast(datetime, cycle["start"]) > now
        ),
        None,
    )
    if next_idle_cycle is None:
        return None
    return cast(datetime, next_idle_cycle["start"])


def summarize_battery_cycles(
    *,
    full_planned_mode_windows: list[dict[str, str | float]],
    energy_balance_slots: list[dict[str, Any]],
    now: datetime,
) -> dict[str, str | float | None]:
    """Build the battery cycle summary exposed by sensors."""

    cycle_windows = build_battery_cycle_windows(full_planned_mode_windows)
    relevant_cycle = select_relevant_battery_cycle(cycle_windows=cycle_windows, now=now)
    next_charge_cycle = find_battery_cycle(cycle_windows=cycle_windows, now=now, family="laden")
    following_charge_cycle = next(
        (
            cycle
            for cycle in cycle_windows
            if str(cycle["family"]) == "laden"
            and next_charge_cycle is not None
            and cast(datetime, cycle["start"]) >= cast(datetime, next_charge_cycle["end"])
        ),
        None,
    )
    next_discharge_cycle = find_battery_cycle(cycle_windows=cycle_windows, now=now, family="ontladen")
    next_idle_start = find_next_idle_start(cycle_windows=cycle_windows, now=now)

    relevant_start = cast(datetime, relevant_cycle["start"]) if relevant_cycle is not None else None
    relevant_end = cast(datetime, relevant_cycle["end"]) if relevant_cycle is not None else None

    return {
        "current_relevant_battery_window_start": relevant_start.isoformat() if relevant_start else None,
        "current_relevant_battery_window_end": relevant_end.isoformat() if relevant_end else None,
        "current_relevant_battery_window_mode": str(relevant_cycle["family"]) if relevant_cycle is not None else None,
        "current_relevant_battery_window_expected_demand_kwh": (
            sum_slot_metric_in_window(
                slots=energy_balance_slots,
                start=relevant_start,
                end=relevant_end,
                metric_key="demand_kwh",
            )
            if relevant_start is not None and relevant_end is not None
            else 0.0
        ),
        "current_relevant_battery_window_expected_solar_kwh": (
            sum_slot_metric_in_window(
                slots=energy_balance_slots,
                start=relevant_start,
                end=relevant_end,
                metric_key="solar_kwh",
            )
            if relevant_start is not None and relevant_end is not None
            else 0.0
        ),
        "next_charge_window_start": (
            cast(datetime, next_charge_cycle["start"]).isoformat() if next_charge_cycle is not None else None
        ),
        "next_charge_window_end": (
            cast(datetime, next_charge_cycle["end"]).isoformat() if next_charge_cycle is not None else None
        ),
        "next_charge_window_hours": (
            round(float(next_charge_cycle.get("usable_hours", 0.0)), 3) if next_charge_cycle is not None else 0.0
        ),
        "following_charge_window_start": (
            cast(datetime, following_charge_cycle["start"]).isoformat() if following_charge_cycle is not None else None
        ),
        "following_charge_window_end": (
            cast(datetime, following_charge_cycle["end"]).isoformat() if following_charge_cycle is not None else None
        ),
        "following_charge_window_hours": (
            round(float(following_charge_cycle.get("usable_hours", 0.0)), 3)
            if following_charge_cycle is not None
            else 0.0
        ),
        "next_discharge_window_start": (
            cast(datetime, next_discharge_cycle["start"]).isoformat() if next_discharge_cycle is not None else None
        ),
        "next_discharge_window_end": (
            cast(datetime, next_discharge_cycle["end"]).isoformat() if next_discharge_cycle is not None else None
        ),
        "next_discharge_window_hours": (
            round(float(next_discharge_cycle.get("usable_hours", 0.0)), 3)
            if next_discharge_cycle is not None
            else 0.0
        ),
        "next_idle_window_start": next_idle_start.isoformat() if next_idle_start is not None else None,
    }


def sum_slot_metric_in_window(
    *,
    slots: list[dict[str, Any]],
    start: datetime,
    end: datetime,
    metric_key: str,
) -> float:
    """Sum a slot metric across an arbitrary time window."""

    total = 0.0
    for slot in slots:
        slot_start = cast(datetime, slot["start"])
        slot_end = cast(datetime, slot["end"])
        overlap_hours = _overlap_hours(slot_start, slot_end, start, end)
        if overlap_hours <= 0:
            continue
        slot_hours = max((slot_end - slot_start).total_seconds() / 3600, 0.0001)
        total += float(slot.get(metric_key, 0.0)) * (overlap_hours / slot_hours)
    return round(total, 3)


def build_battery_mode_schedule(
    *,
    planning_start: datetime,
    full_planned_mode_windows: list[dict[str, str | float]],
) -> list[dict[str, str]]:
    """Build a de-duplicated battery mode transition schedule."""

    schedule = [{"at": planning_start.isoformat(), "mode": BATTERY_MODE_OFF}]
    schedule.extend(
        {"at": str(window["start"]), "mode": str(window.get("mode", BATTERY_MODE_OFF))}
        for window in full_planned_mode_windows
    )

    deduped_schedule: list[dict[str, str]] = []
    for item in sorted(schedule, key=lambda entry: entry["at"]):
        if deduped_schedule and deduped_schedule[-1]["at"] == item["at"]:
            deduped_schedule[-1] = item
            continue
        if deduped_schedule and deduped_schedule[-1]["mode"] == item["mode"]:
            continue
        deduped_schedule.append(item)
    return deduped_schedule


def plan_segment_discharge_kwh(
    *,
    slots: list[dict[str, Any]],
    available_energy_kwh: float,
    max_discharge_kw: float,
) -> dict[datetime, float]:
    """Plan discharge across deficit slots, prioritizing expensive slots."""

    if available_energy_kwh <= 0 or max_discharge_kw <= 0 or not slots:
        return {}

    deficit_slots = [
        {
            "start": slot["start"],
            "price": float(slot["import_price"]),
            "required_kwh": min(
                max_discharge_kw * float(slot["hours"]),
                -float(slot["net_solar_kwh"]),
            ),
        }
        for slot in slots
        if float(slot["net_solar_kwh"]) < 0
        and min(max_discharge_kw * float(slot["hours"]), -float(slot["net_solar_kwh"])) > 0
    ]

    if not deficit_slots:
        return {}

    total_required_kwh = sum(float(slot["required_kwh"]) for slot in deficit_slots)
    if available_energy_kwh >= total_required_kwh:
        return {cast(datetime, slot["start"]): round(float(slot["required_kwh"]), 6) for slot in deficit_slots}

    remaining_energy_kwh = available_energy_kwh
    planned_discharge: dict[datetime, float] = {}
    for slot in sorted(deficit_slots, key=lambda item: (-float(item["price"]), item["start"])):
        if remaining_energy_kwh <= 0:
            break
        assigned_kwh = min(float(slot["required_kwh"]), remaining_energy_kwh)
        if assigned_kwh <= 0:
            continue
        planned_discharge[cast(datetime, slot["start"])] = round(assigned_kwh, 6)
        remaining_energy_kwh -= assigned_kwh
    return planned_discharge


def merge_planned_windows(
    windows: list[dict[str, str | float]],
) -> list[dict[str, str | float]]:
    """Merge adjacent planned windows without requiring the same mode."""

    return merge_windows(windows, same_mode_only=False, pick_max_price=False)


def merge_windows(
    windows: list[dict[str, str | float]],
    *,
    same_mode_only: bool,
    pick_max_price: bool,
) -> list[dict[str, str | float]]:
    """Merge adjacent windows and combine their usable hours."""

    if not windows:
        return []

    merged: list[dict[str, str | float]] = []
    for window in sorted(windows, key=lambda item: str(item["start"])):
        if not merged:
            merged.append(dict(window))
            continue

        previous = merged[-1]
        previous_end = _parse_datetime(previous.get("end"))
        current_start = _parse_datetime(window.get("start"))
        if (
            previous_end is not None
            and current_start is not None
            and previous_end == current_start
            and (not same_mode_only or previous.get("mode") == window.get("mode"))
        ):
            previous["end"] = window["end"]
            previous["usable_hours"] = round(
                float(previous.get("usable_hours", 0.0)) + float(window.get("usable_hours", 0.0)),
                3,
            )
            if "charge_kwh" in previous or "charge_kwh" in window:
                previous["charge_kwh"] = round(
                    float(previous.get("charge_kwh", 0.0)) + float(window.get("charge_kwh", 0.0)),
                    6,
                )
            prev_price = float(previous.get("price", 0.0))
            curr_price = float(window.get("price", 0.0))
            previous["price"] = max(prev_price, curr_price) if pick_max_price else min(prev_price, curr_price)
            continue

        merged.append(dict(window))

    return merged


def select_battery_discharge_windows(
    *,
    windows: list[Any],
    now: datetime,
    after: datetime | None,
    average_price: float,
) -> list[dict[str, str | float]]:
    """Select discharge windows after a given moment."""

    if after is None:
        return []

    discharge_windows = [
        {
            "start": window.start.isoformat(),
            "end": window.end.isoformat(),
            "price": round(window.price, 6),
            "usable_hours": round(max((window.end - max(window.start, now)).total_seconds() / 3600, 0.0), 3),
            "mode": BATTERY_MODE_DISCHARGE,
        }
        for window in windows
        if window.end > now and window.start >= after and window.price >= average_price
    ]
    return merge_planned_windows(discharge_windows)


def mark_discharge_window_modes(
    discharge_windows: list[dict[str, str | float]],
    charge_windows: list[dict[str, str | float]],
) -> list[dict[str, str | float]]:
    """Mark discharge windows that lead directly into a charge cycle as export."""

    if not discharge_windows:
        return []

    charge_starts = sorted(
        charge_start
        for window in charge_windows
        if (charge_start := _parse_datetime(window.get("start"))) is not None
    )

    marked: list[dict[str, str | float]] = []
    for window in discharge_windows:
        window_end = _parse_datetime(window.get("end"))
        mode = BATTERY_MODE_DISCHARGE
        if window_end is not None:
            next_charge_start = next((start for start in charge_starts if start >= window_end), None)
            if next_charge_start is not None and (next_charge_start - window_end).total_seconds() <= 90 * 60:
                mode = BATTERY_MODE_EXPORT

        marked.append({**window, "mode": mode})

    return marked


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _neighbor_active_mode(
    windows: list[dict[str, str | float]],
    index: int,
) -> str | None:
    if not 0 <= index < len(windows):
        return None
    mode = str(windows[index].get("mode", BATTERY_MODE_OFF))
    return mode if mode != BATTERY_MODE_OFF else None


def _overlap_hours(
    start_a: datetime,
    end_a: datetime,
    start_b: datetime,
    end_b: datetime | None,
) -> float:
    if end_b is None:
        return 0.0
    overlap_start = max(start_a, start_b)
    overlap_end = min(end_a, end_b)
    if overlap_end <= overlap_start:
        return 0.0
    return (overlap_end - overlap_start).total_seconds() / 3600
