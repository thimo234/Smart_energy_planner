"""Battery solar and demand forecast helpers."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime, timedelta
from typing import Any, cast

from .battery_models import SolarWindow


DEFAULT_BATTERY_DEMAND_SAFETY_MARGIN = 0.20
HOURS_PER_WEEK = 7 * 24


def build_hourly_home_demand_forecast(
    *,
    non_heating_daily_average_kwh: float,
    heating_estimate_kwh: float,
    hourly_demand_table: dict[str, float] | None = None,
    demand_adjustment_factor: float = 1.0,
    horizon_end: datetime | None = None,
) -> list[dict[str, str | float]]:
    """Build an hourly demand forecast from the 168-slot demand profile."""

    fallback_profile = _fallback_hourly_demand_profile(non_heating_daily_average_kwh)
    table = hourly_demand_table or {}
    adjustment_factor = min(1.35, max(0.75, demand_adjustment_factor))

    heating_profile = [
        0.035, 0.03, 0.03, 0.03, 0.035, 0.045,
        0.06, 0.07, 0.06, 0.045, 0.035, 0.03,
        0.025, 0.025, 0.025, 0.03, 0.04, 0.055,
        0.07, 0.075, 0.065, 0.05, 0.04, 0.035,
    ]
    profile_sum = sum(heating_profile) or 1.0
    now = datetime.now().astimezone()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    horizon_end = horizon_end or (today_start + timedelta(days=1))
    day_count = max(1, (horizon_end.date() - today_start.date()).days + 1)

    forecast: list[dict[str, str | float]] = []
    for day_offset in range(day_count):
        day_start = today_start + timedelta(days=day_offset)
        raw_non_heating_by_hour: list[float] = []
        for hour in range(24):
            slot_start = day_start + timedelta(hours=hour)
            slot_key = str(slot_start.weekday() * 24 + hour)
            raw_non_heating_by_hour.append(
                _hourly_demand_value(
                    table=table,
                    slot_key=slot_key,
                    weekday=slot_start.weekday(),
                    hour=hour,
                    fallback_hourly=fallback_profile[hour],
                )
            )

        day_adjustment_factor = adjustment_factor if day_offset == 0 else 1.0
        target_non_heating_kwh = max(0.0, non_heating_daily_average_kwh * day_adjustment_factor)
        raw_non_heating_total = sum(raw_non_heating_by_hour)
        if target_non_heating_kwh > 0 and raw_non_heating_total > 0:
            non_heating_scale = target_non_heating_kwh / raw_non_heating_total
        else:
            non_heating_scale = 1.0

        for hour, raw_non_heating_hourly in enumerate(raw_non_heating_by_hour):
            slot_start = day_start + timedelta(hours=hour)
            heating_hourly = heating_estimate_kwh * (heating_profile[hour] / profile_sum)
            non_heating_hourly = max(0.0, raw_non_heating_hourly * non_heating_scale)
            total_hourly = round(non_heating_hourly + heating_hourly, 3)
            forecast.append(
                {
                    "start": slot_start.isoformat(),
                    "end": (slot_start + timedelta(hours=1)).isoformat(),
                    "estimated_kwh": total_hourly,
                }
            )

    return forecast


def align_price_responsive_demand_to_cheap_hours(
    hourly_demand: list[dict[str, str | float]],
    price_windows: list[Any],
) -> list[dict[str, str | float]]:
    """Move learned flexible demand peaks to the cheapest matching day hours."""

    if len(hourly_demand) < 3 or len(price_windows) < 3:
        return hourly_demand

    demand_by_day: dict[date, list[dict[str, str | float]]] = {}
    for slot in hourly_demand:
        slot_start = _parse_datetime(slot.get("start"))
        if slot_start is None:
            continue
        demand_by_day.setdefault(slot_start.date(), []).append(slot)

    adjusted = [dict(slot) for slot in hourly_demand]
    adjusted_by_start = {
        str(slot.get("start")): slot
        for slot in adjusted
    }
    for day_slots in demand_by_day.values():
        priced_slots: list[tuple[dict[str, str | float], datetime, datetime, float, float]] = []
        for slot in day_slots:
            slot_start = _parse_datetime(slot.get("start"))
            slot_end = _parse_datetime(slot.get("end"))
            demand_kwh = _coerce_float(slot.get("estimated_kwh"), default=0.0) or 0.0
            if slot_start is None or slot_end is None or demand_kwh <= 0:
                continue
            price = _optional_window_price(
                start=slot_start,
                end=slot_end,
                windows=price_windows,
            )
            if price is not None:
                priced_slots.append((slot, slot_start, slot_end, demand_kwh, price))

        if len(priced_slots) < 3:
            continue

        demand_values = [demand_kwh for _, _, _, demand_kwh, _ in priced_slots]
        median_demand = _median(demand_values)
        high_values = sorted(demand_values)
        upper_quartile = high_values[int(len(high_values) * 0.75)]
        peak_threshold = max(median_demand + 0.25, upper_quartile)
        flexible_by_start = {
            slot_start: max(0.0, demand_kwh - peak_threshold)
            for _, slot_start, _, demand_kwh, _ in priced_slots
        }
        flexible_kwh = sum(flexible_by_start.values())
        max_flexible_kwh = sum(demand_values) * 0.35
        flexible_kwh = min(flexible_kwh, max_flexible_kwh)
        if flexible_kwh <= 0.05:
            continue

        source_total = sum(flexible_by_start.values())
        if source_total <= 0:
            continue

        target_count = max(1, min(len(priced_slots), sum(1 for value in flexible_by_start.values() if value > 0)))
        cheapest_slots = sorted(priced_slots, key=lambda item: (item[4], item[1]))[:target_count]
        per_target_kwh = flexible_kwh / len(cheapest_slots)

        for slot, slot_start, _, demand_kwh, _ in priced_slots:
            slot_key = slot_start.isoformat()
            adjusted_slot = adjusted_by_start.get(str(slot.get("start")))
            if adjusted_slot is None:
                continue
            source_flexible = flexible_by_start.get(slot_start, 0.0)
            removed_kwh = flexible_kwh * (source_flexible / source_total) if source_flexible > 0 else 0.0
            adjusted_slot["estimated_kwh"] = round(max(0.0, demand_kwh - removed_kwh), 3)
            adjusted_by_start[slot_key] = adjusted_slot

        for slot, slot_start, _, _, _ in cheapest_slots:
            adjusted_slot = adjusted_by_start.get(str(slot.get("start")))
            if adjusted_slot is None:
                continue
            adjusted_slot["estimated_kwh"] = round(
                float(adjusted_slot.get("estimated_kwh", 0.0)) + per_target_kwh,
                3,
            )

    return adjusted


def _optional_window_price(
    *,
    start: datetime,
    end: datetime,
    windows: list[Any],
) -> float | None:
    weighted_price = 0.0
    weighted_hours = 0.0
    for window in windows:
        overlap_hours = _overlap_hours(start, end, window.start, window.end)
        if overlap_hours <= 0:
            continue
        weighted_price += float(window.price) * overlap_hours
        weighted_hours += overlap_hours

    if weighted_hours <= 0:
        return None
    return round(weighted_price / weighted_hours, 6)


def observed_hourly_demand_table(
    table: dict[str, Any] | None,
    observed_slots: Iterable[str] | None,
) -> dict[str, float]:
    """Return the planned demand table values that are available."""

    if not table:
        return {}

    observed_keys = {str(slot) for slot in (observed_slots or [])}
    if not observed_keys:
        observed_keys = set(table.keys())

    return {
        slot: value
        for slot in observed_keys
        if (value := _coerce_float(table.get(slot))) is not None and value >= 0
    }


def update_expected_hourly_demand_stats(
    stats: dict[str, dict[str, Any]],
    *,
    slot_key: str,
    measured_kwh: float,
) -> dict[str, dict[str, Any]]:
    """Update compact per-slot demand statistics with one completed hour."""

    measured_value = _coerce_float(measured_kwh)
    if measured_value is None or measured_value < 0:
        return stats

    updated = dict(stats)
    raw_slot = stats.get(slot_key, {})
    count = int(_coerce_float(raw_slot.get("count"), default=0.0) or 0)
    previous_mean = _coerce_float(raw_slot.get("mean_kwh"))

    if previous_mean is None:
        mean_kwh = measured_value
    else:
        alpha = 0.45 if count < 3 else 0.25 if count < 12 else 0.15
        mean_kwh = previous_mean + ((measured_value - previous_mean) * alpha)

    updated[slot_key] = {
        "count": count + 1,
        "mean_kwh": round(mean_kwh, 4),
    }
    return updated


def build_expected_hourly_demand_table(
    stats: dict[str, dict[str, Any]] | None,
    *,
    daily_average_kwh: float,
) -> dict[str, float]:
    """Build a 168-slot expected demand profile from compact learned stats."""

    stats = stats or {}
    fallback_profile = _fallback_hourly_demand_profile(daily_average_kwh)
    table: dict[str, float] = {}
    for slot_index in range(HOURS_PER_WEEK):
        weekday = slot_index // 24
        hour = slot_index % 24
        baseline = fallback_profile[hour]
        table[str(slot_index)] = round(
            _expected_hourly_demand_value(
                stats=stats,
                weekday=weekday,
                hour=hour,
                baseline=baseline,
            ),
            4,
        )
    return table


def _expected_hourly_demand_value(
    *,
    stats: dict[str, dict[str, Any]],
    weekday: int,
    hour: int,
    baseline: float,
) -> float:
    slot_key = str(weekday * 24 + hour)
    exact = _stat_mean(stats.get(slot_key))
    similar_weekdays = [5, 6] if weekday >= 5 else [0, 1, 2, 3, 4]
    similar_values = _stat_means_for_hour(stats, hour=hour, weekdays=similar_weekdays)
    all_hour_values = _stat_means_for_hour(stats, hour=hour, weekdays=range(7))

    if exact is not None:
        peer_values = _stat_means_for_hour(
            stats,
            hour=hour,
            weekdays=[peer_weekday for peer_weekday in similar_weekdays if peer_weekday != weekday],
        )
        peer = _median(peer_values) if len(peer_values) >= 2 else None
        if peer is not None and baseline > 0:
            return max(0.0, (exact * 0.65) + (peer * 0.20) + (baseline * 0.15))
        if baseline > 0:
            return max(0.0, (exact * 0.80) + (baseline * 0.20))
        return max(0.0, exact)

    if len(similar_values) >= 2:
        peer = _median(similar_values)
        return max(0.0, (peer * 0.75) + (baseline * 0.25)) if baseline > 0 else max(0.0, peer)

    if len(all_hour_values) >= 2:
        peer = _median(all_hour_values)
        return max(0.0, (peer * 0.65) + (baseline * 0.35)) if baseline > 0 else max(0.0, peer)

    return max(0.0, baseline)


def _stat_mean(raw_stat: dict[str, Any] | None) -> float | None:
    if not raw_stat:
        return None
    count = int(_coerce_float(raw_stat.get("count"), default=0.0) or 0)
    if count <= 0:
        return None
    value = _coerce_float(raw_stat.get("mean_kwh"))
    return value if value is not None and value >= 0 else None


def _stat_means_for_hour(
    stats: dict[str, dict[str, Any]],
    *,
    hour: int,
    weekdays: Iterable[int],
) -> list[float]:
    return [
        value
        for weekday in weekdays
        if (value := _stat_mean(stats.get(str(weekday * 24 + hour)))) is not None
    ]


def _hourly_demand_value(
    *,
    table: dict[str, float],
    slot_key: str,
    weekday: int,
    hour: int,
    fallback_hourly: float,
) -> float:
    """Return the best historical hourly demand estimate for a forecast slot."""

    table_value = _coerce_float(table.get(slot_key))
    if table_value is not None:
        return min(table_value, 3.0)

    forecast_is_weekend = weekday >= 5
    similar_day_values = _same_hour_values(
        table,
        hour=hour,
        weekdays=[5, 6] if forecast_is_weekend else [0, 1, 2, 3, 4],
    )
    if similar_day_values:
        return min(_median(similar_day_values), 3.0)

    same_hour_values = _same_hour_values(table, hour=hour, weekdays=range(7))
    if same_hour_values:
        return min(_median(same_hour_values), 3.0)

    return fallback_hourly


def _fallback_hourly_demand_profile(daily_kwh: float) -> list[float]:
    """Return a non-flat default home demand profile scaled to the daily total."""

    if daily_kwh <= 0:
        return [0.0] * 24
    weights = [
        0.036, 0.031, 0.029, 0.029, 0.031, 0.035,
        0.044, 0.053, 0.063, 0.072, 0.078, 0.073,
        0.068, 0.068, 0.072, 0.067, 0.058, 0.050,
        0.046, 0.046, 0.050, 0.054, 0.053, 0.044,
    ]
    total_weight = sum(weights) or 1.0
    return [daily_kwh * weight / total_weight for weight in weights]


def _same_hour_values(
    table: dict[str, float],
    *,
    hour: int,
    weekdays: Iterable[int],
) -> list[float]:
    return [
        value
        for weekday in weekdays
        if (value := _coerce_float(table.get(str(weekday * 24 + hour)))) is not None
    ]


def build_expected_hourly_demand_table_from_observations(
    table: dict[str, float],
    *,
    observed_slots: Iterable[str] | None = None,
    daily_average_kwh: float = 0.0,
) -> dict[str, float]:
    """Build an expected table from legacy observed slots."""

    observed_keys = set(observed_slots or table.keys())
    observed_table = {
        slot: value
        for slot in observed_keys
        if (value := _coerce_float(table.get(slot))) is not None and value >= 0
    }
    if not observed_table:
        return {
            slot: value
            for slot, raw_value in table.items()
            if (value := _coerce_float(raw_value)) is not None and value >= 0
        }

    source_table = _stabilize_sparse_observed_table(observed_table)
    stats: dict[str, dict[str, Any]] = {}
    for slot_key, value in source_table.items():
        stats = update_expected_hourly_demand_stats(stats, slot_key=slot_key, measured_kwh=value)
    return build_expected_hourly_demand_table(stats, daily_average_kwh=daily_average_kwh)


def _stabilize_sparse_observed_table(table: dict[str, float]) -> dict[str, float]:
    stabilized = dict(table)
    for slot_key, value in table.items():
        try:
            slot_index = int(slot_key)
        except ValueError:
            continue
        if not 0 <= slot_index < HOURS_PER_WEEK:
            continue

        weekday = slot_index // 24
        hour = slot_index % 24
        similar_weekdays = [5, 6] if weekday >= 5 else [0, 1, 2, 3, 4]
        peer_values = [
            peer_value
            for peer_weekday in similar_weekdays
            if (peer_key := str(peer_weekday * 24 + hour)) != slot_key
            and (peer_value := _coerce_float(table.get(peer_key))) is not None
        ]
        if not peer_values:
            peer_values = [
                peer_value
                for peer_weekday in range(7)
                if (peer_key := str(peer_weekday * 24 + hour)) != slot_key
                and (peer_value := _coerce_float(table.get(peer_key))) is not None
            ]
        if not peer_values:
            continue

        peer_median = _median(peer_values)
        sparse_high_threshold = max(1.5, peer_median * 2.75)
        if value > sparse_high_threshold:
            stabilized[slot_key] = round(peer_median, 4)

    return stabilized


def _median(values: list[float]) -> float:
    sorted_values = sorted(values)
    midpoint = len(sorted_values) // 2
    if len(sorted_values) % 2:
        return sorted_values[midpoint]
    return (sorted_values[midpoint - 1] + sorted_values[midpoint]) / 2


def get_solar_day_end(solar_windows: list[SolarWindow]) -> datetime | None:
    productive_windows = [window for window in solar_windows if window.forecast_kwh > 0]
    if not productive_windows:
        return None
    return max(window.end for window in productive_windows)


def build_fallback_solar_windows(daily_forecast_kwh: float) -> list[SolarWindow]:
    """Approximate hourly solar windows when only the daily forecast total is available."""

    return build_fallback_solar_windows_for_day(daily_forecast_kwh, day_offset=0)


def build_fallback_solar_windows_for_day(
    daily_forecast_kwh: float,
    *,
    day_offset: int,
) -> list[SolarWindow]:
    """Approximate hourly solar windows when only the daily forecast total is available."""

    if daily_forecast_kwh <= 0:
        return []

    now = datetime.now().astimezone()
    day_start = (now + timedelta(days=day_offset)).replace(hour=0, minute=0, second=0, microsecond=0)
    hourly_weights = [
        0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
        0.0, 0.02, 0.05, 0.09, 0.13, 0.16,
        0.17, 0.15, 0.11, 0.07, 0.04, 0.01,
        0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
    ]
    weight_sum = sum(hourly_weights) or 1.0
    windows: list[SolarWindow] = []

    for hour, weight in enumerate(hourly_weights):
        if weight <= 0:
            continue
        start = day_start + timedelta(hours=hour)
        end = start + timedelta(hours=1)
        if day_offset == 0 and end <= now:
            continue
        forecast_kwh = round(daily_forecast_kwh * (weight / weight_sum), 3)
        windows.append(
            SolarWindow(
                start=start,
                end=end,
                forecast_kwh=forecast_kwh,
                forecast_kwh_p10=None,
                forecast_kwh_p90=None,
            )
        )

    return windows


def extract_solar_windows(
    attributes: dict[str, Any],
    *,
    include_past: bool = False,
) -> list[SolarWindow]:
    raw_entries = attributes.get("detailedHourly", [])
    windows: list[SolarWindow] = []
    for entry in raw_entries:
        start_raw = entry.get("period_start")
        if not isinstance(start_raw, str):
            continue
        start = _parse_datetime(start_raw)
        if start is None:
            continue
        end = start + timedelta(hours=1)
        if not include_past and end <= datetime.now().astimezone():
            continue
        windows.append(
            SolarWindow(
                start=start,
                end=end,
                forecast_kwh=_coerce_float(entry.get("pv_estimate"), default=0.0) or 0.0,
                forecast_kwh_p10=_coerce_float(entry.get("pv_estimate10")),
                forecast_kwh_p90=_coerce_float(entry.get("pv_estimate90")),
            )
        )
    return merge_solar_windows(windows)


def merge_solar_windows(windows: list[SolarWindow]) -> list[SolarWindow]:
    """Deduplicate solar windows by time range and keep the strongest forecast."""

    merged: dict[tuple[datetime, datetime], SolarWindow] = {}
    for window in sorted(windows, key=lambda item: item.start):
        key = (window.start, window.end)
        previous = merged.get(key)
        if previous is None:
            merged[key] = window
            continue
        merged[key] = SolarWindow(
            start=window.start,
            end=window.end,
            forecast_kwh=max(previous.forecast_kwh, window.forecast_kwh),
            forecast_kwh_p10=window.forecast_kwh_p10
            if window.forecast_kwh_p10 is not None
            else previous.forecast_kwh_p10,
            forecast_kwh_p90=window.forecast_kwh_p90
            if window.forecast_kwh_p90 is not None
            else previous.forecast_kwh_p90,
        )

    return sorted(merged.values(), key=lambda item: item.start)


def select_best_solar_window(windows: list[SolarWindow]) -> SolarWindow | None:
    productive_windows = [window for window in windows if window.forecast_kwh > 0]
    if not productive_windows:
        return None
    return max(productive_windows, key=lambda item: item.forecast_kwh)


def sum_remaining_solar_until(
    solar_windows: list[SolarWindow],
    now: datetime,
    until: datetime | None,
) -> float:
    if until is None:
        return 0.0
    total = 0.0
    for window in solar_windows:
        overlap_hours = _overlap_hours(window.start, window.end, now, until)
        if overlap_hours <= 0:
            continue
        window_hours = max((window.end - window.start).total_seconds() / 3600, 0.0001)
        total += window.forecast_kwh * (overlap_hours / window_hours)
    return total


def sum_remaining_home_demand_until(
    hourly_demand: list[dict[str, str | float]],
    now: datetime,
    until: datetime | None,
) -> float:
    if until is None:
        return 0.0
    total = 0.0
    for slot in hourly_demand:
        start = _parse_datetime(slot.get("start"))
        end = _parse_datetime(slot.get("end"))
        estimated_kwh = _coerce_float(slot.get("estimated_kwh"), default=0.0) or 0.0
        if start is None or end is None:
            continue
        overlap_hours = _overlap_hours(start, end, now, until)
        if overlap_hours <= 0:
            continue
        slot_hours = max((end - start).total_seconds() / 3600, 0.0001)
        total += estimated_kwh * (overlap_hours / slot_hours)
    return total


def remaining_day_solar_covers_demand(
    *,
    slots: list[dict[str, Any]],
    start: datetime,
) -> bool:
    day_end = (start + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    remaining_solar_kwh = 0.0
    remaining_demand_kwh = 0.0

    for slot in slots:
        slot_start = cast(datetime, slot["start"])
        slot_end = cast(datetime, slot["end"])
        overlap_hours = _overlap_hours(slot_start, slot_end, start, day_end)
        if overlap_hours <= 0:
            continue
        slot_hours = max((slot_end - slot_start).total_seconds() / 3600, 0.0001)
        remaining_solar_kwh += float(slot["solar_kwh"]) * (overlap_hours / slot_hours)
        remaining_demand_kwh += float(slot["demand_kwh"]) * (overlap_hours / slot_hours)

    return remaining_solar_kwh >= remaining_demand_kwh - 1e-6


def build_energy_balance_slots(
    *,
    price_windows: list[Any],
    export_price_windows: list[Any],
    solar_windows: list[SolarWindow],
    hourly_demand: list[dict[str, str | float]],
    horizon_start: datetime,
    demand_safety_margin: float = DEFAULT_BATTERY_DEMAND_SAFETY_MARGIN,
) -> list[dict[str, Any]]:
    """Build battery planning slots and apply a demand safety margin.

    The margin is applied before net solar is calculated, so both charge planning
    and discharge planning account for the same extra expected home demand.
    """

    demand_multiplier = 1.0 + max(0.0, min(1.0, float(demand_safety_margin)))
    slots: list[dict[str, Any]] = []
    for window in price_windows:
        if window.end <= horizon_start:
            continue
        slot_hours = max((window.end - window.start).total_seconds() / 3600, 0.0001)
        demand_kwh = 0.0
        for demand_slot in hourly_demand:
            demand_start = _parse_datetime(demand_slot.get("start"))
            demand_end = _parse_datetime(demand_slot.get("end"))
            estimated_kwh = _coerce_float(demand_slot.get("estimated_kwh"), default=0.0) or 0.0
            if demand_start is None or demand_end is None:
                continue
            overlap_hours = _overlap_hours(window.start, window.end, demand_start, demand_end)
            if overlap_hours <= 0:
                continue
            demand_slot_hours = max((demand_end - demand_start).total_seconds() / 3600, 0.0001)
            demand_kwh += estimated_kwh * (overlap_hours / demand_slot_hours)

        demand_kwh *= demand_multiplier

        solar_kwh = 0.0
        for solar_window in solar_windows:
            overlap_hours = _overlap_hours(window.start, window.end, solar_window.start, solar_window.end)
            if overlap_hours <= 0:
                continue
            solar_slot_hours = max((solar_window.end - solar_window.start).total_seconds() / 3600, 0.0001)
            solar_kwh += float(solar_window.forecast_kwh) * (overlap_hours / solar_slot_hours)
        net_solar_kwh = round(solar_kwh - demand_kwh, 3)
        export_price = match_window_price(
            start=window.start,
            end=window.end,
            windows=export_price_windows,
            default=window.price,
        )
        slots.append(
            {
                "start": window.start,
                "end": window.end,
                "price": window.price,
                "import_price": window.price,
                "export_price": export_price,
                "hours": slot_hours,
                "solar_kwh": round(solar_kwh, 3),
                "demand_kwh": round(demand_kwh, 3),
                "net_solar_kwh": net_solar_kwh,
            }
        )

    return slots


def match_window_price(
    *,
    start: datetime,
    end: datetime,
    windows: list[Any],
    default: float,
) -> float:
    weighted_price = 0.0
    weighted_hours = 0.0
    for window in windows:
        overlap_hours = _overlap_hours(start, end, window.start, window.end)
        if overlap_hours <= 0:
            continue
        weighted_price += float(window.price) * overlap_hours
        weighted_hours += overlap_hours

    if weighted_hours <= 0:
        return round(default, 6)
    return round(weighted_price / weighted_hours, 6)


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


def _coerce_float(value: Any, default: float | None = None) -> float | None:
    if value in (None, "unknown", "unavailable", ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None
