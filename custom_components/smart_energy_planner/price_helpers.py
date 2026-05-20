"""Price window parsing and shaping helpers."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from .const import PRICE_RESOLUTION_HOURLY
from .price_models import PlannerWindow


def extract_price_windows(
    attributes: dict[str, Any],
    current_price: float | None,
    price_resolution: str,
    *,
    include_past: bool = False,
    now: datetime | None = None,
) -> list[PlannerWindow]:
    now = now or datetime.now().astimezone()
    raw_entries = list(attributes.get("raw_today", [])) + list(attributes.get("raw_tomorrow", []))
    windows: list[PlannerWindow] = []
    active_window: PlannerWindow | None = None

    for entry in raw_entries:
        start_raw = entry.get("start")
        end_raw = entry.get("end")
        price_raw = entry.get("value")
        if start_raw is None or end_raw is None or price_raw is None:
            continue
        try:
            start = _parse_datetime(start_raw)
            end = _parse_datetime(end_raw)
            price = float(price_raw)
        except (TypeError, ValueError):
            continue
        if start is None or end is None:
            continue
        if start <= now < end:
            active_window = PlannerWindow(start=start, end=end, price=price)
        if not include_past and end <= now:
            continue
        windows.append(PlannerWindow(start=start, end=end, price=price))

    if not windows:
        windows = extract_price_windows_from_series(attributes, now, include_past=include_past)

    if active_window and not any(w.start == active_window.start and w.end == active_window.end for w in windows):
        windows.insert(0, active_window)

    if not windows and current_price is not None:
        windows.append(PlannerWindow(start=now, end=now + timedelta(hours=1), price=current_price))

    if price_resolution == PRICE_RESOLUTION_HOURLY:
        windows = aggregate_price_windows_to_hourly(windows)

    return sorted(windows, key=lambda item: item.start)


def extract_price_average(
    attributes: dict[str, Any],
    windows: list[PlannerWindow],
) -> float | None:
    """Prefer the source sensor daily average, then fall back to the mean."""

    for key in ("average", "mean"):
        value = _coerce_float(attributes.get(key))
        if value is not None:
            return value
    if not windows:
        return None
    return sum(window.price for window in windows) / len(windows)


def extract_price_windows_from_series(
    attributes: dict[str, Any],
    now: datetime,
    *,
    include_past: bool = False,
) -> list[PlannerWindow]:
    """Build price windows from today/tomorrow lists when raw entries are unavailable."""

    today_values = attributes.get("today")
    tomorrow_values = attributes.get("tomorrow")
    if not isinstance(today_values, list):
        return []

    today_windows = series_to_price_windows(
        today_values,
        now.replace(hour=0, minute=0, second=0, microsecond=0),
    )
    tomorrow_windows: list[PlannerWindow] = []
    if isinstance(tomorrow_values, list) and tomorrow_values:
        tomorrow_start = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        tomorrow_windows = series_to_price_windows(tomorrow_values, tomorrow_start)

    if include_past:
        return [*today_windows, *tomorrow_windows]
    return [window for window in [*today_windows, *tomorrow_windows] if window.end > now]


def series_to_price_windows(
    values: list[Any],
    start_time: datetime,
) -> list[PlannerWindow]:
    """Convert a list of prices into planner windows."""

    if not values:
        return []

    interval_minutes = infer_series_interval_minutes(len(values))
    if interval_minutes is None:
        return []

    windows: list[PlannerWindow] = []
    interval = timedelta(minutes=interval_minutes)
    for index, raw_value in enumerate(values):
        price = _coerce_float(raw_value)
        if price is None:
            continue
        start = start_time + (interval * index)
        end = start + interval
        windows.append(PlannerWindow(start=start, end=end, price=price))
    return windows


def infer_series_interval_minutes(item_count: int) -> int | None:
    """Infer interval size from the number of prices in a daily list."""

    if item_count == 24:
        return 60
    if item_count == 48:
        return 30
    if item_count == 96:
        return 15
    return None


def build_neutral_price_windows(
    current_price: float | None,
    *,
    hours: int = 1,
    now: datetime | None = None,
) -> list[PlannerWindow]:
    """Build flat windows so planning can continue without price data."""

    now = now or datetime.now().astimezone()
    neutral_price = current_price if current_price is not None else 0.0
    window_count = max(1, hours)
    return [
        PlannerWindow(
            start=now + timedelta(hours=index),
            end=now + timedelta(hours=index + 1),
            price=neutral_price,
        )
        for index in range(window_count)
    ]


def extend_price_window_tail(
    *,
    windows: list[PlannerWindow],
    horizon_end: datetime,
    fallback_price: float | None,
) -> list[PlannerWindow]:
    if not windows:
        return windows

    extended_windows = sorted(windows, key=lambda item: item.start)
    last_window = extended_windows[-1]
    if last_window.end >= horizon_end:
        return extended_windows

    interval = last_window.end - last_window.start
    if interval <= timedelta(0):
        interval = timedelta(hours=1)
    fill_price = last_window.price if last_window.price is not None else (fallback_price or 0.0)
    tail_start = last_window.end
    while tail_start < horizon_end:
        tail_end = min(tail_start + interval, horizon_end)
        extended_windows.append(
            PlannerWindow(
                start=tail_start,
                end=tail_end,
                price=fill_price,
            )
        )
        tail_start = tail_end

    return extended_windows


def aggregate_price_windows_to_hourly(windows: list[PlannerWindow]) -> list[PlannerWindow]:
    if not windows:
        return windows
    grouped: dict[datetime, list[PlannerWindow]] = {}
    for window in windows:
        hour_start = window.start.replace(minute=0, second=0, microsecond=0)
        grouped.setdefault(hour_start, []).append(window)

    aggregated: list[PlannerWindow] = []
    for hour_start, grouped_windows in grouped.items():
        grouped_windows = sorted(grouped_windows, key=lambda item: item.start)
        aggregated.append(
            PlannerWindow(
                start=hour_start,
                end=max(window.end for window in grouped_windows),
                price=round(sum(window.price for window in grouped_windows) / len(grouped_windows), 6),
            )
        )
    return sorted(aggregated, key=lambda item: item.start)


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None
    return datetime.fromisoformat(value)


def _coerce_float(value: Any, default: float | None = None) -> float | None:
    if value in (None, "unknown", "unavailable", ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
