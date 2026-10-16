"""Find successive local price peaks and keep committed, bounded on windows."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import math

from .price_models import PlannerWindow

STATE_KEY = "price_peak_state"


def _average(windows, start, end):
    cursor, cost = start, 0.0
    for window in windows:
        a, b = max(start, window.start), min(end, window.end)
        if b <= a:
            continue
        if a != cursor:  # Missing or overlapping source intervals are not a forecast.
            return None
        cost += window.price * (b - a).total_seconds()
        cursor = b
    return cost / (end - start).total_seconds() if cursor == end else None


def plan_price_peaks(windows, *, now, duration_hours, whole_hour_start, minimum_gap_hours=1.0, saved=None):
    """Return persisted state; only complete known peaks can create new blocks.

    A flat summit is one peak, confirmed by lower prices on both sides.
    Commit peaks in time order, then search strictly after the preceding block.
    The configured OFF time (at least 15 minutes) prevents concatenated ON time.
    """
    saved = saved if isinstance(saved, dict) else {}
    local_zone = now.tzinfo
    now = now.astimezone(timezone.utc)
    def stamp(value):
        return value.astimezone(local_zone).isoformat()
    duration = timedelta(hours=duration_hours)
    gap = timedelta(hours=max(0.25, minimum_gap_hours))
    if duration <= timedelta(0):
        return {"windows": [], "last_end": saved.get("last_end")}
    def parse(value):
        try:
            result = datetime.fromisoformat(value)
            return result.astimezone(timezone.utc) if result.tzinfo is not None else None
        except (TypeError, ValueError):
            return None
    committed = []
    last_end = parse(saved.get("last_end"))
    for item in saved.get("windows", []):
        if not isinstance(item, dict):
            continue
        start, end = parse(item.get("start")), parse(item.get("end"))
        if start is None or end is None or end <= start:
            continue
        # A shorter configured maximum may cut short an old commitment, never extend it.
        end = min(end, start + duration)
        last_end = max(last_end, end) if last_end else end
        if end > now:
            committed.append({**item, "end": stamp(end),
                              "duration_hours": (end - start).total_seconds() / 3600})

    known = sorted((PlannerWindow(w.start.astimezone(timezone.utc), w.end.astimezone(timezone.utc), w.price)
                    for w in windows if w.price_known and math.isfinite(w.price)
                    and w.end.timestamp() > w.start.timestamp()), key=lambda w: w.start)
    groups = []
    for window in known:
        if groups and groups[-1].end == window.start and groups[-1].price == window.price:
            groups[-1] = PlannerWindow(groups[-1].start, window.end, window.price)
        else:
            groups.append(window)
    peaks = [top for left, top, right in zip(groups, groups[1:], groups[2:])
             if left.end == top.start and top.end == right.start
             and top.price > left.price and top.price > right.price]
    # Starting at an interval boundary also makes the maximum reproducible at each refresh.
    starts = sorted({w.start for w in known
                     if not whole_hour_start or (w.start.astimezone(local_zone).minute == 0 and w.start.second == 0)})
    for peak in peaks:
        peak_at = peak.start + (peak.end - peak.start) / 2
        if peak_at < now or (last_end and peak_at <= last_end):
            continue
        candidates = []
        for start in starts:
            end = start + duration
            if start < now or not start <= peak_at < end or (last_end and start < last_end + gap):
                continue
            average = _average(known, start, end)
            if average is not None:
                candidates.append((average, start, end))
        if not candidates:
            continue
        average, start, end = max(candidates, key=lambda c: (c[0], -c[1].timestamp()))
        committed.append({"start": stamp(start), "end": stamp(end),
                          "average_price": round(average, 6), "duration_hours": duration_hours,
                          "peak_at": stamp(peak_at), "peak_price": peak.price})
        last_end = end
    return {"windows": sorted(committed, key=lambda w: datetime.fromisoformat(w["start"]).timestamp()),
            "last_end": stamp(last_end) if last_end else None}
