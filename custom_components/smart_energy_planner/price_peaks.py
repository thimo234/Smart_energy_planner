"""Find successive local price peaks and keep committed, bounded on windows."""
from __future__ import annotations

from datetime import datetime, time, timedelta, timezone
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


def _significant_peaks(groups, known, local_zone, minimum_percent):
    """Measure a summit above the higher of its two surrounding valley floors.

    Stop at a higher summit or a data gap, so small ripples on the flank of
    a larger peak do not inherit that larger peak's height. Scale the threshold
    to the known max-minus-min price on the summit's local calendar day.
    """
    day_prices = {}
    for window in known:
        day_prices.setdefault(window.start.astimezone(local_zone).date(), []).append(window.price)
    peaks = []
    for i in range(1, len(groups)-1):
        left, top, right = groups[i-1:i+2]
        if (left.end != top.start or top.end != right.start
                or top.price <= left.price or top.price <= right.price):
            continue
        floors = []
        for direction in (-1, 1):
            floor, cursor, j = top.price, i, i+direction
            while 0 <= j < len(groups):
                neighbor, previous = groups[j], groups[cursor]
                contiguous = neighbor.end == previous.start if direction < 0 else previous.end == neighbor.start
                if not contiguous or neighbor.price > top.price:
                    break
                floor = min(floor, neighbor.price)
                cursor, j = j, j+direction
            floors.append(floor)
        peak_at = top.start + (top.end-top.start)/2
        values = day_prices.get(peak_at.astimezone(local_zone).date(), [top.price])
        span = max(values)-min(values)
        prominence = top.price-max(floors)
        if prominence + 1e-10 >= span*max(0.0, min(100.0, minimum_percent))/100:
            peaks.append((top, prominence, span))
    return peaks


def plan_price_peaks(windows, *, now, duration_hours, whole_hour_start, minimum_gap_hours=1.0,
                     minimum_prominence_percent=10.0, remove_small_pending=True, saved=None):
    """Return persisted state; only complete known peaks can create new blocks.

    A flat summit is one peak, confirmed by lower prices on both sides.
    Prefer more prominent peaks when new blocks compete for the required pause.
    Existing commitments remain fixed; return new blocks in time order.
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
    known = sorted((PlannerWindow(w.start.astimezone(timezone.utc), w.end.astimezone(timezone.utc), w.price)
                    for w in windows if w.price_known and math.isfinite(w.price)
                    and w.end.timestamp() > w.start.timestamp()), key=lambda w: w.start)
    groups = []
    for window in known:
        if groups and groups[-1].end == window.start and groups[-1].price == window.price:
            groups[-1] = PlannerWindow(groups[-1].start, window.end, window.price)
        else:
            groups.append(window)
    peaks = _significant_peaks(groups, known, local_zone, minimum_prominence_percent)
    peak_times = [top.start+(top.end-top.start)/2 for top, _, _ in peaks]
    def normal_price(peak, prominence, span):
        return peak.price-prominence+span*max(0.0, min(100.0, minimum_prominence_percent))/100

    def early_end(start, end, peak_at, threshold):
        if peak_at is None or threshold is None or not start <= peak_at < end:
            return end
        # Never infer a return to normal across missing or overlapping tariffs.
        for window in known:
            if peak_at < window.start < end and window.price <= threshold + 1e-10:
                if _average(known, start, window.start) is not None:
                    return window.start
                return end
        return end

    def complete_days(start, end):
        day = start.astimezone(local_zone).date()
        last_day = (end-timedelta(microseconds=1)).astimezone(local_zone).date()
        while day <= last_day:
            a = datetime.combine(day, time(), local_zone).astimezone(timezone.utc)
            b = datetime.combine(day+timedelta(days=1), time(), local_zone).astimezone(timezone.utc)
            if _average(known, a, b) is None:
                return False
            day += timedelta(days=1)
        return True
    committed = []
    last_end = parse(saved.get("last_end"))
    last_finished = parse(saved.get("last_finished_end"))
    if last_end and last_end <= now:
        last_finished = max(last_finished, last_end) if last_finished else last_end
    removed = False
    for item in saved.get("windows", []):
        if not isinstance(item, dict):
            continue
        start, end = parse(item.get("start")), parse(item.get("end"))
        if start is None or end is None or end <= start:
            continue
        # Changing the maximum may shorten a commitment; it can never extend it.
        end = min(end, start + duration)
        peak_at = parse(item.get("peak_at"))
        threshold = item.get("normal_price_threshold")
        if threshold is None and peak_at is not None:
            matching = next(((p, prominence, span) for p, prominence, span in peaks
                             if p.start <= peak_at < p.end), None)
            if matching:
                threshold = normal_price(*matching)
        shortened = early_end(start, end, peak_at, threshold) if end > now else end
        # On migration an active block can stop now, never retroactively.
        if start <= now < end:
            shortened = max(now, shortened)
        if shortened < end:
            removed = True  # Rebuild the search boundary from actual ends.
            end = shortened
        item = {**item}
        if threshold is not None and end != parse(item.get("end")):
            item["normal_price_threshold"] = threshold
        if end != parse(item.get("end")):
            average = _average(known, start, end)
            if average is not None:
                item["average_price"] = round(average, 6)
        if end <= now:
            last_finished = max(last_finished, end) if last_finished else end
        elif (remove_small_pending and start > now and complete_days(start, end)
              and not any(start <= at < end for at in peak_times)):
            removed = True
            continue
        else:
            committed.append({**item, "end": stamp(end),
                              "duration_hours": (end-start).total_seconds()/3600})
        last_end = max(last_end, end) if last_end else end
    if removed:
        # Old versions saved only the final future end. Conservatively wait a
        # full gap from now when their last completed block cannot be recovered.
        last_end = max([parse(w["end"]) for w in committed] + [last_finished or now])
    # Starting at an interval boundary also makes the maximum reproducible at each refresh.
    starts = sorted({w.start for w in known
                     if not whole_hour_start or (w.start.astimezone(local_zone).minute == 0 and w.start.second == 0)})
    proposals = []
    for peak, prominence, span in peaks:
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
        threshold = normal_price(peak, prominence, span)
        end = early_end(start, end, peak_at, threshold)
        average = _average(known, start, end)
        proposals.append((prominence, start, end, {"start": stamp(start), "end": stamp(end),
                          "average_price": round(average, 6),
                          "duration_hours": (end-start).total_seconds()/3600,
                          "normal_price_threshold": threshold,
                          "peak_at": stamp(peak_at), "peak_price": peak.price,
                          "peak_prominence": round(prominence, 6),
                          "day_price_spread": round(span, 6)}))
    # A minor summit must not consume the OFF interval needed for a larger one.
    # Compare prominence, not absolute price (also works with negative tariffs).
    selected = []
    for prominence, start, end, block in sorted(proposals, key=lambda p: (-p[0], p[1])):
        if any(not (end + gap <= a or b + gap <= start) for a, b in selected):
            continue
        selected.append((start, end))
        committed.append(block)
        last_end = max(last_end, end) if last_end else end
    return {"windows": sorted(committed, key=lambda w: datetime.fromisoformat(w["start"]).timestamp()),
            "last_end": stamp(last_end) if last_end else None,
            "last_finished_end": stamp(last_finished) if last_finished else None}
