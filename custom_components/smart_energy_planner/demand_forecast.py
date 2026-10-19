"""Independent appliance forecasts and the versioned battery input contract."""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
import math
from statistics import fmean

from .battery_forecast import _optional_window_price
from .price_helpers import select_contiguous_price_window


def statistics_hours(rows, *, now):
    """Convert consecutive Recorder hourly sums (requested in kWh) to energy.

    Recorder sums already account for meter resets. Missing hours and negative
    corrections are not silently spread across idle hours or treated as zero.
    """
    result = {}
    previous = None
    for row in sorted(rows, key=lambda r: r['start']):
        try:
            start = datetime.fromtimestamp(float(row['start']), timezone.utc)
            value = float(row['sum'])
            if not math.isfinite(value):
                raise ValueError
        except (KeyError, TypeError, ValueError, OverflowError):
            previous = None
            continue
        if previous:
            at, old = previous
            delta = value-old
            if start-at == timedelta(hours=1) and start+timedelta(hours=1) <= now and delta >= -.001:
                result[start] = max(0.0, delta)
        previous = (start, value)
    return result


def meter_hours(states, *, now):
    """Integrate signed total / reset-aware total_increasing meters into UTC hours.

    Gaps over 75 minutes are unknown, not zero. Duplicate timestamps retain the
    last state. Invalid samples interrupt coverage rather than being bridged.
    """
    points = {}
    for state in states:
        at = state.last_updated.astimezone(timezone.utc)
        if at > now:
            continue
        unit = state.attributes.get('unit_of_measurement')
        kind = state.attributes.get('state_class')
        try:
            value = float(state.state) / (1000 if unit == 'Wh' else 1)
            if unit not in ('Wh', 'kWh') or kind not in ('total', 'total_increasing') or not math.isfinite(value):
                value = None
        except (TypeError, ValueError):
            value = None
        points[at] = (value, kind)
    energy, coverage = defaultdict(float), defaultdict(float)
    points = sorted(points.items())
    for (a, (av, ak)), (b, (bv, bk)) in zip(points, points[1:]):
        seconds = (b-a).total_seconds()
        if av is None or bv is None or ak != bk or not 0 < seconds <= 4500:
            continue
        delta = bv-av
        if bk == 'total_increasing' and delta < 0:
            delta = bv
        cursor = a
        while cursor < b:
            hour = cursor.replace(minute=0, second=0, microsecond=0)
            end = min(b, hour+timedelta(hours=1))
            fraction = (end-cursor).total_seconds()
            energy[hour] += delta*fraction/seconds
            coverage[hour] += fraction
            cursor = end
    return {h: max(0.0, energy[h]) for h in energy
            if coverage[h] >= 3599.99 and energy[h] >= -.001
            and h+timedelta(hours=1) <= now}


def _days(hours, tz):
    days = defaultdict(dict)
    for hour, value in hours.items():
        local = hour.astimezone(tz)
        days[local.date()][local.hour] = value
    # Skip DST transition days: 23/25 physical hours do not form a 24-hour sample.
    return {d: [v[h] for h in range(24)] for d, v in days.items() if len(v) == 24
            and sum(h.astimezone(tz).date() == d for h in hours) == 24}


def _samples(days, day):
    previous = [(d, v) for d, v in sorted(days.items()) if d < day]
    same = [v for d, v in previous if d.weekday() == day.weekday()]
    return same[-4:] if len(same) >= 2 else [v for _, v in previous[-14:]]


def component_forecast(hours, rule, *, now, prices, days=3, duration=5, whole_hour=True):
    """Learn energy and duration; only the schedule is supplied by the user.

    A scheduled device's first sustained active hour is the historical anchor.
    Standby stays at its clock time; the mean excess curve moves with the anchor.
    This is an estimate from hourly energy, not cycle detection or device control.
    """
    history = _days(hours, now.tzinfo)
    if len(history) < 2:
        raise ValueError(f'insufficient_history: {len(history)} complete days from {len(hours)} hourly values; at least 2 complete days required')
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    # UTC grid preserves physical duration across DST changes.
    finish = start+timedelta(days=days)
    grid = []
    cursor = start.astimezone(timezone.utc)
    while cursor < finish:
        grid.append(cursor)
        cursor += timedelta(minutes=15)
    result = {t: 0.0 for t in grid}
    warnings = []

    def distribute(a, b, energy):
        seconds = b.timestamp()-a.timestamp()
        if seconds <= 0:
            return
        for t in grid:
            overlap = max(0.0, min(b.timestamp(), t.timestamp()+900)-max(a.timestamp(), t.timestamp()))
            result[t] += energy*overlap/seconds

    # Include yesterday's scheduled tail when it crosses midnight.
    for offset in range(-1, days):
        day_start = start+timedelta(days=offset)
        samples = _samples(history, day_start.date())
        if not samples:
            raise ValueError('insufficient_history')
        clock_profile = [fmean(v[h] for v in samples) for h in range(24)]
        timing = rule.get('timing', 'learned')
        anchor = None
        if timing == 'fixed':
            hh, mm = map(int, rule.get('start_time', '17:30:00').split(':')[:2])
            anchor = day_start.replace(hour=hh, minute=mm)
        elif timing == 'cheap':
            selected = select_contiguous_price_window(prices, now=day_start,
                duration_hours=duration, cheapest=True, whole_hour_start=whole_hour)
            if selected:
                a, b = datetime.fromisoformat(selected['start']), datetime.fromisoformat(selected['end'])
                if _optional_window_price(start=a, end=b, windows=prices) is not None:
                    anchor = a+timedelta(hours=float(rule.get('offset_hours', 0)))
            if anchor is None and offset >= 0:
                warnings.append(f'{day_start.date()}: historical_schedule_fallback')
        weekdays = rule.get('weekdays', list(range(7)))
        if timing != 'learned' and day_start.weekday() not in weekdays:
            clock_profile = [min(v) for v in samples]
            clock_profile = [fmean(clock_profile)]*24
            anchor = None
        if anchor is None:
            if offset >= 0:
                for h, value in enumerate(clock_profile):
                    a = day_start+timedelta(hours=h)
                    distribute(a, a+timedelta(hours=1), value)
            continue
        bases = [min(v) for v in samples]
        base = fmean(bases)
        shape = [0.0]*24
        for values, baseline in zip(samples, bases):
            excess = [max(0, v-baseline) for v in values]
            active = next((h for h, v in enumerate(excess) if v > max(.05, baseline*.5)), None)
            if active is None:
                # Low-amplitude days retain their measured energy at clock time.
                base += sum(excess)/24/len(samples)
                continue
            for h, value in enumerate(excess):
                shape[(h-active) % 24] += value/len(samples)
        if offset >= 0:
            for h in range(24):
                a = day_start+timedelta(hours=h)
                distribute(a, a+timedelta(hours=1), base)
        for h, value in enumerate(shape):
            a = anchor+timedelta(hours=h)
            distribute(a, a+timedelta(hours=1), value)
    return result, warnings


def build_demand_forecast(histories, devices, *, total_entity, now, prices,
                         duration=5, whole_hour=True, hourly_statistics=None):
    ids = [r['entity_id'] for r in devices]
    if len(ids) != len(set(ids)) or total_entity in ids:
        raise ValueError('duplicate_energy_source')
    hours = {entity: meter_hours(states, now=now) for entity, states in histories.items()}
    for entity, rows in (hourly_statistics or {}).items():
        hours.setdefault(entity, {}).update(statistics_hours(rows, now=now))
    rules = list(devices)
    accounting_warning = None
    total_only = False
    if total_entity:
        total = hours[total_entity]
        common = set(total)
        for entity in ids:
            common &= hours[entity].keys()
        residual = {t: total[t]-sum(hours[entity][t] for entity in ids) for t in common}
        bad = {t: v for t, v in residual.items() if v < -.05}
        bad_days = {t.astimezone(now.tzinfo).date() for t in bad}
        if bad:
            worst = min(bad, key=bad.get)
            accounting_warning = (f'appliance_energy_exceeds_total: {len(bad)} hours across '
                f'{len(bad_days)} days; maximum excess {-bad[worst]:.3f} kWh at {worst.isoformat()}')
            # Reject whole days across all components: never mix device samples
            # from an inconsistent day with a residual learned from another day.
            consistent = {t: v for t, v in residual.items()
                          if t.astimezone(now.tzinfo).date() not in bad_days}
            valid_days = set(_days(consistent, now.tzinfo))
            if len(valid_days) >= 2:
                hours = {entity: {t: v for t, v in values.items()
                                 if t.astimezone(now.tzinfo).date() in valid_days}
                         for entity, values in hours.items()}
                residual = {t: v for t, v in consistent.items()
                            if t.astimezone(now.tzinfo).date() in valid_days}
                accounting_warning += '; inconsistent days excluded from all components'
            else:
                # The confirmed whole-house meter already includes all devices.
                # Use it alone, preserving measured demand without double counting.
                rules = [{'entity_id': total_entity, 'timing': 'learned'}]
                total_only = True
                accounting_warning += '; household_total_only fallback; device schedules omitted'
        if not total_only:
            hours['remaining_home'] = {t: max(0, v) for t, v in residual.items()}
            rules.append({'entity_id': 'remaining_home', 'timing': 'learned'})
    if not rules:
        raise ValueError('no_energy_sources')
    components, aggregate, warnings = {}, defaultdict(float), {}
    if accounting_warning:
        warnings['energy_accounting'] = [accounting_warning]
    for rule in rules:
        entity = rule['entity_id']
        try:
            profile, notes = component_forecast(hours[entity], rule, now=now, prices=prices,
                                                duration=duration, whole_hour=whole_hour)
        except ValueError as err:
            raise ValueError(f'{entity}: {err}') from err
        components[entity] = profile
        warnings[entity] = notes
        for t, value in profile.items():
            aggregate[t] += value

    def serialize(profile):
        return [dict(start=t.astimezone(now.tzinfo).isoformat(),
                     end=(t+timedelta(minutes=15)).astimezone(now.tzinfo).isoformat(),
                     estimated_kwh=round(v, 6)) for t, v in sorted(profile.items())]
    return serialize(aggregate), {k: serialize(v) for k, v in components.items()}, warnings


def read_forecast(state, *, now, horizon_end):
    """Reject stale, malformed, overlapping or incomplete external forecasts."""
    if state is None or state.state in ('unknown', 'unavailable', ''):
        raise ValueError('forecast_unavailable')
    attrs = state.attributes
    if attrs.get('forecast_version') != 1 or attrs.get('unit_of_measurement') != 'kWh':
        raise ValueError('unsupported_forecast')
    try:
        generated = datetime.fromisoformat(attrs['forecast_generated_at'])
        if generated.tzinfo is None or not -60 <= (now-generated).total_seconds() <= 7200:
            raise ValueError('stale_forecast')
        rows = []
        for row in attrs['estimated_hourly_home_demand']:
            a, b = datetime.fromisoformat(row['start']), datetime.fromisoformat(row['end'])
            value = float(row['estimated_kwh'])
            if a.tzinfo is None or b.tzinfo is None or b <= a or value < 0 or not math.isfinite(value):
                raise ValueError('invalid_forecast_interval')
            rows.append((a, b, value))
        rows.sort()
        if any(a < prev_b for (_, prev_b, _), (a, _, _) in zip(rows, rows[1:])):
            raise ValueError('overlapping_forecast')
        cursor = now
        for a, b, value in rows:
            if b <= now or a >= horizon_end:
                continue
            if a > cursor:
                raise ValueError('incomplete_forecast')
            cursor = max(cursor, b)
        if cursor < horizon_end:
            raise ValueError('incomplete_forecast')
        return [dict(start=a.isoformat(), end=b.isoformat(), estimated_kwh=v) for a,b,v in rows]
    except (KeyError, TypeError) as err:
        raise ValueError('invalid_forecast') from err
