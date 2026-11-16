"""Conservative component-level heating learning and bounded same-day updates.

Validation uses historical observed weather, not archived forecasts: its errors
measure model fit out of sample, not the accuracy of the weather provider.
"""
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import math
from statistics import fmean

from .demand_forecast import _days, _samples


def temperature_c(value, unit):
    try:
        value = float(value)
        if unit in ('°F', 'F'):
            value = (value-32)*5/9
        elif unit not in ('°C', 'C'):
            return None
        return value if math.isfinite(value) and -80 <= value <= 60 else None
    except (TypeError, ValueError):
        return None


def temperature_hours(states, *, now):
    """Time-weight observed states; require full hours and gaps <=75 minutes."""
    points = {s.last_updated.astimezone(timezone.utc):
              temperature_c(s.state, s.attributes.get('unit_of_measurement'))
              for s in states if s.last_updated <= now}
    weighted, coverage = defaultdict(float), defaultdict(float)
    for (a, value), (b, _) in zip(sorted(points.items()), sorted(points.items())[1:]):
        if value is None or not 0 < (b-a).total_seconds() <= 4500:
            continue
        cursor = a
        while cursor < b:
            hour = cursor.replace(minute=0, second=0, microsecond=0)
            end = min(b, hour+timedelta(hours=1))
            seconds = (end-cursor).total_seconds()
            weighted[hour] += value*seconds
            coverage[hour] += seconds
            cursor = end
    return {h: weighted[h]/coverage[h] for h in coverage
            if coverage[h] >= 3599.99 and h+timedelta(hours=1) <= now}


def temperature_statistics(rows, *, now):
    """Recorder mean statistics explicitly requested in Celsius; no gap filling."""
    result = {}
    for row in rows:
        try:
            at = datetime.fromtimestamp(float(row['start']), timezone.utc)
            value = temperature_c(row['mean'], '°C')
            if value is not None and at+timedelta(hours=1) <= now:
                result[at] = value
        except (KeyError, TypeError, ValueError, OverflowError):
            continue
    return result


def forecast_temperatures(rows, unit):
    """Parse hourly forecast instants; never reinterpret daily minima as hours."""
    result = {}
    for row in rows or []:
        try:
            at = datetime.fromisoformat(row['datetime'])
            value = temperature_c(row.get('temperature'), unit)
            if at.tzinfo is not None and value is not None:
                result[at.astimezone(timezone.utc)] = value
        except (KeyError, TypeError, ValueError):
            continue
    return result


def _fit(rows):
    if len(rows) < 14:
        return None
    xs, ys = [r[1] for r in rows], [r[2] for r in rows]
    x, y = fmean(xs), fmean(ys)
    variance = sum((v-x)**2 for v in xs)
    if variance/len(rows) < 4:
        return None
    slope = sum((a-x)*(b-y) for a,b in zip(xs,ys))/variance
    intercept = y-slope*x
    return (max(0, intercept), slope) if slope > 0 else None


def correct_component(profile, energy, temperatures, forecast, rule, *, now):
    """Scale each day's component energy only after chronological validation.

    Daily energy modelling preserves shifted appliance shapes; no instantaneous
    clock-temperature fit is applied to shifted schedules. Constant appliances
    do not pass the heating slope / held-out improvement gates.
    """
    cutoff = now.date()
    days = {d:v for d,v in _days(energy, now.tzinfo).items() if d < cutoff}
    temp_days = _days(temperatures, now.tzinfo)
    rows = [(d, max(0,18-fmean(temp_days[d])), sum(days[d]))
            for d in sorted(days) if d in temp_days and d < cutoff]
    diagnostics: dict = dict(temperature_status='insufficient_history', training_days=len(rows),
        validation_days=0, forecast_days_corrected=0, intraday_factor=1.0,
        validation_weather='observed_temperature_proxy_not_archived_forecast')
    history_errors, history_actuals = [], []
    for i, day in enumerate(sorted(days)):
        if i < 7:
            continue
        samples = _samples(days, day)
        actual = sum(days[day])
        history_errors.append(fmean(sum(v) for v in samples)-actual)
        history_actuals.append(actual)
    diagnostics['baseline_history'] = dict(validation_days=len(history_actuals),
        scope='complete_day_energy_chronological_no_intraday_validation',
        mae_kwh=fmean(abs(v) for v in history_errors) if history_errors else None,
        bias_kwh=fmean(history_errors) if history_errors else None,
        wape_percent=100*sum(abs(v) for v in history_errors)/sum(history_actuals)
            if sum(history_actuals)>0 else None)
    baseline_errors, weather_errors, actuals = [], [], []
    # Every held-out day is predicted from strictly earlier complete days.
    for i in range(14,len(rows)):
        day, degree, actual = rows[i]
        fit = _fit(rows[:i])
        samples = _samples(days, day)
        if fit and samples:
            baseline = fmean(sum(v) for v in samples)
            candidate = max(baseline*.5, min(baseline*1.5, fit[0]+fit[1]*degree))
            baseline_errors.append(baseline-actual)
            weather_errors.append(candidate-actual)
            actuals.append(actual)
    active = False
    if len(actuals) >= 7:
        bm, wm = fmean(abs(v) for v in baseline_errors), fmean(abs(v) for v in weather_errors)
        diagnostics.update(validation_days=len(actuals), baseline_mae_kwh=bm,
            weather_mae_kwh=wm, baseline_bias_kwh=fmean(baseline_errors),
            weather_bias_kwh=fmean(weather_errors),
            baseline_wape_percent=100*sum(abs(v) for v in baseline_errors)/sum(actuals) if sum(actuals)>0 else None,
            weather_wape_percent=100*sum(abs(v) for v in weather_errors)/sum(actuals) if sum(actuals)>0 else None)
        active = wm < bm*.9 and bm-wm > .05
        diagnostics['temperature_status'] = 'active' if active else 'no_validated_improvement'
    result = dict(profile)
    grouped = defaultdict(list)
    for at in profile:
        grouped[at.astimezone(now.tzinfo).date()].append(at)
    diagnostics['temperature_model_status'] = 'validated' if active else diagnostics['temperature_status']
    diagnostics['temperature_days'] = {day.isoformat():
        ('missing_forecast_coverage' if active else diagnostics['temperature_status'])
        for day in grouped}
    if active:
        fit = _fit(rows)
        forecast_days = _days(forecast, now.tzinfo)
        for day, times in grouped.items():
            if day not in forecast_days or fit is None:
                continue
            baseline = sum(profile[t] for t in times)
            if baseline <= 0:
                diagnostics['temperature_days'][day.isoformat()] = 'zero_baseline'
                continue
            target = fit[0]+fit[1]*max(0,18-fmean(forecast_days[day]))
            factor = max(.5, min(1.5, target/baseline))
            for at in times:
                result[at] *= factor
            diagnostics['forecast_days_corrected'] += 1
            diagnostics['temperature_days'][day.isoformat()] = 'active'
        count = diagnostics['forecast_days_corrected']
        diagnostics['temperature_status'] = ('active' if count == len(grouped) and count
            else 'partially_active' if count else 'missing_forecast_coverage')
    # Only clock-time learned components: scheduled cycles can move, so their
    # partial-day residuals are not evidence of a change in daily consumption.
    if rule.get('timing','learned') == 'learned' and rule.get('intraday_allowed', True):
        observed, predicted = [], []
        for hour, value in sorted(energy.items()):
            if hour.astimezone(now.tzinfo).date() != cutoff or hour+timedelta(hours=1) > now:
                continue
            slots = [hour+timedelta(minutes=15*i) for i in range(4)]
            if all(t in result for t in slots):
                observed.append(value)
                predicted.append(sum(result[t] for t in slots))
        if len(observed) >= 3 and sum(predicted) >= .3:
            factor = max(.8,min(1.2,sum(observed)/sum(predicted)))
            diagnostics['intraday_factor'] = factor
            diagnostics['intraday_completed_hours'] = len(observed)
            for at in result:
                if at >= now and at.astimezone(now.tzinfo).date() == cutoff:
                    result[at] *= factor
    return result, diagnostics
