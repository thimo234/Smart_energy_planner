"""Inspect cumulative HA CSV history and walk forward without future observations."""
import argparse
import csv
import importlib.util
import json
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
import statistics
import sys
from unittest.mock import patch
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tests'))
from test_support import install_package_stub
install_package_stub()
from custom_components.smart_energy_planner import battery_forecast as current
from custom_components.smart_energy_planner.price_models import PlannerWindow


def price_history(path):
    with Path(path).open(encoding='utf-8-sig') as stream:
        rows = list(csv.DictReader(stream))
    if len({r['entity_id'] for r in rows}) != 1:
        raise ValueError('Select exactly one tariff entity.')
    points = sorted({datetime.fromisoformat(r['last_changed'].replace('Z', '+00:00')):
                     r['state'] for r in rows}.items())
    windows = []
    for (start, value), (end, _) in zip(points, points[1:]):
        try:
            price = float(value)
        except ValueError:
            continue
        tz = ZoneInfo('Europe/Amsterdam')
        windows.append(PlannerWindow(start.astimezone(tz), end.astimezone(tz), price))
    return windows


def hourly_history(path):
    """Distribute counter increments by overlap; reject resets and long gaps."""
    with Path(path).open(encoding='utf-8-sig') as stream:
        rows = list(csv.DictReader(stream))
    if len({r['entity_id'] for r in rows}) != 1:
        raise ValueError('Select exactly one cumulative kWh entity.')
    # Preserve the last state for duplicate timestamps, as Recorder does.
    points = sorted({datetime.fromisoformat(r['last_changed'].replace('Z', '+00:00')):
                     float(r['state']) for r in rows}.items())
    energy, coverage = defaultdict(float), defaultdict(float)
    rejected = 0
    for (start, a), (end, b) in zip(points, points[1:]):
        seconds = (end-start).total_seconds()
        # This calculated total has small negative corrections, not resets.
        # Preserve signed deltas; reject large discontinuities in either direction.
        if not 0 < seconds <= 4500 or abs(b-a) > 10:
            rejected += 1
            continue
        cursor = start
        while cursor < end:
            hour = cursor.replace(minute=0, second=0, microsecond=0)
            finish = min(end, hour + timedelta(hours=1))
            overlap = (finish-cursor).total_seconds()
            energy[hour] += (b-a) * overlap/seconds
            coverage[hour] += overlap
            cursor = finish
    tz = ZoneInfo('Europe/Amsterdam')
    hours = {h.astimezone(tz): v for h, v in energy.items()
             if coverage[h] >= 3599.9 and v >= 0}
    return hours, dict(rows=len(rows), first=points[0][0].astimezone(tz).isoformat(),
                      last=points[-1][0].astimezone(tz).isoformat(), rejected_intervals=rejected)


def evaluate(module, hours, prices=None):
    days = defaultdict(dict)
    for h, v in hours.items():
        days[h.date()][h.hour] = v
    stats, totals, errors, daily_errors = {}, [], [], []
    forecasts = {}
    for day, values in sorted(days.items()):
        start = datetime.combine(day, datetime.min.time(), ZoneInfo('Europe/Amsterdam'))
        if len(totals) >= 7 and len(values) == 24:
            average = statistics.fmean(totals[-7:])
            table = module.build_expected_hourly_demand_table(stats, daily_average_kwh=average)
            class Clock(datetime):
                @classmethod
                def now(cls, tz=None):
                    return start
            with patch.object(module, 'datetime', Clock):
                forecast = module.build_hourly_home_demand_forecast(
                    non_heating_daily_average_kwh=average, heating_estimate_kwh=0,
                    hourly_demand_table=table)[:24]
            if prices:
                forecast = module.align_price_responsive_demand_to_cheap_hours(forecast, prices)
            predicted = [float(s['estimated_kwh']) for s in forecast]
            errors.extend(abs(predicted[h]-values[h]) for h in range(24))
            daily_errors.append(abs(sum(predicted)-sum(values.values())))
            forecasts[day.isoformat()] = forecast
        for h in sorted(values):
            stats = module.update_expected_hourly_demand_stats(
                stats, slot_key=str(day.weekday()*24+h), measured_kwh=values[h])
        if len(values) == 24:
            totals.append(sum(values.values()))
    return dict(days=len(daily_errors), hourly_mae_kwh=statistics.fmean(errors),
                daily_mae_kwh=statistics.fmean(daily_errors)), forecasts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('csv')
    parser.add_argument('--before', type=Path)
    parser.add_argument('--prices', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    hours, info = hourly_history(args.csv)
    info['complete_hours'] = len(hours)
    days = defaultdict(float)
    for h, v in hours.items():
        days[h.date().isoformat()] += v
    info['daily_observed_kwh'] = {d: round(v, 3) for d, v in days.items()}
    info['daily_covered_hours'] = {d: sum(h.date().isoformat() == d for h in hours) for d in days}
    info['tuesday_10_to_15'] = {h.isoformat(): round(v, 3) for h, v in hours.items()
                              if h.weekday() == 1 and 10 <= h.hour <= 15}
    prices = price_history(args.prices) if args.prices else None
    info['new'], forecasts = evaluate(current, hours, prices)
    if args.before:
        spec = importlib.util.spec_from_file_location(
            'custom_components.smart_energy_planner.before', args.before)
        before = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(before)
        info['old'], _ = evaluate(before, hours, prices)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(dict(summary=info, forecasts=forecasts), indent=2))
    print(json.dumps(info, indent=2))


if __name__ == '__main__':
    main()
