"""Walk-forward comparison of v2's fixed and learned cloud scenarios (no HA)."""
from argparse import ArgumentParser
from collections import defaultdict
from datetime import date, datetime
import importlib.util
import json
from math import isfinite
from pathlib import Path
from statistics import median
import sys
from typing import Any


# The optimizer is stand-alone; importing the integration package would load HA.
_spec = importlib.util.spec_from_file_location(
    '_battery_optimizer_offline', Path(__file__).resolve().parents[1]
    / 'custom_components/smart_energy_planner/battery_optimizer.py')
assert _spec is not None and _spec.loader is not None
_optimizer = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = _optimizer
_spec.loader.exec_module(_optimizer)
SOLAR, GRID, HOME, EXPORT, IDLE = (
    _optimizer.SOLAR, _optimizer.GRID, _optimizer.HOME, _optimizer.EXPORT, _optimizer.IDLE)


def timestamp(value):
    result = datetime.fromisoformat(value)
    if result.utcoffset() is None:
        raise ValueError('timestamps must include a UTC offset')
    return result


def number(row, key, *, nonnegative=True):
    value = row.get(key)
    if (isinstance(value, bool) or not isinstance(value, (float, int))
            or not isfinite(value) or (nonnegative and value < 0)):
        raise ValueError(f'{key} must be a valid finite number')
    return float(value)


def read_cases(lines):
    cases = []
    for i, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            case = json.loads(line)
            if not isinstance(case, dict):
                raise ValueError('expected a JSON object')
            cases.append(case)
        except ValueError as exc:
            raise ValueError(f'line {i}: {exc}') from exc
    return cases


def prepare(case, now) -> tuple[dict[str, Any], list[tuple[float, float]]]:
    slots, measured = [], []
    cursor = now
    for row in case['slots']:
        start, end = timestamp(row['start']), timestamp(row['end'])
        if start != cursor or end <= start:
            raise ValueError('slots must be positive and contiguous from now')
        cursor = end
        solar, demand = number(row, 'solar_kwh'), number(row, 'demand_kwh')
        net = (number(row, 'net_solar_kwh', nonnegative=False)
               if 'net_solar_kwh' in row else solar-demand)
        slots.append(dict(start=start, end=end, hours=(end-start).total_seconds()/3600,
                          solar_kwh=solar, demand_kwh=demand, net_solar_kwh=net,
                          solar_transition=bool(row.get('solar_transition', False)),
                          price_known=bool(row.get('price_known', True)),
                          import_price=number(row, 'import_price', nonnegative=False),
                          export_price=number(row, 'export_price', nonnegative=False)))
        measured.append((number(row, 'measured_solar_kwh'), number(row, 'measured_demand_kwh')))
    if not slots:
        raise ValueError('slots cannot be empty')
    settings = {key: number(case, key) for key in ('initial', 'capacity', 'max_charge', 'max_discharge')}
    if settings['initial'] > settings['capacity'] or min(settings[k] for k in (
            'capacity', 'max_charge', 'max_discharge')) <= 0:
        raise ValueError('invalid battery limits or initial stored energy')
    return dict(settings, slots=slots, now=now), measured


def plan_windows(windows, settings):
    cursor = settings['now']
    for window in windows:
        start, end = timestamp(window['start']), timestamp(window['end'])
        if (window['mode'] not in (SOLAR, GRID, HOME, EXPORT, IDLE)
                or max(settings['now'], start) < cursor or end <= max(settings['now'], start)
                or end > settings['slots'][-1]['end']):
            raise ValueError('unsupported, overlapping or out-of-horizon plan window')
        cursor = end
    return windows


def bucket(slot):
    return slot['start'].hour // 6


def daily_ratios(slots, measured):
    groups = defaultdict(list)
    for slot, (solar, _) in zip(slots, measured, strict=True):
        expected = slot['solar_kwh']
        if expected / slot['hours'] >= .1:  # avoid unstable ratios in twilight
            groups[bucket(slot)].append(min(1., solar / expected))
    return {key: median(values) for key, values in groups.items()}


def scenario(slots, history):
    by_bucket = defaultdict(list)
    for daily in history:
        for key, ratio in daily.items():
            by_bucket[key].append(ratio)
    factors = {key: min(.95, sorted(values)[(len(values)-1)//4])
               for key, values in by_bucket.items() if len(values) >= 3}
    if not any(slot['solar_kwh'] > 0 and bucket(slot) in factors for slot in slots):
        return None
    return tuple(factors.get(bucket(slot), .75) for slot in slots)


def actual_score(windows, settings, measured, expensive):
    """Independently integrate the published modes against measured energy."""
    stock = minimum = peak = settings['initial']
    cost = imported = expensive_import = exported = 0.
    switches, previous_mode = 0, None
    schedule = [(timestamp(w['start']), timestamp(w['end']), w['mode']) for w in windows
                if w['mode'] != IDLE and not (w['mode'] == HOME and w.get('standby'))]
    for slot, (solar, demand) in zip(settings['slots'], measured, strict=True):
        start, end = slot['start'], slot['end']
        active = [(max(start, a), min(end, b), mode) for a, b, mode in schedule if a < end and b > start]
        cursor = start
        intervals = []
        for a, b, mode in active:
            if a > cursor:
                intervals.append((cursor, a, IDLE))
            intervals.append((a, b, mode))
            cursor = b
        if cursor < end:
            intervals.append((cursor, end, IDLE))
        for a, b, mode in intervals:
            if previous_mode is not None and previous_mode != mode:
                switches += 1
            previous_mode = mode
            hours = (b-a).total_seconds()/3600
            net = (solar-demand)*hours/slot['hours']
            if mode in (SOLAR, GRID):
                charge = min(settings['capacity']-stock, settings['max_charge']*hours)
                if mode == SOLAR:
                    charge = min(charge, max(0., net))
                stock += charge
                net -= charge
            elif mode in (HOME, EXPORT):
                discharge = min(stock, settings['max_discharge']*hours)
                if mode == HOME:
                    discharge = min(discharge, max(0., -net))
                stock -= discharge
                net += discharge
            minimum, peak = min(minimum, stock), max(peak, stock)
            grid_in, grid_out = max(0., -net), max(0., net)
            cost += grid_in*slot['import_price'] - grid_out*slot['export_price']
            imported += grid_in
            exported += grid_out
            if slot['import_price'] >= expensive:
                expensive_import += grid_in
    return dict(cost_eur=cost, import_kwh=imported, export_kwh=exported,
                expensive_import_kwh=expensive_import, switches=switches,
                min_kwh=minimum, peak_kwh=peak, end_kwh=stock)


def backtest(cases):
    """Each decision sees only its original forecast and completed earlier days."""
    ordered = sorted(((timestamp(case['now']), case) for case in cases), key=lambda pair: pair[0])
    results = []
    history: list[tuple[date, dict[int, float]]] = []
    previous_day: date | None = None
    previous_end: datetime | None = None
    for now, case in ordered:
        if ((previous_day is not None and now.date() <= previous_day)
                or (previous_end is not None and now < previous_end)):
            raise ValueError('use one non-overlapping replay per local day')
        settings, measured = prepare(case, now)
        slots = settings['slots']
        baseline = ('baseline', 'v1')
        plans: dict[tuple[str, str], list] = {baseline: plan_windows(case['baseline'], settings)}
        ids = {'v1'}
        for variant in case['candidates']:
            kind, identifier = variant['kind'], variant['id']
            if kind not in ('solar', 'grid') or not isinstance(identifier, str) or not identifier or identifier in ids:
                raise ValueError('candidate requires a unique non-empty id and solar or grid kind')
            ids.add(identifier)
            plans[kind, identifier] = plan_windows(variant['windows'], settings)
        history = [(day, ratios) for day, ratios in history if (now.date()-day).days <= 28]
        factors = scenario(slots, [ratios for _, ratios in history])
        expensive = max(slot['import_price'] for slot in slots)
        row: dict[str, Any] = dict(now=now.isoformat(), calibrated=factors is not None,
                                  expensive_import_threshold_eur_kwh=expensive)
        for label, option in [('fixed', None), ('learned', factors)]:
            # The frozen clock makes the offline comparison independent of host load.
            # The production candidate limit and all scorer checks still apply.
            selected, reason = _optimizer.optimize_battery_plan(
                baseline=baseline, candidates=tuple(plans)[1:], simulate=plans.__getitem__,
                settings=settings, clock=lambda: 0, cloudy_solar_factors=option)
            selected = selected or baseline
            row[label] = dict(plan=selected[1], reason=reason,
                              **actual_score(plans[selected], settings, measured, expensive))
        results.append(row)
        previous_day, previous_end = now.date(), slots[-1]['end']
        history.append((previous_day, daily_ratios(slots, measured)))
    return results


def main():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('observations', type=Path, help='one captured decision and its later measurements per JSON line')
    args = parser.parse_args()
    try:
        with args.observations.open(encoding='utf-8') as source:
            cases = read_cases(source)
        if not cases:
            raise ValueError('no measured replay days supplied')
        for row in backtest(cases):
            print(json.dumps(row, allow_nan=False))
    except (OSError, ValueError, KeyError, TypeError) as exc:
        parser.error(str(exc))


if __name__ == '__main__':
    main()
