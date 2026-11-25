"""Bounded, solar-only optimizer over policy-validated command schedules."""

from dataclasses import dataclass
from datetime import datetime
from math import isfinite
from time import monotonic

SOLAR = 'laden_met_zonne_energie'
HOME = 'ontladen'
IDLE = 'accu_uit'
EPSILON = .005
_SWITCH_COST = .01
_MAX_CANDIDATES = 8
_MAX_SECONDS = .15


@dataclass(frozen=True, slots=True)
class SolarScore:
    cost: float  # Incremental grid bill versus leaving the battery idle.
    peak: float
    end: float
    switches: int


def evaluate_solar_plan(slots, windows, *, now, initial, capacity, max_charge,
                        max_discharge, solar_factor=1.):
    """Replay solar/home commands; less sun can turn surplus into a deficit.

    Baseline and candidates have identical idle-grid costs, so only battery
    transactions are scored. In the cloud replay, SOC limits actual delivery;
    this is a dispatch sensitivity check, not a forecast of future replans.
    """
    if not (all(isfinite(x) for x in (initial, capacity, max_charge, max_discharge, solar_factor))
            and 0 <= initial <= capacity + EPSILON and min(max_charge, max_discharge) > 0
            and 0 <= solar_factor <= 1):
        return None
    stock = peak = initial
    cost = 0.
    switches = 0
    last_mode, last_end = IDLE, now
    i = 0
    for window in windows:
        mode = window['mode']
        if mode == IDLE or (mode == HOME and window.get('standby')):
            continue
        if mode not in (SOLAR, HOME):
            return None
        start = max(now, datetime.fromisoformat(window['start']))
        end = datetime.fromisoformat(window['end'])
        if end <= start or start < last_end:
            return None
        if last_mode != IDLE:
            switches += 2 if start > last_end else int(mode != last_mode)
        elif start > now:
            switches += 1
        last_mode, last_end = mode, end
        while i < len(slots) and slots[i]['end'] <= start:
            i += 1
        cursor = start
        while i < len(slots) and slots[i]['start'] < end:
            slot = slots[i]
            begin, until = max(start, slot['start']), min(end, slot['end'])
            if begin != cursor or not slot.get('price_known', True):
                return None
            hours = (until - begin).total_seconds() / 3600
            duration = float(slot['hours'])
            production, demand = float(slot['solar_kwh']), float(slot['demand_kwh'])
            tariff_in, tariff_out = float(slot['import_price']), float(slot['export_price'])
            if not (duration > 0 and production >= 0 and demand >= 0
                    and all(isfinite(v) for v in (duration, production, demand, tariff_in, tariff_out))):
                return None
            net = (float(slot['net_solar_kwh']) - (1 - solar_factor) * production) / duration
            if not isfinite(net):
                return None
            if mode == SOLAR:
                if slot.get('solar_transition') or tariff_in < 0:
                    return None
                energy = min(max_charge, max(0., net)) * hours
                if solar_factor == 1. and stock + energy > capacity + EPSILON:
                    return None
                energy = min(max(0., capacity - stock), energy)
                stock += energy
                cost += tariff_out * energy
            else:
                energy = min(max_discharge, max(0., -net)) * hours
                if solar_factor == 1. and energy > stock + EPSILON:
                    return None
                energy = min(stock, energy)
                stock -= energy
                cost -= tariff_in * energy
            peak = max(peak, stock)
            cursor = until
            if slot['end'] > end:
                break
            i += 1
        if cursor != end:
            return None
    return SolarScore(cost, peak, stock, switches)


def optimize_solar_plan(*, baseline, candidates, simulate, settings, clock=monotonic):
    """Return the selected solar source windows, or a V1 fallback reason."""
    deadline = clock() + _MAX_SECONDS

    def assess(windows):
        normal = evaluate_solar_plan(windows=windows, **settings)
        cloudy = evaluate_solar_plan(windows=windows, solar_factor=.75, **settings)
        if normal is None or cloudy is None:
            return None
        objective = .5 * (normal.cost + cloudy.cost) + normal.switches * _SWITCH_COST
        return normal, objective, cloudy

    reference = assess(simulate(baseline))
    if clock() >= deadline:
        return None, 'time_budget'
    if reference is None:
        return None, 'outside_solar_scope'
    best, best_value = None, reference[1] - .01
    candidates = iter(candidates)
    for _ in range(_MAX_CANDIDATES):
        candidate = next(candidates, None)
        if candidate is None:
            break
        if clock() >= deadline:
            return None, 'time_budget'
        if candidate == baseline:
            continue
        trial = assess(simulate(candidate))
        if clock() >= deadline:
            return None, 'time_budget'
        if (trial is not None and trial[0].peak >= settings['capacity'] - EPSILON
                and trial[0].cost <= reference[0].cost + 1e-9
                and trial[2].cost <= reference[2].cost + 1e-9
                and trial[0].end >= reference[0].end - EPSILON and trial[1] < best_value):
            best, best_value = candidate, trial[1]
    if clock() >= deadline:
        return None, 'time_budget'
    return (best, 'solar_cost_and_cloud_risk') if best is not None else (None, 'no_better_solar_plan')
