"""Bounded optimizer over policy-validated battery command schedules."""

from dataclasses import dataclass
from datetime import datetime
from math import isfinite
from time import monotonic

SOLAR = 'laden_met_zonne_energie'
HOME = 'ontladen'
GRID = 'laden_van_net'
EXPORT = 'ontladen_naar_net'
IDLE = 'accu_uit'
EPSILON = .005
_SWITCH_COST = .01
_MAX_CANDIDATES = 8
_MAX_SECONDS = .15


@dataclass(frozen=True, slots=True)
class BatteryScore:
    cost: float  # Incremental grid bill versus leaving the battery idle.
    peak: float
    end: float
    switches: int
    incomplete_grid_fill: bool
    completed_solar_fill: bool


def evaluate_battery_plan(slots, windows, *, now, initial, capacity, max_charge,
                          max_discharge, solar_factor=1., solar_factors=None):
    """Replay published commands using import and export rates separately.

    Baseline and candidates have identical idle-grid costs, so only battery
    transactions are scored. In the cloud replay, SOC limits actual delivery;
    this is a dispatch sensitivity check, not a forecast of future replans.
    """
    if not (all(isfinite(x) for x in (initial, capacity, max_charge, max_discharge, solar_factor))
            and 0 <= initial <= capacity + EPSILON and min(max_charge, max_discharge) > 0
            and 0 <= solar_factor <= 1):
        return None
    if solar_factors is not None and (len(solar_factors) != len(slots) or not all(
            isfinite(factor) and 0 <= factor <= 1 for factor in solar_factors)):
        return None
    nominal = solar_factor == 1. and solar_factors is None
    stock = peak = initial
    cost = 0.
    switches = 0
    grid_fill = incomplete_grid_fill = False
    completed_solar_fill = False
    last_mode, last_end = IDLE, now
    i = 0
    for window in windows:
        mode = window['mode']
        if mode == IDLE or (mode == HOME and window.get('standby')):
            continue
        if mode not in (SOLAR, GRID, HOME, EXPORT):
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
            factor = solar_factor if solar_factors is None else solar_factors[i]
            net = (float(slot['net_solar_kwh']) - (1 - factor) * production) / duration
            if not isfinite(net):
                return None
            if mode in (SOLAR, GRID):
                if mode == SOLAR and (slot.get('solar_transition') or tariff_in < 0):
                    return None
                solar_kw = min(max_charge, max(0., net))
                power = max_charge if mode == GRID else solar_kw
                energy = power * hours
                if nominal and stock + energy > capacity + EPSILON:
                    return None
                energy = min(max(0., capacity - stock), energy)
                solar = energy * (solar_kw / power) if power else 0.
                grid = energy - solar
                grid_fill |= grid > 1e-9
                stock += energy
                cost += tariff_out * solar + tariff_in * grid
                if stock >= capacity - EPSILON:
                    grid_fill = False
                    completed_solar_fill |= mode == SOLAR and energy > 1e-9
            else:
                home_kw = min(max_discharge, max(0., -net))
                energy = (max_discharge if mode == EXPORT else home_kw) * hours
                if nominal and energy > stock + EPSILON:
                    return None
                energy = min(stock, energy)
                if energy > 1e-9 and grid_fill:
                    incomplete_grid_fill = True
                stock -= energy
                home = min(energy, home_kw * hours)
                cost -= tariff_in * home + tariff_out * (energy - home)
            peak = max(peak, stock)
            cursor = until
            if slot['end'] > end:
                break
            i += 1
        if cursor != end:
            return None
    return BatteryScore(cost, peak, stock, switches, incomplete_grid_fill or grid_fill, completed_solar_fill)


def optimize_battery_plan(*, candidates, simulate, settings, clock=monotonic,
                          cloudy_solar_factors=None):
    """Return the best validated candidate found within the search budget.

    Offline replay can supply a slot-aligned cloud scenario; the live caller
    uses the original fixed factor. Nominal scoring and guards are unchanged.
    """
    deadline = clock() + _MAX_SECONDS

    def assess(windows):
        if windows is None:
            return None
        normal = evaluate_battery_plan(windows=windows, **settings)
        cloudy = evaluate_battery_plan(windows=windows, solar_factor=.75,
                                       solar_factors=cloudy_solar_factors, **settings)
        if normal is None or cloudy is None:
            return None
        objective = .5 * (normal.cost + cloudy.cost) + normal.switches * _SWITCH_COST
        return normal, objective, cloudy

    best = incumbent = reserve_only = None
    candidates = iter(candidates)
    for _ in range(_MAX_CANDIDATES):
        if clock() >= deadline:
            return best or reserve_only, 'time_budget'
        candidate = next(candidates, None)
        if candidate is None:
            break
        trial = assess(simulate(candidate))
        if trial is not None:
            nominal, value, cloudy = trial
            if candidate[0] == 'reserve_only':
                reserve_only = candidate
            elif ((candidate[0] == 'continuation' or not nominal.incomplete_grid_fill)
                    and (candidate[0] != 'solar' or nominal.completed_solar_fill)
                    and (incumbent is None or (
                        nominal.peak >= incumbent[0].peak - EPSILON
                        and nominal.cost <= incumbent[0].cost + 1e-9
                        and cloudy.cost <= incumbent[2].cost + 1e-9
                        and nominal.end >= incumbent[0].end - EPSILON
                        and cloudy.end >= incumbent[2].end - EPSILON
                        and value < incumbent[1] - .01))):
                best, incumbent = candidate, trial
        if clock() >= deadline:
            return best or reserve_only, 'time_budget'
    best = best or reserve_only
    return (best, 'battery_cost_and_cloud_risk') if best is not None else (None, 'no_valid_plan')
