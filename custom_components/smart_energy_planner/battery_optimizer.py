"""Bounded optimizer over policy-validated battery command schedules."""

from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
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
    night_topup_completed: bool = False
    unspent_grid_kwh: float = 0.
    spent_grid_kwh: float = 0.
    retained_initial_kwh: float = 0.


@dataclass(slots=True)
class _GridBatch:
    bought: float = 0.
    cost: float = 0.
    highest_price: float = float('-inf')
    used: float = 0.
    value: float = 0.


def instant(value):
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    return value.astimezone(UTC) if value.tzinfo is not None else value


def executable_windows(windows, cutoff):
    limit = instant(cutoff)
    result = []
    for window in windows:
        start, end = instant(window['start']), instant(window['end'])
        if start < limit:
            result.append(window if end <= limit else {**window, 'end': cutoff.isoformat(),
                          'usable_hours': (limit - start).total_seconds() / 3600})
    return result


def command_key(windows, now):
    """Identify the remaining executable commands, ignoring idle and segmentation."""
    result = []
    for window in windows:
        if window.get('mode') not in (SOLAR, GRID, HOME, EXPORT) or not {'start', 'end'} <= window.keys():
            continue
        start, end = max(instant(now), instant(window['start'])), instant(window['end'])
        if end <= start:
            continue
        mode = window['mode'], bool(window.get('standby'))
        if result and result[-1][1:] == (start, mode):
            start = result.pop()[0]
        result.append((start, end, mode))
    return tuple(result)


def feedback_pattern(windows):
    """A reusable preference: charge sources, night import and export use."""
    modes = {w.get('mode') for w in windows}
    night = any(w.get('mode') == GRID and 'start' in w and 0 <= datetime.fromisoformat(w['start']).hour < 7
                for w in windows)
    return GRID in modes, SOLAR in modes, night, EXPORT in modes


def night_topup_target(slots, now, initial, capacity, max_charge):
    day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    if now.hour >= 7:
        day += timedelta(days=1)
    night_end = day + timedelta(hours=7)
    solar = [s for s in slots if s['start'].date() == day.date()
             and instant(s['start']) >= instant(now) and s.get('price_known', True)
             and not s.get('solar_transition') and s['net_solar_kwh'] > 0
             and s['import_price'] >= 0 and s['hours'] > 0]
    pv = sum(min(max_charge * s['hours'], s['net_solar_kwh']) for s in solar)
    if not EPSILON < pv < capacity - initial - EPSILON:
        return None, ()
    night = tuple(s for s in slots if instant(day) <= instant(s['start'])
                  and instant(now) < instant(s['end']) <= min(instant(night_end), instant(solar[0]['start']))
                  and s.get('price_known', True))
    return (solar[0]['start'], solar[-1]['end'], pv), night


def evaluate_battery_plan(slots, windows, *, now, initial, capacity, max_charge,
                          max_discharge, solar_factor=1., solar_factors=None,
                          allow_export=True, discharge_floor=0., fallback_reserve=None,
                          committed_full=False, topup_target=None,
                          minimum_profit=None, allow_refill_backed_discharge=False,
                          committed_grid_fill=False, on_reject=None):
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
    def reject(reason):
        if on_reject is not None:
            on_reject(reason)
        return None

    nominal = solar_factor == 1. and solar_factors is None
    now = instant(now)
    reserve = discharge_floor if fallback_reserve is None else fallback_reserve
    initial_full = committed_full or initial >= capacity - EPSILON
    # A future refill cannot justify draining stock already below the reserve.
    provisional_floor = allow_refill_backed_discharge and not initial_full and initial >= reserve - EPSILON
    floor = discharge_floor if initial_full or provisional_floor else max(discharge_floor, reserve)
    needs_refill = False
    solar_start, solar_end, target_pv = (None, None, 0.) if topup_target is None else (
        instant(topup_target[0]), instant(topup_target[1]), topup_target[2])
    solar_in_fill = 0.
    night_grid = topup_completed = False
    stock = peak = initial
    check_margin = nominal and minimum_profit is not None
    # Reserve stock is not an outlet for new purchases. Only the initially
    # dispatchable stock competes with new energy for scheduled demand. Retained
    # initial stock is decremented if commands actually discharge below it.
    retained_initial = min(initial, max(0., reserve)) if check_margin else 0.
    inventory = deque([(initial - retained_initial, None)]) if check_margin else deque()
    batches = []
    grid_batch = None
    cost = 0.
    switches = 0
    grid_fill = committed_grid_fill and initial < capacity - EPSILON
    incomplete_grid_fill = False
    completed_solar_fill = False
    last_mode, last_end = IDLE, now
    previous_end = now
    i = 0
    for window in windows:
        mode = window['mode']
        if mode not in (IDLE, SOLAR, GRID, HOME, EXPORT):
            return reject('unknown_command')
        start = max(now, instant(window['start']))
        end = instant(window['end'])
        if end <= start or start < previous_end:
            return reject('command_time_order')
        previous_end = end
        if mode == IDLE or (mode == HOME and window.get('standby')):
            continue
        if mode == EXPORT and not allow_export:
            return reject('export_disabled')
        if last_mode != IDLE:
            switches += 2 if start > last_end else int(mode != last_mode)
        elif start > now:
            switches += 1
        last_mode, last_end = mode, end
        while i < len(slots) and instant(slots[i]['end']) <= start:
            i += 1
        cursor = start
        while i < len(slots) and instant(slots[i]['start']) < end:
            slot = slots[i]
            slot_end = instant(slot['end'])
            begin, until = max(start, instant(slot['start'])), min(end, slot_end)
            if begin != cursor or not slot.get('price_known', True):
                return reject('missing_executable_tariff')
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
                    return reject('solar_charge_not_allowed')
                solar_kw = min(max_charge, max(0., net))
                power = max_charge if mode == GRID else solar_kw
                energy = power * hours
                if nominal and stock + energy > capacity + EPSILON:
                    return reject('maximum_soc')
                energy = min(max(0., capacity - stock), energy)
                solar = energy * (solar_kw / power) if power else 0.
                grid = energy - solar
                if energy > EPSILON:
                    provisional_floor = False
                    floor = reserve
                grid_fill |= grid > 1e-9
                if solar_start is not None and solar_end is not None:
                    if grid > EPSILON:
                        night_grid = (night_grid or until <= solar_start) and begin < solar_start
                    if begin >= solar_start and until <= solar_end:
                        solar_in_fill += solar
                stock += energy
                if check_margin:
                    if solar > 0:
                        inventory.append((solar, None))
                    if grid > 0:
                        if grid_batch is None:
                            grid_batch = _GridBatch()
                            batches.append(grid_batch)
                        grid_batch.bought += grid
                        grid_batch.cost += grid * tariff_in
                        grid_batch.highest_price = max(grid_batch.highest_price, tariff_in)
                        inventory.append((grid, grid_batch))
                cost += tariff_out * solar + tariff_in * grid
                if stock >= capacity - EPSILON:
                    needs_refill = False
                    floor = 0. if fallback_reserve is not None else discharge_floor
                    topup_completed |= night_grid and solar_in_fill >= target_pv - EPSILON
                    night_grid, solar_in_fill = False, 0.
                    grid_fill = False
                    completed_solar_fill |= mode == SOLAR and energy > 1e-9
            else:
                home_kw = min(max_discharge, max(0., -net))
                energy = (max_discharge if mode == EXPORT else home_kw) * hours
                available = max(0., stock - floor)
                if nominal and energy > available + EPSILON:
                    return reject('discharge_reserve')
                energy = min(available, energy)
                if energy > 1e-9 and grid_fill:
                    if nominal:
                        return reject('unfinished_grid_fill_discharge')
                    incomplete_grid_fill = True
                if energy > EPSILON:
                    night_grid, solar_in_fill = False, 0.
                stock -= energy
                if provisional_floor and energy > 1e-9 and stock < reserve - EPSILON:
                    needs_refill = True
                home = min(energy, home_kw * hours)
                if check_margin and energy > 1e-9:
                    grid_batch = None
                    export_basis = max((batch.highest_price for quantity, batch in inventory
                                        if quantity > 1e-9 and batch is not None), default=None)
                    if export_basis is not None and energy - home > 1e-9:
                        required = export_basis + minimum_profit
                        if tariff_out + 1e-9 < required or (home > 1e-9 and tariff_in + 1e-9 < required):
                            return reject('grid_minimum_profit')
                    remaining = energy
                    while inventory and remaining > 1e-9:
                        quantity, batch = inventory.popleft()
                        used = min(quantity, remaining)
                        remaining -= used
                        if batch is not None:
                            batch.used += used
                            batch.value += used * (home * tariff_in + (energy - home) * tariff_out) / energy
                        if quantity - used > 1e-9:
                            inventory.appendleft((quantity - used, batch))
                    retained_initial = max(0., retained_initial - remaining)
                cost -= tariff_in * home + tariff_out * (energy - home)
            peak = max(peak, stock)
            cursor = until
            if slot_end > end:
                break
            i += 1
        if cursor != end:
            return reject('missing_executable_tariff')
    if nominal and needs_refill:
        return reject('reserve_refill_not_executable')
    if any(batch.value + 1e-9 < batch.used * (batch.cost / batch.bought + minimum_profit)
           for batch in batches):
        return reject('grid_minimum_profit')
    unspent_grid = sum(quantity for quantity, batch in inventory if batch is not None)
    return BatteryScore(cost, peak, stock, switches, incomplete_grid_fill or grid_fill,
                        completed_solar_fill, topup_completed, unspent_grid, sum(batch.used for batch in batches),
                        retained_initial)


def optimize_battery_plan(*, candidates, simulate, settings, clock=monotonic,
                          cloudy_solar_factors=None, feedback=(), topup_target=None,
                          rejection_reasons=None, candidate_settings=None):
    """Return the best validated candidate found within the search budget.

    Offline replay can supply a slot-aligned cloud scenario; the live caller
    uses the original fixed factor. Nominal scoring and guards are unchanged.
    """
    deadline = clock() + _MAX_SECONDS
    votes = {}
    preferences = {}
    latest = {}
    for record in feedback:
        windows = record.get('plan', {}).get('planned_battery_mode_windows') or ()
        vote = record.get('vote')
        if windows and vote in {'approved', 'rejected'} and all({'start', 'end'} <= w.keys() for w in windows):
            # Voting twice on the same published plan replaces its label.
            fingerprint = tuple((w['start'], w['end'], w.get('mode'), bool(w.get('standby'))) for w in windows)
            latest[fingerprint] = feedback_pattern(windows), vote
        if windows and 'end' in windows[-1] and instant(windows[-1]['end']) > instant(settings['now']):
            votes[command_key(windows, settings['now'])] = vote
    for pattern, vote in latest.values():
        preferences[pattern] = preferences.get(pattern, 0) + (1 if vote == 'approved' else -1)

    def mark(reason):
        if rejection_reasons is not None:
            rejection_reasons.add(reason)

    def assess(candidate, windows):
        if windows is None:
            mark('source_admission')
            return None
        policy = {**settings, 'topup_target': topup_target}
        if candidate_settings is not None:
            policy.update(candidate_settings(candidate))
        policy['committed_grid_fill'] = candidate[0] == 'continuation'
        profit_outlook = policy.pop('profit_outlook', windows)
        require_solar_completion = policy.pop('require_solar_completion', False)
        normal = evaluate_battery_plan(windows=windows, on_reject=mark, **policy)
        if normal is None:
            mark('invalid_executable_plan')
            return None
        if normal.incomplete_grid_fill and candidate[0] != 'continuation':
            mark('grid_fill_does_not_reach_full')
            return None
        # A fresh PV charge after discharging is a separate cycle, not a
        # restart of the released fill on the same remaining sunlight.
        first_active = next((w['mode'] for w in windows
                             if w['mode'] != IDLE and not w.get('standby')), None)
        if (candidate[0] == 'solar' or (require_solar_completion and first_active == SOLAR)
                ) and not normal.completed_solar_fill:
            mark('solar_candidate_does_not_reach_full')
            return None
        if candidate[0] == 'night_topup' and not normal.night_topup_completed:
            mark('night_topup_does_not_complete_with_solar')
            return None
        if (candidate[0] != 'continuation' and policy.get('minimum_profit') is not None
                and (candidate[0] == 'night_topup' or normal.unspent_grid_kwh > EPSILON)):
            # Publication can end at the fill. Require downstream profitability
            # on the same commands, not on an unrelated forecast-only plan.
            profit = normal
            if profit_outlook != windows:
                cutoff = datetime.fromisoformat(windows[-1]['end'])
                if (command_key(executable_windows(profit_outlook, cutoff), policy['now'])
                        == command_key(windows, policy['now'])):
                    profit = evaluate_battery_plan(windows=profit_outlook, on_reject=mark, **policy)
                else:
                    profit = None
            held_reserve = policy.get('fallback_reserve', 0.) or 0.
            grid_reserve_room = 0. if profit is None else max(0., held_reserve - profit.retained_initial_kwh)
            if (profit is None or profit.spent_grid_kwh <= EPSILON
                    or profit.unspent_grid_kwh > grid_reserve_room + EPSILON):
                mark('night_topup_unproven_profit' if candidate[0] == 'night_topup'
                     else 'grid_unproven_profit')
                return None
        cloudy = evaluate_battery_plan(windows=windows, solar_factor=.75,
                                       solar_factors=cloudy_solar_factors, **policy)
        if cloudy is None:
            mark('cloud_replay_invalid')
            return None
        objective = .5 * (normal.cost + cloudy.cost) + normal.switches * _SWITCH_COST
        return normal, objective, cloudy

    best = incumbent = reserve_only = preferred_topup = None
    rejected = False
    best_has_commands = reserve_has_commands = False
    incumbent_preference = (False, 0)
    timed_out = False
    candidates = iter(candidates)
    for _ in range(_MAX_CANDIDATES):
        if clock() >= deadline:
            timed_out = True
            break
        candidate = next(candidates, None)
        if candidate is None:
            break
        windows = simulate(candidate)
        trial = assess(candidate, windows)
        if trial is not None:
            nominal, value, cloudy = trial
            vote = votes.get(command_key(windows, settings['now'])) if votes else None
            preference = (vote == 'approved', preferences.get(feedback_pattern(windows), 0) if preferences else 0)
            if vote == 'rejected' and candidate[0] != 'continuation':
                mark('manually_rejected')
                rejected = True
            elif (candidate[0] == 'night_topup' and nominal.night_topup_completed
                  and (preferred_topup is None or value < preferred_topup[0])):
                preferred_topup = value, candidate
            elif candidate[0] == 'reserve_only':
                reserve_only = candidate
                reserve_has_commands = any(w['mode'] != IDLE for w in windows)
            elif (incumbent is None or (
                        nominal.peak >= incumbent[0].peak - EPSILON
                        and nominal.cost <= incumbent[0].cost + 1e-9
                        and cloudy.cost <= incumbent[2].cost + 1e-9
                        and nominal.end >= incumbent[0].end - EPSILON
                        and cloudy.end >= incumbent[2].end - EPSILON
                        and (value < incumbent[1] - .01 or (preference > incumbent_preference
                            and value <= incumbent[1] + 1e-9)))):
                best, incumbent = candidate, trial
                best_has_commands = any(w['mode'] != IDLE for w in windows)
                incumbent_preference = preference
        if clock() >= deadline:
            timed_out = True
            break
    if preferred_topup is not None:
        return preferred_topup[1], 'time_budget' if timed_out else 'night_topup_with_solar'
    if best is None:
        best, best_has_commands = reserve_only, reserve_has_commands
    if best is not None:
        if not best_has_commands:
            return best, 'no_battery_action_timeout' if timed_out else 'no_battery_action'
        return best, 'time_budget' if timed_out else 'battery_cost_and_cloud_risk'
    if rejected:
        return None, 'feedback_search_timeout' if timed_out else 'no_feedback_alternative'
    return None, 'time_budget' if timed_out else 'no_valid_plan'
