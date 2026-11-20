"""Independent command integration for partial PV policy (no planner internals)."""
from datetime import datetime


def assert_executable_plan(case, slots, windows, *, initial, reserve, minimum=2.,
                           full=10., max_charge=3., max_discharge=3., margin=.08,
                           initial_cost=None, state=None, allow_estimated=False,
                           require_new_pv_profit=False):
    """Check physical stock, original reserve, source gates and acquisition margins.

    Absolute kWh, no clipping: only an integrated full boundary permits a lower
    floor. A grid/mixed fill cannot reverse below full. Every discharged known
    lot (including PV export opportunity cost) must clear the configured margin.
    Unknown historical stock is not assigned a fabricated acquisition cost.
    """
    energy = initial
    saw_full = initial >= full - .005
    floor = minimum if saw_full else reserve
    grid_hold = False if state is None else state['grid_hold']
    lots = ([[max(0., initial-minimum), initial_cost]] if state is None
            else [list(lot) for lot in state['lots']])
    if state is not None:
        floor = state['floor']
    bought = sold = pv_bought = 0.
    existing_free = max(0., initial-floor)
    historical_cost = initial_cost if state is None else state.get('historical_cost', initial_cost)
    pv_basis_kwh = 0. if state is None else state.get('pv_basis_kwh', 0.)
    pv_basis_cost = 0. if state is None else state.get('pv_basis_cost', 0.)
    previous_end = None
    for w in sorted(windows, key=lambda w: w['start']):
        start, end = map(datetime.fromisoformat, (w['start'], w['end']))
        case.assertLess(start, end)
        if previous_end is not None:
            case.assertGreaterEqual(start, previous_end)
        previous_end = end
        for s in slots:
            left, right = max(start, s['start']), min(end, s['end'])
            if right <= left:
                continue
            hours = (right-left).total_seconds()/3600
            rate = float(s['net_solar_kwh'])/float(s['hours'])
            mode = w['mode']
            if mode.startswith('laden'):
                grid = mode == 'laden_van_net'
                amount = (max_charge if grid else min(max_charge, max(0., rate)))*hours
                if grid:
                    grid_hold = True
                cost = float(s['import_price'] if grid else s['export_price'])
                # Preview recalculates unknown tariffs; the original slots'
                # placeholders are not its acquisition/outlet prices. Their
                # physical bounds still apply, but never certify live profit.
                if allow_estimated and not s.get('price_known', True):
                    cost = None
                # The execution margin ledger records PV as unknown-cost;
                # its opportunity price is a separate weighted NEW-PV test.
                lots.append([amount, cost if grid else None])
                energy += amount
                bought += amount
                if grid and cost is not None:
                    historical_cost = max(cost, historical_cost) if historical_cost is not None else cost
                elif not grid:
                    pv_bought += amount
                    if cost is not None:
                        pv_basis_kwh += amount
                        pv_basis_cost += amount*float(cost)
                if amount > 1e-9:
                    floor = reserve
                case.assertLessEqual(energy, full+.005)
                if energy >= full-.005:
                    saw_full = True
                    grid_hold = False
                    floor = minimum
            elif mode.startswith('ontladen'):
                amount = (max_discharge if mode == 'ontladen_naar_net'
                          else min(max_discharge, max(0., -rate)))*hours
                if amount <= 1e-9:
                    continue
                case.assertFalse(grid_hold, 'unfinished grid/mixed fill reversed')
                case.assertTrue(allow_estimated or s.get('price_known', True),
                                'unknown outlet executed')
                value = float(s['export_price'] if mode == 'ontladen_naar_net' else s['import_price'])
                # Execution uses the highest remaining known lot, not only the
                # cheap FIFO head which accounting happens to consume first.
                costs = [cost for quantity, cost in lots if quantity > 1e-9 and cost is not None]
                if historical_cost is not None:
                    costs.append(historical_cost)
                # Unknown old stock may legitimately sell without establishing
                # a NEW-PV profit. Only certification applies the PV basis to
                # all claimed outlets; compatibility still enforces known lots.
                if require_new_pv_profit and pv_basis_kwh > 1e-9:
                    costs.append(pv_basis_cost/pv_basis_kwh)
                if amount > 1e-9 and costs and (s.get('price_known', True) or not allow_estimated):
                    acquisition = max(costs)
                    if mode == 'ontladen_naar_net':
                        case.assertGreater(float(s['export_price']), 0.)
                        if energy > reserve + .005:
                            case.assertGreaterEqual(value-acquisition+1e-6, margin)
                        if rate < -1e-9:
                            case.assertGreaterEqual(float(s['import_price'])-acquisition+1e-6,
                                                    margin, 'mixed export home leg fails margin')
                    else:
                        case.assertGreaterEqual(value-acquisition+1e-6, margin,
                            (s['start'], s['end'], amount, value, acquisition))
                remaining = amount
                for lot in lots:
                    consumed = min(lot[0], remaining)
                    lot[0] -= consumed
                    remaining -= consumed
                    if remaining <= 1e-8:
                        break
                case.assertLessEqual(remaining, .005, 'sold nonexistent stock')
                energy -= amount
                sold += amount
                case.assertGreaterEqual(energy, floor-.005,
                    'original reserve released without an integrated full boundary')
            case.assertGreaterEqual(energy, minimum-.005)
    # No-sale physical compatibility is not a NEW-PV profit proof. Existing free
    # stock must not masquerade as turnover of the new purchase.
    pv_sold = min(pv_bought, max(0., sold-existing_free))
    verified = require_new_pv_profit and pv_sold > 1e-6 and not allow_estimated
    if require_new_pv_profit:
        case.assertTrue(verified, 'no independently verified NEW PV sale')
    return {'energy': energy, 'bought': bought, 'sold': sold, 'saw_full': saw_full,
            'floor': floor, 'grid_hold': grid_hold, 'lots': lots,
            'historical_cost': historical_cost, 'pv_sold': pv_sold,
            'pv_basis_kwh': pv_basis_kwh, 'pv_basis_cost': pv_basis_cost,
            'verified_new_pv_profit': verified}
