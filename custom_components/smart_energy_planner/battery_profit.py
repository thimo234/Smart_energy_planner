"""Weighted margin on discharged imports, with reserve stock held at cost."""
from datetime import timedelta


def grid_supplement_profit(*, selected, slots, after, before, max_charge_kw,
                           max_discharge_kw, minimum_profit, existing_energy=0.,
                           allow_export=False, historical_cost=None,
                           discharge_budget_kwh=None, retained_reserve_kwh=0.):
    by_start = {slot['start']: slot for slot in slots}
    imports = []
    for item in selected:
        if item['kind'] != 'grid':
            continue
        slot = by_start[item['start']]
        solar_fraction = min(1., max(0., float(slot['net_solar_kwh']))
                             / max(max_charge_kw * float(slot['hours']), 1e-9))
        imports.append((float(item['charge_kwh']) * (1 - solar_fraction), float(item['profit_cost'])))
    needed = sum(kwh for kwh, _ in imports)
    cost = sum(kwh * price for kwh, price in imports)
    # Export keeps its existing per-kWh margin protection. Cheap PV does not
    # subsidize imports, and house/export share the same discharge power.
    export_cost = max((price for _, price in imports), default=0.)
    if historical_cost is not None and existing_energy > .01:
        export_cost = max(export_cost, historical_cost)
    demand = []
    for slot in slots:
        hours = max(0., (min(before, slot['end']) - max(after, slot['start'])).total_seconds()/3600)
        power = max_discharge_kw * hours
        home = min(power, max(0., -float(slot['net_solar_kwh'])) * hours / max(float(slot['hours']), 1e-9))
        demand.append((float(slot['import_price']), home))
        if (allow_export and slot['start'] >= before - timedelta(hours=8)
                and float(slot['export_price']) > 0
                and float(slot['export_price']) + 1e-9 >= export_cost + minimum_profit):
            demand.append((float(slot['export_price']), max(0., power-home)))
    remaining = needed
    # Existing stock retained as reserve does not consume the later demand
    # that must justify the new import. Only its dischargeable part competes.
    existing = max(0., existing_energy - retained_reserve_kwh)
    existing_dischargeable = existing
    value = 0.
    budget = float('inf') if discharge_budget_kwh is None else max(0., discharge_budget_kwh)
    for price, capacity in sorted(demand, reverse=True):
        capacity = min(capacity, budget)
        budget -= capacity
        used = min(existing, capacity)
        existing -= used
        take = min(remaining, capacity-used)
        value += take * price
        remaining -= take
    covered = needed-remaining
    average_cost = cost / needed if needed > 1e-9 else 0.
    discharged_cost = covered * average_cost
    # Only the reserve may defer its purchase cost. Missing demand must not
    # turn an arbitrary small peak into support for a complete new fill.
    required_coverage = needed if discharge_budget_kwh is None else min(
        needed, max(0., discharge_budget_kwh - existing_dischargeable))
    return dict(net_kwh=needed, import_cost=cost, covered_kwh=covered,
                discharge_value=value, discharged_import_cost=discharged_cost,
                retained_kwh=remaining, retained_import_cost=cost-discharged_cost,
                average_profit=(value-discharged_cost)/covered if covered > 1e-9 else 0.,
                # Stock at cost contributes no profit and cannot justify a
                # fill without useful discharge. Export retains its own gate.
                profitable=covered > 1e-9 and covered + 1e-6 >= required_coverage
                and value-discharged_cost + 1e-9 >= covered*minimum_profit)
