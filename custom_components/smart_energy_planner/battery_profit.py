"""Energy-weighted profitability of the complete grid supplement."""
from datetime import timedelta


def grid_supplement_profit(*, selected, slots, after, before, max_charge_kw,
                           max_discharge_kw, minimum_profit, existing_energy=0.,
                           allow_export=False, historical_cost=None):
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
    existing = max(0., existing_energy)
    value = 0.
    for price, capacity in sorted(demand, reverse=True):
        used = min(existing, capacity)
        existing -= used
        take = min(remaining, capacity-used)
        value += take * price
        remaining -= take
    covered = needed-remaining
    return dict(net_kwh=needed, import_cost=cost, covered_kwh=covered,
                discharge_value=value,
                average_profit=(value-cost)/needed if needed > 1e-9 else 0.,
                # A zero-import command cannot bypass the solar-profit check.
                profitable=needed > 1e-9 and remaining <= 1e-6
                and value-cost + 1e-9 >= needed*minimum_profit)
