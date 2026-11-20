"""Independent, chronological physical oracle for the fixed-boundary policy."""
from datetime import datetime
from test_battery_energy_accounting import energy_trace


def assert_bound_plan(case, now, slots, windows, *, initial, floor, max_charge=2.5,
                      minimum=2., maximum=10., charging=None,
                      solar_partial=True, charge_source=None):
    # Approved policy: independently profitable PV may reverse without full.
    # Grid and mixed fills retain the strict full-target oracle below; new PV
    # economics and original-reserve integration are checked independently in
    # partial_solar_assertions, not inferred from this direction-only helper.
    energy = initial
    direction = 'charge' if charging else 'discharge' if charging is False else None
    extrema = [energy]
    prior_end = None
    for w in windows:
        start, end = map(datetime.fromisoformat, (w['start'], w['end']))
        case.assertGreater(end, start)
        if prior_end is not None:
            case.assertGreaterEqual(start, prior_end, 'overlapping commands')
        prior_end = end
        family = ('charge' if w['mode'].startswith('laden_') else
                  'discharge' if w['mode'].startswith('ontladen') else None)
        if (family == 'charge' and direction == 'discharge'
                and not (solar_partial and w['mode'] == 'laden_met_zonne_energie')):
            case.assertLessEqual(energy, floor+.005, 'recharge before selected lower boundary')
        if (family == 'discharge' and direction == 'charge'
                and not (solar_partial and charge_source == 'laden_met_zonne_energie')):
            case.assertGreaterEqual(energy, maximum-.005, 'discharge before full charge')
        if family == 'charge':
            if direction != 'charge' or charge_source != 'laden_van_net':
                charge_source = w['mode']
        elif family == 'discharge':
            charge_source = None
        if family:
            direction = family
        trace = energy_trace(now, slots, [w], initial=energy, max_charge=max_charge)
        if trace:
            energy = trace[-1][1]
            extrema.extend(e for _, e, _ in trace)
        case.assertGreaterEqual(energy, min(initial, floor)-.005)
        case.assertGreaterEqual(energy, minimum-.005)
        case.assertLessEqual(energy, maximum+.005)
    return extrema, energy
