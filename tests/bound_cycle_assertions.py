"""Independent, chronological physical oracle for the fixed-boundary policy."""
from datetime import datetime
from test_battery_energy_accounting import energy_trace


def assert_bound_plan(case, now, slots, windows, *, initial, floor, max_charge=2.5,
                      minimum=2., maximum=10., charging=None):
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
        if family == 'charge' and direction == 'discharge':
            case.assertLessEqual(energy, floor+.005, 'recharge before selected lower boundary')
        if family == 'discharge' and direction == 'charge':
            case.assertGreaterEqual(energy, maximum-.005, 'discharge before full charge')
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
