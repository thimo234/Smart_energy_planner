"""Render saved sensor attributes with one reusable comparison chart.

JSON needs only Python; YAML (including pasted Home Assistant text) needs PyYAML.
This is a viewer and independent energy integration, not a planner invocation.
"""

import argparse
from datetime import datetime
import json
from pathlib import Path


def read_snapshot(path):
    text = Path(path).read_text(encoding='utf-8-sig')
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        import yaml
        data = yaml.safe_load(text)
    return data.get('attributes', data)


def plot_data(data, charge_kw, discharge_kw, label):
    parse = datetime.fromisoformat
    prices = data['upcoming_energy_price_windows']
    actual = data['planned_battery_mode_windows']
    preview = data.get('estimated_battery_mode_windows', [])
    windows = sorted(actual + preview, key=lambda w: w['start'])
    if not prices or not actual:
        raise ValueError('Prijsvensters en uitvoerbare modusvensters zijn vereist.')
    start = min(parse(w['start']) for w in actual)
    end = min(max(parse(w['end']) for w in prices), max(parse(w['end']) for w in windows))
    solar = data['estimated_hourly_solar_forecast']
    demand = data['estimated_hourly_home_demand']

    def value_at(items, at, key, power=False):
        item = next((w for w in items if parse(w['start']) <= at < parse(w['end'])), None)
        if item is None:
            raise ValueError(f'Ontbrekende {key}-prognose op {at.isoformat()}')
        value = float(item[key])
        return value / ((parse(item['end'])-parse(item['start'])).total_seconds()/3600) if power else value

    edges = {start, end}
    for item in prices + windows + solar + demand:
        edges.update(parse(item[k]) for k in ('start', 'end') if start < parse(item[k]) < end)
    edges = sorted(edges)
    soc = float(data['battery_soc_percent'])
    energy = float(data['battery_total_energy_kwh'])
    capacity = float(data.get('battery_capacity_kwh') or (energy * 100 / soc if soc else 0))
    if capacity <= 0:
        raise ValueError('Accucapaciteit kan niet worden bepaald; voeg battery_capacity_kwh toe.')
    rows, trace = [], [[start.isoformat(), soc]]
    for begin, finish in zip(edges, edges[1:]):
        pv = value_at(solar, begin, 'estimated_kwh', True)
        load = value_at(demand, begin, 'estimated_kwh', True)
        price = value_at(prices, begin, 'price')
        matches = [w for w in windows if parse(w['start']) <= begin < parse(w['end'])]
        if len(matches) > 1:
            raise ValueError(f'Overlappende modusvensters op {begin.isoformat()}')
        mode = matches[0]['mode'] if matches else 'accu_uit'
        rate = {'laden_van_net': charge_kw,
                'laden_met_zonne_energie': min(charge_kw, max(0, pv-load)),
                'ontladen': -min(discharge_kw, max(0, load-pv)),
                'ontladen_naar_net': -discharge_kw, 'accu_uit': 0}[mode]
        energy += rate * (finish-begin).total_seconds()/3600
        trace.append([finish.isoformat(), round(energy/capacity*100, 3)])
        rows.append(dict(t=begin.isoformat(), price=price, solar=pv, demand=load))
    rows.append({**rows[-1], 't': end.isoformat()})
    preview_note = (' · vooruitblik vanaf ' + min(parse(w['start']) for w in preview).strftime('%d-%m %H:%M')) if preview else ''
    return dict(rows=rows, windows=windows, soc=trace, start=start.isoformat(), end=end.isoformat(),
                minimum=float(data.get('battery_min_soc_percent',
                              (float(data['battery_total_energy_kwh'])-float(data['battery_energy_available_kwh']))/capacity*100)),
                label=f'{label} · {start:%d-%m %H:%M} tot {end:%d-%m %H:%M} · '
                      f'berekende SOC, laden {charge_kw:g} kW / ontladen {discharge_kw:g} kW{preview_note}')


def render(snapshot, output, charge_kw, discharge_kw, comparison=None):
    if charge_kw <= 0 or discharge_kw <= 0:
        raise ValueError('Laad- en ontlaadvermogen moeten positief zijn.')
    payload = {'nieuw': plot_data(read_snapshot(snapshot), charge_kw, discharge_kw, 'Ingelezen planning')}
    if comparison:
        payload['oud'] = plot_data(read_snapshot(comparison), charge_kw, discharge_kw, 'Vergelijkingsplanning')
    template = Path(__file__).with_name('templates').joinpath('battery_chart.html').read_text(encoding='utf-8')
    template = template.replace('__BATTERY_DATA__', json.dumps(payload, ensure_ascii=True).replace('<', '\\u003c'))
    if not comparison:
        template = template.replace('<option value="oud">Vergelijkingsplanning</option>', '')
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(template, encoding='utf-8')
    return output.resolve()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('snapshot', type=Path)
    parser.add_argument('--compare', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--charge-kw', type=float, required=True)
    parser.add_argument('--discharge-kw', type=float, required=True)
    args = parser.parse_args()
    print(render(args.snapshot, args.output, args.charge_kw, args.discharge_kw, args.compare))
