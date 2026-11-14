"""Compute a standalone fixed-boundary review artifact; no live HA/network access.
Run: PYTHONPATH=tests python scripts/preview_bound_cycles.py
"""
from dataclasses import asdict
from datetime import datetime
from html import escape
import json
from pathlib import Path
import sys
import unittest
from xml.etree import ElementTree

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / 'tests')]
from test_battery_bound_cycle_integration import BoundCycleIntegrationTest, NOW, replay, scenario
from test_battery_solar_topup_regression import replay as recorded_replay
from bound_cycle_assertions import assert_bound_plan
from test_battery_partial_cycle_economics import imported_margin
from scripts.visualize_battery import plot_data

OUT = ROOT / 'previews' / 'bound-cycles'
COLORS = {'accu_uit': '#dce2e5', 'laden_met_zonne_energie': '#dba522',
          'laden_van_net': '#527aaf', 'ontladen': '#14947e', 'ontladen_naar_net': '#b65c76'}


def computed_cases():
    for full in (True, False):
        c = BoundCycleIntegrationTest().new(charging=full)
        inputs = scenario(charging=True) if full else scenario(demand_hours=5)
        result = replay(c, inputs, soc=20 if full else 100)
        floor = c._battery_cycle_discharge_floor_kwh + 2
        expected = 2 if full else 7
        assert abs(floor - expected) < 1e-9
        yield ('full' if full else 'reserve', NOW, inputs, result,
               floor, full, 'Synthetische controle: bekende tarieven, 10 kWh, reserve 70%, ' +
               ('volledige rendabele afzet.' if full else 'te weinig rendabele afzet voor normale minimumgrens.'))
    c = BoundCycleIntegrationTest().new()
    c._battery_cycle_discharge_floor_kwh = 4.
    c._battery_grid_charge_price = .80
    c._battery_margin_state = dict(at=NOW.isoformat(), energy=8., reserve=0.,
        lots=[[8., .80]], credit=0., actions=[])
    loss_slots = scenario(demand_hours=0)
    for slot in loss_slots:
        slot.update(import_price=.10, export_price=-.05)
    loss_result = replay(c, loss_slots, reserve=60.)
    yield ('reserve-loss', NOW, loss_slots, loss_result, 6., False,
           'Synthetische verliescontrole: bestaande accu 100%, reserve 60%, '
           'aangenomen inkoopkost €0,80/kWh, terugleverprijs −€0,05/kWh, geen huisvraag. '
           'Ontladen moet exact bij 60% stoppen; onder de reserve geldt geen verliesuitzondering.')
    now, slots, result, _ = recorded_replay()
    # Captured fixture starts at 55%, below its configured 60% reserve:
    # the legacy discharge is already finished and a new fill is permitted.
    yield ('recorded', now, slots, result, 6., True,
           'Vastgelegde sensorfixture 12–13 november 2026; geen nieuwe live meting. '
           'Exporttarieven, vraag en zon komen uit de fixture.')


def render(name, now, slots, result, floor, charging, description):
    snapshot = asdict(result)
    snapshot.update(battery_capacity_kwh=10., battery_min_soc_percent=20.)
    snapshot['upcoming_energy_price_windows'] = [
        dict(start=s['start'].isoformat(), end=s['end'].isoformat(),
             price=s['import_price'], export_price=s['export_price'],
             price_known=s.get('price_known', True)) for s in slots]
    # The synthetic production replay patches the balance-slot boundary; use
    # precisely those computed inputs, never the helper's unrelated fixture
    # metadata, for the independent graph integration.
    for field, key in (('estimated_hourly_solar_forecast', 'solar_kwh'),
                       ('estimated_hourly_home_demand', 'demand_kwh')):
        snapshot[field] = [dict(start=s['start'].isoformat(), end=s['end'].isoformat(),
                                estimated_kwh=s[key]) for s in slots]
    windows = result.planned_battery_mode_windows + result.estimated_battery_mode_windows
    case = unittest.TestCase()
    initial = result.battery_soc_percent / 10
    assert_bound_plan(case, now, slots, windows, initial=initial, floor=floor, charging=charging)
    if result.estimated_battery_mode_windows:
        assert result.planned_battery_mode_windows[-1]['end'] == result.estimated_battery_mode_windows[0]['start']
    data = plot_data(snapshot, 2.5, 3., name)
    soc = [v for _, v in data['soc']]
    assert min(soc) >= 19.95 and max(soc) <= 100.05
    ledger = imported_margin(slots, windows, initial_usable=initial-2., reserve_usable=floor-2.)
    if ledger['sold'] > 1e-6:
        assert ledger['margin'] >= result.battery_min_profit_per_kwh-1e-6, ledger
    evidence = dict(description=description, selected_floor_percent=floor*10,
                    soc_min=min(soc), soc_max=max(soc), executable_windows=len(result.planned_battery_mode_windows),
                    outlook_windows=len(result.estimated_battery_mode_windows), economics=ledger)
    for suffix, value in [('snapshot', snapshot), ('plot-data', data), ('evidence', evidence)]:
        (OUT / f'{name}-{suffix}.json').write_text(json.dumps(value, indent=2), encoding='utf-8')
    start, end = (datetime.fromisoformat(data[k]).timestamp() for k in ('start', 'end'))
    def x(t):
        return 64 + 1050 * (datetime.fromisoformat(t).timestamp()-start)/(end-start)
    svg = []
    def panel(title, traces, low, high, top):
        svg.append(f'<text x="64" y="{top-12}">{escape(title)}</text>')
        def y(v):
            return top + 110 * (high-v)/(high-low)
        for i in range(5):
            value = low+(high-low)*i/4
            svg.append(f'<line x1="64" x2="1114" y1="{y(value)}" y2="{y(value)}" stroke="#dde3e5"/><text x="55" y="{y(value)+4}" text-anchor="end" class="tick">{value:.2f}</text>')
        for points, color in traces:
            coords = ' '.join(f'{x(t):.3f},{y(v):.3f}' for t, v in points)
            svg.append(f'<polyline points="{coords}" fill="none" stroke="{color}" stroke-width="2"/>')
        return y
    def steps(key):
        return [(t, row[key]) for row, following in zip(data['rows'], data['rows'][1:])
                for t in (row['t'], following['t'])]
    prices = [r['price'] for r in data['rows']]
    panel('Importprijs · €/kWh', [(steps('price'), '#293f50')], min(0., min(prices)), max(.1, max(prices))*1.1, 38)
    power = max(1., *(r[k] for r in data['rows'] for k in ('solar', 'demand')))
    panel('Zon (goud) / huisvraag (blauw) · kW', [(steps('solar'), '#b57400'), (steps('demand'), '#526a86')], 0., power*1.1, 208)
    y = panel('Onafhankelijk geïntegreerde SOC · % (geen begrenzing)', [(data['soc'], '#087b65')], 0., 105., 378)
    for value, label, color in ((20., 'Fysiek minimum 20%', '#888'), (floor*10, f'Gekozen ondergrens {floor*10:g}%', '#ba493c')):
        svg.append(f'<line x1="64" x2="1114" y1="{y(value)}" y2="{y(value)}" stroke="{color}" stroke-dasharray="6 4"/><text x="1114" y="{y(value)-5}" text-anchor="end" class="tick">{label}</text>')
    estimated = {(w['start'], w['end'], w['mode']) for w in result.estimated_battery_mode_windows}
    svg.append('<text x="64" y="540">Modi: grijs uit · goud zonladen · blauw netladen · groen huis · roze export</text>')
    table = []
    for w in windows:
        is_estimated = (w['start'], w['end'], w['mode']) in estimated
        label = 'vooruitblik, niet uitvoerbaar' if is_estimated else 'uitvoerbaar'
        tooltip = escape(f"{w['start']} — {w['end']}: {w['mode']} · {label}")
        svg.append(f'<rect x="{x(w["start"]):.3f}" y="555" width="{x(w["end"])-x(w["start"]):.3f}" height="28" fill="{COLORS[w["mode"]]}" opacity="{.4 if is_estimated else 1}"><title>{tooltip}</title></rect>')
        table.append('<tr>'+''.join(f'<td>{escape(str(v))}</td>' for v in (w['start'], w['end'], w['mode'], label))+'</tr>')
    for i in range(9):
        at = datetime.fromtimestamp(start+(end-start)*i/8, now.tzinfo)
        svg.append(f'<text x="{x(at.isoformat()):.3f}" y="610" text-anchor="middle" class="tick">{at:%d-%m %H:%M}</text>')
    image = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1180 640" role="img" aria-label="Berekende tarieven, vermogen, SOC en modusvensters">'+''.join(svg)+'</svg>'
    ElementTree.fromstring(image)
    return (f'<section><h2>{escape(name)} · grens {floor*10:g}%</h2><p>{escape(description)}</p>'
            f'<p>SOC {min(soc):.3f}–{max(soc):.3f}%. Lichtere modi: niet-uitvoerbare vooruitblik. '
            'Vermogenslimieten: laden 2.5 kW / ontladen 3 kW.</p><div class="scroll">'+image+'</div>'
            '<details><summary>Alle berekende opdrachten</summary><table><tr><th>Begin</th><th>Einde</th><th>Modus</th><th>Status</th></tr>'+''.join(table)+'</table></details></section>'), evidence


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    sections, evidence = [], {}
    for case in computed_cases():
        section, values = render(*case)
        sections.append(section)
        evidence[case[0]] = values
    html = '''<!doctype html><html lang="nl"><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>Vaste cyclusgrenzen — offline preview</title>
<style>body{font:16px Arial,sans-serif;color:#22343e;background:#f6f8f8;max-width:1200px;margin:32px auto;padding:0 18px}section{margin:36px 0}svg{width:100%;background:white}svg text{font:14px Arial,sans-serif;fill:#243942}svg .tick{font-size:11px}.warning{background:#fff0d6;padding:14px}.scroll{overflow:auto}td,th{border-bottom:1px solid #dce2e4;text-align:left;padding:8px;font:12px monospace}details{overflow:auto}@media(max-width:650px){svg{min-width:850px}}</style>
<h1>Vaste cyclusgrenzen — berekende offline preview</h1><p class="warning">Lokale kandidaat, nog niet vrijgegeven. Grafiekreview door gebruiker vereist. Geen commit, push, deployment of live verificatie.</p>
<p>Voorkeur: volledige rendabele cyclus maximum → normaal minimum. Anders maximum → ingestelde hogere reserve (hier 70% in de synthetische controle, niet hardcoded 50%). De gekozen grens blijft vast gedurende pauzes en prognosewijzigingen. Onvolledig laden geeft geen toestemming tot ontladen; een onvoltooide ontlading geeft geen toestemming tot herladen. Wachten is toegestaan. Een gekozen reservecyclus mag met verlies ontladen en terugleveren tot de ingestelde hogere reserve; onder die reserve geldt deze uitzondering niet.</p>
''' + ''.join(sections) + '<p>Reproduceerbaar: <code>PYTHONPATH=tests python scripts/preview_bound_cycles.py</code>. JSON-bestanden naast deze zelfstandige HTML bevatten de exacte berekende data; docs/bound-cycle-policy-verification.md bevat de verificatiestatus.</p></html>'
    (OUT / 'index.html').write_text(html, encoding='utf-8')
    (OUT / 'verification.json').write_text(json.dumps(evidence, indent=2), encoding='utf-8')
    print(json.dumps(dict(html=str(OUT / 'index.html'), cases=evidence), indent=2))


if __name__ == '__main__':
    main()
