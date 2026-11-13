"""Offline, dependency-free chart of an actual fixture replay (not live HA).

Run from repository root: PYTHONPATH=tests python scripts/preview_partial_cycles.py
The chart uses visualize_battery.plot_data's independent physical integration.
"""
from dataclasses import asdict
from datetime import datetime
from html import escape
import json
from pathlib import Path
import sys
from xml.etree import ElementTree

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "tests")]
from test_battery_solar_topup_regression import replay
from scripts.visualize_battery import plot_data
from test_battery_partial_cycle_economics import imported_margin


def main():
    now, slots, result, captured = replay()
    snapshot = asdict(result)
    snapshot.update(battery_capacity_kwh=10., battery_min_soc_percent=20.)
    data = plot_data(snapshot, 2.5, 3., "Gedeeltelijke cycli — offline fixture")
    estimated_keys = {(w['start'], w['end'], w['mode'])
                      for w in result.estimated_battery_mode_windows}
    def estimated(window):
        return (window['start'], window['end'], window['mode']) in estimated_keys
    out = ROOT / "previews" / "partial-cycles"
    out.mkdir(parents=True, exist_ok=True)
    (out / "computed-snapshot.json").write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
    (out / "plot-data.json").write_text(json.dumps(data, indent=2), encoding="utf-8")
    begin = datetime.fromisoformat(data['start']).timestamp()
    finish = datetime.fromisoformat(data['end']).timestamp()
    left, width = 72, 1050
    def x(t):
        return left + width * (datetime.fromisoformat(t).timestamp() - begin) / (finish - begin)
    def y(v, low, high, top, height=120):
        return top + height * (high-v) / (high-low)
    svg = []
    for title, top in [('Importprijs · €/kWh', 30), ('Vermogen · kW', 215), ('Berekende SOC · %', 400), ('Modusvensters', 585)]:
        svg.append(f'<text x="72" y="{top}">{title}</text>')
    def panel(series, low, high, top):
        for i in range(5):
            value = low + (high-low)*i/4
            py = y(value, low, high, top)
            svg.append(f'<line x1="72" y1="{py}" x2="1122" y2="{py}" stroke="#dce2e4"/>')
            svg.append(f'<text x="62" y="{py+4}" text-anchor="end" class="tick">{value:.2f}</text>')
        for points, color in series:
            coords = ' '.join(f'{x(t):.3f},{y(v,low,high,top):.3f}' for t,v in points)
            svg.append(f'<polyline points="{coords}" fill="none" stroke="{color}" stroke-width="2"/>')
    # Step functions: the next boundary retains the preceding value before
    # switching, rather than implying interpolation between tariffs/powers.
    def steps(key):
        points = []
        rows = data['rows']
        for row, following in zip(rows, rows[1:]):
            points.extend([(row['t'], row[key]), (following['t'], row[key])])
        return points
    prices = [r['price'] for r in data['rows']]
    power_max = max(1., *(r[k] for r in data['rows'] for k in ('solar','demand')))
    panel([(steps('price'), '#293f50')], min(0., min(prices)), max(.1, max(prices))*1.1, 48)
    panel([(steps('solar'), '#b57400'), (steps('demand'), '#526a86')], 0., power_max*1.1, 233)
    soc = [v for _,v in data['soc']]
    assert min(soc) >= min(float(snapshot['battery_soc_percent']), float(captured['battery_no_charge_min_soc_percent']))-.05
    assert max(soc) <= 100.05
    ledger = imported_margin(slots, data['windows'],
        initial_usable=float(snapshot['battery_soc_percent'])/10.-2.,
        reserve_usable=float(captured['battery_no_charge_min_soc_percent'])/10.-2.)
    if ledger['sold'] > 1e-6:
        assert ledger['margin']+.001 >= result.battery_min_profit_per_kwh, ledger
    (out / 'economics.json').write_text(json.dumps(ledger, indent=2), encoding='utf-8')
    panel([(data['soc'], '#087b65')], min(0., min(soc)), max(100., max(soc)), 418)
    reserve = float(captured['battery_no_charge_min_soc_percent'])
    ry = y(reserve, min(0., min(soc)), max(100., max(soc)), 418)
    svg.append(f'<line x1="72" x2="1122" y1="{ry}" y2="{ry}" stroke="#ba493c" stroke-dasharray="6 4"/><text x="1120" y="{ry-5}" text-anchor="end" class="tick">Reserve {reserve:g}%</text>')
    colors = {'accu_uit':'#e0e5e7','laden_met_zonne_energie':'#dba522','laden_van_net':'#527aaf','ontladen':'#14947e','ontladen_naar_net':'#b65c76'}
    for w in data['windows']:
        status = 'vooruitblik, niet uitvoerbaar' if estimated(w) else 'uitvoerbare planning'
        opacity = .45 if estimated(w) else 1.
        svg.append(f'<rect x="{x(w["start"]):.3f}" y="605" width="{max(0.,x(w["end"])-x(w["start"])):.3f}" height="28" fill="{colors[w["mode"]]}" opacity="{opacity}"><title>{escape(w["start"] + " — " + w["end"] + ": " + w["mode"] + " · " + status)}</title></rect>')
    for i in range(9):
        stamp = begin + (finish-begin)*i/8
        at = datetime.fromtimestamp(stamp, now.tzinfo).isoformat()
        svg.append(f'<text x="{x(at):.3f}" y="660" text-anchor="middle" class="tick">{datetime.fromisoformat(at):%d-%m %H:%M}</text>')
    table = ''.join(f'<tr><td>{escape(w["start"])}</td><td>{escape(w["end"])}</td><td>{escape(w["mode"])}</td><td>{"vooruitblik" if estimated(w) else "uitvoerbaar"}</td></tr>' for w in data['windows'])
    html = f'''<!doctype html><html lang="nl"><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>Gedeeltelijke cycli — offline preview</title>
<style>body{{font:16px Arial,sans-serif;color:#22343e;background:#f6f8f8;margin:32px auto;max-width:1200px;padding:0 18px}}h1{{font-size:26px}}.warning{{background:#fff0d6;padding:14px}}svg{{width:100%;background:white}}svg text{{font:14px Arial,sans-serif;fill:#243942}}svg .tick{{font-size:11px}}table{{border-collapse:collapse;font:13px monospace;width:100%}}td,th{{border-bottom:1px solid #dce2e4;text-align:left;padding:8px}}.scroll{{overflow:auto}}@media(max-width:650px){{body{{margin:12px auto}}svg{{min-width:850px}}}}</style>
<h1>Gedeeltelijke cycli — berekende offline planning</h1>
<p class="warning"><strong>Lokale kandidaat: NIET vrijgegeven.</strong> Zie docs/partial-cycle-policy-verification.md voor de actuele testresultaten. Nog geen gebruikersgoedkeuring van deze grafiek; geen commit, push of deployment. Deze preview is geen bewijs voor live Home Assistant-gedrag.</p>
<p>Bron: vastgelegde sensorfixture 12–13 november 2026. Replay vanaf {escape(now.isoformat())}. Geen nieuwe live metingen. Ongewijzigde brondata, nieuwe planneruitvoer.</p>
<p>Onafhankelijk geïntegreerde SOC: {min(soc):.3f}–{max(soc):.3f}%. Laden 2.5 kW, ontladen 3 kW; capaciteit 10 kWh; fysieke minimum-SOC 20%; reserve {reserve:g}%. Vooruitblikvensters blijven niet-uitvoerbare schattingen.</p>
<p>Vermogen: <span style="color:#b57400">zon</span> / <span style="color:#526a86">huis</span>. Modus: grijs uit · goud zonladen · blauw netladen · groen huisontladen · roze export. Lichtere modusblokken: vooruitblik, geen uitvoerbare opdrachten.</p>
<div class="scroll"><svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1180 690" role="img" aria-label="Importprijzen, zon en huisvraag, onafhankelijk berekende SOC en modusvensters">{''.join(svg)}</svg></div>
<details><summary>Alle berekende modusvensters</summary><div class="scroll"><table><thead><tr><th>Begin</th><th>Einde</th><th>Modus</th><th>Status</th></tr></thead><tbody>{table}</tbody></table></div></details>
<p>Reproduceerbaar met <code>PYTHONPATH=tests python scripts/preview_partial_cycles.py</code>. De JSON-bestanden naast deze HTML bevatten de berekende snapshot en de exacte grafiekdata.</p></html>'''
    (out / "index.html").write_text(html, encoding="utf-8")
    ElementTree.fromstring(html[html.index('<svg '):html.index('</svg>')+6])
    assert len(data['windows']) == len(result.planned_battery_mode_windows)+len(result.estimated_battery_mode_windows)
    print(json.dumps(dict(html=str(out / 'index.html'), soc_min=min(soc), soc_max=max(soc), soc_points=len(soc), actual_windows=len(result.planned_battery_mode_windows), estimated_windows=len(result.estimated_battery_mode_windows), economics=ledger)))


if __name__ == '__main__':
    main()
