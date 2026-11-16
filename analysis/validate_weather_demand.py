"""Offline chronological validation of a captured read-only Recorder dataset.

Run from the repository: python analysis/validate_weather_demand.py [capture.json]
No HA access, writes to HA, or synthetic observations. Local JSON report only.
"""
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
import types
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
# Load the pure forecasting helpers without executing HA's integration setup.
for name, directory in (
        ('custom_components', ROOT / 'custom_components'),
        ('custom_components.smart_energy_planner', ROOT / 'custom_components' / 'smart_energy_planner')):
    package = types.ModuleType(name)
    package.__path__ = [str(directory)]
    sys.modules.setdefault(name, package)
from custom_components.smart_energy_planner.demand_weather import correct_component
from custom_components.smart_energy_planner.demand_forecast import component_forecast


def validate(source):
    data = json.loads(source.read_text())
    now = datetime.fromisoformat(data['captured_at']).astimezone(ZoneInfo(data['timezone']))
    observed = {
        datetime.fromtimestamp(row['start']/1000, timezone.utc): row['mean']
        for rows in data['temperature_statistics'].values() for row in rows
        if row.get('mean') is not None
    }
    report = {
        'source': source.name, 'captured_at': data['captured_at'],
        'method': 'production correct_component chronological expanding-window validation',
        'limitations': [
            'Observed historical temperature proxy, not archived weather forecasts.',
            'Per-component daily errors do not establish aggregate hourly accuracy.',
            'Baseline-history scores exclude intraday adjustment and configured schedule changes.',
            'Candidate validation also selects activation; not a separate final test set.',
            'Runtime merges Recorder states and Celsius mean statistics; this capture validates statistics coverage only.',
        ],
        'components': {},
    }
    for entity, rows in data['hourly_energy_kwh'].items():
        hours = {
            datetime.fromtimestamp(row['start']/1000, timezone.utc): row['kwh']
            for row in rows
            if datetime.fromtimestamp(row['start']/1000, timezone.utc)+timedelta(hours=1) <= now
        }
        # Here timing is deliberately learned for energy relationship validation;
        # no historical schedule or forecast-provider issuance archive exists.
        rule = {'timing': 'learned', 'intraday_allowed': False}
        try:
            profile, _ = component_forecast(hours, rule, now=now, prices=[])
            _, metrics = correct_component(profile, hours, observed, {}, rule, now=now)
            report['components'][entity] = metrics
        except ValueError as error:
            report['components'][entity] = {'error': str(error)}
    return report


if __name__ == '__main__':
    source = Path(sys.argv[1]) if len(sys.argv)>1 else Path(__file__).with_name('weather-demand-live.json')
    report = validate(source)
    target = source.with_name('weather-demand-validation.json')
    target.write_text(json.dumps(report, indent=2, allow_nan=False)+'\n')
    print(json.dumps(report, indent=2, allow_nan=False))
    print(f'Report: {target}')
