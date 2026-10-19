"""Preview the independent demand-source contract with labelled synthetic devices."""
import argparse
from dataclasses import asdict
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tests'))
from test_demand_integration import demand_plan
from visualize_battery import render


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    _, _, result = demand_plan()
    data = asdict(result)
    data.update(battery_capacity_kwh=10, battery_min_soc_percent=20,
                scenario_note='Synthetic appliance measurements and prices; no solar; '
                'dishwasher 11:00, heat pump 13:00, cooking 17:30; '
                '60% initial SOC, 10 kWh, 3 kW, 20% battery demand margin.')
    args.output.mkdir(parents=True, exist_ok=True)
    snapshot = args.output/'planning.json'
    snapshot.write_text(json.dumps(data, indent=2), encoding='utf-8')
    print(render(snapshot, args.output/'planning.html', 3, 3))


if __name__ == '__main__':
    main()
