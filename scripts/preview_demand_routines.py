"""Replay demand before a chosen day, with optional actual tariff history."""
import argparse
from collections import defaultdict
from dataclasses import asdict
from datetime import datetime, timedelta
import importlib.util
import json
from pathlib import Path
import statistics
from unittest.mock import patch

from analyze_demand_history import hourly_history, price_history, current
from test_battery_energy_accounting import replay_full_plan
from custom_components.smart_energy_planner.price_models import PlannerWindow
from visualize_battery import render, plot_data


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('csv')
    parser.add_argument('--before', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--prices', type=Path)
    parser.add_argument('--day', default='2026-10-20')
    args = parser.parse_args()
    spec = importlib.util.spec_from_file_location('custom_components.smart_energy_planner.before', args.before)
    before = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(before)
    hours, _ = hourly_history(args.csv)
    start = datetime.fromisoformat(args.day+'T00:00:00+02:00')
    hours = {h: v for h, v in hours.items() if h < start}
    days = defaultdict(list)
    for h, value in hours.items():
        days[h.date()].append(value)
    average = statistics.fmean([sum(v) for d, v in sorted(days.items()) if len(v) == 24][-7:])
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return start
    args.output.mkdir(parents=True, exist_ok=True)
    for name, module in [('old', before), ('new', current)]:
        stats = {}
        for h, value in sorted(hours.items()):
            stats = module.update_expected_hourly_demand_stats(
                stats, slot_key=str(h.weekday()*24+h.hour), measured_kwh=value)
        table = module.build_expected_hourly_demand_table(stats, daily_average_kwh=average)
        with patch.object(module, 'datetime', Clock):
            demand = module.build_hourly_home_demand_forecast(
                non_heating_daily_average_kwh=average, heating_estimate_kwh=0,
                hourly_demand_table=table, horizon_end=start+timedelta(days=1))
        prices, solar = [], []
        for i in range(48):
            at = start + timedelta(hours=i)
            price = .12 if 13 <= at.hour < 16 else .38 if 17 <= at.hour < 22 else .24
            prices.append(PlannerWindow(at, at+timedelta(hours=1), price))
            solar.append(dict(start=at.isoformat(), end=(at+timedelta(hours=1)).isoformat(),
                              estimated_kwh=.8 if 10 <= at.hour < 16 else 0))
        if args.prices:
            finish = start+timedelta(days=2)
            prices = [PlannerWindow(max(p.start, start), min(p.end, finish), p.price)
                      for p in price_history(args.prices) if p.start < finish and p.end > start]
        demand = module.align_price_responsive_demand_to_cheap_hours(demand, prices)
        data = dict(upcoming_energy_price_windows=[dict(start=p.start.isoformat(),
                     end=p.end.isoformat(), price=p.price, price_known=True) for p in prices],
                    estimated_hourly_solar_forecast=solar, estimated_hourly_home_demand=demand)
        with patch.object(Path, 'read_text', return_value=json.dumps(data)):
            _, _, result = replay_full_plan(timestamp=start.isoformat(), soc=60, discharging=True,
                                           reserve=60, tax_deduction=.11, isolated_cycles=True)
        snapshot = asdict(result)
        snapshot['battery_capacity_kwh'] = 10
        snapshot['battery_min_soc_percent'] = 20
        snapshot['scenario_note'] = ('CSV-trained demand; CSV tariffs if provided, otherwise synthetic; '
                                     'synthetic solar; assumed 60% SOC, '
                                     '10 kWh battery, 3 kW charge/discharge, zero extra forecast margins.')
        chart = plot_data(snapshot, 3, 3, name)
        assert min(v for _, v in chart['soc']) >= 19.95
        assert max(v for _, v in chart['soc']) <= 100.05
        (args.output / f'{name}.json').write_text(json.dumps(snapshot, indent=2))
    print(render(args.output/'new.json', args.output/'planning.html', 3, 3, args.output/'old.json'))


if __name__ == '__main__':
    main()
