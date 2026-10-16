"""The reusable viewer integrates sources and refuses ambiguous commands."""
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('battery_viewer', Path(__file__).parents[1] / 'scripts/visualize_battery.py')
viewer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(viewer)


class BatteryVisualizationTest(unittest.TestCase):
    def test_peak_bit_returns_low_at_end_and_checks_maximum_duration(self):
        data = dict(planner_kind='price_window', configured_duration_hours=1, minimum_gap_hours=.25,
                    upcoming_energy_price_windows=[dict(start='2026-10-16T10:00:00+02:00',
                        end='2026-10-16T14:00:00+02:00', price=.3)],
                    planned_price_peak_windows=[dict(start='2026-10-16T11:00:00+02:00',
                        end='2026-10-16T12:00:00+02:00')])
        result = viewer.plot_data(data, 2.5, 3, '')
        self.assertEqual([v for _,v in result['soc']], [0,1,0,0])
        data['planned_price_peak_windows'][0]['end'] = '2026-10-16T12:15:00+02:00'
        with self.assertRaisesRegex(ValueError, 'maximale duur'):
            viewer.plot_data(data, 2.5, 3, '')

    def data(self):
        period = dict(start='2026-09-28T12:00:00+02:00', end='2026-09-28T13:00:00+02:00')
        return dict(battery_soc_percent=50, battery_total_energy_kwh=5,
                    battery_energy_available_kwh=3,
                    upcoming_energy_price_windows=[dict(period, price=.3)],
                    estimated_hourly_solar_forecast=[dict(period, estimated_kwh=2)],
                    estimated_hourly_home_demand=[dict(period, estimated_kwh=1)],
                    planned_battery_mode_windows=[dict(period, mode='laden_van_net')])

    def test_grid_includes_solar_and_does_not_clamp_overcharge(self):
        data = self.data()
        self.assertEqual(viewer.plot_data(data, 3, 3, '')['soc'][-1][1], 80)
        self.assertEqual(viewer.plot_data(data, 6, 3, '')['soc'][-1][1], 110)
        data['planned_battery_mode_windows'][0]['mode'] = 'laden_met_zonne_energie'
        self.assertEqual(viewer.plot_data(data, 3, 3, '')['soc'][-1][1], 60)

    def test_overlapping_modes_and_missing_forecast_are_rejected(self):
        data = self.data()
        data['estimated_battery_mode_windows'] = data['planned_battery_mode_windows']
        with self.assertRaisesRegex(ValueError, 'Overlappende'):
            viewer.plot_data(data, 3, 3, '')
        data = self.data()
        data['estimated_hourly_solar_forecast'] = []
        with self.assertRaisesRegex(ValueError, 'Ontbrekende'):
            viewer.plot_data(data, 3, 3, '')
