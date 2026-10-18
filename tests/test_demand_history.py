"""Counter corrections and held-out days in the offline forecast evaluation."""
import csv
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from analyze_demand_history import hourly_history, evaluate, current


class DemandHistoryTest(unittest.TestCase):
    def test_duplicate_timestamp_uses_last_state_and_signed_corrections(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'history.csv'
            with path.open('w', newline='') as stream:
                writer = csv.writer(stream)
                writer.writerow(['entity_id', 'state', 'last_changed'])
                for time, value in [('00:00:00', 100), ('00:15:00', 100.3),
                                    ('00:30:00', 9000), ('00:30:00', 100.2), ('01:00:00', 100.5)]:
                    writer.writerow(['sensor.house', value, '2026-10-01T'+time+'Z'])
            hours, info = hourly_history(path)
            self.assertEqual(info['rejected_intervals'], 0)
            self.assertEqual(len(hours), 1)
            self.assertAlmostEqual(next(iter(hours.values())), .5)

    def test_later_consumption_cannot_change_earlier_forecast(self):
        start = datetime(2026, 10, 1, tzinfo=timezone.utc)
        hours = {start+timedelta(hours=h): .4 for h in range(24*10)}
        _, first = evaluate(current, hours)
        altered = {h: v if h.date().isoformat() < '2026-10-09' else 2 for h, v in hours.items()}
        _, second = evaluate(current, altered)
        self.assertEqual(first['2026-10-08'], second['2026-10-08'])
        self.assertEqual(first['2026-10-09'], second['2026-10-09'])
        self.assertNotEqual(first['2026-10-10'], second['2026-10-10'])


if __name__ == '__main__':
    unittest.main()
