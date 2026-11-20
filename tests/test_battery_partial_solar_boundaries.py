"""Boundary regressions discovered while replacing the old solar-full contract."""
from datetime import datetime, timedelta
import unittest

from test_battery_energy_accounting import coordinator, energy_trace, replay_full_plan
from partial_solar_assertions import assert_executable_plan


class PartialSolarBoundaryTest(unittest.TestCase):
    def test_expired_97_percent_pv_cannot_borrow_lower_floor_from_tomorrows_full_refill(self):
        c = coordinator()
        c._active_charge_phase_mode = 'laden_met_zonne_energie'
        c._active_charge_phase_end = datetime.fromisoformat('2026-10-05T15:30:00+02:00')
        _, slots, result = replay_full_plan('2026_10_05_reduced_solar',
            '2026-10-05T15:39:59.411439+02:00', soc=97., reserve=60.,
            max_charge=2.5, instance=c, isolated_cycles=True, snapshot_exports=True)
        self.assertEqual(c._battery_cycle_discharge_floor_kwh, 4.)
        assert_executable_plan(self, slots, result.planned_battery_mode_windows,
            initial=9.7, reserve=6., max_charge=2.5)

    def test_observed_complete_grid_cycle_does_not_taint_a_new_solar_phase(self):
        from types import SimpleNamespace
        c = coordinator()
        c.config_entry = SimpleNamespace(data={'battery_capacity_kwh': 10.,
            'battery_min_soc_percent': 20., 'battery_no_charge_min_soc_percent': 60.}, options={})
        now = datetime.fromisoformat('2026-11-20T00:00:00+01:00')
        c._battery_charge_source = 'laden_van_net'
        previous = {'at': now.isoformat(), 'energy': 0., 'actions': [
            {'start': now.isoformat(), 'end': (now+timedelta(hours=8)).isoformat(),
             'mode': 'laden_van_net', 'solar_kw': 0., 'grid_kw': 1.},
            {'start': (now+timedelta(hours=8)).isoformat(),
             'end': (now+timedelta(hours=9)).isoformat(),
             'mode': 'ontladen', 'power': 1.}]}
        c._reconcile_elapsed_battery_phase(previous, now+timedelta(hours=9), 90.)
        self.assertIsNone(c._battery_charge_source)
        c._charge_session_started = True
        c._note_battery_charge_source('laden_met_zonne_energie')
        c._release_solar_charge_hold(4.)
        self.assertFalse(c._charge_session_started)

    def test_below_reserve_partial_pv_does_not_invent_completed_full_cycle(self):
        now = datetime.fromisoformat('2026-09-26T12:04:06.122+02:00')
        c = coordinator()
        energy = 2.
        for _ in range(3):
            _, slots, result = replay_full_plan('2026_09_26_drifting', now.isoformat(),
                soc=energy*10, reserve=60, tax_deduction=.11,
                isolated_cycles=True, instance=c)
            until = now+timedelta(minutes=5)
            commands = [{**w, 'end': min(datetime.fromisoformat(w['end']), until).isoformat()}
                for w in result.planned_battery_mode_windows
                if datetime.fromisoformat(w['start']) < until]
            checked = assert_executable_plan(self, slots, commands,
                initial=energy, reserve=6.)
            energy = checked['energy']
            self.assertLess(energy, 6.)
            self.assertFalse(any(w['mode'].startswith('ontladen') for w in commands))
            now = until
