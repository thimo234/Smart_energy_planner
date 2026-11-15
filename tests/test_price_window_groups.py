"""Grouped price planners keep independent identities and registry ownership."""
import unittest
from types import SimpleNamespace as NS
from unittest.mock import Mock, AsyncMock, patch

from test_household import entry, sub
from test_household_flows import load_flow, load_runtime
from custom_components.smart_energy_planner import household as h
from custom_components.smart_energy_planner.const import DOMAIN, RUNTIME_STATE
from custom_components.smart_energy_planner.diagnostics import async_get_config_entry_diagnostics

MEMBERS = '_price_window_planners'


def grouped():
    home = entry(price_sensor='sensor.price')
    home.subentries = {'prices': sub('prices', 'price_window', **{MEMBERS: {
        'old': {'planner_kind': 'price_window', 'planner_name': 'One hour',
                '_legacy_entry_id': 'old', 'price_window_duration_hours': 1},
        'three': {'planner_kind': 'price_window', 'planner_name': 'Three hours',
                  '_legacy_entry_id': 'three', 'price_window_duration_hours': 3},
    }})}
    return home


class GroupBindingsTest(unittest.IsolatedAsyncioTestCase):
    def test_removed_member_archive_can_be_explicitly_adopted_again(self):
        home = grouped()
        archive = entry('removed', 'price_window', _adopted_by={'entry_id': 'home', 'subentry_id': 'prices'})
        manager = NS(async_entries=lambda d: [home, archive],
                     async_get_entry=lambda identity: home if identity == 'home' else archive)
        self.assertTrue(h.legacy_available(NS(config_entries=manager), archive))

    def test_group_ui_steps_have_translation_labels(self):
        import json
        from pathlib import Path
        root = Path(__file__).resolve().parents[1] / 'custom_components/smart_energy_planner'
        for filename in ('strings.json', 'translations/en.json', 'translations/nl.json'):
            data = json.loads((root / filename).read_text())
            steps = data['config_subentries']['price_window']['step']
            self.assertEqual(set(steps['members']['data']), {'member_id', 'action'})
            self.assertIn('confirm', steps['remove_member']['data'])

    async def test_members_have_distinct_configs_and_shared_subentry(self):
        bindings = h.planner_bindings(grouped())
        self.assertEqual([b.entry_id for b in bindings], ['old', 'three'])
        self.assertEqual([b.subentry_id for b in bindings], ['prices', 'prices'])
        self.assertEqual([b.data['price_window_duration_hours'] for b in bindings], [1, 3])
        self.assertEqual([b.title for b in bindings], ['One hour', 'Three hours'])
        self.assertTrue(all(b.data['price_sensor'] == 'sensor.price' for b in bindings))

    async def test_platforms_check_individual_runtime_ownership(self):
        home = grouped()
        one, three = object(), object()
        hass = NS(data={DOMAIN: {'home': {'old': one, 'three': three}, 'old': one, 'three': three}})
        setup, add = AsyncMock(), Mock()
        await h.async_setup_platform(hass, home, add, setup)
        self.assertEqual(setup.await_count, 2)
        for call in setup.await_args_list:
            call.args[2]([])
        self.assertEqual([c.kwargs for c in add.call_args_list],
                         [{'config_subentry_id': 'prices'}] * 2)

    async def test_diagnostics_reports_each_owned_member(self):
        home = grouped()
        one, three = NS(data=None), NS(data=None)
        hass = NS(data={DOMAIN: {'home': {'old': one, 'three': three}, 'old': one, 'three': three}})
        report = await async_get_config_entry_diagnostics(hass, home)
        self.assertEqual([p['coordinator_available'] for p in report['planners']], [True, True])
        self.assertEqual([p['subentry_id'] for p in report['planners']], ['prices', 'prices'])

    async def test_removed_members_are_cleaned_only_within_their_group(self):
        home = grouped()
        home.subentries['prices'].data['_removed_price_window_planners'] = ['gone']
        records = {'sensor.gone': NS(config_entry_id='home', config_subentry_id='prices', unique_id='gone_plan'),
                   'sensor.foreign': NS(config_entry_id='other', config_subentry_id='prices', unique_id='gone_plan'),
                   'sensor.other': NS(config_entry_id='home', config_subentry_id='prices', unique_id='old_plan')}
        for identity, record in records.items():
            record.entity_id = identity
        er = NS(entities=records, async_remove=Mock(side_effect=lambda identity: records.pop(identity)))
        devices = {'gone': NS(id='gone', config_entry_id='home', config_subentry_id='prices',
                              identifiers={(DOMAIN, 'gone')})}
        dr = NS(devices=devices, async_remove_device=Mock(side_effect=lambda identity: devices.pop(identity)))
        manager = NS(async_update_subentry=Mock(side_effect=lambda p, s, **kw: s.__dict__.update(kw)))
        cleanup = getattr(h, 'cleanup_removed_price_windows', None)
        self.assertTrue(callable(cleanup), 'removed members need registry cleanup after unload')
        cleanup(NS(config_entries=manager), home, er, dr)
        self.assertEqual(set(records), {'sensor.foreign', 'sensor.other'})
        self.assertEqual(devices, {})
        self.assertNotIn('_removed_price_window_planners', home.subentries['prices'].data)

    async def test_setup_and_unload_keep_both_runtimes(self):
        runtime, doubles, _ = load_runtime()
        home = grouped()
        home.async_on_unload = Mock()
        home.add_update_listener = Mock()
        hass = NS(data={}, state='starting', bus=NS(async_listen_once=Mock(return_value=Mock())), config_entries=NS(
            async_get_entry=lambda identity: None, async_entries=lambda domain: [home],
            async_forward_entry_setups=AsyncMock(), async_unload_platforms=AsyncMock(return_value=True)))
        with patch.dict('sys.modules', doubles):
            self.assertTrue(await runtime.async_setup_entry(hass, home))
            self.assertEqual(set(hass.data[DOMAIN]['home']), {'old', 'three'})
            self.assertIsNot(hass.data[DOMAIN]['old'], hass.data[DOMAIN]['three'])
            self.assertTrue(await runtime.async_unload_entry(hass, home))
        self.assertEqual(hass.data[DOMAIN], {})
        self.assertEqual(hass.data[RUNTIME_STATE], {})


class GroupMigrationTest(unittest.TestCase):
    def fixture(self):
        home = entry()
        home.subentries = {'one': sub('one', 'price_window', _legacy_entry_id='old',
                                     price_window_duration_hours=1),
                           'three': sub('three', 'price_window', price_window_duration_hours=3),
                           'battery': sub('battery')}
        old = entry('old', 'price_window')
        old.data['_adopted_by'] = {'entry_id': 'home', 'subentry_id': 'one'}
        records = {key: NS(entity_id='sensor.' + key, unique_id=key + '_plan',
                          config_entry_id='home', config_subentry_id=key,
                          name='Custom ' + key, device_id='device_' + key)
                   for key in ('one', 'three')}
        devices = {key: NS(id=key, config_entry_id='home', config_subentry_id=key)
                   for key in ('one', 'three')}
        events = []
        def update_sub(parent, child, **kw):
            parent.subentries[child.subentry_id] = NS(**{**child.__dict__, **kw})
        def update_entity(identity, **kw):
            events.append('entity')
            record = next(v for v in records.values() if v.entity_id == identity)
            record.__dict__.update(kw)
        def update_device(identity, **kw):
            events.append('device')
            devices[identity].config_subentry_id = kw['new_config_subentry_id']
        manager = NS(async_update_subentry=Mock(side_effect=update_sub),
                     async_remove_subentry=Mock(side_effect=lambda p, s: p.subentries.pop(s)),
                     async_get_entry=lambda identity: old if identity == 'old' else None,
                     async_update_entry=Mock(side_effect=lambda e, **kw: e.__dict__.update(kw)))
        return (home, old, records, devices, events, NS(config_entries=manager),
                NS(entities=records, async_update_entity=Mock(side_effect=update_entity)),
                NS(devices=devices, async_update_device=Mock(side_effect=update_device)))

    def test_consolidate_preserves_ids_settings_archives_and_is_idempotent(self):
        home, old, records, devices, events, hass, er, dr = self.fixture()
        migrate = getattr(h, 'consolidate_price_windows', None)
        self.assertTrue(callable(migrate), 'household group consolidation is missing')
        before = [(r.entity_id, r.unique_id, r.name, r.device_id) for r in records.values()]
        migrate(hass, home, er, dr)
        self.assertEqual(set(home.subentries), {'one', 'battery'})
        bindings = h.planner_bindings(home)[:2]
        self.assertEqual([b.entry_id for b in bindings], ['old', 'three'])
        self.assertEqual([b.data['price_window_duration_hours'] for b in bindings], [1, 3])
        self.assertEqual(records['three'].config_subentry_id, 'one')
        self.assertEqual(devices['three'].config_subentry_id, 'one')
        self.assertEqual(old.data['_adopted_by']['subentry_id'], 'one')
        self.assertEqual(before, [(r.entity_id, r.unique_id, r.name, r.device_id) for r in records.values()])
        self.assertEqual(events, ['entity', 'device'])
        calls = hass.config_entries.async_update_subentry.call_count
        migrate(hass, home, er, dr)
        self.assertEqual(hass.config_entries.async_update_subentry.call_count, calls)
        self.assertEqual(events, ['entity', 'device'])

    def test_consolidation_preserves_pending_member_removals(self):
        home, old, records, devices, events, hass, er, dr = self.fixture()
        home.subentries['three'].data['_removed_price_window_planners'] = ['removed']
        h.consolidate_price_windows(hass, home, er, dr)
        self.assertEqual(home.subentries['one'].data['_removed_price_window_planners'], ['removed'])

    def test_interrupted_registry_transfer_retries_without_removing_children(self):
        home, old, records, devices, events, hass, er, dr = self.fixture()
        migrate = getattr(h, 'consolidate_price_windows', None)
        self.assertTrue(callable(migrate), 'household group consolidation is missing')
        original = er.async_update_entity.side_effect
        er.async_update_entity.side_effect = RuntimeError('interrupt')
        with self.assertRaises(RuntimeError):
            migrate(hass, home, er, dr)
        self.assertEqual(set(home.subentries), {'one', 'three', 'battery'})
        dr.async_update_device.assert_not_called()
        hass.config_entries.async_remove_subentry.assert_not_called()
        er.async_update_entity.side_effect = original
        migrate(hass, home, er, dr)
        self.assertEqual(set(home.subentries), {'one', 'battery'})


class GroupDeletionRetryTest(unittest.IsolatedAsyncioTestCase):
    def fixture(self):
        import json
        home, old, records, devices, events, hass, er, dr = GroupMigrationTest().fixture()
        # HA removes the subentry via the parent's update, then registry listeners
        # remove its owned records. Device removal also removes linked entities.
        for key, record in records.items():
            record.device_id = key
        def remove(parent, identity):
            remaining = dict(parent.subentries)
            remaining.pop(identity)
            parent.subentries = remaining
            removed_devices = {key for key, device in devices.items()
                               if device.config_subentry_id == identity}
            for key, record in list(records.items()):
                if record.config_subentry_id == identity or record.device_id in removed_devices:
                    records.pop(key)
            for key in removed_devices:
                devices.pop(key)
        hass.config_entries.async_remove_subentry.side_effect = remove
        hass.config_entries.async_get_entry = lambda identity: {'home': home, 'old': old}.get(identity)
        hass.config_entries.async_entries = lambda domain: [home, old]
        def restart():
            # Only persisted JSON data survives; no in-memory source ownership.
            home.data = json.loads(json.dumps(home.data))
            old.data = json.loads(json.dumps(old.data))
            home.subentries = {key: NS(**json.loads(json.dumps(child.__dict__)))
                               for key, child in home.subentries.items()}
        return home, old, records, devices, hass, er, dr, restart

    async def test_retirement_write_interruptions_resume_or_delete_without_resurrection(self):
        for stage in ('retirement', 'snapshot'):
            for after_write in (False, True):
                for delete_target in (False, True):
                    with self.subTest(stage=stage, after_write=after_write, delete_target=delete_target):
                        home, old, records, devices, hass, er, dr, restart = self.fixture()
                        manager = hass.config_entries
                        callback = manager.async_update_entry if stage == 'retirement' else manager.async_update_subentry
                        original = callback.side_effect
                        def interrupt(*args, **kwargs):
                            if after_write:
                                original(*args, **kwargs)
                            raise RuntimeError('persistence interrupt')
                        callback.side_effect = interrupt
                        with self.assertRaisesRegex(RuntimeError, 'persistence interrupt'):
                            h.consolidate_price_windows(hass, home, er, dr)
                        callback.side_effect = original
                        er.async_update_entity.assert_not_called()
                        dr.async_update_device.assert_not_called()
                        retirement_persisted = stage == 'snapshot' or after_write
                        if delete_target:
                            manager.async_remove_subentry(home, 'one')
                        restart()
                        if retirement_persisted:
                            self.assertNotIn('three', [b.subentry_id for b in h.planner_bindings(home)])
                        h.consolidate_price_windows(hass, home, er, dr)
                        restart()
                        if delete_target and retirement_persisted:
                            self.assertEqual([b.entry_id for b in h.planner_bindings(home)], ['battery'])
                            self.assertEqual(records, {})
                            self.assertEqual(devices, {})
                        elif not delete_target:
                            self.assertEqual([b.entry_id for b in h.planner_bindings(home)], ['old', 'three', 'battery'])
                            self.assertEqual({r.config_subentry_id for r in records.values()}, {'one'})
                        else:
                            # A write failing BEFORE persistence has not retired
                            # anything and must not discard an unrelated source.
                            self.assertEqual([b.entry_id for b in h.planner_bindings(home)], ['three', 'battery'])

    async def test_retirement_before_snapshot_keeps_legacy_claim_until_owner_deleted(self):
        home, old, records, devices, hass, er, dr, restart = self.fixture()
        archive = entry('three', 'price_window', _adopted_by={
            'entry_id': 'home', 'subentry_id': 'three'})
        recovery = entry('recovery')
        recovery.subentries = {'recovered': sub('recovered', 'price_window', _legacy_entry_id='three')}
        entries = {'home': home, 'old': old, 'three': archive}
        hass.config_entries.async_get_entry = entries.get
        hass.config_entries.async_entries = lambda domain: list(entries.values())
        hass.data = {DOMAIN: {}}
        hass.config_entries.async_update_subentry.side_effect = RuntimeError('snapshot interrupt')
        with self.assertRaisesRegex(RuntimeError, 'snapshot interrupt'):
            h.consolidate_price_windows(hass, home, er, dr)
        restart()
        self.assertFalse(h.legacy_available(hass, archive))
        entries['recovery'] = recovery
        with self.assertRaisesRegex(ValueError, 'already claimed'):
            await h.async_adopt_legacy(hass, recovery, h.planner_bindings(recovery)[0], er, dr)
        self.assertEqual(archive.data['_adopted_by']['subentry_id'], 'three')
        hass.config_entries.async_remove_subentry(home, 'one')
        restart()
        self.assertFalse(h.legacy_available(hass, archive))  # configured recovery still claims it
        del entries['recovery']
        self.assertTrue(h.legacy_available(hass, archive))

    async def test_edit_before_snapshot_keeps_exclusive_claim_through_restart_and_retry(self):
        home, old, records, devices, hass, er, dr, restart = self.fixture()
        archive = entry('three', 'price_window', _adopted_by={
            'entry_id': 'home', 'subentry_id': 'three'})
        recovery = grouped()
        recovery.entry_id = 'recovery'
        recovery.subentries['prices'].data[MEMBERS] = {}
        entries = {'home': home, 'old': old, 'three': archive, 'recovery': recovery}
        hass.config_entries.async_get_entry = entries.get
        hass.config_entries.async_entries = lambda domain: list(entries.values())
        hass.data = {DOMAIN: {}}
        hass.states = NS(async_all=lambda *args: [])
        flows = load_flow()
        def flow_for(parent, context):
            flow = flows.PlannerSubentryFlow()
            flow.handler, flow.context, flow.hass = (parent.entry_id, 'price_window'), context, hass
            flow._get_entry = lambda: parent
            return flow
        # An already-open confirmation must recheck ownership at submission too.
        pending_adoption = flow_for(recovery, {'source': 'user'})
        pending_adoption._legacy_id = 'three'
        update = hass.config_entries.async_update_subentry.side_effect
        hass.config_entries.async_update_subentry.side_effect = RuntimeError('snapshot interrupt')
        with self.assertRaisesRegex(RuntimeError, 'snapshot interrupt'):
            h.consolidate_price_windows(hass, home, er, dr)
        hass.config_entries.async_update_subentry.side_effect = update
        self.assertEqual(home.data['_price_window_retired_sources'], {'three': 'one'})
        self.assertNotIn('_price_window_group_sources', home.subentries['one'].data)
        restart()
        edit = flow_for(home, {'source': 'reconfigure', 'subentry_id': 'one'})
        await edit.async_step_reconfigure()
        await edit.async_step_members({'member_id': 'old', 'action': 'edit'})
        result = await edit.async_step_configure({
            'planner_name': 'Edited before snapshot', 'price_window_duration_hours': 4})
        self.assertEqual(result.get('reason'), 'reconfigure_successful')
        self.assertEqual(set(home.subentries['one'].data[MEMBERS]), {'old'})
        restart()
        # Grouped shape and a missing member do not prove snapshot/removal.
        with self.subTest(guard='selection'):
            result = await flow_for(recovery, {'source': 'user'}).async_step_user({'legacy_entry_id': 'three'})
            self.assertEqual(result.get('reason'), 'legacy_unavailable')
        with self.subTest(guard='confirmation'):
            result = await pending_adoption.async_step_adopt({'confirm': True})
            self.assertEqual(result.get('reason'), 'legacy_unavailable')
        with self.subTest(guard='configured_claim'):
            self.assertEqual(h.legacy_claims(hass, 'three'), [('home', 'one')])
        recovery.subentries['prices'].data[MEMBERS] = {}
        with self.subTest(guard='setup'):
            candidate = h.PlannerBinding(recovery, recovery.subentries['prices'],
                ('three', {'_legacy_entry_id': 'three', 'planner_kind': 'price_window'}))
            with self.assertRaisesRegex(ValueError, 'already claimed'):
                await h.async_adopt_legacy(hass, recovery, candidate, er, dr)
        h.consolidate_price_windows(hass, home, er, dr)
        restart()
        self.assertEqual(set(home.subentries), {'one', 'battery'})
        self.assertEqual(home.subentries['one'].data[MEMBERS]['old']['price_window_duration_hours'], 4)
        self.assertEqual(h.legacy_claims(hass, 'three'), [('home', 'one')])
        self.assertEqual(archive.data['_adopted_by'], {'entry_id': 'home', 'subentry_id': 'one'})
        self.assertEqual({r.config_subentry_id for r in records.values()}, {'one'})
        self.assertEqual({d.config_subentry_id for d in devices.values()}, {'one'})
        # Once snapshotted, explicit member removal really does release ownership.
        remove = flow_for(home, {'source': 'reconfigure', 'subentry_id': 'one'})
        await remove.async_step_reconfigure()
        await remove.async_step_members({'member_id': 'three', 'action': 'remove'})
        await remove.async_step_remove_member({'confirm': True})
        restart()
        self.assertTrue(h.legacy_available(hass, archive))
        result = await flow_for(recovery, {'source': 'user'}).async_step_user({'legacy_entry_id': 'three'})
        self.assertEqual(result.get('step_id'), 'adopt')

    async def test_retired_legacy_source_is_recoverable_only_explicitly(self):
        home, old, records, devices, hass, er, dr, restart = self.fixture()
        source_archive = entry('three', 'price_window', _adopted_by={
            'entry_id': 'home', 'subentry_id': 'three'})
        entries = {'home': home, 'old': old, 'three': source_archive}
        hass.config_entries.async_get_entry = entries.get
        hass.config_entries.async_entries = lambda domain: list(entries.values())
        hass.data = {DOMAIN: {}}
        er.async_update_entity.side_effect = RuntimeError('interrupt')
        with self.assertRaises(RuntimeError):
            h.consolidate_price_windows(hass, home, er, dr)
        self.assertFalse(h.legacy_available(hass, source_archive))
        hass.config_entries.async_remove_subentry(home, 'one')
        restart()
        self.assertTrue(h.legacy_available(hass, source_archive))
        self.assertEqual(source_archive.data['_adopted_by']['subentry_id'], 'three')
        # Explicit adoption may recover an orphan archive even while its old,
        # independently retired source awaits cleanup. It never starts itself.
        recovery = entry('recovery')
        recovery.subentries = {'recovered': sub('recovered', 'price_window', _legacy_entry_id='three')}
        entries['recovery'] = recovery
        er.async_update_entity.side_effect = lambda *a, **kw: None
        await h.async_adopt_legacy(hass, recovery, h.planner_bindings(recovery)[0], er, dr)
        self.assertEqual(source_archive.data['_adopted_by'], {
            'entry_id': 'recovery', 'subentry_id': 'recovered'})
        self.assertEqual([b.entry_id for b in h.planner_bindings(home)], ['battery'])

    async def test_deleted_group_source_cleanup_retries_both_sides_of_remove(self):
        for after_write in (False, True):
            with self.subTest(after_write=after_write):
                home, old, records, devices, hass, er, dr, restart = self.fixture()
                er.async_update_entity.side_effect = RuntimeError('registry interrupt')
                with self.assertRaises(RuntimeError):
                    h.consolidate_price_windows(hass, home, er, dr)
                manager = hass.config_entries
                manager.async_remove_subentry(home, 'one')
                original = manager.async_remove_subentry.side_effect
                def interrupt(*args):
                    if after_write:
                        original(*args)
                    raise RuntimeError('remove interrupt')
                manager.async_remove_subentry.side_effect = interrupt
                with self.assertRaisesRegex(RuntimeError, 'remove interrupt'):
                    h.consolidate_price_windows(hass, home, er, dr)
                restart()
                self.assertEqual([b.entry_id for b in h.planner_bindings(home)], ['battery'])
                manager.async_remove_subentry.side_effect = original
                h.consolidate_price_windows(hass, home, er, dr)
                self.assertEqual(set(home.subentries), {'battery'})
                self.assertEqual(records, {})
                self.assertEqual(devices, {})

    async def test_deleted_group_does_not_resurrect_source_after_restart(self):
        for after_entity_write in (False, True):
            with self.subTest(after_entity_write=after_entity_write):
                home, old, records, devices, hass, er, dr, restart = self.fixture()
                original = er.async_update_entity.side_effect
                def interrupt(*args, **kwargs):
                    if after_entity_write:
                        original(*args, **kwargs)
                    raise RuntimeError('registry interrupt')
                er.async_update_entity.side_effect = interrupt
                with self.assertRaisesRegex(RuntimeError, 'registry interrupt'):
                    h.consolidate_price_windows(hass, home, er, dr)
                self.assertEqual(set(home.subentries['one'].data[MEMBERS]), {'old', 'three'})
                hass.config_entries.async_remove_subentry(home, 'one')
                restart()
                self.assertEqual([b.entry_id for b in h.planner_bindings(home)], ['battery'])
                # Both new and already-open source forms must stay inert even
                # before setup has had a chance to finish the interrupted cleanup.
                flow = load_flow().PlannerSubentryFlow()
                flow.handler = ('home', 'price_window')
                flow.context = {'source': 'reconfigure', 'subentry_id': 'three'}
                flow._get_entry = lambda: home
                flow.hass = NS(config_entries=hass.config_entries)
                self.assertEqual((await flow.async_step_reconfigure()).get('reason'), 'legacy_unavailable')
                flow._member_id = 'three'
                self.assertEqual(flow._save_price_member({'planner_name': 'Stale'}, 'three').get('reason'),
                                 'legacy_unavailable')
                self.assertEqual((await flow.async_step_remove_member({'confirm': True})).get('reason'),
                                 'legacy_unavailable')
                add_flow = load_flow().PlannerSubentryFlow()
                add_flow.handler, add_flow.context = ('home', 'price_window'), {'source': 'user'}
                add_flow._get_entry = lambda: home
                add_flow.hass = flow.hass
                add_flow.async_create_entry = Mock()
                self.assertEqual(add_flow._save_price_member({'planner_name': 'Add'}).get('reason'),
                                 'legacy_unavailable')
                add_flow.async_create_entry.assert_not_called()
                # No registry move or recreation is allowed on setup retry.
                er.async_update_entity.side_effect = original
                er.async_update_entity.reset_mock()
                dr.async_update_device.reset_mock()
                h.consolidate_price_windows(hass, home, er, dr)
                restart()
                h.consolidate_price_windows(hass, home, er, dr)
                self.assertEqual(set(home.subentries), {'battery'})
                self.assertEqual(records, {})
                self.assertEqual(devices, {})
                er.async_update_entity.assert_not_called()
                dr.async_update_device.assert_not_called()
                self.assertIn('_adopted_by', old.data)
                self.assertTrue(h.legacy_available(hass, old))
                runtime, doubles, _ = load_runtime()
                home.async_on_unload = Mock()
                home.add_update_listener = Mock()
                hass.data, hass.state = {}, 'starting'
                hass.bus = NS(async_listen_once=Mock(return_value=Mock()))
                hass.config_entries.async_forward_entry_setups = AsyncMock()
                hass.config_entries.async_unload_platforms = AsyncMock(return_value=True)
                with patch.dict('sys.modules', doubles):
                    self.assertTrue(await runtime.async_setup_entry(hass, home))
                    self.assertEqual(set(hass.data[DOMAIN]['home']), {'battery'})
                    self.assertNotIn('three', hass.data[DOMAIN])
                    self.assertTrue(await runtime.async_unload_entry(hass, home))


class GroupRetryMutationTest(unittest.IsolatedAsyncioTestCase):
    async def test_remove_after_interruption_is_not_resurrected_on_retry(self):
        home, old, records, devices, events, hass, er, dr = GroupMigrationTest().fixture()
        original = er.async_update_entity.side_effect
        er.async_update_entity.side_effect = RuntimeError('interrupt')
        with self.assertRaises(RuntimeError):
            h.consolidate_price_windows(hass, home, er, dr)
        flow = load_flow().PlannerSubentryFlow()
        flow.handler, flow.context = ('home', 'price_window'), {'subentry_id': 'one', 'source': 'reconfigure'}
        flow._get_entry = lambda: home
        flow.hass = NS(states=NS(async_all=lambda *a: []), config_entries=hass.config_entries)
        await flow.async_step_reconfigure()
        await flow.async_step_members({'member_id': 'three', 'action': 'remove'})
        await flow.async_step_remove_member({'confirm': True})
        er.async_update_entity.side_effect = original
        h.consolidate_price_windows(hass, home, er, dr)
        self.assertNotIn('three', home.subentries['one'].data[MEMBERS])
        self.assertEqual(home.subentries['one'].data['_removed_price_window_planners'], ['three'])
        er.async_remove = Mock(side_effect=lambda identity: records.pop(identity.removeprefix('sensor.')))
        devices['one'].identifiers = {(DOMAIN, 'old')}
        devices['three'].identifiers = {(DOMAIN, 'three')}
        dr.async_remove_device = Mock(side_effect=lambda identity: devices.pop(identity))
        h.cleanup_removed_price_windows(hass, home, er, dr)
        self.assertEqual(set(records), {'one'})
        self.assertEqual(set(devices), {'one'})
        self.assertEqual([binding.entry_id for binding in h.planner_bindings(home)], ['old', 'battery'])

    async def test_edit_after_interruption_is_authoritative_on_retry(self):
        home, old, records, devices, events, hass, er, dr = GroupMigrationTest().fixture()
        original = er.async_update_entity.side_effect
        er.async_update_entity.side_effect = RuntimeError('interrupt')
        with self.assertRaises(RuntimeError):
            h.consolidate_price_windows(hass, home, er, dr)
        flow = load_flow().PlannerSubentryFlow()
        flow.handler, flow.context = ('home', 'price_window'), {'subentry_id': 'one', 'source': 'reconfigure'}
        flow._get_entry = lambda: home
        flow.hass = NS(states=NS(async_all=lambda *a: []), config_entries=hass.config_entries)
        await flow.async_step_reconfigure()
        await flow.async_step_members({'member_id': 'three', 'action': 'edit'})
        result = await flow.async_step_configure({'planner_name': 'Edited', 'price_window_duration_hours': 4})
        self.assertEqual(result['type'], 'abort')
        er.async_update_entity.side_effect = original
        h.consolidate_price_windows(hass, home, er, dr)
        self.assertEqual(home.subentries['one'].data[MEMBERS]['three']['price_window_duration_hours'], 4)
        self.assertEqual(set(home.subentries), {'one', 'battery'})


class GroupFlowTest(unittest.IsolatedAsyncioTestCase):
    async def test_open_source_flow_cannot_mutate_snapshotted_source(self):
        home, old, records, devices, events, hass, er, dr = GroupMigrationTest().fixture()
        flow = load_flow().PlannerSubentryFlow()
        flow.handler, flow.context = ('home', 'price_window'), {'source': 'reconfigure', 'subentry_id': 'three'}
        flow._get_entry = lambda: home
        flow.hass = NS(states=NS(async_all=lambda *a: []), config_entries=hass.config_entries)
        await flow.async_step_reconfigure()
        await flow.async_step_members({'member_id': 'three', 'action': 'edit'})
        er.async_update_entity.side_effect = RuntimeError('interrupt')
        with self.assertRaises(RuntimeError):
            h.consolidate_price_windows(hass, home, er, dr)
        hass.config_entries.async_update_subentry.reset_mock()
        result = await flow.async_step_configure({'planner_name': 'Obsolete source'})
        self.assertEqual(result.get('reason'), 'legacy_unavailable')
        hass.config_entries.async_update_subentry.assert_not_called()

    async def test_concurrent_first_adds_cannot_create_two_groups(self):
        home = grouped()
        home.subentries.clear()
        flow = load_flow().PlannerSubentryFlow()
        flow.handler, flow.context = ('home', 'price_window'), {'source': 'user'}
        flow._get_entry = lambda: home
        flow.hass = NS(states=NS(async_all=lambda *a: []), config_entries=NS(async_update_subentry=Mock()))
        await flow.async_step_configure()
        home.subentries.update(grouped().subentries)
        flow.async_create_entry = Mock(return_value={'type': 'create_entry'})
        result = await flow.async_step_configure({'planner_name': 'Second'})
        self.assertEqual(result.get('reason'), 'legacy_unavailable')
        flow.async_create_entry.assert_not_called()
        flow.hass.config_entries.async_update_subentry.assert_not_called()

    async def test_group_success_and_actions_use_translations(self):
        import json
        from pathlib import Path
        home = grouped()
        flow = load_flow().PlannerSubentryFlow()
        flow.handler, flow.context = ('home', 'price_window'), {'source': 'reconfigure', 'subentry_id': 'prices'}
        flow._get_entry = lambda: home
        flow.hass = NS(config_entries=NS(async_update_subentry=Mock()))
        result = await flow.async_step_reconfigure()
        action = next(value for key, value in result['data_schema'].schema.items() if key.schema == 'action')
        self.assertEqual(action.config.config.get('translation_key'), 'price_window_action')
        root = Path(__file__).resolve().parents[1] / 'custom_components/smart_energy_planner'
        for filename in ('strings.json', 'translations/en.json', 'translations/nl.json'):
            data = json.loads((root / filename).read_text())
            self.assertEqual(set(data['selector']['price_window_action']['options']), {'edit', 'add', 'remove'})
        result = flow._save_price_member({'planner_name': 'Added'})
        self.assertEqual(result.get('translation_domain'), 'homeassistant')
        flow._member_id = 'three'
        result = await flow.async_step_remove_member({'confirm': True})
        self.assertEqual(result.get('translation_domain'), 'homeassistant')

    async def test_stale_group_submissions_abort_before_ha_create_or_write(self):
        # HA 2026.11.1 ConfigSubentryFlow.async_create_entry only accepts
        # SOURCE_USER; _get_reconfigure_subentry raises for missing IDs.
        for source, action, replacement in (
                ('reconfigure', 'edit', False), ('reconfigure', 'edit', True),
                ('reconfigure', 'add', False), ('reconfigure', 'add', True),
                ('reconfigure', 'remove', True), ('user', 'add', True),
                ('user', 'add', False)):
            with self.subTest(source=source, action=action, replacement=replacement):
                home = grouped()
                flow = load_flow().PlannerSubentryFlow()
                flow.handler = ('home', 'price_window')
                flow.context = {'source': source}
                if source == 'reconfigure':
                    flow.context['subentry_id'] = 'prices'
                flow._get_entry = lambda: home
                flow.hass = NS(states=NS(async_all=lambda *a: []),
                               config_entries=NS(async_entries=lambda domain: [], async_update_subentry=Mock()))
                def ha_create(**kw):
                    if flow.context['source'] != 'user':
                        raise ValueError('Source is reconfigure, expected user')
                    return {'type': 'create_entry', **kw}
                flow.async_create_entry = Mock(side_effect=ha_create)
                if source == 'reconfigure':
                    await flow.async_step_reconfigure()
                    await flow.async_step_members({'member_id': 'three', 'action': action})
                    if action == 'add':
                        await flow.async_step_user({'legacy_entry_id': 'new'})
                else:
                    await flow.async_step_user({'legacy_entry_id': 'new'})
                del home.subentries['prices']
                if replacement:
                    home.subentries['replacement'] = sub('replacement', 'price_window', **{
                        MEMBERS: {'three': {'planner_name': 'Replacement', '_legacy_entry_id': 'three'}}})
                if action == 'remove':
                    result = await flow.async_step_remove_member({'confirm': True})
                else:
                    result = await flow.async_step_configure({'planner_name': 'Stale'})
                self.assertEqual(result.get('reason'), 'legacy_unavailable')
                flow.async_create_entry.assert_not_called()
                flow.hass.config_entries.async_update_subentry.assert_not_called()

    async def test_reconfigure_missing_original_group_aborts_before_ha_lookup(self):
        home = grouped()
        del home.subentries['prices']
        flow = load_flow().PlannerSubentryFlow()
        flow.handler, flow.context = ('home', 'price_window'), {'source': 'reconfigure', 'subentry_id': 'prices'}
        flow._get_entry = lambda: home
        self.assertEqual((await flow.async_step_reconfigure())['reason'], 'legacy_unavailable')

    async def test_add_appends_to_existing_group_instead_of_creating_subentry(self):
        flows = load_flow()
        home = grouped()
        flow = flows.PlannerSubentryFlow()
        flow.handler, flow.context = ('home', 'price_window'), {}
        flow._get_entry = lambda: home
        def update(parent, child, **kw):
            parent.subentries[child.subentry_id] = NS(**{**child.__dict__, **kw})
        flow.hass = NS(config_entries=NS(async_update_subentry=Mock(side_effect=update)))
        result = await flow.async_step_configure({'planner_name': 'Five hours', 'price_window_duration_hours': 5})
        self.assertEqual(result['type'], 'abort')
        self.assertEqual(len(home.subentries['prices'].data[MEMBERS]), 3)
        self.assertEqual(home.subentries['prices'].data[MEMBERS]['old']['price_window_duration_hours'], 1)

    async def test_reconfigure_selects_member_and_preserves_identity(self):
        flows = load_flow()
        home = grouped()
        flow = flows.PlannerSubentryFlow()
        flow.handler, flow.context = ('home', 'price_window'), {'subentry_id': 'prices'}
        flow._get_entry = lambda: home
        flow._get_reconfigure_subentry = lambda: home.subentries['prices']
        flow.hass = NS(states=NS(async_all=lambda *a: []), config_entries=NS(async_update_subentry=Mock()))
        result = await flow.async_step_reconfigure()
        self.assertEqual(result['step_id'], 'members')
        result = await flow.async_step_members({'member_id': 'three', 'action': 'edit'})
        self.assertEqual(result['step_id'], 'configure')
        result = await flow.async_step_configure({'planner_name': 'Renamed', 'price_window_duration_hours': 4})
        self.assertEqual(result['type'], 'abort')
        payload = flow.hass.config_entries.async_update_subentry.call_args.kwargs['data'][MEMBERS]
        self.assertEqual(payload['three']['_legacy_entry_id'], 'three')
        self.assertEqual(payload['three']['price_window_duration_hours'], 4)
        self.assertEqual(payload['old']['price_window_duration_hours'], 1)

    async def test_remove_member_requires_confirmation_and_retains_other_settings(self):
        flows = load_flow()
        home = grouped()
        flow = flows.PlannerSubentryFlow()
        flow.handler, flow.context = ('home', 'price_window'), {'subentry_id': 'prices'}
        flow._get_entry = lambda: home
        flow._get_reconfigure_subentry = lambda: home.subentries['prices']
        flow.hass = NS(states=NS(async_all=lambda *a: []), config_entries=NS(async_update_subentry=Mock()))
        result = await flow.async_step_reconfigure()
        self.assertEqual(result['step_id'], 'members')
        result = await flow.async_step_members({'member_id': 'three', 'action': 'remove'})
        self.assertEqual(result['step_id'], 'remove_member')
        flow.hass.config_entries.async_update_subentry.assert_not_called()
        result = await flow.async_step_remove_member({'confirm': True})
        self.assertEqual(result['type'], 'abort')
        payload = flow.hass.config_entries.async_update_subentry.call_args.kwargs['data'][MEMBERS]
        self.assertEqual(set(payload), {'old'})
        self.assertEqual(flow.hass.config_entries.async_update_subentry.call_args.kwargs['data']
                         ['_removed_price_window_planners'], ['three'])

    async def test_stale_member_edit_cannot_resurrect_removed_identity(self):
        flows = load_flow()
        home = grouped()
        flow = flows.PlannerSubentryFlow()
        flow.handler, flow.context = ('home', 'price_window'), {'subentry_id': 'prices'}
        flow._get_entry = lambda: home
        flow._get_reconfigure_subentry = lambda: home.subentries['prices']
        flow.hass = NS(states=NS(async_all=lambda *a: []), config_entries=NS(async_update_subentry=Mock()))
        await flow.async_step_reconfigure()
        await flow.async_step_members({'member_id': 'three', 'action': 'edit'})
        del home.subentries['prices'].data[MEMBERS]['three']
        result = await flow.async_step_configure({'planner_name': 'Removed'})
        self.assertEqual(result['reason'], 'legacy_unavailable')
        flow.hass.config_entries.async_update_subentry.assert_not_called()
