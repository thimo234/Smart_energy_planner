"""Household composition, stable planner identities and explicit legacy adoption.

This module deliberately does not import Home Assistant: the composition and
migration invariants can be exercised with serialized real-shaped entries.
"""
from __future__ import annotations

import asyncio
import logging
from copy import deepcopy
from time import monotonic
from types import SimpleNamespace

from .const import (
    CONF_EXPORT_PRICE_TAX_DEDUCTION, CONF_PLANNER_KIND, CONF_PLANNER_NAME,
    DEFAULT_EXPORT_PRICE_TAX_DEDUCTION, DOMAIN, PLANNER_KIND_HOUSEHOLD, SHARED_SOURCE_KEYS,
)


_LOGGER = logging.getLogger(__name__)
PRICE_WINDOW_MEMBERS = '_price_window_planners'
_GROUP_SOURCES = '_price_window_group_sources'
_RETIRED_SOURCES = '_price_window_retired_sources'


def price_window_source_retired(entry, subentry_id):
    """Source ownership survives deletion of its authoritative destination."""
    return (subentry_id in entry.data.get(_RETIRED_SOURCES, {})
            or any(subentry_id in child.data.get(_GROUP_SOURCES, [])
                   for child in entry.subentries.values()))


def _price_window_owner(entry, subentry_id):
    """Resolve archive ownership even before the destination snapshot is saved."""
    return entry.data.get(_RETIRED_SOURCES, {}).get(subentry_id, next(
        (child.subentry_id for child in entry.subentries.values()
         if subentry_id in child.data.get(_GROUP_SOURCES, [])), subentry_id))


def price_window_members(subentry):
    """Detach member data; legacy flat subentries are one-member groups."""
    if PRICE_WINDOW_MEMBERS in subentry.data:
        return deepcopy(subentry.data[PRICE_WINDOW_MEMBERS])
    identity = subentry.data.get('_legacy_entry_id', subentry.subentry_id)
    return {identity: {**child_data(subentry.data), '_legacy_entry_id': identity,
                       CONF_PLANNER_NAME: subentry.data.get(CONF_PLANNER_NAME, subentry.title)}}


def consolidate_price_windows(hass, parent, registry, devices):
    """Consolidate this household's price planners without yielding or new IDs.

    Retire sources in the parent before persisting the destination retry journal.
    A deleted destination must never make an interrupted source authoritative.
    Transfer ALL entities before devices (HA device events can delete old owners).
    Remove source subentries only after every registry move and archive retarget.
    A failed callback leaves the journal and source entries for the next setup.
    """
    retired = dict(parent.data.get(_RETIRED_SOURCES, {}))
    for source_id, owner_id in retired.items():
        if owner_id not in parent.subentries and source_id in parent.subentries:
            # HA's normal removal listeners clean entities/devices. Never move
            # these records to a newly created group or replay their old members.
            hass.config_entries.async_remove_subentry(parent, source_id)
    groups = [s for s in parent.subentries.values() if s.subentry_type == 'price_window']
    destinations = [s for s in groups if s.subentry_id not in retired]
    if not destinations:
        return
    target = next((s for s in destinations if _GROUP_SOURCES in s.data), destinations[0])
    sources = list(dict.fromkeys([*target.data.get(_GROUP_SOURCES, []),
                                *(s.subentry_id for s in groups if s is not target)]))
    members = price_window_members(target)
    for source in groups:
        # Journaled sources were already snapshotted. The destination may have
        # been edited since interruption; never replay stale members/removals.
        if source is target or source.subentry_id in target.data.get(_GROUP_SOURCES, []):
            continue
        for identity, data in price_window_members(source).items():
            if identity in members and members[identity] != data:
                raise ValueError('Conflicting price planner identities')
            members.setdefault(identity, data)
    removed = list(dict.fromkeys(identity for group in groups
                   for identity in group.data.get('_removed_price_window_planners', [])))
    final_payload = {PRICE_WINDOW_MEMBERS: members}
    if removed:
        final_payload['_removed_price_window_planners'] = removed
    payload = {**final_payload, _GROUP_SOURCES: sources}
    if sources:
        ownership = {**retired, **dict.fromkeys(sources, target.subentry_id)}
        if ownership != retired:
            # One synchronous persistent write, before the destination becomes
            # authoritative. Failure before it changes nothing; failure after it
            # can resume copying sources unless the user deleted their owner.
            hass.config_entries.async_update_entry(parent, data={
                **parent.data, _RETIRED_SOURCES: ownership})
    if sources and target.data != payload:
        hass.config_entries.async_update_subentry(parent, target, data=payload, title='Price Windows')
        target = parent.subentries[target.subentry_id]
    elif not sources:
        # A singleton can stay flat until another planner is added.
        return
    for entity in list(registry.entities.values()):
        if entity.config_entry_id == parent.entry_id and entity.config_subentry_id in sources:
            registry.async_update_entity(entity.entity_id, config_entry_id=parent.entry_id,
                                         config_subentry_id=target.subentry_id)
    for device in list(devices.devices.values()):
        if device.config_entry_id == parent.entry_id and device.config_subentry_id in sources:
            devices.async_update_device(device.id, new_config_entry_id=parent.entry_id,
                                        new_config_subentry_id=target.subentry_id)
    for identity in members:
        archive = hass.config_entries.async_get_entry(identity)
        adopted = archive.data.get('_adopted_by') if archive else None
        if adopted and adopted['entry_id'] == parent.entry_id and adopted['subentry_id'] in sources:
            hass.config_entries.async_update_entry(archive, data={**archive.data, '_adopted_by': {
                'entry_id': parent.entry_id, 'subentry_id': target.subentry_id}})
    for source_id in sources:
        if source_id in parent.subentries:
            hass.config_entries.async_remove_subentry(parent, source_id)
    hass.config_entries.async_update_subentry(parent, parent.subentries[target.subentry_id],
        data=final_payload, title='Price Windows')


def cleanup_removed_price_windows(hass, parent, registry, devices):
    """After parent unload, delete only explicitly removed members' records.

    Tombstones survive interruption; entities are removed before devices. Stored
    runtime history is deliberately retained, just as for removed HA subentries.
    """
    for group in list(parent.subentries.values()):
        removed = group.data.get('_removed_price_window_planners', [])
        if group.subentry_type != 'price_window' or not removed:
            continue
        active = price_window_members(group)
        identities = set(removed) - active.keys()
        for entity in list(registry.entities.values()):
            if (entity.config_entry_id == parent.entry_id
                    and entity.config_subentry_id == group.subentry_id
                    and any(entity.unique_id.startswith(identity + '_') for identity in identities)):
                registry.async_remove(entity.entity_id)
        for device in list(devices.devices.values()):
            if (device.config_entry_id == parent.entry_id
                    and device.config_subentry_id == group.subentry_id
                    and any((DOMAIN, identity) in device.identifiers for identity in identities)):
                # Never remove a device that still hosts unrelated entities.
                if not any(getattr(e, 'device_id', None) == device.id for e in registry.entities.values()):
                    devices.async_remove_device(device.id)
        payload = {k: v for k, v in group.data.items() if k != '_removed_price_window_planners'}
        hass.config_entries.async_update_subentry(parent, group, data=payload)


def merged_data(entry):
    return {**entry.data, **entry.options}


def is_self_forecast(hass, identity, entity_id):
    """Use stable registry identity, not names, to reject an own forecast source."""
    if not identity or not entity_id:
        return False
    from homeassistant.helpers import entity_registry as er
    record = er.async_get(hass).async_get(entity_id)
    return bool(record and record.unique_id.startswith(identity + '_'))


def child_data(data):
    """Remove household-owned fields, retaining all planner policy and metadata."""
    return {k: v for k, v in data.items() if k not in SHARED_SOURCE_KEYS}


class PlannerBinding:
    """Planner-facing identity/config; HA itself always receives the real parent."""

    def __init__(self, parent, subentry, member=None):
        self.parent = parent
        self.subentry_id = subentry.subentry_id
        self.runtime_key = subentry.subentry_id
        if member is not None:
            identity, data = member
            self.runtime_key = identity
            subentry = SimpleNamespace(subentry_id=subentry.subentry_id,
                subentry_type=subentry.subentry_type, data=data,
                title=data.get(CONF_PLANNER_NAME, identity))
        self.subentry = subentry
        self.entry_id = subentry.data.get('_legacy_entry_id', subentry.subentry_id)
        self.title = subentry.title
        self.options = {}

    @property
    def data(self):
        shared = merged_data(self.parent)
        return {**child_data(self.subentry.data),
                **{k: v for k, v in shared.items() if k in SHARED_SOURCE_KEYS},
                CONF_PLANNER_KIND: self.subentry.subentry_type}

    def async_on_unload(self, func):
        return self.parent.async_on_unload(func)


def planner_bindings(entry):
    if merged_data(entry).get(CONF_PLANNER_KIND) != PLANNER_KIND_HOUSEHOLD:
        return [entry]
    result = []
    for subentry in entry.subentries.values():
        if price_window_source_retired(entry, subentry.subentry_id):
            continue
        if subentry.subentry_type == 'price_window' and PRICE_WINDOW_MEMBERS in subentry.data:
            result.extend(PlannerBinding(entry, subentry, member)
                          for member in price_window_members(subentry).items())
        else:
            result.append(PlannerBinding(entry, subentry))
    return result


async def async_setup_platform(hass, entry, async_add_entities, setup):
    """Register each successful child's entities with its actual HA subentry."""
    planners = hass.data[DOMAIN]
    for binding in planner_bindings(entry):
        if isinstance(binding, PlannerBinding):
            owned = planners.get(entry.entry_id, {}).get(binding.runtime_key)
            if owned is None or planners.get(binding.entry_id) is not owned:
                continue
        elif binding.entry_id not in planners:
            continue
        def add(entities, binding=binding):
            if hasattr(binding, 'subentry_id'):
                async_add_entities(entities, config_subentry_id=binding.subentry_id)
            else:
                async_add_entities(entities)
        try:
            await setup(hass, binding, add)
        except Exception:
            if not isinstance(binding, PlannerBinding):
                raise
            _LOGGER.exception('Could not set up entities for planner %s', binding.title)


def _source_value(data, key):
    value = data.get(key)
    if isinstance(value, dict):
        value = value.get('entity_id')
    if key == CONF_EXPORT_PRICE_TAX_DEDUCTION and value is None:
        return DEFAULT_EXPORT_PRICE_TAX_DEDUCTION
    return None if value == '' else value


def adoption_conflicts(parent, legacy):
    """Missing legacy sources inherit; configured differing sources need resolution.

    An unset household source must not silently erase a configured legacy source.
    The export tax default is only relevant to batteries.
    """
    shared, old = merged_data(parent), merged_data(legacy)
    conflicts = {}
    for key in SHARED_SOURCE_KEYS:
        if key == CONF_EXPORT_PRICE_TAX_DEDUCTION and old.get(CONF_PLANNER_KIND, 'battery') != 'battery':
            continue
        before, after = _source_value(old, key), _source_value(shared, key)
        if before is not None and before != after:
            conflicts[key] = (before, after)
    return conflicts


def legacy_claims(hass, legacy_id):
    """Find configured owners, including failed setup and pending snapshots."""
    claims = []
    for entry in hass.config_entries.async_entries(DOMAIN):
        if merged_data(entry).get(CONF_PLANNER_KIND) != PLANNER_KIND_HOUSEHOLD:
            continue
        claims.extend((entry.entry_id, child.subentry_id) for child in planner_bindings(entry)
                      if child.data.get('_legacy_entry_id') == legacy_id)
        for source_id, owner_id in entry.data.get(_RETIRED_SOURCES, {}).items():
            owner = entry.subentries.get(owner_id)
            source = entry.subentries.get(source_id)
            if (owner is None or source is None
                    or source_id in owner.data.get(_GROUP_SOURCES, [])):
                continue
            # Retirement precedes the snapshot. An intervening edit can group
            # the destination without copying this source; only its explicit
            # snapshot journal makes member absence authoritative for removal.
            if legacy_id in price_window_members(source):
                claims.append((entry.entry_id, owner_id))
    return list(dict.fromkeys(claims))


def legacy_available(hass, legacy):
    """An orphan archive needs explicit adoption; never restart it on its own."""
    if legacy_claims(hass, legacy.entry_id):
        return False
    adopted = legacy.data.get('_adopted_by')
    if not adopted:
        return True
    owner = hass.config_entries.async_get_entry(adopted['entry_id'])
    owner_id = _price_window_owner(owner, adopted.get('subentry_id')) if owner else None
    if owner is None or owner_id not in owner.subentries:
        return True
    child = owner.subentries[owner_id]
    return PRICE_WINDOW_MEMBERS in child.data and legacy.entry_id not in price_window_members(child)


async def async_adopt_legacy(hass, parent, subentry, registry, devices):
    """Resume explicit adoption idempotently; keep the old entry as an archive.

    No entities or persisted planner state are deleted. The old entry is marked
    inert before registry reassociation, so interrupted migrations cannot start
    two controllers. Repeated setup resumes any remaining registry moves.
    """
    legacy_id = subentry.data.get('_legacy_entry_id')
    if not legacy_id:
        return
    target = {'entry_id': parent.entry_id, 'subentry_id': subentry.subentry_id}
    claims = legacy_claims(hass, legacy_id)
    if any(claim != (parent.entry_id, subentry.subentry_id) for claim in claims):
        raise ValueError('Legacy planner is already claimed by another child')
    legacy = hass.config_entries.async_get_entry(legacy_id)
    adopted = legacy.data.get('_adopted_by') if legacy else None
    if adopted and adopted != target:
        owner = hass.config_entries.async_get_entry(adopted['entry_id'])
        owner_id = _price_window_owner(owner, adopted['subentry_id']) if owner else None
        if (owner is not None and owner_id in owner.subentries
                and (owner.entry_id, owner_id) != (parent.entry_id, subentry.subentry_id)):
            child = owner.subentries[owner_id]
            if PRICE_WINDOW_MEMBERS not in child.data or legacy_id in price_window_members(child):
                raise ValueError('Legacy planner is already adopted by another child')
    live = hass.data.get(DOMAIN, {}).get(legacy_id)
    if live is not None and (legacy is None or adopted):
        raise ValueError('Legacy planner still has a live controller')
    if legacy is None:
        # The user may have removed the inert archive after successful adoption.
        return
    if adopted != target:
        if adoption_conflicts(parent, legacy):
            raise ValueError('Shared sources changed or conflict; resolve before adoption')
        if not adopted and not await hass.config_entries.async_unload(legacy_id):
            raise ValueError('Legacy planner could not be unloaded')
        hass.config_entries.async_update_entry(legacy, data={**legacy.data, '_adopted_by': target})
    # Transfer ALL entities before any device. HA's queued device-update handler
    # deletes entries still owned by the device's old config entry/subentry. An
    # exception partway through either loop must leave that cleanup harmless.
    # HA validates the target entry/subentry and device existence, not matching
    # device ownership, so keeping each stable device link here is supported.
    # Both registry APIs are synchronous callbacks: do not yield between moves.
    for entity in list(registry.entities.values()):
        if entity.config_entry_id == legacy_id:
            registry.async_update_entity(entity.entity_id, config_entry_id=parent.entry_id,
                                         config_subentry_id=subentry.subentry_id)
    # Devices must also move before the inert archive can be safely removed.
    for device in list(devices.devices.values()):
        if device.config_entry_id == legacy_id:
            devices.async_update_device(device.id, new_config_entry_id=parent.entry_id,
                                        new_config_subentry_id=subentry.subentry_id)


class SharedSources:
    """Coalesce identical expensive source reads, not planner results or policy."""

    def __init__(self):
        self._cache = {}
        self._pending = {}
        self._generation = 0

    def invalidate(self):
        self._generation += 1
        self._cache.clear()

    async def async_cached(self, key, load, *, ttl=60):
        key = (self._generation, key)
        cached = self._cache.get(key)
        if cached and monotonic() - cached[0] < ttl:
            return deepcopy(cached[1])
        task = self._pending.get(key)
        if task is None:
            task = self._pending[key] = asyncio.create_task(load())
        try:
            value = await asyncio.shield(task)
            if key[0] == self._generation:
                self._cache[key] = (monotonic(), value)
            return deepcopy(value)
        finally:
            if task.done():
                self._pending.pop(key, None)
