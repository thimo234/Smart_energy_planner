"""Household composition, stable planner identities and explicit legacy adoption.

This module deliberately does not import Home Assistant: the composition and
migration invariants can be exercised with serialized real-shaped entries.
"""
from __future__ import annotations

import asyncio
import logging
from copy import deepcopy
from time import monotonic

from .const import (
    CONF_EXPORT_PRICE_TAX_DEDUCTION, CONF_PLANNER_KIND, CONF_PLANNER_NAME,
    DEFAULT_EXPORT_PRICE_TAX_DEDUCTION, DOMAIN, PLANNER_KIND_HOUSEHOLD, SHARED_SOURCE_KEYS,
)


_LOGGER = logging.getLogger(__name__)


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

    def __init__(self, parent, subentry):
        self.parent = parent
        self.subentry = subentry
        self.entry_id = subentry.data.get('_legacy_entry_id', subentry.subentry_id)
        self.subentry_id = subentry.subentry_id
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
    return [PlannerBinding(entry, subentry) for subentry in entry.subentries.values()]


async def async_setup_platform(hass, entry, async_add_entities, setup):
    """Register each successful child's entities with its actual HA subentry."""
    planners = hass.data[DOMAIN]
    for binding in planner_bindings(entry):
        if isinstance(binding, PlannerBinding):
            owned = planners.get(entry.entry_id, {}).get(binding.subentry_id)
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
    """Find configured owners, including children whose setup has failed."""
    return [(entry.entry_id, child.subentry_id)
            for entry in hass.config_entries.async_entries(DOMAIN)
            for child in entry.subentries.values()
            if child.data.get('_legacy_entry_id') == legacy_id]


def legacy_available(hass, legacy):
    """An orphan archive needs explicit adoption; never restart it on its own."""
    if legacy_claims(hass, legacy.entry_id):
        return False
    adopted = legacy.data.get('_adopted_by')
    if not adopted:
        return True
    owner = hass.config_entries.async_get_entry(adopted['entry_id'])
    return owner is None or adopted['subentry_id'] not in owner.subentries


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
        if owner is not None and adopted['subentry_id'] in owner.subentries:
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
