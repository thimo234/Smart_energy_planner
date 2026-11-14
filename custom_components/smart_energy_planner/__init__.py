"""The Smart Energy Planner integration."""

from __future__ import annotations

import asyncio
import hashlib
import logging
from datetime import timedelta
from pathlib import Path
from typing import Any

from homeassistant.components.http import StaticPathConfig
from homeassistant.config_entries import ConfigEntry, ConfigEntryNotReady
from homeassistant.const import EVENT_HOMEASSISTANT_STARTED, Platform
from homeassistant.core import CoreState, Event, EventStateChangedData, HomeAssistant, callback
from homeassistant.helpers.event import (
    async_call_later,
    async_track_state_change_event,
    async_track_time_interval,
)
from homeassistant.helpers.storage import Store

_LOGGER = logging.getLogger(__name__)

from .const import (
    CONF_EXPORT_PRICE_SENSOR,
    CONF_PLANNER_KIND,
    CONF_BATTERY_SOC_SENSOR,
    CONF_PRICE_SENSOR,
    CONF_SOLCAST_TODAY_SENSOR,
    CONF_SOLCAST_TOMORROW_SENSOR,
    CONF_TOTAL_ENERGY_SENSOR,
    DOMAIN,
    PLANNER_KIND_BATTERY,
    PLANNER_KIND_PRICE_WINDOW,
    RUNTIME_STATE,
    STORAGE_KEY,
    STORAGE_VERSION,
)
from .coordinator import SmartEnergyPlannerCoordinator
from .const import PLANNER_KIND_HOUSEHOLD
from .household import SharedSources, async_adopt_legacy, planner_bindings
from .const import PLANNER_KIND_DEMAND, CONF_DEMAND_FORECAST_SENSOR, CONF_DEMAND_DEVICES
from .frontend_resource import (
    async_load_and_upsert_card_resource,
    lovelace_uses_storage_resources,
)

PLATFORMS: list[Platform] = [Platform.SENSOR, Platform.BINARY_SENSOR]
_CARD_STATIC_URL = "/smart_energy_planner"
_CARD_STATIC_PATH = Path(__file__).parent / "frontend"
_CARD_FILENAME = "smart-energy-planner-card.js"
_CARD_VERSION = hashlib.sha256((_CARD_STATIC_PATH / _CARD_FILENAME).read_bytes()).hexdigest()[:12]
_RUNTIME_STORE = f"{DOMAIN}_runtime_store"
_RUNTIME_STORE_LOCK = f"{DOMAIN}_runtime_store_lock"


async def async_setup(hass: HomeAssistant, config: dict[str, Any]) -> bool:
    """Set up integration-level resources."""
    await _async_register_frontend(hass)
    return True


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Smart Energy Planner from a config entry."""
    merged = {**entry.data, **entry.options}
    if entry.data.get('_adopted_by'):
        _LOGGER.info("Planner %s is an inert adoption archive", entry.entry_id)
        return True
    if merged.get(CONF_PLANNER_KIND) == PLANNER_KIND_HOUSEHOLD:
        return await _async_setup_household(hass, entry)
    if merged.get(CONF_PLANNER_KIND, PLANNER_KIND_BATTERY) not in (
        PLANNER_KIND_BATTERY, PLANNER_KIND_PRICE_WINDOW, PLANNER_KIND_DEMAND,
    ):
        _LOGGER.error("Planner type is no longer supported; remove this integration entry")
        return False
    await _async_register_frontend(hass)

    await _async_setup_planner(hass, entry)
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    entry.async_on_unload(entry.add_update_listener(_async_update_listener))
    return True


async def _async_setup_planner(hass, entry, shared=None):
    """One lifecycle for standalone and household planners."""
    merged = {**entry.data, **entry.options}

    # Load persisted runtime state BEFORE the first coordinator refresh so
    # that battery profit tracking reads the correct stored values instead of
    # starting from an empty dict and overwriting them with 0.
    runtime_state = hass.data.setdefault(RUNTIME_STATE, {})
    persisted_state = await _async_load_runtime_state(hass, entry.entry_id)
    runtime_state[entry.entry_id] = {
        "price_peak_state": persisted_state.get("price_peak_state", {}),
        "battery_profit_total_eur": persisted_state.get("battery_profit_total_eur", 0.0),
        "battery_profit_cost_basis_eur": persisted_state.get("battery_profit_cost_basis_eur", 0.0),
        "battery_profit_tracked_energy_kwh": persisted_state.get("battery_profit_tracked_energy_kwh", 0.0),
        "battery_profit_last_energy_kwh": persisted_state.get("battery_profit_last_energy_kwh"),
        "battery_profit_last_updated": persisted_state.get("battery_profit_last_updated"),
        "battery_cycle_state": persisted_state.get("battery_cycle_state", {}),
        "battery_grid_charge_price": persisted_state.get("battery_grid_charge_price"),
        "expected_hourly_demand_stats": persisted_state.get("expected_hourly_demand_stats", {}),
        "expected_hourly_demand_table": persisted_state.get("expected_hourly_demand_table", {}),
        "expected_hourly_demand_adjustment_factor": persisted_state.get(
            "expected_hourly_demand_adjustment_factor",
            1.0,
        ),
        "expected_hourly_demand_last_value": persisted_state.get("expected_hourly_demand_last_value"),
        "expected_hourly_demand_last_hour_key": persisted_state.get("expected_hourly_demand_last_hour_key"),
        "expected_hourly_demand_last_ts": persisted_state.get("expected_hourly_demand_last_ts"),
        "expected_hourly_demand_today": persisted_state.get("expected_hourly_demand_today", {}),
        **persisted_state,
    }

    coordinator = SmartEnergyPlannerCoordinator(hass, entry)
    coordinator.shared_sources = shared
    try:
        await coordinator.async_refresh()
        if coordinator.data is None and shared is None:
            raise ConfigEntryNotReady("Planner data is not ready yet")
    except BaseException:
        await coordinator.async_shutdown()
        raise

    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = coordinator
    planner_kind = merged.get(CONF_PLANNER_KIND, PLANNER_KIND_BATTERY)

    def _entity_id(key: str) -> str | None:
        value = merged.get(key)
        if isinstance(value, dict):
            value = value.get("entity_id")
        return str(value) if value else None

    tracked_entities = [_entity_id(CONF_PRICE_SENSOR)]
    if planner_kind == PLANNER_KIND_BATTERY:
        tracked_entities.append(_entity_id(CONF_EXPORT_PRICE_SENSOR))
        tracked_entities.append(_entity_id(CONF_SOLCAST_TODAY_SENSOR))
        tracked_entities.append(_entity_id(CONF_SOLCAST_TOMORROW_SENSOR))
        tracked_entities.append(_entity_id(CONF_BATTERY_SOC_SENSOR))
    if planner_kind == PLANNER_KIND_BATTERY:
        tracked_entities.append(_entity_id(CONF_TOTAL_ENERGY_SENSOR))
        tracked_entities.append(_entity_id(CONF_DEMAND_FORECAST_SENSOR))
    # Demand learning uses completed Recorder history, refreshed hourly. The
    # regular coordinator interval checks source health; avoid recomputing a
    # three-day forecast on every appliance meter tick. Prices still trigger it.

    tracked = [entity_id for entity_id in tracked_entities if entity_id]
    _LOGGER.info("Smart Energy Planner: tracking state changes for entities: %s", tracked)

    @callback
    def _handle_source_state_change(event: Event[EventStateChangedData]) -> None:
        new_state = event.data.get("new_state")
        if new_state is None:
            return
        _LOGGER.info(
            "Smart Energy Planner: state change for %s (state=%s), triggering refresh",
            event.data.get("entity_id", "?"),
            new_state.state,
        )
        hass.async_create_task(coordinator.async_request_refresh())

    coordinator.source_entities = set(tracked)
    if shared is None:
        entry.async_on_unload(async_track_state_change_event(
            hass, tracked, _handle_source_state_change,
        ))

    # Ensure a fresh coordinator refresh runs once HA has fully started so
    # that source entities (ESPHome, Solcast) are available.  Two cases:
    #
    # 1. HA is still starting: fire at EVENT_HOMEASSISTANT_STARTED then
    #    again 30 seconds later to catch devices that connect after startup.
    # 2. HA already running (integration reloaded after boot, or
    #    ConfigEntryNotReady delayed setup past the startup event):
    #    refresh immediately and schedule a 30-second delayed refresh.
    @callback
    def _do_refresh(_=None) -> None:
        hass.async_create_task(coordinator.async_request_refresh())

    @callback
    def _on_ha_started(_event=None) -> None:
        _do_refresh()
        entry.async_on_unload(async_call_later(hass, 30, _do_refresh))

    if hass.state is CoreState.running:
        _on_ha_started()
    else:
        entry.async_on_unload(
            hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STARTED, _on_ha_started)
        )

    # Independent backup recovery: poll every 30s while there are source
    # errors.  Registered at entry level so it is immune to anything the
    # coordinator does internally (including exceptions in _async_update_data).
    @callback
    def _backup_recovery_check(_now) -> None:
        data = coordinator.data
        if data is None:
            _LOGGER.info("Smart Energy Planner backup recovery: no data yet, triggering refresh")
            hass.async_create_task(coordinator.async_request_refresh())
            return
        errors = getattr(data, "source_errors", None)
        if errors:
            _LOGGER.info(
                "Smart Energy Planner backup recovery: source errors present (%s), triggering refresh",
                errors,
            )
            hass.async_create_task(coordinator.async_request_refresh())
        else:
            _LOGGER.debug("Smart Energy Planner backup recovery: no errors, skipping")

    entry.async_on_unload(
        async_track_time_interval(
            hass, _backup_recovery_check, timedelta(seconds=30)
        )
    )

    entry.async_on_unload(coordinator._cancel_source_error_retry)
    return coordinator


async def _async_setup_household(hass, entry):
    """Keep child refreshes, failures and state independent under one HA entry."""
    from homeassistant.helpers import device_registry as dr, entity_registry as er

    await _async_register_frontend(hass)
    shared = SharedSources()
    children = {}
    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = children
    dr.async_get(hass).async_get_or_create(
        config_entry_id=entry.entry_id, identifiers={(DOMAIN, entry.entry_id)},
        name=entry.title, manufacturer="Smart Energy Planner", model="Household",
    )
    for binding in planner_bindings(entry):
        try:
            await async_adopt_legacy(hass, entry, binding.subentry, er.async_get(hass))
        except Exception:
            # Adoption has not transferred ownership: a live standalone planner
            # may still own this identity and its runtime state.
            _LOGGER.exception("Unable to adopt child %s; other planners remain active", binding.title)
            continue
        previous = hass.data.get(DOMAIN, {}).get(binding.entry_id)
        previous_state = hass.data.get(RUNTIME_STATE, {}).get(binding.entry_id)
        try:
            children[binding.subentry_id] = await _async_setup_planner(hass, binding, shared)
        except Exception:
            _LOGGER.exception("Unable to set up child %s; other planners remain active", binding.title)
            planners = hass.data.get(DOMAIN, {})
            failed = planners.get(binding.entry_id)
            if failed is not None and failed is not previous:
                planners.pop(binding.entry_id)
                failed._cancel_source_error_retry()
                await failed.async_shutdown()
            states = hass.data.get(RUNTIME_STATE, {})
            if states.get(binding.entry_id) is not previous_state:
                if previous_state is None:
                    states.pop(binding.entry_id, None)
                else:
                    states[binding.entry_id] = previous_state

    @callback
    def source_changed(event):
        entity_id = event.data.get('entity_id')
        shared.invalidate()
        for coordinator in children.values():
            if entity_id in coordinator.source_entities:
                hass.async_create_task(coordinator.async_request_refresh())

    tracked = set().union(*(c.source_entities for c in children.values())) if children else set()
    if tracked:
        entry.async_on_unload(async_track_state_change_event(hass, list(tracked), source_changed))
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    entry.async_on_unload(entry.add_update_listener(_async_update_listener))
    return True


async def _async_register_frontend(hass: HomeAssistant) -> None:
    """Register the bundled Lovelace card as a static frontend resource."""
    registered_key = f"{DOMAIN}_frontend_registered"
    if not hass.data.get(registered_key):
        await hass.http.async_register_static_paths(
            [
                StaticPathConfig(
                    _CARD_STATIC_URL,
                    str(_CARD_STATIC_PATH),
                    cache_headers=False,
                )
            ]
        )
        hass.data[registered_key] = True

    await _async_register_lovelace_resource(hass)


async def _async_register_lovelace_resource(hass: HomeAssistant, retry: int = 0) -> None:
    """Add or update the Lovelace resource when Home Assistant uses storage mode."""
    lovelace = hass.data.get("lovelace")
    resources = getattr(lovelace, "resources", None) if lovelace is not None else None
    if resources is None:
        if retry < 6:
            async_call_later(hass, 5, lambda _now: hass.async_create_task(
                _async_register_lovelace_resource(hass, retry + 1)
            ))
        return

    if not lovelace_uses_storage_resources(lovelace):
        return

    result = await async_load_and_upsert_card_resource(
        resources,
        card_filename=_CARD_FILENAME,
        default_base_url=f"{_CARD_STATIC_URL}/{_CARD_FILENAME}",
        version=_CARD_VERSION,
    )
    if result == "not_loaded":
        if retry < 6:
            async_call_later(hass, 5, lambda _now: hass.async_create_task(
                _async_register_lovelace_resource(hass, retry + 1)
            ))
        return


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    if entry.data.get('_adopted_by'):
        # Its stable identity now indexes the live child, not this inert archive.
        return True
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        value = hass.data.get(DOMAIN, {}).pop(entry.entry_id, None)
        identities = [c.planner_entry.entry_id for c in value.values()] if isinstance(value, dict) else [entry.entry_id]
        for identity in identities:
            hass.data.get(DOMAIN, {}).pop(identity, None)
            hass.data.get(RUNTIME_STATE, {}).pop(identity, None)
    return unload_ok


async def _async_update_listener(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Reload the config entry when options change."""
    await hass.config_entries.async_reload(entry.entry_id)


async def _async_load_runtime_state(hass: HomeAssistant, entry_id: str) -> dict[str, Any]:
    """Load persisted runtime state for a planner entry."""
    store, lock = _runtime_store(hass)
    async with lock:
        data = await store.async_load() or {}
        return data.get(entry_id, {})


def _runtime_store(hass: HomeAssistant) -> tuple[Store[dict[str, Any]], asyncio.Lock]:
    """Return the shared runtime store and its read-modify-write lock."""
    store = hass.data.get(_RUNTIME_STORE)
    if store is None:
        store = Store[dict[str, Any]](hass, STORAGE_VERSION, STORAGE_KEY)
        hass.data[_RUNTIME_STORE] = store
    lock = hass.data.get(_RUNTIME_STORE_LOCK)
    if lock is None:
        lock = asyncio.Lock()
        hass.data[_RUNTIME_STORE_LOCK] = lock
    return store, lock
