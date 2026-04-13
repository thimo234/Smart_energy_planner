"""The Smart Energy Planner integration."""

from __future__ import annotations

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import Event, EventStateChangedData, HomeAssistant, callback
from homeassistant.helpers.event import async_track_state_change_event

from .const import (
    CONF_HEATING_ENERGY_SENSOR,
    CONF_PLANNER_KIND,
    CONF_PRICE_SENSOR,
    CONF_SOLCAST_TODAY_SENSOR,
    CONF_TEMPERATURE_SENSOR,
    CONF_TOTAL_ENERGY_SENSOR,
    DOMAIN,
    PLANNER_KIND_BATTERY,
    PLANNER_KIND_COMBINED,
    PLANNER_KIND_THERMOSTAT,
)
from .coordinator import SmartEnergyPlannerCoordinator

PLATFORMS: list[Platform] = [Platform.SENSOR]


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Smart Energy Planner from a config entry."""
    coordinator = SmartEnergyPlannerCoordinator(hass, entry)
    await coordinator.async_refresh()

    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = coordinator
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    merged = {**entry.data, **entry.options}
    planner_kind = merged.get(CONF_PLANNER_KIND, PLANNER_KIND_COMBINED)

    tracked_entities = [
        merged.get(CONF_PRICE_SENSOR),
        merged.get(CONF_SOLCAST_TODAY_SENSOR),
    ]
    if planner_kind in (PLANNER_KIND_COMBINED, PLANNER_KIND_THERMOSTAT):
        tracked_entities.extend(
            [
                merged.get(CONF_TEMPERATURE_SENSOR),
                merged.get(CONF_HEATING_ENERGY_SENSOR),
            ]
        )
    if planner_kind in (PLANNER_KIND_COMBINED, PLANNER_KIND_BATTERY):
        tracked_entities.append(merged.get(CONF_TOTAL_ENERGY_SENSOR))

    @callback
    def _handle_source_state_change(event: Event[EventStateChangedData]) -> None:
        """Refresh quickly when a dependent sensor becomes available or updates."""
        new_state = event.data.get("new_state")
        old_state = event.data.get("old_state")
        if new_state is None or new_state == old_state:
            return
        hass.async_create_task(coordinator.async_request_refresh())

    entry.async_on_unload(
        async_track_state_change_event(
            hass,
            [entity_id for entity_id in tracked_entities if entity_id],
            _handle_source_state_change,
        )
    )
    entry.async_on_unload(entry.add_update_listener(_async_update_listener))
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        hass.data[DOMAIN].pop(entry.entry_id, None)
    return unload_ok


async def _async_update_listener(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Reload the config entry when options change."""
    await hass.config_entries.async_reload(entry.entry_id)
