"""The Smart Energy Planner integration."""

from __future__ import annotations

from typing import Any

from homeassistant.components.climate.const import SERVICE_SET_TEMPERATURE
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import Event, EventStateChangedData, HomeAssistant, callback
from homeassistant.helpers.event import async_track_state_change_event

from .const import (
    CONF_HEATING_ENERGY_SENSOR,
    CONF_HEATING_SWITCH_ENTITY,
    CONF_PLANNER_KIND,
    CONF_PRICE_SENSOR,
    CONF_SOLCAST_TODAY_SENSOR,
    CONF_ROOM_TEMPERATURE_SENSOR,
    CONF_TEMPERATURE_SENSOR,
    CONF_THERMOSTAT_ENTITY,
    CONF_TOTAL_ENERGY_SENSOR,
    DOMAIN,
    PLANNER_KIND_BATTERY,
    PLANNER_KIND_COMBINED,
    PLANNER_KIND_THERMOSTAT,
)
from .coordinator import SmartEnergyPlannerCoordinator

PLATFORMS: list[Platform] = [Platform.SENSOR]
RUNTIME_STATE = f"{DOMAIN}_runtime"


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Smart Energy Planner from a config entry."""
    coordinator = SmartEnergyPlannerCoordinator(hass, entry)
    await coordinator.async_refresh()

    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = coordinator
    runtime_state = hass.data.setdefault(RUNTIME_STATE, {})
    runtime_state[entry.entry_id] = {"restore_temperature": None}
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    merged = {**entry.data, **entry.options}
    planner_kind = merged.get(CONF_PLANNER_KIND, PLANNER_KIND_COMBINED)

    tracked_entities = [
        merged.get(CONF_PRICE_SENSOR),
    ]
    if planner_kind in (PLANNER_KIND_COMBINED, PLANNER_KIND_BATTERY):
        tracked_entities.append(merged.get(CONF_SOLCAST_TODAY_SENSOR))
    if planner_kind in (PLANNER_KIND_COMBINED, PLANNER_KIND_THERMOSTAT):
        tracked_entities.extend(
            [
                merged.get(CONF_TEMPERATURE_SENSOR),
                merged.get(CONF_ROOM_TEMPERATURE_SENSOR),
                merged.get(CONF_HEATING_ENERGY_SENSOR),
                merged.get(CONF_HEATING_SWITCH_ENTITY),
                merged.get(CONF_THERMOSTAT_ENTITY),
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
    
    @callback
    def _handle_coordinator_update() -> None:
        hass.async_create_task(
            _async_apply_thermostat_control(
                hass,
                entry,
                coordinator,
                runtime_state[entry.entry_id],
            )
        )

    entry.async_on_unload(coordinator.async_add_listener(_handle_coordinator_update))
    hass.async_create_task(
        _async_apply_thermostat_control(
            hass,
            entry,
            coordinator,
            runtime_state[entry.entry_id],
        )
    )
    entry.async_on_unload(entry.add_update_listener(_async_update_listener))
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        hass.data[DOMAIN].pop(entry.entry_id, None)
        hass.data.get(RUNTIME_STATE, {}).pop(entry.entry_id, None)
    return unload_ok


async def _async_update_listener(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Reload the config entry when options change."""
    await hass.config_entries.async_reload(entry.entry_id)


async def _async_apply_thermostat_control(
    hass: HomeAssistant,
    entry: ConfigEntry,
    coordinator: SmartEnergyPlannerCoordinator,
    runtime_state: dict[str, Any],
) -> None:
    """Automatically apply thermostat eco control for thermostat planners."""
    merged = {**entry.data, **entry.options}
    planner_kind = merged.get(CONF_PLANNER_KIND, PLANNER_KIND_COMBINED)
    if planner_kind not in (PLANNER_KIND_COMBINED, PLANNER_KIND_THERMOSTAT):
        return

    thermostat_entity = merged.get(CONF_THERMOSTAT_ENTITY)
    if not thermostat_entity or coordinator.data is None:
        return

    thermostat_state = hass.states.get(thermostat_entity)
    if thermostat_state is None:
        return

    current_target = _extract_thermostat_target(thermostat_state)
    eco_target = getattr(coordinator.data, "thermostat_eco_setpoint_c", None)
    strategy = coordinator.data.heat_pump_strategy
    restore_target = runtime_state.get("restore_temperature")

    if strategy == "energy_saving_on" and eco_target is not None:
        if current_target is None:
            return
        if restore_target is None and current_target > eco_target + 0.05:
            runtime_state["restore_temperature"] = current_target
        if current_target > eco_target + 0.05:
            await hass.services.async_call(
                "climate",
                SERVICE_SET_TEMPERATURE,
                {"entity_id": thermostat_entity, "temperature": eco_target},
                blocking=True,
            )
        return

    if restore_target is None:
        return

    if current_target is None:
        runtime_state["restore_temperature"] = None
        return

    if eco_target is not None and current_target <= eco_target + 0.1:
        await hass.services.async_call(
            "climate",
            SERVICE_SET_TEMPERATURE,
            {"entity_id": thermostat_entity, "temperature": restore_target},
            blocking=True,
        )
    runtime_state["restore_temperature"] = None


def _extract_thermostat_target(thermostat_state) -> float | None:
    for key in ("temperature", "target_temp_high", "target_temp_low"):
        value = thermostat_state.attributes.get(key)
        try:
            if value is not None:
                return float(value)
        except (TypeError, ValueError):
            continue
    return None
