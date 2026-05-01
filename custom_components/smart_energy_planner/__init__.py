"""The Smart Energy Planner integration."""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

from homeassistant.components.climate.const import HVACMode
from homeassistant.config_entries import ConfigEntry, ConfigEntryNotReady
from homeassistant.const import EVENT_HOMEASSISTANT_STARTED, Platform
from homeassistant.core import CoreState, Event, EventStateChangedData, HomeAssistant, callback
from homeassistant.helpers.event import (
    async_call_later,
    async_track_state_change_event,
    async_track_time_interval,
)
from homeassistant.helpers.storage import Store
from homeassistant.util import dt as dt_util

_LOGGER = logging.getLogger(__name__)

from .const import (
    CONF_EXPORT_PRICE_SENSOR,
    CONF_HEATING_SWITCH_ENTITY,
    CONF_COOLING_MODE_SWITCH_ENTITY,
    CONF_PLANNER_KIND,
    CONF_BATTERY_SOC_SENSOR,
    CONF_PRICE_SENSOR,
    CONF_PRICE_RESOLUTION,
    CONF_ROOM_TEMPERATURE_SENSOR,
    CONF_SOLCAST_TODAY_SENSOR,
    CONF_SOLCAST_TOMORROW_SENSOR,
    CONF_TEMPERATURE_SENSOR,
    CONF_THERMOSTAT_COLD_TOLERANCE,
    CONF_THERMOSTAT_CONTROL_CHECK_MINUTES,
    CONF_THERMOSTAT_HOT_TOLERANCE,
    CONF_THERMOSTAT_MAX_TEMP,
    CONF_THERMOSTAT_MIN_CYCLE_MINUTES,
    CONF_THERMOSTAT_MIN_TEMP,
    CONF_THERMOSTAT_PREHEAT_MINUTES,
    CONF_TOTAL_ENERGY_SENSOR,
    DEFAULT_THERMOSTAT_COLD_TOLERANCE,
    DEFAULT_THERMOSTAT_CONTROL_CHECK_MINUTES,
    DEFAULT_THERMOSTAT_HOT_TOLERANCE,
    DEFAULT_THERMOSTAT_MAX_TEMP,
    DEFAULT_THERMOSTAT_MIN_CYCLE_MINUTES,
    DEFAULT_THERMOSTAT_MIN_TEMP,
    DEFAULT_THERMOSTAT_PREHEAT_MINUTES,
    DOMAIN,
    HVAC_MODE_SMART,
    PLANNER_KIND_BATTERY,
    PLANNER_KIND_THERMOSTAT,
    PRESET_ECO,
    PRESET_NORMAL,
    PRESET_PREHEAT,
    RUNTIME_STATE,
    STORAGE_KEY,
    STORAGE_VERSION,
)
from .coordinator import SmartEnergyPlannerCoordinator, _THERMOSTAT_MAX_COOLDOWN_HOURS

PLATFORMS: list[Platform] = [Platform.SENSOR, Platform.CLIMATE]


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Smart Energy Planner from a config entry."""
    merged = {**entry.data, **entry.options}

    # Load persisted runtime state BEFORE the first coordinator refresh so
    # that battery profit tracking reads the correct stored values instead of
    # starting from an empty dict and overwriting them with 0.
    runtime_state = hass.data.setdefault(RUNTIME_STATE, {})
    persisted_state = await _async_load_runtime_state(hass, entry.entry_id)
    runtime_state[entry.entry_id] = {
        "manual_temperature": persisted_state.get("manual_temperature", _default_manual_temperature(merged)),
        "manual_cool_temperature": persisted_state.get(
            "manual_cool_temperature",
            _default_manual_cool_temperature(merged),
        ),
        "manual_eco_temperature": persisted_state.get(
            "manual_eco_temperature",
            _default_manual_eco_temperature(merged),
        ),
        "manual_preheat_temperature": persisted_state.get(
            "manual_preheat_temperature",
            _default_manual_preheat_temperature(merged),
        ),
        "hvac_mode": persisted_state.get("hvac_mode", HVACMode.HEAT),
        "manual_preset_mode": persisted_state.get("manual_preset_mode", PRESET_NORMAL),
        "last_switch_change": None,
        "cooling_model": persisted_state.get("cooling_model", {}),
        "last_cooling_observation": persisted_state.get("last_cooling_observation"),
        "eco_cooling_session": persisted_state.get("eco_cooling_session"),
        "battery_profit_total_eur": persisted_state.get("battery_profit_total_eur", 0.0),
        "battery_profit_cost_basis_eur": persisted_state.get("battery_profit_cost_basis_eur", 0.0),
        "battery_profit_tracked_energy_kwh": persisted_state.get("battery_profit_tracked_energy_kwh", 0.0),
        "battery_profit_last_energy_kwh": persisted_state.get("battery_profit_last_energy_kwh"),
        "battery_profit_last_updated": persisted_state.get("battery_profit_last_updated"),
        "hourly_demand_table": persisted_state.get("hourly_demand_table", {}),
        "hourly_demand_last_value": persisted_state.get("hourly_demand_last_value"),
        "hourly_demand_last_hour_key": persisted_state.get("hourly_demand_last_hour_key"),
        "hourly_demand_last_ts": persisted_state.get("hourly_demand_last_ts"),
        "smoothed_eco_hours": persisted_state.get("smoothed_eco_hours"),
    }

    coordinator = SmartEnergyPlannerCoordinator(hass, entry)
    await coordinator.async_refresh()
    if coordinator.data is None:
        raise ConfigEntryNotReady("Planner data is not ready yet")

    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = coordinator
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
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
    if planner_kind == PLANNER_KIND_THERMOSTAT:
        tracked_entities.extend(
            [
                _entity_id(CONF_TEMPERATURE_SENSOR),
                _entity_id(CONF_ROOM_TEMPERATURE_SENSOR),
                _entity_id(CONF_HEATING_SWITCH_ENTITY),
                _entity_id(CONF_COOLING_MODE_SWITCH_ENTITY),
            ]
        )
    if planner_kind == PLANNER_KIND_BATTERY:
        tracked_entities.append(_entity_id(CONF_TOTAL_ENERGY_SENSOR))

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
        hass.async_create_task(coordinator.async_refresh())

    entry.async_on_unload(
        async_track_state_change_event(
            hass,
            tracked,
            _handle_source_state_change,
        )
    )

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
        hass.async_create_task(coordinator.async_refresh())

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
            hass.async_create_task(coordinator.async_refresh())
            return
        errors = getattr(data, "source_errors", None)
        if errors:
            _LOGGER.info(
                "Smart Energy Planner backup recovery: source errors present (%s), triggering refresh",
                errors,
            )
            hass.async_create_task(coordinator.async_refresh())
        else:
            _LOGGER.debug("Smart Energy Planner backup recovery: no errors, skipping")

    entry.async_on_unload(
        async_track_time_interval(
            hass, _backup_recovery_check, timedelta(seconds=30)
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

    check_minutes = int(
        merged.get(CONF_THERMOSTAT_CONTROL_CHECK_MINUTES, DEFAULT_THERMOSTAT_CONTROL_CHECK_MINUTES)
    )

    @callback
    def _handle_periodic_check(_now) -> None:
        hass.async_create_task(
            _async_apply_thermostat_control(
                hass,
                entry,
                coordinator,
                runtime_state[entry.entry_id],
            )
        )

    entry.async_on_unload(
        async_track_time_interval(
            hass,
            _handle_periodic_check,
            timedelta(minutes=check_minutes),
        )
    )
    entry.async_on_unload(entry.add_update_listener(_async_update_listener))
    entry.async_on_unload(coordinator._cancel_source_error_retry)
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
    """Apply the heating switch state for planner thermostats."""
    merged = {**entry.data, **entry.options}
    planner_kind = merged.get(CONF_PLANNER_KIND, PLANNER_KIND_BATTERY)
    if planner_kind != PLANNER_KIND_THERMOSTAT:
        return

    await _async_apply_heating_switch_control(hass, merged, coordinator, runtime_state)


async def _async_apply_heating_switch_control(
    hass: HomeAssistant,
    merged: dict[str, Any],
    coordinator: SmartEnergyPlannerCoordinator,
    runtime_state: dict[str, Any],
) -> None:
    """Turn the heating switch on/off using hysteresis around the active target."""
    heating_switch_entity = merged.get(CONF_HEATING_SWITCH_ENTITY)
    if not heating_switch_entity or coordinator.data is None:
        return

    switch_state = hass.states.get(heating_switch_entity)
    if switch_state is None:
        return

    current_temperature = coordinator.data.room_temperature_c
    base_target = coordinator.data.thermostat_setpoint_c
    cool_target = getattr(coordinator.data, "thermostat_cool_setpoint_c", None)
    eco_target = coordinator.data.thermostat_eco_setpoint_c
    preheat_target = getattr(coordinator.data, "thermostat_preheat_setpoint_c", None)
    cooling_mode_switch_entity = merged.get(CONF_COOLING_MODE_SWITCH_ENTITY)
    cooling_mode_switch_state = hass.states.get(cooling_mode_switch_entity) if cooling_mode_switch_entity else None
    cooling_mode_active = str(cooling_mode_switch_state.state).lower() in {"on", "heat", "heating", "cool", "cooling"} if cooling_mode_switch_state else False
    hvac_mode = runtime_state.get("hvac_mode", HVACMode.HEAT)
    if hvac_mode == HVACMode.OFF:
        if str(switch_state.state).lower() in {"on", "heat", "heating"}:
            await _async_call_turn_service(hass, heating_switch_entity, "turn_off")
            runtime_state["last_switch_change"] = dt_util.now()
        return
    if cooling_mode_active:
        active_preset_mode = PRESET_NORMAL
        active_target = cool_target
        cooling_active = True
    else:
        cooling_active = False
        manual_preset_mode = runtime_state.get("manual_preset_mode", PRESET_NORMAL)
        if hvac_mode in {HVAC_MODE_SMART, "smart", HVACMode.AUTO}:
            if coordinator.data.heat_pump_strategy == "preheating":
                active_preset_mode = PRESET_PREHEAT
            elif coordinator.data.heat_pump_strategy == "energy_saving_on":
                active_preset_mode = PRESET_ECO
            else:
                active_preset_mode = PRESET_NORMAL
        elif manual_preset_mode in {PRESET_NORMAL, PRESET_PREHEAT, PRESET_ECO}:
            active_preset_mode = manual_preset_mode
        else:
            active_preset_mode = PRESET_NORMAL

        if active_preset_mode == PRESET_PREHEAT:
            active_target = preheat_target
        elif active_preset_mode == PRESET_ECO:
            active_target = eco_target
        else:
            active_target = base_target
    if current_temperature is None or active_target is None:
        return

    await _async_update_cooling_model(
        hass,
        coordinator.config_entry.entry_id,
        runtime_state,
        current_temperature=current_temperature,
        outdoor_temperature=coordinator.hass.states.get(merged.get(CONF_TEMPERATURE_SENSOR)).state
        if merged.get(CONF_TEMPERATURE_SENSOR) and coordinator.hass.states.get(merged.get(CONF_TEMPERATURE_SENSOR))
        else None,
        heating_is_on=str(switch_state.state).lower() in {"on", "heat", "heating"},
        eco_active=(not cooling_active) and active_preset_mode == PRESET_ECO,
        eco_setpoint=eco_target,
    )

    cold_tolerance = float(merged.get(CONF_THERMOSTAT_COLD_TOLERANCE, DEFAULT_THERMOSTAT_COLD_TOLERANCE))
    hot_tolerance = float(merged.get(CONF_THERMOSTAT_HOT_TOLERANCE, DEFAULT_THERMOSTAT_HOT_TOLERANCE))
    min_cycle_minutes = int(merged.get(CONF_THERMOSTAT_MIN_CYCLE_MINUTES, DEFAULT_THERMOSTAT_MIN_CYCLE_MINUTES))

    # Hysteresis around the active target. When heating, turn the switch on
    # once the room falls below (target - cold_tolerance) and off once it
    # rises above (target + hot_tolerance). When cooling, the same tolerances
    # are applied but inverted: turn on when too warm, off when cooled down.
    if cooling_active:
        should_turn_on = current_temperature >= active_target + hot_tolerance
        should_turn_off = current_temperature <= active_target - cold_tolerance
    else:
        should_turn_on = current_temperature <= active_target - cold_tolerance
        should_turn_off = current_temperature >= active_target + hot_tolerance
    current_is_on = str(switch_state.state).lower() in {"on", "heat", "heating"}

    last_switch_change = runtime_state.get("last_switch_change")
    cycle_blocked = False
    if last_switch_change is not None:
        cycle_blocked = dt_util.now() - last_switch_change < timedelta(minutes=min_cycle_minutes)

    if should_turn_on and not current_is_on and not cycle_blocked:
        await _async_call_turn_service(hass, heating_switch_entity, "turn_on")
        runtime_state["last_switch_change"] = dt_util.now()
    elif should_turn_off and current_is_on and not cycle_blocked:
        await _async_call_turn_service(hass, heating_switch_entity, "turn_off")
        runtime_state["last_switch_change"] = dt_util.now()


async def _async_call_turn_service(hass: HomeAssistant, entity_id: str, service: str) -> None:
    """Call turn_on/turn_off on the entity domain."""
    domain = entity_id.split(".", maxsplit=1)[0]
    await hass.services.async_call(
        domain,
        service,
        {"entity_id": entity_id},
        blocking=True,
    )


def _default_manual_temperature(merged: dict[str, Any]) -> float:
    min_temp = float(merged.get(CONF_THERMOSTAT_MIN_TEMP, DEFAULT_THERMOSTAT_MIN_TEMP))
    max_temp = float(merged.get(CONF_THERMOSTAT_MAX_TEMP, DEFAULT_THERMOSTAT_MAX_TEMP))
    return round(min(max(20.0, min_temp), max_temp), 2)


def _default_manual_cool_temperature(merged: dict[str, Any]) -> float:
    min_temp = float(merged.get(CONF_THERMOSTAT_MIN_TEMP, DEFAULT_THERMOSTAT_MIN_TEMP))
    max_temp = float(merged.get(CONF_THERMOSTAT_MAX_TEMP, DEFAULT_THERMOSTAT_MAX_TEMP))
    return round(min(max(24.0, min_temp), max_temp), 2)


def _default_manual_eco_temperature(merged: dict[str, Any]) -> float:
    min_temp = float(merged.get(CONF_THERMOSTAT_MIN_TEMP, DEFAULT_THERMOSTAT_MIN_TEMP))
    max_temp = float(merged.get(CONF_THERMOSTAT_MAX_TEMP, DEFAULT_THERMOSTAT_MAX_TEMP))
    configured = float(merged.get("thermostat_eco_temperature", 18.0))
    return round(min(max(configured, min_temp), max_temp), 2)


def _default_manual_preheat_temperature(merged: dict[str, Any]) -> float:
    min_temp = float(merged.get(CONF_THERMOSTAT_MIN_TEMP, DEFAULT_THERMOSTAT_MIN_TEMP))
    max_temp = float(merged.get(CONF_THERMOSTAT_MAX_TEMP, DEFAULT_THERMOSTAT_MAX_TEMP))
    base = _default_manual_temperature(merged)
    return round(min(max(base + 1.0, min_temp), max_temp), 2)


async def _async_load_runtime_state(hass: HomeAssistant, entry_id: str) -> dict[str, Any]:
    """Load persisted runtime state for a planner entry."""
    store = Store[dict[str, Any]](hass, STORAGE_VERSION, STORAGE_KEY)
    data = await store.async_load() or {}
    return data.get(entry_id, {})


async def _async_save_runtime_state(hass: HomeAssistant, entry_id: str, runtime_state: dict[str, Any]) -> None:
    """Persist selected runtime fields for a planner entry."""
    store = Store[dict[str, Any]](hass, STORAGE_VERSION, STORAGE_KEY)
    data = await store.async_load() or {}
    data[entry_id] = {
        "manual_temperature": runtime_state.get("manual_temperature"),
        "manual_cool_temperature": runtime_state.get("manual_cool_temperature"),
        "manual_eco_temperature": runtime_state.get("manual_eco_temperature"),
        "manual_preheat_temperature": runtime_state.get("manual_preheat_temperature"),
        "hvac_mode": runtime_state.get("hvac_mode", HVACMode.HEAT),
        "manual_preset_mode": runtime_state.get("manual_preset_mode", PRESET_NORMAL),
        "cooling_model": runtime_state.get("cooling_model", {}),
        "last_cooling_observation": runtime_state.get("last_cooling_observation"),
        "eco_cooling_session": runtime_state.get("eco_cooling_session"),
        "battery_profit_total_eur": runtime_state.get("battery_profit_total_eur", 0.0),
        "battery_profit_cost_basis_eur": runtime_state.get("battery_profit_cost_basis_eur", 0.0),
        "battery_profit_tracked_energy_kwh": runtime_state.get("battery_profit_tracked_energy_kwh", 0.0),
        "battery_profit_last_energy_kwh": runtime_state.get("battery_profit_last_energy_kwh"),
        "battery_profit_last_updated": runtime_state.get("battery_profit_last_updated"),
    }
    await store.async_save(data)


async def _async_update_cooling_model(
    hass: HomeAssistant,
    entry_id: str,
    runtime_state: dict[str, Any],
    *,
    current_temperature: float | None,
    outdoor_temperature,
    heating_is_on: bool,
    eco_active: bool,
    eco_setpoint: float | None = None,
) -> None:
    """Learn normalized room cooling speed from eco windows.

    Two kinds of sessions are counted:
    - Complete: heating fired during eco (room reached eco setpoint). The time
      from eco-start to first-heating is the true passive cooling duration.
    - Partial: eco window ended before eco setpoint was reached, but the room
      DID cool down (temperature dropped). Solar gain keeps the temperature from
      dropping, so a real drop confirms passive cooling without contamination.
      The observed cooling rate (°C/h per °C outdoor delta) is still valid; we
      project the full cooling duration from it to break the chicken-and-egg
      deadlock where a too-short eco window prevents the model from ever learning.
    """
    if current_temperature is None:
        return
    try:
        outdoor_temp = float(outdoor_temperature) if outdoor_temperature is not None else None
    except (TypeError, ValueError):
        outdoor_temp = None
    if outdoor_temp is None:
        return

    now = dt_util.now()
    runtime_state["last_cooling_observation"] = {
        "timestamp": now.isoformat(),
        "room_temperature_c": round(current_temperature, 3),
        "outdoor_temperature_c": round(outdoor_temp, 3),
        "heating_is_on": heating_is_on,
    }

    session = runtime_state.get("eco_cooling_session")
    if eco_active:
        if session is None:
            runtime_state["eco_cooling_session"] = {
                "start_timestamp": now.isoformat(),
                "start_room_temperature_c": round(current_temperature, 3),
                "start_outdoor_temperature_c": round(outdoor_temp, 3),
                "eco_setpoint_c": round(eco_setpoint, 3) if eco_setpoint is not None else None,
            }
        elif heating_is_on and session.get("first_heating_timestamp") is None:
            # Record when heating first fires — the room reached eco setpoint.
            session["first_heating_timestamp"] = now.isoformat()
            session["first_heating_room_temperature_c"] = round(current_temperature, 3)
            session["first_heating_outdoor_temperature_c"] = round(outdoor_temp, 3)
        await _async_save_runtime_state(hass, entry_id, runtime_state)
        return

    if session is None:
        await _async_save_runtime_state(hass, entry_id, runtime_state)
        return

    runtime_state["eco_cooling_session"] = None

    try:
        start_time = dt_util.parse_datetime(session["start_timestamp"])
        start_room_temp = float(session["start_room_temperature_c"])
        start_outdoor_temp = float(session["start_outdoor_temperature_c"])
    except (KeyError, TypeError, ValueError):
        await _async_save_runtime_state(hass, entry_id, runtime_state)
        return

    if start_time is None:
        await _async_save_runtime_state(hass, entry_id, runtime_state)
        return

    first_heating_ts_raw = session.get("first_heating_timestamp")
    first_heating_ts = dt_util.parse_datetime(first_heating_ts_raw) if first_heating_ts_raw else None

    if first_heating_ts is not None:
        # Complete session: room reached eco setpoint. Use the exact cooling duration.
        end_time = first_heating_ts
        end_room_temp = float(session.get("first_heating_room_temperature_c", current_temperature))
        end_outdoor_temp = float(session.get("first_heating_outdoor_temperature_c", outdoor_temp))
        partial = False
    else:
        # Partial session: eco window ended before eco setpoint was reached.
        # Only count if the room actually cooled — a rising or flat temperature
        # means solar gain kept the house warm; skip to avoid polluting the model.
        end_time = now
        end_room_temp = current_temperature
        end_outdoor_temp = outdoor_temp
        partial = True

    elapsed_hours = (end_time - start_time).total_seconds() / 3600
    cooling_drop = start_room_temp - end_room_temp

    # Require meaningful cooling — a flat or rising temperature means external
    # heat (sun) dominated; discard to avoid biasing the model.
    if elapsed_hours < 0.25 or cooling_drop < 0.3:
        await _async_save_runtime_state(hass, entry_id, runtime_state)
        return

    average_room_temp = (start_room_temp + end_room_temp) / 2
    average_outdoor_temp = (start_outdoor_temp + end_outdoor_temp) / 2
    average_delta_temp = max(average_room_temp - average_outdoor_temp, 0.5)
    cooling_rate = cooling_drop / elapsed_hours
    normalized_cooling_rate = cooling_rate / average_delta_temp

    if partial:
        # Project what the full cooling duration would have been at the observed
        # rate, so the model can grow beyond the current (possibly too short)
        # eco window even before eco setpoint is ever reached.
        session_eco_setpoint = session.get("eco_setpoint_c")
        if session_eco_setpoint is not None:
            full_delta = max(start_room_temp - float(session_eco_setpoint), 0.1)
            projected_hours = full_delta / max(cooling_rate, 0.01)
            duration_for_model = min(projected_hours, _THERMOSTAT_MAX_COOLDOWN_HOURS)
        else:
            # No setpoint recorded — can't project; skip partial session.
            await _async_save_runtime_state(hass, entry_id, runtime_state)
            return
    else:
        duration_for_model = elapsed_hours

    cooling_model = runtime_state.setdefault("cooling_model", {})
    previous_factor = cooling_model.get("rolling_cooling_factor")
    previous_samples = int(cooling_model.get("sample_count", 0) or 0)
    previous_eco_samples = int(cooling_model.get("eco_sample_count", 0) or 0)
    learned_factor = (
        normalized_cooling_rate
        if previous_factor is None
        else ((float(previous_factor) * 0.8) + (normalized_cooling_rate * 0.2))
    )
    cooling_model["rolling_cooling_factor"] = round(max(0.001, learned_factor), 5)
    cooling_model["sample_count"] = previous_samples + 1
    if not partial:
        cooling_model["eco_sample_count"] = previous_eco_samples + 1
    cooling_model["last_delta_temp_c"] = round(average_delta_temp, 3)
    cooling_model["last_observed_drop_c_per_hour"] = round(cooling_rate, 4)
    cooling_model["last_eco_duration_hours"] = round(duration_for_model, 3)
    cooling_model["last_eco_start_temp_c"] = round(start_room_temp, 3)
    cooling_model["last_eco_end_temp_c"] = round(end_room_temp, 3)
    await _async_save_runtime_state(hass, entry_id, runtime_state)
