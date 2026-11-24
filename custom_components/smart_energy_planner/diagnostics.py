"""Read-only, allowlisted Home Assistant config-entry diagnostics.

No source entity states, credentials, titles, exception text or unrelated HA data
are read. Config/source names are omitted or redacted; stable entry/subentry IDs
are retained to explain ownership and persisted-state lookup. This is an in-memory
snapshot, not a storage reload or a diagnosis of why a plan was selected.
"""
from __future__ import annotations

from datetime import date, datetime
from math import isfinite
from typing import TYPE_CHECKING, Any

from . import const as c
from .household import merged_data, planner_bindings

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant

_REDACTED = '**REDACTED**'
# Defaults mirror the coordinator; options and household source inheritance are
# resolved by the same bindings as setup. No coordinator calculation is invoked.
_CONFIG_DEFAULTS = {
    'planner_kind': c.DEFAULT_PLANNER_KIND,
    'price_resolution': c.DEFAULT_PRICE_RESOLUTION,
    'battery_enabled': c.DEFAULT_BATTERY_ENABLED,
    'battery_capacity_kwh': c.DEFAULT_BATTERY_CAPACITY_KWH,
    'battery_min_soc_percent': c.DEFAULT_BATTERY_MIN_SOC_PERCENT,
    'battery_min_profit_per_kwh': c.DEFAULT_BATTERY_MIN_PROFIT_PER_KWH,
    'battery_max_charge_kw': c.DEFAULT_BATTERY_MAX_CHARGE_KW,
    'battery_max_discharge_kw': c.DEFAULT_BATTERY_MAX_DISCHARGE_KW,
    'battery_charge_safety_margin': c.DEFAULT_BATTERY_CHARGE_SAFETY_MARGIN,
    'battery_solar_transition_buffer_minutes': c.DEFAULT_BATTERY_SOLAR_TRANSITION_BUFFER_MINUTES,
    'battery_demand_safety_margin': c.DEFAULT_BATTERY_DEMAND_SAFETY_MARGIN,
    'export_price_tax_deduction': c.DEFAULT_EXPORT_PRICE_TAX_DEDUCTION,
}
_SOURCE_KEYS = (
    'price_sensor', 'export_price_sensor', 'solcast_today_sensor',
    'solcast_tomorrow_sensor', 'total_energy_sensor', 'battery_soc_sensor',
    'demand_forecast_sensor', 'weather_entity', 'outdoor_temperature_sensor',
)
_CYCLE_KEYS = (
    'charge_session_started', 'discharge_session_started', 'initialized',
    'discharge_floor_kwh', 'updated_at',
)
_COORDINATOR_KEYS = (
    'charge_session_started', 'discharge_session_started',
    'battery_cycle_discharge_floor_kwh', 'battery_cycle_state_initialized',
    'battery_cycle_state_restored_recent', 'battery_grid_charge_price',
    'active_charge_phase_end', 'active_charge_phase_mode',
)
_ACTION_KEYS = (
    'start', 'end', 'mode', 'solar_kw', 'grid_kw', 'power',
    'sale_price', 'import_price',
)
_WINDOW_KEYS = (
    'start', 'end', 'at', 'mode', 'price', 'price_known', 'export_price',
    'hours', 'usable_hours', 'average_price', 'energy_kwh', 'expected_demand_kwh', 'expected_solar_kwh',
    'estimated_kwh', 'estimated_kw', 'forecast_kwh', 'forecast_kwh_p10',
    'forecast_kwh_p90',
)
_PLAN_KEYS = (
    'planner_kind', 'status', 'battery_strategy', 'forecast_generated_at',
    'current_price', 'current_export_price', 'price_resolution',
    'battery_soc_percent', 'battery_min_soc_percent',
    'battery_no_charge_min_soc_percent', 'battery_no_charge_reserve_active',
    'battery_total_energy_kwh', 'battery_energy_available_kwh',
    'battery_remaining_capacity_kwh', 'battery_min_profit_per_kwh',
    'battery_charge_cycle_active', 'battery_discharge_cycle_active',
    'battery_reserved_energy_kwh', 'battery_energy_available_for_discharge_kwh',
    'battery_exportable_energy_kwh', 'battery_room_needed_for_solar_kwh',
    'battery_simulated_remaining_kwh_after_discharge',
    'next_charge_opportunity_start', 'next_charge_window_start',
    'next_charge_window_end', 'next_charge_window_hours',
    'following_charge_window_start', 'following_charge_window_end',
    'following_charge_window_hours', 'next_discharge_window_start',
    'next_discharge_window_end', 'next_discharge_window_hours',
    'next_idle_window_start', 'current_relevant_battery_window_start',
    'current_relevant_battery_window_end', 'current_relevant_battery_window_mode',
    'current_relevant_battery_window_expected_demand_kwh',
    'current_relevant_battery_window_expected_solar_kwh',
    'home_demand_until_next_charge_kwh', 'estimated_total_home_demand_kwh',
)
_PLAN_SERIES = (
    'planned_battery_mode_windows', 'planned_battery_mode_schedule',
    'estimated_battery_mode_windows', 'planned_grid_charge_windows',
    'planned_solar_charge_windows', 'upcoming_energy_price_windows',
    'upcoming_export_price_windows', 'estimated_hourly_home_demand',
    'estimated_hourly_solar_forecast',
)


def _scalar(value: Any) -> Any:
    """Detach JSON primitives only; never stringify an arbitrary object."""
    if value is None or type(value) in (bool, int):
        return value
    if type(value) is float:
        return value if isfinite(value) else None
    if type(value) is str:
        return value[:256]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return None


def _get(value: Any, key: str) -> Any:
    return value.get(key) if isinstance(value, dict) else getattr(value, key, None)


def _project(value: Any, keys: tuple[str, ...]) -> dict[str, Any]:
    return {key: _scalar(_get(value, key)) for key in keys}


def _rows(value: Any, project: Any) -> dict[str, Any] | None:
    if not isinstance(value, (list, tuple)):
        return None
    # FIFO tails, action chronology and forecast horizons affect replay results.
    # Filter fields, not rows; keep the existing wrapper for diagnostic consumers.
    return {'items': [project(row) for row in value],
            'total': len(value), 'truncated': False}


def _margin(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    result = _project(value, ('at', 'energy', 'reserve', 'credit'))
    result['lots'] = _rows(value.get('lots'), lambda row: (
        [_scalar(row[0]), _scalar(row[1])]
        if isinstance(row, (list, tuple)) and len(row) == 2 else None
    ))
    for key in ('actions', 'pending_actions'):
        # Older snapshots have no pending evidence; preserve that absence.
        result[key] = _rows(value.get(key), lambda row: _project(row, _ACTION_KEYS))
    return result


def _runtime(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    cycle = value.get('battery_cycle_state')
    return {
        'battery_cycle_state': _project(cycle, _CYCLE_KEYS) if isinstance(cycle, dict) else None,
        'battery_grid_charge_price': _scalar(value.get('battery_grid_charge_price')),
        'battery_margin_state': _margin(value.get('battery_margin_state')),
    }


def _config(binding: Any) -> dict[str, Any]:
    data = merged_data(binding)
    result = {key: _scalar(data.get(key, default)) for key, default in _CONFIG_DEFAULTS.items()}
    result['battery_no_charge_min_soc_percent'] = _scalar(data.get(
        'battery_no_charge_min_soc_percent', result['battery_min_soc_percent']))
    for key in _SOURCE_KEYS:
        source = data.get(key)
        if isinstance(source, dict):
            source = source.get('entity_id')
        result[key] = _REDACTED if source else None
    return result


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant, entry: ConfigEntry,
) -> dict[str, Any]:
    """Return a safe snapshot for this entry and its owned planner children.

    Missing/failed children stay visible, but cannot borrow a standalone legacy
    owner's coordinator or runtime merely because they claim the same identity.
    Collection never refreshes the coordinator or loads/writes persistent storage.
    """
    planners = hass.data.get(c.DOMAIN, {})
    runtime = hass.data.get(c.RUNTIME_STATE, {})
    reports = []
    for binding in planner_bindings(entry):
        subentry_id = getattr(binding, 'subentry_id', None)
        coordinator = planners.get(binding.entry_id)
        if subentry_id is not None:
            children = planners.get(entry.entry_id)
            owned = children.get(binding.runtime_key) if isinstance(children, dict) else None
            if owned is None or coordinator is not owned:
                coordinator = None
        elif isinstance(coordinator, dict):
            coordinator = None
        state = None
        plan = None
        if coordinator is not None:
            state = {key: _scalar(getattr(coordinator, '_' + key, None))
                     for key in _COORDINATOR_KEYS}
            state['battery_margin_state'] = _margin(getattr(coordinator, '_battery_margin_state', None))
            data = getattr(coordinator, 'data', None)
            if data is not None:
                plan = _project(data, _PLAN_KEYS)
                for key in _PLAN_SERIES:
                    plan[key] = _rows(_get(data, key), lambda row: _project(row, _WINDOW_KEYS))
                # Error text can contain entity IDs, URLs or credentials.
                errors = _get(data, 'source_errors')
                plan['source_error_count'] = len(errors) if isinstance(errors, (list, tuple)) else None
        reports.append({
            'planner_entry_id': _scalar(binding.entry_id),
            'subentry_id': _scalar(subentry_id),
            'effective_config': _config(binding),
            'coordinator_available': coordinator is not None,
            'last_update_success': _scalar(getattr(coordinator, 'last_update_success', None)),
            'coordinator_state': state,
            'runtime_state': _runtime(runtime.get(binding.entry_id)) if coordinator is not None else None,
            'plan': plan,
        })
    return {'entry_id': _scalar(entry.entry_id), 'planners': reports}
