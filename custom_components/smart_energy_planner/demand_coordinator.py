"""Recorder-backed standalone consumption prediction, independent of batteries."""
from datetime import timedelta

from homeassistant.components.recorder import get_instance, history

from .const import (
    CONF_TOTAL_ENERGY_SENSOR, CONF_DEMAND_DEVICES, CONF_PRICE_SENSOR,
    CONF_PRICE_WINDOW_DURATION_HOURS, CONF_PRICE_WINDOW_WHOLE_HOUR_START,
    PLANNER_KIND_DEMAND,
)
from .demand_forecast import build_demand_forecast
from .price_helpers import extract_price_windows


def load_hourly_statistics(hass, entities, now):
    from homeassistant.components.recorder.statistics import statistics_during_period
    return statistics_during_period(hass, now-timedelta(days=28, hours=1), now,
                                    set(entities), 'hour', {'energy': 'kWh'}, {'sum'})


async def async_demand_result(coordinator, now):
    config, hass = coordinator._config, coordinator.hass
    devices = config.get(CONF_DEMAND_DEVICES, [])
    total = coordinator._config_entity_id(CONF_TOTAL_ENERGY_SENSOR)
    entities = list(dict.fromkeys([r['entity_id'] for r in devices]+([total] if total else [])))
    status = {}
    for entity in entities:
        state = hass.states.get(entity)
        status[entity] = coordinator._state_status(entity, state)
        if state and status[entity] == 'ok' and (
                state.attributes.get('unit_of_measurement') not in ('Wh', 'kWh')
                or state.attributes.get('state_class') not in ('total', 'total_increasing')):
            status[entity] = 'cumulative_energy_sensor_required'
    errors = coordinator._collect_source_errors(status)
    result = coordinator._build_pending_result('waiting_for_demand_sources', PLANNER_KIND_DEMAND, status, errors)
    if errors:
        return result
    cache = getattr(coordinator, '_demand_history_cache', None)
    signature = tuple(entities)
    if not cache or cache[0] != signature or (now-cache[1]).total_seconds() >= 3600:
        def load():
            states = history.get_significant_states(hass, now-timedelta(days=28), now,
                entities, include_start_time_state=True, significant_changes_only=False,
                no_attributes=False)
            return states, load_hourly_statistics(hass, entities, now)
        histories, hourly_statistics = await get_instance(hass).async_add_executor_job(load)
        coordinator._demand_history_cache = (signature, now, histories, hourly_statistics)
    else:
        histories = cache[2]
        hourly_statistics = cache[3]
    price_entity = coordinator._config_entity_id(CONF_PRICE_SENSOR)
    price_state = hass.states.get(price_entity) if price_entity else None
    prices = extract_price_windows(price_state.attributes if price_state else {}, None,
                                   'quarter_hourly', include_past=True, now=now)
    try:
        forecast, components, warnings = build_demand_forecast(
            {e: histories.get(e, []) for e in entities}, devices, total_entity=total,
            now=now, prices=prices,
            duration=float(config.get(CONF_PRICE_WINDOW_DURATION_HOURS, 5)),
            whole_hour=bool(config.get(CONF_PRICE_WINDOW_WHOLE_HOUR_START, True)),
            hourly_statistics=hourly_statistics)
    except ValueError as err:
        result.source_status['demand_history'] = str(err)
        result.source_errors = coordinator._collect_source_errors(result.source_status)
        return result
    result.status = 'ready'
    result.recommendation = 'demand_forecast'
    result.estimated_hourly_home_demand = forecast
    result.demand_components = components
    result.forecast_generated_at = now.isoformat()
    result.estimated_total_home_demand_kwh = round(sum(
        w['estimated_kwh'] for w in forecast if w['start'][:10] == now.date().isoformat()), 3)
    for entity, notes in warnings.items():
        if notes:
            result.source_status[f'{entity}:schedule'] = '; '.join(notes)
    result.source_errors = coordinator._collect_source_errors(result.source_status)
    if result.source_errors:
        result.status = 'ready_with_warnings'
    result.rationale = 'Independent consumption forecast; no battery margin applied'
    return result
