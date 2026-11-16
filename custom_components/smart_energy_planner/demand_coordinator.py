"""Recorder-backed standalone consumption prediction, independent of batteries."""
import asyncio
from datetime import timedelta, timezone

from homeassistant.components.recorder import get_instance, history

from .const import (
    CONF_TOTAL_ENERGY_SENSOR, CONF_DEMAND_DEVICES, CONF_PRICE_SENSOR,
    CONF_PRICE_WINDOW_DURATION_HOURS, CONF_PRICE_WINDOW_WHOLE_HOUR_START,
    PLANNER_KIND_DEMAND,
)
from .demand_forecast import build_demand_forecast
from .demand_weather import forecast_temperatures, temperature_c, temperature_hours, temperature_statistics
from .const import CONF_WEATHER_ENTITY, CONF_OUTDOOR_TEMPERATURE_SENSOR
from .price_helpers import extract_price_windows


async def async_weather_inputs(coordinator, now):
    """Optional Recorder observations and a read-only HA hourly forecast query."""
    hass = coordinator.hass
    weather = coordinator._config_entity_id(CONF_WEATHER_ENTITY)
    sensor = coordinator._config_entity_id(CONF_OUTDOOR_TEMPERATURE_SENSOR)
    sources = {'weather_entity': weather, 'outdoor_temperature_sensor': sensor,
               'forecast': 'not_configured', 'history': 'not_configured'}
    observed, predicted = {}, {}
    hour = int(now.timestamp() // 3600)
    if sensor:
        cache = getattr(coordinator, '_demand_temperature_cache', None)
        if cache and cache[:2] == (sensor, hour):
            observed = cache[2]
            if len(cache) > 3:
                sources.update(cache[3])
        else:
            try:
                def load():
                    return history.get_significant_states(hass, now-timedelta(days=28), now,
                        [sensor], include_start_time_state=True, significant_changes_only=False,
                        no_attributes=False)
                rows = await get_instance(hass).async_add_executor_job(load)
                observed = temperature_hours(rows.get(sensor, []), now=now)
            except Exception as err:  # Optional input must not discard energy history.
                sources['history'] = f'unavailable: {type(err).__name__}'
            try:
                rows = await get_instance(hass).async_add_executor_job(
                    lambda: load_temperature_statistics(hass, sensor, now))
                means = temperature_statistics(rows.get(sensor, []), now=now)
                means.update(observed)  # Fine-grained observations take precedence.
                observed = means
                sources['statistics'] = 'ok' if means else 'insufficient_coverage'
            except Exception as err:
                sources['statistics'] = f'unavailable: {type(err).__name__}'
            if sources['history'] == 'not_configured':
                sources['history'] = 'ok' if observed else 'insufficient_coverage'
            coordinator._demand_temperature_cache = (sensor, hour, observed,
                {key: sources[key] for key in ('history', 'statistics')})
        if sources['history'] == 'not_configured':
            sources['history'] = 'ok' if observed else 'insufficient_coverage'
    sources['temperature_history_hours'] = len(observed)
    if weather:
        state = hass.states.get(weather)
        if state is None or state.state in ('unknown', 'unavailable'):
            sources['forecast'] = 'unavailable'
        elif not (isinstance(state.attributes.get('supported_features'), int)
                  and state.attributes['supported_features'] & 2):
            sources['forecast'] = 'hourly_not_supported'
        else:
            cache = getattr(coordinator, '_demand_weather_cache', None)
            if cache and cache[:2] == (weather, hour):
                predicted = cache[2]
            else:
                try:
                    response = await asyncio.wait_for(hass.services.async_call('weather', 'get_forecasts',
                        {'entity_id': weather, 'type': 'hourly'}, blocking=True, return_response=True), timeout=30)
                    predicted = forecast_temperatures(response.get(weather, {}).get('forecast', []),
                                                      state.attributes.get('temperature_unit'))
                    coordinator._demand_weather_cache = (weather, hour, predicted)
                except Exception as err:  # HA/provider errors degrade to the base model.
                    sources['forecast'] = f'unavailable: {type(err).__name__}'
            if sources['forecast'] == 'not_configured':
                sources['forecast'] = 'ok' if predicted else 'invalid_or_empty'
    # Only forecasts for the current/future hour are eligible. Completed hours
    # require measurements: neither stale forecasts nor current values fill gaps.
    current_hour = now.astimezone(timezone.utc).replace(
        minute=0, second=0, microsecond=0)
    combined = {t:v for t,v in predicted.items() if t >= current_hour}
    combined.update({t:v for t,v in observed.items()
                     if t.astimezone(now.tzinfo).date() == now.date()
                     and t+timedelta(hours=1) <= now})
    sources['current_hour'] = 'forecast' if current_hour in combined else 'unavailable'
    # The current reading is a daily-mean proxy, never completed training data.
    # Query it on every refresh, independently of the hourly Recorder cache.
    for entity, source, value_key, unit_key in (
            (sensor, 'outdoor_temperature_sensor', None, 'unit_of_measurement'),
            (weather, 'weather_entity', 'temperature', 'temperature_unit')):
        state = hass.states.get(entity) if entity else None
        if state is None or state.state in ('unknown', 'unavailable'):
            continue
        value = temperature_c(state.attributes.get(value_key) if value_key else state.state,
                              state.attributes.get(unit_key))
        if value is not None:
            combined[current_hour] = value
            sources['current_hour'] = source
            break
    return observed, combined, sources


def load_temperature_statistics(hass, entity, now):
    from homeassistant.components.recorder.statistics import statistics_during_period
    return statistics_during_period(hass, now-timedelta(days=28), now,
                                    {entity}, 'hour', {'temperature': '°C'}, {'mean'})


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
    signature = (tuple(entities), int(now.timestamp() // 3600))
    if not cache or cache[0] != signature or (now-cache[1]).total_seconds() >= 3600:
        def load():
            states = history.get_significant_states(hass, now-timedelta(days=28), now,
                entities, include_start_time_state=True, significant_changes_only=False,
                no_attributes=False)
            return states, load_hourly_statistics(hass, entities, now)
        async def async_load():
            return await get_instance(hass).async_add_executor_job(load)
        shared = getattr(coordinator, 'shared_sources', None)
        histories, hourly_statistics = (await shared.async_cached(
            ('demand_history', tuple(sorted(entities)), int(now.timestamp() // 3600)), async_load, ttl=3600)
            if shared else await async_load())
        coordinator._demand_history_cache = (signature, now, histories, hourly_statistics)
    else:
        histories = cache[2]
        hourly_statistics = cache[3]
    price_entity = coordinator._config_entity_id(CONF_PRICE_SENSOR)
    price_state = hass.states.get(price_entity) if price_entity else None
    prices = extract_price_windows(price_state.attributes if price_state else {}, None,
                                   'quarter_hourly', include_past=True, now=now)
    observed, predicted, sources = await async_weather_inputs(coordinator, now)
    result.demand_weather_sources = sources
    # Optional-source degradation warns without invalidating energy history.
    for config_key, source_key in ((CONF_WEATHER_ENTITY, 'forecast'),
                                   (CONF_OUTDOOR_TEMPERATURE_SENSOR, 'history')):
        if sources[config_key]:
            result.source_status[config_key] = sources[source_key]
    if sources.get('statistics', '').startswith('unavailable:'):
        result.source_status['outdoor_temperature_statistics'] = sources['statistics']
    result.demand_accuracy = {}
    try:
        forecast, components, warnings = build_demand_forecast(
            {e: histories.get(e, []) for e in entities}, devices, total_entity=total,
            now=now, prices=prices,
            duration=float(config.get(CONF_PRICE_WINDOW_DURATION_HOURS, 5)),
            whole_hour=bool(config.get(CONF_PRICE_WINDOW_WHOLE_HOUR_START, True)),
            hourly_statistics=hourly_statistics, temperature_history=observed,
            temperature_forecast=predicted, diagnostics=result.demand_accuracy)
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
