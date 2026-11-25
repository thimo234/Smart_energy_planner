"""Config flow for Smart Energy Planner."""

from __future__ import annotations

from typing import Any
import math
from uuid import uuid4

import voluptuous as vol

from homeassistant.components.sensor import SensorDeviceClass
from homeassistant.config_entries import ConfigFlow, ConfigFlowResult, ConfigSubentryFlow, OptionsFlow
from homeassistant.const import UnitOfEnergy
from homeassistant.core import HomeAssistant
from homeassistant.helpers import selector
from .const import (PLANNER_KIND_DEMAND, CONF_DEMAND_DEVICES, CONF_DEMAND_ENERGY_SENSORS,
                    CONF_DEMAND_FORECAST_SENSOR)
from .const import PLANNER_KIND_HOUSEHOLD, SHARED_SOURCE_KEYS
from .household import (adoption_conflicts, child_data, legacy_available, merged_data,
                        PRICE_WINDOW_MEMBERS, price_window_members, price_window_source_retired)


def _build_household_schema(hass, data=None):
    """Reuse source selectors; solar is optional for price/demand-only homes."""
    fields = {}
    for marker, value in _build_battery_schema(hass, data).schema.items():
        key = marker.schema
        if key not in SHARED_SOURCE_KEYS and key != CONF_PLANNER_NAME:
            continue
        if key in SHARED_SOURCE_KEYS - {CONF_PRICE_SENSOR, CONF_EXPORT_PRICE_TAX_DEDUCTION}:
            current = (data or {}).get(key)
            marker = vol.Optional(key, **({'default': current} if current else {}))
        fields[marker] = value
    for key, domain, device_class in (
            ('weather_entity', 'weather', None),
            ('outdoor_temperature_sensor', 'sensor', 'temperature')):
        current = (data or {}).get(key)
        filters = {'domain': domain}
        if device_class:
            filters['device_class'] = device_class
        fields[vol.Optional(key, **({'default': current} if current else {}))] = selector.EntitySelector(
            selector.EntitySelectorConfig(**filters))
    return vol.Schema(fields)


from .const import (
    CONF_PRICE_PEAK_MIN_DURATION_MINUTES, DEFAULT_PRICE_PEAK_MIN_DURATION_MINUTES,
    CONF_PRICE_PEAK_MIN_GAP_HOURS,
    DEFAULT_PRICE_PEAK_MIN_GAP_HOURS,
    CONF_PRICE_PEAK_MIN_PROMINENCE_PERCENT,
    DEFAULT_PRICE_PEAK_MIN_PROMINENCE_PERCENT,
    CONF_BATTERY_CAPACITY_KWH,
    CONF_BATTERY_CHARGE_SAFETY_MARGIN,
    CONF_BATTERY_DEMAND_SAFETY_MARGIN,
    CONF_BATTERY_SOLAR_TRANSITION_BUFFER_MINUTES,
    CONF_BATTERY_OPTIMIZER_V2,
    CONF_BATTERY_OPTIMIZER_V2_FULL,
    CONF_BATTERY_ENABLED,
    CONF_BATTERY_MAX_CHARGE_KW,
    CONF_BATTERY_MAX_DISCHARGE_KW,
    CONF_BATTERY_MIN_SOC_PERCENT,
    CONF_BATTERY_NO_CHARGE_MIN_SOC_PERCENT,
    CONF_BATTERY_MIN_PROFIT_PER_KWH,
    CONF_BATTERY_SOC_SENSOR,
    CONF_EXPORT_PRICE_SENSOR,
    CONF_EXPORT_PRICE_TAX_DEDUCTION,
    DEFAULT_EXPORT_PRICE_TAX_DEDUCTION,
    CONF_PLANNER_NAME,
    CONF_PLANNER_KIND,
    CONF_PRICE_SENSOR,
    CONF_PRICE_RESOLUTION,
    CONF_PRICE_WINDOW_TYPE,
    CONF_PRICE_WINDOW_DURATION_HOURS,
    CONF_PRICE_WINDOW_WHOLE_HOUR_START,
    CONF_SOLCAST_TODAY_SENSOR,
    CONF_SOLCAST_TOMORROW_SENSOR,
    CONF_TOTAL_ENERGY_SENSOR,
    DEFAULT_BATTERY_CAPACITY_KWH,
    DEFAULT_BATTERY_ENABLED,
    DEFAULT_BATTERY_CHARGE_SAFETY_MARGIN,
    DEFAULT_BATTERY_DEMAND_SAFETY_MARGIN,
    DEFAULT_BATTERY_SOLAR_TRANSITION_BUFFER_MINUTES,
    MAX_BATTERY_SOLAR_TRANSITION_BUFFER_MINUTES,
    DEFAULT_BATTERY_MAX_CHARGE_KW,
    DEFAULT_BATTERY_MAX_DISCHARGE_KW,
    DEFAULT_BATTERY_MIN_SOC_PERCENT,
    DEFAULT_BATTERY_MIN_PROFIT_PER_KWH,
    DEFAULT_NAME,
    DEFAULT_PLANNER_KIND,
    DEFAULT_PRICE_RESOLUTION,
    DEFAULT_PRICE_WINDOW_TYPE,
    DEFAULT_PRICE_WINDOW_DURATION_HOURS,
    DEFAULT_PRICE_WINDOW_WHOLE_HOUR_START,
    DOMAIN,
    PLANNER_KIND_BATTERY,
    PLANNER_KIND_PRICE_WINDOW,
    PRICE_RESOLUTION_HOURLY,
    PRICE_RESOLUTION_QUARTER_HOURLY,
    PRICE_WINDOW_TYPE_CHEAPEST,
    PRICE_WINDOW_TYPE_MOST_EXPENSIVE,
)


def _sensor_options(hass: HomeAssistant) -> list[Any]:
    """Return all sensor states."""
    return list(hass.states.async_all("sensor"))


def _filter_price_sensors(hass: HomeAssistant) -> list[str]:
    """Return Nord Pool compatible price sensors."""
    return [
        state.entity_id
        for state in _sensor_options(hass)
        if state.attributes.get("raw_today")
    ]


def _filter_solcast_sensors(hass: HomeAssistant) -> list[str]:
    """Return Solcast forecast sensors that include today's estimate."""
    return [
        state.entity_id
        for state in _sensor_options(hass)
        if "estimate" in state.attributes or state.attributes.get("detailedHourly")
    ]


def _filter_energy_sensors(hass: HomeAssistant) -> list[str]:
    """Return likely cumulative energy sensors."""
    return [
        state.entity_id
        for state in _sensor_options(hass)
        if state.attributes.get("device_class")
        in (SensorDeviceClass.ENERGY, SensorDeviceClass.ENERGY.value)
        or state.attributes.get("unit_of_measurement") == UnitOfEnergy.KILO_WATT_HOUR
    ]


def _filter_battery_soc_sensors(hass: HomeAssistant) -> list[str]:
    """Return likely battery state-of-charge percentage sensors."""
    return [
        state.entity_id
        for state in _sensor_options(hass)
        if state.attributes.get("device_class")
        in (SensorDeviceClass.BATTERY, SensorDeviceClass.BATTERY.value)
        or state.attributes.get("unit_of_measurement") == "%"
    ]


def _entity_selector(
    include_entities: list[str],
    *,
    current_value: str | None,
    domain: str | None = "sensor",
) -> selector.EntitySelector:
    """Build an entity selector and keep the current entity selectable."""
    include_entities = list(dict.fromkeys(include_entities))
    if current_value and current_value not in include_entities:
        include_entities = [*include_entities, current_value]

    config: dict[str, Any] = {}
    if domain:
        config["domain"] = domain
    if include_entities:
        config["include_entities"] = include_entities

    return selector.EntitySelector(selector.EntitySelectorConfig(**config))


def _base_defaults(user_input: dict[str, Any] | None) -> dict[str, Any]:
    return _normalize_selector_values(user_input or {})


def _normalize_selector_value(value: Any) -> Any:
    if isinstance(value, dict) and "entity_id" in value:
        return value["entity_id"]
    return value


def _normalize_selector_values(data: dict[str, Any]) -> dict[str, Any]:
    return {
        key: _normalize_selector_value(value)
        for key, value in data.items()
    }


def _merge_options_data(
    existing: dict[str, Any],
    user_input: dict[str, Any],
    *,
    planner_kind: str,
) -> dict[str, Any]:
    """Preserve existing option values that Home Assistant did not resubmit."""

    return {
        **existing,
        **user_input,
        CONF_PLANNER_KIND: planner_kind,
    }


def _build_battery_schema(hass: HomeAssistant, user_input: dict[str, Any] | None = None) -> vol.Schema:
    """Build the battery planner schema."""
    user_input = _base_defaults(user_input)
    return vol.Schema(
        {
            vol.Required(
                CONF_PLANNER_NAME,
                default=user_input.get(CONF_PLANNER_NAME, f"{DEFAULT_NAME} Battery"),
            ): selector.TextSelector(),
            vol.Required(
                CONF_PRICE_SENSOR, default=user_input.get(CONF_PRICE_SENSOR)
            ): _entity_selector(_filter_price_sensors(hass), current_value=user_input.get(CONF_PRICE_SENSOR)),
            vol.Optional(
                CONF_EXPORT_PRICE_SENSOR,
                default=user_input.get(CONF_EXPORT_PRICE_SENSOR),
            ): _entity_selector(
                _filter_price_sensors(hass),
                current_value=user_input.get(CONF_EXPORT_PRICE_SENSOR),
            ),
            vol.Optional(
                CONF_EXPORT_PRICE_TAX_DEDUCTION,
                default=user_input.get(CONF_EXPORT_PRICE_TAX_DEDUCTION, DEFAULT_EXPORT_PRICE_TAX_DEDUCTION),
            ): selector.NumberSelector(
                # HA rejects numeric steps below 0.001; allow precise tax values.
                selector.NumberSelectorConfig(min=0, max=1, step="any", mode=selector.NumberSelectorMode.BOX)
            ),
            vol.Required(
                CONF_SOLCAST_TODAY_SENSOR, default=user_input.get(CONF_SOLCAST_TODAY_SENSOR)
            ): _entity_selector(
                _filter_solcast_sensors(hass), current_value=user_input.get(CONF_SOLCAST_TODAY_SENSOR)
            ),
            vol.Optional(
                CONF_SOLCAST_TOMORROW_SENSOR, default=user_input.get(CONF_SOLCAST_TOMORROW_SENSOR)
            ): _entity_selector(
                _filter_solcast_sensors(hass), current_value=user_input.get(CONF_SOLCAST_TOMORROW_SENSOR)
            ),
            vol.Optional(
                CONF_TOTAL_ENERGY_SENSOR, default=user_input.get(CONF_TOTAL_ENERGY_SENSOR)
            ): _entity_selector(
                _filter_energy_sensors(hass), current_value=user_input.get(CONF_TOTAL_ENERGY_SENSOR)
            ),
            vol.Optional(CONF_DEMAND_FORECAST_SENSOR,
                         default=user_input.get(CONF_DEMAND_FORECAST_SENSOR)): _entity_selector(
                [s.entity_id for s in _sensor_options(hass) if s.attributes.get('forecast_version') == 1],
                current_value=user_input.get(CONF_DEMAND_FORECAST_SENSOR)),
            vol.Required(
                CONF_BATTERY_SOC_SENSOR, default=user_input.get(CONF_BATTERY_SOC_SENSOR)
            ): _entity_selector(
                _filter_battery_soc_sensors(hass), current_value=user_input.get(CONF_BATTERY_SOC_SENSOR)
            ),
            vol.Required(
                CONF_PRICE_RESOLUTION,
                default=user_input.get(CONF_PRICE_RESOLUTION, DEFAULT_PRICE_RESOLUTION),
            ): selector.SelectSelector(
                selector.SelectSelectorConfig(
                    options=[
                        selector.SelectOptionDict(value=PRICE_RESOLUTION_HOURLY, label="Hourly contract"),
                        selector.SelectOptionDict(
                            value=PRICE_RESOLUTION_QUARTER_HOURLY, label="Quarter-hour contract"
                        ),
                    ],
                    mode=selector.SelectSelectorMode.DROPDOWN,
                )
            ),
            vol.Required(
                CONF_BATTERY_ENABLED, default=user_input.get(CONF_BATTERY_ENABLED, True)
            ): selector.BooleanSelector(),
            vol.Required(
                CONF_BATTERY_CAPACITY_KWH,
                default=user_input.get(CONF_BATTERY_CAPACITY_KWH, DEFAULT_BATTERY_CAPACITY_KWH),
            ): selector.NumberSelector(
                selector.NumberSelectorConfig(min=0, max=100, step=0.1, mode=selector.NumberSelectorMode.BOX)
            ),
            vol.Required(
                CONF_BATTERY_MIN_SOC_PERCENT,
                default=user_input.get(CONF_BATTERY_MIN_SOC_PERCENT, DEFAULT_BATTERY_MIN_SOC_PERCENT),
            ): selector.NumberSelector(
                selector.NumberSelectorConfig(min=0, max=100, step=1, mode=selector.NumberSelectorMode.BOX)
            ),
            vol.Required(
                CONF_BATTERY_NO_CHARGE_MIN_SOC_PERCENT,
                default=user_input.get(
                    CONF_BATTERY_NO_CHARGE_MIN_SOC_PERCENT,
                    user_input.get(CONF_BATTERY_MIN_SOC_PERCENT, DEFAULT_BATTERY_MIN_SOC_PERCENT),
                ),
            ): selector.NumberSelector(
                selector.NumberSelectorConfig(min=0, max=100, step=1, mode=selector.NumberSelectorMode.BOX)
            ),
            vol.Required(
                CONF_BATTERY_MIN_PROFIT_PER_KWH,
                default=user_input.get(
                    CONF_BATTERY_MIN_PROFIT_PER_KWH, DEFAULT_BATTERY_MIN_PROFIT_PER_KWH
                ),
            ): selector.NumberSelector(
                selector.NumberSelectorConfig(min=0, max=1, step=0.01, mode=selector.NumberSelectorMode.BOX)
            ),
            vol.Required(
                CONF_BATTERY_MAX_CHARGE_KW,
                default=user_input.get(CONF_BATTERY_MAX_CHARGE_KW, DEFAULT_BATTERY_MAX_CHARGE_KW),
            ): selector.NumberSelector(
                selector.NumberSelectorConfig(min=0, max=50, step=0.1, mode=selector.NumberSelectorMode.BOX)
            ),
            vol.Required(
                CONF_BATTERY_MAX_DISCHARGE_KW,
                default=user_input.get(
                    CONF_BATTERY_MAX_DISCHARGE_KW, DEFAULT_BATTERY_MAX_DISCHARGE_KW
                ),
            ): selector.NumberSelector(
                selector.NumberSelectorConfig(min=0, max=50, step=0.1, mode=selector.NumberSelectorMode.BOX)
            ),
            vol.Required(
                CONF_BATTERY_CHARGE_SAFETY_MARGIN,
                default=user_input.get(CONF_BATTERY_CHARGE_SAFETY_MARGIN, DEFAULT_BATTERY_CHARGE_SAFETY_MARGIN),
            ): selector.NumberSelector(
                selector.NumberSelectorConfig(min=0, max=50, step=5, mode=selector.NumberSelectorMode.SLIDER)
            ),
            vol.Required(
                CONF_BATTERY_DEMAND_SAFETY_MARGIN,
                default=user_input.get(CONF_BATTERY_DEMAND_SAFETY_MARGIN, DEFAULT_BATTERY_DEMAND_SAFETY_MARGIN),
            ): selector.NumberSelector(
                selector.NumberSelectorConfig(min=0, max=100, step=5, mode=selector.NumberSelectorMode.SLIDER)
            ),
            vol.Required(
                CONF_BATTERY_SOLAR_TRANSITION_BUFFER_MINUTES,
                default=user_input.get(CONF_BATTERY_SOLAR_TRANSITION_BUFFER_MINUTES,
                                       DEFAULT_BATTERY_SOLAR_TRANSITION_BUFFER_MINUTES),
            ): selector.NumberSelector(selector.NumberSelectorConfig(
                min=0, max=MAX_BATTERY_SOLAR_TRANSITION_BUFFER_MINUTES, step=1,
                mode=selector.NumberSelectorMode.BOX,
            )),
            vol.Optional(CONF_BATTERY_OPTIMIZER_V2,
                         default=user_input.get(CONF_BATTERY_OPTIMIZER_V2, False)): selector.BooleanSelector(),
            vol.Optional(CONF_BATTERY_OPTIMIZER_V2_FULL,
                         default=user_input.get(CONF_BATTERY_OPTIMIZER_V2_FULL, False)): selector.BooleanSelector(),
        }
    )


def _build_price_window_schema(hass: HomeAssistant, user_input: dict[str, Any] | None = None) -> vol.Schema:
    """Build the price window planner schema."""
    user_input = _base_defaults(user_input)
    return vol.Schema(
        {
            vol.Required(
                CONF_PLANNER_NAME,
                default=user_input.get(CONF_PLANNER_NAME, f"{DEFAULT_NAME} Price Windows"),
            ): selector.TextSelector(),
            vol.Required(
                CONF_PRICE_SENSOR, default=user_input.get(CONF_PRICE_SENSOR)
            ): _entity_selector(_filter_price_sensors(hass), current_value=user_input.get(CONF_PRICE_SENSOR)),
            vol.Required(
                CONF_PRICE_WINDOW_TYPE,
                default=user_input.get(CONF_PRICE_WINDOW_TYPE, DEFAULT_PRICE_WINDOW_TYPE),
            ): selector.SelectSelector(
                selector.SelectSelectorConfig(
                    options=[
                        selector.SelectOptionDict(value=PRICE_WINDOW_TYPE_CHEAPEST, label="Cheapest window"),
                        selector.SelectOptionDict(value=PRICE_WINDOW_TYPE_MOST_EXPENSIVE, label="Successive price peaks"),
                    ],
                    mode=selector.SelectSelectorMode.DROPDOWN,
                )
            ),
            vol.Required(
                CONF_PRICE_WINDOW_DURATION_HOURS,
                default=user_input.get(CONF_PRICE_WINDOW_DURATION_HOURS, DEFAULT_PRICE_WINDOW_DURATION_HOURS),
            ): selector.NumberSelector(
                selector.NumberSelectorConfig(min=1, max=24, step=1, mode=selector.NumberSelectorMode.BOX)
            ),
            vol.Required(
                CONF_PRICE_PEAK_MIN_PROMINENCE_PERCENT,
                default=user_input.get(CONF_PRICE_PEAK_MIN_PROMINENCE_PERCENT, DEFAULT_PRICE_PEAK_MIN_PROMINENCE_PERCENT),
            ): selector.NumberSelector(
                selector.NumberSelectorConfig(min=0, max=100, step=1, mode=selector.NumberSelectorMode.BOX)
            ),
            vol.Required(
                CONF_PRICE_PEAK_MIN_DURATION_MINUTES,
                default=user_input.get(CONF_PRICE_PEAK_MIN_DURATION_MINUTES, DEFAULT_PRICE_PEAK_MIN_DURATION_MINUTES),
            ): selector.NumberSelector(
                selector.NumberSelectorConfig(min=0, max=1440, step=15, mode=selector.NumberSelectorMode.BOX)
            ),
            vol.Required(
                CONF_PRICE_PEAK_MIN_GAP_HOURS,
                default=user_input.get(CONF_PRICE_PEAK_MIN_GAP_HOURS, DEFAULT_PRICE_PEAK_MIN_GAP_HOURS),
            ): selector.NumberSelector(
                selector.NumberSelectorConfig(min=0.25, max=48, step=0.25, mode=selector.NumberSelectorMode.BOX)
            ),
            vol.Required(
                CONF_PRICE_WINDOW_WHOLE_HOUR_START,
                default=user_input.get(
                    CONF_PRICE_WINDOW_WHOLE_HOUR_START,
                    DEFAULT_PRICE_WINDOW_WHOLE_HOUR_START,
                ),
            ): selector.BooleanSelector(),
            vol.Required(
                CONF_PRICE_RESOLUTION,
                default=user_input.get(CONF_PRICE_RESOLUTION, DEFAULT_PRICE_RESOLUTION),
            ): selector.SelectSelector(
                selector.SelectSelectorConfig(
                    options=[
                        selector.SelectOptionDict(value=PRICE_RESOLUTION_HOURLY, label="Hourly contract"),
                        selector.SelectOptionDict(
                            value=PRICE_RESOLUTION_QUARTER_HOURLY, label="Quarter-hour contract"
                        ),
                    ],
                    mode=selector.SelectSelectorMode.DROPDOWN,
                )
            ),
        }
    )


def _schema_for_kind(
    hass: HomeAssistant,
    planner_kind: str,
    user_input: dict[str, Any] | None = None,
) -> vol.Schema:
    if planner_kind == PLANNER_KIND_BATTERY:
        return _build_battery_schema(hass, user_input)
    if planner_kind == PLANNER_KIND_PRICE_WINDOW:
        return _build_price_window_schema(hass, user_input)
    if planner_kind == PLANNER_KIND_DEMAND:
        return _build_demand_schema(hass, user_input)
    raise ValueError(f"Unsupported planner kind: {planner_kind}")


def _title_for_kind(planner_kind: str) -> str:
    if planner_kind == PLANNER_KIND_BATTERY:
        return f"{DEFAULT_NAME} Battery"
    if planner_kind == PLANNER_KIND_PRICE_WINDOW:
        return f"{DEFAULT_NAME} Price Windows"
    if planner_kind == PLANNER_KIND_DEMAND:
        return f"{DEFAULT_NAME} Consumption"
    return f"{DEFAULT_NAME} Battery"


def _build_demand_schema(hass, user_input=None):
    data = _base_defaults(user_input)
    return vol.Schema({
        vol.Required(CONF_PLANNER_NAME, default=data.get(CONF_PLANNER_NAME, 'Home Consumption')): selector.TextSelector(),
        vol.Optional(CONF_TOTAL_ENERGY_SENSOR, default=data.get(CONF_TOTAL_ENERGY_SENSOR)):
            _demand_meter_selector(hass),
        vol.Required(CONF_DEMAND_ENERGY_SENSORS, default=data.get(CONF_DEMAND_ENERGY_SENSORS, [])):
            _demand_meter_selector(hass, multiple=True),
        vol.Optional(CONF_PRICE_SENSOR, default=data.get(CONF_PRICE_SENSOR)):
            _entity_selector(_filter_price_sensors(hass), current_value=data.get(CONF_PRICE_SENSOR)),
        vol.Required(CONF_PRICE_WINDOW_DURATION_HOURS, default=data.get(CONF_PRICE_WINDOW_DURATION_HOURS, 5)):
            selector.NumberSelector(selector.NumberSelectorConfig(min=1, max=24, step=1, mode=selector.NumberSelectorMode.BOX)),
        vol.Required(CONF_PRICE_WINDOW_WHOLE_HOUR_START, default=data.get(CONF_PRICE_WINDOW_WHOLE_HOUR_START, True)):
            selector.BooleanSelector(),
    })


def _demand_meter_ids(hass):
    """Only meters with the units and counter semantics accepted by learning."""
    result = []
    for state in _sensor_options(hass):
        if (state.attributes.get('unit_of_measurement') not in ('Wh', 'kWh')
                or state.attributes.get('state_class') not in ('total', 'total_increasing')):
            continue
        try:
            if not math.isfinite(float(state.state)):
                continue
        except (TypeError, ValueError):
            continue
        result.append(state.entity_id)
    return result


def _demand_meter_selector(hass, multiple=False):
    # A closed select also stays closed when no meters match. An empty entity
    # include_entities list otherwise permits all entities in HA.
    options = [selector.SelectOptionDict(value=entity, label=(
        f"{hass.states.get(entity).attributes.get('friendly_name', entity)} ({entity})"))
        for entity in _demand_meter_ids(hass)]
    return selector.SelectSelector(selector.SelectSelectorConfig(
        options=options, multiple=multiple, custom_value=False,
        mode=selector.SelectSelectorMode.DROPDOWN))


class DemandDeviceFlow:
    """Configure each selected meter with normal HA selectors, also in options."""

    async def _start_demand_devices(self, data, finish_step):
        self._demand_data = dict(data)
        self._demand_finish_step = finish_step
        self._demand_queue = list(data.get(CONF_DEMAND_ENERGY_SENSORS, []))
        self._demand_rules = []
        total = data.get(CONF_TOTAL_ENERGY_SENSOR)
        error = ('duplicate_energy_source' if total in self._demand_queue or len(set(self._demand_queue)) != len(self._demand_queue)
                 else 'no_energy_sources' if not total and not self._demand_queue else None)
        selected = self._demand_queue + ([total] if total else [])
        if not error and any(entity not in _demand_meter_ids(self.hass) for entity in selected):
            error = 'unsupported_energy_meter'
        if error:
            schema = self._child_schema(data) if hasattr(self, '_child_schema') else _build_demand_schema(self.hass, data)
            return self.async_show_form(step_id=finish_step, data_schema=schema, errors={'base': error})
        return await self.async_step_device()

    async def _save_demand_device(self, rule):
        rule['entity_id'] = self._demand_queue.pop(0)
        self._demand_rules.append(rule)
        return await self.async_step_device()

    async def async_step_device(self, user_input=None):
        errors = {}
        if user_input is not None:
            timing = user_input['timing']
            if timing == 'cheap' and not self._demand_data.get(CONF_PRICE_SENSOR):
                errors['base'] = 'price_sensor_required'
            elif timing == 'learned':
                return await self._save_demand_device({'timing': timing})
            else:
                self._demand_timing = timing
                return await self.async_step_device_schedule()
        if not self._demand_queue:
            self._demand_data[CONF_DEMAND_DEVICES] = self._demand_rules
            self._demand_devices_complete = True
            return await getattr(self, 'async_step_'+self._demand_finish_step)(self._demand_data)
        entity = self._demand_queue[0]
        defaults = user_input if errors else next((r for r in self._demand_data.get(CONF_DEMAND_DEVICES, [])
                                                 if r['entity_id'] == entity), {})
        schema = vol.Schema({
            vol.Required('timing', default=defaults.get('timing', 'learned')): selector.SelectSelector(
                selector.SelectSelectorConfig(options=[
                    selector.SelectOptionDict(value='learned', label='Learn clock time and weekly routine'),
                    selector.SelectOptionDict(value='fixed', label='Fixed start time'),
                    selector.SelectOptionDict(value='cheap', label='Start relative to cheapest block')],
                    mode=selector.SelectSelectorMode.DROPDOWN)),
        })
        return self.async_show_form(step_id='device', data_schema=schema, errors=errors,
                                   description_placeholders={'entity': entity})

    async def async_step_device_schedule(self, user_input=None):
        timing = self._demand_timing
        if user_input is not None:
            if timing == 'fixed':
                rule = dict(timing=timing, start_time=user_input['start_time'],
                            weekdays=[int(d) for d in user_input['weekdays']])
            else:
                rule = dict(timing=timing, offset_hours=user_input['offset_hours'],
                            shiftable_fraction=user_input.get('shiftable_fraction', 1))
            return await self._save_demand_device(rule)
        entity = self._demand_queue[0]
        defaults = next((r for r in self._demand_data.get(CONF_DEMAND_DEVICES, [])
                         if r['entity_id'] == entity), {})
        fields = {}
        if timing == 'fixed':
            fields[vol.Required('start_time', default=defaults.get('start_time', '17:30:00'))] = selector.TimeSelector()
            fields[vol.Required('weekdays', default=[str(d) for d in defaults.get('weekdays', range(7))])] = selector.SelectSelector(
                selector.SelectSelectorConfig(options=[selector.SelectOptionDict(value=str(i), label=d)
                    for i,d in enumerate(['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'])], multiple=True))
        else:
            fields[vol.Required('offset_hours', default=defaults.get('offset_hours', 0))] = selector.NumberSelector(
                selector.NumberSelectorConfig(min=0, max=23, step=.25, mode=selector.NumberSelectorMode.BOX))
            fields[vol.Required('shiftable_fraction', default=defaults.get('shiftable_fraction', 1))] = selector.NumberSelector(
                selector.NumberSelectorConfig(min=0, max=1, step=.05, mode=selector.NumberSelectorMode.BOX))
        return self.async_show_form(step_id='device_schedule', data_schema=vol.Schema(fields),
                                   description_placeholders={'entity': entity})


class PlannerSubentryFlow(DemandDeviceFlow, ConfigSubentryFlow):
    """Add, explicitly adopt, or reconfigure a planner under its household."""

    def _child_schema(self, data=None):
        fields = _schema_for_kind(self.hass, self._subentry_type, data).schema
        return vol.Schema({key: value for key, value in fields.items()
                           if key.schema not in SHARED_SOURCE_KEYS})

    def _legacy_choices(self):
        result = []
        for entry in self.hass.config_entries.async_entries(DOMAIN):
            if (merged_data(entry).get(CONF_PLANNER_KIND, PLANNER_KIND_BATTERY) == self._subentry_type
                    and legacy_available(self.hass, entry)):
                result.append(entry)
        return result

    async def async_step_user(self, user_input=None):
        if self._subentry_type == PLANNER_KIND_PRICE_WINDOW:
            self._price_group()
        if merged_data(self._get_entry()).get(CONF_PLANNER_KIND) != PLANNER_KIND_HOUSEHOLD:
            return self.async_abort(reason='unsupported_planner_kind')
        if user_input is not None:
            identity = user_input.get('legacy_entry_id', 'new')
            if identity == 'new':
                return await self.async_step_configure()
            legacy = next((e for e in self._legacy_choices() if e.entry_id == identity), None)
            if legacy is None:
                return self.async_abort(reason='legacy_unavailable')
            conflicts = adoption_conflicts(self._get_entry(), legacy)
            if conflicts:
                return self.async_abort(reason='shared_source_conflict',
                    description_placeholders={'sources': ', '.join(sorted(conflicts))})
            self._legacy_id = identity
            return await self.async_step_adopt()
        choices = [selector.SelectOptionDict(value='new', label='New planner')]
        choices.extend(selector.SelectOptionDict(value=e.entry_id, label=e.title) for e in self._legacy_choices())
        return self.async_show_form(step_id='user', data_schema=vol.Schema({
            vol.Required('legacy_entry_id', default='new'): selector.SelectSelector(
                selector.SelectSelectorConfig(options=choices))}))

    async def async_step_adopt(self, user_input=None):
        legacy = self.hass.config_entries.async_get_entry(self._legacy_id)
        if legacy is None or not legacy_available(self.hass, legacy):
            return self.async_abort(reason='legacy_unavailable')
        conflicts = adoption_conflicts(self._get_entry(), legacy)
        if conflicts:
            return self.async_abort(reason='shared_source_conflict',
                description_placeholders={'sources': ', '.join(sorted(conflicts))})
        if user_input is not None and user_input.get('confirm'):
            data = {**child_data(merged_data(legacy)), '_legacy_entry_id': legacy.entry_id}
            data.pop('_adopted_by', None)
            if self._subentry_type == PLANNER_KIND_PRICE_WINDOW:
                return self._save_price_member(data, legacy.entry_id)
            return self.async_create_entry(title=legacy.title, data=data)
        return self.async_show_form(step_id='adopt', data_schema=vol.Schema({
            vol.Required('confirm', default=False): selector.BooleanSelector()}),
            description_placeholders={'planner': legacy.title, 'household': self._get_entry().title})

    def _price_group(self):
        # Bind the flow to its original group, not whichever group exists later.
        entries = self._get_entry().subentries
        if not hasattr(self, '_price_group_id'):
            group = next((s for s in entries.values()
                          if s.subentry_type == PLANNER_KIND_PRICE_WINDOW), None)
            self._price_group_id = self.context.get('subentry_id') or (group.subentry_id if group else None)
        group = entries.get(self._price_group_id)
        if group is None or group.subentry_type != PLANNER_KIND_PRICE_WINDOW:
            return None
        # Journaled sources are no longer authoritative after snapshotting.
        if price_window_source_retired(self._get_entry(), group.subentry_id):
            return None
        return group

    def _save_price_member(self, payload, identity=None):
        group = self._price_group()
        if group is None:
            if self._price_group_id is not None or any(
                    s.subentry_type == PLANNER_KIND_PRICE_WINDOW
                    for s in self._get_entry().subentries.values()):
                return self.async_abort(reason='legacy_unavailable')
            return self.async_create_entry(title=payload[CONF_PLANNER_NAME], data=payload)
        # Re-read current data at submission; concurrent flows must not erase peers.
        members = price_window_members(group)
        identity = identity or payload.get('_legacy_entry_id') or uuid4().hex
        if getattr(self, '_member_id', None) and identity not in members:
            return self.async_abort(reason='legacy_unavailable')
        if identity in members and not getattr(self, '_member_id', None):
            return self.async_abort(reason='legacy_unavailable')
        members[identity] = {**payload, '_legacy_entry_id': identity}
        self.hass.config_entries.async_update_subentry(self._get_entry(), group,
            title='Price Windows', data={**group.data, PRICE_WINDOW_MEMBERS: members})
        return self.async_abort(reason='reconfigure_successful', translation_domain='homeassistant')

    async def async_step_members(self, user_input=None):
        group = self._price_group()
        if group is None:
            return self.async_abort(reason='legacy_unavailable')
        members = price_window_members(group)
        if not members:
            return await self.async_step_configure()
        if user_input is not None:
            if user_input.get('action') == 'add':
                self._member_id = None
                return await self.async_step_user()
            identity = user_input.get('member_id')
            if identity not in members:
                return self.async_abort(reason='legacy_unavailable')
            self._member_id = identity
            if user_input.get('action') == 'remove':
                return await self.async_step_remove_member()
            return self.async_show_form(step_id='configure', data_schema=self._child_schema(members[identity]))
        return self.async_show_form(step_id='members', data_schema=vol.Schema({
            vol.Required('member_id', default=next(iter(members))): selector.SelectSelector(
                selector.SelectSelectorConfig(options=[selector.SelectOptionDict(
                    value=identity, label=data.get(CONF_PLANNER_NAME, identity))
                    for identity, data in members.items()])),
            vol.Required('action', default='edit'): selector.SelectSelector(
                selector.SelectSelectorConfig(options=['edit', 'add', 'remove'],
                    translation_key='price_window_action'))}))

    async def async_step_remove_member(self, user_input=None):
        group = self._price_group()
        if group is None or self._member_id not in price_window_members(group):
            return self.async_abort(reason='legacy_unavailable')
        if user_input is not None and user_input.get('confirm'):
            members = price_window_members(group)
            del members[self._member_id]
            self.hass.config_entries.async_update_subentry(self._get_entry(), group,
                title='Price Windows', data={**group.data, PRICE_WINDOW_MEMBERS: members,
                    '_removed_price_window_planners': list(dict.fromkeys([
                        *group.data.get('_removed_price_window_planners', []), self._member_id]))})
            return self.async_abort(reason='reconfigure_successful', translation_domain='homeassistant')
        return self.async_show_form(step_id='remove_member', data_schema=vol.Schema({
            vol.Required('confirm', default=False): selector.BooleanSelector()}))

    async def async_step_reconfigure(self, user_input=None):
        if self._subentry_type == PLANNER_KIND_PRICE_WINDOW:
            if self._price_group() is None:
                return self.async_abort(reason='legacy_unavailable')
            return await self.async_step_members(user_input)
        self._editing = self._get_reconfigure_subentry()
        if user_input is None:
            return self.async_show_form(step_id='configure', data_schema=self._child_schema(self._editing.data))
        return await self.async_step_configure(user_input)

    async def async_step_configure(self, user_input=None):
        if self._subentry_type == PLANNER_KIND_PRICE_WINDOW:
            self._price_group()
        errors = {}
        if user_input is not None:
            data = _normalize_selector_values(user_input)
            # Never let stale child values override the shared household sources.
            data = {**child_data(data), **{k: v for k, v in merged_data(self._get_entry()).items()
                                         if k in SHARED_SOURCE_KEYS}, CONF_PLANNER_KIND: self._subentry_type}
            if self._subentry_type == PLANNER_KIND_DEMAND and not getattr(self, '_demand_devices_complete', False):
                return await self._start_demand_devices(data, 'configure')
            if self._subentry_type == PLANNER_KIND_BATTERY:
                if not (data.get(CONF_TOTAL_ENERGY_SENSOR) or data.get(CONF_DEMAND_FORECAST_SENSOR)):
                    errors['base'] = 'demand_source_required'
                if not data.get(CONF_SOLCAST_TODAY_SENSOR):
                    errors['base'] = 'solar_source_required'
                forecast = data.get(CONF_DEMAND_FORECAST_SENSOR)
                if forecast:
                    from .household import is_self_forecast
                    editing = getattr(self, '_editing', None)
                    identity = editing.data.get('_legacy_entry_id', editing.subentry_id) if editing else None
                    if is_self_forecast(self.hass, identity, forecast):
                        errors['base'] = 'self_forecast'
            if not errors:
                title = str(data.get(CONF_PLANNER_NAME) or _title_for_kind(self._subentry_type))
                payload = child_data(data)
                if self._subentry_type == PLANNER_KIND_PRICE_WINDOW:
                    return self._save_price_member(payload, getattr(self, '_member_id', None))
                editing = getattr(self, '_editing', None)
                if editing:
                    # Persist adoption identity through every edit and HA restart.
                    if '_legacy_entry_id' in editing.data:
                        payload['_legacy_entry_id'] = editing.data['_legacy_entry_id']
                    return self.async_update_and_abort(self._get_entry(), editing, title=title, data=payload)
                return self.async_create_entry(title=title, data=payload)
        return self.async_show_form(step_id='configure', data_schema=self._child_schema(user_input), errors=errors)


class SmartEnergyPlannerConfigFlow(DemandDeviceFlow, ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Smart Energy Planner."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Create the household; planners are real HA config subentries."""
        if user_input is not None:
            if user_input.get(CONF_PLANNER_KIND, 'household') != 'household':
                return self.async_abort(reason="unsupported_planner_kind")
            data = {**_normalize_selector_values(user_input), CONF_PLANNER_KIND: PLANNER_KIND_HOUSEHOLD}
            return self.async_create_entry(title=data[CONF_PLANNER_NAME], data=data)

        return self.async_show_form(step_id="user", data_schema=_build_household_schema(self.hass))

    @classmethod
    def async_get_supported_subentry_types(cls, config_entry):
        if merged_data(config_entry).get(CONF_PLANNER_KIND) != PLANNER_KIND_HOUSEHOLD:
            return {}
        return {kind: PlannerSubentryFlow for kind in (
            PLANNER_KIND_BATTERY, PLANNER_KIND_PRICE_WINDOW, PLANNER_KIND_DEMAND,
        )}

    async def async_step_configure(self, user_input=None):
        """Abort stale pre-household flows without creating a duplicate planner."""
        return self.async_abort(reason="unsupported_planner_kind")

    @staticmethod
    def async_get_options_flow(config_entry):
        """Return the options flow."""
        return SmartEnergyPlannerOptionsFlow()


class SmartEnergyPlannerOptionsFlow(DemandDeviceFlow, OptionsFlow):
    """Handle Smart Energy Planner options."""

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Manage the options."""
        merged = {**self.config_entry.data, **self.config_entry.options}
        planner_kind = merged.get(CONF_PLANNER_KIND, DEFAULT_PLANNER_KIND)
        if planner_kind == PLANNER_KIND_HOUSEHOLD:
            if user_input is not None:
                data = {**{key: None for key in SHARED_SOURCE_KEYS},
                        CONF_EXPORT_PRICE_TAX_DEDUCTION: DEFAULT_EXPORT_PRICE_TAX_DEDUCTION,
                        **_normalize_selector_values(user_input), CONF_PLANNER_KIND: PLANNER_KIND_HOUSEHOLD}
                self.hass.config_entries.async_update_entry(self.config_entry, title=data[CONF_PLANNER_NAME])
                return self.async_create_entry(title='', data=data)
            return self.async_show_form(step_id='init', data_schema=_build_household_schema(self.hass, merged))
        if planner_kind not in (PLANNER_KIND_BATTERY, PLANNER_KIND_PRICE_WINDOW, PLANNER_KIND_DEMAND):
            return self.async_abort(reason="unsupported_planner_kind")

        if user_input is not None:
            user_input = _normalize_selector_values(user_input)
            options_data = _merge_options_data(
                merged,
                user_input,
                planner_kind=planner_kind,
            )
            if planner_kind == PLANNER_KIND_DEMAND and not getattr(self, '_demand_devices_complete', False):
                return await self._start_demand_devices(options_data, 'init')
            if planner_kind == PLANNER_KIND_BATTERY and not (options_data.get(CONF_TOTAL_ENERGY_SENSOR) or options_data.get(CONF_DEMAND_FORECAST_SENSOR)):
                return self.async_show_form(step_id='init', data_schema=_build_battery_schema(self.hass, options_data), errors={'base': 'demand_source_required'})
            title = str(options_data.get(CONF_PLANNER_NAME) or self.config_entry.title)
            self.hass.config_entries.async_update_entry(self.config_entry, title=title)
            return self.async_create_entry(title="", data=options_data)

        return self.async_show_form(
            step_id="init",
            data_schema=_schema_for_kind(self.hass, planner_kind, merged),
        )
