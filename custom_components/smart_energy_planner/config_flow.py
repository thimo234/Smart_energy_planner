"""Config flow for Smart Energy Planner."""

from __future__ import annotations

from typing import Any

import voluptuous as vol

from homeassistant.components.sensor import SensorDeviceClass
from homeassistant.config_entries import ConfigFlow, ConfigFlowResult, OptionsFlow
from homeassistant.const import UnitOfEnergy, UnitOfTemperature
from homeassistant.core import HomeAssistant
from homeassistant.helpers import selector

from .const import (
    CONF_BATTERY_CAPACITY_KWH,
    CONF_BATTERY_ENABLED,
    CONF_BATTERY_MAX_CHARGE_KW,
    CONF_BATTERY_MAX_DISCHARGE_KW,
    CONF_BATTERY_MIN_SOC_PERCENT,
    CONF_BATTERY_MIN_PROFIT_PER_KWH,
    CONF_BATTERY_SOC_SENSOR,
    CONF_COOLING_MODE_SWITCH_ENTITY,
    CONF_HEATING_SWITCH_ENTITY,
    CONF_HEATING_LOOKBACK_DAYS,
    CONF_PLANNER_NAME,
    CONF_PLANNER_KIND,
    CONF_PRICE_SENSOR,
    CONF_PRICE_RESOLUTION,
    CONF_ROOM_TEMPERATURE_SENSOR,
    CONF_SOLCAST_TODAY_SENSOR,
    CONF_SOLCAST_TOMORROW_SENSOR,
    CONF_TEMPERATURE_SENSOR,
    CONF_THERMOSTAT_CONTROL_CHECK_MINUTES,
    CONF_THERMOSTAT_COLD_TOLERANCE,
    CONF_THERMOSTAT_ECO_TEMPERATURE,
    CONF_THERMOSTAT_HOT_TOLERANCE,
    CONF_THERMOSTAT_MAX_TEMP,
    CONF_THERMOSTAT_MIN_CYCLE_MINUTES,
    CONF_THERMOSTAT_MIN_TEMP,
    CONF_THERMOSTAT_PREHEAT_MINUTES,
    CONF_TOTAL_ENERGY_SENSOR,
    DEFAULT_BATTERY_CAPACITY_KWH,
    DEFAULT_BATTERY_ENABLED,
    DEFAULT_BATTERY_MAX_CHARGE_KW,
    DEFAULT_BATTERY_MAX_DISCHARGE_KW,
    DEFAULT_BATTERY_MIN_SOC_PERCENT,
    DEFAULT_BATTERY_MIN_PROFIT_PER_KWH,
    DEFAULT_HEATING_LOOKBACK_DAYS,
    DEFAULT_NAME,
    DEFAULT_PLANNER_KIND,
    DEFAULT_PRICE_RESOLUTION,
    DEFAULT_THERMOSTAT_CONTROL_CHECK_MINUTES,
    DEFAULT_THERMOSTAT_COLD_TOLERANCE,
    DEFAULT_THERMOSTAT_ECO_TEMPERATURE,
    DEFAULT_THERMOSTAT_HOT_TOLERANCE,
    DEFAULT_THERMOSTAT_MAX_TEMP,
    DEFAULT_THERMOSTAT_MIN_CYCLE_MINUTES,
    DEFAULT_THERMOSTAT_MIN_TEMP,
    DEFAULT_THERMOSTAT_PREHEAT_MINUTES,
    DOMAIN,
    PLANNER_KIND_BATTERY,
    PLANNER_KIND_THERMOSTAT,
    PRICE_RESOLUTION_HOURLY,
    PRICE_RESOLUTION_QUARTER_HOURLY,
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


def _filter_temperature_sensors(hass: HomeAssistant) -> list[str]:
    """Return likely temperature sensors."""
    return [
        state.entity_id
        for state in _sensor_options(hass)
        if state.attributes.get("device_class")
        in (SensorDeviceClass.TEMPERATURE, SensorDeviceClass.TEMPERATURE.value)
        or state.attributes.get("unit_of_measurement")
        in (UnitOfTemperature.CELSIUS, UnitOfTemperature.FAHRENHEIT)
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

def _filter_heating_switch_entities(hass: HomeAssistant) -> list[str]:
    """Return likely heating control switches."""
    switch_entities = [state.entity_id for state in hass.states.async_all("switch")]
    boolean_entities = [state.entity_id for state in hass.states.async_all("input_boolean")]
    return [*switch_entities, *boolean_entities]


def _build_kind_schema(current_value: str | None = None) -> vol.Schema:
    """Build the planner type selector schema."""
    return vol.Schema(
        {
            vol.Required(
                CONF_PLANNER_KIND,
                default=current_value or DEFAULT_PLANNER_KIND,
            ): selector.SelectSelector(
                selector.SelectSelectorConfig(
                    options=[
                        selector.SelectOptionDict(value=PLANNER_KIND_BATTERY, label="Battery planner"),
                        selector.SelectOptionDict(value=PLANNER_KIND_THERMOSTAT, label="Thermostat planner"),
                    ],
                    mode=selector.SelectSelectorMode.DROPDOWN,
                )
            )
        }
    )


def _base_defaults(user_input: dict[str, Any] | None) -> dict[str, Any]:
    return user_input or {}


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
            vol.Required(
                CONF_TOTAL_ENERGY_SENSOR, default=user_input.get(CONF_TOTAL_ENERGY_SENSOR)
            ): _entity_selector(
                _filter_energy_sensors(hass), current_value=user_input.get(CONF_TOTAL_ENERGY_SENSOR)
            ),
            vol.Required(
                CONF_BATTERY_SOC_SENSOR, default=user_input.get(CONF_BATTERY_SOC_SENSOR)
            ): _entity_selector(
                _filter_battery_soc_sensors(hass), current_value=user_input.get(CONF_BATTERY_SOC_SENSOR)
            ),
            vol.Required(
                CONF_HEATING_LOOKBACK_DAYS,
                default=user_input.get(CONF_HEATING_LOOKBACK_DAYS, DEFAULT_HEATING_LOOKBACK_DAYS),
            ): selector.NumberSelector(
                selector.NumberSelectorConfig(min=2, max=30, step=1, mode=selector.NumberSelectorMode.BOX)
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
        }
    )


def _build_thermostat_schema(hass: HomeAssistant, user_input: dict[str, Any] | None = None) -> vol.Schema:
    """Build the thermostat planner schema."""
    user_input = _base_defaults(user_input)
    return vol.Schema(
        {
            vol.Required(
                CONF_PLANNER_NAME,
                default=user_input.get(CONF_PLANNER_NAME, f"{DEFAULT_NAME} Thermostat"),
            ): selector.TextSelector(),
            vol.Required(
                CONF_PRICE_SENSOR, default=user_input.get(CONF_PRICE_SENSOR)
            ): _entity_selector(_filter_price_sensors(hass), current_value=user_input.get(CONF_PRICE_SENSOR)),
            vol.Optional(
                CONF_TEMPERATURE_SENSOR, default=user_input.get(CONF_TEMPERATURE_SENSOR)
            ): _entity_selector(
                _filter_temperature_sensors(hass), current_value=user_input.get(CONF_TEMPERATURE_SENSOR)
            ),
            vol.Required(
                CONF_ROOM_TEMPERATURE_SENSOR, default=user_input.get(CONF_ROOM_TEMPERATURE_SENSOR)
            ): _entity_selector(
                _filter_temperature_sensors(hass), current_value=user_input.get(CONF_ROOM_TEMPERATURE_SENSOR)
            ),
            vol.Required(
                CONF_HEATING_SWITCH_ENTITY, default=user_input.get(CONF_HEATING_SWITCH_ENTITY)
            ): _entity_selector(
                _filter_heating_switch_entities(hass),
                current_value=user_input.get(CONF_HEATING_SWITCH_ENTITY),
                domain=None,
            ),
            vol.Optional(
                CONF_COOLING_MODE_SWITCH_ENTITY,
                default=user_input.get(CONF_COOLING_MODE_SWITCH_ENTITY),
            ): _entity_selector(
                _filter_heating_switch_entities(hass),
                current_value=user_input.get(CONF_COOLING_MODE_SWITCH_ENTITY),
                domain=None,
            ),
            vol.Required(
                CONF_THERMOSTAT_ECO_TEMPERATURE,
                default=user_input.get(
                    CONF_THERMOSTAT_ECO_TEMPERATURE, DEFAULT_THERMOSTAT_ECO_TEMPERATURE
                ),
            ): selector.NumberSelector(
                selector.NumberSelectorConfig(min=5, max=30, step=0.5, mode=selector.NumberSelectorMode.BOX)
            ),
            vol.Required(
                CONF_THERMOSTAT_COLD_TOLERANCE,
                default=user_input.get(CONF_THERMOSTAT_COLD_TOLERANCE, DEFAULT_THERMOSTAT_COLD_TOLERANCE),
            ): selector.NumberSelector(
                selector.NumberSelectorConfig(min=0.1, max=3, step=0.1, mode=selector.NumberSelectorMode.BOX)
            ),
            vol.Required(
                CONF_THERMOSTAT_HOT_TOLERANCE,
                default=user_input.get(CONF_THERMOSTAT_HOT_TOLERANCE, DEFAULT_THERMOSTAT_HOT_TOLERANCE),
            ): selector.NumberSelector(
                selector.NumberSelectorConfig(min=0.1, max=3, step=0.1, mode=selector.NumberSelectorMode.BOX)
            ),
            vol.Required(
                CONF_THERMOSTAT_MIN_TEMP,
                default=user_input.get(CONF_THERMOSTAT_MIN_TEMP, DEFAULT_THERMOSTAT_MIN_TEMP),
            ): selector.NumberSelector(
                selector.NumberSelectorConfig(min=5, max=30, step=0.5, mode=selector.NumberSelectorMode.BOX)
            ),
            vol.Required(
                CONF_THERMOSTAT_MAX_TEMP,
                default=user_input.get(CONF_THERMOSTAT_MAX_TEMP, DEFAULT_THERMOSTAT_MAX_TEMP),
            ): selector.NumberSelector(
                selector.NumberSelectorConfig(min=5, max=35, step=0.5, mode=selector.NumberSelectorMode.BOX)
            ),
            vol.Required(
                CONF_THERMOSTAT_MIN_CYCLE_MINUTES,
                default=user_input.get(
                    CONF_THERMOSTAT_MIN_CYCLE_MINUTES, DEFAULT_THERMOSTAT_MIN_CYCLE_MINUTES
                ),
            ): selector.NumberSelector(
                selector.NumberSelectorConfig(min=1, max=60, step=1, mode=selector.NumberSelectorMode.BOX)
            ),
            vol.Required(
                CONF_THERMOSTAT_CONTROL_CHECK_MINUTES,
                default=user_input.get(
                    CONF_THERMOSTAT_CONTROL_CHECK_MINUTES, DEFAULT_THERMOSTAT_CONTROL_CHECK_MINUTES
                ),
            ): selector.NumberSelector(
                selector.NumberSelectorConfig(min=1, max=60, step=1, mode=selector.NumberSelectorMode.BOX)
            ),
            vol.Required(
                CONF_THERMOSTAT_PREHEAT_MINUTES,
                default=user_input.get(
                    CONF_THERMOSTAT_PREHEAT_MINUTES, DEFAULT_THERMOSTAT_PREHEAT_MINUTES
                ),
            ): selector.NumberSelector(
                selector.NumberSelectorConfig(min=0, max=180, step=5, mode=selector.NumberSelectorMode.BOX)
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
        }
    )


def _schema_for_kind(
    hass: HomeAssistant,
    planner_kind: str,
    user_input: dict[str, Any] | None = None,
) -> vol.Schema:
    if planner_kind == PLANNER_KIND_BATTERY:
        return _build_battery_schema(hass, user_input)
    return _build_thermostat_schema(hass, user_input)


def _title_for_kind(planner_kind: str) -> str:
    if planner_kind == PLANNER_KIND_BATTERY:
        return f"{DEFAULT_NAME} Battery"
    if planner_kind == PLANNER_KIND_THERMOSTAT:
        return f"{DEFAULT_NAME} Thermostat"
    return f"{DEFAULT_NAME} Battery"


class SmartEnergyPlannerConfigFlow(ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Smart Energy Planner."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Handle the initial step."""
        if user_input is not None:
            self.context["planner_kind"] = user_input[CONF_PLANNER_KIND]
            return await self.async_step_configure()

        return self.async_show_form(step_id="user", data_schema=_build_kind_schema())

    async def async_step_configure(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Configure the selected planner kind."""
        planner_kind = self.context.get("planner_kind", DEFAULT_PLANNER_KIND)

        if user_input is not None:
            data = {CONF_PLANNER_KIND: planner_kind, **user_input}
            title = str(data.get(CONF_PLANNER_NAME) or _title_for_kind(planner_kind))
            unique_anchor = (
                user_input.get(CONF_HEATING_SWITCH_ENTITY)
                or user_input.get(CONF_ROOM_TEMPERATURE_SENSOR)
                or user_input.get(CONF_BATTERY_SOC_SENSOR)
                or user_input.get(CONF_TOTAL_ENERGY_SENSOR)
                or user_input.get(CONF_PLANNER_NAME)
                or user_input.get(CONF_PRICE_SENSOR)
                or user_input.get(CONF_TOTAL_ENERGY_SENSOR)
                or planner_kind
            )
            await self.async_set_unique_id(f"{DOMAIN}-{planner_kind}-{unique_anchor}")
            self._abort_if_unique_id_configured()
            return self.async_create_entry(title=title, data=data)

        return self.async_show_form(
            step_id="configure",
            data_schema=_schema_for_kind(self.hass, planner_kind),
        )

    @staticmethod
    def async_get_options_flow(config_entry):
        """Return the options flow."""
        return SmartEnergyPlannerOptionsFlow()


class SmartEnergyPlannerOptionsFlow(OptionsFlow):
    """Handle Smart Energy Planner options."""

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Manage the options."""
        merged = {**self.config_entry.data, **self.config_entry.options}
        planner_kind = merged.get(CONF_PLANNER_KIND, DEFAULT_PLANNER_KIND)

        if user_input is not None:
            title = str(user_input.get(CONF_PLANNER_NAME) or self.config_entry.title)
            self.hass.config_entries.async_update_entry(self.config_entry, title=title)
            return self.async_create_entry(title="", data={CONF_PLANNER_KIND: planner_kind, **user_input})

        return self.async_show_form(
            step_id="init",
            data_schema=_schema_for_kind(self.hass, planner_kind, merged),
        )
