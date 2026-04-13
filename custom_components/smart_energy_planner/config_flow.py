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
    CONF_HEATING_ENERGY_SENSOR,
    CONF_HEATING_LOOKBACK_DAYS,
    CONF_PRICE_SENSOR,
    CONF_PRICE_RESOLUTION,
    CONF_SOLCAST_TODAY_SENSOR,
    CONF_TEMPERATURE_SENSOR,
    CONF_TOTAL_ENERGY_SENSOR,
    DEFAULT_BATTERY_CAPACITY_KWH,
    DEFAULT_BATTERY_ENABLED,
    DEFAULT_BATTERY_MAX_CHARGE_KW,
    DEFAULT_BATTERY_MAX_DISCHARGE_KW,
    DEFAULT_HEATING_LOOKBACK_DAYS,
    DEFAULT_NAME,
    DEFAULT_PRICE_RESOLUTION,
    DOMAIN,
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
    """Return Solcast forecast sensors that include today's estimate and hourly detail."""
    return [
        state.entity_id
        for state in _sensor_options(hass)
        if "estimate" in state.attributes and state.attributes.get("detailedHourly")
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


def _entity_selector(
    include_entities: list[str],
    *,
    current_value: str | None,
) -> selector.EntitySelector:
    """Build an entity selector and keep the current entity selectable."""
    include_entities = list(dict.fromkeys(include_entities))
    if current_value and current_value not in include_entities:
        include_entities = [*include_entities, current_value]

    config: dict[str, Any] = {"domain": "sensor"}
    if include_entities:
        config["include_entities"] = include_entities

    return selector.EntitySelector(selector.EntitySelectorConfig(**config))


def _build_schema(hass: HomeAssistant, user_input: dict[str, Any] | None = None) -> vol.Schema:
    """Build the main config schema."""
    user_input = user_input or {}
    return vol.Schema(
        {
            vol.Required(
                CONF_PRICE_SENSOR, default=user_input.get(CONF_PRICE_SENSOR)
            ): _entity_selector(_filter_price_sensors(hass), current_value=user_input.get(CONF_PRICE_SENSOR)),
            vol.Required(
                CONF_SOLCAST_TODAY_SENSOR, default=user_input.get(CONF_SOLCAST_TODAY_SENSOR)
            ): _entity_selector(
                _filter_solcast_sensors(hass), current_value=user_input.get(CONF_SOLCAST_TODAY_SENSOR)
            ),
            vol.Required(
                CONF_TEMPERATURE_SENSOR, default=user_input.get(CONF_TEMPERATURE_SENSOR)
            ): _entity_selector(
                _filter_temperature_sensors(hass), current_value=user_input.get(CONF_TEMPERATURE_SENSOR)
            ),
            vol.Required(
                CONF_HEATING_ENERGY_SENSOR, default=user_input.get(CONF_HEATING_ENERGY_SENSOR)
            ): _entity_selector(
                _filter_energy_sensors(hass), current_value=user_input.get(CONF_HEATING_ENERGY_SENSOR)
            ),
            vol.Required(
                CONF_TOTAL_ENERGY_SENSOR, default=user_input.get(CONF_TOTAL_ENERGY_SENSOR)
            ): _entity_selector(
                _filter_energy_sensors(hass), current_value=user_input.get(CONF_TOTAL_ENERGY_SENSOR)
            ),
            vol.Required(
                CONF_HEATING_LOOKBACK_DAYS,
                default=user_input.get(CONF_HEATING_LOOKBACK_DAYS, DEFAULT_HEATING_LOOKBACK_DAYS),
            ): selector.NumberSelector(
                selector.NumberSelectorConfig(min=2, max=14, step=1, mode=selector.NumberSelectorMode.BOX)
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
                CONF_BATTERY_ENABLED, default=user_input.get(CONF_BATTERY_ENABLED, DEFAULT_BATTERY_ENABLED)
            ): selector.BooleanSelector(),
            vol.Required(
                CONF_BATTERY_CAPACITY_KWH,
                default=user_input.get(CONF_BATTERY_CAPACITY_KWH, DEFAULT_BATTERY_CAPACITY_KWH),
            ): selector.NumberSelector(
                selector.NumberSelectorConfig(min=0, max=100, step=0.1, mode=selector.NumberSelectorMode.BOX)
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


class SmartEnergyPlannerConfigFlow(ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Smart Energy Planner."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Handle the initial step."""
        if user_input is not None:
            await self.async_set_unique_id(f"{DOMAIN}-{user_input[CONF_PRICE_SENSOR]}")
            self._abort_if_unique_id_configured()
            return self.async_create_entry(title=DEFAULT_NAME, data=user_input)

        return self.async_show_form(step_id="user", data_schema=_build_schema(self.hass))

    @staticmethod
    def async_get_options_flow(config_entry):
        """Return the options flow."""
        return SmartEnergyPlannerOptionsFlow(config_entry)


class SmartEnergyPlannerOptionsFlow(OptionsFlow):
    """Handle Smart Energy Planner options."""

    def __init__(self, config_entry) -> None:
        """Initialize the options flow."""
        self.config_entry = config_entry

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Manage the options."""
        if user_input is not None:
            return self.async_create_entry(title="", data=user_input)

        merged = {**self.config_entry.data, **self.config_entry.options}
        return self.async_show_form(step_id="init", data_schema=_build_schema(self.hass, merged))
