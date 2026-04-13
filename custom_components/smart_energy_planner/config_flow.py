"""Config flow for Smart Energy Planner."""

from __future__ import annotations

from typing import Any

import voluptuous as vol

from homeassistant.config_entries import ConfigFlow, ConfigFlowResult, OptionsFlow
from homeassistant.helpers import selector

from .const import (
    CONF_BATTERY_CAPACITY_KWH,
    CONF_BATTERY_ENABLED,
    CONF_BATTERY_MAX_CHARGE_KW,
    CONF_BATTERY_MAX_DISCHARGE_KW,
    CONF_HEATING_ENERGY_SENSOR,
    CONF_HEATING_LOOKBACK_DAYS,
    CONF_PRICE_SENSOR,
    CONF_SOLCAST_TODAY_SENSOR,
    CONF_TEMPERATURE_SENSOR,
    DEFAULT_BATTERY_CAPACITY_KWH,
    DEFAULT_BATTERY_ENABLED,
    DEFAULT_BATTERY_MAX_CHARGE_KW,
    DEFAULT_BATTERY_MAX_DISCHARGE_KW,
    DEFAULT_HEATING_LOOKBACK_DAYS,
    DEFAULT_NAME,
    DOMAIN,
)


def _build_schema(user_input: dict[str, Any] | None = None) -> vol.Schema:
    """Build the main config schema."""
    user_input = user_input or {}
    battery_enabled = user_input.get(CONF_BATTERY_ENABLED, DEFAULT_BATTERY_ENABLED)

    schema: dict[vol.Marker, Any] = {
        vol.Required(CONF_PRICE_SENSOR, default=user_input.get(CONF_PRICE_SENSOR)): selector.EntitySelector(
            selector.EntitySelectorConfig(domain="sensor")
        ),
        vol.Required(
            CONF_SOLCAST_TODAY_SENSOR,
            default=user_input.get(CONF_SOLCAST_TODAY_SENSOR),
        ): selector.EntitySelector(selector.EntitySelectorConfig(domain="sensor")),
        vol.Required(
            CONF_TEMPERATURE_SENSOR,
            default=user_input.get(CONF_TEMPERATURE_SENSOR),
        ): selector.EntitySelector(selector.EntitySelectorConfig(domain="sensor")),
        vol.Required(
            CONF_HEATING_ENERGY_SENSOR,
            default=user_input.get(CONF_HEATING_ENERGY_SENSOR),
        ): selector.EntitySelector(selector.EntitySelectorConfig(domain="sensor")),
        vol.Required(
            CONF_HEATING_LOOKBACK_DAYS,
            default=user_input.get(CONF_HEATING_LOOKBACK_DAYS, DEFAULT_HEATING_LOOKBACK_DAYS),
        ): selector.NumberSelector(
            selector.NumberSelectorConfig(min=2, max=14, step=1, mode=selector.NumberSelectorMode.BOX)
        ),
        vol.Required(
            CONF_BATTERY_ENABLED,
            default=battery_enabled,
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
    return vol.Schema(schema)


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

            return self.async_create_entry(
                title=DEFAULT_NAME,
                data=user_input,
            )

        return self.async_show_form(
            step_id="user",
            data_schema=_build_schema(),
        )

    @staticmethod
    def async_get_options_flow(config_entry):
        """Return the options flow."""
        return SmartEnergyPlannerOptionsFlow(config_entry)


class SmartEnergyPlannerOptionsFlow(OptionsFlow):
    """Handle Smart Energy Planner options."""

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Manage the options."""
        if user_input is not None:
            return self.async_create_entry(title="", data=user_input)

        merged = {**self.config_entry.data, **self.config_entry.options}
        return self.async_show_form(
            step_id="init",
            data_schema=_build_schema(merged),
        )
