"""Config flow for Smart Energy Planner."""

from __future__ import annotations

from typing import Any

import voluptuous as vol

from homeassistant.components.sensor import SensorDeviceClass
from homeassistant.config_entries import ConfigFlow, ConfigFlowResult, OptionsFlow
from homeassistant.const import UnitOfEnergy
from homeassistant.core import HomeAssistant
from homeassistant.helpers import selector

from .const import (
    CONF_PRICE_PEAK_MIN_GAP_HOURS,
    DEFAULT_PRICE_PEAK_MIN_GAP_HOURS,
    CONF_BATTERY_CAPACITY_KWH,
    CONF_BATTERY_CHARGE_SAFETY_MARGIN,
    CONF_BATTERY_DEMAND_SAFETY_MARGIN,
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
                        selector.SelectOptionDict(value=PLANNER_KIND_PRICE_WINDOW, label="Price window planner"),
                    ],
                    mode=selector.SelectSelectorMode.DROPDOWN,
                )
            )
        }
    )


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
    raise ValueError(f"Unsupported planner kind: {planner_kind}")


def _title_for_kind(planner_kind: str) -> str:
    if planner_kind == PLANNER_KIND_BATTERY:
        return f"{DEFAULT_NAME} Battery"
    if planner_kind == PLANNER_KIND_PRICE_WINDOW:
        return f"{DEFAULT_NAME} Price Windows"
    return f"{DEFAULT_NAME} Battery"


class SmartEnergyPlannerConfigFlow(ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Smart Energy Planner."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Handle the initial step."""
        if user_input is not None:
            if user_input.get(CONF_PLANNER_KIND) not in (PLANNER_KIND_BATTERY, PLANNER_KIND_PRICE_WINDOW):
                return self.async_abort(reason="unsupported_planner_kind")
            self.context["planner_kind"] = user_input[CONF_PLANNER_KIND]
            return await self.async_step_configure()

        return self.async_show_form(step_id="user", data_schema=_build_kind_schema())

    async def async_step_configure(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Configure the selected planner kind."""
        planner_kind = self.context.get("planner_kind", DEFAULT_PLANNER_KIND)
        if planner_kind not in (PLANNER_KIND_BATTERY, PLANNER_KIND_PRICE_WINDOW):
            return self.async_abort(reason="unsupported_planner_kind")

        if user_input is not None:
            user_input = _normalize_selector_values(user_input)
            data = {CONF_PLANNER_KIND: planner_kind, **user_input}
            title = str(data.get(CONF_PLANNER_NAME) or _title_for_kind(planner_kind))
            unique_anchor = (
                user_input.get(CONF_BATTERY_SOC_SENSOR)
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
        if planner_kind not in (PLANNER_KIND_BATTERY, PLANNER_KIND_PRICE_WINDOW):
            return self.async_abort(reason="unsupported_planner_kind")

        if user_input is not None:
            user_input = _normalize_selector_values(user_input)
            options_data = _merge_options_data(
                merged,
                user_input,
                planner_kind=planner_kind,
            )
            title = str(options_data.get(CONF_PLANNER_NAME) or self.config_entry.title)
            self.hass.config_entries.async_update_entry(self.config_entry, title=title)
            return self.async_create_entry(title="", data=options_data)

        return self.async_show_form(
            step_id="init",
            data_schema=_schema_for_kind(self.hass, planner_kind, merged),
        )
