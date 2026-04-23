"""Coordinator for Smart Energy Planner."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import logging
import statistics
from typing import Any, cast

from homeassistant.components.recorder import history
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import STATE_OFF, STATE_ON, STATE_UNAVAILABLE, STATE_UNKNOWN
from homeassistant.core import HomeAssistant
from homeassistant.helpers.storage import Store
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator
from homeassistant.util import dt as dt_util

from .const import (
    CONF_BATTERY_CAPACITY_KWH,
    CONF_BATTERY_ENABLED,
    CONF_BATTERY_MAX_CHARGE_KW,
    CONF_BATTERY_MAX_DISCHARGE_KW,
    CONF_BATTERY_MIN_SOC_PERCENT,
    CONF_BATTERY_MIN_PROFIT_PER_KWH,
    CONF_BATTERY_SOC_SENSOR,
    CONF_COOLING_MODE_SWITCH_ENTITY,
    CONF_EXPORT_PRICE_SENSOR,
    CONF_HEATING_SWITCH_ENTITY,
    CONF_PLANNER_KIND,
    CONF_PRICE_SENSOR,
    CONF_PRICE_RESOLUTION,
    CONF_ROOM_TEMPERATURE_SENSOR,
    CONF_SOLCAST_TODAY_SENSOR,
    CONF_SOLCAST_TOMORROW_SENSOR,
    CONF_TEMPERATURE_SENSOR,
    CONF_THERMOSTAT_ECO_TEMPERATURE,
    CONF_THERMOSTAT_MAX_TEMP,
    CONF_THERMOSTAT_MIN_TEMP,
    CONF_THERMOSTAT_PREHEAT_MINUTES,
    CONF_TOTAL_ENERGY_SENSOR,
    COORDINATOR_UPDATE_INTERVAL,
    DEFAULT_BATTERY_CAPACITY_KWH,
    DEFAULT_BATTERY_ENABLED,
    DEFAULT_BATTERY_MAX_CHARGE_KW,
    DEFAULT_BATTERY_MAX_DISCHARGE_KW,
    DEFAULT_BATTERY_MIN_SOC_PERCENT,
    DEFAULT_BATTERY_MIN_PROFIT_PER_KWH,
    DEFAULT_THERMOSTAT_ECO_TEMPERATURE,
    DEFAULT_THERMOSTAT_MAX_TEMP,
    DEFAULT_THERMOSTAT_MIN_TEMP,
    DEFAULT_THERMOSTAT_PREHEAT_MINUTES,
    DOMAIN,
    PLANNER_KIND_BATTERY,
    PLANNER_KIND_THERMOSTAT,
    PRICE_RESOLUTION_HOURLY,
    RUNTIME_STATE,
    STORAGE_KEY,
    STORAGE_VERSION,
)

_LOGGER = logging.getLogger(__name__)
_HISTORY_LOOKBACK_DAYS = 7
_CLEAR_PRICE_PEAK_MIN_DELTA = 0.02
_THERMOSTAT_ECO_MERGE_GAP = timedelta(hours=1)
_THERMOSTAT_FALLBACK_COOLING_FACTOR = 0.04
_THERMOSTAT_MIN_FALLBACK_COOLDOWN_HOURS = 2.0
_THERMOSTAT_MAX_COOLDOWN_HOURS = 18.0
# Number of completed eco sessions after which the learned cooling factor is
# fully trusted. Below this number the planner blends the learned rate with a
# conservative fallback.
_THERMOSTAT_COOLING_LEARN_SAMPLES = 3
# Minimum absolute change (kWh) in the tracked battery energy before the profit
# tracker updates the cost basis. Changes below this are treated as sensor
# noise, but the baseline is still persisted so we don't lose precision across
# Home Assistant restarts.
_BATTERY_PROFIT_NOISE_FLOOR_KWH = 0.01


@dataclass(slots=True)
class PlannerWindow:
    start: datetime
    end: datetime
    price: float


@dataclass(slots=True)
class SolarWindow:
    start: datetime
    end: datetime
    forecast_kwh: float
    forecast_kwh_p10: float | None
    forecast_kwh_p90: float | None


@dataclass(slots=True)
class PlannerResult:
    planner_kind: str
    status: str
    score: int
    recommendation: str
    battery_strategy: str
    heat_pump_strategy: str
    heating_estimate_kwh: float
    solar_forecast_kwh: float
    current_price: float | None
    price_spread: float
    next_window_start: str | None
    next_window_end: str | None
    next_window_price: float | None
    best_solar_window_start: str | None
    best_solar_window_end: str | None
    best_solar_window_kwh: float | None
    solcast_confidence: float | None
    lookback_daily_average_kwh: float
    total_energy_daily_average_kwh: float
    non_heating_daily_average_kwh: float
    estimated_total_home_demand_kwh: float
    estimated_hourly_home_demand: list[dict[str, str | float]]
    projected_remaining_solar_until_sunset_kwh: float
    projected_remaining_home_demand_until_sunset_kwh: float
    projected_solar_surplus_until_sunset_kwh: float
    grid_charge_needed_until_sunset_kwh: float
    battery_charge_hours_needed_until_sunset: float
    target_battery_full_by_sunset: bool
    planned_grid_charge_windows: list[dict[str, str | float]]
    planned_solar_charge_windows: list[dict[str, str | float]]
    planned_battery_mode_schedule: list[dict[str, str]]
    battery_soc_percent: float | None
    battery_min_soc_percent: float
    battery_total_energy_kwh: float
    battery_energy_available_kwh: float
    battery_remaining_capacity_kwh: float
    next_charge_opportunity_start: str | None
    next_charge_window_start: str | None
    next_charge_window_end: str | None
    next_charge_window_hours: float
    following_charge_window_start: str | None
    following_charge_window_end: str | None
    following_charge_window_hours: float
    next_discharge_window_start: str | None
    next_discharge_window_end: str | None
    next_discharge_window_hours: float
    next_idle_window_start: str | None
    current_relevant_battery_window_start: str | None
    current_relevant_battery_window_end: str | None
    current_relevant_battery_window_mode: str | None
    current_relevant_battery_window_expected_demand_kwh: float
    current_relevant_battery_window_expected_solar_kwh: float
    home_demand_until_next_charge_kwh: float
    battery_reserved_energy_kwh: float
    battery_energy_available_for_discharge_kwh: float
    battery_exportable_energy_kwh: float
    battery_room_needed_for_solar_kwh: float
    battery_charge_hours_needed_total: float
    battery_full_discharge_hours: float
    battery_simulated_remaining_kwh_after_discharge: float
    next_high_price_window_start: str | None
    next_high_price_window_price: float | None
    room_temperature_c: float | None
    thermostat_setpoint_c: float | None
    thermostat_cool_setpoint_c: float | None
    thermostat_preheat_setpoint_c: float | None
    thermostat_eco_setpoint_c: float | None
    room_cooling_hours_to_eco: float | None
    room_cooling_rate_c_per_hour: float | None
    cooling_reference_outdoor_temp_c: float | None
    planned_eco_window_start: str | None
    planned_eco_window_end: str | None
    planned_eco_windows: list[dict[str, str | float]]
    planned_preheat_window_start: str | None
    planned_preheat_window_end: str | None
    planned_preheat_windows: list[dict[str, str | float]]
    battery_min_profit_per_kwh: float
    price_resolution: str
    source_status: dict[str, str]
    source_errors: list[str]
    rationale: str


class SmartEnergyPlannerCoordinator(DataUpdateCoordinator[PlannerResult]):
    """Coordinate planner calculations."""

    config_entry: ConfigEntry

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
        self.config_entry = entry
        super().__init__(
            hass,
            logger=_LOGGER,
            name=DOMAIN,
            update_interval=COORDINATOR_UPDATE_INTERVAL,
        )
        self._active_charge_phase_end: datetime | None = None
        self._active_charge_phase_mode = "accu_uit"
        self._eco_early_exit_until: datetime | None = None

    @property
    def _config(self) -> dict[str, Any]:
        return {**self.config_entry.data, **self.config_entry.options}

    def _config_entity_id(self, key: str) -> str | None:
        value = self._config.get(key)
        if isinstance(value, dict):
            value = value.get("entity_id")
        return str(value) if value else None

    async def _async_update_data(self) -> PlannerResult:
        """Fetch data and calculate planner output."""
        try:
            now = dt_util.now()
            planner_kind = str(self._config.get(CONF_PLANNER_KIND, PLANNER_KIND_BATTERY))
            price_sensor = self._config_entity_id(CONF_PRICE_SENSOR) or str(self._config[CONF_PRICE_SENSOR])
            export_price_sensor = self._config_entity_id(CONF_EXPORT_PRICE_SENSOR)
            solar_sensor = self._config_entity_id(CONF_SOLCAST_TODAY_SENSOR)
            solar_tomorrow_sensor = self._config_entity_id(CONF_SOLCAST_TOMORROW_SENSOR)
            temperature_sensor = self._config_entity_id(CONF_TEMPERATURE_SENSOR)
            room_temperature_sensor = self._config_entity_id(CONF_ROOM_TEMPERATURE_SENSOR)
            heating_switch_entity = self._config_entity_id(CONF_HEATING_SWITCH_ENTITY)
            cooling_mode_switch_entity = self._config_entity_id(CONF_COOLING_MODE_SWITCH_ENTITY)
            total_energy_sensor = self._config_entity_id(CONF_TOTAL_ENERGY_SENSOR)
            battery_soc_sensor = self._config_entity_id(CONF_BATTERY_SOC_SENSOR)

            price_state = self.hass.states.get(price_sensor)
            export_price_state = self.hass.states.get(export_price_sensor) if export_price_sensor else None
            solar_state = self.hass.states.get(solar_sensor) if solar_sensor else None
            solar_tomorrow_state = self.hass.states.get(solar_tomorrow_sensor) if solar_tomorrow_sensor else None
            temperature_state = self.hass.states.get(temperature_sensor) if temperature_sensor else None
            room_temperature_state = self.hass.states.get(room_temperature_sensor) if room_temperature_sensor else None
            heating_switch_state = self.hass.states.get(heating_switch_entity) if heating_switch_entity else None
            cooling_mode_switch_state = (
                self.hass.states.get(cooling_mode_switch_entity) if cooling_mode_switch_entity else None
            )
            total_energy_state = self.hass.states.get(total_energy_sensor) if total_energy_sensor else None
            battery_soc_state = self.hass.states.get(battery_soc_sensor) if battery_soc_sensor else None

            source_status = self._build_source_status(
                price_sensor=price_sensor,
                price_state=price_state,
                export_price_sensor=export_price_sensor,
                export_price_state=export_price_state,
                solar_sensor=solar_sensor,
                solar_state=solar_state,
                solar_tomorrow_sensor=solar_tomorrow_sensor,
                solar_tomorrow_state=solar_tomorrow_state,
                temperature_sensor=temperature_sensor,
                temperature_state=temperature_state,
                room_temperature_sensor=room_temperature_sensor,
                room_temperature_state=room_temperature_state,
                heating_switch_entity=heating_switch_entity,
                heating_switch_state=heating_switch_state,
                cooling_mode_switch_entity=cooling_mode_switch_entity,
                cooling_mode_switch_state=cooling_mode_switch_state,
                total_energy_sensor=total_energy_sensor,
                total_energy_state=total_energy_state,
                battery_soc_sensor=battery_soc_sensor,
                battery_soc_state=battery_soc_state,
                planner_kind=planner_kind,
            )
            source_errors = self._collect_source_errors(source_status)

            current_price = _coerce_float(price_state.state) if price_state else None
            export_current_price = (
                _coerce_float(export_price_state.state)
                if export_price_state
                else current_price
            )
            price_resolution = str(self._config.get(CONF_PRICE_RESOLUTION, PRICE_RESOLUTION_HOURLY))
            windows = self._extract_price_windows(
                price_state.attributes if price_state else {},
                current_price,
                price_resolution,
            )
            all_windows = self._extract_price_windows(
                price_state.attributes if price_state else {},
                current_price,
                price_resolution,
                include_past=True,
            )
            export_windows = self._extract_price_windows(
                export_price_state.attributes if export_price_state else (price_state.attributes if price_state else {}),
                export_current_price,
                price_resolution,
            )
            all_export_windows = self._extract_price_windows(
                export_price_state.attributes if export_price_state else (price_state.attributes if price_state else {}),
                export_current_price,
                price_resolution,
                include_past=True,
            )
            battery_switch_windows = self._build_battery_switch_windows(
                attributes=price_state.attributes if price_state else {},
                current_price=current_price,
                price_resolution=price_resolution,
                include_past=True,
            )
            price_average = self._extract_price_average(
                price_state.attributes if price_state else {},
                windows,
            )
            export_price_average = self._extract_price_average(
                export_price_state.attributes if export_price_state else (price_state.attributes if price_state else {}),
                export_windows,
            )

            if not price_state:
                source_status["price_sensor"] = "waiting_for_price_sensor"
                if planner_kind == PLANNER_KIND_THERMOSTAT:
                    windows = self._build_neutral_price_windows(current_price)
                else:
                    neutral_windows = self._build_neutral_price_windows(current_price, hours=48)
                    windows = list(neutral_windows)
                    all_windows = list(neutral_windows)
                    export_windows = list(neutral_windows)
                    all_export_windows = list(neutral_windows)
                    battery_switch_windows = list(neutral_windows)

            if not windows:
                source_status["price_sensor"] = "no_price_windows"
                if planner_kind == PLANNER_KIND_THERMOSTAT:
                    windows = self._build_neutral_price_windows(current_price)
                else:
                    neutral_windows = self._build_neutral_price_windows(current_price, hours=48)
                    windows = list(neutral_windows)
                    all_windows = list(neutral_windows)
                    export_windows = list(neutral_windows)
                    all_export_windows = list(neutral_windows)
                    battery_switch_windows = list(neutral_windows)

            solar_forecast = _coerce_float(
                solar_state.attributes.get("estimate") if solar_state else None,
                default=_coerce_float(solar_state.state, default=0.0) if solar_state else 0.0,
            )
            solar_tomorrow_forecast = _coerce_float(
                solar_tomorrow_state.attributes.get("estimate") if solar_tomorrow_state else None,
                default=_coerce_float(solar_tomorrow_state.state, default=0.0) if solar_tomorrow_state else 0.0,
            )
            battery_soc_percent = _coerce_float(battery_soc_state.state if battery_soc_state else None)
            if battery_soc_percent is not None:
                battery_soc_percent = max(0.0, min(100.0, battery_soc_percent))
            outdoor_temperature = _coerce_float(
                temperature_state.state if temperature_state else None, default=7.0
            )
            room_temperature = _coerce_float(room_temperature_state.state if room_temperature_state else None)
            thermostat_setpoint = self._get_manual_thermostat_setpoint()
            thermostat_cool_setpoint = self._get_manual_cool_temperature()
            thermostat_preheat_setpoint = self._get_manual_preheat_temperature()
            thermostat_eco_setpoint = self._get_manual_eco_temperature(thermostat_setpoint)
            solar_windows = self._extract_solar_windows(solar_state.attributes if solar_state else {})
            solar_windows.extend(
                self._extract_solar_windows(solar_tomorrow_state.attributes if solar_tomorrow_state else {})
            )
            all_solar_windows = self._extract_solar_windows(
                solar_state.attributes if solar_state else {},
                include_past=True,
            )
            all_solar_windows.extend(
                self._extract_solar_windows(
                    solar_tomorrow_state.attributes if solar_tomorrow_state else {},
                    include_past=True,
                )
            )
            solcast_confidence = _coerce_float(
                solar_state.attributes.get("analysis", {}).get("confidence") if solar_state else None
            )
            if planner_kind == PLANNER_KIND_BATTERY and not solar_windows and solar_forecast and solar_forecast > 0:
                fallback_today_windows = self._build_fallback_solar_windows(solar_forecast)
                solar_windows = [*solar_windows, *fallback_today_windows]
                all_solar_windows = [*all_solar_windows, *fallback_today_windows]
            if (
                planner_kind == PLANNER_KIND_BATTERY
                and solar_tomorrow_state
                and not any(window.start.date() > dt_util.now().date() for window in solar_windows)
                and solar_tomorrow_forecast
                and solar_tomorrow_forecast > 0
            ):
                fallback_tomorrow_windows = self._build_fallback_solar_windows_for_day(
                    solar_tomorrow_forecast,
                    day_offset=1,
                )
                solar_windows.extend(fallback_tomorrow_windows)
                all_solar_windows.extend(fallback_tomorrow_windows)
            solar_windows = self._merge_solar_windows(solar_windows)
            all_solar_windows = self._merge_solar_windows(all_solar_windows)
            if planner_kind == PLANNER_KIND_BATTERY:
                battery_price_horizon_end = max(
                    [window.end for window in [*all_windows, *all_solar_windows]],
                    default=now + timedelta(days=1),
                )
                all_windows = self._extend_price_window_tail(
                    windows=all_windows,
                    horizon_end=battery_price_horizon_end,
                    fallback_price=current_price,
                )
                all_export_windows = self._extend_price_window_tail(
                    windows=all_export_windows,
                    horizon_end=battery_price_horizon_end,
                    fallback_price=export_current_price,
                )
                battery_switch_windows = self._extend_price_window_tail(
                    windows=battery_switch_windows,
                    horizon_end=battery_price_horizon_end,
                    fallback_price=current_price,
                )
            if solar_state and solar_forecast <= 0 and not solar_windows:
                source_status["solcast_today_sensor"] = "no_solcast_forecast_data"
            elif solar_state and solar_forecast is None:
                source_status["solcast_today_sensor"] = "invalid_solcast_value"
            if solar_tomorrow_state and solar_tomorrow_forecast <= 0 and not any(
                window.start.date() > dt_util.now().date() for window in all_solar_windows
            ):
                source_status["solcast_tomorrow_sensor"] = "no_solcast_forecast_data"
            elif solar_tomorrow_state and solar_tomorrow_forecast is None:
                source_status["solcast_tomorrow_sensor"] = "invalid_solcast_value"

            total_energy_daily_average = (
                await self._async_get_average_daily_usage(total_energy_sensor)
                if total_energy_state and total_energy_sensor
                else 0.0
            )

            if temperature_state and outdoor_temperature is None:
                source_status["temperature_sensor"] = "invalid_temperature_value"
            if room_temperature_state and room_temperature is None:
                source_status["room_temperature_sensor"] = "invalid_temperature_value"
            if total_energy_state and total_energy_daily_average <= 0:
                source_status["total_energy_sensor"] = "no_total_energy_history_yet"
            if planner_kind == PLANNER_KIND_BATTERY and battery_soc_state and battery_soc_percent is None:
                source_status["battery_soc_sensor"] = "invalid_battery_soc_value"

            if planner_kind == PLANNER_KIND_THERMOSTAT:
                total_energy_daily_average = 0.0
                non_heating_daily_average = 0.0
                heating_estimate = 0.0
                historical_hourly_usage: dict[datetime, float] = {}
            else:
                non_heating_daily_average = total_energy_daily_average
                heating_estimate = 0.0
                historical_hourly_usage = await self._async_get_hourly_energy_usage(total_energy_sensor) if total_energy_sensor else {}

            cooling_profile = await self._async_estimate_room_cooling_profile(
                room_temperature_sensor=room_temperature_sensor,
                heating_switch_entity=heating_switch_entity,
                outdoor_temperature_sensor=temperature_sensor,
                room_temperature_c=room_temperature,
                outdoor_temperature_c=outdoor_temperature,
                thermostat_setpoint_c=thermostat_setpoint,
                thermostat_cool_setpoint_c=thermostat_cool_setpoint,
                thermostat_preheat_setpoint_c=thermostat_preheat_setpoint,
                thermostat_eco_setpoint_c=thermostat_eco_setpoint,
            )

            try:
                result = self._build_plan(
                    planner_kind=planner_kind,
                    windows=windows,
                    all_windows=all_windows,
                    export_windows=export_windows,
                    all_export_windows=all_export_windows,
                    battery_switch_windows=battery_switch_windows,
                    price_average=price_average,
                    export_price_average=export_price_average,
                    current_price=current_price,
                    solar_forecast_kwh=solar_forecast,
                    solar_windows=solar_windows,
                    all_solar_windows=all_solar_windows,
                    solcast_confidence=solcast_confidence,
                    heating_estimate_kwh=heating_estimate,
                    lookback_average_kwh=total_energy_daily_average if planner_kind == PLANNER_KIND_BATTERY else 0.0,
                    total_energy_daily_average_kwh=total_energy_daily_average,
                    non_heating_daily_average_kwh=non_heating_daily_average,
                    historical_hourly_usage=historical_hourly_usage,
                    room_temperature_c=room_temperature,
                    thermostat_setpoint_c=thermostat_setpoint,
                    thermostat_cool_setpoint_c=thermostat_cool_setpoint,
                    thermostat_preheat_setpoint_c=thermostat_preheat_setpoint,
                    thermostat_eco_setpoint_c=thermostat_eco_setpoint,
                    room_cooling_hours_to_eco=cooling_profile["hours_to_eco"],
                    room_cooling_rate_c_per_hour=cooling_profile["cooling_rate_c_per_hour"],
                    cooling_reference_outdoor_temp_c=cooling_profile["reference_outdoor_temp_c"],
                    battery_soc_percent=battery_soc_percent,
                    price_resolution=price_resolution,
                    source_status=source_status,
                    source_errors=self._collect_source_errors(source_status),
                )
            except Exception as err:
                if planner_kind != PLANNER_KIND_THERMOSTAT:
                    raise
                _LOGGER.exception("Thermostat planner failed; falling back to normal mode")
                fallback_errors = self._collect_source_errors(source_status)
                fallback_error = f"thermostat_planning_error: {err!s}"
                if fallback_error not in fallback_errors:
                    fallback_errors.append(fallback_error)
                result = self._build_pending_result(
                    "ready_with_warnings",
                    planner_kind,
                    source_status,
                    fallback_errors,
                )
                result.current_price = current_price
                result.room_temperature_c = room_temperature
                result.thermostat_setpoint_c = thermostat_setpoint
                result.thermostat_cool_setpoint_c = thermostat_cool_setpoint
                result.thermostat_preheat_setpoint_c = thermostat_preheat_setpoint
                result.thermostat_eco_setpoint_c = thermostat_eco_setpoint
                result.room_cooling_hours_to_eco = cooling_profile["hours_to_eco"]
                result.room_cooling_rate_c_per_hour = cooling_profile["cooling_rate_c_per_hour"]
                result.cooling_reference_outdoor_temp_c = cooling_profile["reference_outdoor_temp_c"]
                result.rationale = "thermostat planning degraded after runtime error; using normal mode"
            if planner_kind == PLANNER_KIND_BATTERY and battery_soc_percent is not None:
                configured_battery_capacity = float(
                    self._config.get(CONF_BATTERY_CAPACITY_KWH, DEFAULT_BATTERY_CAPACITY_KWH)
                )
                battery_total_energy_kwh = round(configured_battery_capacity * (battery_soc_percent / 100), 3)
                await self._async_update_battery_profit_tracking(
                    current_battery_energy_kwh=battery_total_energy_kwh,
                    current_mode=result.battery_strategy,
                    import_price=current_price,
                    export_price=export_current_price,
                )
            return result
        except Exception as err:
            _LOGGER.exception("Planner update failed")
            planner_kind = str(self._config.get(CONF_PLANNER_KIND, PLANNER_KIND_BATTERY))
            if planner_kind == PLANNER_KIND_THERMOSTAT:
                fallback_status = self._unknown_source_status(planner_kind)
                fallback_errors = [f"thermostat_planning_error: {err!s}"]
                result = self._build_pending_result(
                    "ready_with_warnings",
                    planner_kind,
                    fallback_status,
                    fallback_errors,
                )
                thermostat_setpoint = self._get_manual_thermostat_setpoint()
                thermostat_cool_setpoint = self._get_manual_cool_temperature()
                thermostat_preheat_setpoint = self._get_manual_preheat_temperature()
                thermostat_eco_setpoint = self._get_manual_eco_temperature(thermostat_setpoint)
                result.thermostat_setpoint_c = thermostat_setpoint
                result.thermostat_cool_setpoint_c = thermostat_cool_setpoint
                result.thermostat_preheat_setpoint_c = thermostat_preheat_setpoint
                result.thermostat_eco_setpoint_c = thermostat_eco_setpoint
                result.rationale = "thermostat planning degraded after runtime error; using normal mode"
                return result
            return self._build_pending_result(
                "planner_runtime_error",
                planner_kind,
                self._unknown_source_status(planner_kind),
                [f"planner_runtime_error: {err!s}"],
            )

    def _build_pending_result(
        self, status: str, planner_kind: str, source_status: dict[str, str], source_errors: list[str]
    ) -> PlannerResult:
        battery_strategy = "accu_uit"
        if planner_kind == PLANNER_KIND_BATTERY and status == "planner_runtime_error":
            battery_strategy = "zelfvoorzienend"
        elif planner_kind == PLANNER_KIND_THERMOSTAT:
            battery_strategy = "not_applicable"

        return PlannerResult(
            planner_kind=planner_kind,
            status=status,
            score=0,
            recommendation="waiting_for_data",
            battery_strategy=battery_strategy,
            heat_pump_strategy="normal",
            heating_estimate_kwh=0.0,
            solar_forecast_kwh=0.0,
            current_price=None,
            price_spread=0.0,
            next_window_start=None,
            next_window_end=None,
            next_window_price=None,
            best_solar_window_start=None,
            best_solar_window_end=None,
            best_solar_window_kwh=None,
            solcast_confidence=None,
            lookback_daily_average_kwh=0.0,
            total_energy_daily_average_kwh=0.0,
            non_heating_daily_average_kwh=0.0,
            estimated_total_home_demand_kwh=0.0,
            estimated_hourly_home_demand=[],
            projected_remaining_solar_until_sunset_kwh=0.0,
            projected_remaining_home_demand_until_sunset_kwh=0.0,
            projected_solar_surplus_until_sunset_kwh=0.0,
            grid_charge_needed_until_sunset_kwh=0.0,
            battery_charge_hours_needed_until_sunset=0.0,
            target_battery_full_by_sunset=False,
            planned_grid_charge_windows=[],
            planned_solar_charge_windows=[],
            planned_battery_mode_schedule=[],
            battery_soc_percent=None,
            battery_min_soc_percent=float(
                self._config.get(CONF_BATTERY_MIN_SOC_PERCENT, DEFAULT_BATTERY_MIN_SOC_PERCENT)
            ),
            battery_total_energy_kwh=0.0,
            battery_energy_available_kwh=0.0,
            battery_remaining_capacity_kwh=0.0,
            next_charge_opportunity_start=None,
            next_charge_window_start=None,
            next_charge_window_end=None,
            next_charge_window_hours=0.0,
            following_charge_window_start=None,
            following_charge_window_end=None,
            following_charge_window_hours=0.0,
            next_discharge_window_start=None,
            next_discharge_window_end=None,
            next_discharge_window_hours=0.0,
            next_idle_window_start=None,
            current_relevant_battery_window_start=None,
            current_relevant_battery_window_end=None,
            current_relevant_battery_window_mode=None,
            current_relevant_battery_window_expected_demand_kwh=0.0,
            current_relevant_battery_window_expected_solar_kwh=0.0,
            home_demand_until_next_charge_kwh=0.0,
            battery_reserved_energy_kwh=0.0,
            battery_energy_available_for_discharge_kwh=0.0,
            battery_exportable_energy_kwh=0.0,
            battery_room_needed_for_solar_kwh=0.0,
            battery_charge_hours_needed_total=0.0,
            battery_full_discharge_hours=0.0,
            battery_simulated_remaining_kwh_after_discharge=0.0,
            next_high_price_window_start=None,
            next_high_price_window_price=None,
            room_temperature_c=None,
            thermostat_setpoint_c=None,
            thermostat_cool_setpoint_c=None,
            thermostat_preheat_setpoint_c=None,
            thermostat_eco_setpoint_c=None,
            room_cooling_hours_to_eco=None,
            room_cooling_rate_c_per_hour=None,
            cooling_reference_outdoor_temp_c=None,
            planned_eco_window_start=None,
            planned_eco_window_end=None,
            planned_eco_windows=[],
            planned_preheat_window_start=None,
            planned_preheat_window_end=None,
            planned_preheat_windows=[],
            battery_min_profit_per_kwh=float(
                self._config.get(CONF_BATTERY_MIN_PROFIT_PER_KWH, DEFAULT_BATTERY_MIN_PROFIT_PER_KWH)
            ),
            price_resolution=str(self._config.get(CONF_PRICE_RESOLUTION, PRICE_RESOLUTION_HOURLY)),
            source_status=source_status,
            source_errors=source_errors,
            rationale=status.replace("_", " "),
        )

    def _build_source_status(
        self,
        *,
        price_sensor: str,
        price_state,
        export_price_sensor: str | None,
        export_price_state,
        solar_sensor: str,
        solar_state,
        solar_tomorrow_sensor: str | None,
        solar_tomorrow_state,
        temperature_sensor: str,
        temperature_state,
        room_temperature_sensor: str | None,
        room_temperature_state,
        heating_switch_entity: str | None,
        heating_switch_state,
        cooling_mode_switch_entity: str | None,
        cooling_mode_switch_state,
        total_energy_sensor: str | None,
        total_energy_state,
        battery_soc_sensor: str | None,
        battery_soc_state,
        planner_kind: str,
    ) -> dict[str, str]:
        if planner_kind == PLANNER_KIND_BATTERY:
            return {
                "price_sensor": self._state_status(price_sensor, price_state),
                "export_price_sensor": self._state_status(export_price_sensor, export_price_state),
                "solcast_today_sensor": self._state_status(solar_sensor, solar_state),
                "solcast_tomorrow_sensor": self._state_status(solar_tomorrow_sensor, solar_tomorrow_state),
                "total_energy_sensor": self._state_status(total_energy_sensor, total_energy_state),
                "battery_soc_sensor": self._state_status(battery_soc_sensor, battery_soc_state),
            }

        return {
            "price_sensor": self._state_status(price_sensor, price_state),
            "temperature_sensor": self._state_status(temperature_sensor, temperature_state),
            "room_temperature_sensor": self._state_status(room_temperature_sensor, room_temperature_state),
            "heating_switch_entity": self._state_status(heating_switch_entity, heating_switch_state),
            "cooling_mode_switch_entity": self._state_status(
                cooling_mode_switch_entity,
                cooling_mode_switch_state,
            ),
        }

    def _collect_source_errors(self, source_status: dict[str, str]) -> list[str]:
        return [
            f"{name}: {status}"
            for name, status in source_status.items()
            if status not in ("ok", "not_configured")
        ]

    def _state_status(self, entity_id: str, state) -> str:
        if not entity_id:
            return "not_configured"
        if state is None:
            return "entity_not_found"
        if state.state in (STATE_UNKNOWN, STATE_UNAVAILABLE, ""):
            return "entity_unavailable"
        return "ok"

    def _unknown_source_status(self, planner_kind: str) -> dict[str, str]:
        if planner_kind == PLANNER_KIND_BATTERY:
            return {
                "price_sensor": "unknown",
                "export_price_sensor": "unknown",
                "solcast_today_sensor": "unknown",
                "solcast_tomorrow_sensor": "unknown",
                "total_energy_sensor": "unknown",
                "battery_soc_sensor": "unknown",
            }

        return {
            "price_sensor": "unknown",
            "temperature_sensor": "unknown",
            "room_temperature_sensor": "unknown",
            "heating_switch_entity": "unknown",
            "cooling_mode_switch_entity": "unknown",
        }

    async def _async_update_battery_profit_tracking(
        self,
        *,
        current_battery_energy_kwh: float,
        current_mode: str,
        import_price: float | None,
        export_price: float | None,
    ) -> None:
        runtime_state = self.hass.data.setdefault(RUNTIME_STATE, {}).setdefault(self.config_entry.entry_id, {})
        last_energy = _coerce_float(runtime_state.get("battery_profit_last_energy_kwh"))
        total_profit = _coerce_float(runtime_state.get("battery_profit_total_eur"), default=0.0) or 0.0
        tracked_energy = _coerce_float(runtime_state.get("battery_profit_tracked_energy_kwh"), default=0.0) or 0.0
        cost_basis = _coerce_float(runtime_state.get("battery_profit_cost_basis_eur"), default=0.0) or 0.0
        now = dt_util.now().isoformat()

        if last_energy is None:
            runtime_state["battery_profit_last_energy_kwh"] = round(current_battery_energy_kwh, 3)
            runtime_state["battery_profit_last_updated"] = now
            await self._async_persist_runtime_state(runtime_state)
            return

        delta_kwh = round(current_battery_energy_kwh - last_energy, 3)
        if abs(delta_kwh) < _BATTERY_PROFIT_NOISE_FLOOR_KWH:
            runtime_state["battery_profit_last_energy_kwh"] = round(current_battery_energy_kwh, 3)
            runtime_state["battery_profit_last_updated"] = now
            # Persist the updated baseline so a restart cannot lose precision
            # by comparing a fresh SoC reading against a stale disk baseline.
            await self._async_persist_runtime_state(runtime_state)
            return

        if delta_kwh > 0 and current_mode == "laden_van_net" and import_price is not None:
            tracked_energy += delta_kwh
            cost_basis += delta_kwh * import_price
        elif delta_kwh > 0 and current_mode == "laden_met_zonne_energie" and export_price is not None:
            tracked_energy += delta_kwh
            cost_basis += delta_kwh * export_price
        elif delta_kwh < 0 and current_mode in ("ontladen", "ontladen_naar_net"):
            discharged_kwh = min(-delta_kwh, tracked_energy)
            if discharged_kwh > 0:
                average_cost = cost_basis / tracked_energy if tracked_energy > 0 else 0.0
                realized_price = (
                    import_price if current_mode == "ontladen" else export_price
                )
                if realized_price is not None:
                    total_profit += discharged_kwh * (realized_price - average_cost)
                tracked_energy = max(0.0, tracked_energy - discharged_kwh)
                cost_basis = max(0.0, cost_basis - (average_cost * discharged_kwh))

        runtime_state["battery_profit_total_eur"] = round(total_profit, 4)
        runtime_state["battery_profit_tracked_energy_kwh"] = round(max(0.0, tracked_energy), 4)
        runtime_state["battery_profit_cost_basis_eur"] = round(max(0.0, cost_basis), 4)
        runtime_state["battery_profit_last_energy_kwh"] = round(current_battery_energy_kwh, 3)
        runtime_state["battery_profit_last_updated"] = now
        await self._async_persist_runtime_state(runtime_state)

    async def _async_persist_runtime_state(self, runtime_state: dict[str, Any]) -> None:
        store = Store[dict[str, Any]](self.hass, STORAGE_VERSION, STORAGE_KEY)
        data = await store.async_load() or {}
        data[self.config_entry.entry_id] = {
            **(data.get(self.config_entry.entry_id, {}) or {}),
            "manual_temperature": runtime_state.get("manual_temperature"),
            "manual_cool_temperature": runtime_state.get("manual_cool_temperature"),
            "manual_eco_temperature": runtime_state.get("manual_eco_temperature"),
            "manual_preheat_temperature": runtime_state.get("manual_preheat_temperature"),
            "hvac_mode": runtime_state.get("hvac_mode"),
            "manual_preset_mode": runtime_state.get("manual_preset_mode"),
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

    async def _async_get_average_daily_usage(self, entity_id: str) -> float:
        """Estimate average daily usage from recorder history of a cumulative kWh sensor."""
        lookback_days = _HISTORY_LOOKBACK_DAYS
        end = dt_util.now()
        start = end - timedelta(days=lookback_days)

        try:
            def _load_history():
                return history.get_significant_states(
                    self.hass,
                    start,
                    end,
                    [entity_id],
                    include_start_time_state=True,
                    significant_changes_only=False,
                    no_attributes=True,
                )

            history_result = await self.hass.async_add_executor_job(_load_history)
        except Exception:
            return 0.0

        states = history_result.get(entity_id, [])
        if len(states) < 2:
            return 0.0

        grouped: dict[date, list[float]] = {}
        for state in states:
            value = _coerce_float(state.state)
            if value is None:
                continue
            grouped.setdefault(state.last_updated.date(), []).append(value)

        deltas: list[float] = []
        for values in grouped.values():
            if len(values) < 2:
                continue
            delta = values[-1] - values[0]
            if delta >= 0:
                deltas.append(delta)

        if not deltas:
            return 0.0
        return round(statistics.fmean(deltas), 2)

    async def _async_get_hourly_energy_usage(
        self,
        entity_id: str,
        *,
        horizon_end: datetime | None = None,
    ) -> dict[datetime, float]:
        """Estimate hourly usage from a cumulative energy sensor by distributing deltas over time."""
        lookback_days = _HISTORY_LOOKBACK_DAYS
        end = horizon_end or dt_util.now()
        start = (dt_util.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=max(lookback_days, 8)))

        try:
            def _load_history():
                return history.get_significant_states(
                    self.hass,
                    start,
                    end,
                    [entity_id],
                    include_start_time_state=True,
                    significant_changes_only=False,
                    no_attributes=True,
                )

            history_result = await self.hass.async_add_executor_job(_load_history)
        except Exception:
            return {}

        states = history_result.get(entity_id, [])
        if len(states) < 2:
            return {}

        valid_states: list[tuple[datetime, float]] = []
        for state in states:
            value = _coerce_float(state.state)
            if value is None:
                continue
            last_updated = getattr(state, "last_updated", None)
            if last_updated is None:
                continue
            if dt_util.as_local(last_updated) > dt_util.as_local(dt_util.now()):
                continue
            valid_states.append((dt_util.as_local(last_updated), value))

        if len(valid_states) < 2:
            return {}

        hourly_usage: dict[datetime, float] = {}
        for (previous_time, previous_value), (current_time, current_value) in zip(valid_states, valid_states[1:], strict=False):
            if current_time <= previous_time:
                continue
            delta_kwh = current_value - previous_value
            if delta_kwh < 0:
                continue

            segment_seconds = (current_time - previous_time).total_seconds()
            if segment_seconds <= 0:
                continue

            bucket_start = previous_time.replace(minute=0, second=0, microsecond=0)
            while bucket_start < current_time:
                bucket_end = bucket_start + timedelta(hours=1)
                overlap_start = max(previous_time, bucket_start)
                overlap_end = min(current_time, bucket_end)
                overlap_seconds = (overlap_end - overlap_start).total_seconds()
                if overlap_seconds > 0:
                    hourly_usage[bucket_start] = hourly_usage.get(bucket_start, 0.0) + (
                        delta_kwh * (overlap_seconds / segment_seconds)
                    )
                bucket_start = bucket_end

        return {
            hour_start: round(usage_kwh, 6)
            for hour_start, usage_kwh in hourly_usage.items()
            if usage_kwh >= 0
        }

    def _get_manual_thermostat_setpoint(self) -> float:
        runtime_state = self.hass.data.get(RUNTIME_STATE, {}).get(self.config_entry.entry_id, {})
        manual_temperature = _coerce_float(runtime_state.get("manual_temperature"))
        if manual_temperature is None:
            manual_temperature = min(max(20.0, self._thermostat_min_temp()), self._thermostat_max_temp())
        return round(min(self._thermostat_max_temp(), max(self._thermostat_min_temp(), manual_temperature)), 2)

    def _get_manual_eco_temperature(self, thermostat_setpoint_c: float | None) -> float | None:
        runtime_state = self.hass.data.get(RUNTIME_STATE, {}).get(self.config_entry.entry_id, {})
        eco_temperature = _coerce_float(runtime_state.get("manual_eco_temperature"))
        if eco_temperature is None:
            eco_temperature = _coerce_float(
                self._config.get(CONF_THERMOSTAT_ECO_TEMPERATURE, DEFAULT_THERMOSTAT_ECO_TEMPERATURE)
            )
        if eco_temperature is None:
            return None
        if thermostat_setpoint_c is not None:
            eco_temperature = min(eco_temperature, thermostat_setpoint_c)
        return round(min(self._thermostat_max_temp(), max(self._thermostat_min_temp(), eco_temperature)), 2)

    def _get_manual_cool_temperature(self) -> float:
        runtime_state = self.hass.data.get(RUNTIME_STATE, {}).get(self.config_entry.entry_id, {})
        cool_temperature = _coerce_float(runtime_state.get("manual_cool_temperature"))
        if cool_temperature is None:
            cool_temperature = min(self._thermostat_max_temp(), max(self._thermostat_min_temp(), 24.0))
        return round(min(self._thermostat_max_temp(), max(self._thermostat_min_temp(), cool_temperature)), 2)

    def _get_manual_preheat_temperature(self) -> float:
        runtime_state = self.hass.data.get(RUNTIME_STATE, {}).get(self.config_entry.entry_id, {})
        preheat_temperature = _coerce_float(runtime_state.get("manual_preheat_temperature"))
        if preheat_temperature is None:
            base_temperature = self._get_manual_thermostat_setpoint()
            preheat_temperature = min(self._thermostat_max_temp(), max(self._thermostat_min_temp(), base_temperature + 1.0))
        return round(min(self._thermostat_max_temp(), max(self._thermostat_min_temp(), preheat_temperature)), 2)

    def _thermostat_min_temp(self) -> float:
        return float(self._config.get(CONF_THERMOSTAT_MIN_TEMP, DEFAULT_THERMOSTAT_MIN_TEMP))

    def _thermostat_max_temp(self) -> float:
        return float(self._config.get(CONF_THERMOSTAT_MAX_TEMP, DEFAULT_THERMOSTAT_MAX_TEMP))

    async def _async_estimate_room_cooling_profile(
        self,
        *,
        room_temperature_c: float | None,
        outdoor_temperature_c: float,
        thermostat_setpoint_c: float | None,
        thermostat_eco_setpoint_c: float | None,
        **_: Any,
    ) -> dict[str, float | None]:
        if thermostat_setpoint_c is None or thermostat_eco_setpoint_c is None or room_temperature_c is None:
            return {
                "hours_to_eco": None,
                "cooling_rate_c_per_hour": None,
                "reference_outdoor_temp_c": outdoor_temperature_c,
            }

        cooldown_reference_temperature = max(room_temperature_c, thermostat_setpoint_c)
        cooldown_delta = max(cooldown_reference_temperature - thermostat_eco_setpoint_c, 0.3)
        estimated_rate, hours_to_eco = self._estimate_cooling_profile_from_model(
            outdoor_temperature_c=outdoor_temperature_c,
            room_temperature_c=cooldown_reference_temperature,
            cooldown_delta_c=cooldown_delta,
        )
        reference_outdoor = outdoor_temperature_c

        return {
            "hours_to_eco": round(hours_to_eco, 2),
            "cooling_rate_c_per_hour": round(estimated_rate, 3),
            "reference_outdoor_temp_c": reference_outdoor,
        }

    def _estimate_cooling_profile_from_model(
        self,
        *,
        outdoor_temperature_c: float,
        room_temperature_c: float,
        cooldown_delta_c: float,
    ) -> tuple[float, float]:
        runtime_state = self.hass.data.get(RUNTIME_STATE, {}).get(self.config_entry.entry_id, {})
        cooling_model = runtime_state.get("cooling_model", {})
        learned_factor = _coerce_float(cooling_model.get("rolling_cooling_factor"))
        learned_samples = int(_coerce_float(cooling_model.get("eco_sample_count"), default=0.0) or 0)
        last_eco_duration_hours = _coerce_float(cooling_model.get("last_eco_duration_hours"))
        delta_temp = max(room_temperature_c - outdoor_temperature_c, 1.0)

        # Floor heating cools down much slower than the old generic fallback
        # assumed. Use a more conservative base model and start blending in the
        # learned factor as soon as any completed eco session exists.
        fallback_rate = max(0.03, delta_temp * _THERMOSTAT_FALLBACK_COOLING_FACTOR)
        fallback_hours = min(
            _THERMOSTAT_MAX_COOLDOWN_HOURS,
            max(_THERMOSTAT_MIN_FALLBACK_COOLDOWN_HOURS, cooldown_delta_c / fallback_rate),
        )

        if learned_factor is not None and learned_samples > 0:
            delta_temp = max(room_temperature_c - outdoor_temperature_c, 0.5)
            learned_rate = max(0.03, min(2.0, learned_factor * delta_temp))
            blend = min(1.0, learned_samples / float(_THERMOSTAT_COOLING_LEARN_SAMPLES))
            estimated_rate = (fallback_rate * (1.0 - blend)) + (learned_rate * blend)
            estimated_hours = min(
                _THERMOSTAT_MAX_COOLDOWN_HOURS,
                max(1.0, cooldown_delta_c / max(estimated_rate, 0.03)),
            )
            if last_eco_duration_hours is not None:
                estimated_hours = max(estimated_hours, min(_THERMOSTAT_MAX_COOLDOWN_HOURS, last_eco_duration_hours))
            return estimated_rate, estimated_hours

        return fallback_rate, fallback_hours

    def _select_most_expensive_window_block(
        self,
        *,
        windows: list[PlannerWindow],
        now: datetime,
        duration_hours: float,
    ) -> dict[str, datetime | float] | None:
        if duration_hours <= 0:
            return None

        eligible_windows = [window for window in windows if window.end > now]
        if not eligible_windows:
            return None

        best_block: dict[str, datetime | float] | None = None
        for start_index, start_window in enumerate(eligible_windows):
            block_start = max(start_window.start, now)
            accumulated_hours = 0.0
            weighted_price = 0.0
            block_end = block_start

            for window in eligible_windows[start_index:]:
                usable_start = max(window.start, block_end if block_end > window.start else window.start)
                usable_end = window.end
                usable_hours = (usable_end - usable_start).total_seconds() / 3600
                if usable_hours <= 0:
                    continue
                take_hours = min(usable_hours, duration_hours - accumulated_hours)
                weighted_price += window.price * take_hours
                accumulated_hours += take_hours
                block_end = usable_start + timedelta(hours=take_hours)
                if accumulated_hours >= duration_hours:
                    average_price = weighted_price / accumulated_hours
                    candidate = {"start": block_start, "end": block_end, "average_price": average_price}
                    if best_block is None or average_price > float(best_block["average_price"]):
                        best_block = candidate
                    break

        return best_block

    def _select_expensive_peak_blocks(
        self,
        *,
        windows: list[PlannerWindow],
        now: datetime,
        duration_hours: float,
        expensive_threshold: float,
    ) -> list[dict[str, datetime | float]]:
        """Select one eco block for each distinct expensive price peak."""
        eligible_windows = [
            window
            for window in windows
            if window.end > now and window.price >= expensive_threshold
        ]
        if not eligible_windows:
            return []

        grouped_windows: list[list[PlannerWindow]] = []
        current_group: list[PlannerWindow] = []
        for window in eligible_windows:
            if not current_group:
                current_group = [window]
                continue
            previous = current_group[-1]
            if window.start <= previous.end:
                current_group.append(window)
            else:
                grouped_windows.append(current_group)
                current_group = [window]
        if current_group:
            grouped_windows.append(current_group)

        peak_blocks: list[dict[str, datetime | float]] = []
        for group in grouped_windows:
            group_start = max(group[0].start, now)
            group_end = group[-1].end
            group_hours = max((group_end - group_start).total_seconds() / 3600, 0.0)
            if group_hours <= 0:
                continue

            if duration_hours > 0 and group_hours > duration_hours:
                selected = self._select_most_expensive_window_block(
                    windows=group,
                    now=now,
                    duration_hours=duration_hours,
                )
                if selected is not None:
                    peak_blocks.append(selected)
                continue

            weighted_price = 0.0
            total_hours = 0.0
            for window in group:
                usable_start = max(window.start, now)
                usable_hours = max((window.end - usable_start).total_seconds() / 3600, 0.0)
                if usable_hours <= 0:
                    continue
                weighted_price += window.price * usable_hours
                total_hours += usable_hours
            if total_hours <= 0:
                continue
            peak_blocks.append(
                {
                    "start": group_start,
                    "end": group_end,
                    "average_price": weighted_price / total_hours,
                }
            )

        return sorted(peak_blocks, key=lambda item: item["start"])

    def _select_thermostat_peak_eco_windows(
        self,
        *,
        windows: list[PlannerWindow],
        now: datetime,
        cooldown_hours: float,
        average_price: float,
        expensive_threshold: float,
    ) -> list[dict[str, datetime | float]]:
        """Plan eco windows by expanding around the local peak price."""
        if cooldown_hours <= 0:
            return []

        planning_start = now - timedelta(hours=cooldown_hours)
        planning_windows = [window for window in windows if window.end > planning_start]
        future_windows = [window for window in planning_windows if window.end > now]
        if not future_windows:
            return []

        expensive_windows = [
            window for window in future_windows if window.price >= expensive_threshold
        ]
        if not expensive_windows:
            return []

        grouped_windows: list[list[PlannerWindow]] = []
        current_group: list[PlannerWindow] = []
        for window in expensive_windows:
            if not current_group:
                current_group = [window]
                continue
            previous = current_group[-1]
            if window.start <= previous.end:
                current_group.append(window)
            else:
                grouped_windows.append(current_group)
                current_group = [window]
        if current_group:
            grouped_windows.append(current_group)

        eco_windows: list[dict[str, datetime | float]] = []
        for group in grouped_windows:
            peak_window = max(group, key=lambda item: item.price)
            # Use identity lookup so two windows with coincidentally identical
            # start/end/price (e.g. neutral fallback windows) cannot return the
            # wrong index from list.index's equality comparison.
            peak_index = next(
                (index for index, window in enumerate(planning_windows) if window is peak_window),
                -1,
            )
            if peak_index < 0:
                # Fallback: find by timestamp if identity lookup fails for some reason.
                peak_index = next(
                    (
                        index
                        for index, window in enumerate(planning_windows)
                        if window.start == peak_window.start and window.end == peak_window.end
                    ),
                    -1,
                )
            if peak_index < 0:
                continue
            selected_indices = {peak_index}
            total_hours = max((peak_window.end - max(peak_window.start, now)).total_seconds() / 3600, 0.0)
            left_index = peak_index - 1
            right_index = peak_index + 1

            while total_hours < cooldown_hours:
                left_window = planning_windows[left_index] if left_index >= 0 else None
                right_window = planning_windows[right_index] if right_index < len(planning_windows) else None
                if left_window is None and right_window is None:
                    break

                choose_left = False
                if left_window is not None and right_window is not None:
                    choose_left = float(left_window.price) >= float(right_window.price)
                elif left_window is not None:
                    choose_left = True

                chosen_index = left_index if choose_left else right_index
                chosen_window = planning_windows[chosen_index]
                selected_indices.add(chosen_index)
                total_hours += max((chosen_window.end - max(chosen_window.start, now)).total_seconds() / 3600, 0.0)
                if choose_left:
                    left_index -= 1
                else:
                    right_index += 1

            selected_windows = [planning_windows[index] for index in sorted(selected_indices)]
            eco_start = selected_windows[0].start
            eco_end = selected_windows[-1].end
            below_average_at = next(
                (
                    window.start
                    for window in planning_windows
                    if window.start >= peak_window.end and window.price < average_price
                ),
                None,
            )
            if below_average_at is not None:
                eco_end = min(eco_end, below_average_at)
            if eco_end <= eco_start:
                continue

            span_hours = (eco_end - eco_start).total_seconds() / 3600
            if span_hours <= 0:
                continue

            weighted_price = 0.0
            total_hours = 0.0
            for window in planning_windows:
                overlap_start = max(window.start, eco_start)
                overlap_end = min(window.end, eco_end)
                overlap_hours = (overlap_end - overlap_start).total_seconds() / 3600
                if overlap_hours <= 0:
                    continue
                weighted_price += window.price * overlap_hours
                total_hours += overlap_hours

            average_window_price = (
                weighted_price / total_hours if total_hours > 0 else peak_window.price
            )
            eco_windows.append(
                {
                    "start": eco_start,
                    "peak_start": peak_window.start,
                    "end": eco_end,
                    "average_price": average_window_price,
                }
            )

        if not eco_windows:
            return []

        merged_windows: list[dict[str, datetime | float]] = []
        for window in sorted(eco_windows, key=lambda item: item["start"]):
            if not merged_windows:
                merged_windows.append(window)
                continue

            previous = merged_windows[-1]
            if window["start"] <= previous["end"] + _THERMOSTAT_ECO_MERGE_GAP:
                previous["end"] = max(previous["end"], window["end"])
                previous["peak_start"] = min(previous.get("peak_start", previous["start"]), window.get("peak_start", window["start"]))
                previous["average_price"] = max(
                    float(previous["average_price"]),
                    float(window["average_price"]),
                )
            else:
                merged_windows.append(window)

        return merged_windows

    def _build_plan(
        self,
        *,
        planner_kind: str,
        windows: list[PlannerWindow],
        all_windows: list[PlannerWindow],
        export_windows: list[PlannerWindow],
        all_export_windows: list[PlannerWindow],
        battery_switch_windows: list[PlannerWindow],
        price_average: float | None,
        export_price_average: float | None,
        current_price: float | None,
        solar_forecast_kwh: float,
        solar_windows: list[SolarWindow],
        all_solar_windows: list[SolarWindow],
        solcast_confidence: float | None,
        heating_estimate_kwh: float,
        lookback_average_kwh: float,
        total_energy_daily_average_kwh: float,
        non_heating_daily_average_kwh: float,
        historical_hourly_usage: dict[datetime, float],
        room_temperature_c: float | None,
        thermostat_setpoint_c: float | None,
        thermostat_cool_setpoint_c: float | None,
        thermostat_preheat_setpoint_c: float | None,
        thermostat_eco_setpoint_c: float | None,
        room_cooling_hours_to_eco: float | None,
        room_cooling_rate_c_per_hour: float | None,
        cooling_reference_outdoor_temp_c: float | None,
        battery_soc_percent: float | None,
        price_resolution: str,
        source_status: dict[str, str],
        source_errors: list[str],
    ) -> PlannerResult:
        now = dt_util.now()
        if not windows:
            fallback_price = current_price if current_price is not None else 0.0
            fallback_window = PlannerWindow(
                start=now,
                end=now + timedelta(hours=1),
                price=fallback_price,
            )
            windows = [fallback_window]
        if not all_windows:
            all_windows = list(windows)
        if not export_windows:
            export_windows = list(windows)
        if not all_export_windows:
            all_export_windows = list(export_windows)
        if not battery_switch_windows:
            battery_switch_windows = list(all_windows)

        sorted_by_price = sorted(windows, key=lambda item: item.price)
        cheapest = sorted_by_price[0]
        most_expensive = sorted_by_price[-1]
        average_price = (
            price_average
            if price_average is not None
            else sum(window.price for window in windows) / len(windows)
        )
        price_spread = round(most_expensive.price - cheapest.price, 4)
        price_signal_available = (
            source_status.get("price_sensor") == "ok"
            and len(windows) > 1
            and price_spread > 0
        )
        today_solar_windows = [window for window in solar_windows if window.start.date() == now.date()]
        future_solar_windows = [window for window in solar_windows if window.start.date() > now.date()]
        best_solar_window = self._select_best_solar_window(today_solar_windows or solar_windows)
        planning_horizon_end = max(
            [window.end for window in [*all_windows, *all_solar_windows]],
            default=now + timedelta(days=1),
        )
        estimated_hourly_home_demand = self._build_hourly_home_demand_forecast(
            non_heating_daily_average_kwh=non_heating_daily_average_kwh,
            heating_estimate_kwh=heating_estimate_kwh,
            historical_hourly_usage=historical_hourly_usage,
            horizon_end=planning_horizon_end,
        )
        estimated_total_home_demand_kwh = round(
            sum(
                float(slot["estimated_kwh"])
                for slot in estimated_hourly_home_demand
                if dt_util.parse_datetime(str(slot["start"])) is not None
                and dt_util.parse_datetime(str(slot["start"])).date() == now.date()
            ),
            2,
        )
        sunset_time = self._get_solar_day_end(today_solar_windows)
        planning_horizon_solar_end = self._get_solar_day_end(solar_windows)
        remaining_solar_until_sunset = self._sum_remaining_solar_until(today_solar_windows, now, sunset_time)
        remaining_home_demand_until_sunset = self._sum_remaining_home_demand_until(
            estimated_hourly_home_demand, now, sunset_time
        )
        projected_solar_surplus_until_sunset = max(
            0.0, round(remaining_solar_until_sunset - remaining_home_demand_until_sunset, 3)
        )
        net_solar_balance_until_sunset = round(
            remaining_solar_until_sunset - remaining_home_demand_until_sunset,
            3,
        )

        cheap_threshold = cheapest.price + (price_spread * 0.25 if price_signal_available else 0)
        mid_price_threshold = cheapest.price + (price_spread * 0.5 if price_signal_available else 0)
        expensive_threshold = most_expensive.price - (price_spread * 0.25 if price_signal_available else 0)
        next_cheap = next(
            (window for window in windows if window.start > now and window.price <= cheap_threshold),
            next((window for window in windows if window.price <= cheap_threshold), cheapest),
        )
        solar_covers_today = (
            self._sum_remaining_solar_until(today_solar_windows, now, sunset_time) >= remaining_home_demand_until_sunset
            and estimated_total_home_demand_kwh > 0
        )
        cheap_now = current_price is not None and current_price <= cheap_threshold
        best_solar_is_now = (
            best_solar_window is not None
            and best_solar_window.start <= now < best_solar_window.end
        )
        future_windows = [window for window in windows if window.start > now]
        future_min_price = min((window.price for window in future_windows), default=None)
        future_max_price = max((window.price for window in future_windows), default=None)
        battery_min_profit = float(
            self._config.get(CONF_BATTERY_MIN_PROFIT_PER_KWH, DEFAULT_BATTERY_MIN_PROFIT_PER_KWH)
        )
        future_cheaper_by = (
            round(current_price - future_min_price, 4)
            if current_price is not None and future_min_price is not None
            else None
        )
        future_more_expensive_by = (
            round(future_max_price - current_price, 4)
            if current_price is not None and future_max_price is not None
            else None
        )
        future_solar_charge_window = False
        eco_duration_hours = room_cooling_hours_to_eco or 0.0
        eco_expensive_threshold = (
            mid_price_threshold if planner_kind == PLANNER_KIND_THERMOSTAT else expensive_threshold
        )
        eco_windows = []
        thermostat_planning_error: str | None = None
        if price_signal_available:
            try:
                if planner_kind == PLANNER_KIND_THERMOSTAT:
                    eco_windows = self._select_thermostat_peak_eco_windows(
                        windows=windows,
                        now=now,
                        cooldown_hours=eco_duration_hours,
                        average_price=average_price,
                        expensive_threshold=expensive_threshold,
                    )
                else:
                    eco_windows = self._select_expensive_peak_blocks(
                        windows=windows,
                        now=now,
                        duration_hours=eco_duration_hours,
                        expensive_threshold=eco_expensive_threshold,
                    )
            except Exception as err:
                if planner_kind == PLANNER_KIND_THERMOSTAT:
                    thermostat_planning_error = f"thermostat_planning_error: {err!s}"
                    _LOGGER.exception("Thermostat peak planning failed")
                    eco_windows = []
                else:
                    raise
        if thermostat_planning_error is not None and thermostat_planning_error not in source_errors:
            source_errors = [*source_errors, thermostat_planning_error]
        # Only relevant for thermostat planners; battery planners have no room sensor
        eco_temp_reached = (
            planner_kind == PLANNER_KIND_THERMOSTAT
            and room_temperature_c is not None
            and thermostat_eco_setpoint_c is not None
            and room_temperature_c <= thermostat_eco_setpoint_c + 0.1
        )
        active_eco_window = next(
            (window for window in eco_windows if window["start"] <= now < window["end"]),
            None,
        )
        # Clear the early-exit latch when:
        # (a) the window it was set for has ended, or
        # (b) the planner is now in a different eco window (new planning cycle produced
        #     a window with a different end time — fresh window deserves a fresh start).
        if self._eco_early_exit_until is not None:
            if now >= self._eco_early_exit_until:
                self._eco_early_exit_until = None
            elif active_eco_window is not None and active_eco_window["end"] != self._eco_early_exit_until:
                self._eco_early_exit_until = None
        # Once the room reaches the eco setpoint inside an active eco window, latch
        # eco off for the rest of that window.  The room has used its stored floor
        # heat; normal heating resumes to maintain comfort.  The latch prevents
        # rapid oscillation between eco and normal every update cycle.
        if eco_temp_reached and active_eco_window is not None and self._eco_early_exit_until is None:
            self._eco_early_exit_until = cast(datetime, active_eco_window["end"])
        # Eco is only active when:
        # - inside an active eco window
        # - the current price slot is expensive (≥ average) — eco is off during
        #   cheap "dal" periods within a wide eco window
        # - the room has not yet reached the eco setpoint (no latch set)
        eco_active_now = (
            active_eco_window is not None
            and current_price is not None
            and current_price >= average_price
            and not eco_temp_reached
            and self._eco_early_exit_until is None
        )
        eco_window = (
            active_eco_window
            if eco_active_now
            else next((window for window in eco_windows if window["start"] > now), None)
        )
        preheat_minutes = int(
            self._config.get(CONF_THERMOSTAT_PREHEAT_MINUTES, DEFAULT_THERMOSTAT_PREHEAT_MINUTES)
        )
        # Preheat is timed before peak_start (the expensive part of the eco window),
        # not before the eco window's start which may be hours earlier in cheap slots.
        # Battery eco_windows (from _select_expensive_peak_blocks) have no peak_start;
        # fall back to window["start"] so the battery planner doesn't crash.
        preheat_windows = [
            {
                "start": max(
                    cast(datetime, window.get("peak_start", window["start"])) - timedelta(minutes=preheat_minutes),
                    now.replace(hour=0, minute=0, second=0, microsecond=0),
                ),
                "end": cast(datetime, window.get("peak_start", window["start"])),
                "average_price": window["average_price"],
            }
            for window in eco_windows
            if preheat_minutes > 0
        ]
        preheat_windows = [
            window
            for window in preheat_windows
            if window["end"] > now
        ]
        preheat_window = next(
            (
                window
                for window in preheat_windows
                if window["start"] <= now < window["end"]
            ),
            next((window for window in preheat_windows if window["start"] > now), None),
        )
        preheat_active_now = any(window["start"] <= now < window["end"] for window in preheat_windows)

        battery_enabled = bool(self._config.get(CONF_BATTERY_ENABLED, DEFAULT_BATTERY_ENABLED))
        battery_capacity = float(
            self._config.get(CONF_BATTERY_CAPACITY_KWH, DEFAULT_BATTERY_CAPACITY_KWH)
        )
        battery_min_soc_percent = float(
            self._config.get(CONF_BATTERY_MIN_SOC_PERCENT, DEFAULT_BATTERY_MIN_SOC_PERCENT)
        )
        max_charge = float(self._config.get(CONF_BATTERY_MAX_CHARGE_KW, DEFAULT_BATTERY_MAX_CHARGE_KW))
        max_discharge = float(
            self._config.get(CONF_BATTERY_MAX_DISCHARGE_KW, DEFAULT_BATTERY_MAX_DISCHARGE_KW)
        )
        minimum_battery_reserve_kwh = round(battery_capacity * max(0.0, min(100.0, battery_min_soc_percent)) / 100, 3)
        battery_total_energy_kwh = (
            round(battery_capacity * (battery_soc_percent / 100), 3)
            if battery_soc_percent is not None
            else 0.0
        )
        battery_energy_available_kwh = (
            max(0.0, round(battery_total_energy_kwh - minimum_battery_reserve_kwh, 3))
        )
        battery_remaining_capacity_kwh = max(0.0, round(battery_capacity - battery_total_energy_kwh, 3))
        usable_battery_capacity_kwh = max(0.0, round(battery_capacity - minimum_battery_reserve_kwh, 3))
        remaining_usable_capacity_kwh = max(0.0, round(usable_battery_capacity_kwh - battery_energy_available_kwh, 3))
        target_battery_full_by_sunset = battery_enabled and remaining_usable_capacity_kwh > 0
        energy_balance_slots = self._build_energy_balance_slots(
            price_windows=battery_switch_windows or all_windows,
            export_price_windows=all_export_windows or export_windows or all_windows,
            solar_windows=all_solar_windows,
            hourly_demand=estimated_hourly_home_demand,
            horizon_start=now,
        )
        planned_solar_charge_windows, planned_grid_charge_windows = self._plan_charge_windows_for_horizon(
            slots=energy_balance_slots,
            now=now,
            usable_capacity_kwh=usable_battery_capacity_kwh,
            current_remaining_capacity_kwh=remaining_usable_capacity_kwh,
            max_charge_kw=max_charge,
            battery_min_profit=battery_min_profit,
        )
        next_planned_solar_charge_start = min(
            (
                window_start
                for window in planned_solar_charge_windows
                if (window_start := dt_util.parse_datetime(str(window["start"]))) is not None and window_start > now
            ),
            default=None,
        )
        next_planned_grid_charge_start = min(
            (
                window_start
                for window in planned_grid_charge_windows
                if (window_start := dt_util.parse_datetime(str(window["start"]))) is not None and window_start > now
            ),
            default=None,
        )
        next_charge_opportunity = min(
            (
                moment
                for moment in (next_planned_solar_charge_start, next_planned_grid_charge_start)
                if moment is not None
            ),
            default=None,
        )
        home_demand_until_next_charge_kwh = round(
            self._sum_remaining_home_demand_until(estimated_hourly_home_demand, now, next_charge_opportunity),
            3,
        )
        solar_until_next_charge_kwh = round(
            self._sum_remaining_solar_until(all_solar_windows, now, next_charge_opportunity),
            3,
        )
        discharge_to_grid_window_start = (
            max(now, next_charge_opportunity - timedelta(hours=8))
            if next_charge_opportunity is not None
            else None
        )
        home_demand_before_next_charge_window_kwh = round(
            self._sum_remaining_home_demand_until(
                estimated_hourly_home_demand,
                discharge_to_grid_window_start or now,
                next_charge_opportunity,
            ),
            3,
        )
        solar_before_next_charge_window_kwh = round(
            self._sum_remaining_solar_until(
                all_solar_windows,
                discharge_to_grid_window_start or now,
                next_charge_opportunity,
            ),
            3,
        )
        battery_reserved_energy_kwh = min(
            battery_energy_available_kwh,
            max(0.0, round(home_demand_before_next_charge_window_kwh - solar_before_next_charge_window_kwh, 3)),
        )
        battery_energy_available_for_discharge_kwh = max(
            0.0,
            round(battery_energy_available_kwh - battery_reserved_energy_kwh, 3),
        )
        battery_export_protected_energy_kwh = min(
            battery_energy_available_kwh,
            max(0.0, round(home_demand_until_next_charge_kwh - solar_until_next_charge_kwh, 3)),
        )
        battery_exportable_energy_kwh = max(
            0.0,
            round(battery_energy_available_kwh - battery_export_protected_energy_kwh, 3),
        )
        battery_room_needed_for_solar_kwh = max(
            0.0,
            round(battery_total_energy_kwh + projected_solar_surplus_until_sunset - battery_capacity, 3),
        )
        next_high_price_window = (
            self._find_next_high_price_window(
                future_windows=future_windows,
                average_price=average_price,
            )
            if price_signal_available
            else None
        )
        full_planned_mode_windows, planned_current_mode = self._build_mode_windows_from_hourly_plan(
            slots=energy_balance_slots,
            now=now,
            planned_solar_charge_windows=planned_solar_charge_windows,
            planned_grid_charge_windows=planned_grid_charge_windows,
            initial_usable_energy_kwh=battery_energy_available_kwh,
            minimum_energy_before_next_charge_kwh=battery_reserved_energy_kwh,
            minimum_energy_for_export_before_next_charge_kwh=battery_export_protected_energy_kwh,
            usable_capacity_kwh=usable_battery_capacity_kwh,
            average_price=average_price,
            average_export_price=export_price_average if export_price_average is not None else average_price,
            max_charge_kw=max_charge,
            max_discharge_kw=max_discharge,
        )
        if planner_kind == PLANNER_KIND_BATTERY and battery_soc_percent is None:
            full_planned_mode_windows = []
            planned_current_mode = "accu_uit"

        full_planned_discharge_windows = [
            window
            for window in full_planned_mode_windows
            if str(window.get("mode")) in ("ontladen", "ontladen_naar_net")
        ]
        grid_charge_needed_until_sunset = round(
            sum(float(window.get("usable_hours", 0.0)) * max_charge for window in planned_grid_charge_windows),
            3,
        )
        battery_charge_hours_needed_until_sunset = round(
            sum(float(window.get("usable_hours", 0.0)) for window in planned_grid_charge_windows),
            3,
        )
        battery_charge_hours_needed_total = round(
            sum(float(window.get("usable_hours", 0.0)) for window in [*planned_solar_charge_windows, *planned_grid_charge_windows]),
            3,
        )
        battery_full_discharge_hours = round(
            battery_energy_available_kwh / max_discharge,
            3,
        ) if max_discharge > 0 else 0.0
        battery_cycle_summary = self._summarize_battery_cycles(
            full_planned_mode_windows=full_planned_mode_windows,
            energy_balance_slots=energy_balance_slots,
            now=now,
        )
        battery_simulated_remaining_kwh_after_discharge = round(
            max(
                0.0,
                battery_energy_available_kwh
                - float(battery_cycle_summary.get("current_relevant_battery_window_expected_demand_kwh", 0.0)),
            ),
            3,
        )
        _LOGGER.debug(
            "Battery summary: relevant=%s %s->%s charge=%s->%s discharge=%s->%s exportable=%.3f usable=%.3f",
            battery_cycle_summary["current_relevant_battery_window_mode"],
            battery_cycle_summary["current_relevant_battery_window_start"],
            battery_cycle_summary["current_relevant_battery_window_end"],
            battery_cycle_summary["next_charge_window_start"],
            battery_cycle_summary["next_charge_window_end"],
            battery_cycle_summary["next_discharge_window_start"],
            battery_cycle_summary["next_discharge_window_end"],
            battery_exportable_energy_kwh,
            battery_energy_available_kwh,
        )

        score = 50
        rationale_parts: list[str] = []
        recommendation = "wait"

        if cheap_now:
            recommendation = "run_flexible_loads_now"
            score += 25
            rationale_parts.append("current price is in the cheap band")
        else:
            rationale_parts.append("current price is above the preferred cheap band")

        if (
            best_solar_window is not None
            and best_solar_window.forecast_kwh >= 1.0
            and solar_forecast_kwh >= max(2.0, estimated_total_home_demand_kwh * 0.25)
        ):
            recommendation = "shift_loads_to_solar_window"
            score += 15
            rationale_parts.append("Solcast shows a useful daytime solar production window")

        if planner_kind == PLANNER_KIND_BATTERY and non_heating_daily_average_kwh > 0:
            rationale_parts.append("estimated home demand is derived from total self-used energy history")

        heat_pump_strategy = "normal"
        if planner_kind == PLANNER_KIND_BATTERY:
            heat_pump_strategy = "not_applicable"
        elif planner_kind == PLANNER_KIND_THERMOSTAT and eco_active_now:
            heat_pump_strategy = "energy_saving_on"
            score += 8
            recommendation = "set_thermostat_to_eco"
            rationale_parts.append(
                "thermostat eco is active around the current price peak"
            )
            if thermostat_eco_setpoint_c is not None:
                rationale_parts.append(
                    f"set the room thermostat to about {thermostat_eco_setpoint_c:.1f} C until the room is cooled down or the price falls below the daily average"
                )
        elif planner_kind == PLANNER_KIND_THERMOSTAT and preheat_active_now:
            heat_pump_strategy = "preheating"
            score += 6
            rationale_parts.append(
                "preheating is active so the floor can store heat before the upcoming eco window"
            )
        elif cheap_now and (best_solar_is_now or solar_covers_today):
            heat_pump_strategy = "normal"
            rationale_parts.append("heat pump does not need power saving because this is already a cheap solar window")
        elif planner_kind == PLANNER_KIND_THERMOSTAT and eco_windows:
            rationale_parts.append(
                f"thermostat eco is planned around {len(eco_windows)} above-average price peak(s) in the current planning horizon"
            )
            if preheat_minutes > 0:
                rationale_parts.append(
                    f"preheating starts about {preheat_minutes} minute(s) before each eco window"
                )
            rationale_parts.append(
                "each eco window is now built around the most expensive moment of the next peak by adding the most expensive neighboring hours until the cooldown duration is covered, and it ends once the price drops below the daily average"
            )

        battery_strategy = "accu_uit"
        if planner_kind == PLANNER_KIND_THERMOSTAT:
            battery_strategy = "not_applicable"
        elif battery_enabled:
            if battery_soc_percent is None:
                rationale_parts.append("battery state of charge is unavailable, so battery control stays idle")
            else:
                battery_strategy = planned_current_mode
                if battery_strategy == "laden_met_zonne_energie":
                    score += 12
                    rationale_parts.append(
                        "battery is in the selected contiguous solar charge block built from the cheapest productive hours"
                    )
                elif battery_strategy == "laden_van_net":
                    score += 8
                    rationale_parts.append(
                        "grid charging is only used because the planned solar charge block is not enough to fill the usable battery capacity"
                    )
                elif battery_strategy == "ontladen":
                    score += 8
                    rationale_parts.append(
                        "battery discharges after solar production no longer covers the expected hourly demand"
                    )
                elif battery_strategy == "ontladen_naar_net":
                    score += 6
                    rationale_parts.append(
                        "battery has enough energy to cover expected own demand before the next charge block, so export is allowed but optional"
                    )
                elif battery_energy_available_for_discharge_kwh <= 0 and battery_energy_available_kwh > 0:
                    rationale_parts.append(
                        "battery keeps its remaining charge for household demand until the next charging opportunity"
                    )
                elif battery_total_energy_kwh <= minimum_battery_reserve_kwh and battery_soc_percent is not None:
                    rationale_parts.append(
                        f"battery stays above the configured minimum reserve of {battery_min_soc_percent:.0f}%"
                    )
                else:
                    rationale_parts.append(
                        "battery is idle because the current hour is outside the planned charge and discharge phases"
                    )

        planning_start = min((window.start for window in all_windows), default=now.replace(hour=0, minute=0, second=0, microsecond=0))
        planned_battery_mode_schedule = self._build_battery_mode_schedule(
            planning_start=planning_start,
            full_planned_mode_windows=full_planned_mode_windows,
        )

        rationale = ". ".join(rationale_parts) if rationale_parts else "planner inputs are balanced"
        planner_status = "ready_with_warnings" if source_errors else "ready"

        return PlannerResult(
            planner_kind=planner_kind,
            status=planner_status,
            score=max(0, min(100, score)),
            recommendation=recommendation,
            battery_strategy=battery_strategy,
            heat_pump_strategy=heat_pump_strategy,
            heating_estimate_kwh=heating_estimate_kwh,
            solar_forecast_kwh=solar_forecast_kwh,
            current_price=current_price,
            price_spread=price_spread,
            next_window_start=next_cheap.start.isoformat(),
            next_window_end=next_cheap.end.isoformat(),
            next_window_price=next_cheap.price,
            best_solar_window_start=best_solar_window.start.isoformat() if best_solar_window else None,
            best_solar_window_end=best_solar_window.end.isoformat() if best_solar_window else None,
            best_solar_window_kwh=best_solar_window.forecast_kwh if best_solar_window else None,
            solcast_confidence=solcast_confidence,
            lookback_daily_average_kwh=lookback_average_kwh,
            total_energy_daily_average_kwh=round(total_energy_daily_average_kwh, 2),
            non_heating_daily_average_kwh=round(non_heating_daily_average_kwh, 2),
            estimated_total_home_demand_kwh=estimated_total_home_demand_kwh,
            estimated_hourly_home_demand=estimated_hourly_home_demand,
            projected_remaining_solar_until_sunset_kwh=round(remaining_solar_until_sunset, 3),
            projected_remaining_home_demand_until_sunset_kwh=round(remaining_home_demand_until_sunset, 3),
            projected_solar_surplus_until_sunset_kwh=projected_solar_surplus_until_sunset,
            grid_charge_needed_until_sunset_kwh=grid_charge_needed_until_sunset,
            battery_charge_hours_needed_until_sunset=battery_charge_hours_needed_until_sunset,
            target_battery_full_by_sunset=target_battery_full_by_sunset,
            planned_grid_charge_windows=planned_grid_charge_windows,
            planned_solar_charge_windows=planned_solar_charge_windows,
            planned_battery_mode_schedule=planned_battery_mode_schedule,
            battery_soc_percent=battery_soc_percent,
            battery_min_soc_percent=battery_min_soc_percent,
            battery_total_energy_kwh=battery_total_energy_kwh,
            battery_energy_available_kwh=battery_energy_available_kwh,
            battery_remaining_capacity_kwh=battery_remaining_capacity_kwh,
            next_charge_opportunity_start=next_charge_opportunity.isoformat() if next_charge_opportunity else None,
            next_charge_window_start=cast(str | None, battery_cycle_summary["next_charge_window_start"]),
            next_charge_window_end=cast(str | None, battery_cycle_summary["next_charge_window_end"]),
            next_charge_window_hours=float(battery_cycle_summary["next_charge_window_hours"]),
            following_charge_window_start=cast(str | None, battery_cycle_summary["following_charge_window_start"]),
            following_charge_window_end=cast(str | None, battery_cycle_summary["following_charge_window_end"]),
            following_charge_window_hours=float(battery_cycle_summary["following_charge_window_hours"]),
            next_discharge_window_start=cast(str | None, battery_cycle_summary["next_discharge_window_start"]),
            next_discharge_window_end=cast(str | None, battery_cycle_summary["next_discharge_window_end"]),
            next_discharge_window_hours=float(battery_cycle_summary["next_discharge_window_hours"]),
            next_idle_window_start=cast(str | None, battery_cycle_summary["next_idle_window_start"]),
            current_relevant_battery_window_start=cast(
                str | None, battery_cycle_summary["current_relevant_battery_window_start"]
            ),
            current_relevant_battery_window_end=cast(
                str | None, battery_cycle_summary["current_relevant_battery_window_end"]
            ),
            current_relevant_battery_window_mode=cast(
                str | None, battery_cycle_summary["current_relevant_battery_window_mode"]
            ),
            current_relevant_battery_window_expected_demand_kwh=float(
                battery_cycle_summary["current_relevant_battery_window_expected_demand_kwh"]
            ),
            current_relevant_battery_window_expected_solar_kwh=float(
                battery_cycle_summary["current_relevant_battery_window_expected_solar_kwh"]
            ),
            home_demand_until_next_charge_kwh=home_demand_until_next_charge_kwh,
            battery_reserved_energy_kwh=battery_reserved_energy_kwh,
            battery_energy_available_for_discharge_kwh=battery_energy_available_for_discharge_kwh,
            battery_exportable_energy_kwh=battery_exportable_energy_kwh,
            battery_room_needed_for_solar_kwh=battery_room_needed_for_solar_kwh,
            battery_charge_hours_needed_total=battery_charge_hours_needed_total,
            battery_full_discharge_hours=battery_full_discharge_hours,
            battery_simulated_remaining_kwh_after_discharge=battery_simulated_remaining_kwh_after_discharge,
            next_high_price_window_start=next_high_price_window.start.isoformat() if next_high_price_window else None,
            next_high_price_window_price=next_high_price_window.price if next_high_price_window else None,
            room_temperature_c=room_temperature_c,
            thermostat_setpoint_c=thermostat_setpoint_c,
            thermostat_cool_setpoint_c=thermostat_cool_setpoint_c,
            thermostat_preheat_setpoint_c=thermostat_preheat_setpoint_c,
            thermostat_eco_setpoint_c=thermostat_eco_setpoint_c,
            room_cooling_hours_to_eco=room_cooling_hours_to_eco,
            room_cooling_rate_c_per_hour=room_cooling_rate_c_per_hour,
            cooling_reference_outdoor_temp_c=cooling_reference_outdoor_temp_c,
            planned_eco_window_start=eco_window["start"].isoformat() if eco_window else None,
            planned_eco_window_end=eco_window["end"].isoformat() if eco_window else None,
            planned_eco_windows=[
                {
                    "start": str(window["start"].isoformat()),
                    "end": str(window["end"].isoformat()),
                    "average_price": round(float(window["average_price"]), 6),
                }
                for window in eco_windows
            ],
            planned_preheat_window_start=preheat_window["start"].isoformat() if preheat_window else None,
            planned_preheat_window_end=preheat_window["end"].isoformat() if preheat_window else None,
            planned_preheat_windows=[
                {
                    "start": str(window["start"].isoformat()),
                    "end": str(window["end"].isoformat()),
                    "average_price": round(float(window["average_price"]), 6),
                }
                for window in preheat_windows
            ],
            battery_min_profit_per_kwh=battery_min_profit,
            price_resolution=price_resolution,
            source_status=source_status,
            source_errors=source_errors,
            rationale=rationale,
        )

    def _build_hourly_home_demand_forecast(
        self,
        *,
        non_heating_daily_average_kwh: float,
        heating_estimate_kwh: float,
        historical_hourly_usage: dict[datetime, float] | None = None,
        horizon_end: datetime | None = None,
    ) -> list[dict[str, str | float]]:
        """Build a simple hourly demand forecast for the visible planning horizon."""
        base_hourly = non_heating_daily_average_kwh / 24 if non_heating_daily_average_kwh > 0 else 0.0
        empty_slot_hourly = max(0.4, base_hourly)
        historical_hourly_usage = historical_hourly_usage or {}

        # Higher heating share during morning and evening hours.
        heating_profile = [
            0.035, 0.03, 0.03, 0.03, 0.035, 0.045,
            0.06, 0.07, 0.06, 0.045, 0.035, 0.03,
            0.025, 0.025, 0.025, 0.03, 0.04, 0.055,
            0.07, 0.075, 0.065, 0.05, 0.04, 0.035,
        ]
        profile_sum = sum(heating_profile) or 1.0
        now = dt_util.now()
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        horizon_end = horizon_end or (today_start + timedelta(days=1))
        day_count = max(1, (horizon_end.date() - today_start.date()).days + 1)

        hourly_average_by_hour: dict[int, float] = {}
        for hour in range(24):
            hour_values = [
                usage_kwh
                for slot_start, usage_kwh in historical_hourly_usage.items()
                if slot_start.hour == hour
            ]
            if hour_values:
                hourly_average_by_hour[hour] = statistics.fmean(hour_values)

        forecast: list[dict[str, str | float]] = []
        for day_offset in range(day_count):
            day_start = today_start + timedelta(days=day_offset)
            for hour in range(24):
                slot_start = day_start + timedelta(hours=hour)
                slot_end = slot_start + timedelta(hours=1)
                previous_week_usage = historical_hourly_usage.get(slot_start - timedelta(days=7))
                previous_week_previous_hour_usage = historical_hourly_usage.get(
                    slot_start - timedelta(days=7, hours=1)
                )
                hour_average = hourly_average_by_hour.get(hour)
                if previous_week_usage is not None and previous_week_previous_hour_usage is not None:
                    historical_hourly = previous_week_previous_hour_usage + (
                        (previous_week_usage - previous_week_previous_hour_usage) * 0.2
                    )
                elif previous_week_usage is not None:
                    historical_hourly = previous_week_usage
                elif hour_average is not None:
                    historical_hourly = hour_average
                else:
                    historical_hourly = empty_slot_hourly
                historical_hourly = min(3.0, max(0.0, historical_hourly))
                heating_hourly = heating_estimate_kwh * (heating_profile[hour] / profile_sum)
                total_hourly = round(max(0.0, historical_hourly) + heating_hourly, 3)
                forecast.append(
                    {
                        "start": slot_start.isoformat(),
                        "end": slot_end.isoformat(),
                        "estimated_kwh": total_hourly,
                    }
                )

        return forecast

    def _get_solar_day_end(self, solar_windows: list[SolarWindow]) -> datetime | None:
        productive_windows = [window for window in solar_windows if window.forecast_kwh > 0]
        if not productive_windows:
            return None
        return max(window.end for window in productive_windows)

    def _build_fallback_solar_windows(self, daily_forecast_kwh: float) -> list[SolarWindow]:
        """Approximate hourly solar windows when only the daily forecast total is available."""
        return self._build_fallback_solar_windows_for_day(daily_forecast_kwh, day_offset=0)

    def _build_fallback_solar_windows_for_day(
        self,
        daily_forecast_kwh: float,
        *,
        day_offset: int,
    ) -> list[SolarWindow]:
        """Approximate hourly solar windows when only the daily forecast total is available."""
        if daily_forecast_kwh <= 0:
            return []

        now = dt_util.now()
        day_start = (now + timedelta(days=day_offset)).replace(hour=0, minute=0, second=0, microsecond=0)
        hourly_weights = [
            0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
            0.0, 0.02, 0.05, 0.09, 0.13, 0.16,
            0.17, 0.15, 0.11, 0.07, 0.04, 0.01,
            0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
        ]
        weight_sum = sum(hourly_weights) or 1.0
        windows: list[SolarWindow] = []

        for hour, weight in enumerate(hourly_weights):
            if weight <= 0:
                continue
            start = day_start + timedelta(hours=hour)
            end = start + timedelta(hours=1)
            if day_offset == 0 and end <= now:
                continue
            forecast_kwh = round(daily_forecast_kwh * (weight / weight_sum), 3)
            windows.append(
                SolarWindow(
                    start=start,
                    end=end,
                    forecast_kwh=forecast_kwh,
                    forecast_kwh_p10=None,
                    forecast_kwh_p90=None,
                )
            )

        return windows

    def _sum_remaining_solar_until(
        self,
        solar_windows: list[SolarWindow],
        now: datetime,
        until: datetime | None,
    ) -> float:
        if until is None:
            return 0.0
        total = 0.0
        for window in solar_windows:
            overlap_hours = self._overlap_hours(window.start, window.end, now, until)
            if overlap_hours <= 0:
                continue
            window_hours = max((window.end - window.start).total_seconds() / 3600, 0.0001)
            total += window.forecast_kwh * (overlap_hours / window_hours)
        return total

    def _sum_remaining_home_demand_until(
        self,
        hourly_demand: list[dict[str, str | float]],
        now: datetime,
        until: datetime | None,
    ) -> float:
        if until is None:
            return 0.0
        total = 0.0
        for slot in hourly_demand:
            start = dt_util.parse_datetime(str(slot.get("start")))
            end = dt_util.parse_datetime(str(slot.get("end")))
            estimated_kwh = _coerce_float(slot.get("estimated_kwh"), default=0.0) or 0.0
            if start is None or end is None:
                continue
            overlap_hours = self._overlap_hours(start, end, now, until)
            if overlap_hours <= 0:
                continue
            slot_hours = max((end - start).total_seconds() / 3600, 0.0001)
            total += estimated_kwh * (overlap_hours / slot_hours)
        return total

    def _remaining_day_solar_covers_demand(
        self,
        *,
        slots: list[dict[str, Any]],
        start: datetime,
    ) -> bool:
        day_end = (start + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        remaining_solar_kwh = 0.0
        remaining_demand_kwh = 0.0

        for slot in slots:
            slot_start = cast(datetime, slot["start"])
            slot_end = cast(datetime, slot["end"])
            overlap_hours = self._overlap_hours(slot_start, slot_end, start, day_end)
            if overlap_hours <= 0:
                continue
            slot_hours = max((slot_end - slot_start).total_seconds() / 3600, 0.0001)
            remaining_solar_kwh += float(slot["solar_kwh"]) * (overlap_hours / slot_hours)
            remaining_demand_kwh += float(slot["demand_kwh"]) * (overlap_hours / slot_hours)

        return remaining_solar_kwh >= remaining_demand_kwh - 1e-6

    def _build_energy_balance_slots(
        self,
        *,
        price_windows: list[PlannerWindow],
        export_price_windows: list[PlannerWindow],
        solar_windows: list[SolarWindow],
        hourly_demand: list[dict[str, str | float]],
        horizon_start: datetime,
    ) -> list[dict[str, Any]]:
        slots: list[dict[str, Any]] = []
        for window in price_windows:
            if window.end <= horizon_start:
                continue
            slot_hours = max((window.end - window.start).total_seconds() / 3600, 0.0001)
            demand_kwh = 0.0
            for demand_slot in hourly_demand:
                demand_start = dt_util.parse_datetime(str(demand_slot.get("start")))
                demand_end = dt_util.parse_datetime(str(demand_slot.get("end")))
                estimated_kwh = _coerce_float(demand_slot.get("estimated_kwh"), default=0.0) or 0.0
                if demand_start is None or demand_end is None:
                    continue
                overlap_hours = self._overlap_hours(window.start, window.end, demand_start, demand_end)
                if overlap_hours <= 0:
                    continue
                demand_slot_hours = max((demand_end - demand_start).total_seconds() / 3600, 0.0001)
                demand_kwh += estimated_kwh * (overlap_hours / demand_slot_hours)

            solar_kwh = 0.0
            for solar_window in solar_windows:
                overlap_hours = self._overlap_hours(window.start, window.end, solar_window.start, solar_window.end)
                if overlap_hours <= 0:
                    continue
                solar_slot_hours = max((solar_window.end - solar_window.start).total_seconds() / 3600, 0.0001)
                solar_kwh += float(solar_window.forecast_kwh) * (overlap_hours / solar_slot_hours)
            net_solar_kwh = round(solar_kwh - demand_kwh, 3)
            export_price = self._match_window_price(
                start=window.start,
                end=window.end,
                windows=export_price_windows,
                default=window.price,
            )
            slots.append(
                {
                    "start": window.start,
                    "end": window.end,
                    "price": window.price,
                    "import_price": window.price,
                    "export_price": export_price,
                    "hours": slot_hours,
                    "solar_kwh": round(solar_kwh, 3),
                    "demand_kwh": round(demand_kwh, 3),
                    "net_solar_kwh": net_solar_kwh,
                }
            )

        return slots

    def _match_window_price(
        self,
        *,
        start: datetime,
        end: datetime,
        windows: list[PlannerWindow],
        default: float,
    ) -> float:
        weighted_price = 0.0
        weighted_hours = 0.0
        for window in windows:
            overlap_hours = self._overlap_hours(start, end, window.start, window.end)
            if overlap_hours <= 0:
                continue
            weighted_price += float(window.price) * overlap_hours
            weighted_hours += overlap_hours

        if weighted_hours <= 0:
            return round(default, 6)
        return round(weighted_price / weighted_hours, 6)

    def _build_battery_switch_windows(
        self,
        *,
        attributes: dict[str, Any],
        current_price: float | None,
        price_resolution: str,
        include_past: bool = False,
    ) -> list[PlannerWindow]:
        raw_windows = self._extract_price_windows(
            attributes,
            current_price,
            "__battery_switch__",
            include_past=include_past,
        )
        if not raw_windows:
            return []
        if price_resolution != PRICE_RESOLUTION_HOURLY:
            return raw_windows

        hourly_windows = self._aggregate_price_windows_to_hourly(raw_windows)
        hourly_price_by_start = {
            window.start: window.price
            for window in hourly_windows
        }
        switch_windows: list[PlannerWindow] = []
        for window in raw_windows:
            hour_start = window.start.replace(minute=0, second=0, microsecond=0)
            switch_windows.append(
                PlannerWindow(
                    start=window.start,
                    end=window.end,
                    price=hourly_price_by_start.get(hour_start, window.price),
                )
            )
        return switch_windows

    def _select_contiguous_charge_block(
        self,
        *,
        slots: list[dict[str, Any]],
        target_kwh: float,
        max_charge_kw: float,
    ) -> list[dict[str, Any]]:
        if target_kwh <= 0 or max_charge_kw <= 0:
            return []

        candidates = []
        for slot in slots:
            charge_potential_kwh = min(max_charge_kw * float(slot["hours"]), max(0.0, float(slot["net_solar_kwh"])))
            if charge_potential_kwh <= 0:
                continue
            candidates.append({**slot, "charge_potential_kwh": round(charge_potential_kwh, 3)})

        if not candidates:
            return []

        best_block: list[dict[str, Any]] = []
        best_meets_target = False
        best_average_price = float("inf")
        best_charge_kwh = 0.0

        for start_index in range(len(candidates)):
            block: list[dict[str, Any]] = []
            total_charge_kwh = 0.0
            weighted_price = 0.0
            previous_end = None

            for slot in candidates[start_index:]:
                if previous_end is not None and slot["start"] != previous_end:
                    break
                block.append(slot)
                total_charge_kwh += float(slot["charge_potential_kwh"])
                weighted_price += float(slot["export_price"]) * float(slot["charge_potential_kwh"])
                previous_end = slot["end"]
                if total_charge_kwh >= target_kwh:
                    break

            if not block or total_charge_kwh <= 0:
                continue

            meets_target = total_charge_kwh >= target_kwh
            average_price = weighted_price / total_charge_kwh
            if (
                not best_block
                or (meets_target and not best_meets_target)
                or (
                    meets_target == best_meets_target
                    and (
                        average_price < best_average_price
                        or (
                            abs(average_price - best_average_price) < 1e-9
                            and total_charge_kwh > best_charge_kwh
                        )
                    )
                )
            ):
                best_block = block
                best_meets_target = meets_target
                best_average_price = average_price
                best_charge_kwh = total_charge_kwh

        return best_block

    def _plan_charge_windows_for_horizon(
        self,
        *,
        slots: list[dict[str, Any]],
        now: datetime,
        usable_capacity_kwh: float,
        current_remaining_capacity_kwh: float,
        max_charge_kw: float,
        battery_min_profit: float,
    ) -> tuple[list[dict[str, str | float]], list[dict[str, str | float]]]:
        planned_solar_charge_windows: list[dict[str, str | float]] = []
        planned_grid_charge_windows: list[dict[str, str | float]] = []
        future_slots = [slot for slot in slots if slot["end"] > now]
        if not future_slots:
            return planned_solar_charge_windows, planned_grid_charge_windows

        # `current_remaining_capacity_kwh` is the amount of empty space left in
        # the usable battery capacity (so it is already <= usable_capacity_kwh).
        # Clamp defensively in case the caller ever passes a larger value.
        target_charge_kwh = max(0.0, min(usable_capacity_kwh, current_remaining_capacity_kwh))

        has_export_price_sensor = bool(self._config.get(CONF_EXPORT_PRICE_SENSOR))
        productive_solar_slot_starts = self._select_contiguous_productive_solar_slot_starts(
            slots=future_slots,
            max_charge_kw=max_charge_kw,
            minimum_slots=2,
        )

        # Increase the charge target to account for energy that will be discharged
        # between now and the first productive solar slot.  Without this a battery
        # at 96 % would plan only a 12-minute solar window even though overnight
        # discharge will leave it nearly empty by morning.
        first_productive_solar_start = (
            min(productive_solar_slot_starts) if productive_solar_slot_starts else None
        )
        if first_productive_solar_start is not None:
            current_available_kwh = usable_capacity_kwh - current_remaining_capacity_kwh
            pre_solar_discharge_kwh = 0.0
            for slot in future_slots:
                if slot["start"] >= first_productive_solar_start:
                    break
                net_solar = float(slot.get("net_solar_kwh", 0.0))
                if net_solar < 0:
                    deficit = min(-net_solar, current_available_kwh - pre_solar_discharge_kwh)
                    if deficit > 0:
                        pre_solar_discharge_kwh += deficit
            target_charge_kwh = min(
                usable_capacity_kwh,
                current_remaining_capacity_kwh + pre_solar_discharge_kwh,
            )

        # Skip planning for trivial remaining capacity (< 100 Wh).  A tiny rounding
        # remainder would otherwise produce a charge window that hold_solar_charge_mode
        # then stretches over the full 4-hour solar block even though the battery is
        # effectively full.
        if target_charge_kwh < 0.1:
            return planned_solar_charge_windows, planned_grid_charge_windows
        total_future_solar_capacity_kwh = round(
            sum(
                min(
                    max_charge_kw * float(slot["hours"]),
                    max(0.0, float(slot["net_solar_kwh"])),
                )
                for slot in future_slots
            ),
            6,
        )
        # Grid charging should be limited to what solar cannot deliver BEFORE the first
        # significant discharge window.  Comparing against all future solar is wrong
        # when discharge comes before the solar: the battery needs to be full for
        # tonight's discharge even if tomorrow's solar would eventually fill the gap.
        first_discharge_slot_start = next(
            (slot["start"] for slot in future_slots if float(slot.get("net_solar_kwh", 0)) < -0.05),
            None,
        )
        solar_before_discharge_kwh = round(
            sum(
                min(max_charge_kw * float(slot["hours"]), max(0.0, float(slot["net_solar_kwh"])))
                for slot in future_slots
                if first_discharge_slot_start is None or slot["start"] < first_discharge_slot_start
            ),
            6,
        )
        grid_charge_limit_kwh = max(0.0, round(target_charge_kwh - solar_before_discharge_kwh, 6))
        selected_solar_charge_by_start: dict[datetime, float] = {}
        selected_grid_charge_by_start: dict[datetime, float] = {}
        charge_candidates: list[dict[str, Any]] = []

        for slot in future_slots:
            slot_capacity_kwh = max_charge_kw * float(slot["hours"])
            solar_charge_kwh = min(
                slot_capacity_kwh,
                max(0.0, float(slot["net_solar_kwh"])),
            )
            if solar_charge_kwh > 0 and slot["start"] in productive_solar_slot_starts:
                charge_candidates.append(
                    {
                        "kind": "solar",
                        "start": slot["start"],
                        "end": slot["end"],
                        "charge_kwh": round(solar_charge_kwh, 6),
                        "net_solar_kwh": round(float(slot["net_solar_kwh"]), 6),
                        "effective_price": round(
                            float(slot["export_price"])
                            if has_export_price_sensor
                            else float(slot["import_price"]) - 0.15,
                            6,
                        ),
                    }
                )
                # Solar slots are never grid-charged — skip grid candidate for this slot.
                continue

            if grid_charge_limit_kwh <= 0 or (
                next_peak_price := self._calculate_next_battery_peak_price(
                    future_slots,
                    slot["end"],
                    price_key="import_price",
                )
            ) is None or next_peak_price - float(slot["import_price"]) < battery_min_profit:
                continue

            grid_charge_kwh = slot_capacity_kwh
            if grid_charge_kwh <= 0:
                continue
            charge_candidates.append(
                {
                    "kind": "grid",
                    "start": slot["start"],
                    "end": slot["end"],
                    "charge_kwh": round(min(grid_charge_kwh, grid_charge_limit_kwh), 6),
                    "effective_price": round(float(slot["import_price"]), 6),
                }
            )

        charged_kwh = 0.0
        charged_grid_kwh = 0.0
        for candidate in sorted(
            charge_candidates,
            key=lambda item: (
                # Solar always before grid; within each group sort independently.
                0 if item["kind"] == "solar" else 1,
                # Solar: highest raw production (net_solar_kwh) first — picks solar noon
                # before low-yield morning/evening slots, even when prices differ.
                # Grid: cheapest slot first.
                -float(item.get("net_solar_kwh", item["charge_kwh"])) if item["kind"] == "solar"
                else float(item["effective_price"]),
                item["start"],
            ),
        ):
            if charged_kwh >= target_charge_kwh:
                break

            candidate_charge_kwh = float(candidate["charge_kwh"])
            if candidate["kind"] == "grid":
                candidate_charge_kwh = min(
                    candidate_charge_kwh,
                    max(0.0, grid_charge_limit_kwh - charged_grid_kwh),
                )
            usable_charge_kwh = min(candidate_charge_kwh, target_charge_kwh - charged_kwh)
            if usable_charge_kwh <= 0:
                continue

            if candidate["kind"] == "solar":
                selected_solar_charge_by_start[candidate["start"]] = round(
                    selected_solar_charge_by_start.get(candidate["start"], 0.0) + usable_charge_kwh,
                    6,
                )
            else:
                selected_grid_charge_by_start[candidate["start"]] = round(
                    selected_grid_charge_by_start.get(candidate["start"], 0.0) + usable_charge_kwh,
                    6,
                )
                charged_grid_kwh += usable_charge_kwh
            charged_kwh += usable_charge_kwh

        for slot in future_slots:
            charge_kwh = float(selected_solar_charge_by_start.get(slot["start"], 0.0))
            if charge_kwh <= 0:
                continue
            planned_solar_charge_windows.append(
                {
                    "start": slot["start"].isoformat(),
                    "end": slot["end"].isoformat(),
                    "price": round(float(slot["export_price"]), 6),
                    "usable_hours": round(charge_kwh / max_charge_kw, 3),
                }
            )

        # Extend the solar charge window to cover until the first discharge slot that
        # comes AFTER the last selected solar slot, so the battery stays in laden mode
        # rather than going idle between the planned peak charge window and the start
        # of the evening discharge.  The global first_discharge_slot_start may be
        # today's evening (before tomorrow's solar), so we compute the cutoff relative
        # to last_selected_solar_end instead.
        if planned_solar_charge_windows:
            last_selected_solar_end = max(
                (
                    slot["end"]
                    for slot in future_slots
                    if slot["start"] in selected_solar_charge_by_start
                ),
                default=None,
            )
            if last_selected_solar_end is not None:
                # First slot after the selected window where solar goes negative (evening).
                post_solar_discharge_start = next(
                    (
                        slot["start"]
                        for slot in future_slots
                        if slot["start"] >= last_selected_solar_end
                        and float(slot.get("net_solar_kwh", 0)) < -0.05
                    ),
                    None,
                )
                if post_solar_discharge_start is not None:
                    for slot in future_slots:
                        if slot["start"] < last_selected_solar_end:
                            continue
                        if slot["start"] >= post_solar_discharge_start:
                            break
                        if slot["start"] not in productive_solar_slot_starts:
                            continue
                        extension_charge_kwh = min(
                            max_charge_kw * float(slot["hours"]),
                            max(0.0, float(slot["net_solar_kwh"])),
                        )
                        if extension_charge_kwh <= 0:
                            continue
                        planned_solar_charge_windows.append(
                            {
                                "start": slot["start"].isoformat(),
                                "end": slot["end"].isoformat(),
                                "price": round(
                                    float(slot["export_price"])
                                    if has_export_price_sensor
                                    else float(slot["import_price"]) - 0.15,
                                    6,
                                ),
                                "usable_hours": round(extension_charge_kwh / max_charge_kw, 3),
                            }
                        )

        for slot in future_slots:
            slot_charge_kwh = float(selected_grid_charge_by_start.get(slot["start"], 0.0))
            if slot_charge_kwh <= 0:
                continue
            planned_grid_charge_windows.append(
                {
                    "start": slot["start"].isoformat(),
                    "end": slot["end"].isoformat(),
                    "price": round(float(slot["import_price"]), 6),
                    "usable_hours": round(slot_charge_kwh / max_charge_kw, 3),
                }
            )

        return (
            self._merge_planned_windows(planned_solar_charge_windows),
            self._merge_planned_windows(planned_grid_charge_windows),
        )

    def _select_contiguous_productive_solar_slot_starts(
        self,
        *,
        slots: list[dict[str, Any]],
        max_charge_kw: float,
        minimum_slots: int,
    ) -> set[datetime]:
        if minimum_slots <= 1:
            return {
                cast(datetime, slot["start"])
                for slot in slots
                if min(max_charge_kw * float(slot["hours"]), max(0.0, float(slot["net_solar_kwh"]))) > 0
            }

        productive_starts: set[datetime] = set()
        current_run: list[dict[str, Any]] = []
        previous_end: datetime | None = None

        for slot in slots:
            charge_potential_kwh = min(
                max_charge_kw * float(slot["hours"]),
                max(0.0, float(slot["net_solar_kwh"])),
            )
            slot_start = cast(datetime, slot["start"])
            slot_end = cast(datetime, slot["end"])
            if charge_potential_kwh <= 0:
                if len(current_run) >= minimum_slots:
                    productive_starts.update(cast(datetime, run_slot["start"]) for run_slot in current_run)
                current_run = []
                previous_end = None
                continue

            if previous_end is not None and slot_start != previous_end:
                if len(current_run) >= minimum_slots:
                    productive_starts.update(cast(datetime, run_slot["start"]) for run_slot in current_run)
                current_run = []

            current_run.append(slot)
            previous_end = slot_end

        if len(current_run) >= minimum_slots:
            productive_starts.update(cast(datetime, run_slot["start"]) for run_slot in current_run)

        return productive_starts

    def _calculate_next_battery_peak_price(
        self,
        slots: list[dict[str, Any]],
        after: datetime,
        *,
        price_key: str = "price",
    ) -> float | None:
        trailing_slots = [slot for slot in slots if slot["start"] >= after]
        if len(trailing_slots) < 2:
            return None

        prices = [float(slot[price_key]) for slot in trailing_slots]
        index = 0
        while index + 1 < len(prices) and prices[index + 1] <= prices[index]:
            index += 1

        peak_max = prices[index]
        while index + 1 < len(prices) and prices[index + 1] >= prices[index]:
            index += 1
            peak_max = max(peak_max, prices[index])

        return peak_max

    def _build_charge_window_lookup(
        self,
        windows: list[dict[str, str | float]],
        *,
        max_charge_kw: float,
    ) -> dict[datetime, dict[str, float | datetime]]:
        lookup: dict[datetime, dict[str, float | datetime]] = {}
        for window in windows:
            parsed_start = dt_util.parse_datetime(str(window["start"]))
            parsed_end = dt_util.parse_datetime(str(window["end"]))
            if parsed_start is None or parsed_end is None:
                continue
            usable_hours = float(window.get("usable_hours", 0.0))
            lookup[parsed_start] = {
                "end": parsed_end,
                "usable_hours": usable_hours,
                "charge_kwh": round(max(0.0, usable_hours * max_charge_kw), 6),
            }
        return lookup

    def _find_next_high_price_window(
        self,
        *,
        future_windows: list[PlannerWindow],
        average_price: float,
    ) -> PlannerWindow | None:
        if not future_windows:
            return None

        highest_future_price = max((window.price for window in future_windows), default=None)
        if highest_future_price is None or highest_future_price <= average_price:
            return None

        return min(
            (window for window in future_windows if window.price >= highest_future_price - 1e-9),
            key=lambda window: window.start,
            default=None,
        )

    def _battery_mode_family(self, mode: str) -> str:
        if mode in ("laden_met_zonne_energie", "laden_van_net"):
            return "laden"
        if mode in ("ontladen", "ontladen_naar_net"):
            return "ontladen"
        return "accu_uit"

    def _build_battery_cycle_windows(
        self,
        windows: list[dict[str, str | float]],
    ) -> list[dict[str, str | datetime | float]]:
        cycles: list[dict[str, str | datetime | float]] = []
        for window in sorted(windows, key=lambda item: str(item["start"])):
            start = dt_util.parse_datetime(str(window["start"]))
            end = dt_util.parse_datetime(str(window["end"]))
            if start is None or end is None:
                continue

            family = self._battery_mode_family(str(window.get("mode", "accu_uit")))
            if (
                cycles
                and cast(datetime, cycles[-1]["end"]) == start
                and str(cycles[-1]["family"]) == family
            ):
                cycles[-1]["end"] = end
                cycles[-1]["usable_hours"] = round(
                    float(cycles[-1].get("usable_hours", 0.0)) + float(window.get("usable_hours", 0.0)),
                    3,
                )
                continue

            cycles.append(
                {
                    "start": start,
                    "end": end,
                    "family": family,
                    "usable_hours": round(float(window.get("usable_hours", 0.0)), 3),
                }
            )

        return cycles

    def _sum_slot_metric_in_window(
        self,
        *,
        slots: list[dict[str, Any]],
        start: datetime,
        end: datetime,
        metric_key: str,
    ) -> float:
        total = 0.0
        for slot in slots:
            slot_start = cast(datetime, slot["start"])
            slot_end = cast(datetime, slot["end"])
            overlap_hours = self._overlap_hours(slot_start, slot_end, start, end)
            if overlap_hours <= 0:
                continue
            slot_hours = max((slot_end - slot_start).total_seconds() / 3600, 0.0001)
            total += float(slot.get(metric_key, 0.0)) * (overlap_hours / slot_hours)
        return round(total, 3)

    def _select_relevant_battery_cycle(
        self,
        *,
        cycle_windows: list[dict[str, str | datetime | float]],
        now: datetime,
    ) -> dict[str, str | datetime | float] | None:
        current_cycle = next(
            (
                cycle
                for cycle in cycle_windows
                if cast(datetime, cycle["start"]) <= now < cast(datetime, cycle["end"])
            ),
            None,
        )
        if current_cycle is not None and str(current_cycle["family"]) != "accu_uit":
            return current_cycle

        next_relevant_cycle = next(
            (
                cycle
                for cycle in cycle_windows
                if cast(datetime, cycle["start"]) > now and str(cycle["family"]) != "accu_uit"
            ),
            None,
        )
        return next_relevant_cycle or current_cycle

    def _find_battery_cycle(
        self,
        *,
        cycle_windows: list[dict[str, str | datetime | float]],
        now: datetime,
        family: str,
    ) -> dict[str, str | datetime | float] | None:
        return next(
            (
                cycle
                for cycle in cycle_windows
                if str(cycle["family"]) == family
                and cast(datetime, cycle["end"]) > now
            ),
            None,
        )

    def _find_next_idle_start(
        self,
        *,
        cycle_windows: list[dict[str, str | datetime | float]],
        now: datetime,
    ) -> datetime | None:
        next_idle_cycle = next(
            (
                cycle
                for cycle in cycle_windows
                if str(cycle["family"]) == "accu_uit" and cast(datetime, cycle["start"]) > now
            ),
            None,
        )
        if next_idle_cycle is None:
            return None
        return cast(datetime, next_idle_cycle["start"])

    def _summarize_battery_cycles(
        self,
        *,
        full_planned_mode_windows: list[dict[str, str | float]],
        energy_balance_slots: list[dict[str, Any]],
        now: datetime,
    ) -> dict[str, str | float | None]:
        cycle_windows = self._build_battery_cycle_windows(full_planned_mode_windows)
        relevant_cycle = self._select_relevant_battery_cycle(cycle_windows=cycle_windows, now=now)
        next_charge_cycle = self._find_battery_cycle(cycle_windows=cycle_windows, now=now, family="laden")
        following_charge_cycle = next(
            (
                cycle
                for cycle in cycle_windows
                if str(cycle["family"]) == "laden"
                and next_charge_cycle is not None
                and cast(datetime, cycle["start"]) >= cast(datetime, next_charge_cycle["end"])
            ),
            None,
        )
        next_discharge_cycle = self._find_battery_cycle(cycle_windows=cycle_windows, now=now, family="ontladen")
        next_idle_start = self._find_next_idle_start(cycle_windows=cycle_windows, now=now)

        relevant_start = cast(datetime, relevant_cycle["start"]) if relevant_cycle is not None else None
        relevant_end = cast(datetime, relevant_cycle["end"]) if relevant_cycle is not None else None

        return {
            "current_relevant_battery_window_start": relevant_start.isoformat() if relevant_start else None,
            "current_relevant_battery_window_end": relevant_end.isoformat() if relevant_end else None,
            "current_relevant_battery_window_mode": (
                str(relevant_cycle["family"]) if relevant_cycle is not None else None
            ),
            "current_relevant_battery_window_expected_demand_kwh": (
                self._sum_slot_metric_in_window(
                    slots=energy_balance_slots,
                    start=relevant_start,
                    end=relevant_end,
                    metric_key="demand_kwh",
                )
                if relevant_start is not None and relevant_end is not None
                else 0.0
            ),
            "current_relevant_battery_window_expected_solar_kwh": (
                self._sum_slot_metric_in_window(
                    slots=energy_balance_slots,
                    start=relevant_start,
                    end=relevant_end,
                    metric_key="solar_kwh",
                )
                if relevant_start is not None and relevant_end is not None
                else 0.0
            ),
            "next_charge_window_start": (
                cast(datetime, next_charge_cycle["start"]).isoformat() if next_charge_cycle is not None else None
            ),
            "next_charge_window_end": (
                cast(datetime, next_charge_cycle["end"]).isoformat() if next_charge_cycle is not None else None
            ),
            "next_charge_window_hours": (
                round(float(next_charge_cycle.get("usable_hours", 0.0)), 3) if next_charge_cycle is not None else 0.0
            ),
            "following_charge_window_start": (
                cast(datetime, following_charge_cycle["start"]).isoformat()
                if following_charge_cycle is not None
                else None
            ),
            "following_charge_window_end": (
                cast(datetime, following_charge_cycle["end"]).isoformat()
                if following_charge_cycle is not None
                else None
            ),
            "following_charge_window_hours": (
                round(float(following_charge_cycle.get("usable_hours", 0.0)), 3)
                if following_charge_cycle is not None
                else 0.0
            ),
            "next_discharge_window_start": (
                cast(datetime, next_discharge_cycle["start"]).isoformat()
                if next_discharge_cycle is not None
                else None
            ),
            "next_discharge_window_end": (
                cast(datetime, next_discharge_cycle["end"]).isoformat()
                if next_discharge_cycle is not None
                else None
            ),
            "next_discharge_window_hours": (
                round(float(next_discharge_cycle.get("usable_hours", 0.0)), 3)
                if next_discharge_cycle is not None
                else 0.0
            ),
            "next_idle_window_start": next_idle_start.isoformat() if next_idle_start is not None else None,
        }

    def _resolve_charge_phase_bounds(
        self,
        *,
        charge_windows: list[dict[str, float | datetime]],
        now: datetime,
    ) -> tuple[list[dict[str, datetime]], str]:
        cluster_gap = timedelta(minutes=90)
        normalized_windows = sorted(
            [
                {
                    "start": cast(datetime, window["start"]),
                    "end": cast(datetime, window["end"]),
                }
                for window in charge_windows
                if isinstance(window.get("start"), datetime) and isinstance(window.get("end"), datetime)
            ],
            key=lambda window: window["start"],
        )
        active_charge_phase_end = self._active_charge_phase_end
        active_charge_phase_mode = self._active_charge_phase_mode
        if active_charge_phase_end is not None and active_charge_phase_end <= now:
            active_charge_phase_end = None
            active_charge_phase_mode = "accu_uit"
            self._active_charge_phase_end = None
            self._active_charge_phase_mode = "accu_uit"
        if (
            active_charge_phase_end is not None
            and active_charge_phase_end > now
            and any(w["start"] <= now < w["end"] for w in normalized_windows)
        ):
            normalized_windows.append({"start": now, "end": active_charge_phase_end})
            normalized_windows.sort(key=lambda window: window["start"])
        else:
            # Stale persisted window is not relevant; discard its mode so it cannot
            # bleed into charge_phase_mode and label future slots (e.g. night grid
            # charge windows) with the wrong mode (e.g. laden_met_zonne_energie).
            active_charge_phase_mode = "accu_uit"

        clusters: list[dict[str, datetime]] = []
        for window in normalized_windows:
            if not clusters or window["start"] > clusters[-1]["end"] + cluster_gap:
                clusters.append(dict(window))
                continue
            clusters[-1]["end"] = max(clusters[-1]["end"], window["end"])

        return clusters, active_charge_phase_mode

    def _append_charge_window_mode(
        self,
        *,
        hourly_modes: list[dict[str, str | float]],
        slot: dict[str, Any],
        charge_window: dict[str, float | datetime],
        mode: str,
        price_key: str,
        now: datetime,
        current_mode: str,
        sim_usable_energy_kwh: float,
        usable_capacity_kwh: float,
    ) -> tuple[str, float, datetime]:
        charge_end = cast(datetime, charge_window["end"])
        charge_kwh = float(charge_window["charge_kwh"])
        usable_hours = float(charge_window["usable_hours"])
        # After a planned charge window, follow-on discharge planning should assume
        # the battery is effectively full instead of carrying a partial estimate.
        # `charge_kwh` is intentionally unused in the simulated SOC update because we
        # optimistically treat the battery as full after the entire charge phase.
        _ = charge_kwh
        sim_usable_energy_kwh = usable_capacity_kwh
        if slot["start"] <= now < charge_end:
            current_mode = mode
        hourly_modes.append(
            {
                "start": slot["start"].isoformat(),
                "end": charge_end.isoformat(),
                "price": round(float(slot[price_key]), 6),
                "usable_hours": round(usable_hours, 3),
                "mode": mode,
            }
        )
        return current_mode, sim_usable_energy_kwh, charge_end

    def _resolve_charge_phase_mode(
        self,
        *,
        last_charge_mode: str,
        active_charge_phase_mode: str,
    ) -> str:
        if last_charge_mode != "accu_uit":
            return last_charge_mode
        if active_charge_phase_mode != "accu_uit":
            return active_charge_phase_mode
        return "accu_uit"

    def _update_active_charge_phase_state(
        self,
        *,
        now: datetime,
        active_charge_phase: dict[str, datetime] | None,
        current_mode: str,
        last_charge_mode: str,
        active_charge_phase_mode: str,
    ) -> None:
        if active_charge_phase is not None and active_charge_phase["start"] <= now < active_charge_phase["end"]:
            self._active_charge_phase_end = active_charge_phase["end"]
            self._active_charge_phase_mode = (
                current_mode
                if current_mode in ("laden_met_zonne_energie", "laden_van_net")
                else (
                    last_charge_mode
                    if last_charge_mode in ("laden_met_zonne_energie", "laden_van_net")
                    else active_charge_phase_mode
                )
            )
            return

        self._active_charge_phase_end = None
        self._active_charge_phase_mode = "accu_uit"

    def _build_battery_mode_schedule(
        self,
        *,
        planning_start: datetime,
        full_planned_mode_windows: list[dict[str, str | float]],
    ) -> list[dict[str, str]]:
        schedule = [{"at": planning_start.isoformat(), "mode": "accu_uit"}]
        schedule.extend(
            {"at": str(window["start"]), "mode": str(window.get("mode", "accu_uit"))}
            for window in full_planned_mode_windows
        )

        deduped_schedule: list[dict[str, str]] = []
        for item in sorted(schedule, key=lambda entry: entry["at"]):
            if deduped_schedule and deduped_schedule[-1]["at"] == item["at"]:
                deduped_schedule[-1] = item
                continue
            if deduped_schedule and deduped_schedule[-1]["mode"] == item["mode"]:
                continue
            deduped_schedule.append(item)
        return deduped_schedule

    def _build_mode_windows_from_hourly_plan(
        self,
        *,
        slots: list[dict[str, Any]],
        now: datetime,
        planned_solar_charge_windows: list[dict[str, str | float]],
        planned_grid_charge_windows: list[dict[str, str | float]],
        initial_usable_energy_kwh: float,
        minimum_energy_before_next_charge_kwh: float,
        minimum_energy_for_export_before_next_charge_kwh: float,
        usable_capacity_kwh: float,
        average_price: float,
        average_export_price: float,
        max_charge_kw: float,
        max_discharge_kw: float,
    ) -> tuple[list[dict[str, str | float]], str]:
        solar_charge_starts = self._build_charge_window_lookup(
            planned_solar_charge_windows,
            max_charge_kw=max_charge_kw,
        )
        grid_charge_starts = self._build_charge_window_lookup(
            planned_grid_charge_windows,
            max_charge_kw=max_charge_kw,
        )

        charge_starts = {
            **solar_charge_starts,
            **grid_charge_starts,
        }
        charge_windows = [
            {"start": start, **window}
            for start, window in charge_starts.items()
        ]
        charge_phase_clusters, active_charge_phase_mode = self._resolve_charge_phase_bounds(
            charge_windows=charge_windows,
            now=now,
        )
        first_charge_phase_start = charge_phase_clusters[0]["start"] if charge_phase_clusters else None
        active_charge_phase = next(
            (
                cluster
                for cluster in charge_phase_clusters
                if cluster["start"] <= now < cluster["end"]
            ),
            None,
        )

        sim_usable_energy_kwh = max(0.0, initial_usable_energy_kwh)
        hourly_modes: list[dict[str, str | float]] = []
        current_mode = "accu_uit"
        last_charge_mode = "accu_uit"

        slot_index = 0
        while slot_index < len(slots):
            slot = slots[slot_index]
            slot_start = slot["start"]
            if slot_start in solar_charge_starts:
                mode = "laden_met_zonne_energie"
                current_mode, sim_usable_energy_kwh, charge_end = self._append_charge_window_mode(
                    hourly_modes=hourly_modes,
                    slot=slot,
                    charge_window=solar_charge_starts[slot_start],
                    mode=mode,
                    price_key="export_price",
                    now=now,
                    current_mode=current_mode,
                    sim_usable_energy_kwh=sim_usable_energy_kwh,
                    usable_capacity_kwh=usable_capacity_kwh,
                )
                last_charge_mode = mode
                while slot_index < len(slots) and slots[slot_index]["start"] < charge_end:
                    slot_index += 1
                continue

            if slot_start in grid_charge_starts:
                mode = "laden_van_net"
                current_mode, sim_usable_energy_kwh, charge_end = self._append_charge_window_mode(
                    hourly_modes=hourly_modes,
                    slot=slot,
                    charge_window=grid_charge_starts[slot_start],
                    mode=mode,
                    price_key="import_price",
                    now=now,
                    current_mode=current_mode,
                    sim_usable_energy_kwh=sim_usable_energy_kwh,
                    usable_capacity_kwh=usable_capacity_kwh,
                )
                last_charge_mode = mode
                while slot_index < len(slots) and slots[slot_index]["start"] < charge_end:
                    slot_index += 1
                continue

            segment_end_index = slot_index
            while segment_end_index < len(slots):
                segment_slot_start = slots[segment_end_index]["start"]
                if (
                    segment_end_index > slot_index
                    and (
                        segment_slot_start in solar_charge_starts
                        or segment_slot_start in grid_charge_starts
                    )
                ):
                    break
                segment_end_index += 1

            segment_slots = slots[slot_index:segment_end_index]
            next_charge_window = None
            if segment_end_index < len(slots):
                next_charge_window = charge_starts.get(slots[segment_end_index]["start"])
            before_first_charge_phase = (
                first_charge_phase_start is None  # no charge windows → drain freely, no price gating
                or (
                    bool(segment_slots)
                    and segment_slots[0]["start"] < first_charge_phase_start
                )
            )
            target_end_energy_kwh = minimum_energy_before_next_charge_kwh if before_first_charge_phase else max(
                0.0,
                usable_capacity_kwh - (float(next_charge_window["charge_kwh"]) if next_charge_window else 0.0),
            )
            export_target_end_energy_kwh = (
                max(
                    target_end_energy_kwh,
                    minimum_energy_for_export_before_next_charge_kwh,
                    # Only export genuine surplus: energy above what solar will need to
                    # recharge.  If the battery is at 28 % the remaining 7.2 kWh capacity
                    # means there is nothing left to export without immediately re-charging
                    # it from solar.  At 96 % the 0.4 kWh remaining capacity is tiny so
                    # most of the available energy is genuine surplus.
                    usable_capacity_kwh - sim_usable_energy_kwh,
                )
                if before_first_charge_phase
                else target_end_energy_kwh
            )
            discharge_budget_kwh = (
                max(0.0, sim_usable_energy_kwh - target_end_energy_kwh)
                if before_first_charge_phase
                else sim_usable_energy_kwh
            )
            current_segment_slot = next(
                (
                    candidate
                    for candidate in segment_slots
                    if cast(datetime, candidate["start"]) <= now < cast(datetime, candidate["end"])
                ),
                segment_slots[0] if segment_slots else None,
            )
            current_segment_price = (
                float(current_segment_slot["price"])
                if current_segment_slot is not None
                else None
            )
            discharge_start_threshold_price = self._calculate_battery_discharge_start_threshold(segment_slots)
            has_meaningful_later_peak = (
                current_segment_price is not None
                and discharge_start_threshold_price is not None
                and any(
                    cast(datetime, candidate["start"]) > cast(datetime, current_segment_slot["start"])
                    and float(candidate["price"]) > current_segment_price
                    and float(candidate["net_solar_kwh"]) < 0
                    for candidate in segment_slots
                )
            )
            if not has_meaningful_later_peak:
                discharge_start_threshold_price = None
            # Outside the pre-charge drain phase, fall back to the average import price
            # as the minimum price required to discharge.  This prevents cheap morning
            # slots (e.g. 06:00–08:00 at €0.10 right after an overnight grid charge)
            # from triggering ontladen mode just because the segment is too short to
            # compute a proper peak-based threshold.
            if discharge_start_threshold_price is None and not before_first_charge_phase:
                discharge_start_threshold_price = average_price
            planned_discharge_kwh = self._plan_segment_discharge_kwh(
                slots=segment_slots,
                available_energy_kwh=discharge_budget_kwh,
                max_discharge_kw=max_discharge_kw,
                # Pre-charge: sort by price (most expensive first), simple greedy —
                # battery is distributed over the most expensive demand hours until empty.
                # Post-charge: price-ordered with later-demand protection for arbitrage.
                prefer_higher_prices=before_first_charge_phase or (discharge_start_threshold_price is not None and not before_first_charge_phase),
                protect_later_demand=not before_first_charge_phase,
            )
            forced_export_kwh = self._plan_segment_export_kwh(
                slots=segment_slots,
                planned_discharge_kwh=planned_discharge_kwh,
                available_energy_kwh=sim_usable_energy_kwh,
                target_end_energy_kwh=export_target_end_energy_kwh,
                export_window_start=None,
                export_window_end=None,
                max_discharge_kw=max_discharge_kw,
            )

            for segment_slot in segment_slots:
                segment_slot_start = segment_slot["start"]
                segment_slot_end = segment_slot["end"]
                segment_discharge_kwh = float(planned_discharge_kwh.get(segment_slot_start, 0.0))
                remaining_planned_discharge_kwh = sum(
                    float(planned_discharge_kwh.get(other["start"], 0.0))
                    for other in segment_slots
                    if other["start"] > segment_slot_start
                )
                segment_export_kwh = float(forced_export_kwh.get(segment_slot_start, 0.0))
                discharge_threshold_reached = (
                    discharge_start_threshold_price is None
                    or float(segment_slot["price"]) >= discharge_start_threshold_price
                )
                within_charge_phase = (
                    any(
                        segment_slot_end > cluster["start"] and segment_slot_start < cluster["end"]
                        for cluster in charge_phase_clusters
                    )
                )
                # After a solar charge, hold in laden_met_zonne_energie until a
                # planned discharge slot starts — so the inverter doesn't flip to
                # accu_uit (or export) between the charge end and discharge window.
                hold_solar_charge_mode = (
                    last_charge_mode == "laden_met_zonne_energie"
                    and sim_usable_energy_kwh > 0
                    and segment_discharge_kwh <= 0
                )
                charge_phase_mode = self._resolve_charge_phase_mode(
                    last_charge_mode=last_charge_mode,
                    active_charge_phase_mode=active_charge_phase_mode,
                )

                mode = charge_phase_mode
                if within_charge_phase or hold_solar_charge_mode:
                    mode = charge_phase_mode
                elif (
                    segment_discharge_kwh > 0
                    and sim_usable_energy_kwh > 0
                    and (discharge_threshold_reached or before_first_charge_phase)
                ):
                    mode = "ontladen"
                    last_charge_mode = "accu_uit"
                    sim_usable_energy_kwh = max(
                        0.0,
                        sim_usable_energy_kwh - min(segment_discharge_kwh, sim_usable_energy_kwh),
                    )
                elif segment_export_kwh > 0 and sim_usable_energy_kwh > 0:
                    mode = "ontladen_naar_net"
                    last_charge_mode = "accu_uit"
                    sim_usable_energy_kwh = max(
                        0.0,
                        sim_usable_energy_kwh - min(segment_export_kwh, sim_usable_energy_kwh),
                    )
                else:
                    exportable_kwh = max(0.0, sim_usable_energy_kwh - remaining_planned_discharge_kwh)
                    slot_export_capacity_kwh = max_discharge_kw * float(segment_slot["hours"])
                    if (
                        not before_first_charge_phase
                        and
                        exportable_kwh > 0
                        and float(segment_slot["net_solar_kwh"]) >= 0
                        and segment_end_index < len(slots)
                        and float(segment_slot["export_price"]) >= average_export_price
                    ):
                        mode = "ontladen_naar_net"
                        last_charge_mode = "accu_uit"
                        sim_usable_energy_kwh = max(
                            0.0,
                            sim_usable_energy_kwh - min(slot_export_capacity_kwh, exportable_kwh),
                        )

                if segment_slot["start"] <= now < segment_slot["end"]:
                    current_mode = mode

                hourly_modes.append(
                    {
                        "start": segment_slot["start"].isoformat(),
                        "end": segment_slot["end"].isoformat(),
                        "price": round(float(segment_slot["import_price"]), 6),
                        "usable_hours": round(float(segment_slot["hours"]), 3),
                        "mode": mode,
                    }
                )

            slot_index = segment_end_index

        self._update_active_charge_phase_state(
            now=now,
            active_charge_phase=active_charge_phase,
            current_mode=current_mode,
            last_charge_mode=last_charge_mode,
            active_charge_phase_mode=active_charge_phase_mode,
        )

        return self._merge_mode_windows(hourly_modes), current_mode

    def _calculate_battery_discharge_start_threshold(
        self,
        slots: list[dict[str, Any]],
    ) -> float | None:
        if len(slots) < 3:
            return None

        prices = [float(slot["price"]) for slot in slots]
        valley_min = prices[0]
        index = 0
        while index + 1 < len(prices) and prices[index + 1] <= prices[index]:
            index += 1
            valley_min = min(valley_min, prices[index])

        peak_max = prices[index]
        while index + 1 < len(prices) and prices[index + 1] >= prices[index]:
            index += 1
            peak_max = max(peak_max, prices[index])

        if peak_max <= valley_min or index >= len(prices) - 1:
            return None

        post_peak_min = min(prices[index + 1 :], default=peak_max)
        if (
            peak_max - valley_min < _CLEAR_PRICE_PEAK_MIN_DELTA
            or peak_max - post_peak_min < _CLEAR_PRICE_PEAK_MIN_DELTA
        ):
            return None

        return (valley_min + peak_max) / 2.0

    def _plan_segment_export_kwh(
        self,
        *,
        slots: list[dict[str, Any]],
        planned_discharge_kwh: dict[datetime, float],
        available_energy_kwh: float,
        target_end_energy_kwh: float,
        export_window_start: datetime | None,
        export_window_end: datetime | None,
        max_discharge_kw: float,
    ) -> dict[datetime, float]:
        if (
            available_energy_kwh <= 0
            or max_discharge_kw <= 0
            or not slots
        ):
            return {}

        total_planned_discharge_kwh = sum(float(value) for value in planned_discharge_kwh.values())
        target_end_energy_kwh = max(0.0, target_end_energy_kwh)
        required_export_kwh = max(
            0.0,
            available_energy_kwh - total_planned_discharge_kwh - target_end_energy_kwh,
        )
        if required_export_kwh <= 0:
            return {}

        export_slots: list[dict[str, Any]] = []
        for slot in slots:
            if float(planned_discharge_kwh.get(slot["start"], 0.0)) > 0:
                continue
            if float(slot["net_solar_kwh"]) < 0:
                continue
            allowed_hours = float(slot["hours"])
            if export_window_start is not None and export_window_end is not None:
                allowed_hours = self._overlap_hours(
                    slot["start"],
                    slot["end"],
                    export_window_start,
                    export_window_end,
                )
            if allowed_hours <= 0:
                continue
            export_capacity_kwh = min(
                max_discharge_kw * allowed_hours,
                max(0.0, available_energy_kwh),
            )
            if export_capacity_kwh <= 0:
                continue
            export_slots.append(
                {
                    "start": slot["start"],
                    "price": float(slot["export_price"]),
                    "capacity_kwh": export_capacity_kwh,
                }
            )

        if not export_slots:
            return {}

        remaining_export_kwh = required_export_kwh
        planned_export: dict[datetime, float] = {}
        for slot in sorted(export_slots, key=lambda item: (-float(item["price"]), item["start"])):
            if remaining_export_kwh <= 0:
                break
            assigned_kwh = min(float(slot["capacity_kwh"]), remaining_export_kwh)
            if assigned_kwh <= 0:
                continue
            planned_export[slot["start"]] = round(assigned_kwh, 6)
            remaining_export_kwh -= assigned_kwh

        return planned_export

    def _plan_segment_discharge_kwh(
        self,
        *,
        slots: list[dict[str, Any]],
        available_energy_kwh: float,
        max_discharge_kw: float,
        prefer_higher_prices: bool,
        protect_later_demand: bool = True,
    ) -> dict[datetime, float]:
        if available_energy_kwh <= 0 or max_discharge_kw <= 0 or not slots:
            return {}

        deficit_slots: list[dict[str, Any]] = []
        for slot in slots:
            deficit_kwh = min(
                max_discharge_kw * float(slot["hours"]),
                max(0.0, abs(float(slot["net_solar_kwh"]))) if float(slot["net_solar_kwh"]) < 0 else 0.0,
            )
            if deficit_kwh <= 0:
                continue
            deficit_slots.append(
                {
                    "start": slot["start"],
                    "price": float(slot["import_price"]),
                    "required_kwh": deficit_kwh,
                }
            )

        if not deficit_slots:
            return {}

        total_required_kwh = sum(float(slot["required_kwh"]) for slot in deficit_slots)
        if available_energy_kwh >= total_required_kwh:
            return {
                slot["start"]: round(float(slot["required_kwh"]), 6)
                for slot in deficit_slots
            }

        remaining_energy_kwh = available_energy_kwh
        planned_discharge: dict[datetime, float] = {}
        slot_order = (
            sorted(deficit_slots, key=lambda item: (-float(item["price"]), item["start"]))
            if prefer_higher_prices
            else sorted(deficit_slots, key=lambda item: item["start"])
        )
        # Simple greedy: fill slots in order (price-desc or chronological) until budget runs out.
        # Used for pre-charge discharge (prefer_higher_prices=True, protect_later_demand=False)
        # as well as for the plain chronological case.
        if not prefer_higher_prices or not protect_later_demand:
            for slot in slot_order:
                if remaining_energy_kwh <= 0:
                    break
                assigned_kwh = min(float(slot["required_kwh"]), remaining_energy_kwh)
                if assigned_kwh <= 0:
                    continue
                planned_discharge[cast(datetime, slot["start"])] = round(assigned_kwh, 6)
                remaining_energy_kwh -= assigned_kwh
            return planned_discharge

        segment_slots_by_start = {
            cast(datetime, slot["start"]): slot
            for slot in slots
        }
        for slot in slot_order:
            if remaining_energy_kwh <= 0:
                break

            slot_start = cast(datetime, slot["start"])
            assigned_total_kwh = sum(float(value) for value in planned_discharge.values())
            assigned_later_kwh = sum(
                float(value)
                for start, value in planned_discharge.items()
                if start > slot_start
            )
            later_net_need_kwh = max(
                0.0,
                -sum(
                    float(segment_slots_by_start[start]["net_solar_kwh"])
                    for start in segment_slots_by_start
                    if start > slot_start
                ),
            )
            protected_later_kwh = max(0.0, later_net_need_kwh - assigned_later_kwh)
            remaining_energy_kwh = max(
                0.0,
                float(available_energy_kwh) - assigned_total_kwh - protected_later_kwh,
            )
            assigned_kwh = min(float(slot["required_kwh"]), remaining_energy_kwh)
            if assigned_kwh <= 0:
                continue
            planned_discharge[slot_start] = round(assigned_kwh, 6)

        return planned_discharge

    def _merge_mode_windows(
        self,
        windows: list[dict[str, str | float]],
    ) -> list[dict[str, str | float]]:
        if not windows:
            return []

        merged: list[dict[str, str | float]] = []
        for window in sorted(windows, key=lambda item: str(item["start"])):
            if not merged:
                merged.append(dict(window))
                continue

            previous = merged[-1]
            previous_end = dt_util.parse_datetime(str(previous["end"]))
            current_start = dt_util.parse_datetime(str(window["start"]))
            if (
                previous_end is not None
                and current_start is not None
                and previous_end == current_start
                and previous.get("mode") == window.get("mode")
            ):
                previous["end"] = window["end"]
                previous["usable_hours"] = round(
                    float(previous.get("usable_hours", 0.0)) + float(window.get("usable_hours", 0.0)),
                    3,
                )
                previous["price"] = max(float(previous.get("price", 0.0)), float(window.get("price", 0.0)))
                continue

            merged.append(dict(window))

        return merged

    def _select_cheapest_charge_windows(
        self,
        *,
        windows: list[PlannerWindow],
        now: datetime,
        until: datetime | None,
        needed_kwh: float,
        max_charge_kw: float,
    ) -> list[dict[str, str | float]]:
        if until is None or needed_kwh <= 0 or max_charge_kw <= 0:
            return []

        remaining_hours = needed_kwh / max_charge_kw
        eligible_windows = [
            window
            for window in windows
            if self._overlap_hours(window.start, window.end, now, until) > 0
        ]
        ordered_windows = sorted(eligible_windows, key=lambda item: (item.price, item.start))

        planned_windows: list[dict[str, str | float]] = []
        for window in ordered_windows:
            if remaining_hours <= 0:
                break
            usable_hours = self._overlap_hours(window.start, window.end, now, until)
            if usable_hours <= 0:
                continue
            planned_windows.append(
                {
                    "start": window.start.isoformat(),
                    "end": window.end.isoformat(),
                    "price": round(window.price, 6),
                    "usable_hours": round(min(usable_hours, remaining_hours), 3),
                }
            )
            remaining_hours -= usable_hours

        return sorted(planned_windows, key=lambda item: str(item["start"]))

    def _select_cheapest_solar_charge_windows(
        self,
        *,
        price_windows: list[PlannerWindow],
        solar_windows: list[SolarWindow],
        hourly_demand: list[dict[str, str | float]],
        now: datetime,
        until: datetime | None,
        needed_kwh: float,
        max_charge_kw: float,
    ) -> list[dict[str, str | float]]:
        if until is None or needed_kwh <= 0 or max_charge_kw <= 0:
            return []

        candidates: list[dict[str, str | float]] = []
        for solar_window in solar_windows:
            usable_hours = self._overlap_hours(solar_window.start, solar_window.end, now, until)
            if usable_hours <= 0 or solar_window.forecast_kwh <= 0:
                continue

            demand_kwh = self._sum_remaining_home_demand_until(
                hourly_demand,
                max(now, solar_window.start),
                min(solar_window.end, until),
            )
            net_solar_kwh = max(0.0, solar_window.forecast_kwh - demand_kwh)
            available_hours = min(usable_hours, net_solar_kwh / max_charge_kw if max_charge_kw > 0 else 0.0)
            if available_hours <= 0:
                continue

            matched_price = min(
                (
                    window.price
                    for window in price_windows
                    if self._overlap_hours(window.start, window.end, solar_window.start, solar_window.end) > 0
                ),
                default=0.0,
            )
            candidates.append(
                {
                    "start": solar_window.start.isoformat(),
                    "end": solar_window.end.isoformat(),
                    "price": round(matched_price, 6),
                    "usable_hours": round(available_hours, 3),
                }
            )

        if not candidates:
            return []

        remaining_hours = needed_kwh / max_charge_kw
        ordered = sorted(
            candidates,
            key=lambda item: dt_util.parse_datetime(str(item["start"])) or dt_util.now(),
        )

        best_block: list[dict[str, str | float]] = []
        best_block_meets_target = False
        best_block_avg_price = float("inf")
        best_block_hours = 0.0

        for start_index in range(len(ordered)):
            block: list[dict[str, str | float]] = []
            total_hours = 0.0
            weighted_price = 0.0
            previous_end = None

            for window in ordered[start_index:]:
                window_start = dt_util.parse_datetime(str(window["start"]))
                window_end = dt_util.parse_datetime(str(window["end"]))
                if window_start is None or window_end is None:
                    continue
                if previous_end is not None and window_start != previous_end:
                    break

                usable_hours = float(window["usable_hours"])
                if usable_hours <= 0:
                    continue

                take_hours = min(usable_hours, remaining_hours - total_hours) if remaining_hours > total_hours else 0.0
                if take_hours <= 0:
                    break

                block.append(
                    {
                        "start": str(window["start"]),
                        "end": str(window["end"]),
                        "price": float(window["price"]),
                        "usable_hours": round(take_hours, 3),
                    }
                )
                total_hours += take_hours
                weighted_price += float(window["price"]) * take_hours
                previous_end = window_end

                if total_hours >= remaining_hours:
                    break

            if not block or total_hours <= 0:
                continue

            meets_target = total_hours >= remaining_hours
            average_price = weighted_price / total_hours
            if (
                best_block == []
                or (meets_target and not best_block_meets_target)
                or (
                    meets_target == best_block_meets_target
                    and (
                        average_price < best_block_avg_price
                        or (
                            abs(average_price - best_block_avg_price) < 1e-9
                            and total_hours > best_block_hours
                        )
                    )
                )
            ):
                best_block = block
                best_block_meets_target = meets_target
                best_block_avg_price = average_price
                best_block_hours = total_hours

        return best_block

    def _merge_planned_windows(
        self,
        windows: list[dict[str, str | float]],
    ) -> list[dict[str, str | float]]:
        if not windows:
            return []

        merged: list[dict[str, str | float]] = []
        for window in sorted(windows, key=lambda item: str(item["start"])):
            if not merged:
                merged.append(dict(window))
                continue

            previous = merged[-1]
            previous_end = dt_util.parse_datetime(str(previous["end"]))
            current_start = dt_util.parse_datetime(str(window["start"]))
            if previous_end is not None and current_start is not None and previous_end == current_start:
                previous["end"] = window["end"]
                previous["usable_hours"] = round(
                    float(previous.get("usable_hours", 0.0)) + float(window.get("usable_hours", 0.0)),
                    3,
                )
                previous["price"] = min(float(previous.get("price", 0.0)), float(window.get("price", 0.0)))
                continue

            merged.append(dict(window))

        return merged

    def _select_battery_discharge_windows(
        self,
        *,
        windows: list[PlannerWindow],
        now: datetime,
        after: datetime | None,
        average_price: float,
    ) -> list[dict[str, str | float]]:
        if after is None:
            return []

        discharge_windows = [
            {
                "start": window.start.isoformat(),
                "end": window.end.isoformat(),
                "price": round(window.price, 6),
                "usable_hours": round(max((window.end - max(window.start, now)).total_seconds() / 3600, 0.0), 3),
                "mode": "ontladen",
            }
            for window in windows
            if window.end > now
            and window.start >= after
            and window.price >= average_price
        ]
        return self._merge_planned_windows(discharge_windows)

    def _mark_discharge_window_modes(
        self,
        discharge_windows: list[dict[str, str | float]],
        charge_windows: list[dict[str, str | float]],
    ) -> list[dict[str, str | float]]:
        if not discharge_windows:
            return []

        charge_starts = sorted(
            (
                charge_start
                for window in charge_windows
                if (charge_start := dt_util.parse_datetime(str(window.get("start")))) is not None
            )
        )

        marked: list[dict[str, str | float]] = []
        for window in discharge_windows:
            window_end = dt_util.parse_datetime(str(window.get("end")))
            mode = "ontladen"
            if window_end is not None:
                next_charge_start = next((start for start in charge_starts if start >= window_end), None)
                if next_charge_start is not None and next_charge_start - window_end <= timedelta(minutes=90):
                    mode = "ontladen_naar_net"

            marked.append({**window, "mode": mode})

        return marked

    def _overlap_hours(
        self,
        start_a: datetime,
        end_a: datetime,
        start_b: datetime,
        end_b: datetime | None,
    ) -> float:
        if end_b is None:
            return 0.0
        overlap_start = max(start_a, start_b)
        overlap_end = min(end_a, end_b)
        if overlap_end <= overlap_start:
            return 0.0
        return (overlap_end - overlap_start).total_seconds() / 3600

    def _extract_price_windows(
        self,
        attributes: dict[str, Any],
        current_price: float | None,
        price_resolution: str,
        *,
        include_past: bool = False,
    ) -> list[PlannerWindow]:
        now = dt_util.now()
        raw_entries = list(attributes.get("raw_today", [])) + list(attributes.get("raw_tomorrow", []))
        windows: list[PlannerWindow] = []
        active_window: PlannerWindow | None = None

        for entry in raw_entries:
            start_raw = entry.get("start")
            end_raw = entry.get("end")
            price_raw = entry.get("value")
            if start_raw is None or end_raw is None or price_raw is None:
                continue
            try:
                start = dt_util.parse_datetime(start_raw)
                end = dt_util.parse_datetime(end_raw)
                price = float(price_raw)
            except (TypeError, ValueError):
                continue
            if start is None or end is None:
                continue
            if start <= now < end:
                active_window = PlannerWindow(start=start, end=end, price=price)
            if not include_past and end <= now:
                continue
            windows.append(PlannerWindow(start=start, end=end, price=price))

        if not windows:
            windows = self._extract_price_windows_from_series(attributes, now, include_past=include_past)

        if active_window and not any(w.start == active_window.start and w.end == active_window.end for w in windows):
            windows.insert(0, active_window)

        if not windows and current_price is not None:
            windows.append(PlannerWindow(start=now, end=now + timedelta(hours=1), price=current_price))

        if price_resolution == PRICE_RESOLUTION_HOURLY:
            windows = self._aggregate_price_windows_to_hourly(windows)

        return sorted(windows, key=lambda item: item.start)

    def _extract_price_average(
        self,
        attributes: dict[str, Any],
        windows: list[PlannerWindow],
    ) -> float | None:
        """Prefer the source sensor daily average, then fall back to the mean."""
        for key in ("average", "mean"):
            value = _coerce_float(attributes.get(key))
            if value is not None:
                return value
        if not windows:
            return None
        return sum(window.price for window in windows) / len(windows)

    def _extract_price_windows_from_series(
        self,
        attributes: dict[str, Any],
        now: datetime,
        *,
        include_past: bool = False,
    ) -> list[PlannerWindow]:
        """Build price windows from today/tomorrow lists when raw entries are unavailable."""
        today_values = attributes.get("today")
        tomorrow_values = attributes.get("tomorrow")
        if not isinstance(today_values, list):
            return []

        today_windows = self._series_to_price_windows(today_values, now.replace(hour=0, minute=0, second=0, microsecond=0))
        tomorrow_windows: list[PlannerWindow] = []
        if isinstance(tomorrow_values, list) and tomorrow_values:
            tomorrow_start = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            tomorrow_windows = self._series_to_price_windows(tomorrow_values, tomorrow_start)

        if include_past:
            return [*today_windows, *tomorrow_windows]
        return [window for window in [*today_windows, *tomorrow_windows] if window.end > now]

    def _series_to_price_windows(
        self,
        values: list[Any],
        start_time: datetime,
    ) -> list[PlannerWindow]:
        """Convert a list of prices into planner windows."""
        if not values:
            return []

        interval_minutes = self._infer_series_interval_minutes(len(values))
        if interval_minutes is None:
            return []

        windows: list[PlannerWindow] = []
        interval = timedelta(minutes=interval_minutes)
        for index, raw_value in enumerate(values):
            price = _coerce_float(raw_value)
            if price is None:
                continue
            start = start_time + (interval * index)
            end = start + interval
            windows.append(PlannerWindow(start=start, end=end, price=price))
        return windows

    def _infer_series_interval_minutes(self, item_count: int) -> int | None:
        """Infer interval size from the number of prices in a daily list."""
        if item_count == 24:
            return 60
        if item_count == 48:
            return 30
        if item_count == 96:
            return 15
        return None

    def _build_neutral_price_windows(
        self,
        current_price: float | None,
        *,
        hours: int = 1,
    ) -> list[PlannerWindow]:
        """Build flat windows so planning can continue without price data."""
        now = dt_util.now()
        neutral_price = current_price if current_price is not None else 0.0
        window_count = max(1, hours)
        return [
            PlannerWindow(
                start=now + timedelta(hours=index),
                end=now + timedelta(hours=index + 1),
                price=neutral_price,
            )
            for index in range(window_count)
        ]

    def _extend_price_window_tail(
        self,
        *,
        windows: list[PlannerWindow],
        horizon_end: datetime,
        fallback_price: float | None,
    ) -> list[PlannerWindow]:
        if not windows:
            return windows

        extended_windows = sorted(windows, key=lambda item: item.start)
        last_window = extended_windows[-1]
        if last_window.end >= horizon_end:
            return extended_windows

        interval = last_window.end - last_window.start
        if interval <= timedelta(0):
            interval = timedelta(hours=1)
        fill_price = last_window.price if last_window.price is not None else (fallback_price or 0.0)
        tail_start = last_window.end
        while tail_start < horizon_end:
            tail_end = min(tail_start + interval, horizon_end)
            extended_windows.append(
                PlannerWindow(
                    start=tail_start,
                    end=tail_end,
                    price=fill_price,
                )
            )
            tail_start = tail_end

        return extended_windows

    def _aggregate_price_windows_to_hourly(self, windows: list[PlannerWindow]) -> list[PlannerWindow]:
        if not windows:
            return windows
        grouped: dict[datetime, list[PlannerWindow]] = {}
        for window in windows:
            hour_start = window.start.replace(minute=0, second=0, microsecond=0)
            grouped.setdefault(hour_start, []).append(window)

        aggregated: list[PlannerWindow] = []
        for hour_start, grouped_windows in grouped.items():
            grouped_windows = sorted(grouped_windows, key=lambda item: item.start)
            aggregated.append(
                PlannerWindow(
                    start=hour_start,
                    end=max(window.end for window in grouped_windows),
                    price=round(sum(window.price for window in grouped_windows) / len(grouped_windows), 6),
                )
            )
        return sorted(aggregated, key=lambda item: item.start)

    def _extract_solar_windows(
        self,
        attributes: dict[str, Any],
        *,
        include_past: bool = False,
    ) -> list[SolarWindow]:
        raw_entries = attributes.get("detailedHourly", [])
        windows: list[SolarWindow] = []
        for entry in raw_entries:
            start_raw = entry.get("period_start")
            if not isinstance(start_raw, str):
                continue
            start = dt_util.parse_datetime(start_raw)
            if start is None:
                continue
            end = start + timedelta(hours=1)
            if not include_past and end <= dt_util.now():
                continue
            windows.append(
                SolarWindow(
                    start=start,
                    end=end,
                    forecast_kwh=_coerce_float(entry.get("pv_estimate"), default=0.0) or 0.0,
                    forecast_kwh_p10=_coerce_float(entry.get("pv_estimate10")),
                    forecast_kwh_p90=_coerce_float(entry.get("pv_estimate90")),
                )
            )
        return self._merge_solar_windows(windows)

    def _merge_solar_windows(self, windows: list[SolarWindow]) -> list[SolarWindow]:
        """Deduplicate solar windows by time range and keep the strongest forecast."""
        merged: dict[tuple[datetime, datetime], SolarWindow] = {}
        for window in sorted(windows, key=lambda item: item.start):
            key = (window.start, window.end)
            previous = merged.get(key)
            if previous is None:
                merged[key] = window
                continue
            merged[key] = SolarWindow(
                start=window.start,
                end=window.end,
                forecast_kwh=max(previous.forecast_kwh, window.forecast_kwh),
                forecast_kwh_p10=window.forecast_kwh_p10
                if window.forecast_kwh_p10 is not None
                else previous.forecast_kwh_p10,
                forecast_kwh_p90=window.forecast_kwh_p90
                if window.forecast_kwh_p90 is not None
                else previous.forecast_kwh_p90,
            )

        return sorted(merged.values(), key=lambda item: item.start)

    def _select_best_solar_window(self, windows: list[SolarWindow]) -> SolarWindow | None:
        productive_windows = [window for window in windows if window.forecast_kwh > 0]
        if not productive_windows:
            return None
        return max(productive_windows, key=lambda item: item.forecast_kwh)


def _coerce_float(value: Any, default: float | None = None) -> float | None:
    if value in (None, STATE_UNKNOWN, STATE_UNAVAILABLE, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
