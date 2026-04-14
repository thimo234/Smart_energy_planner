"""Coordinator for Smart Energy Planner."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import logging
import statistics
from typing import Any

from homeassistant.components.recorder import history
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import STATE_OFF, STATE_ON, STATE_UNAVAILABLE, STATE_UNKNOWN
from homeassistant.core import HomeAssistant
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
    CONF_HEATING_SWITCH_ENTITY,
    CONF_HEATING_LOOKBACK_DAYS,
    CONF_PLANNER_KIND,
    CONF_PRICE_SENSOR,
    CONF_PRICE_RESOLUTION,
    CONF_ROOM_TEMPERATURE_SENSOR,
    CONF_SOLCAST_TODAY_SENSOR,
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
    DEFAULT_HEATING_LOOKBACK_DAYS,
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
)

_LOGGER = logging.getLogger(__name__)


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
    battery_energy_available_kwh: float
    battery_remaining_capacity_kwh: float
    next_charge_opportunity_start: str | None
    home_demand_until_next_charge_kwh: float
    battery_reserved_energy_kwh: float
    battery_energy_available_for_discharge_kwh: float
    battery_room_needed_for_solar_kwh: float
    next_high_price_window_start: str | None
    next_high_price_window_price: float | None
    room_temperature_c: float | None
    thermostat_setpoint_c: float | None
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

    @property
    def _config(self) -> dict[str, Any]:
        return {**self.config_entry.data, **self.config_entry.options}

    async def _async_update_data(self) -> PlannerResult:
        """Fetch data and calculate planner output."""
        try:
            planner_kind = str(self._config.get(CONF_PLANNER_KIND, PLANNER_KIND_BATTERY))
            price_sensor = self._config[CONF_PRICE_SENSOR]
            solar_sensor = self._config.get(CONF_SOLCAST_TODAY_SENSOR)
            temperature_sensor = self._config.get(CONF_TEMPERATURE_SENSOR)
            room_temperature_sensor = self._config.get(CONF_ROOM_TEMPERATURE_SENSOR)
            heating_switch_entity = self._config.get(CONF_HEATING_SWITCH_ENTITY)
            total_energy_sensor = self._config.get(CONF_TOTAL_ENERGY_SENSOR)
            battery_soc_sensor = self._config.get(CONF_BATTERY_SOC_SENSOR)

            price_state = self.hass.states.get(price_sensor)
            solar_state = self.hass.states.get(solar_sensor) if solar_sensor else None
            temperature_state = self.hass.states.get(temperature_sensor) if temperature_sensor else None
            room_temperature_state = self.hass.states.get(room_temperature_sensor) if room_temperature_sensor else None
            heating_switch_state = self.hass.states.get(heating_switch_entity) if heating_switch_entity else None
            total_energy_state = self.hass.states.get(total_energy_sensor) if total_energy_sensor else None
            battery_soc_state = self.hass.states.get(battery_soc_sensor) if battery_soc_sensor else None

            source_status = self._build_source_status(
                price_sensor=price_sensor,
                price_state=price_state,
                solar_sensor=solar_sensor,
                solar_state=solar_state,
                temperature_sensor=temperature_sensor,
                temperature_state=temperature_state,
                room_temperature_sensor=room_temperature_sensor,
                room_temperature_state=room_temperature_state,
                heating_switch_entity=heating_switch_entity,
                heating_switch_state=heating_switch_state,
                total_energy_sensor=total_energy_sensor,
                total_energy_state=total_energy_state,
                battery_soc_sensor=battery_soc_sensor,
                battery_soc_state=battery_soc_state,
                planner_kind=planner_kind,
            )
            source_errors = self._collect_source_errors(source_status)

            current_price = _coerce_float(price_state.state) if price_state else None
            price_resolution = str(self._config.get(CONF_PRICE_RESOLUTION, PRICE_RESOLUTION_HOURLY))
            windows = self._extract_price_windows(
                price_state.attributes if price_state else {},
                current_price,
                price_resolution,
            )
            price_average = self._extract_price_average(
                price_state.attributes if price_state else {},
                windows,
            )

            if not price_state and planner_kind == PLANNER_KIND_THERMOSTAT:
                source_status["price_sensor"] = "waiting_for_price_sensor"
                windows = self._build_neutral_price_windows(current_price)
            elif not price_state:
                return self._build_pending_result(
                    "waiting_for_price_sensor", planner_kind, source_status, source_errors
                )

            if not windows:
                source_status["price_sensor"] = "no_price_windows"
                if planner_kind == PLANNER_KIND_THERMOSTAT:
                    windows = self._build_neutral_price_windows(current_price)
                else:
                    return self._build_pending_result(
                        "waiting_for_nordpool_prices",
                        planner_kind,
                        source_status,
                        self._collect_source_errors(source_status),
                    )

            solar_forecast = _coerce_float(
                solar_state.attributes.get("estimate") if solar_state else None,
                default=_coerce_float(solar_state.state, default=0.0) if solar_state else 0.0,
            )
            battery_soc_percent = _coerce_float(battery_soc_state.state if battery_soc_state else None)
            if battery_soc_percent is not None:
                battery_soc_percent = max(0.0, min(100.0, battery_soc_percent))
            outdoor_temperature = _coerce_float(
                temperature_state.state if temperature_state else None, default=7.0
            )
            room_temperature = _coerce_float(room_temperature_state.state if room_temperature_state else None)
            thermostat_setpoint = self._get_manual_thermostat_setpoint()
            thermostat_eco_setpoint = self._get_manual_eco_temperature(thermostat_setpoint)
            solar_windows = self._extract_solar_windows(solar_state.attributes if solar_state else {})
            solcast_confidence = _coerce_float(
                solar_state.attributes.get("analysis", {}).get("confidence") if solar_state else None
            )
            if planner_kind == PLANNER_KIND_BATTERY and not solar_windows and solar_forecast and solar_forecast > 0:
                solar_windows = self._build_fallback_solar_windows(solar_forecast)
            if solar_state and solar_forecast <= 0 and not solar_windows:
                source_status["solcast_today_sensor"] = "no_solcast_forecast_data"
            elif solar_state and solar_forecast is None:
                source_status["solcast_today_sensor"] = "invalid_solcast_value"

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
            else:
                non_heating_daily_average = total_energy_daily_average
                heating_estimate = 0.0

            cooling_profile = await self._async_estimate_room_cooling_profile(
                room_temperature_sensor=room_temperature_sensor,
                heating_switch_entity=heating_switch_entity,
                outdoor_temperature_sensor=temperature_sensor,
                room_temperature_c=room_temperature,
                outdoor_temperature_c=outdoor_temperature,
                thermostat_setpoint_c=thermostat_setpoint,
                thermostat_eco_setpoint_c=thermostat_eco_setpoint,
            )

            return self._build_plan(
                planner_kind=planner_kind,
                windows=windows,
                price_average=price_average,
                current_price=current_price,
                solar_forecast_kwh=solar_forecast,
                solar_windows=solar_windows,
                solcast_confidence=solcast_confidence,
                heating_estimate_kwh=heating_estimate,
                lookback_average_kwh=total_energy_daily_average if planner_kind == PLANNER_KIND_BATTERY else 0.0,
                total_energy_daily_average_kwh=total_energy_daily_average,
                non_heating_daily_average_kwh=non_heating_daily_average,
                room_temperature_c=room_temperature,
                thermostat_setpoint_c=thermostat_setpoint,
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
            _LOGGER.exception("Planner update failed")
            planner_kind = str(self._config.get(CONF_PLANNER_KIND, PLANNER_KIND_BATTERY))
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
            battery_energy_available_kwh=0.0,
            battery_remaining_capacity_kwh=0.0,
            next_charge_opportunity_start=None,
            home_demand_until_next_charge_kwh=0.0,
            battery_reserved_energy_kwh=0.0,
            battery_energy_available_for_discharge_kwh=0.0,
            battery_room_needed_for_solar_kwh=0.0,
            next_high_price_window_start=None,
            next_high_price_window_price=None,
            room_temperature_c=None,
            thermostat_setpoint_c=None,
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
        solar_sensor: str,
        solar_state,
        temperature_sensor: str,
        temperature_state,
        room_temperature_sensor: str | None,
        room_temperature_state,
        heating_switch_entity: str | None,
        heating_switch_state,
        total_energy_sensor: str | None,
        total_energy_state,
        battery_soc_sensor: str | None,
        battery_soc_state,
        planner_kind: str,
    ) -> dict[str, str]:
        if planner_kind == PLANNER_KIND_BATTERY:
            return {
                "price_sensor": self._state_status(price_sensor, price_state),
                "solcast_today_sensor": self._state_status(solar_sensor, solar_state),
                "total_energy_sensor": self._state_status(total_energy_sensor, total_energy_state),
                "battery_soc_sensor": self._state_status(battery_soc_sensor, battery_soc_state),
            }

        return {
            "price_sensor": self._state_status(price_sensor, price_state),
            "temperature_sensor": self._state_status(temperature_sensor, temperature_state),
            "room_temperature_sensor": self._state_status(room_temperature_sensor, room_temperature_state),
            "heating_switch_entity": self._state_status(heating_switch_entity, heating_switch_state),
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
                "solcast_today_sensor": "unknown",
                "total_energy_sensor": "unknown",
                "battery_soc_sensor": "unknown",
            }

        return {
            "price_sensor": "unknown",
            "temperature_sensor": "unknown",
            "room_temperature_sensor": "unknown",
            "heating_switch_entity": "unknown",
        }

    async def _async_get_average_daily_usage(self, entity_id: str) -> float:
        """Estimate average daily usage from recorder history of a cumulative kWh sensor."""
        lookback_days = int(self._config.get(CONF_HEATING_LOOKBACK_DAYS, DEFAULT_HEATING_LOOKBACK_DAYS))
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

        cooldown_delta = max(thermostat_setpoint_c - thermostat_eco_setpoint_c, 0.1)
        estimated_rate = self._estimate_cooling_rate_from_model(outdoor_temperature_c, room_temperature_c)
        reference_outdoor = outdoor_temperature_c

        hours_to_eco = round(min(12.0, max(1.0, cooldown_delta / estimated_rate)), 2)
        return {
            "hours_to_eco": hours_to_eco,
            "cooling_rate_c_per_hour": round(estimated_rate, 3),
            "reference_outdoor_temp_c": reference_outdoor,
        }

    def _estimate_cooling_rate_from_model(
        self,
        outdoor_temperature_c: float,
        room_temperature_c: float,
    ) -> float:
        runtime_state = self.hass.data.get(RUNTIME_STATE, {}).get(self.config_entry.entry_id, {})
        cooling_model = runtime_state.get("cooling_model", {})
        learned_factor = _coerce_float(cooling_model.get("rolling_cooling_factor"))
        if learned_factor is not None:
            delta_temp = max(room_temperature_c - outdoor_temperature_c, 0.5)
            return max(0.05, learned_factor * delta_temp)

        temp_delta_now = max(room_temperature_c - outdoor_temperature_c, 1.0)
        return max(0.1, temp_delta_now * 0.03)

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
    ) -> list[dict[str, datetime | float]]:
        """Plan eco windows around each above-average price peak.

        Eco starts before the peak based on the estimated cooldown time and
        ends as soon as the room is expected to have cooled down or when the
        price drops back below the daily average, whichever comes first.
        """
        if cooldown_hours <= 0:
            return []

        future_windows = [window for window in windows if window.end > now]
        if not future_windows:
            return []

        above_average_windows = [
            window for window in future_windows if window.price > average_price
        ]
        if not above_average_windows:
            return []

        grouped_windows: list[list[PlannerWindow]] = []
        current_group: list[PlannerWindow] = []
        for window in above_average_windows:
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
        cooldown_delta = timedelta(hours=cooldown_hours)
        for group in grouped_windows:
            peak_window = max(group, key=lambda item: item.price)
            peak_start = max(peak_window.start, now)
            eco_start = max(now, peak_start - cooldown_delta)
            cooled_at = eco_start + cooldown_delta
            below_average_at = next(
                (
                    window.start
                    for window in future_windows
                    if window.start >= eco_start and window.price <= average_price
                ),
                None,
            )

            candidate_ends = [cooled_at]
            if below_average_at is not None:
                candidate_ends.append(below_average_at)
            eco_end = min(candidate_ends)
            if eco_end <= eco_start:
                continue

            span_hours = (eco_end - eco_start).total_seconds() / 3600
            if span_hours <= 0:
                continue

            weighted_price = 0.0
            total_hours = 0.0
            for window in future_windows:
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
            if window["start"] <= previous["end"]:
                previous["end"] = max(previous["end"], window["end"])
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
        price_average: float | None,
        current_price: float | None,
        solar_forecast_kwh: float,
        solar_windows: list[SolarWindow],
        solcast_confidence: float | None,
        heating_estimate_kwh: float,
        lookback_average_kwh: float,
        total_energy_daily_average_kwh: float,
        non_heating_daily_average_kwh: float,
        room_temperature_c: float | None,
        thermostat_setpoint_c: float | None,
        thermostat_eco_setpoint_c: float | None,
        room_cooling_hours_to_eco: float | None,
        room_cooling_rate_c_per_hour: float | None,
        cooling_reference_outdoor_temp_c: float | None,
        battery_soc_percent: float | None,
        price_resolution: str,
        source_status: dict[str, str],
        source_errors: list[str],
    ) -> PlannerResult:
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
        best_solar_window = self._select_best_solar_window(solar_windows)
        estimated_total_home_demand_kwh = round(non_heating_daily_average_kwh + heating_estimate_kwh, 2)
        estimated_hourly_home_demand = self._build_hourly_home_demand_forecast(
            non_heating_daily_average_kwh=non_heating_daily_average_kwh,
            heating_estimate_kwh=heating_estimate_kwh,
        )
        now = dt_util.now()
        sunset_time = self._get_solar_day_end(solar_windows)
        remaining_solar_until_sunset = self._sum_remaining_solar_until(solar_windows, now, sunset_time)
        remaining_home_demand_until_sunset = self._sum_remaining_home_demand_until(
            estimated_hourly_home_demand, now, sunset_time
        )
        projected_solar_surplus_until_sunset = max(
            0.0, round(remaining_solar_until_sunset - remaining_home_demand_until_sunset, 3)
        )

        cheap_threshold = cheapest.price + (price_spread * 0.25 if price_signal_available else 0)
        mid_price_threshold = cheapest.price + (price_spread * 0.5 if price_signal_available else 0)
        expensive_threshold = most_expensive.price - (price_spread * 0.25 if price_signal_available else 0)
        next_cheap = next(
            (window for window in windows if window.start > now and window.price <= cheap_threshold),
            next((window for window in windows if window.price <= cheap_threshold), cheapest),
        )
        solar_covers_today = solar_forecast_kwh >= estimated_total_home_demand_kwh and estimated_total_home_demand_kwh > 0
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
        if price_signal_available:
            if planner_kind == PLANNER_KIND_THERMOSTAT:
                eco_windows = self._select_thermostat_peak_eco_windows(
                    windows=windows,
                    now=now,
                    cooldown_hours=eco_duration_hours,
                    average_price=average_price,
                )
            else:
                eco_windows = self._select_expensive_peak_blocks(
                    windows=windows,
                    now=now,
                    duration_hours=eco_duration_hours,
                    expensive_threshold=eco_expensive_threshold,
                )
        eco_window = next(
            (
                window
                for window in eco_windows
                if window["start"] <= now < window["end"]
            ),
            next((window for window in eco_windows if window["start"] > now), None),
        )
        eco_active_now = any(window["start"] <= now < window["end"] for window in eco_windows)
        preheat_minutes = int(
            self._config.get(CONF_THERMOSTAT_PREHEAT_MINUTES, DEFAULT_THERMOSTAT_PREHEAT_MINUTES)
        )
        preheat_windows = [
            {
                "start": max(window["start"] - timedelta(minutes=preheat_minutes), now.replace(hour=0, minute=0, second=0, microsecond=0)),
                "end": window["start"],
                "average_price": window["average_price"],
            }
            for window in eco_windows
            if preheat_minutes > 0
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
        target_battery_full_by_sunset = battery_enabled and sunset_time is not None and sunset_time > now
        grid_charge_needed_until_sunset = (
            max(
                0.0,
                round(
                    battery_capacity
                    - battery_total_energy_kwh
                    - projected_solar_surplus_until_sunset,
                    3,
                ),
            )
            if target_battery_full_by_sunset
            else 0.0
        )
        battery_charge_hours_needed_until_sunset = (
            round(grid_charge_needed_until_sunset / max_charge, 3)
            if target_battery_full_by_sunset and max_charge > 0
            else 0.0
        )
        solar_charge_target_kwh = (
            max(0.0, min(battery_remaining_capacity_kwh, projected_solar_surplus_until_sunset))
            if target_battery_full_by_sunset and max_charge > 0
            else 0.0
        )
        planned_solar_charge_windows = self._select_cheapest_solar_charge_windows(
            price_windows=windows,
            solar_windows=solar_windows,
            hourly_demand=estimated_hourly_home_demand,
            now=now,
            until=sunset_time,
            needed_kwh=solar_charge_target_kwh,
            max_charge_kw=max_charge,
        )
        planned_solar_charge_windows = self._merge_planned_windows(planned_solar_charge_windows)
        planned_grid_charge_windows = self._select_cheapest_charge_windows(
            windows=windows,
            now=now,
            until=sunset_time,
            needed_kwh=grid_charge_needed_until_sunset,
            max_charge_kw=max_charge,
        )
        planned_grid_charge_windows = self._merge_planned_windows(planned_grid_charge_windows)
        in_planned_solar_charge_window = any(
            (window_start := dt_util.parse_datetime(str(window["start"]))) is not None
            and (window_end := dt_util.parse_datetime(str(window["end"]))) is not None
            and window_start <= now < window_end
            for window in planned_solar_charge_windows
        )
        next_planned_solar_charge_start = min(
            (
                window_start
                for window in planned_solar_charge_windows
                if (window_start := dt_util.parse_datetime(str(window["start"]))) is not None and window_start > now
            ),
            default=None,
        )
        planned_grid_charge_price_floor = min(
            (window["price"] for window in planned_grid_charge_windows),
            default=None,
        )
        planned_grid_charge_price_ceiling = max(
            (window["price"] for window in planned_grid_charge_windows),
            default=None,
        )
        in_planned_grid_charge_window = any(
            (window_start := dt_util.parse_datetime(str(window["start"]))) is not None
            and (window_end := dt_util.parse_datetime(str(window["end"]))) is not None
            and window_start <= now < window_end
            for window in planned_grid_charge_windows
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
        future_solar_charge_window = next_planned_solar_charge_start is not None
        charge_window_active_now = in_planned_solar_charge_window or in_planned_grid_charge_window
        charge_session_consumed_today = (
            next_charge_opportunity is not None
            and next_charge_opportunity.date() == now.date()
            and now > next_charge_opportunity
            and not charge_window_active_now
        )
        charge_allowed_today = charge_window_active_now or not charge_session_consumed_today
        home_demand_until_next_charge_kwh = round(
            self._sum_remaining_home_demand_until(estimated_hourly_home_demand, now, next_charge_opportunity),
            3,
        )
        battery_reserved_energy_kwh = min(
            battery_energy_available_kwh,
            max(0.0, round(home_demand_until_next_charge_kwh, 3)),
        )
        battery_energy_available_for_discharge_kwh = max(
            0.0,
            round(battery_energy_available_kwh - battery_reserved_energy_kwh, 3),
        )
        battery_room_needed_for_solar_kwh = max(
            0.0,
            round(battery_total_energy_kwh + projected_solar_surplus_until_sunset - battery_capacity, 3),
        )
        next_high_price_window = min(
            (
                window
                for window in future_windows
                if current_price is None or window.price >= current_price + battery_min_profit
            ),
            key=lambda item: item.start,
            default=None,
        )
        planned_discharge_windows = self._select_battery_discharge_windows(
            windows=windows,
            now=now,
            after=sunset_time,
            average_price=average_price,
            battery_min_profit=battery_min_profit,
        )
        keep_energy_for_future_peak = (
            next_high_price_window is not None
            and current_price is not None
            and next_high_price_window.price - current_price >= battery_min_profit
        )
        charge_opportunity_before_peak = (
            next_charge_opportunity is not None
            and next_high_price_window is not None
            and next_charge_opportunity < next_high_price_window.start
            and charge_allowed_today
        )
        if charge_opportunity_before_peak:
            keep_energy_for_future_peak = False
        should_make_room_for_solar_now = (
            battery_room_needed_for_solar_kwh > 0
            and battery_energy_available_for_discharge_kwh > 0
            and current_price is not None
            and current_price >= average_price
            and (
                best_solar_is_now
                or projected_solar_surplus_until_sunset
                >= max(battery_remaining_capacity_kwh, max_charge)
            )
        )
        future_price_justifies_grid_charge = (
            current_price is not None
            and future_max_price is not None
            and future_max_price - current_price >= battery_min_profit
        )
        discharge_profitable_now = (
            current_price is not None
            and (
                (future_min_price is not None and current_price - future_min_price >= battery_min_profit)
                or current_price >= expensive_threshold
            )
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
                "each eco window starts early enough for the room to cool down before the peak and ends when cooling is done or the price normalizes"
            )

        battery_strategy = "accu_uit"
        if planner_kind == PLANNER_KIND_THERMOSTAT:
            battery_strategy = "not_applicable"
        elif battery_enabled:
            if battery_soc_percent is None:
                rationale_parts.append("battery state of charge is unavailable, so battery control stays idle")
            elif in_planned_solar_charge_window and battery_remaining_capacity_kwh > 0 and charge_allowed_today:
                battery_strategy = "laden_met_zonne_energie"
                score += 12
                rationale_parts.append(
                    f"battery should charge in this planned cheap solar window up to {min(max_charge, battery_remaining_capacity_kwh, max_charge):.1f} kW"
                )
            elif (
                should_make_room_for_solar_now
            ):
                battery_strategy = "ontladen_naar_net"
                score += 12
                rationale_parts.append(
                    f"battery can discharge about {min(battery_energy_available_for_discharge_kwh, max_discharge):.1f} kWh-equivalent now to create room for the coming solar surplus"
                )
                rationale_parts.append(
                    "forecast solar is likely to fill the remaining battery capacity, so exporting now creates useful room"
                )
            elif (
                target_battery_full_by_sunset
                and grid_charge_needed_until_sunset > 0
                and in_planned_grid_charge_window
                and battery_remaining_capacity_kwh > 0
                and charge_allowed_today
            ):
                battery_strategy = "laden_van_net"
                score += 12
                rationale_parts.append(
                    f"battery targets a full state by sunset and still needs about {grid_charge_needed_until_sunset:.1f} kWh"
                )
                rationale_parts.append(
                    f"the planner reserved roughly {battery_charge_hours_needed_until_sunset:.1f} charging hours in the cheapest pre-sunset windows"
                )
            elif solar_covers_today and in_planned_solar_charge_window and charge_allowed_today:
                battery_strategy = "laden_met_zonne_energie"
                score += 10
                rationale_parts.append(
                    f"battery should use this selected solar charging window up to {min(max_charge, battery_remaining_capacity_kwh):.1f} kW"
                )
                rationale_parts.append("grid charging is not needed because forecast solar covers the expected demand")
            elif (
                keep_energy_for_future_peak
                and battery_energy_available_for_discharge_kwh <= 0
            ):
                battery_strategy = "accu_uit"
                rationale_parts.append(
                    "battery stays idle because the remaining charge is reserved for household use until the next charging moment"
                )
            elif (
                keep_energy_for_future_peak
                and current_price is not None
                and next_high_price_window is not None
            ):
                battery_strategy = "accu_uit"
                rationale_parts.append(
                    f"battery saves charge for a later higher-price window around {next_high_price_window.start.isoformat()}"
                )
            elif (
                charge_opportunity_before_peak
                and discharge_profitable_now
                and battery_energy_available_for_discharge_kwh > 0
            ):
                battery_strategy = "ontladen"
                score += 9
                rationale_parts.append(
                    "battery can discharge now because there is still a charging opportunity before the next higher-price window"
                )
            elif (
                discharge_profitable_now
                and battery_energy_available_for_discharge_kwh > 0
                and (future_solar_charge_window or future_min_price is not None or battery_room_needed_for_solar_kwh > 0)
            ):
                battery_strategy = (
                    "ontladen_naar_net" if future_solar_charge_window or battery_room_needed_for_solar_kwh > 0 else "ontladen"
                )
                score += 10
                rationale_parts.append(
                    f"battery can discharge up to {min(max_discharge, battery_energy_available_for_discharge_kwh):.1f} kW because a later charging opportunity is at least {battery_min_profit:.2f} EUR/kWh cheaper"
                )
                if future_solar_charge_window:
                    rationale_parts.append("battery may discharge to the grid now to create room for cheap solar charging later")
            elif (
                current_price is not None
                and planned_grid_charge_price_floor is not None
                and current_price - planned_grid_charge_price_floor >= battery_min_profit
                and not in_planned_grid_charge_window
                and target_battery_full_by_sunset
                and grid_charge_needed_until_sunset > 0
                and battery_energy_available_for_discharge_kwh > 0
            ):
                battery_strategy = "ontladen"
                score += 8
                rationale_parts.append(
                    "battery can discharge now because later pre-sunset charging windows are cheaper and can refill it before sunset"
                )
            elif (
                cheap_now
                and future_price_justifies_grid_charge
                and not solar_covers_today
                and battery_remaining_capacity_kwh > 0
                and charge_allowed_today
            ):
                battery_strategy = "laden_van_net"
                score += 10
                rationale_parts.append(
                    f"battery can charge from the grid up to {min(max_charge, battery_remaining_capacity_kwh):.1f} kW because a later discharge window is at least {battery_min_profit:.2f} EUR/kWh more expensive"
                )
            elif solar_covers_today and future_solar_charge_window and charge_allowed_today:
                battery_strategy = "accu_uit"
                rationale_parts.append("battery can stay idle until the later solar charging window starts")
            elif charge_session_consumed_today and next_charge_opportunity is not None:
                rationale_parts.append(
                    "battery uses one charge window per day, so after the daytime charge block it only allows idle or discharge until tomorrow"
                )
            elif target_battery_full_by_sunset and grid_charge_needed_until_sunset <= 0:
                battery_strategy = "laden_met_zonne_energie" if in_planned_solar_charge_window else "accu_uit"
                rationale_parts.append(
                    "forecast solar after household demand is enough to fill the battery by sunset without grid charging"
                )
            elif target_battery_full_by_sunset and planned_grid_charge_price_ceiling is not None:
                rationale_parts.append(
                    f"grid charging only makes sense in pre-sunset windows up to about {planned_grid_charge_price_ceiling:.3f} EUR/kWh"
                )
            elif battery_energy_available_for_discharge_kwh <= 0 and battery_energy_available_kwh > 0:
                rationale_parts.append(
                    "battery keeps its remaining charge for household demand until the next charging opportunity"
                )
            elif battery_total_energy_kwh <= minimum_battery_reserve_kwh and battery_soc_percent is not None:
                rationale_parts.append(
                    f"battery stays above the configured minimum reserve of {battery_min_soc_percent:.0f}%"
                )

        planned_battery_mode_schedule = self._build_battery_mode_schedule(
            now=now,
            current_mode=battery_strategy,
            planned_solar_charge_windows=planned_solar_charge_windows,
            planned_grid_charge_windows=planned_grid_charge_windows,
            planned_discharge_windows=planned_discharge_windows,
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
            battery_energy_available_kwh=battery_energy_available_kwh,
            battery_remaining_capacity_kwh=battery_remaining_capacity_kwh,
            next_charge_opportunity_start=next_charge_opportunity.isoformat() if next_charge_opportunity else None,
            home_demand_until_next_charge_kwh=home_demand_until_next_charge_kwh,
            battery_reserved_energy_kwh=battery_reserved_energy_kwh,
            battery_energy_available_for_discharge_kwh=battery_energy_available_for_discharge_kwh,
            battery_room_needed_for_solar_kwh=battery_room_needed_for_solar_kwh,
            next_high_price_window_start=next_high_price_window.start.isoformat() if next_high_price_window else None,
            next_high_price_window_price=next_high_price_window.price if next_high_price_window else None,
            room_temperature_c=room_temperature_c,
            thermostat_setpoint_c=thermostat_setpoint_c,
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
    ) -> list[dict[str, str | float]]:
        """Build a simple hourly demand forecast for today."""
        base_hourly = non_heating_daily_average_kwh / 24 if non_heating_daily_average_kwh > 0 else 0.0

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

        forecast: list[dict[str, str | float]] = []
        for hour in range(24):
            slot_start = today_start + timedelta(hours=hour)
            slot_end = slot_start + timedelta(hours=1)
            heating_hourly = heating_estimate_kwh * (heating_profile[hour] / profile_sum)
            total_hourly = round(base_hourly + heating_hourly, 3)
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
        if daily_forecast_kwh <= 0:
            return []

        now = dt_util.now()
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
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
            if end <= now:
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

        remaining_hours = needed_kwh / max_charge_kw
        planned_windows: list[dict[str, str | float]] = []
        for window in sorted(candidates, key=lambda item: (float(item["price"]), str(item["start"]))):
            if remaining_hours <= 0:
                break
            usable_hours = float(window["usable_hours"])
            if usable_hours <= 0:
                continue
            planned_windows.append(
                {
                    "start": str(window["start"]),
                    "end": str(window["end"]),
                    "price": float(window["price"]),
                    "usable_hours": round(min(usable_hours, remaining_hours), 3),
                }
            )
            remaining_hours -= usable_hours

        return sorted(planned_windows, key=lambda item: str(item["start"]))

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
        battery_min_profit: float,
    ) -> list[dict[str, str | float]]:
        if after is None:
            return []

        discharge_windows = [
            {
                "start": window.start.isoformat(),
                "end": window.end.isoformat(),
                "price": round(window.price, 6),
                "usable_hours": round(max((window.end - max(window.start, now)).total_seconds() / 3600, 0.0), 3),
            }
            for window in windows
            if window.end > now
            and window.start >= after
            and window.price >= max(average_price, average_price + (battery_min_profit / 2))
        ]
        return self._merge_planned_windows(discharge_windows)

    def _build_battery_mode_schedule(
        self,
        *,
        now: datetime,
        current_mode: str,
        planned_solar_charge_windows: list[dict[str, str | float]],
        planned_grid_charge_windows: list[dict[str, str | float]],
        planned_discharge_windows: list[dict[str, str | float]],
    ) -> list[dict[str, str]]:
        schedule: list[dict[str, str]] = [{"at": now.isoformat(), "mode": current_mode}]

        for window in planned_solar_charge_windows:
            schedule.append({"at": str(window["start"]), "mode": "laden_met_zonne_energie"})
            schedule.append({"at": str(window["end"]), "mode": "accu_uit"})

        for window in planned_grid_charge_windows:
            schedule.append({"at": str(window["start"]), "mode": "laden_van_net"})
            schedule.append({"at": str(window["end"]), "mode": "accu_uit"})

        for window in planned_discharge_windows:
            schedule.append({"at": str(window["start"]), "mode": "ontladen_naar_net"})
            schedule.append({"at": str(window["end"]), "mode": "accu_uit"})

        deduped: list[dict[str, str]] = []
        for item in sorted(schedule, key=lambda entry: entry["at"]):
            if deduped and deduped[-1]["at"] == item["at"] and deduped[-1]["mode"] == item["mode"]:
                continue
            if deduped and deduped[-1]["at"] == item["at"] and deduped[-1]["mode"] != item["mode"]:
                deduped[-1] = item
                continue
            deduped.append(item)
        return deduped

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
        self, attributes: dict[str, Any], current_price: float | None, price_resolution: str
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
            if end <= now:
                continue
            windows.append(PlannerWindow(start=start, end=end, price=price))

        if not windows:
            windows = self._extract_price_windows_from_series(attributes, now)

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

    def _build_neutral_price_windows(self, current_price: float | None) -> list[PlannerWindow]:
        """Build a single neutral window so thermostat control can continue without price data."""
        now = dt_util.now()
        return [
            PlannerWindow(
                start=now,
                end=now + timedelta(hours=1),
                price=current_price if current_price is not None else 0.0,
            )
        ]

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

    def _extract_solar_windows(self, attributes: dict[str, Any]) -> list[SolarWindow]:
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
            if end <= dt_util.now():
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
        return sorted(windows, key=lambda item: item.start)

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
