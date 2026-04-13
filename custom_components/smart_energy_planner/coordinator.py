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
    CONF_BATTERY_MIN_PROFIT_PER_KWH,
    CONF_HEATING_SWITCH_ENTITY,
    CONF_HEATING_LOOKBACK_DAYS,
    CONF_PLANNER_KIND,
    CONF_PRICE_SENSOR,
    CONF_PRICE_RESOLUTION,
    CONF_ROOM_TEMPERATURE_SENSOR,
    CONF_SOLCAST_TODAY_SENSOR,
    CONF_TEMPERATURE_SENSOR,
    CONF_THERMOSTAT_ECO_SETBACK,
    CONF_THERMOSTAT_ENTITY,
    CONF_TOTAL_ENERGY_SENSOR,
    COORDINATOR_UPDATE_INTERVAL,
    DEFAULT_BATTERY_MIN_PROFIT_PER_KWH,
    DEFAULT_THERMOSTAT_ECO_SETBACK,
    DOMAIN,
    PLANNER_KIND_BATTERY,
    PLANNER_KIND_COMBINED,
    PLANNER_KIND_THERMOSTAT,
    PRICE_RESOLUTION_HOURLY,
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
    room_temperature_c: float | None
    thermostat_setpoint_c: float | None
    thermostat_eco_setpoint_c: float | None
    room_cooling_hours_to_eco: float | None
    room_cooling_rate_c_per_hour: float | None
    cooling_reference_outdoor_temp_c: float | None
    planned_eco_window_start: str | None
    planned_eco_window_end: str | None
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
            planner_kind = str(self._config.get(CONF_PLANNER_KIND, PLANNER_KIND_COMBINED))
            price_sensor = self._config[CONF_PRICE_SENSOR]
            solar_sensor = self._config.get(CONF_SOLCAST_TODAY_SENSOR)
            temperature_sensor = self._config.get(CONF_TEMPERATURE_SENSOR)
            room_temperature_sensor = self._config.get(CONF_ROOM_TEMPERATURE_SENSOR)
            thermostat_entity = self._config.get(CONF_THERMOSTAT_ENTITY)
            heating_switch_entity = self._config.get(CONF_HEATING_SWITCH_ENTITY)
            total_energy_sensor = self._config.get(CONF_TOTAL_ENERGY_SENSOR)

            price_state = self.hass.states.get(price_sensor)
            solar_state = self.hass.states.get(solar_sensor) if solar_sensor else None
            temperature_state = self.hass.states.get(temperature_sensor) if temperature_sensor else None
            room_temperature_state = self.hass.states.get(room_temperature_sensor) if room_temperature_sensor else None
            thermostat_state = self.hass.states.get(thermostat_entity) if thermostat_entity else None
            heating_switch_state = self.hass.states.get(heating_switch_entity) if heating_switch_entity else None
            total_energy_state = self.hass.states.get(total_energy_sensor) if total_energy_sensor else None

            source_status = self._build_source_status(
                price_sensor=price_sensor,
                price_state=price_state,
                solar_sensor=solar_sensor,
                solar_state=solar_state,
                temperature_sensor=temperature_sensor,
                temperature_state=temperature_state,
                room_temperature_sensor=room_temperature_sensor,
                room_temperature_state=room_temperature_state,
                thermostat_entity=thermostat_entity,
                thermostat_state=thermostat_state,
                heating_switch_entity=heating_switch_entity,
                heating_switch_state=heating_switch_state,
                total_energy_sensor=total_energy_sensor,
                total_energy_state=total_energy_state,
                planner_kind=planner_kind,
            )
            source_errors = self._collect_source_errors(source_status)

            if not price_state:
                return self._build_pending_result(
                    "waiting_for_price_sensor", planner_kind, source_status, source_errors
                )

            current_price = _coerce_float(price_state.state)
            price_resolution = str(self._config.get(CONF_PRICE_RESOLUTION, PRICE_RESOLUTION_HOURLY))
            windows = self._extract_price_windows(price_state.attributes, current_price, price_resolution)
            if not windows:
                source_status["price_sensor"] = "no_price_windows"
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
            outdoor_temperature = _coerce_float(
                temperature_state.state if temperature_state else None, default=12.0
            )
            room_temperature = _coerce_float(room_temperature_state.state if room_temperature_state else None)
            thermostat_setpoint = self._extract_thermostat_setpoint(thermostat_state)
            eco_setback = float(self._config.get(CONF_THERMOSTAT_ECO_SETBACK, DEFAULT_THERMOSTAT_ECO_SETBACK))
            thermostat_eco_setpoint = (
                round(max(5.0, thermostat_setpoint - eco_setback), 2)
                if thermostat_setpoint is not None
                else None
            )
            solar_windows = self._extract_solar_windows(solar_state.attributes if solar_state else {})
            solcast_confidence = _coerce_float(
                solar_state.attributes.get("analysis", {}).get("confidence") if solar_state else None
            )
            if planner_kind in (PLANNER_KIND_BATTERY, PLANNER_KIND_COMBINED) and not solar_windows and solar_forecast and solar_forecast > 0:
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
            if thermostat_state and thermostat_setpoint is None:
                source_status["thermostat_entity"] = "invalid_thermostat_setpoint"
            if total_energy_state and total_energy_daily_average <= 0:
                source_status["total_energy_sensor"] = "no_total_energy_history_yet"

            if planner_kind == PLANNER_KIND_BATTERY:
                heating_estimate = 0.0
                non_heating_daily_average = total_energy_daily_average
            elif planner_kind == PLANNER_KIND_THERMOSTAT:
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
                current_price=current_price,
                solar_forecast_kwh=solar_forecast,
                solar_windows=solar_windows,
                solcast_confidence=solcast_confidence,
                heating_estimate_kwh=heating_estimate,
                lookback_average_kwh=0.0,
                total_energy_daily_average_kwh=total_energy_daily_average,
                non_heating_daily_average_kwh=non_heating_daily_average,
                room_temperature_c=room_temperature,
                thermostat_setpoint_c=thermostat_setpoint,
                thermostat_eco_setpoint_c=thermostat_eco_setpoint,
                room_cooling_hours_to_eco=cooling_profile["hours_to_eco"],
                room_cooling_rate_c_per_hour=cooling_profile["cooling_rate_c_per_hour"],
                cooling_reference_outdoor_temp_c=cooling_profile["reference_outdoor_temp_c"],
                price_resolution=price_resolution,
                source_status=source_status,
                source_errors=self._collect_source_errors(source_status),
            )
        except Exception as err:
            _LOGGER.exception("Planner update failed")
            return self._build_pending_result(
                "planner_runtime_error",
                str(self._config.get(CONF_PLANNER_KIND, PLANNER_KIND_COMBINED)),
                {
                    "price_sensor": "unknown",
                    "solcast_today_sensor": "unknown",
                    "temperature_sensor": "unknown",
                    "room_temperature_sensor": "unknown",
                    "thermostat_entity": "unknown",
                    "heating_switch_entity": "unknown",
                    "total_energy_sensor": "unknown",
                },
                [f"planner_runtime_error: {err!s}"],
            )

    def _build_pending_result(
        self, status: str, planner_kind: str, source_status: dict[str, str], source_errors: list[str]
    ) -> PlannerResult:
        return PlannerResult(
            planner_kind=planner_kind,
            status=status,
            score=0,
            recommendation="waiting_for_data",
            battery_strategy="accu_uit",
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
            room_temperature_c=None,
            thermostat_setpoint_c=None,
            thermostat_eco_setpoint_c=None,
            room_cooling_hours_to_eco=None,
            room_cooling_rate_c_per_hour=None,
            cooling_reference_outdoor_temp_c=None,
            planned_eco_window_start=None,
            planned_eco_window_end=None,
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
        thermostat_entity: str | None,
        thermostat_state,
        heating_switch_entity: str | None,
        heating_switch_state,
        total_energy_sensor: str | None,
        total_energy_state,
        planner_kind: str,
    ) -> dict[str, str]:
        solar_status = self._state_status(solar_sensor, solar_state)
        temperature_status = self._state_status(temperature_sensor, temperature_state)
        room_temperature_status = self._state_status(room_temperature_sensor, room_temperature_state)
        thermostat_status = self._state_status(thermostat_entity, thermostat_state)
        heating_switch_status = self._state_status(heating_switch_entity, heating_switch_state)
        total_energy_status = self._state_status(total_energy_sensor, total_energy_state)

        if planner_kind == PLANNER_KIND_BATTERY:
            temperature_status = "not_configured"
            room_temperature_status = "not_configured"
            thermostat_status = "not_configured"
            heating_switch_status = "not_configured"
        elif planner_kind == PLANNER_KIND_THERMOSTAT:
            solar_status = "not_configured"
            total_energy_status = "not_configured"

        return {
            "price_sensor": self._state_status(price_sensor, price_state),
            "solcast_today_sensor": solar_status,
            "temperature_sensor": temperature_status,
            "room_temperature_sensor": room_temperature_status,
            "thermostat_entity": thermostat_status,
            "heating_switch_entity": heating_switch_status,
            "total_energy_sensor": total_energy_status,
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

    async def _async_get_average_daily_usage(self, entity_id: str) -> float:
        """Estimate average daily usage from recorder history of a cumulative kWh sensor."""
        lookback_days = int(self._config[CONF_HEATING_LOOKBACK_DAYS])
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

    def _estimate_heating_need(self, outdoor_temperature: float, average_daily_heating_kwh: float) -> float:
        heating_factor = max(0.2, min(1.6, (18 - outdoor_temperature) / 10))
        return round(average_daily_heating_kwh * heating_factor, 2)

    def _extract_thermostat_setpoint(self, thermostat_state) -> float | None:
        if thermostat_state is None:
            return None
        for key in ("temperature", "target_temp_high", "target_temp_low"):
            value = _coerce_float(thermostat_state.attributes.get(key))
            if value is not None:
                return value
        return None

    async def _async_load_entity_history(
        self,
        entity_ids: list[str],
        lookback_days: int,
    ) -> dict[str, list[Any]]:
        end = dt_util.now()
        start = end - timedelta(days=lookback_days)

        def _load_history():
            return history.get_significant_states(
                self.hass,
                start,
                end,
                entity_ids,
                include_start_time_state=True,
                significant_changes_only=False,
                no_attributes=True,
            )

        try:
            return await self.hass.async_add_executor_job(_load_history)
        except Exception:
            return {}

    async def _async_estimate_room_cooling_profile(
        self,
        *,
        room_temperature_sensor: str | None,
        heating_switch_entity: str | None,
        outdoor_temperature_sensor: str | None,
        room_temperature_c: float | None,
        outdoor_temperature_c: float,
        thermostat_setpoint_c: float | None,
        thermostat_eco_setpoint_c: float | None,
    ) -> dict[str, float | None]:
        if (
            room_temperature_sensor is None
            or heating_switch_entity is None
            or thermostat_setpoint_c is None
            or thermostat_eco_setpoint_c is None
            or room_temperature_c is None
        ):
            return {
                "hours_to_eco": None,
                "cooling_rate_c_per_hour": None,
                "reference_outdoor_temp_c": outdoor_temperature_c,
            }

        lookback_days = int(self._config.get(CONF_HEATING_LOOKBACK_DAYS, 5))
        entity_ids = [room_temperature_sensor, heating_switch_entity]
        if outdoor_temperature_sensor:
            entity_ids.append(outdoor_temperature_sensor)
        history_result = await self._async_load_entity_history(entity_ids, lookback_days)

        room_states = history_result.get(room_temperature_sensor, [])
        switch_states = history_result.get(heating_switch_entity, [])
        outdoor_states = history_result.get(outdoor_temperature_sensor, []) if outdoor_temperature_sensor else []

        coefficients: list[float] = []
        measured_rates: list[float] = []
        reference_outdoor_temps: list[float] = []

        for index, switch_state in enumerate(switch_states):
            if not self._is_switch_off(switch_state.state):
                continue

            off_start = switch_state.last_updated
            off_end = dt_util.now()
            for next_state in switch_states[index + 1 :]:
                if not self._is_switch_off(next_state.state):
                    off_end = next_state.last_updated
                    break

            duration_hours = (off_end - off_start).total_seconds() / 3600
            if duration_hours < 0.75:
                continue

            start_temp = self._get_state_value_at(room_states, off_start)
            end_temp = self._get_state_value_at(room_states, off_end)
            outdoor_avg = self._average_state_value_between(outdoor_states, off_start, off_end, outdoor_temperature_c)
            if start_temp is None or end_temp is None or outdoor_avg is None:
                continue

            temp_drop = start_temp - end_temp
            if temp_drop <= 0:
                continue

            measured_rate = temp_drop / duration_hours
            temp_delta = max(start_temp - outdoor_avg, 1.0)
            coefficients.append(measured_rate / temp_delta)
            measured_rates.append(measured_rate)
            reference_outdoor_temps.append(outdoor_avg)

        cooldown_delta = max(thermostat_setpoint_c - thermostat_eco_setpoint_c, 0.1)
        temp_delta_now = max(room_temperature_c - outdoor_temperature_c, 1.0)

        if coefficients:
            average_coefficient = statistics.fmean(coefficients)
            estimated_rate = max(0.05, average_coefficient * temp_delta_now)
            reference_outdoor = round(statistics.fmean(reference_outdoor_temps), 2)
        elif measured_rates:
            estimated_rate = max(0.05, statistics.fmean(measured_rates))
            reference_outdoor = outdoor_temperature_c
        else:
            fallback_rate = max(0.1, temp_delta_now * 0.03)
            estimated_rate = fallback_rate
            reference_outdoor = outdoor_temperature_c

        hours_to_eco = round(min(12.0, max(1.0, cooldown_delta / estimated_rate)), 2)
        return {
            "hours_to_eco": hours_to_eco,
            "cooling_rate_c_per_hour": round(estimated_rate, 3),
            "reference_outdoor_temp_c": reference_outdoor,
        }

    def _get_state_value_at(self, states: list[Any], moment: datetime) -> float | None:
        latest_value: float | None = None
        for state in states:
            if state.last_updated > moment:
                break
            latest_value = _coerce_float(state.state)
        return latest_value

    def _average_state_value_between(
        self,
        states: list[Any],
        start: datetime,
        end: datetime,
        default: float | None,
    ) -> float | None:
        values = [
            value
            for state in states
            if start <= state.last_updated <= end
            if (value := _coerce_float(state.state)) is not None
        ]
        if values:
            return statistics.fmean(values)
        return default

    def _is_switch_off(self, state: str | None) -> bool:
        return str(state).lower() in {STATE_OFF, "off", "idle", "closed"}

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

    def _build_plan(
        self,
        *,
        planner_kind: str,
        windows: list[PlannerWindow],
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
        price_resolution: str,
        source_status: dict[str, str],
        source_errors: list[str],
    ) -> PlannerResult:
        sorted_by_price = sorted(windows, key=lambda item: item.price)
        cheapest = sorted_by_price[0]
        most_expensive = sorted_by_price[-1]
        price_spread = round(most_expensive.price - cheapest.price, 4)
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

        cheap_threshold = cheapest.price + (price_spread * 0.25)
        next_cheap = next((window for window in windows if window.price <= cheap_threshold), cheapest)
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
        future_solar_charge_window = best_solar_window is not None and best_solar_window.start > now
        eco_duration_hours = room_cooling_hours_to_eco or 0.0
        eco_window = self._select_most_expensive_window_block(
            windows=windows,
            now=now,
            duration_hours=eco_duration_hours,
        )
        eco_active_now = eco_window is not None and eco_window["start"] <= now < eco_window["end"]

        battery_enabled = bool(self._config[CONF_BATTERY_ENABLED])
        battery_capacity = float(self._config[CONF_BATTERY_CAPACITY_KWH])
        max_charge = float(self._config[CONF_BATTERY_MAX_CHARGE_KW])
        max_discharge = float(self._config[CONF_BATTERY_MAX_DISCHARGE_KW])
        target_battery_full_by_sunset = battery_enabled and sunset_time is not None and sunset_time > now
        grid_charge_needed_until_sunset = (
            max(0.0, round(battery_capacity - projected_solar_surplus_until_sunset, 3))
            if target_battery_full_by_sunset
            else 0.0
        )
        battery_charge_hours_needed_until_sunset = (
            round(grid_charge_needed_until_sunset / max_charge, 3)
            if target_battery_full_by_sunset and max_charge > 0
            else 0.0
        )
        planned_grid_charge_windows = self._select_cheapest_charge_windows(
            windows=windows,
            now=now,
            until=sunset_time,
            needed_kwh=grid_charge_needed_until_sunset,
            max_charge_kw=max_charge,
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

        if planner_kind in (PLANNER_KIND_COMBINED, PLANNER_KIND_BATTERY) and non_heating_daily_average_kwh > 0:
            rationale_parts.append("non-heating household usage is derived from total energy history")

        heat_pump_strategy = "normal"
        if planner_kind == PLANNER_KIND_BATTERY:
            heat_pump_strategy = "not_applicable"
        elif planner_kind == PLANNER_KIND_THERMOSTAT and eco_active_now:
            heat_pump_strategy = "energy_saving_on"
            score += 8
            recommendation = "set_thermostat_to_eco"
            rationale_parts.append(
                "thermostat should be in eco mode during the selected expensive window"
            )
            if thermostat_eco_setpoint_c is not None:
                rationale_parts.append(
                    f"set the room thermostat to about {thermostat_eco_setpoint_c:.1f} C until the expensive block ends"
                )
        elif cheap_now and (best_solar_is_now or solar_covers_today):
            heat_pump_strategy = "normal"
            rationale_parts.append("heat pump does not need power saving because this is already a cheap solar window")
        elif (
            current_price is not None
            and current_price > cheap_threshold
            and best_solar_window is not None
            and best_solar_window.start > dt_util.now()
            and best_solar_window.forecast_kwh >= 1.0
        ):
            heat_pump_strategy = "energy_saving_on"
            score += 5
            rationale_parts.append("thermostat can wait for a cheaper or sunnier period")
        elif (
            current_price is not None
            and price_spread > 0
            and not cheap_now
            and current_price >= most_expensive.price - (price_spread * 0.15)
        ):
            heat_pump_strategy = "energy_saving_on"
            score += 5
            rationale_parts.append("current price is close to the daily peak, so eco mode is preferred")

        battery_strategy = "accu_uit"
        if planner_kind == PLANNER_KIND_THERMOSTAT:
            battery_strategy = "not_applicable"
        elif battery_enabled:
            if cheap_now and best_solar_is_now:
                battery_strategy = "laden_met_zonne_energie"
                score += 12
                rationale_parts.append(
                    f"battery should use the active solar window and can charge up to {min(max_charge, battery_capacity):.1f} kW"
                )
            elif (
                target_battery_full_by_sunset
                and grid_charge_needed_until_sunset > 0
                and in_planned_grid_charge_window
            ):
                battery_strategy = "laden_van_net"
                score += 12
                rationale_parts.append(
                    f"battery targets a full state by sunset and still needs about {grid_charge_needed_until_sunset:.1f} kWh"
                )
                rationale_parts.append(
                    f"the planner reserved roughly {battery_charge_hours_needed_until_sunset:.1f} charging hours in the cheapest pre-sunset windows"
                )
            elif solar_covers_today and best_solar_is_now:
                battery_strategy = "laden_met_zonne_energie"
                score += 10
                rationale_parts.append(
                    f"battery should keep room for solar charging up to {min(max_charge, battery_capacity):.1f} kW"
                )
                rationale_parts.append("grid charging is not needed because forecast solar covers the expected demand")
            elif (
                current_price is not None
                and future_cheaper_by is not None
                and future_cheaper_by >= battery_min_profit
                and (future_solar_charge_window or future_min_price is not None)
            ):
                battery_strategy = "ontladen"
                score += 10
                rationale_parts.append(
                    f"battery can discharge up to {min(max_discharge, battery_capacity):.1f} kW because a later cheaper charging window is at least {battery_min_profit:.2f} EUR/kWh better"
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
            ):
                battery_strategy = "ontladen"
                score += 8
                rationale_parts.append(
                    "battery can discharge now because later pre-sunset charging windows are cheaper and can refill it before sunset"
                )
            elif (
                cheap_now
                and future_more_expensive_by is not None
                and future_more_expensive_by >= battery_min_profit
                and not solar_covers_today
            ):
                battery_strategy = "laden_van_net"
                score += 10
                rationale_parts.append(
                    f"battery can charge from the grid up to {min(max_charge, battery_capacity):.1f} kW because a later discharge window is at least {battery_min_profit:.2f} EUR/kWh more expensive"
                )
            elif solar_covers_today and future_solar_charge_window:
                battery_strategy = "accu_uit"
                rationale_parts.append("battery can stay idle until the later solar charging window starts")
            elif target_battery_full_by_sunset and grid_charge_needed_until_sunset <= 0:
                battery_strategy = "laden_met_zonne_energie" if best_solar_is_now else "accu_uit"
                rationale_parts.append(
                    "forecast solar after household demand is enough to fill the battery by sunset without grid charging"
                )
            elif target_battery_full_by_sunset and planned_grid_charge_price_ceiling is not None:
                rationale_parts.append(
                    f"grid charging only makes sense in pre-sunset windows up to about {planned_grid_charge_price_ceiling:.3f} EUR/kWh"
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
            room_temperature_c=room_temperature_c,
            thermostat_setpoint_c=thermostat_setpoint_c,
            thermostat_eco_setpoint_c=thermostat_eco_setpoint_c,
            room_cooling_hours_to_eco=room_cooling_hours_to_eco,
            room_cooling_rate_c_per_hour=room_cooling_rate_c_per_hour,
            cooling_reference_outdoor_temp_c=cooling_reference_outdoor_temp_c,
            planned_eco_window_start=eco_window["start"].isoformat() if eco_window else None,
            planned_eco_window_end=eco_window["end"].isoformat() if eco_window else None,
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

        if active_window and not any(w.start == active_window.start and w.end == active_window.end for w in windows):
            windows.insert(0, active_window)

        if not windows and current_price is not None:
            windows.append(PlannerWindow(start=now, end=now + timedelta(hours=1), price=current_price))

        if price_resolution == PRICE_RESOLUTION_HOURLY:
            windows = self._aggregate_price_windows_to_hourly(windows)

        return sorted(windows, key=lambda item: item.start)

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
