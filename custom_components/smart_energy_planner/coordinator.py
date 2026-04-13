"""Coordinator for Smart Energy Planner."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import logging
import statistics
from typing import Any

from homeassistant.components.recorder import history
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import STATE_UNAVAILABLE, STATE_UNKNOWN
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator
from homeassistant.util import dt as dt_util

from .const import (
    CONF_BATTERY_CAPACITY_KWH,
    CONF_BATTERY_ENABLED,
    CONF_BATTERY_MAX_CHARGE_KW,
    CONF_BATTERY_MAX_DISCHARGE_KW,
    CONF_BATTERY_MIN_PROFIT_PER_KWH,
    CONF_HEATING_ENERGY_SENSOR,
    CONF_HEATING_LOOKBACK_DAYS,
    CONF_HEAT_PUMP_MAX_OFF_HOURS,
    CONF_HEAT_PUMP_MIN_ON_HOURS,
    CONF_PRICE_SENSOR,
    CONF_PRICE_RESOLUTION,
    CONF_SOLCAST_TODAY_SENSOR,
    CONF_TEMPERATURE_SENSOR,
    CONF_TOTAL_ENERGY_SENSOR,
    COORDINATOR_UPDATE_INTERVAL,
    DEFAULT_BATTERY_MIN_PROFIT_PER_KWH,
    DEFAULT_HEAT_PUMP_MAX_OFF_HOURS,
    DEFAULT_HEAT_PUMP_MIN_ON_HOURS,
    DOMAIN,
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
    battery_min_profit_per_kwh: float
    heat_pump_max_off_hours: int
    heat_pump_min_on_hours: int
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
            price_sensor = self._config[CONF_PRICE_SENSOR]
            solar_sensor = self._config[CONF_SOLCAST_TODAY_SENSOR]
            temperature_sensor = self._config[CONF_TEMPERATURE_SENSOR]
            heating_sensor = self._config[CONF_HEATING_ENERGY_SENSOR]
            total_energy_sensor = self._config.get(CONF_TOTAL_ENERGY_SENSOR)

            price_state = self.hass.states.get(price_sensor)
            solar_state = self.hass.states.get(solar_sensor)
            temperature_state = self.hass.states.get(temperature_sensor)
            heating_state = self.hass.states.get(heating_sensor)
            total_energy_state = self.hass.states.get(total_energy_sensor) if total_energy_sensor else None

            source_status = self._build_source_status(
                price_sensor=price_sensor,
                price_state=price_state,
                solar_sensor=solar_sensor,
                solar_state=solar_state,
                temperature_sensor=temperature_sensor,
                temperature_state=temperature_state,
                heating_sensor=heating_sensor,
                heating_state=heating_state,
                total_energy_sensor=total_energy_sensor,
                total_energy_state=total_energy_state,
            )
            source_errors = self._collect_source_errors(source_status)

            if not price_state:
                return self._build_pending_result("waiting_for_price_sensor", source_status, source_errors)

            current_price = _coerce_float(price_state.state)
            price_resolution = str(self._config.get(CONF_PRICE_RESOLUTION, PRICE_RESOLUTION_HOURLY))
            windows = self._extract_price_windows(price_state.attributes, current_price, price_resolution)
            if not windows:
                source_status["price_sensor"] = "no_price_windows"
                return self._build_pending_result(
                    "waiting_for_nordpool_prices", source_status, self._collect_source_errors(source_status)
                )

            solar_forecast = _coerce_float(
                solar_state.attributes.get("estimate") if solar_state else None,
                default=_coerce_float(solar_state.state, default=0.0) if solar_state else 0.0,
            )
            outdoor_temperature = _coerce_float(
                temperature_state.state if temperature_state else None, default=12.0
            )
            solar_windows = self._extract_solar_windows(solar_state.attributes if solar_state else {})
            solcast_confidence = _coerce_float(
                solar_state.attributes.get("analysis", {}).get("confidence") if solar_state else None
            )
            if not solar_windows and solar_forecast and solar_forecast > 0:
                solar_windows = self._build_fallback_solar_windows(solar_forecast)
            if solar_state and solar_forecast <= 0 and not solar_windows:
                source_status["solcast_today_sensor"] = "no_solcast_forecast_data"
            elif solar_state and solar_forecast is None:
                source_status["solcast_today_sensor"] = "invalid_solcast_value"

            heating_daily_average = (
                await self._async_get_average_daily_usage(heating_sensor) if heating_state else 0.0
            )
            total_energy_daily_average = (
                await self._async_get_average_daily_usage(total_energy_sensor)
                if total_energy_state and total_energy_sensor
                else 0.0
            )

            if temperature_state and outdoor_temperature is None:
                source_status["temperature_sensor"] = "invalid_temperature_value"
            if heating_state and heating_daily_average <= 0:
                source_status["heating_energy_sensor"] = "no_heating_history_yet"
            if total_energy_state and total_energy_daily_average <= 0:
                source_status["total_energy_sensor"] = "no_total_energy_history_yet"

            non_heating_daily_average = max(0.0, total_energy_daily_average - heating_daily_average)
            heating_estimate = self._estimate_heating_need(outdoor_temperature, heating_daily_average)

            return self._build_plan(
                windows=windows,
                current_price=current_price,
                solar_forecast_kwh=solar_forecast,
                solar_windows=solar_windows,
                solcast_confidence=solcast_confidence,
                heating_estimate_kwh=heating_estimate,
                lookback_average_kwh=heating_daily_average,
                total_energy_daily_average_kwh=total_energy_daily_average,
                non_heating_daily_average_kwh=non_heating_daily_average,
                price_resolution=price_resolution,
                source_status=source_status,
                source_errors=self._collect_source_errors(source_status),
            )
        except Exception as err:
            _LOGGER.exception("Planner update failed")
            return self._build_pending_result(
                "planner_runtime_error",
                {
                    "price_sensor": "unknown",
                    "solcast_today_sensor": "unknown",
                    "temperature_sensor": "unknown",
                    "heating_energy_sensor": "unknown",
                    "total_energy_sensor": "unknown",
                },
                [f"planner_runtime_error: {err!s}"],
            )

    def _build_pending_result(
        self, status: str, source_status: dict[str, str], source_errors: list[str]
    ) -> PlannerResult:
        return PlannerResult(
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
            battery_min_profit_per_kwh=float(
                self._config.get(CONF_BATTERY_MIN_PROFIT_PER_KWH, DEFAULT_BATTERY_MIN_PROFIT_PER_KWH)
            ),
            heat_pump_max_off_hours=int(
                self._config.get(CONF_HEAT_PUMP_MAX_OFF_HOURS, DEFAULT_HEAT_PUMP_MAX_OFF_HOURS)
            ),
            heat_pump_min_on_hours=int(
                self._config.get(CONF_HEAT_PUMP_MIN_ON_HOURS, DEFAULT_HEAT_PUMP_MIN_ON_HOURS)
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
        heating_sensor: str,
        heating_state,
        total_energy_sensor: str | None,
        total_energy_state,
    ) -> dict[str, str]:
        return {
            "price_sensor": self._state_status(price_sensor, price_state),
            "solcast_today_sensor": self._state_status(solar_sensor, solar_state),
            "temperature_sensor": self._state_status(temperature_sensor, temperature_state),
            "heating_energy_sensor": self._state_status(heating_sensor, heating_state),
            "total_energy_sensor": self._state_status(total_energy_sensor, total_energy_state),
        }

    def _collect_source_errors(self, source_status: dict[str, str]) -> list[str]:
        return [f"{name}: {status}" for name, status in source_status.items() if status != "ok"]

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

    def _build_plan(
        self,
        *,
        windows: list[PlannerWindow],
        current_price: float | None,
        solar_forecast_kwh: float,
        solar_windows: list[SolarWindow],
        solcast_confidence: float | None,
        heating_estimate_kwh: float,
        lookback_average_kwh: float,
        total_energy_daily_average_kwh: float,
        non_heating_daily_average_kwh: float,
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
        heat_pump_max_off_hours = int(
            self._config.get(CONF_HEAT_PUMP_MAX_OFF_HOURS, DEFAULT_HEAT_PUMP_MAX_OFF_HOURS)
        )
        heat_pump_min_on_hours = int(
            self._config.get(CONF_HEAT_PUMP_MIN_ON_HOURS, DEFAULT_HEAT_PUMP_MIN_ON_HOURS)
        )

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

        if heating_estimate_kwh >= lookback_average_kwh * 1.1 and lookback_average_kwh > 0:
            score -= 10
            rationale_parts.append("heating demand is elevated because of lower outdoor temperature")
        elif lookback_average_kwh <= 0:
            rationale_parts.append("recent heating history is not available yet, so heating demand is conservative")

        if non_heating_daily_average_kwh > 0:
            rationale_parts.append("non-heating household usage is derived from total energy history")

        heat_pump_strategy = "normal"
        if cheap_now and (best_solar_is_now or solar_covers_today):
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
            rationale_parts.append(
                f"heat pump can wait for a cheaper or sunnier period for up to {heat_pump_max_off_hours} hours, after which it should run for at least {heat_pump_min_on_hours} hours"
            )
        elif (
            current_price is not None
            and price_spread > 0
            and not cheap_now
            and current_price >= most_expensive.price - (price_spread * 0.15)
        ):
            heat_pump_strategy = "energy_saving_on"
            score += 5
            rationale_parts.append(
                f"current price is close to the daily peak, so power saving may stay on for up to {heat_pump_max_off_hours} hours"
            )

        battery_strategy = "accu_uit"
        if battery_enabled:
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
            battery_min_profit_per_kwh=battery_min_profit,
            heat_pump_max_off_hours=heat_pump_max_off_hours,
            heat_pump_min_on_hours=heat_pump_min_on_hours,
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
