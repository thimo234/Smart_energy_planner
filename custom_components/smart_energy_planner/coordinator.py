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
    CONF_HEATING_ENERGY_SENSOR,
    CONF_HEATING_LOOKBACK_DAYS,
    CONF_PRICE_SENSOR,
    CONF_PRICE_RESOLUTION,
    CONF_SOLCAST_TODAY_SENSOR,
    CONF_TEMPERATURE_SENSOR,
    CONF_TOTAL_ENERGY_SENSOR,
    COORDINATOR_UPDATE_INTERVAL,
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
            total_energy_sensor = self._config[CONF_TOTAL_ENERGY_SENSOR]

            price_state = self.hass.states.get(price_sensor)
            solar_state = self.hass.states.get(solar_sensor)
            temperature_state = self.hass.states.get(temperature_sensor)
            heating_state = self.hass.states.get(heating_sensor)
            total_energy_state = self.hass.states.get(total_energy_sensor)

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
            if solar_state and solar_forecast <= 0 and not solar_windows:
                source_status["solcast_today_sensor"] = "no_solcast_forecast_data"
            elif solar_state and solar_forecast is None:
                source_status["solcast_today_sensor"] = "invalid_solcast_value"

            heating_daily_average = (
                await self._async_get_average_daily_usage(heating_sensor) if heating_state else 0.0
            )
            total_energy_daily_average = (
                await self._async_get_average_daily_usage(total_energy_sensor) if total_energy_state else 0.0
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
        total_energy_sensor: str,
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

        cheap_threshold = cheapest.price + (price_spread * 0.25)
        next_cheap = next((window for window in windows if window.price <= cheap_threshold), cheapest)

        battery_enabled = bool(self._config[CONF_BATTERY_ENABLED])
        battery_capacity = float(self._config[CONF_BATTERY_CAPACITY_KWH])
        max_charge = float(self._config[CONF_BATTERY_MAX_CHARGE_KW])
        max_discharge = float(self._config[CONF_BATTERY_MAX_DISCHARGE_KW])

        score = 50
        rationale_parts: list[str] = []
        recommendation = "wait"

        if current_price is not None and current_price <= cheap_threshold:
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
        if (
            current_price is not None
            and current_price > cheap_threshold
            and best_solar_window is not None
            and best_solar_window.start > dt_util.now()
            and best_solar_window.forecast_kwh >= 1.0
        ):
            heat_pump_strategy = "energy_saving_on"
            score += 5
            rationale_parts.append("heat pump can wait for a cheaper or sunnier period")
        elif current_price is not None and current_price >= most_expensive.price - (price_spread * 0.15):
            heat_pump_strategy = "energy_saving_on"
            score += 5
            rationale_parts.append("current price is close to the daily peak")

        battery_strategy = "accu_uit"
        if battery_enabled:
            if solar_forecast_kwh > estimated_total_home_demand_kwh:
                battery_strategy = "laden_met_zonne_energie"
                score += 10
                rationale_parts.append(
                    f"battery should keep room for solar charging up to {min(max_charge, battery_capacity):.1f} kW"
                )
            elif current_price is not None and current_price <= cheap_threshold and solar_forecast_kwh < 4.0:
                battery_strategy = "laden_van_net"
                score += 10
                rationale_parts.append(
                    f"battery can charge from the grid up to {min(max_charge, battery_capacity):.1f} kW"
                )
            else:
                expensive_threshold = most_expensive.price - (price_spread * 0.20)
                if current_price is not None and current_price >= expensive_threshold:
                    battery_strategy = "ontladen"
                    score += 5
                    rationale_parts.append(
                        f"battery can discharge up to {min(max_discharge, battery_capacity):.1f} kW during high prices"
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
            price_resolution=price_resolution,
            source_status=source_status,
            source_errors=source_errors,
            rationale=rationale,
        )

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
            if start_raw is None:
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
