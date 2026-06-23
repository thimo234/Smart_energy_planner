"""Coordinator for Smart Energy Planner."""

from __future__ import annotations

from datetime import date, datetime, timedelta
import logging
import statistics
from typing import Any, cast

from homeassistant.components.recorder import get_instance as get_recorder_instance, history
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import STATE_OFF, STATE_ON, STATE_UNAVAILABLE, STATE_UNKNOWN
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.event import async_track_time_interval
from homeassistant.helpers.storage import Store
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator
from homeassistant.util import dt as dt_util

from .battery_forecast import (
    build_energy_balance_slots,
    build_fallback_solar_windows,
    build_fallback_solar_windows_for_day,
    build_hourly_home_demand_forecast,
    extract_solar_windows,
    get_solar_day_end,
    merge_solar_windows,
    populate_hourly_demand_table,
    sum_remaining_home_demand_until,
    sum_remaining_solar_until,
    select_best_solar_window,
)
from .battery_models import SolarWindow
from .battery_planner import (
    build_battery_mode_schedule,
    build_charge_window_lookup,
    calculate_next_battery_peak_price,
    collapse_short_off_mode_windows,
    merge_planned_windows,
    merge_windows,
    normalize_full_battery_charge_mode,
    normalize_full_battery_mode_windows,
    plan_segment_discharge_kwh,
    select_contiguous_productive_solar_slot_starts,
    summarize_battery_cycles,
)
from .thermostat_planner import (
    THERMOSTAT_MAX_COOLDOWN_HOURS,
    estimate_cooling_profile_from_model,
    find_next_valley_start,
    select_expensive_peak_blocks,
    select_thermostat_peak_eco_windows,
)
from .const import (
    CONF_BATTERY_CAPACITY_KWH,
    CONF_BATTERY_DEMAND_SAFETY_MARGIN,
    CONF_BATTERY_ENABLED,
    CONF_BATTERY_CHARGE_SAFETY_MARGIN,
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
    CONF_PRICE_WINDOW_TYPE,
    CONF_PRICE_WINDOW_DURATION_HOURS,
    CONF_PRICE_WINDOW_WHOLE_HOUR_START,
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
    DEFAULT_BATTERY_DEMAND_SAFETY_MARGIN,
    DEFAULT_BATTERY_ENABLED,
    DEFAULT_BATTERY_CHARGE_SAFETY_MARGIN,
    DEFAULT_BATTERY_MAX_CHARGE_KW,
    DEFAULT_BATTERY_MAX_DISCHARGE_KW,
    DEFAULT_BATTERY_MIN_SOC_PERCENT,
    DEFAULT_BATTERY_MIN_PROFIT_PER_KWH,
    DEFAULT_PRICE_WINDOW_DURATION_HOURS,
    DEFAULT_PRICE_WINDOW_TYPE,
    DEFAULT_PRICE_WINDOW_WHOLE_HOUR_START,
    DEFAULT_THERMOSTAT_ECO_TEMPERATURE,
    DEFAULT_THERMOSTAT_MAX_TEMP,
    DEFAULT_THERMOSTAT_MIN_TEMP,
    DEFAULT_THERMOSTAT_PREHEAT_MINUTES,
    DOMAIN,
    PLANNER_KIND_BATTERY,
    PLANNER_KIND_PRICE_WINDOW,
    PLANNER_KIND_THERMOSTAT,
    PRICE_RESOLUTION_HOURLY,
    PRICE_WINDOW_TYPE_MOST_EXPENSIVE,
    RUNTIME_STATE,
    STORAGE_KEY,
    STORAGE_VERSION,
)
from .price_helpers import (
    aggregate_price_windows_to_hourly,
    build_neutral_price_windows,
    extend_price_window_tail,
    extract_price_average,
    extract_price_windows,
    select_contiguous_price_window,
)
from .price_models import PlannerWindow
from .planner_result import PlannerResult

_LOGGER = logging.getLogger(__name__)
_HISTORY_LOOKBACK_DAYS = 7
# Minimum absolute change (kWh) in the tracked battery energy before the profit
# tracker updates the cost basis. Changes below this are treated as sensor
# noise, but the baseline is still persisted so we don't lose precision across
# Home Assistant restarts.
_BATTERY_PROFIT_NOISE_FLOOR_KWH = 0.01
_DEMAND_PROFILE_ALPHA = 0.35
_DEMAND_PROFILE_MAX_KWH_PER_HOUR = 10.0
_DEMAND_TODAY_ADJUSTMENT_WEIGHT = 0.35
_DEMAND_TODAY_ADJUSTMENT_MIN_COMPLETED_HOURS = 3


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
        self._eco_early_exit_until: datetime | None = None  # kept for state compat, no longer used
        self._locked_eco_window: dict | None = None
        self._locked_preheat_end: datetime | None = None
        self._preheat_expired_at: datetime | None = None  # set when preheat lock times out without eco
        self._source_error_retry_unsub: object | None = None
        self._discharge_session_started: bool = False

    @callback
    def _schedule_source_error_retry(self) -> None:
        if self._source_error_retry_unsub is not None:
            return
        # Self-repeating interval â€” fires every 30s while there are source
        # errors and stops only when _cancel_source_error_retry runs.  This
        # is robust against any code path failing to reschedule a one-shot
        # timer (e.g. an unexpected exception inside _async_update_data).
        _LOGGER.info("Smart Energy Planner: scheduling source error retry (every 30s)")
        self._source_error_retry_unsub = async_track_time_interval(
            self.hass,
            self._handle_source_error_retry,
            timedelta(seconds=30),
        )

    @callback
    def _handle_source_error_retry(self, _now) -> None:
        _LOGGER.info("Smart Energy Planner: source error retry firing")
        self.hass.async_create_task(self.async_refresh())

    def _cancel_source_error_retry(self) -> None:
        if self._source_error_retry_unsub is not None:
            _LOGGER.info("Smart Energy Planner: cancelling source error retry")
            self._source_error_retry_unsub()
            self._source_error_retry_unsub = None

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

            if battery_soc_sensor and battery_soc_state is None:
                similar = sorted(
                    s.entity_id
                    for s in self.hass.states.async_all()
                    if any(kw in s.entity_id.lower() for kw in ("zonnepanelen", "battery", "soc"))
                )
                _LOGGER.warning(
                    "Battery SOC entity '%s' not found in state machine. Available similar entities: %s",
                    battery_soc_sensor,
                    similar or "none",
                )

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
            if source_errors:
                _LOGGER.warning("Smart Energy Planner source errors: %s", source_errors)
                self._schedule_source_error_retry()
            else:
                self._cancel_source_error_retry()

            current_price = _coerce_float(price_state.state) if price_state else None
            export_current_price = (
                _coerce_float(export_price_state.state)
                if export_price_state
                else current_price
            )
            price_resolution = str(self._config.get(CONF_PRICE_RESOLUTION, PRICE_RESOLUTION_HOURLY))
            windows = extract_price_windows(
                price_state.attributes if price_state else {},
                current_price,
                price_resolution,
            )
            all_windows = extract_price_windows(
                price_state.attributes if price_state else {},
                current_price,
                price_resolution,
                include_past=True,
            )
            export_windows = extract_price_windows(
                export_price_state.attributes if export_price_state else (price_state.attributes if price_state else {}),
                export_current_price,
                price_resolution,
            )
            all_export_windows = extract_price_windows(
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
            price_average = extract_price_average(
                price_state.attributes if price_state else {},
                windows,
            )
            export_price_average = extract_price_average(
                export_price_state.attributes if export_price_state else (price_state.attributes if price_state else {}),
                export_windows,
            )

            if not price_state:
                source_status["price_sensor"] = "waiting_for_price_sensor"
                if planner_kind == PLANNER_KIND_THERMOSTAT:
                    windows = build_neutral_price_windows(current_price)
                else:
                    neutral_windows = build_neutral_price_windows(current_price, hours=48)
                    windows = list(neutral_windows)
                    all_windows = list(neutral_windows)
                    export_windows = list(neutral_windows)
                    all_export_windows = list(neutral_windows)
                    battery_switch_windows = list(neutral_windows)

            if not windows:
                source_status["price_sensor"] = "no_price_windows"
                if planner_kind == PLANNER_KIND_THERMOSTAT:
                    windows = build_neutral_price_windows(current_price)
                else:
                    neutral_windows = build_neutral_price_windows(current_price, hours=48)
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
            solar_windows = extract_solar_windows(solar_state.attributes if solar_state else {})
            solar_windows.extend(
                extract_solar_windows(solar_tomorrow_state.attributes if solar_tomorrow_state else {})
            )
            all_solar_windows = extract_solar_windows(
                solar_state.attributes if solar_state else {},
                include_past=True,
            )
            all_solar_windows.extend(
                extract_solar_windows(
                    solar_tomorrow_state.attributes if solar_tomorrow_state else {},
                    include_past=True,
                )
            )
            solcast_confidence = _coerce_float(
                solar_state.attributes.get("analysis", {}).get("confidence") if solar_state else None
            )
            if planner_kind == PLANNER_KIND_BATTERY and not solar_windows and solar_forecast and solar_forecast > 0:
                fallback_today_windows = build_fallback_solar_windows(solar_forecast)
                solar_windows = [*solar_windows, *fallback_today_windows]
                all_solar_windows = [*all_solar_windows, *fallback_today_windows]
            if (
                planner_kind == PLANNER_KIND_BATTERY
                and solar_tomorrow_state
                and not any(window.start.date() > dt_util.now().date() for window in solar_windows)
                and solar_tomorrow_forecast
                and solar_tomorrow_forecast > 0
            ):
                fallback_tomorrow_windows = build_fallback_solar_windows_for_day(
                    solar_tomorrow_forecast,
                    day_offset=1,
                )
                solar_windows.extend(fallback_tomorrow_windows)
                all_solar_windows.extend(fallback_tomorrow_windows)
            solar_windows = merge_solar_windows(solar_windows)
            all_solar_windows = merge_solar_windows(all_solar_windows)
            if planner_kind == PLANNER_KIND_BATTERY:
                battery_price_horizon_end = max(
                    [window.end for window in [*all_windows, *all_solar_windows]],
                    default=now + timedelta(days=1),
                )
                all_windows = extend_price_window_tail(
                    windows=all_windows,
                    horizon_end=battery_price_horizon_end,
                    fallback_price=current_price,
                )
                all_export_windows = extend_price_window_tail(
                    windows=all_export_windows,
                    horizon_end=battery_price_horizon_end,
                    fallback_price=export_current_price,
                )
                battery_switch_windows = extend_price_window_tail(
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
            if (
                planner_kind == PLANNER_KIND_BATTERY
                and battery_soc_state is not None
                and battery_soc_state.state not in (STATE_UNAVAILABLE, STATE_UNKNOWN, "")
                and battery_soc_percent is None
            ):
                source_status["battery_soc_sensor"] = (
                    f"invalid_battery_soc_value ({battery_soc_sensor}: state={battery_soc_state.state!r})"
                )

            # Re-check after late-discovered overrides (e.g. no_total_energy_history_yet
            # is set above, after the initial retry decision at the top of this method).
            late_source_errors = self._collect_source_errors(source_status)
            if late_source_errors:
                if late_source_errors != source_errors:
                    _LOGGER.warning("Smart Energy Planner late source errors: %s", late_source_errors)
                self._schedule_source_error_retry()
            elif not source_errors:
                self._cancel_source_error_retry()

            if planner_kind != PLANNER_KIND_BATTERY:
                total_energy_daily_average = 0.0
                non_heating_daily_average = 0.0
                heating_estimate = 0.0
                hourly_demand_table: dict[str, float] = {}
                demand_adjustment_factor = 1.0
            else:
                non_heating_daily_average = total_energy_daily_average
                heating_estimate = 0.0
                hourly_demand_table = {}
                demand_adjustment_factor = 1.0
                if total_energy_sensor and total_energy_state:
                    current_energy = _coerce_float(total_energy_state.state)
                    if current_energy is not None:
                        hourly_demand_table = await self._async_update_hourly_demand_table(
                            current_value=current_energy,
                            entity_id=total_energy_sensor,
                            now=now,
                        )
                    else:
                        hourly_demand_table = dict(
                            (self.hass.data.get(RUNTIME_STATE, {})
                             .get(self.config_entry.entry_id, {})
                             .get("hourly_demand_table") or {})
                        )
                    demand_adjustment_factor = (
                        _coerce_float(
                            self.hass.data.get(RUNTIME_STATE, {})
                            .get(self.config_entry.entry_id, {})
                            .get("hourly_demand_adjustment_factor")
                        )
                        or 1.0
                    )

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

            # Apply EMA (Î±=0.2) to smooth hours_to_eco and suppress per-refresh noise.
            raw_eco_hours = cooling_profile.get("hours_to_eco")
            if raw_eco_hours is not None and planner_kind == PLANNER_KIND_THERMOSTAT:
                _rs = self.hass.data.setdefault(RUNTIME_STATE, {}).setdefault(
                    self.config_entry.entry_id, {}
                )
                last_smoothed = _coerce_float(_rs.get("smoothed_eco_hours"))
                smoothed = (
                    round(last_smoothed + (raw_eco_hours - last_smoothed) * 0.2, 2)
                    if last_smoothed is not None
                    else round(raw_eco_hours, 2)
                )
                _rs["smoothed_eco_hours"] = smoothed
                cooling_profile["hours_to_eco"] = smoothed

            # Eco window duration = time to cool from the CURRENT room temperature to the
            # eco setpoint, so the scheduled eco block always covers the full cool-down
            # starting from wherever the room actually is right now.
            cooling_hours_from_current: float | None = None
            if (
                planner_kind == PLANNER_KIND_THERMOSTAT
                and room_temperature is not None
                and thermostat_eco_setpoint is not None
            ):
                if room_temperature <= thermostat_eco_setpoint:
                    cooling_hours_from_current = 0.0
                else:
                    _cur_delta = room_temperature - thermostat_eco_setpoint
                    _last_rate = cooling_profile.get("last_observed_rate_c_per_hour")
                    _last_delta_t = cooling_profile.get("last_observed_delta_temp_c")
                    _cur_delta_t = (
                        room_temperature - outdoor_temperature
                        if outdoor_temperature is not None else None
                    )
                    if (
                        _last_rate is not None and _last_rate > 0
                        and _last_delta_t is not None and _last_delta_t > 0
                        and _cur_delta_t is not None and _cur_delta_t > 0
                    ):
                        _adjusted_rate = _last_rate * (_cur_delta_t / _last_delta_t)
                        cooling_hours_from_current = round(
                            min(_cur_delta / max(_adjusted_rate, 0.01), THERMOSTAT_MAX_COOLDOWN_HOURS), 2
                        )
                    else:
                        _model_hours = cooling_profile.get("hours_to_eco")
                        _cooldown_ref = max(room_temperature, thermostat_setpoint or room_temperature)
                        _cooldown_delta = _cooldown_ref - thermostat_eco_setpoint
                        if _model_hours is not None and _cooldown_delta > 0:
                            cooling_hours_from_current = round(
                                min((_cur_delta / _cooldown_delta) * _model_hours, THERMOSTAT_MAX_COOLDOWN_HOURS), 2
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
                    hourly_demand_table=hourly_demand_table,
                    demand_adjustment_factor=demand_adjustment_factor,
                    room_temperature_c=room_temperature,
                    thermostat_setpoint_c=thermostat_setpoint,
                    thermostat_cool_setpoint_c=thermostat_cool_setpoint,
                    thermostat_preheat_setpoint_c=thermostat_preheat_setpoint,
                    thermostat_eco_setpoint_c=thermostat_eco_setpoint,
                    room_cooling_hours_to_eco=cooling_hours_from_current if cooling_hours_from_current is not None else cooling_profile.get("hours_to_eco"),
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
            if cooling_hours_from_current is not None:
                result.room_cooling_hours_to_eco = cooling_hours_from_current
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
            # Always schedule a retry when an unexpected exception occurs so the
            # integration can recover without a manual reload.
            self._schedule_source_error_retry()
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
            upcoming_energy_price_windows=[],
            estimated_total_home_demand_kwh=0.0,
            estimated_hourly_home_demand=[],
            estimated_hourly_solar_forecast=[],
            projected_remaining_solar_until_sunset_kwh=0.0,
            projected_remaining_home_demand_until_sunset_kwh=0.0,
            projected_solar_surplus_until_sunset_kwh=0.0,
            grid_charge_needed_until_sunset_kwh=0.0,
            battery_charge_hours_needed_until_sunset=0.0,
            target_battery_full_by_sunset=False,
            planned_grid_charge_windows=[],
            planned_solar_charge_windows=[],
            planned_battery_mode_schedule=[],
            planned_battery_mode_windows=[],
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
            selected_price_window=None,
            price_window_type=None,
            cheapest_price_window=None,
            tomorrow_cheapest_price_window=None,
            most_expensive_price_window=None,
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

        if planner_kind == PLANNER_KIND_PRICE_WINDOW:
            return {
                "price_sensor": self._state_status(price_sensor, price_state),
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
            parts = [p.lower() for p in entity_id.replace(".", "_").split("_") if len(p) > 2]
            # Use first two significant parts + last part for broadest matching
            keywords = list({*parts[1:3], parts[-1]} if parts else set())
            similar = sorted(
                s.entity_id
                for s in self.hass.states.async_all()
                if any(kw in s.entity_id.lower() for kw in keywords)
            )[:10]
            hint = f" | similar: {similar}" if similar else " | no similar entities in state machine"
            return f"entity_not_found ({entity_id}){hint}"
        if state.state in (STATE_UNKNOWN, STATE_UNAVAILABLE, ""):
            return f"entity_unavailable ({entity_id})"
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

    async def _async_update_hourly_demand_table(
        self,
        *,
        current_value: float,
        entity_id: str | None = None,
        now: datetime,
    ) -> dict[str, float]:
        """Maintain a 168-slot (7 Ã— 24) table of hourly energy consumption.

        Each slot key is str(weekday * 24 + hour). Complete recorder history
        hours are preferred because they line up with real hour boundaries. A
        recorder-derived slot replaces the previous slot value instead of being
        EMA-applied on every refresh; otherwise the same history sample would be
        counted over and over. If recorder history is not available yet we fall
        back to the previous incremental EMA update path.
        """
        runtime_state = self.hass.data.setdefault(RUNTIME_STATE, {}).setdefault(
            self.config_entry.entry_id, {}
        )
        table: dict[str, float] = dict(runtime_state.get("hourly_demand_table") or {})
        observed_slots = {
            str(slot)
            for slot in (
                runtime_state.get("hourly_demand_observed_slots")
                or table.keys()
            )
        }
        last_value = _coerce_float(runtime_state.get("hourly_demand_last_value"))
        last_hour_key = runtime_state.get("hourly_demand_last_hour_key")
        last_ts_raw = runtime_state.get("hourly_demand_last_ts")
        current_hour_key = now.weekday() * 24 + now.hour
        changed = False
        updated_from_history = False

        if entity_id:
            hourly_usage = await self._async_get_hourly_energy_usage(entity_id, horizon_end=now)
            runtime_state["hourly_demand_adjustment_factor"] = _calculate_demand_adjustment_factor(
                hourly_usage=hourly_usage,
                table=table,
                now=now,
            )
            usage_by_slot: dict[str, list[float]] = {}
            for hour_start, usage_kwh in hourly_usage.items():
                if hour_start + timedelta(hours=1) > now:
                    continue
                if not 0.0 <= usage_kwh <= _DEMAND_PROFILE_MAX_KWH_PER_HOUR:
                    continue
                slot = str(hour_start.weekday() * 24 + hour_start.hour)
                usage_by_slot.setdefault(slot, []).append(usage_kwh)

            for slot, values in usage_by_slot.items():
                historical_hourly = _robust_hourly_demand(values)
                if historical_hourly is None:
                    continue
                table[slot] = round(historical_hourly, 4)
                observed_slots.add(slot)
                updated_from_history = True

        if last_value is None or last_hour_key is None:
            # First call: store the baseline without updating any slot.
            runtime_state["hourly_demand_last_value"] = round(current_value, 3)
            runtime_state["hourly_demand_last_hour_key"] = current_hour_key
            runtime_state["hourly_demand_last_ts"] = now.isoformat()
            changed = True
        elif last_hour_key != current_hour_key and not updated_from_history:
            # The hour has turned: compute consumption of the completed hour.
            last_ts = dt_util.parse_datetime(last_ts_raw) if last_ts_raw else None
            elapsed_hours = (
                max(0.1, (now - last_ts).total_seconds() / 3600) if last_ts else 1.0
            )
            delta = current_value - last_value
            # If HA was down for more than one hour normalise to per-hour.
            hourly_delta = delta / elapsed_hours if elapsed_hours > 1.5 else delta
            # Cap and reject negatives (meter reset / rollover).
            if 0.0 <= hourly_delta <= _DEMAND_PROFILE_MAX_KWH_PER_HOUR:
                slot = str(last_hour_key)
                existing = _coerce_float(table.get(slot))
                if existing is None:
                    table[slot] = round(hourly_delta, 4)
                else:
                    table[slot] = round(existing + (hourly_delta - existing) * 0.2, 4)
                observed_slots.add(slot)
            runtime_state["hourly_demand_last_value"] = round(current_value, 3)
            runtime_state["hourly_demand_last_hour_key"] = current_hour_key
            runtime_state["hourly_demand_last_ts"] = now.isoformat()
            runtime_state["hourly_demand_table"] = table
            changed = True
        elif updated_from_history:
            runtime_state["hourly_demand_last_value"] = round(current_value, 3)
            runtime_state["hourly_demand_last_hour_key"] = current_hour_key
            runtime_state["hourly_demand_last_ts"] = now.isoformat()
            runtime_state["hourly_demand_table"] = table
            changed = True

        if changed:
            table = populate_hourly_demand_table(table, observed_slots=observed_slots)
            runtime_state["hourly_demand_table"] = table
            runtime_state["hourly_demand_observed_slots"] = sorted(observed_slots)
            await self._async_persist_runtime_state(runtime_state)

        return table

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
            "hourly_demand_table": runtime_state.get("hourly_demand_table", {}),
            "hourly_demand_observed_slots": runtime_state.get("hourly_demand_observed_slots", []),
            "hourly_demand_adjustment_factor": runtime_state.get("hourly_demand_adjustment_factor", 1.0),
            "hourly_demand_last_value": runtime_state.get("hourly_demand_last_value"),
            "hourly_demand_last_hour_key": runtime_state.get("hourly_demand_last_hour_key"),
            "hourly_demand_last_ts": runtime_state.get("hourly_demand_last_ts"),
            "smoothed_eco_hours": runtime_state.get("smoothed_eco_hours"),
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

            history_result = await get_recorder_instance(self.hass).async_add_executor_job(_load_history)
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

            history_result = await get_recorder_instance(self.hass).async_add_executor_job(_load_history)
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
                "last_observed_rate_c_per_hour": None,
                "last_observed_delta_temp_c": None,
            }

        runtime_state = self.hass.data.get(RUNTIME_STATE, {}).get(self.config_entry.entry_id, {})
        cooling_model = runtime_state.get("cooling_model", {})
        last_observed_rate = _coerce_float(cooling_model.get("last_observed_drop_c_per_hour"))
        last_observed_delta = _coerce_float(cooling_model.get("last_delta_temp_c"))

        cooldown_reference_temperature = max(room_temperature_c, thermostat_setpoint_c)
        cooldown_delta = max(cooldown_reference_temperature - thermostat_eco_setpoint_c, 0.3)
        estimated_rate, hours_to_eco = estimate_cooling_profile_from_model(
            cooling_model=cooling_model,
            outdoor_temperature_c=outdoor_temperature_c,
            room_temperature_c=cooldown_reference_temperature,
            cooldown_delta_c=cooldown_delta,
        )
        reference_outdoor = outdoor_temperature_c

        return {
            "hours_to_eco": round(hours_to_eco, 2),
            "cooling_rate_c_per_hour": round(estimated_rate, 3),
            "reference_outdoor_temp_c": reference_outdoor,
            "last_observed_rate_c_per_hour": last_observed_rate,
            "last_observed_delta_temp_c": last_observed_delta,
        }

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
        hourly_demand_table: dict[str, float],
        demand_adjustment_factor: float,
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
        if planner_kind == PLANNER_KIND_PRICE_WINDOW:
            duration_hours = _coerce_float(
                self._config.get(CONF_PRICE_WINDOW_DURATION_HOURS),
                float(DEFAULT_PRICE_WINDOW_DURATION_HOURS),
            ) or float(DEFAULT_PRICE_WINDOW_DURATION_HOURS)
            price_window_type = str(self._config.get(CONF_PRICE_WINDOW_TYPE, DEFAULT_PRICE_WINDOW_TYPE))
            wants_most_expensive = price_window_type == PRICE_WINDOW_TYPE_MOST_EXPENSIVE
            whole_hour_start = bool(
                self._config.get(
                    CONF_PRICE_WINDOW_WHOLE_HOUR_START,
                    DEFAULT_PRICE_WINDOW_WHOLE_HOUR_START,
                )
            )
            cheapest_price_window = select_contiguous_price_window(
                all_windows,
                now=now,
                duration_hours=duration_hours,
                cheapest=True,
                whole_hour_start=whole_hour_start,
            )
            tomorrow_cheapest_price_window = select_contiguous_price_window(
                all_windows,
                now=now,
                duration_hours=duration_hours,
                cheapest=True,
                whole_hour_start=whole_hour_start,
                day_offset=1,
            )
            most_expensive_price_window = select_contiguous_price_window(
                all_windows,
                now=now,
                duration_hours=duration_hours,
                cheapest=False,
                whole_hour_start=whole_hour_start,
            )
            selected_price_window = (
                most_expensive_price_window
                if wants_most_expensive
                else cheapest_price_window
            )
            day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            day_end = day_start + timedelta(days=1)
            result = self._build_pending_result(
                "ready_with_warnings" if source_errors else "ready",
                planner_kind,
                source_status,
                source_errors,
            )
            result.recommendation = "price_window"
            result.current_price = current_price
            result.price_spread = price_spread
            result.next_window_start = selected_price_window.get("start") if selected_price_window else None
            result.next_window_end = selected_price_window.get("end") if selected_price_window else None
            result.next_window_price = (
                float(selected_price_window["average_price"]) if selected_price_window else None
            )
            result.next_high_price_window_start = (
                str(most_expensive_price_window["start"]) if most_expensive_price_window else None
            )
            result.next_high_price_window_price = (
                float(most_expensive_price_window["average_price"]) if most_expensive_price_window else None
            )
            result.upcoming_energy_price_windows = self._serialize_price_windows(
                all_windows,
                horizon_start=day_start,
                horizon_end=day_end,
            )
            result.selected_price_window = selected_price_window
            result.price_window_type = price_window_type
            result.cheapest_price_window = cheapest_price_window
            result.tomorrow_cheapest_price_window = tomorrow_cheapest_price_window
            result.most_expensive_price_window = most_expensive_price_window
            result.rationale = (
                f"selected {duration_hours:g} contiguous hour price windows between "
                f"{day_start.isoformat()} and {day_end.isoformat()}"
            )
            return result

        today_solar_windows = [window for window in solar_windows if window.start.date() == now.date()]
        future_solar_windows = [window for window in solar_windows if window.start.date() > now.date()]
        best_solar_window = select_best_solar_window(today_solar_windows or solar_windows)
        planning_horizon_end = max(
            [window.end for window in [*all_windows, *all_solar_windows]],
            default=now + timedelta(days=1),
        )
        planning_start = min(
            (window.start for window in all_windows),
            default=now.replace(hour=0, minute=0, second=0, microsecond=0),
        )
        estimated_hourly_home_demand = build_hourly_home_demand_forecast(
            non_heating_daily_average_kwh=non_heating_daily_average_kwh,
            heating_estimate_kwh=heating_estimate_kwh,
            hourly_demand_table=hourly_demand_table,
            demand_adjustment_factor=demand_adjustment_factor,
            horizon_end=planning_horizon_end,
        )
        demand_safety_margin = (
            self._battery_demand_safety_margin()
            if planner_kind == PLANNER_KIND_BATTERY
            else 0.0
        )
        estimated_hourly_home_demand = self._apply_hourly_demand_safety_margin(
            estimated_hourly_home_demand,
            demand_safety_margin=demand_safety_margin,
        )
        estimated_hourly_solar_forecast = self._serialize_solar_windows(
            all_solar_windows,
            horizon_start=planning_start,
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
        sunset_time = get_solar_day_end(today_solar_windows)
        planning_horizon_solar_end = get_solar_day_end(solar_windows)
        remaining_solar_until_sunset = sum_remaining_solar_until(today_solar_windows, now, sunset_time)
        remaining_home_demand_until_sunset = sum_remaining_home_demand_until(
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
            sum_remaining_solar_until(today_solar_windows, now, sunset_time) >= remaining_home_demand_until_sunset
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
        eco_duration_hours = room_cooling_hours_to_eco or 0.0
        eco_expensive_threshold = (
            mid_price_threshold if planner_kind == PLANNER_KIND_THERMOSTAT else expensive_threshold
        )
        eco_windows = []
        thermostat_planning_error: str | None = None
        if price_signal_available:
            try:
                if planner_kind == PLANNER_KIND_THERMOSTAT:
                    eco_windows = select_thermostat_peak_eco_windows(
                        windows=all_windows,
                        now=now,
                        cooldown_hours=eco_duration_hours,
                        expensive_threshold=eco_expensive_threshold,
                    )
                else:
                    eco_windows = select_expensive_peak_blocks(
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
        active_eco_window = next(
            (window for window in eco_windows if window["start"] <= now < window["end"]),
            None,
        )
        # Lock the eco window once entered so that mid-session price updates
        # (e.g. day-ahead prices arriving at ~13:00) cannot silently switch to
        # a different window and break the active eco session.
        if self._locked_eco_window is not None:
            locked_end = cast(datetime, self._locked_eco_window["end"])
            room_reached_eco = (
                planner_kind == PLANNER_KIND_THERMOSTAT
                and room_temperature_c is not None
                and thermostat_eco_setpoint_c is not None
                and room_temperature_c <= thermostat_eco_setpoint_c
            )
            if now >= locked_end or room_reached_eco:
                self._locked_eco_window = None  # window expired or room cooled enough
            else:
                active_eco_window = self._locked_eco_window  # keep running window
        if self._locked_eco_window is None and active_eco_window is not None:
            self._locked_eco_window = active_eco_window  # latch new window

        preheat_minutes = int(
            self._config.get(CONF_THERMOSTAT_PREHEAT_MINUTES, DEFAULT_THERMOSTAT_PREHEAT_MINUTES)
        )
        # Preheat ends exactly when the eco window begins (window["start"]), so the
        # room warms up before eco drops the setpoint â€” not during it.
        # With multiple eco peaks, clamp each preheat start to not overlap with the
        # preceding eco window â€” otherwise preheat for peak N+1 suppresses eco for peak N.
        sorted_eco_for_preheat = sorted(eco_windows, key=lambda w: w["start"])
        preheat_windows = []
        for idx, eco_win in enumerate(sorted_eco_for_preheat):
            if preheat_minutes <= 0:
                break
            eco_start = cast(datetime, eco_win["start"])
            raw_start = eco_start - timedelta(minutes=preheat_minutes)
            prev_eco_end = cast(datetime, sorted_eco_for_preheat[idx - 1]["end"]) if idx > 0 else None
            clamped_start = max(
                raw_start,
                now.replace(hour=0, minute=0, second=0, microsecond=0),
                *(([prev_eco_end]) if prev_eco_end is not None else []),
            )
            if clamped_start < eco_start and eco_start > now:
                preheat_windows.append(
                    {"start": clamped_start, "end": eco_start, "average_price": eco_win["average_price"]}
                )
        preheat_window = next(
            (
                window
                for window in preheat_windows
                if window["start"] <= now < window["end"]
            ),
            next((window for window in preheat_windows if window["start"] > now), None),
        )
        preheat_active_now = any(window["start"] <= now < window["end"] for window in preheat_windows)
        # â”€â”€ Step 1: resolve an existing lock â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        # Lock preheat once it activates so that eco-window drift (caused by
        # EMA updates to hours_to_eco) cannot push the preheat-window start
        # past NOW and revert the mode to "normal" mid-preheat.  The lock is
        # released as soon as eco starts (locked_eco_window is set) or the
        # locked end-time is reached.
        if self._locked_preheat_end is not None:
            if self._locked_eco_window is not None:
                # Eco started â€” normal end of preheat session.
                self._locked_preheat_end = None
                self._preheat_expired_at = None
            elif now >= self._locked_preheat_end:
                # Timeout without eco starting: record the expiry timestamp so
                # we can suppress a back-to-back session (see Step 2 below).
                self._preheat_expired_at = self._locked_preheat_end
                self._locked_preheat_end = None
            else:
                preheat_active_now = True  # hold preheat mode across planning ticks
        # â”€â”€ Step 2: suppress back-to-back sessions â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        # When the previous preheat session timed out without eco starting
        # (e.g. day-ahead prices arriving at 13:00 shifted the eco window
        # 2 h forward) a new preheat window may start at exactly the moment
        # the old one expired.  Without this guard that would double the total
        # preheat duration.  Suppress any preheat window whose start â‰¤ the
        # recorded expiry; the suppression is lifted once eco starts.
        if self._preheat_expired_at is not None:
            if self._locked_eco_window is not None:
                self._preheat_expired_at = None  # eco arrived â€” fresh cycle
            elif preheat_active_now and any(
                w["start"] <= self._preheat_expired_at
                for w in preheat_windows
                if w["start"] <= now < w["end"]
            ):
                preheat_active_now = False
        # â”€â”€ Step 3: latch a new lock for this session â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        if preheat_active_now and self._locked_preheat_end is None:
            preheat_eco_start = next(
                (cast(datetime, w["end"]) for w in preheat_windows if w["start"] <= now < w["end"]),
                None,
            )
            if preheat_eco_start is not None:
                self._locked_preheat_end = preheat_eco_start
        # Never preheat while a locked eco session is running: preheating would
        # counteract the active eco session AND break eco_active_now via the
        # preheat_active_now condition.  The preheat for the NEXT eco cycle is
        # applied naturally once the locked window expires.
        if self._locked_eco_window is not None:
            preheat_active_now = False

        # Eco continues for the full planned window regardless of individual cheap
        # price slots within it â€” interrupting eco mid-window for a cheap slot
        # prevents the room from ever cooling to eco temperature.
        # Eco may only be interrupted early when no proper cheap valley remains
        # after the eco block ends.  A valley requires at least 4 cheap windows.
        next_valley_reachable = True
        if active_eco_window is not None:
            eco_end_ts = cast(datetime, active_eco_window["end"])
            windows_after_eco = [w for w in all_windows if w.start >= eco_end_ts]
            # Only evaluate when enough post-eco data exists; if the eco window
            # extends to or beyond the price-data horizon, assume a valley is
            # reachable rather than prematurely breaking eco.
            if len(windows_after_eco) >= 8:
                # The room is already cooling during eco, so the cooling-time
                # offset is NOT applied here â€” using now + time_to_eco would
                # push the search window 18 h into the future while the room is
                # already cooling, causing eco to break prematurely.
                # Instead, require at least 4 consecutive cheap windows after
                # eco ends (= a proper valley, as requested by the user).
                cheap_reachable = [
                    w for w in all_windows
                    if w.start >= eco_end_ts and w.price <= average_price
                ]
                next_valley_reachable = len(cheap_reachable) >= 4
        eco_active_now = (
            active_eco_window is not None
            and not preheat_active_now
            and next_valley_reachable
        )
        # Dynamic permanent-eco: instead of a fixed 18-hour cap, check whether
        # the room can actually cool to eco setpoint before the next cheap valley.
        # The "next valley" is the first cheap window (price <= average) that
        # follows the next upcoming expensive period.  If the remaining cooling
        # time (from the CURRENT room temperature, not the setpoint) exceeds
        # hours_to_next_valley, the room won't reach eco setpoint in time â€” so
        # force eco continuously.  Once the room has cooled enough the condition
        # stops being met and normal scheduling resumes.
        if planner_kind == PLANNER_KIND_THERMOSTAT and eco_duration_hours > 0:
            # Compute remaining time to reach eco setpoint from the actual current
            # room temperature rather than from the (higher) setpoint temperature.
            # This prevents forcing eco when the room is already close to eco temp.
            if (
                room_temperature_c is not None
                and thermostat_eco_setpoint_c is not None
                and room_cooling_rate_c_per_hour is not None
                and room_cooling_rate_c_per_hour > 0
                and room_temperature_c > thermostat_eco_setpoint_c
            ):
                _current_delta = room_temperature_c - thermostat_eco_setpoint_c
                _eco_hours_from_current = _current_delta / room_cooling_rate_c_per_hour
            elif (
                room_temperature_c is not None
                and thermostat_eco_setpoint_c is not None
                and room_temperature_c <= thermostat_eco_setpoint_c
            ):
                _eco_hours_from_current = 0.0  # room already at or below eco temp
            else:
                _eco_hours_from_current = eco_duration_hours  # fallback to setpoint-based
            _next_valley_start = find_next_valley_start(
                all_windows, now, average_price
            )
            if _next_valley_start is not None:
                hours_to_next_valley = max(
                    0.0, (_next_valley_start - now).total_seconds() / 3600
                )
                if _eco_hours_from_current > hours_to_next_valley:
                    eco_active_now = True
                    preheat_active_now = False
                    self._locked_preheat_end = None
        eco_window = (
            active_eco_window
            if eco_active_now
            else next((window for window in eco_windows if window["start"] > now), None)
        )

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
        energy_balance_slots = build_energy_balance_slots(
            price_windows=battery_switch_windows or all_windows,
            export_price_windows=all_export_windows or export_windows or all_windows,
            solar_windows=all_solar_windows,
            hourly_demand=estimated_hourly_home_demand,
            horizon_start=now,
            demand_safety_margin=0.0,
        )
        charge_safety_margin = max(
            0.0,
            min(0.5, float(self._config.get(CONF_BATTERY_CHARGE_SAFETY_MARGIN, DEFAULT_BATTERY_CHARGE_SAFETY_MARGIN)) / 100.0),
        )
        planned_solar_charge_windows, planned_grid_charge_windows = self._plan_charge_windows_for_horizon(
            slots=energy_balance_slots,
            now=now,
            usable_capacity_kwh=usable_battery_capacity_kwh,
            current_remaining_capacity_kwh=remaining_usable_capacity_kwh,
            max_charge_kw=max_charge,
            battery_min_profit=battery_min_profit,
            charge_safety_margin=charge_safety_margin,
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
            sum_remaining_home_demand_until(estimated_hourly_home_demand, now, next_charge_opportunity),
            3,
        )
        # No energy reservation: the battery drains to empty at the end of every
        # discharge window and fills to full at the end of every charge window.
        battery_reserved_energy_kwh = 0.0
        battery_energy_available_for_discharge_kwh = battery_energy_available_kwh
        battery_exportable_energy_kwh = battery_energy_available_kwh
        battery_room_needed_for_solar_kwh = 0.0
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
            usable_capacity_kwh=usable_battery_capacity_kwh,
            average_price=average_price,
            average_export_price=export_price_average if export_price_average is not None else average_price,
            max_charge_kw=max_charge,
            max_discharge_kw=max_discharge,
        )
        if planner_kind == PLANNER_KIND_BATTERY and battery_soc_percent is None:
            full_planned_mode_windows = []
            planned_current_mode = "accu_uit"
        elif planner_kind == PLANNER_KIND_BATTERY:
            full_planned_mode_windows = normalize_full_battery_mode_windows(
                windows=full_planned_mode_windows,
                usable_energy_kwh=battery_energy_available_kwh,
                usable_capacity_kwh=usable_battery_capacity_kwh,
            )
            full_planned_mode_windows = collapse_short_off_mode_windows(
                full_planned_mode_windows,
            )
            planned_current_mode = normalize_full_battery_charge_mode(
                mode=planned_current_mode,
                usable_energy_kwh=battery_energy_available_kwh,
                usable_capacity_kwh=usable_battery_capacity_kwh,
            )
            planned_current_mode = _mode_at_time(full_planned_mode_windows, now) or planned_current_mode

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
        battery_cycle_summary = summarize_battery_cycles(
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
        elif planner_kind == PLANNER_KIND_THERMOSTAT and cheap_now and any(
            cast(datetime, w["start"]) > now for w in eco_windows
        ):
            heat_pump_strategy = "normal"
            score += 4
            rationale_parts.append(
                "price is cheap now and an eco window is upcoming: good time to preheat the floor"
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
                elif battery_total_energy_kwh <= minimum_battery_reserve_kwh and battery_soc_percent is not None:
                    rationale_parts.append(
                        f"battery stays above the configured minimum reserve of {battery_min_soc_percent:.0f}%"
                    )
                else:
                    rationale_parts.append(
                        "battery is idle because the current hour is outside the planned charge and discharge phases"
                    )

        planned_battery_mode_schedule = build_battery_mode_schedule(
            planning_start=planning_start,
            full_planned_mode_windows=full_planned_mode_windows,
        )
        planned_battery_mode_windows = _serialize_mode_windows(full_planned_mode_windows)

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
            upcoming_energy_price_windows=self._serialize_price_windows(
                all_windows,
                horizon_start=now - timedelta(hours=1),
                horizon_end=planning_horizon_end,
            ),
            estimated_total_home_demand_kwh=estimated_total_home_demand_kwh,
            estimated_hourly_home_demand=estimated_hourly_home_demand,
            estimated_hourly_solar_forecast=estimated_hourly_solar_forecast,
            projected_remaining_solar_until_sunset_kwh=round(remaining_solar_until_sunset, 3),
            projected_remaining_home_demand_until_sunset_kwh=round(remaining_home_demand_until_sunset, 3),
            projected_solar_surplus_until_sunset_kwh=projected_solar_surplus_until_sunset,
            grid_charge_needed_until_sunset_kwh=grid_charge_needed_until_sunset,
            battery_charge_hours_needed_until_sunset=battery_charge_hours_needed_until_sunset,
            target_battery_full_by_sunset=target_battery_full_by_sunset,
            planned_grid_charge_windows=planned_grid_charge_windows,
            planned_solar_charge_windows=planned_solar_charge_windows,
            planned_battery_mode_schedule=planned_battery_mode_schedule,
            planned_battery_mode_windows=planned_battery_mode_windows,
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
            planned_eco_windows=self._serialize_planned_price_windows(eco_windows),
            planned_preheat_window_start=preheat_window["start"].isoformat() if preheat_window else None,
            planned_preheat_window_end=preheat_window["end"].isoformat() if preheat_window else None,
            planned_preheat_windows=self._serialize_planned_price_windows(preheat_windows),
            selected_price_window=None,
            price_window_type=None,
            cheapest_price_window=None,
            tomorrow_cheapest_price_window=None,
            most_expensive_price_window=None,
            battery_min_profit_per_kwh=battery_min_profit,
            price_resolution=price_resolution,
            source_status=source_status,
            source_errors=source_errors,
            rationale=rationale,
        )

    def _serialize_planned_price_windows(
        self,
        windows: list[dict[str, datetime | float]],
    ) -> list[dict[str, str | float]]:
        return [
            {
                "start": cast(datetime, window["start"]).isoformat(),
                "end": cast(datetime, window["end"]).isoformat(),
                "average_price": round(float(window["average_price"]), 6),
            }
            for window in windows
        ]

    def _serialize_price_windows(
        self,
        windows: list[PlannerWindow],
        *,
        horizon_start: datetime,
        horizon_end: datetime,
    ) -> list[dict[str, str | float]]:
        return [
            {
                "start": window.start.isoformat(),
                "end": min(window.end, horizon_end).isoformat(),
                "price": round(float(window.price), 6),
            }
            for window in windows
            if window.end > horizon_start and window.start < horizon_end
        ]

    def _serialize_solar_windows(
        self,
        windows: list[SolarWindow],
        *,
        horizon_start: datetime,
        horizon_end: datetime,
    ) -> list[dict[str, str | float]]:
        return [
            {
                "start": window.start.isoformat(),
                "end": min(window.end, horizon_end).isoformat(),
                "estimated_kwh": round(float(window.forecast_kwh), 6),
            }
            for window in windows
            if window.end > horizon_start and window.start < horizon_end
        ]

    def _battery_demand_safety_margin(self) -> float:
        configured_margin = _coerce_float(
            self._config.get(CONF_BATTERY_DEMAND_SAFETY_MARGIN),
            DEFAULT_BATTERY_DEMAND_SAFETY_MARGIN,
        )
        configured_margin = (
            DEFAULT_BATTERY_DEMAND_SAFETY_MARGIN
            if configured_margin is None
            else configured_margin
        )
        return max(0.0, min(1.0, configured_margin / 100.0))

    def _apply_hourly_demand_safety_margin(
        self,
        hourly_demand: list[dict[str, str | float]],
        *,
        demand_safety_margin: float,
    ) -> list[dict[str, str | float]]:
        if demand_safety_margin <= 0:
            return hourly_demand

        demand_multiplier = 1.0 + demand_safety_margin
        return [
            {
                **slot,
                "estimated_kwh": round(
                    float(slot.get("estimated_kwh", 0.0)) * demand_multiplier,
                    3,
                ),
            }
            for slot in hourly_demand
        ]

    def _build_battery_switch_windows(
        self,
        *,
        attributes: dict[str, Any],
        current_price: float | None,
        price_resolution: str,
        include_past: bool = False,
    ) -> list[PlannerWindow]:
        raw_windows = extract_price_windows(
            attributes,
            current_price,
            "__battery_switch__",
            include_past=include_past,
        )
        if not raw_windows:
            return []
        if price_resolution != PRICE_RESOLUTION_HOURLY:
            return raw_windows

        hourly_windows = aggregate_price_windows_to_hourly(raw_windows)
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
        charge_safety_margin: float = 0.0,
    ) -> tuple[list[dict[str, str | float]], list[dict[str, str | float]]]:
        planned_solar_charge_windows: list[dict[str, str | float]] = []
        planned_grid_charge_windows: list[dict[str, str | float]] = []
        future_slots = [slot for slot in slots if slot["end"] > now]
        if not future_slots:
            return planned_solar_charge_windows, planned_grid_charge_windows

        current_usable_kwh = max(0.0, usable_capacity_kwh - current_remaining_capacity_kwh)
        # Inflate the planning target by the safety margin so the selection loop
        # picks more and earlier solar slots as a buffer against solar underperformance.
        # When solar delivers the full forecast the battery simply fills at usable_capacity_kwh
        # and stops; when solar falls short the extra selected slots cover the gap.
        planning_capacity_kwh = round(usable_capacity_kwh * (1.0 + charge_safety_margin), 6)
        target_charge_kwh = round(planning_capacity_kwh - current_usable_kwh, 6)

        productive_solar_slot_starts = select_contiguous_productive_solar_slot_starts(
            slots=future_slots,
            max_charge_kw=max_charge_kw,
            minimum_slots=1,
        )

        # Split the planning horizon at midnight so that cheaper neg-price slots
        # tomorrow do not displace charging opportunities that must happen today
        # (before tonight's discharge).  Each cycle fills its own independent
        # target; price sorting within each cycle is preserved.
        cycle_end = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)

        # Per-cycle productive solar for independent grid-charge limits.
        current_cycle_solar_kwh = round(
            sum(
                min(max_charge_kw * float(s["hours"]), max(0.0, float(s["net_solar_kwh"])))
                for s in future_slots
                if s["start"] < cycle_end and s["start"] in productive_solar_slot_starts
            ),
            6,
        )
        next_cycle_solar_kwh = round(
            sum(
                min(max_charge_kw * float(s["hours"]), max(0.0, float(s["net_solar_kwh"])))
                for s in future_slots
                if s["start"] >= cycle_end and s["start"] in productive_solar_slot_starts
            ),
            6,
        )

        current_grid_limit_kwh = max(
            0.0,
            round(planning_capacity_kwh - current_usable_kwh - current_cycle_solar_kwh, 6),
        )

        # Discharge cycle tracking: once actual discharge has started, suppress
        # current-cycle grid charging to prevent brief laden_van_net windows from
        # interrupting the active discharge session.  A well-charged battery also
        # suppresses extra current-cycle grid charging, but it does not start the
        # irreversible discharge session by itself.
        # The cycle resets when an active charge phase resumes or the battery is
        # critically low (safety valve for winter / no-solar scenarios).
        usable_soc_fraction = (
            current_usable_kwh / usable_capacity_kwh if usable_capacity_kwh > 0 else 0.0
        )
        if self._active_charge_phase_end is not None and self._active_charge_phase_end > now:
            # Charge phase is actively running â€” not a discharge cycle.
            self._discharge_session_started = False
        elif usable_soc_fraction < 0.15:
            # Safety valve: battery nearly empty â€” allow grid charging regardless.
            self._discharge_session_started = False
        # Between 15 % and 70 %: flag coasts - maintains state from previous tick.

        if self._discharge_session_started or usable_soc_fraction >= 0.70:
            current_grid_limit_kwh = 0.0

        # Next cycle: battery assumed empty â€” must fill full planning capacity.
        next_grid_limit_kwh = max(
            0.0,
            round(planning_capacity_kwh - next_cycle_solar_kwh, 6),
        )
        # Combined gate for candidate building: add a grid candidate if either
        # cycle still has a gap that solar alone cannot fill.
        grid_charge_limit_kwh = max(current_grid_limit_kwh, next_grid_limit_kwh)
        selected_solar_charge_by_start: dict[datetime, float] = {}
        selected_grid_charge_by_start: dict[datetime, float] = {}
        charge_candidates: list[dict[str, Any]] = []

        for slot in future_slots:
            slot_capacity_kwh = max_charge_kw * float(slot["hours"])

            # Negative import price: grid pays us to consume â€” always charge from
            # the grid at full rate regardless of solar or profit margin.
            if float(slot["import_price"]) < 0:
                charge_candidates.append(
                    {
                        "kind": "negative_grid",
                        "start": slot["start"],
                        "end": slot["end"],
                        "charge_kwh": round(slot_capacity_kwh, 6),
                        "effective_price": round(float(slot["import_price"]), 6),
                    }
                )
                continue

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
                        "effective_price": round(float(slot["import_price"]), 6),
                    }
                )
                # If there's remaining slot capacity after solar and the import price
                # here beats the next peak, allow grid to top-up the remainder.
                # Non-solar slots with cheaper prices are selected first (kind=2 sorted
                # by effective_price), so this only kicks in when no better option exists.
                remaining_after_solar_kwh = slot_capacity_kwh - solar_charge_kwh
                if grid_charge_limit_kwh > 0 and remaining_after_solar_kwh > 0:
                    solar_slot_peak_price = calculate_next_battery_peak_price(
                        future_slots,
                        slot["end"],
                        price_key="import_price",
                    )
                    if (
                        solar_slot_peak_price is not None
                        and solar_slot_peak_price - float(slot["import_price"]) >= battery_min_profit
                    ):
                        charge_candidates.append(
                            {
                                "kind": "grid",
                                "start": slot["start"],
                                "end": slot["end"],
                                "charge_kwh": round(
                                    min(remaining_after_solar_kwh, grid_charge_limit_kwh), 6
                                ),
                                "effective_price": round(float(slot["import_price"]), 6),
                            }
                        )
                continue

            if grid_charge_limit_kwh <= 0 or (
                next_peak_price := calculate_next_battery_peak_price(
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

        def _selection_sort_key(item: dict[str, Any]) -> tuple:
            if item["kind"] == "negative_grid":
                # Current neg-price slot first; remaining sort cheapest-price first.
                secondary: Any = (
                    item["start"] > now or item["end"] <= now,
                    float(item["effective_price"]),
                )
            elif item["kind"] == "grid":
                secondary = float(item["effective_price"])
            else:
                secondary = (item["start"].date(), float(item["effective_price"]), item["start"])
            return (
                0 if item["kind"] == "negative_grid" else (1 if item["kind"] == "solar" else 2),
                secondary,
            )

        current_candidates = [c for c in charge_candidates if c["start"] < cycle_end]
        next_candidates = [c for c in charge_candidates if c["start"] >= cycle_end]

        # â”€â”€ Current-cycle selection (today) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        # Solar slots are sorted cheapest-price first; we greedily select them
        # until the battery is full â€” no tier filter.
        charged_kwh = 0.0
        charged_grid_kwh = 0.0
        neg_grid_charged_kwh = 0.0
        for candidate in sorted(current_candidates, key=_selection_sort_key):
            if charged_kwh >= target_charge_kwh:
                break
            candidate_charge_kwh = float(candidate["charge_kwh"])
            if candidate["kind"] == "grid":
                candidate_charge_kwh = min(
                    candidate_charge_kwh,
                    max(0.0, current_grid_limit_kwh - charged_grid_kwh),
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
                if candidate["kind"] == "grid":
                    charged_grid_kwh += usable_charge_kwh
                else:
                    neg_grid_charged_kwh += usable_charge_kwh
            charged_kwh += usable_charge_kwh

        # Drop current-cycle solar that starts before the first neg-price slot so
        # the battery stays idle until cheap electricity arrives and solar goes to
        # home demand instead of being stored pre-emptively.
        if neg_grid_charged_kwh > 0 and selected_solar_charge_by_start:
            current_neg_starts = {
                c["start"]
                for c in current_candidates
                if c["kind"] == "negative_grid" and c["start"] in selected_grid_charge_by_start
            }
            if current_neg_starts:
                first_neg_start = min(current_neg_starts)
                for start in list(selected_solar_charge_by_start.keys()):
                    if start < first_neg_start:
                        del selected_solar_charge_by_start[start]

        # â”€â”€ Next-cycle selection (tomorrow) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        # Battery is assumed discharged to empty before midnight; plan to refill
        # it using tomorrow's candidates, price-sorted within each kind.
        next_target_kwh = planning_capacity_kwh
        next_charged_kwh = 0.0
        next_charged_grid_kwh = 0.0
        next_neg_grid_charged_kwh = 0.0
        for candidate in sorted(next_candidates, key=_selection_sort_key):
            if next_charged_kwh >= next_target_kwh:
                break
            # Next-cycle: no tier filter for solar â€” we need to fill the full
            # battery and should use all productive solar hours, cheapest first.
            candidate_charge_kwh = float(candidate["charge_kwh"])
            if candidate["kind"] == "grid":
                candidate_charge_kwh = min(
                    candidate_charge_kwh,
                    max(0.0, next_grid_limit_kwh - next_charged_grid_kwh),
                )
            usable_charge_kwh = min(candidate_charge_kwh, next_target_kwh - next_charged_kwh)
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
                if candidate["kind"] == "grid":
                    next_charged_grid_kwh += usable_charge_kwh
                else:
                    next_neg_grid_charged_kwh += usable_charge_kwh
            next_charged_kwh += usable_charge_kwh

        # Drop next-cycle solar that starts before the first next-cycle neg-price slot.
        if next_neg_grid_charged_kwh > 0:
            next_neg_starts = {
                c["start"]
                for c in next_candidates
                if c["kind"] == "negative_grid" and c["start"] in selected_grid_charge_by_start
            }
            if next_neg_starts:
                first_next_neg_start = min(next_neg_starts)
                for start in list(selected_solar_charge_by_start.keys()):
                    if cycle_end <= start < first_next_neg_start:
                        del selected_solar_charge_by_start[start]

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
            merge_planned_windows(planned_solar_charge_windows),
            merge_planned_windows(planned_grid_charge_windows),
        )

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
        if active_charge_phase_end is not None and active_charge_phase_end > now:
            # The charge phase is still running (or was running when the plan was
            # last evaluated).  Re-inject it as a cluster anchored at *now* so that
            # the current slot stays in charge mode even when the new planning run
            # does not (re-)select that slot â€” e.g. because the battery is nearly
            # full and the solar window is skipped by the capacity gate.
            # Safety: _active_charge_phase_end is only written while we are inside
            # an active charge window, so it can never carry a stale value from a
            # different day/cycle (it is cleared by _update_active_charge_phase_state
            # the moment active_charge_phase becomes None).
            normalized_windows.append({"start": now, "end": active_charge_phase_end})
            normalized_windows.sort(key=lambda window: window["start"])
        else:
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
        max_charge_kw: float,
        mode_start: datetime | None = None,
    ) -> tuple[str, float, datetime]:
        charge_start = cast(datetime, charge_window.get("start", slot["start"]))
        charge_end = cast(datetime, charge_window["end"])
        mode_start = mode_start or charge_start
        usable_hours = max((charge_end - mode_start).total_seconds() / 3600, 0.0)
        charge_kwh = min(
            float(charge_window.get("charge_kwh", 0.0)),
            max(0.0, usable_hours * max_charge_kw),
        )
        sim_usable_energy_kwh = min(usable_capacity_kwh, sim_usable_energy_kwh + charge_kwh)
        if mode_start <= now < charge_end:
            current_mode = mode
        hourly_modes.append(
            {
                "start": mode_start.isoformat(),
                "end": charge_end.isoformat(),
                "price": round(float(slot[price_key]), 6),
                "usable_hours": round(usable_hours, 3),
                "mode": mode,
            }
        )
        return current_mode, sim_usable_energy_kwh, charge_end

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

    def _build_mode_windows_from_hourly_plan(
        self,
        *,
        slots: list[dict[str, Any]],
        now: datetime,
        planned_solar_charge_windows: list[dict[str, str | float]],
        planned_grid_charge_windows: list[dict[str, str | float]],
        initial_usable_energy_kwh: float,
        usable_capacity_kwh: float,
        average_price: float,
        average_export_price: float,
        max_charge_kw: float,
        max_discharge_kw: float,
    ) -> tuple[list[dict[str, str | float]], str]:
        solar_charge_starts = build_charge_window_lookup(
            planned_solar_charge_windows,
            max_charge_kw=max_charge_kw,
        )
        grid_charge_starts = build_charge_window_lookup(
            planned_grid_charge_windows,
            max_charge_kw=max_charge_kw,
        )

        charge_starts = {
            **solar_charge_starts,
            **grid_charge_starts,
        }
        charge_windows = [
            {"start": start, "mode": "laden_van_net", "price_key": "import_price", **window}
            for start, window in grid_charge_starts.items()
        ] + [
            {"start": start, "mode": "laden_met_zonne_energie", "price_key": "export_price", **window}
            for start, window in solar_charge_starts.items()
        ]
        discharge_session_started = self._discharge_session_started
        if discharge_session_started:
            self._active_charge_phase_end = None
            self._active_charge_phase_mode = "accu_uit"
            charge_starts = {}
            charge_windows = []
            solar_charge_starts = {}
            grid_charge_starts = {}
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
        cycle_end = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        hourly_modes: list[dict[str, str | float]] = []
        current_mode = "accu_uit"
        last_charge_mode = "accu_uit"

        slot_index = 0
        while slot_index < len(slots):
            slot = slots[slot_index]
            slot_start = slot["start"]
            slot_end = slot["end"]
            charge_window_handled = False
            overlapping_charge_window = next(
                (
                    window
                    for window in charge_windows
                    if slot_end > cast(datetime, window["start"])
                    and slot_start < cast(datetime, window["end"])
                ),
                None,
            )
            if overlapping_charge_window is not None and slot_start not in charge_starts:
                charge_mode = str(overlapping_charge_window["mode"])
                current_mode, sim_usable_energy_kwh, charge_end = self._append_charge_window_mode(
                    hourly_modes=hourly_modes,
                    slot=slot,
                    charge_window=overlapping_charge_window,
                    mode=charge_mode,
                    price_key=str(overlapping_charge_window["price_key"]),
                    now=now,
                    current_mode=current_mode,
                    sim_usable_energy_kwh=sim_usable_energy_kwh,
                    usable_capacity_kwh=usable_capacity_kwh,
                    max_charge_kw=max_charge_kw,
                    mode_start=max(slot_start, cast(datetime, overlapping_charge_window["start"])),
                )
                last_charge_mode = charge_mode
                while slot_index < len(slots) and slots[slot_index]["start"] < charge_end:
                    slot_index += 1
                charge_window_handled = True
            if charge_window_handled:
                continue

            for charge_lookup, charge_mode, price_key in (
                (grid_charge_starts,  "laden_van_net",           "import_price"),
                (solar_charge_starts, "laden_met_zonne_energie", "export_price"),
            ):
                if slot_start not in charge_lookup:
                    continue
                current_mode, sim_usable_energy_kwh, charge_end = self._append_charge_window_mode(
                    hourly_modes=hourly_modes,
                    slot=slot,
                    charge_window=charge_lookup[slot_start],
                    mode=charge_mode,
                    price_key=price_key,
                    now=now,
                    current_mode=current_mode,
                    sim_usable_energy_kwh=sim_usable_energy_kwh,
                    usable_capacity_kwh=usable_capacity_kwh,
                    max_charge_kw=max_charge_kw,
                )
                last_charge_mode = charge_mode
                while slot_index < len(slots) and slots[slot_index]["start"] < charge_end:
                    slot_index += 1
                charge_window_handled = True
                break
            if charge_window_handled:
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
            before_first_charge_phase = (
                first_charge_phase_start is None  # no charge windows â†’ drain freely, no price gating
                or (
                    bool(segment_slots)
                    and segment_slots[0]["start"] < first_charge_phase_start
                )
            )
            # Distribute battery capacity across the most expensive deficit hours
            # first (highest price â†’ next highest â†’ ... until battery empty).
            discharge_budget_kwh = sim_usable_energy_kwh
            planned_discharge_kwh = plan_segment_discharge_kwh(
                slots=segment_slots,
                available_energy_kwh=discharge_budget_kwh,
                max_discharge_kw=max_discharge_kw,
            )
            total_segment_demand_kwh = sum(
                float(slot.get("demand_kwh", 0.0)) for slot in segment_slots
            )
            # Surplus = battery energy minus what is actually planned to discharge
            # for home demand (NOT total segment demand).  The segment may span
            # until the next charge window (e.g. 06:00-12:00) while the battery
            # only covers the expensive early hours; the remaining energy would
            # otherwise sit idle and represents genuine export surplus.
            total_planned_discharge_kwh = sum(planned_discharge_kwh.values())
            # Plan export for any genuine surplus: battery energy above what the
            # discharge plan needs.  The within_export_window gate (8 h before
            # the next charge) prevents depleting the battery too early.
            forced_export_kwh = self._plan_segment_export_kwh(
                slots=segment_slots,
                available_energy_kwh=sim_usable_energy_kwh,
                total_segment_demand_kwh=total_planned_discharge_kwh,
                max_discharge_kw=max_discharge_kw,
            )
            has_export_surplus = sim_usable_energy_kwh > total_planned_discharge_kwh

            # Export to grid is only allowed within 8 hours before the next
            # planned charge phase.  Exporting earlier risks draining the
            # battery so much that home demand can't be covered until the
            # next charge window.  When no charge is scheduled the battery
            # won't be refilled, so export proceeds freely.
            _next_charge_after_segment = min(
                (c["start"] for c in charge_phase_clusters if segment_slots and c["start"] >= segment_slots[-1]["end"]),
                default=None,
            )
            _export_allowed_from = (
                _next_charge_after_segment - timedelta(hours=8)
                if _next_charge_after_segment is not None
                else None
            )

            # Precompute suffix discharge sums: remaining_planned_discharge[s] =
            # total planned discharge for all segment slots that start AFTER s.
            suffix_discharge_kwh: dict[datetime, float] = {}
            running_suffix = 0.0
            for _slot in reversed(segment_slots):
                suffix_discharge_kwh[_slot["start"]] = running_suffix
                running_suffix += float(planned_discharge_kwh.get(_slot["start"], 0.0))

            for segment_slot in segment_slots:
                segment_slot_start = segment_slot["start"]
                segment_slot_end = segment_slot["end"]
                segment_discharge_kwh = float(planned_discharge_kwh.get(segment_slot_start, 0.0))
                remaining_planned_discharge_kwh = suffix_discharge_kwh[segment_slot_start]
                within_export_window = (
                    _export_allowed_from is not None
                    and segment_slot_start >= _export_allowed_from
                )
                segment_export_kwh = float(forced_export_kwh.get(segment_slot_start, 0.0))
                within_charge_phase = (
                    any(
                        segment_slot_end > cluster["start"] and segment_slot_start < cluster["end"]
                        for cluster in charge_phase_clusters
                    )
                )
                # Hold in laden_met_zonne_energie after any charge phase so the
                # inverter doesn't flip to accu_uit between the charge window end
                # and the discharge window.  Use laden_met_zonne_energie even after
                # grid charging so the inverter accepts solar passively.
                hold_charge_mode = (
                    last_charge_mode in ("laden_met_zonne_energie", "laden_van_net")
                    and sim_usable_energy_kwh > 0
                )
                charge_phase_mode = (
                    last_charge_mode if last_charge_mode != "accu_uit"
                    else active_charge_phase_mode
                )
                # Once a charge phase has started, suppress opportunistic discharge
                # between charge windows within today's cycle.  This keeps the
                # battery in charge/hold mode until all today's charge windows are
                # done, so the full evening discharge happens in one uninterrupted
                # session rather than draining partially between neg-price windows.
                more_todays_charge_ahead = any(
                    cluster["end"] > segment_slot_end and cluster["start"] < cycle_end
                    for cluster in charge_phase_clusters
                )
                suppress_discharge = (
                    not before_first_charge_phase
                    and (hold_charge_mode or within_charge_phase)
                    and more_todays_charge_ahead
                )

                # Discharge takes priority over charging, except between charge
                # windows within today's cycle (suppress_discharge).
                mode = "accu_uit"
                if (
                    segment_discharge_kwh > 0
                    and sim_usable_energy_kwh > 0
                    and not suppress_discharge
                    and not within_charge_phase
                ):
                    last_charge_mode = "accu_uit"
                    if segment_export_kwh > 0 and within_export_window:
                        # Export surplus on top of home-demand discharge: drain both
                        # at the most expensive slot inside the discharge window.
                        mode = "ontladen_naar_net"
                        sim_usable_energy_kwh = max(
                            0.0, sim_usable_energy_kwh - segment_discharge_kwh - segment_export_kwh
                        )
                    else:
                        mode = "ontladen"
                        sim_usable_energy_kwh = max(0.0, sim_usable_energy_kwh - segment_discharge_kwh)
                elif within_charge_phase:
                    # A charge window always beats a plain export slot.  Export
                    # that is a forced part of a segment is handled separately
                    # through segment_discharge_kwh; segment_export_kwh must
                    # never override a planned charge phase.
                    mode = charge_phase_mode
                elif hold_charge_mode:
                    mode = "laden_met_zonne_energie"
                elif segment_export_kwh > 0 and sim_usable_energy_kwh > 0 and within_export_window:
                    mode = "ontladen_naar_net"
                    last_charge_mode = "accu_uit"
                    sim_usable_energy_kwh = max(0.0, sim_usable_energy_kwh - segment_export_kwh)
                else:
                    exportable_kwh = max(0.0, sim_usable_energy_kwh - remaining_planned_discharge_kwh)
                    slot_export_capacity_kwh = max_discharge_kw * float(segment_slot["hours"])
                    slot_solar_kwh = float(segment_slot.get("net_solar_kwh", 0))
                    if (
                        not before_first_charge_phase
                        and has_export_surplus
                        and exportable_kwh > 0
                        and slot_solar_kwh >= 0
                        and segment_end_index < len(slots)
                        and float(segment_slot["export_price"]) >= average_export_price
                        and within_export_window
                    ):
                        mode = "ontladen_naar_net"
                        last_charge_mode = "accu_uit"
                        sim_usable_energy_kwh = max(0.0, sim_usable_energy_kwh - min(slot_export_capacity_kwh, exportable_kwh))

                if segment_slot["start"] <= now < segment_slot["end"]:
                    current_mode = mode
                    if mode in ("ontladen", "ontladen_naar_net"):
                        discharge_session_started = True

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

        if discharge_session_started:
            self._discharge_session_started = True

        self._update_active_charge_phase_state(
            now=now,
            active_charge_phase=active_charge_phase,
            current_mode=current_mode,
            last_charge_mode=last_charge_mode,
            active_charge_phase_mode=active_charge_phase_mode,
        )

        return self._merge_mode_windows(hourly_modes), current_mode

    def _plan_segment_export_kwh(
        self,
        *,
        slots: list[dict[str, Any]],
        available_energy_kwh: float,
        total_segment_demand_kwh: float,
        max_discharge_kw: float,
    ) -> dict[datetime, float]:
        if available_energy_kwh <= 0 or max_discharge_kw <= 0 or not slots:
            return {}

        # Only export if the battery holds more energy than the total home demand
        # in this segment.  When demand â‰¥ battery, all energy goes to own use and
        # the solar merely displaces grid draw â€” no genuine surplus exists.
        required_export_kwh = max(0.0, available_energy_kwh - total_segment_demand_kwh)
        if required_export_kwh <= 0:
            return {}

        export_slots: list[dict[str, Any]] = []
        for slot in slots:
            export_capacity_kwh = min(
                max_discharge_kw * float(slot["hours"]),
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

    def _merge_mode_windows(
        self,
        windows: list[dict[str, str | float]],
    ) -> list[dict[str, str | float]]:
        return merge_windows(windows, same_mode_only=True, pick_max_price=True)

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

            demand_kwh = sum_remaining_home_demand_until(
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


def _robust_hourly_demand(values: list[float]) -> float | None:
    valid_values = sorted(
        value
        for value in values
        if 0.0 <= value <= _DEMAND_PROFILE_MAX_KWH_PER_HOUR
    )
    if not valid_values:
        return None
    if len(valid_values) < 3:
        return round(statistics.median(valid_values), 4)

    median_value = statistics.median(valid_values)
    lower_bound = 0.0 if median_value < 0.1 else median_value * 0.35
    upper_bound = min(
        _DEMAND_PROFILE_MAX_KWH_PER_HOUR,
        max(0.2, median_value * 2.75),
    )
    trimmed_values = [
        value
        for value in valid_values
        if lower_bound <= value <= upper_bound
    ]
    return round(statistics.median(trimmed_values or valid_values), 4)


def _calculate_demand_adjustment_factor(
    *,
    hourly_usage: dict[datetime, float],
    table: dict[str, float],
    now: datetime,
) -> float:
    today = dt_util.as_local(now).date()
    completed_today = [
        (dt_util.as_local(hour_start), usage_kwh)
        for hour_start, usage_kwh in hourly_usage.items()
        if dt_util.as_local(hour_start).date() == today
        and dt_util.as_local(hour_start) + timedelta(hours=1) <= now
        and 0.0 <= usage_kwh <= _DEMAND_PROFILE_MAX_KWH_PER_HOUR
    ]
    if len(completed_today) < _DEMAND_TODAY_ADJUSTMENT_MIN_COMPLETED_HOURS:
        return 1.0

    actual_kwh = sum(usage_kwh for _, usage_kwh in completed_today)
    expected_kwh = sum(
        _demand_profile_value_for_time(table, hour_start) or 0.0
        for hour_start, _ in completed_today
    )
    if expected_kwh < 0.5:
        return 1.0

    raw_factor = actual_kwh / expected_kwh
    damped_factor = 1.0 + ((raw_factor - 1.0) * _DEMAND_TODAY_ADJUSTMENT_WEIGHT)
    return round(min(1.35, max(0.75, damped_factor)), 3)


def _demand_profile_value_for_time(table: dict[str, float], hour_start: datetime) -> float | None:
    hour_start = dt_util.as_local(hour_start)
    weekday = hour_start.weekday()
    hour = hour_start.hour
    exact_value = _coerce_float(table.get(str(weekday * 24 + hour)))
    if exact_value is not None:
        return exact_value

    forecast_is_weekend = weekday >= 5
    similar_weekdays = [5, 6] if forecast_is_weekend else [0, 1, 2, 3, 4]
    similar_values = [
        value
        for similar_weekday in similar_weekdays
        if (value := _coerce_float(table.get(str(similar_weekday * 24 + hour)))) is not None
    ]
    if similar_values:
        return statistics.median(similar_values)

    same_hour_values = [
        value
        for any_weekday in range(7)
        if (value := _coerce_float(table.get(str(any_weekday * 24 + hour)))) is not None
    ]
    if same_hour_values:
        return statistics.median(same_hour_values)

    return None


def _serialize_mode_windows(windows: list[dict[str, str | datetime | float]]) -> list[dict[str, str | float]]:
    serialized: list[dict[str, str | float]] = []
    for window in windows:
        start = window.get("start")
        end = window.get("end")
        mode = window.get("mode")
        if start is None or end is None or mode is None:
            continue
        serialized_window: dict[str, str | float] = {
            "start": start.isoformat() if isinstance(start, datetime) else str(start),
            "end": end.isoformat() if isinstance(end, datetime) else str(end),
            "mode": str(mode),
        }
        price = _coerce_float(window.get("price"))
        if price is not None:
            serialized_window["price"] = round(price, 6)
        serialized.append(serialized_window)
    return serialized


def _mode_at_time(windows: list[dict[str, str | float]], at: datetime) -> str | None:
    for window in windows:
        start = _parse_datetime(window.get("start"))
        end = _parse_datetime(window.get("end"))
        if start is None or end is None:
            continue
        if start <= at < end:
            return str(window.get("mode", "accu_uit"))
    return None


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _coerce_float(value: Any, default: float | None = None) -> float | None:
    if value in (None, STATE_UNKNOWN, STATE_UNAVAILABLE, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
