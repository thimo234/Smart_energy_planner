"""Coordinator for Smart Energy Planner."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from dataclasses import replace
from copy import copy
import asyncio
import logging
import statistics
from typing import Any, cast

from homeassistant.components.recorder import get_instance as get_recorder_instance, history
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import STATE_UNAVAILABLE, STATE_UNKNOWN
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.event import async_track_time_interval
from homeassistant.helpers.storage import Store
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator
from homeassistant.util import dt as dt_util

from .battery_forecast import (
    align_price_responsive_demand_to_cheap_hours,
    build_energy_balance_slots,
    chargeable_solar_kwh,
    build_expected_hourly_demand_table,
    build_fallback_solar_windows,
    build_fallback_solar_windows_for_day,
    build_hourly_home_demand_forecast,
    DEMAND_ADJUSTMENT_MAX_FACTOR,
    DEMAND_ADJUSTMENT_MIN_FACTOR,
    extract_solar_windows,
    get_solar_day_end,
    merge_solar_windows,
    observed_hourly_demand_table,
    sum_remaining_home_demand_until,
    sum_remaining_solar_until,
    select_best_solar_window,
    update_expected_hourly_demand_stats,
)
from .const import PLANNER_KIND_DEMAND, CONF_DEMAND_FORECAST_SENSOR
from .demand_forecast import read_forecast
from .battery_models import SolarWindow
from .battery_profit import grid_supplement_profit
from .battery_margin import observed_stock, execution_actions, charge_stock
from .battery_planner import (
    bundle_slot_allocations,
    build_battery_mode_schedule,
    build_charge_window_lookup,
    merge_planned_windows,
    merge_windows,
    normalize_full_battery_charge_mode,
    plan_segment_discharge_kwh,
    reserve_without_charge_opportunity,
    summarize_battery_cycles,
)
from .const import (
    CONF_BATTERY_NO_CHARGE_MIN_SOC_PERCENT,
    CONF_BATTERY_CAPACITY_KWH,
    CONF_BATTERY_DEMAND_SAFETY_MARGIN,
    CONF_BATTERY_ENABLED,
    CONF_BATTERY_CHARGE_SAFETY_MARGIN,
    CONF_BATTERY_SOLAR_TRANSITION_BUFFER_MINUTES,
    CONF_BATTERY_MAX_CHARGE_KW,
    CONF_BATTERY_MAX_DISCHARGE_KW,
    CONF_BATTERY_MIN_SOC_PERCENT,
    CONF_BATTERY_MIN_PROFIT_PER_KWH,
    CONF_BATTERY_SOC_SENSOR,
    CONF_EXPORT_PRICE_SENSOR,
    CONF_PLANNER_KIND,
    CONF_PRICE_SENSOR,
    CONF_PRICE_RESOLUTION,
    CONF_PRICE_WINDOW_TYPE,
    CONF_PRICE_WINDOW_DURATION_HOURS,
    CONF_PRICE_WINDOW_WHOLE_HOUR_START,
    CONF_SOLCAST_TODAY_SENSOR,
    CONF_SOLCAST_TOMORROW_SENSOR,
    CONF_TOTAL_ENERGY_SENSOR,
    COORDINATOR_UPDATE_INTERVAL,
    DEFAULT_BATTERY_CAPACITY_KWH,
    DEFAULT_BATTERY_DEMAND_SAFETY_MARGIN,
    DEFAULT_BATTERY_ENABLED,
    DEFAULT_BATTERY_CHARGE_SAFETY_MARGIN,
    DEFAULT_BATTERY_MAX_CHARGE_KW,
    DEFAULT_BATTERY_SOLAR_TRANSITION_BUFFER_MINUTES,
    MAX_BATTERY_SOLAR_TRANSITION_BUFFER_MINUTES,
    DEFAULT_BATTERY_MAX_DISCHARGE_KW,
    DEFAULT_BATTERY_MIN_SOC_PERCENT,
    DEFAULT_BATTERY_MIN_PROFIT_PER_KWH,
    DEFAULT_PRICE_WINDOW_DURATION_HOURS,
    DEFAULT_PRICE_WINDOW_TYPE,
    DEFAULT_PRICE_WINDOW_WHOLE_HOUR_START,
    DOMAIN,
    PLANNER_KIND_BATTERY,
    PLANNER_KIND_PRICE_WINDOW,
    PRICE_RESOLUTION_HOURLY,
    PRICE_WINDOW_TYPE_MOST_EXPENSIVE,
    RUNTIME_STATE,
    STORAGE_KEY,
    STORAGE_VERSION,
)
from .price_helpers import (
    deduct_export_prices,
    export_price_deduction,
    aggregate_price_windows_to_hourly,
    build_neutral_price_windows,
    extend_price_window_tail,
    extract_price_average,
    extract_price_windows,
    select_contiguous_price_window,
)
from .price_peaks import STATE_KEY as PRICE_PEAK_STATE_KEY, plan_price_peaks
from .const import (
    CONF_PRICE_PEAK_MIN_DURATION_MINUTES, DEFAULT_PRICE_PEAK_MIN_DURATION_MINUTES,
    CONF_PRICE_PEAK_MIN_GAP_HOURS, DEFAULT_PRICE_PEAK_MIN_GAP_HOURS,
    CONF_PRICE_PEAK_MIN_PROMINENCE_PERCENT, DEFAULT_PRICE_PEAK_MIN_PROMINENCE_PERCENT,
)
from .price_models import PlannerWindow
from .planner_result import PlannerResult
from .battery_optimizer import optimize_battery_plan

_LOGGER = logging.getLogger(__name__)
_HISTORY_LOOKBACK_DAYS = 7
# Minimum absolute change (kWh) in the tracked battery energy before the profit
# tracker updates the cost basis. Changes below this are treated as sensor
# noise, but the baseline is still persisted so we don't lose precision across
# Home Assistant restarts.
_BATTERY_PROFIT_NOISE_FLOOR_KWH = 0.01
_DEMAND_PROFILE_ALPHA = 0.35
_DEMAND_PROFILE_MAX_KWH_PER_HOUR = 10.0
_DEMAND_TODAY_ADJUSTMENT_UP_WEIGHT = 0.35
_DEMAND_TODAY_ADJUSTMENT_DOWN_WEIGHT = 0.70
_DEMAND_TODAY_ADJUSTMENT_MIN_COMPLETED_HOURS = 3
_DEMAND_TODAY_ADJUSTMENT_MIN_EXPECTED_KWH = 0.5
_DEMAND_TODAY_PARTIAL_MIN_HOURS = 0.25
_EXPECTED_DEMAND_STATS_KEY = "expected_hourly_demand_stats"
_EXPECTED_DEMAND_TABLE_KEY = "expected_hourly_demand_table"
_EXPECTED_DEMAND_ADJUSTMENT_KEY = "expected_hourly_demand_adjustment_factor"
_EXPECTED_DEMAND_LAST_VALUE_KEY = "expected_hourly_demand_last_value"
_EXPECTED_DEMAND_LAST_HOUR_KEY = "expected_hourly_demand_last_hour_key"
_EXPECTED_DEMAND_LAST_TS_KEY = "expected_hourly_demand_last_ts"
_EXPECTED_DEMAND_TODAY_KEY = "expected_hourly_demand_today"
_BATTERY_CYCLE_STATE_KEY = "battery_cycle_state"
_BATTERY_CYCLE_STATE_MAX_RESTORE_AGE = timedelta(minutes=30)
_LEGACY_DEMAND_KEYS = (
    "hourly_demand_table",
    "hourly_demand_observed_slots",
    "hourly_demand_adjustment_factor",
    "hourly_demand_last_value",
    "hourly_demand_last_hour_key",
    "hourly_demand_last_ts",
)
_BATTERY_DEPLETION_EPSILON_KWH = 0.05
_BATTERY_DISCHARGE_SOC_THRESHOLD_PERCENT = 90.0
_BATTERY_NEAR_FULL_GRID_TOPUP_BLOCK_PERCENT = 5.0
_MIN_CHARGE_WINDOW_GAP = timedelta(hours=3)


class SmartEnergyPlannerCoordinator(DataUpdateCoordinator[PlannerResult]):
    """Coordinate planner calculations."""

    config_entry: ConfigEntry

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
        self._planner_entry = entry
        self.config_entry = getattr(entry, 'parent', entry)
        super().__init__(
            hass,
            logger=_LOGGER,
            name=DOMAIN,
            update_interval=COORDINATOR_UPDATE_INTERVAL,
        )
        self._active_charge_phase_end: datetime | None = None
        self._active_charge_phase_mode = "accu_uit"
        self._source_error_retry_unsub: object | None = None
        self._charge_session_started: bool = False
        self._discharge_session_started: bool = False
        self._battery_cycle_discharge_floor_kwh: float | None = None
        self._battery_cycle_state_initialized: bool = False
        self._battery_cycle_state_restored_recent: bool = False
        self._restore_recent_battery_cycle_state()

    @property
    def planner_entry(self):
        """Stable planner identity, separate from the owning HA config entry."""
        return getattr(self, '_planner_entry', self.config_entry)

    def _restore_recent_battery_cycle_state(self) -> None:
        runtime_state = self.hass.data.get(RUNTIME_STATE, {}).get(self.planner_entry.entry_id, {})
        self._battery_grid_charge_price = _coerce_float(runtime_state.get("battery_grid_charge_price"))
        self._battery_margin_state = runtime_state.get("battery_margin_state")
        cycle_state = runtime_state.get(_BATTERY_CYCLE_STATE_KEY)
        if not isinstance(cycle_state, dict):
            return

        updated_at_raw = cycle_state.get("updated_at")
        updated_at = dt_util.parse_datetime(updated_at_raw) if isinstance(updated_at_raw, str) else None
        if updated_at is None:
            return
        boundary = _coerce_float(cycle_state.get("discharge_floor_kwh"))
        committed_boundary = boundary is not None and 0.0 <= boundary < float("inf")
        # A committed boundary and its observed phase are one durable decision.
        # Downtime cannot finish a cycle; the next measured SOC reconciles it
        # against the selected floor / configured maximum in normal planning.
        # Legacy snapshots without that decision retain their old freshness rule.
        if (not committed_boundary
                and dt_util.as_local(dt_util.now()) - dt_util.as_local(updated_at)
                    > _BATTERY_CYCLE_STATE_MAX_RESTORE_AGE):
            return

        self._charge_session_started = bool(cycle_state.get("charge_session_started", False))
        self._battery_charge_source = cycle_state.get("charge_source")
        self._discharge_session_started = bool(cycle_state.get("discharge_session_started", False))
        if self._charge_session_started and self._discharge_session_started:
            self._discharge_session_started = False
        self._battery_cycle_state_initialized = bool(
            cycle_state.get("initialized", True)
            or self._charge_session_started
            or self._discharge_session_started
        )
        self._battery_cycle_state_restored_recent = True
        self._battery_cycle_discharge_floor_kwh = _coerce_float(cycle_state.get("discharge_floor_kwh"))
        self._battery_solar_reserve_relaxation = bool(cycle_state.get("solar_reserve_relaxation", False))

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

    def _store_battery_cycle_state_snapshot(self, now: datetime) -> None:
        if not hasattr(self, "hass"):
            return
        runtime_state = self.hass.data.setdefault(RUNTIME_STATE, {}).setdefault(
            self.planner_entry.entry_id,
            {},
        )
        runtime_state[_BATTERY_CYCLE_STATE_KEY] = {
            "charge_session_started": bool(getattr(self, "_charge_session_started", False)),
            "charge_source": getattr(self, "_battery_charge_source", None),
            "discharge_session_started": bool(getattr(self, "_discharge_session_started", False)),
            "initialized": bool(getattr(self, "_battery_cycle_state_initialized", False)),
            "discharge_floor_kwh": getattr(self, "_battery_cycle_discharge_floor_kwh", None),
            "solar_reserve_relaxation": bool(getattr(self, "_battery_solar_reserve_relaxation", False)),
            "updated_at": dt_util.as_local(now).isoformat(),
        }
        runtime_state["battery_grid_charge_price"] = getattr(self, "_battery_grid_charge_price", None)
        runtime_state["battery_margin_state"] = getattr(self, "_battery_margin_state", None)

    @property
    def _config(self) -> dict[str, Any]:
        return {**self.planner_entry.data, **self.planner_entry.options}

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
            if planner_kind == PLANNER_KIND_DEMAND:
                from .demand_coordinator import async_demand_result
                result = await async_demand_result(self, now)
                errors = tuple(result.source_errors)
                if errors != getattr(self, '_last_demand_source_errors', ()):
                    if errors:
                        _LOGGER.warning("Consumption forecast %s: %s", self.planner_entry.entry_id, errors)
                    self._last_demand_source_errors = errors
                if result.status in ('ready', 'ready_with_warnings'):
                    self._cancel_source_error_retry()
                else:
                    self._schedule_source_error_retry()
                return result
            external_demand = None
            demand_error = None
            forecast_sensor = self._config_entity_id(CONF_DEMAND_FORECAST_SENSOR)
            if planner_kind == PLANNER_KIND_BATTERY and forecast_sensor:
                try:
                    from .household import is_self_forecast
                    if is_self_forecast(self.hass, self.planner_entry.entry_id, forecast_sensor):
                        raise ValueError('self_forecast')
                    external_demand = read_forecast(self.hass.states.get(forecast_sensor), now=now,
                        horizon_end=now.replace(hour=0, minute=0, second=0, microsecond=0)+timedelta(days=2))
                except ValueError as err:
                    demand_error = str(err)
            price_sensor = self._config_entity_id(CONF_PRICE_SENSOR) or str(self._config.get(CONF_PRICE_SENSOR, ''))
            export_price_sensor = self._config_entity_id(CONF_EXPORT_PRICE_SENSOR)
            solar_sensor = self._config_entity_id(CONF_SOLCAST_TODAY_SENSOR)
            solar_tomorrow_sensor = self._config_entity_id(CONF_SOLCAST_TOMORROW_SENSOR)
            total_energy_sensor = self._config_entity_id(CONF_TOTAL_ENERGY_SENSOR)
            if forecast_sensor:
                total_energy_sensor = None
            battery_soc_sensor = self._config_entity_id(CONF_BATTERY_SOC_SENSOR)

            price_state = self.hass.states.get(price_sensor)
            export_price_state = self.hass.states.get(export_price_sensor) if export_price_sensor else None
            solar_state = self.hass.states.get(solar_sensor) if solar_sensor else None
            solar_tomorrow_state = self.hass.states.get(solar_tomorrow_sensor) if solar_tomorrow_sensor else None
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
                total_energy_sensor=total_energy_sensor,
                total_energy_state=total_energy_state,
                battery_soc_sensor=battery_soc_sensor,
                battery_soc_state=battery_soc_state,
                planner_kind=planner_kind,
            )
            source_errors = self._collect_source_errors(source_status)
            if demand_error:
                source_status['demand_forecast_sensor'] = demand_error
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
            if planner_kind == PLANNER_KIND_BATTERY:
                export_current_price, export_price_average, export_windows, all_export_windows = deduct_export_prices(
                    export_current_price, export_price_average, export_windows, all_export_windows,
                    export_price_deduction(self._config),
                )

            if not price_state:
                source_status["price_sensor"] = "waiting_for_price_sensor"
                neutral_windows = build_neutral_price_windows(current_price, hours=48)
                windows = list(neutral_windows)
                all_windows = list(neutral_windows)
                export_windows = list(neutral_windows)
                all_export_windows = list(neutral_windows)
                battery_switch_windows = list(neutral_windows)

            if not windows:
                source_status["price_sensor"] = "no_price_windows"
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
                battery_price_horizon_end = (now + timedelta(days=2)).replace(
                    hour=0,
                    minute=0,
                    second=0,
                    microsecond=0,
                )
                if battery_price_horizon_end <= now:
                    battery_price_horizon_end += timedelta(days=1)
                battery_price_horizon_end = max(
                    [window.end for window in [*all_windows, *all_solar_windows]] + [battery_price_horizon_end],
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

            if demand_error:
                # Keep source diagnostics visible while withholding battery commands.
                result = self._build_pending_result('waiting_for_demand_forecast', planner_kind,
                    source_status, self._collect_source_errors(source_status))
                result.current_price = current_price
                result.current_export_price = export_current_price
                result.battery_soc_percent = battery_soc_percent
                start = now.replace(hour=0, minute=0, second=0, microsecond=0)
                end = battery_price_horizon_end
                result.upcoming_energy_price_windows = self._serialize_price_windows(
                    all_windows, horizon_start=start, horizon_end=end)
                result.upcoming_export_price_windows = self._serialize_price_windows(
                    all_export_windows, horizon_start=start, horizon_end=end)
                result.estimated_hourly_solar_forecast = self._serialize_solar_windows(
                    all_solar_windows, horizon_start=start, horizon_end=end)
                capacity = float(self._config.get(CONF_BATTERY_CAPACITY_KWH, DEFAULT_BATTERY_CAPACITY_KWH))
                if battery_soc_percent is not None:
                    result.battery_total_energy_kwh = round(capacity*battery_soc_percent/100, 3)
                    result.battery_remaining_capacity_kwh = max(0, round(capacity-result.battery_total_energy_kwh, 3))
                    result.battery_energy_available_kwh = max(0, round(result.battery_total_energy_kwh
                        - capacity*result.battery_min_soc_percent/100, 3))
                self._schedule_source_error_retry()
                return result

            total_energy_daily_average = (
                await self._async_get_average_daily_usage(total_energy_sensor)
                if total_energy_state and total_energy_sensor
                else 0.0
            )

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
                hourly_demand_table: dict[str, float] = {}
                demand_adjustment_factor = 1.0
            else:
                non_heating_daily_average = total_energy_daily_average
                hourly_demand_table = {}
                demand_adjustment_factor = 1.0
                if total_energy_sensor and total_energy_state:
                    runtime_state = (
                        self.hass.data.get(RUNTIME_STATE, {})
                        .get(self.planner_entry.entry_id, {})
                    )
                    current_energy = _coerce_float(total_energy_state.state)
                    if current_energy is not None:
                        hourly_demand_table = await self._async_update_expected_hourly_demand_table(
                            current_value=current_energy,
                            now=now,
                            daily_average_kwh=non_heating_daily_average,
                        )
                    else:
                        hourly_demand_table = observed_hourly_demand_table(
                            runtime_state.get(_EXPECTED_DEMAND_TABLE_KEY),
                            None,
                        )
                    demand_adjustment_factor = (
                        _coerce_float(
                            self.hass.data.get(RUNTIME_STATE, {})
                            .get(self.planner_entry.entry_id, {})
                            .get(_EXPECTED_DEMAND_ADJUSTMENT_KEY)
                        )
                        or 1.0
                    )

            result = self._build_plan(
                external_hourly_demand=external_demand,
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
                lookback_average_kwh=total_energy_daily_average if planner_kind == PLANNER_KIND_BATTERY else 0.0,
                total_energy_daily_average_kwh=total_energy_daily_average,
                non_heating_daily_average_kwh=non_heating_daily_average,
                hourly_demand_table=hourly_demand_table,
                demand_adjustment_factor=demand_adjustment_factor,
                battery_soc_percent=battery_soc_percent,
                price_resolution=price_resolution,
                source_status=source_status,
                source_errors=self._collect_source_errors(source_status),
            )
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
            if planner_kind == PLANNER_KIND_BATTERY and battery_soc_percent is not None:
                runtime_state = self.hass.data.setdefault(RUNTIME_STATE, {}).setdefault(
                    self.planner_entry.entry_id,
                    {},
                )
                self._store_battery_cycle_state_snapshot(now)
                await self._async_persist_runtime_state(runtime_state)
            if planner_kind == PLANNER_KIND_PRICE_WINDOW:
                runtime_state = self.hass.data.setdefault(RUNTIME_STATE, {}).setdefault(
                    self.planner_entry.entry_id, {},
                )
                if PRICE_PEAK_STATE_KEY in runtime_state:
                    await self._async_persist_runtime_state(runtime_state)
            return result
        except Exception as err:
            _LOGGER.exception("Planner update failed")
            # Always schedule a retry when an unexpected exception occurs so the
            # integration can recover without a manual reload.
            self._schedule_source_error_retry()
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

        return PlannerResult(
            planner_kind=planner_kind,
            status=status,
            score=0,
            recommendation="waiting_for_data",
            battery_strategy=battery_strategy,
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
            battery_no_charge_min_soc_percent=float(self._config.get(
                CONF_BATTERY_NO_CHARGE_MIN_SOC_PERCENT,
                self._config.get(CONF_BATTERY_MIN_SOC_PERCENT, DEFAULT_BATTERY_MIN_SOC_PERCENT),
            )),
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
            battery_charge_cycle_active=False,
            battery_discharge_cycle_active=False,
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
            battery_planner_engine="v2" if planner_kind == PLANNER_KIND_BATTERY else None,
            battery_planner_engine_reason="missing_data" if planner_kind == PLANNER_KIND_BATTERY else "disabled",
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

        return {"price_sensor": self._state_status(price_sensor, price_state)}

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
        }

    async def _async_update_battery_profit_tracking(
        self,
        *,
        current_battery_energy_kwh: float,
        current_mode: str,
        import_price: float | None,
        export_price: float | None,
    ) -> None:
        runtime_state = self.hass.data.setdefault(RUNTIME_STATE, {}).setdefault(self.planner_entry.entry_id, {})
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

    async def _async_update_expected_hourly_demand_table(
        self,
        *,
        current_value: float,
        now: datetime,
        daily_average_kwh: float,
    ) -> dict[str, float]:
        """Maintain compact learned hourly demand stats and an expected table."""
        runtime_state = self.hass.data.setdefault(RUNTIME_STATE, {}).setdefault(
            self.planner_entry.entry_id, {}
        )
        stats: dict[str, dict[str, Any]] = dict(runtime_state.get(_EXPECTED_DEMAND_STATS_KEY) or {})
        table: dict[str, float] = dict(runtime_state.get(_EXPECTED_DEMAND_TABLE_KEY) or {})
        last_value = _coerce_float(runtime_state.get(_EXPECTED_DEMAND_LAST_VALUE_KEY))
        last_hour_key = runtime_state.get(_EXPECTED_DEMAND_LAST_HOUR_KEY)
        last_ts_raw = runtime_state.get(_EXPECTED_DEMAND_LAST_TS_KEY)
        current_hour_key = now.weekday() * 24 + now.hour
        changed = False

        if last_value is None or last_hour_key is None:
            # First call: store the baseline without updating any slot.
            runtime_state[_EXPECTED_DEMAND_LAST_VALUE_KEY] = round(current_value, 3)
            runtime_state[_EXPECTED_DEMAND_LAST_HOUR_KEY] = current_hour_key
            runtime_state[_EXPECTED_DEMAND_LAST_TS_KEY] = now.isoformat()
            changed = True
        elif last_hour_key != current_hour_key:
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
                completed_hour_start = (last_ts or now).replace(minute=0, second=0, microsecond=0)
                self._update_today_demand_adjustment(
                    runtime_state,
                    hour_start=completed_hour_start,
                    actual_kwh=hourly_delta,
                    expected_kwh=_demand_profile_value_for_time(table, completed_hour_start),
                    now=now,
                )
                stats = update_expected_hourly_demand_stats(
                    stats,
                    slot_key=slot,
                    measured_kwh=hourly_delta,
                )
            runtime_state[_EXPECTED_DEMAND_LAST_VALUE_KEY] = round(current_value, 3)
            runtime_state[_EXPECTED_DEMAND_LAST_HOUR_KEY] = current_hour_key
            runtime_state[_EXPECTED_DEMAND_LAST_TS_KEY] = now.isoformat()
            runtime_state[_EXPECTED_DEMAND_STATS_KEY] = stats
            changed = True
        else:
            last_ts = dt_util.parse_datetime(last_ts_raw) if last_ts_raw else None
            elapsed_hours = (
                max(0.0, (now - last_ts).total_seconds() / 3600) if last_ts else 0.0
            )
            partial_delta = current_value - last_value
            if (
                elapsed_hours >= _DEMAND_TODAY_PARTIAL_MIN_HOURS
                and 0.0 <= partial_delta <= _DEMAND_PROFILE_MAX_KWH_PER_HOUR
            ):
                current_hour_start = now.replace(minute=0, second=0, microsecond=0)
                expected_kwh = _demand_profile_value_for_time(table, current_hour_start)
                if expected_kwh is not None and expected_kwh > 0:
                    partial_expected_kwh = expected_kwh * min(1.0, elapsed_hours)
                    changed = self._refresh_today_demand_adjustment(
                        runtime_state,
                        now=now,
                        partial_actual_kwh=partial_delta,
                        partial_expected_kwh=partial_expected_kwh,
                    ) or changed

        if changed or not table:
            table = build_expected_hourly_demand_table(
                stats,
                daily_average_kwh=daily_average_kwh,
            )
            runtime_state[_EXPECTED_DEMAND_TABLE_KEY] = table
            runtime_state[_EXPECTED_DEMAND_STATS_KEY] = stats
            await self._async_persist_runtime_state(runtime_state)

        return observed_hourly_demand_table(table, None)

    def _update_today_demand_adjustment(
        self,
        runtime_state: dict[str, Any],
        *,
        hour_start: datetime,
        actual_kwh: float,
        expected_kwh: float | None,
        now: datetime,
    ) -> None:
        today_key = dt_util.as_local(now).date().isoformat()
        today_state = runtime_state.get(_EXPECTED_DEMAND_TODAY_KEY)
        if not isinstance(today_state, dict) or today_state.get("date") != today_key:
            today_state = {
                "date": today_key,
                "actual_kwh": 0.0,
                "expected_kwh": 0.0,
                "completed_hours": 0,
                "seen_slots": [],
            }

        slot_key = str(dt_util.as_local(hour_start).weekday() * 24 + dt_util.as_local(hour_start).hour)
        seen_slots = {str(slot) for slot in today_state.get("seen_slots", [])}
        if slot_key in seen_slots:
            runtime_state[_EXPECTED_DEMAND_TODAY_KEY] = today_state
            return
        seen_slots.add(slot_key)

        today_state["actual_kwh"] = round(
            _coerce_float(today_state.get("actual_kwh"), default=0.0) + actual_kwh,
            4,
        )
        if expected_kwh is not None and expected_kwh > 0:
            today_state["expected_kwh"] = round(
                _coerce_float(today_state.get("expected_kwh"), default=0.0) + expected_kwh,
                4,
            )
        today_state["completed_hours"] = int(today_state.get("completed_hours", 0)) + 1
        today_state["seen_slots"] = sorted(seen_slots)
        runtime_state[_EXPECTED_DEMAND_TODAY_KEY] = today_state

        if (
            today_state["completed_hours"] >= _DEMAND_TODAY_ADJUSTMENT_MIN_COMPLETED_HOURS
            and _coerce_float(today_state.get("expected_kwh"), default=0.0) >= _DEMAND_TODAY_ADJUSTMENT_MIN_EXPECTED_KWH
        ):
            self._refresh_today_demand_adjustment(runtime_state, now=now)

    def _refresh_today_demand_adjustment(
        self,
        runtime_state: dict[str, Any],
        *,
        now: datetime,
        partial_actual_kwh: float = 0.0,
        partial_expected_kwh: float = 0.0,
    ) -> bool:
        today_key = dt_util.as_local(now).date().isoformat()
        today_state = runtime_state.get(_EXPECTED_DEMAND_TODAY_KEY)
        if not isinstance(today_state, dict) or today_state.get("date") != today_key:
            return False

        completed_hours = int(today_state.get("completed_hours", 0))
        expected_total = (
            (_coerce_float(today_state.get("expected_kwh"), default=0.0) or 0.0)
            + max(0.0, partial_expected_kwh)
        )
        if expected_total < _DEMAND_TODAY_ADJUSTMENT_MIN_EXPECTED_KWH:
            return False
        if completed_hours < _DEMAND_TODAY_ADJUSTMENT_MIN_COMPLETED_HOURS and partial_expected_kwh <= 0:
            return False

        actual_total = (
            (_coerce_float(today_state.get("actual_kwh"), default=0.0) or 0.0)
            + max(0.0, partial_actual_kwh)
        )
        raw_factor = actual_total / expected_total
        weight = (
            _DEMAND_TODAY_ADJUSTMENT_DOWN_WEIGHT
            if raw_factor < 1.0
            else _DEMAND_TODAY_ADJUSTMENT_UP_WEIGHT
        )
        damped_factor = 1.0 + ((raw_factor - 1.0) * weight)
        new_factor = round(
            min(DEMAND_ADJUSTMENT_MAX_FACTOR, max(DEMAND_ADJUSTMENT_MIN_FACTOR, damped_factor)),
            3,
        )
        previous_factor = _coerce_float(runtime_state.get(_EXPECTED_DEMAND_ADJUSTMENT_KEY), default=1.0)
        if previous_factor == new_factor:
            return False

        runtime_state[_EXPECTED_DEMAND_ADJUSTMENT_KEY] = new_factor
        today_state["last_adjustment_factor"] = new_factor
        if partial_expected_kwh > 0:
            today_state["partial_actual_kwh"] = round(max(0.0, partial_actual_kwh), 4)
            today_state["partial_expected_kwh"] = round(max(0.0, partial_expected_kwh), 4)
        runtime_state[_EXPECTED_DEMAND_TODAY_KEY] = today_state
        return True

    async def _async_persist_runtime_state(self, runtime_state: dict[str, Any]) -> None:
        # Battery and peak planners share the same store; serialize each read-modify-write.
        lock = self.hass.data.setdefault(f"{DOMAIN}_runtime_store_lock", asyncio.Lock())
        async with lock:
            store = Store[dict[str, Any]](self.hass, STORAGE_VERSION, STORAGE_KEY)
            data = await store.async_load() or {}
            entry_state = {
                **(data.get(self.planner_entry.entry_id, {}) or {}),
                PRICE_PEAK_STATE_KEY: runtime_state.get(PRICE_PEAK_STATE_KEY, {}),
                "battery_profit_total_eur": runtime_state.get("battery_profit_total_eur", 0.0),
                "battery_profit_cost_basis_eur": runtime_state.get("battery_profit_cost_basis_eur", 0.0),
                "battery_profit_tracked_energy_kwh": runtime_state.get("battery_profit_tracked_energy_kwh", 0.0),
                "battery_profit_last_energy_kwh": runtime_state.get("battery_profit_last_energy_kwh"),
                "battery_profit_last_updated": runtime_state.get("battery_profit_last_updated"),
                "battery_grid_charge_price": runtime_state.get("battery_grid_charge_price"),
                "battery_margin_state": runtime_state.get("battery_margin_state"),
                _BATTERY_CYCLE_STATE_KEY: runtime_state.get(_BATTERY_CYCLE_STATE_KEY, {}),
                _EXPECTED_DEMAND_STATS_KEY: runtime_state.get(_EXPECTED_DEMAND_STATS_KEY, {}),
                _EXPECTED_DEMAND_TABLE_KEY: runtime_state.get(_EXPECTED_DEMAND_TABLE_KEY, {}),
                _EXPECTED_DEMAND_ADJUSTMENT_KEY: runtime_state.get(_EXPECTED_DEMAND_ADJUSTMENT_KEY, 1.0),
                _EXPECTED_DEMAND_LAST_VALUE_KEY: runtime_state.get(_EXPECTED_DEMAND_LAST_VALUE_KEY),
                _EXPECTED_DEMAND_LAST_HOUR_KEY: runtime_state.get(_EXPECTED_DEMAND_LAST_HOUR_KEY),
                _EXPECTED_DEMAND_LAST_TS_KEY: runtime_state.get(_EXPECTED_DEMAND_LAST_TS_KEY),
                _EXPECTED_DEMAND_TODAY_KEY: runtime_state.get(_EXPECTED_DEMAND_TODAY_KEY, {}),
            }
            for legacy_key in _LEGACY_DEMAND_KEYS:
                entry_state.pop(legacy_key, None)
            data[self.planner_entry.entry_id] = entry_state
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

            async def load():
                return await get_recorder_instance(self.hass).async_add_executor_job(_load_history)
            shared = getattr(self, 'shared_sources', None)
            history_result = (await shared.async_cached(('battery_history', entity_id), load)
                              if shared else await load())
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


    @staticmethod
    def _two_cycle_end(windows):
        """End of two mode families; pauses and source changes are not cycles."""
        family = None
        cycles = 0
        end = None
        for window in windows:
            mode = window["mode"]
            if mode == "accu_uit" or window.get("standby"):
                continue
            next_family = "charge" if mode in ("laden_van_net", "laden_met_zonne_energie") else "discharge"
            if next_family != family:
                cycles += 1
                if cycles > 2:
                    break
                family = next_family
            end = datetime.fromisoformat(window["end"])
        return end

    @property
    def _cycle_export_enabled(self) -> bool:
        """Evaluate recharge after feasible surplus export, still energy-gated."""
        return bool(getattr(self, "_isolated_cycle_preview", False)
                    or getattr(self, "_cycle_export_planning", False))

    def _reconcile_elapsed_battery_phase(self, previous, now, soc):
        """Reconcile an accepted fill/discharge transition between SOC samples.

        Only the published execution prefix is evidence, never the outlook.
        A fill finishing between samples may already be discharging at the next
        sample. Require both its full endpoint and the measured endpoint to
        agree before releasing the latch. Only direction and the cycle boundary
        change: measured stock, acquisition lots and realized margin remain
        observed_stock's job.
        """
        at = datetime.fromisoformat(previous["at"])
        if at >= now:
            return
        capacity = float(self._config.get(CONF_BATTERY_CAPACITY_KWH, DEFAULT_BATTERY_CAPACITY_KWH))
        minimum = capacity * float(self._config.get(
            CONF_BATTERY_MIN_SOC_PERCENT, DEFAULT_BATTERY_MIN_SOC_PERCENT)) / 100.0
        full = capacity - minimum
        measured = capacity * soc / 100.0 - minimum
        projected = previous["energy"]
        completed_fill = False
        last_direction = None
        for action in sorted(previous.get("actions", []), key=lambda a: a["start"]):
            start = max(at, datetime.fromisoformat(action["start"]))
            end = min(now, datetime.fromisoformat(action["end"]))
            if end <= start:
                continue
            hours = (end-start).total_seconds() / 3600.0
            if action["mode"].startswith("laden_"):
                last_direction = "charge"
                self._note_battery_charge_source(action["mode"])
                self._charge_session_started = True
                projected += (action["solar_kw"] + action["grid_kw"]) * hours
                completed_fill = abs(projected - full) <= 0.005
            elif action["mode"].startswith("ontladen"):
                last_direction = "discharge"
                projected -= action["power"] * hours
        if last_direction == "charge":
            self._charge_session_started = True
            self._discharge_session_started = False
        elif (last_direction == "discharge" and completed_fill
                and abs(projected - measured) <= 0.005
                and abs(measured - previous["energy"]) > 1e-9):
            # The elapsed full endpoint ends the old cycle's permission just
            # like a measured-full sample. Reset before releasing its latch;
            # normal admission may then authorize a genuine next full refill.
            minimum_soc = float(self._config.get(
                CONF_BATTERY_MIN_SOC_PERCENT, DEFAULT_BATTERY_MIN_SOC_PERCENT))
            reserve = max(minimum_soc, min(100.0, float(self._config.get(
                CONF_BATTERY_NO_CHARGE_MIN_SOC_PERCENT, minimum_soc))))
            self._battery_cycle_discharge_floor_kwh = max(0.0, capacity * (reserve - minimum_soc) / 100)
            self._battery_charge_source = None
            self._charge_session_started = False
            self._discharge_session_started = True

    def _solar_transition_buffer_minutes(self) -> float:
        minutes = _coerce_float(self._config.get(CONF_BATTERY_SOLAR_TRANSITION_BUFFER_MINUTES),
                                default=DEFAULT_BATTERY_SOLAR_TRANSITION_BUFFER_MINUTES)
        return max(0.0, min(minutes or 0.0, MAX_BATTERY_SOLAR_TRANSITION_BUFFER_MINUTES))

    def _build_plan(self, **inputs) -> PlannerResult:
        """Execute two cycles; calculate any later outlook on isolated state."""
        if inputs["planner_kind"] != PLANNER_KIND_BATTERY:
            return self._build_plan_for_horizon(**inputs)

        if inputs["battery_soc_percent"] is None:
            # Startup sensors may remain unavailable over several refreshes.
            # Unknown SOC is not an empty battery: do not initialize, reverse
            # or persist a new cycle until a measured SOC is available.
            result = self._build_pending_result(
                "waiting_for_battery_soc", PLANNER_KIND_BATTERY,
                {**inputs["source_status"], "battery_soc_sensor": "waiting_for_battery_soc"},
                list(inputs["source_errors"]),
            )
            return result

        now = dt_util.now()
        tomorrow = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        horizon_end = tomorrow + timedelta(days=1)
        # A published fill can start between refreshes. Preserve that commanded
        # direction even if the measured net SOC still fell over an interval
        # containing both discharge and charge. This is a safety latch only:
        # observed_stock remains solely responsible for measured accounting.
        previous_margin = getattr(self, "_battery_margin_state", None)
        if previous_margin and inputs["battery_soc_percent"] < 100.0:
            self._reconcile_elapsed_battery_phase(previous_margin, now,
                inputs["battery_soc_percent"])
        # Keep the economic look-ahead: later known tariffs are necessary to
        # assess a charge cycle's profitability. Only the first two cycles are
        # published as executable commands; the outlook is computed afterwards.
        if inputs["battery_soc_percent"] >= 100.0:
            self._battery_charge_source = None
            if getattr(self, "_charge_session_started", False):
                # Measured completion ends the preceding commitment before
                # releasing its charge latch. Admit the next cycle afresh:
                # only its own executable full refill can authorize floor zero.
                capacity = float(self._config.get(CONF_BATTERY_CAPACITY_KWH, DEFAULT_BATTERY_CAPACITY_KWH))
                minimum = float(self._config.get(CONF_BATTERY_MIN_SOC_PERCENT, DEFAULT_BATTERY_MIN_SOC_PERCENT))
                reserve = max(minimum, min(100.0, float(self._config.get(
                    CONF_BATTERY_NO_CHARGE_MIN_SOC_PERCENT, minimum))))
                self._battery_cycle_discharge_floor_kwh = max(0.0, capacity * (reserve - minimum) / 100)
            self._charge_session_started = False
            self._discharge_session_started = True
        previous_export_planning = getattr(self, "_cycle_export_planning", False)
        self._cycle_export_planning = True
        direction_undecided = (not getattr(self, "_charge_session_started", False)
                               and not self._discharge_session_started)
        try:
            result = self._build_plan_for_horizon(**inputs)
            if direction_undecided and self._discharge_session_started:
                # A cold start can first establish discharge while selecting
                # a refill sized for the pre-discharge free capacity. Replan
                # once with that direction so the published refill reaches full.
                retry = copy(self)
                retry._store_battery_cycle_state_snapshot = lambda timestamp: None
                replacement = retry._build_plan_for_horizon(**inputs)
                if (replacement.battery_planner_engine_reason not in
                        ('optimizer_error', 'no_valid_plan', 'missing_data')
                        and (replacement.battery_planner_engine_reason != 'time_budget'
                             or replacement.planned_battery_mode_windows)):
                    result = replacement
                    for key, value in vars(retry).items():
                        if key != '_store_battery_cycle_state_snapshot':
                            setattr(self, key, value)
        finally:
            self._cycle_export_planning = previous_export_planning
        cutoff = self._two_cycle_end(result.planned_battery_mode_windows) or tomorrow
        result.planned_battery_mode_windows = [
            {**w, "end": min(datetime.fromisoformat(w["end"]), cutoff).isoformat(),
             "usable_hours": (min(datetime.fromisoformat(w["end"]), cutoff)
                              - datetime.fromisoformat(w["start"])).total_seconds() / 3600}
            for w in result.planned_battery_mode_windows
            if datetime.fromisoformat(w["start"]) < cutoff
        ]
        result.planned_battery_mode_schedule = [w for w in result.planned_battery_mode_schedule
                                               if datetime.fromisoformat(w["at"]) < cutoff]
        result.planned_battery_mode_schedule.append({"at": cutoff.isoformat(), "mode": "accu_uit"})

        # These complete series are presentation inputs only. They are attached
        # after all live decisions, summaries and persistent cycle state updates.
        prices = inputs["battery_switch_windows"] or inputs["all_windows"]
        exports = inputs["all_export_windows"] or inputs["export_windows"] or prices
        margin = max(0.0, min(0.5, float(self._config.get(
            CONF_BATTERY_CHARGE_SAFETY_MARGIN, DEFAULT_BATTERY_CHARGE_SAFETY_MARGIN,
        )) / 100.0))
        solar = [replace(w, forecast_kwh=max(0.0, w.forecast_kwh * (1.0 - margin)))
                 for w in inputs["all_solar_windows"]]
        demand = result.estimated_hourly_home_demand
        slots = build_energy_balance_slots(
            price_windows=prices, export_price_windows=exports, solar_windows=solar,
            hourly_demand=demand, horizon_start=now, demand_safety_margin=0.0,
            solar_transition_buffer_minutes=self._solar_transition_buffer_minutes(),
        )
        margin_state = getattr(self, "_battery_margin_state", None)
        if margin_state is not None:
            self._battery_margin_state = {**margin_state, "actions": execution_actions(
                result.planned_battery_mode_windows, slots,
                float(self._config.get(CONF_BATTERY_MAX_CHARGE_KW, DEFAULT_BATTERY_MAX_CHARGE_KW)),
                float(self._config.get(CONF_BATTERY_MAX_DISCHARGE_KW, DEFAULT_BATTERY_MAX_DISCHARGE_KW)))}
            self._store_battery_cycle_state_snapshot(now)
        for key in ("planned_solar_charge_windows", "planned_grid_charge_windows"):
            setattr(result, key, [w for w in getattr(result, key)
                                  if datetime.fromisoformat(w["end"]) <= cutoff])
        for key, value in summarize_battery_cycles(
            full_planned_mode_windows=result.planned_battery_mode_windows,
            energy_balance_slots=slots, now=now,
        ).items():
            if hasattr(result, key):
                setattr(result, key, value)
        if (result.next_charge_opportunity_start is not None
                and datetime.fromisoformat(result.next_charge_opportunity_start) >= cutoff):
            result.next_charge_opportunity_start = None
        capacity = result.battery_total_energy_kwh + result.battery_remaining_capacity_kwh
        minimum = capacity * result.battery_min_soc_percent / 100.0
        reserve = max(0.0, capacity * result.battery_no_charge_min_soc_percent / 100.0 - minimum)
        result.estimated_battery_mode_windows = self._estimate_unknown_price_plan(
            slots=slots, now=now, actual_windows=result.planned_battery_mode_windows,
            initial_energy=result.battery_energy_available_kwh, capacity=max(0.0, capacity-minimum),
            max_charge=float(self._config.get(CONF_BATTERY_MAX_CHARGE_KW, DEFAULT_BATTERY_MAX_CHARGE_KW)),
            max_discharge=float(self._config.get(CONF_BATTERY_MAX_DISCHARGE_KW, DEFAULT_BATTERY_MAX_DISCHARGE_KW)),
            reserve=reserve, min_profit=result.battery_min_profit_per_kwh,
            include_known_prices=True, preview_start=cutoff,
        ) if inputs["battery_soc_percent"] is not None else []
        result.upcoming_energy_price_windows = self._serialize_price_windows(
            inputs["all_windows"], horizon_start=now.replace(hour=0, minute=0, second=0, microsecond=0), horizon_end=horizon_end,
        )
        result.upcoming_export_price_windows = self._serialize_price_windows(
            exports, horizon_start=now-timedelta(hours=1), horizon_end=horizon_end,
        )
        result.estimated_hourly_solar_forecast = self._serialize_solar_windows(
            solar, horizon_start=now.replace(hour=0, minute=0, second=0, microsecond=0),
            horizon_end=horizon_end,
        )
        result.estimated_hourly_home_demand = demand
        return result

    def _build_plan_for_horizon(
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
        lookback_average_kwh: float,
        total_energy_daily_average_kwh: float,
        non_heating_daily_average_kwh: float,
        hourly_demand_table: dict[str, float],
        demand_adjustment_factor: float,
        battery_soc_percent: float | None,
        price_resolution: str,
        source_status: dict[str, str],
        source_errors: list[str],
        external_hourly_demand: list[dict] | None = None,
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
        battery_min_horizon_end: datetime | None = None
        if planner_kind == PLANNER_KIND_BATTERY:
            battery_min_horizon_end = (now + timedelta(days=1)).replace(
                hour=18,
                minute=0,
                second=0,
                microsecond=0,
            )
            if battery_min_horizon_end <= now:
                battery_min_horizon_end += timedelta(days=1)

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
            peak_windows = []
            most_expensive_price_window = select_contiguous_price_window(
                all_windows, now=now, duration_hours=duration_hours,
                cheapest=False, whole_hour_start=whole_hour_start,
            ) if not wants_most_expensive else None
            if wants_most_expensive:
                runtime_state = self.hass.data.setdefault(RUNTIME_STATE, {}).setdefault(
                    self.planner_entry.entry_id, {},
                )
                peak_state = plan_price_peaks(
                    all_windows if source_status.get("price_sensor") == "ok" else [],
                    now=now, duration_hours=duration_hours, whole_hour_start=whole_hour_start,
                    minimum_duration_minutes=float(self._config.get(
                        CONF_PRICE_PEAK_MIN_DURATION_MINUTES, DEFAULT_PRICE_PEAK_MIN_DURATION_MINUTES,
                    )),
                    minimum_gap_hours=float(self._config.get(
                        CONF_PRICE_PEAK_MIN_GAP_HOURS, DEFAULT_PRICE_PEAK_MIN_GAP_HOURS,
                    )),
                    minimum_prominence_percent=float(self._config.get(
                        CONF_PRICE_PEAK_MIN_PROMINENCE_PERCENT, DEFAULT_PRICE_PEAK_MIN_PROMINENCE_PERCENT,
                    )),
                    saved=runtime_state.get(PRICE_PEAK_STATE_KEY),
                    reselect_pending=not getattr(self, "_price_peaks_initialized", False),
                )
                if peak_state.get("startup_reselected"):
                    self._price_peaks_initialized = True
                runtime_state[PRICE_PEAK_STATE_KEY] = peak_state
                peak_windows = peak_state["windows"]
                most_expensive_price_window = next(
                    (w for w in peak_windows if datetime.fromisoformat(w["end"]) > now), None,
                )
            selected_price_window = (
                most_expensive_price_window
                if wants_most_expensive
                else cheapest_price_window
            )
            day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            day_end = day_start + timedelta(days=1)
            price_horizon_end = max((w.end for w in all_windows), default=day_end) if wants_most_expensive else day_end
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
                horizon_end=price_horizon_end,
            )
            result.selected_price_window = selected_price_window
            result.price_window_type = price_window_type
            result.cheapest_price_window = cheapest_price_window
            result.tomorrow_cheapest_price_window = tomorrow_cheapest_price_window
            result.most_expensive_price_window = most_expensive_price_window
            result.planned_price_peak_windows = peak_windows
            result.rationale = (
                "local price peaks locked in sequence; on duration and minimum off time are enforced"
                if wants_most_expensive else
                f"selected {duration_hours:g} contiguous hour price windows between "
                f"{day_start.isoformat()} and {day_end.isoformat()}"
            )
            return result

        today_solar_windows = [window for window in solar_windows if window.start.date() == now.date()]
        future_solar_windows = [window for window in solar_windows if window.start.date() > now.date()]
        best_solar_window = select_best_solar_window(today_solar_windows or solar_windows)
        planning_horizon_end = max(
            [window.end for window in [*all_windows, *all_solar_windows]]
            + ([battery_min_horizon_end] if planner_kind == PLANNER_KIND_BATTERY and battery_min_horizon_end else []),
            default=now + timedelta(days=1),
        )
        planning_start = min(
            (window.start for window in all_windows),
            default=now.replace(hour=0, minute=0, second=0, microsecond=0),
        )
        if external_hourly_demand is not None:
            estimated_hourly_home_demand = [dict(w) for w in external_hourly_demand]
            source_status = {**source_status, 'demand_forecast_sensor': 'ok'}
        else:
            estimated_hourly_home_demand = build_hourly_home_demand_forecast(
                heating_estimate_kwh=0.0,
                non_heating_daily_average_kwh=non_heating_daily_average_kwh,
                hourly_demand_table=hourly_demand_table,
                demand_adjustment_factor=demand_adjustment_factor,
                horizon_end=planning_horizon_end,
            )
            estimated_hourly_home_demand = align_price_responsive_demand_to_cheap_hours(
                estimated_hourly_home_demand, all_windows, now=now,
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
        charge_safety_margin = (
            max(0.0, min(0.5, float(self._config.get(
                CONF_BATTERY_CHARGE_SAFETY_MARGIN, DEFAULT_BATTERY_CHARGE_SAFETY_MARGIN,
            )) / 100.0)) if planner_kind == PLANNER_KIND_BATTERY else 0.0
        )
        planning_solar_windows = [
            replace(window, forecast_kwh=max(0.0, window.forecast_kwh * (1.0 - charge_safety_margin)))
            for window in all_solar_windows
        ]
        estimated_hourly_solar_forecast = self._serialize_solar_windows(
            planning_solar_windows,
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
        battery_enabled = bool(self._config.get(CONF_BATTERY_ENABLED, DEFAULT_BATTERY_ENABLED))
        battery_capacity = float(
            self._config.get(CONF_BATTERY_CAPACITY_KWH, DEFAULT_BATTERY_CAPACITY_KWH)
        )
        battery_min_soc_percent = float(
            self._config.get(CONF_BATTERY_MIN_SOC_PERCENT, DEFAULT_BATTERY_MIN_SOC_PERCENT)
        )
        battery_no_charge_min_soc_percent = max(battery_min_soc_percent, min(100.0, float(
            self._config.get(CONF_BATTERY_NO_CHARGE_MIN_SOC_PERCENT, battery_min_soc_percent)
        )))
        no_charge_reserve_kwh = max(
            0.0, battery_capacity * (battery_no_charge_min_soc_percent - battery_min_soc_percent) / 100,
        )
        max_charge = float(self._config.get(CONF_BATTERY_MAX_CHARGE_KW, DEFAULT_BATTERY_MAX_CHARGE_KW))
        max_discharge = float(
            self._config.get(CONF_BATTERY_MAX_DISCHARGE_KW, DEFAULT_BATTERY_MAX_DISCHARGE_KW)
        )
        minimum_battery_reserve_kwh = round(battery_capacity * max(0.0, min(100.0, battery_min_soc_percent)) / 100, 3)
        # Keep measured precision for command budgets. Rounding up at every
        # feedback update invents stock and can discharge below the reserve.
        battery_total_energy_kwh = (
            battery_capacity * (battery_soc_percent / 100)
            if battery_soc_percent is not None
            else 0.0
        )
        battery_energy_available_kwh = max(0.0, battery_total_energy_kwh - minimum_battery_reserve_kwh)
        battery_remaining_capacity_kwh = max(0.0, battery_capacity - battery_total_energy_kwh)
        usable_battery_capacity_kwh = max(0.0, battery_capacity - minimum_battery_reserve_kwh)
        remaining_usable_capacity_kwh = max(0.0, usable_battery_capacity_kwh - battery_energy_available_kwh)
        target_battery_full_by_sunset = battery_enabled and remaining_usable_capacity_kwh > 0
        energy_balance_slots = build_energy_balance_slots(
            price_windows=battery_switch_windows or all_windows,
            export_price_windows=all_export_windows or export_windows or all_windows,
            solar_windows=planning_solar_windows,
            hourly_demand=estimated_hourly_home_demand,
            horizon_start=now,
            demand_safety_margin=0.0,
            solar_transition_buffer_minutes=(self._solar_transition_buffer_minutes()
                                             if planner_kind == PLANNER_KIND_BATTERY else 0.0),
        )
        battery_planner = self
        if battery_soc_percent is None:
            battery_planner = copy(self)
            battery_planner._store_battery_cycle_state_snapshot = lambda timestamp: None
        source_args = dict(
            slots=energy_balance_slots,
            now=now,
            usable_capacity_kwh=usable_battery_capacity_kwh,
            current_remaining_capacity_kwh=remaining_usable_capacity_kwh,
            max_charge_kw=max_charge,
            max_discharge_kw=max_discharge,
            battery_min_profit=battery_min_profit,
            # Solar uncertainty is already included in the energy balance.
            charge_safety_margin=0.0,
            no_charge_reserve_kwh=no_charge_reserve_kwh,
        )
        engine = "v2" if planner_kind == PLANNER_KIND_BATTERY else None
        engine_reason = "disabled"
        selected = None
        if battery_enabled and engine:
            selected, engine_reason = battery_planner._select_battery_plan(
                source_args=source_args,
                slots=energy_balance_slots, now=now, initial_usable_energy_kwh=battery_energy_available_kwh,
                usable_capacity_kwh=usable_battery_capacity_kwh, battery_soc_percent=battery_soc_percent,
                average_price=average_price,
                average_export_price=export_price_average if export_price_average is not None else average_price,
                max_charge_kw=max_charge, max_discharge_kw=max_discharge,
                no_charge_reserve_kwh=no_charge_reserve_kwh, battery_min_profit=battery_min_profit,
            )
        planned_solar_charge_windows, planned_grid_charge_windows = selected or ([], [])
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
        active_no_charge_reserve_kwh = reserve_without_charge_opportunity(
            slots=energy_balance_slots,
            charge_windows=[*planned_solar_charge_windows, *planned_grid_charge_windows],
            after=now,
            reserve_kwh=no_charge_reserve_kwh,
        )
        battery_reserved_energy_kwh = min(battery_energy_available_kwh, active_no_charge_reserve_kwh)
        battery_energy_available_for_discharge_kwh = max(0.0, battery_energy_available_kwh - battery_reserved_energy_kwh)
        battery_exportable_energy_kwh = battery_energy_available_for_discharge_kwh
        battery_room_needed_for_solar_kwh = 0.0
        next_high_price_window = (
            self._find_next_high_price_window(
                future_windows=future_windows,
                average_price=average_price,
            )
            if price_signal_available
            else None
        )
        # A selected refill can still be rejected by the physical cycle
        # simulation. Such a discarded command must not release the reserve.
        full_planned_mode_windows, planned_current_mode = [], "accu_uit"
        for reserve_pass in range(2 if selected is not None else 0):
            full_planned_mode_windows, planned_current_mode = battery_planner._build_mode_windows_from_hourly_plan(
                slots=energy_balance_slots,
                now=now,
                planned_solar_charge_windows=planned_solar_charge_windows,
                planned_grid_charge_windows=planned_grid_charge_windows,
                initial_usable_energy_kwh=battery_energy_available_kwh,
                no_charge_reserve_kwh=no_charge_reserve_kwh,
                battery_min_profit=battery_min_profit,
                usable_capacity_kwh=usable_battery_capacity_kwh,
                battery_soc_percent=battery_soc_percent,
                average_price=average_price,
                average_export_price=export_price_average if export_price_average is not None else average_price,
                max_charge_kw=max_charge,
                max_discharge_kw=max_discharge,
            )
            has_executable_charge = any(w["mode"] in ("laden_van_net", "laden_met_zonne_energie")
                                        for w in full_planned_mode_windows)
            if has_executable_charge or not (planned_solar_charge_windows or planned_grid_charge_windows):
                break
            planned_solar_charge_windows = []
            planned_grid_charge_windows = []
            next_charge_opportunity = None
            active_no_charge_reserve_kwh = max(0.0, no_charge_reserve_kwh)
            battery_reserved_energy_kwh = min(battery_energy_available_kwh, active_no_charge_reserve_kwh)
            battery_energy_available_for_discharge_kwh = max(0.0, battery_energy_available_kwh - battery_reserved_energy_kwh)
            battery_exportable_energy_kwh = battery_energy_available_for_discharge_kwh
            home_demand_until_next_charge_kwh = round(
                sum_remaining_home_demand_until(estimated_hourly_home_demand, now, None), 3,
            )
        if planner_kind == PLANNER_KIND_BATTERY and battery_soc_percent is None:
            full_planned_mode_windows = []
            planned_current_mode = "accu_uit"
        elif planner_kind == PLANNER_KIND_BATTERY:
            # Keep the energy-limited durations from the simulation. Extending
            # short gaps or converting future charges based on today's SOC
            # changes the energy balance without recalculating it.
            planned_current_mode = normalize_full_battery_charge_mode(
                mode=planned_current_mode,
                usable_energy_kwh=battery_energy_available_kwh,
                usable_capacity_kwh=usable_battery_capacity_kwh,
            )
            planned_current_mode = _mode_at_time(full_planned_mode_windows, now) or planned_current_mode

        estimated_mode_windows = battery_planner._estimate_unknown_price_plan(
            slots=energy_balance_slots, now=now, actual_windows=full_planned_mode_windows,
            initial_energy=battery_energy_available_kwh, capacity=usable_battery_capacity_kwh,
            max_charge=max_charge, max_discharge=max_discharge,
            reserve=no_charge_reserve_kwh, min_profit=battery_min_profit,
        ) if selected is not None else []

        # Publish the capacity-limited windows actually simulated, including
        # later solar cycles that start with energy left in the battery.
        def actual_charge_windows(windows, mode):
            actual = []
            for window in windows:
                for command in full_planned_mode_windows:
                    if command["mode"] != mode:
                        continue
                    start = max(datetime.fromisoformat(window["start"]), datetime.fromisoformat(command["start"]))
                    end = min(datetime.fromisoformat(window["end"]), datetime.fromisoformat(command["end"]))
                    if end <= start:
                        continue
                    energy = 0.0
                    for slot in energy_balance_slots:
                        hours = max(0.0, (min(end, slot["end"]) - max(start, slot["start"])).total_seconds() / 3600)
                        rate = max_charge if mode == "laden_van_net" else min(
                            max_charge, chargeable_solar_kwh(slot) / max(float(slot["hours"]), 1e-9),
                        )
                        energy += hours * rate
                    actual.append({**window, "start": start.isoformat(), "end": end.isoformat(),
                                   "usable_hours": round((end - start).total_seconds() / 3600, 3),
                                   "charge_kwh": round(energy, 6)})
            return actual

        planned_solar_charge_windows = actual_charge_windows(planned_solar_charge_windows, "laden_met_zonne_energie")
        planned_grid_charge_windows = actual_charge_windows(planned_grid_charge_windows, "laden_van_net")
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
            battery_energy_available_for_discharge_kwh / max_discharge,
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

        battery_strategy = "accu_uit"
        if battery_enabled:
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
                        "grid charging uses a profitable price window before later demand, accounting for planned solar charging"
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
                elif active_no_charge_reserve_kwh > 0 and battery_energy_available_for_discharge_kwh <= 0:
                    rationale_parts.append(
                        f"battery keeps a {battery_no_charge_min_soc_percent:.0f}% reserve because no profitable charge window or solar surplus is forecast"
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
            current_export_price=next((w.price for w in all_export_windows if w.start <= now < w.end), None),
            upcoming_export_price_windows=self._serialize_price_windows(
                all_export_windows,
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
            estimated_battery_mode_windows=estimated_mode_windows,
            planned_battery_mode_windows=planned_battery_mode_windows,
            battery_soc_percent=battery_soc_percent,
            battery_min_soc_percent=battery_min_soc_percent,
            battery_no_charge_min_soc_percent=battery_no_charge_min_soc_percent,
            battery_no_charge_reserve_active=active_no_charge_reserve_kwh > 0,
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
            battery_charge_cycle_active=getattr(self, "_charge_session_started", False),
            battery_discharge_cycle_active=self._discharge_session_started,
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
            battery_planner_engine=engine,
            battery_planner_engine_reason=engine_reason,
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
    ) -> list[dict[str, str | float | bool]]:
        return [
            {
                "start": window.start.isoformat(),
                "end": min(window.end, horizon_end).isoformat(),
                "price": round(float(window.price), 6),
                "price_known": window.price_known,
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
        serialized: list[dict[str, str | float]] = []
        for window in windows:
            if window.end <= horizon_start or window.start >= horizon_end:
                continue
            duration_hours = max((window.end - window.start).total_seconds() / 3600.0, 0.0001)
            serialized.append({
                "start": window.start.isoformat(),
                "end": min(window.end, horizon_end).isoformat(),
                "estimated_kwh": round(float(window.forecast_kwh), 6),
                "estimated_kw": round(float(window.forecast_kwh) / duration_hours, 6),
            })
        return serialized

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
            charge_potential_kwh = min(max_charge_kw * float(slot["hours"]), chargeable_solar_kwh(slot))
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

    def _note_battery_charge_source(self, mode):
        # A solar tail cannot complete an unfinished mixed/grid charge phase.
        if (mode == "laden_van_net" or not (
            self._charge_session_started
            and getattr(self, "_battery_charge_source", None) == "laden_van_net"
        )):
            self._battery_charge_source = mode

    def _release_solar_charge_hold(self, reserve=None):
        """PV may end below full; keep its floor and measured accounting intact.

        This releases direction only, never a completed-full boundary. A new
        selected PV command must independently pass admission on every refresh.
        Missing/legacy source evidence retains the strict grid hold.
        """
        source = getattr(self, "_battery_charge_source", None)
        if source is None:
            source = getattr(self, "_active_charge_phase_mode", None)
        if (getattr(self, "_charge_session_started", False)
                and source == "laden_met_zonne_energie"):
            self._battery_charge_source = source
            self._charge_session_started = False
            self._discharge_session_started = True
            # A legacy solar plateau may lack a persisted boundary. Releasing
            # its direction cannot borrow a zero floor from a future refill.
            if (reserve is not None
                    and getattr(self, '_battery_cycle_discharge_floor_kwh', None) is None):
                self._battery_cycle_discharge_floor_kwh = reserve

    def _plan_bound_charge_windows(self, *, slots, now, usable_capacity_kwh,
                                   current_remaining_capacity_kwh, max_charge_kw,
                                   max_discharge_kw, battery_min_profit,
                                   no_charge_reserve_kwh, charge_safety_margin=0.0):
        """Select the boundary before sizing purchases against actual stock.

        A zero-floor trial must generate its own sources: reusing a reserve-
        sized refill after selecting a full discharge underfills by the reserve.
        Only its boundary decision is copied back, never its future phase.
        """
        self._release_solar_charge_hold(no_charge_reserve_kwh)
        if getattr(self, '_battery_solar_reserve_relaxation', False):
            # This permission is forecast-bound, unlike a committed normal cycle.
            self._battery_cycle_discharge_floor_kwh = no_charge_reserve_kwh
        self._battery_solar_reserve_relaxation = False
        initial = max(0.0, usable_capacity_kwh - current_remaining_capacity_kwh)
        boundary = getattr(self, "_battery_cycle_discharge_floor_kwh", None)
        if (getattr(self, "_charge_session_started", False)
                and initial >= usable_capacity_kwh - 0.005):
            # Source selection must see the newly completed cycle too. Waiting
            # for mode construction leaves its old fill latch blocking the
            # NEXT profitable refill, especially across the preview boundary.
            self._charge_session_started = False
            self._discharge_session_started = False
            boundary = no_charge_reserve_kwh
        if boundary is not None and boundary > 0.005:
            # A saved reserve decision follows the configured reserve; it is
            # not permission to drain to yesterday's lower setting.
            boundary = no_charge_reserve_kwh
        if (boundary is not None and getattr(self, "_discharge_session_started", False)
                and initial <= boundary + 0.005):
            self._discharge_session_started = False
            # Reaching the floor permits a new admission; it is not evidence
            # of a fill already executing, whose profitability may be waived.
            self._charge_session_started = False
            boundary = None
        source_args = dict(slots=slots, now=now, usable_capacity_kwh=usable_capacity_kwh,
                           current_remaining_capacity_kwh=current_remaining_capacity_kwh,
                           max_charge_kw=max_charge_kw, max_discharge_kw=max_discharge_kw,
                           battery_min_profit=battery_min_profit,
                           charge_safety_margin=charge_safety_margin,
                           loss_completion_reserve_kwh=no_charge_reserve_kwh)
        if boundary is None or boundary > 0.005:
            trial = copy(self)
            trial._store_battery_cycle_state_snapshot = lambda timestamp: None
            trial._battery_cycle_discharge_floor_kwh = 0.0
            solar, grid = trial._plan_charge_windows_for_horizon(
                **source_args, no_charge_reserve_kwh=0.0)
            trial._battery_cycle_discharge_floor_kwh = boundary
            trial._build_mode_windows_from_hourly_plan(
                slots=slots, now=now, planned_solar_charge_windows=solar,
                planned_grid_charge_windows=grid, initial_usable_energy_kwh=initial,
                usable_capacity_kwh=usable_capacity_kwh, max_charge_kw=max_charge_kw,
                max_discharge_kw=max_discharge_kw, battery_min_profit=battery_min_profit,
                no_charge_reserve_kwh=no_charge_reserve_kwh,
                average_price=sum(float(s['import_price']) for s in slots) / max(1, len(slots)),
                average_export_price=sum(float(s['export_price']) for s in slots) / max(1, len(slots)))
            boundary = trial._battery_cycle_discharge_floor_kwh
        if (no_charge_reserve_kwh > 0.005 and 0.005 < initial
                and boundary != 0.0
                and not getattr(self, '_charge_session_started', False)
                and sum(min(chargeable_solar_kwh(slot), max_charge_kw * float(slot['hours']))
                        for slot in slots if slot['end'] > now) >= usable_capacity_kwh - 0.005):
            trial = copy(self)
            trial._battery_solar_reserve_relaxation = True
            trial._battery_cycle_discharge_floor_kwh = None
            solar, grid = trial._plan_charge_windows_for_horizon(
                **source_args, no_charge_reserve_kwh=0.0)
            trial._battery_cycle_discharge_floor_kwh = no_charge_reserve_kwh
            plan = dict(slots=slots, now=now, initial_usable_energy_kwh=initial,
                        usable_capacity_kwh=usable_capacity_kwh, max_charge_kw=max_charge_kw,
                        max_discharge_kw=max_discharge_kw, battery_min_profit=battery_min_profit,
                        no_charge_reserve_kwh=no_charge_reserve_kwh,
                        average_price=sum(float(s['import_price']) for s in slots) / len(slots),
                        average_export_price=sum(float(s['export_price']) for s in slots) / len(slots))

            def completes_solar_cycle(solar, grid):
                return bool(solar) and trial._select_cycle_discharge_floor(
                    **plan, planned_solar_charge_windows=solar,
                    planned_grid_charge_windows=grid) == 0.0

            accepted = completes_solar_cycle(solar, grid)
            if not accepted and solar and not grid:
                # Only repair incomplete PV selection; leave grid admission alone.
                trial._full_arbitrage_intervals = []
                solar = next((block for block in self._full_solar_refill_blocks(
                    slots, now, usable_capacity_kwh, max_charge_kw, battery_min_profit)
                    if completes_solar_cycle(block, grid)), [])
                accepted = bool(solar)
            if accepted:
                self._battery_solar_reserve_relaxation = True
                self._battery_cycle_discharge_floor_kwh = 0.0
                self._full_arbitrage_intervals = trial._full_arbitrage_intervals
                return solar, grid
        self._battery_cycle_discharge_floor_kwh = boundary
        # A real unfinished fill is not an outlet for newly bought energy.
        # Keep its admission reserve without changing the execution boundary.
        source_floor = (no_charge_reserve_kwh if getattr(self, "_charge_session_started", False)
                        and initial < usable_capacity_kwh - 0.005 else boundary)
        return self._plan_charge_windows_for_horizon(
            **source_args, no_charge_reserve_kwh=source_floor)

    def _select_battery_plan(self, *, source_args, **plan):
        """Search from observed state without persisting rejected trial state."""
        if (plan['battery_soc_percent'] is None or not plan['slots']
                or min(plan['max_charge_kw'], plan['max_discharge_kw']) <= 0):
            return None, 'missing_data'
        continuing = getattr(self, '_charge_session_started', False)
        capacity = plan['usable_capacity_kwh']
        initial = plan['initial_usable_energy_kwh']
        reserve = min(initial, plan['no_charge_reserve_kwh'])
        unrestricted = ('grid', None)
        assessed = []

        def candidates():
            if continuing:
                # A measured commitment cannot be replaced by a new cycle.
                yield 'continuation', None
                return
            yield 'reserve_only', None
            yield unrestricted
            grid = next((purchased for variant, _, purchased, _ in assessed if variant == unrestricted), [])
            if grid:
                yield 'grid', frozenset()
                start = datetime.fromisoformat(grid[0]['start'])
                yield 'grid', frozenset(s['start'] for s in plan['slots'] if s['start'] > start)
            eligible = [s for s in plan['slots'] if s['end'] > plan['now'] and s.get('price_known', True)]
            prices = sorted({float(s['import_price']) for s in eligible})
            if len(prices) > 1:
                cap = prices[(len(prices) - 1) // 3]
                yield 'grid', frozenset(s['start'] for s in eligible if s['import_price'] <= cap)
            # Interleave full PV refill targets so one size cannot starve the others.
            sizes = sorted({capacity, capacity - initial, capacity - reserve}, reverse=True)
            generators = [self._full_solar_refill_blocks(
                plan['slots'], plan['now'], size, plan['max_charge_kw'], plan['battery_min_profit'])
                for size in sizes if size > .005]
            while generators:
                for generate in tuple(generators):
                    block = next(generate, None)
                    if block is None:
                        generators.remove(generate)
                    else:
                        yield 'solar', block

        def simulate(variant):
            kind, value = variant
            trial = copy(self)
            trial._store_battery_cycle_state_snapshot = lambda timestamp: None
            if getattr(trial, '_battery_solar_reserve_relaxation', False):
                trial._battery_solar_reserve_relaxation = False
                trial._battery_cycle_discharge_floor_kwh = plan['no_charge_reserve_kwh']
            trial._full_arbitrage_intervals = []
            if kind == 'solar':
                solar, purchased = value, []
            elif kind == 'reserve_only':
                solar, purchased = [], []
                if getattr(trial, '_battery_cycle_discharge_floor_kwh', None) != 0.0:
                    trial._battery_cycle_discharge_floor_kwh = plan['no_charge_reserve_kwh']
            else:
                if value is not None:
                    trial._optimizer_grid_starts = value
                solar, purchased = trial._plan_bound_charge_windows(**source_args)
                if (purchased and not solar and not continuing
                        and sum(float(w['charge_kwh']) for w in purchased)
                            < source_args['current_remaining_capacity_kwh'] - .005):
                    if variant != unrestricted:
                        return None
                    solar, purchased = [], []
                    trial._full_arbitrage_intervals = []
                    trial._battery_solar_reserve_relaxation = False
                    trial._battery_cycle_discharge_floor_kwh = plan['no_charge_reserve_kwh']
            admitted = copy(trial)
            windows = trial._build_mode_windows_from_hourly_plan(
                **plan, planned_solar_charge_windows=solar, planned_grid_charge_windows=purchased)[0]
            if (solar or purchased) and not any(w['mode'] in ('laden_van_net', 'laden_met_zonne_energie')
                                                for w in windows):
                if variant != unrestricted and not continuing:
                    return None
                # A rejected refill cannot release the reserve or allow export.
                solar, purchased = [], []
                admitted._full_arbitrage_intervals = []
                admitted._battery_solar_reserve_relaxation = False
                admitted._battery_cycle_discharge_floor_kwh = plan['no_charge_reserve_kwh']
                trial = copy(admitted)
                windows = trial._build_mode_windows_from_hourly_plan(
                    **plan, planned_solar_charge_windows=[], planned_grid_charge_windows=[])[0]
            assessed.append((variant, solar, purchased, admitted))
            if not getattr(self, '_cycle_export_planning', False):
                return windows
            # Score the commands that _build_plan publishes, not the outlook.
            tomorrow = plan['now'].replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
            cutoff = self._two_cycle_end(windows) or tomorrow
            return [{**w, 'end': min(datetime.fromisoformat(w['end']), cutoff).isoformat()}
                    for w in windows if datetime.fromisoformat(w['start']) < cutoff]

        try:
            selected, reason = optimize_battery_plan(candidates=candidates(), simulate=simulate,
                settings=dict(slots=plan['slots'], now=plan['now'], initial=initial, capacity=capacity,
                              max_charge=plan['max_charge_kw'], max_discharge=plan['max_discharge_kw']))
            if selected is None:
                return None, reason
            _, solar, purchased, admitted = next(row for row in assessed if row[0] is selected)
            # Commit only selected source permissions, never speculative future SOC.
            for attr in ('_full_arbitrage_intervals', '_battery_cycle_discharge_floor_kwh',
                         '_battery_solar_reserve_relaxation', '_charge_session_started',
                         '_discharge_session_started'):
                if hasattr(admitted, attr):
                    setattr(self, attr, getattr(admitted, attr))
            return (solar, purchased), 'active_charge' if continuing and reason != 'time_budget' else reason
        except Exception:
            _LOGGER.exception("Battery optimization failed; no new battery commands")
            return None, 'optimizer_error'

    def _full_solar_refill_blocks(self, slots, now, capacity_kwh, max_charge_kw, min_profit):
        """Yield full, contiguous PV refills in chronological start order."""
        if max_charge_kw <= 0 or capacity_kwh <= 0:
            return
        future = sorted((slot for slot in slots if slot['end'] > now),
                        key=lambda slot: slot['start'])
        if not future:
            return
        run = []
        for slot in future:
            start = max(now, slot['start'])
            hours = (slot['end'] - start).total_seconds() / 3600
            rate = min(max_charge_kw, chargeable_solar_kwh(slot) / max(float(slot['hours']), 1e-9))
            price = float(slot['export_price'])
            if not (slot.get('price_known', True) and float(slot['import_price']) >= 0
                    and rate * hours > 1e-6 and self._has_profitable_discharge(
                        slots=future, start=slot['end'], end=future[-1]['end'],
                        cost=price, minimum_profit=min_profit)):
                run = []
                continue
            if run and run[-1][1] != start:
                run = []
            run.append((start, slot['end'], rate, price))
            # Each suffix only becomes a candidate once it can fill the battery.
            # Later starts also leave more time to discharge before sunrise.
            while run and sum((end - begin).total_seconds() / 3600 * kw
                              for begin, end, kw, _ in run) >= capacity_kwh - 1e-9:
                remaining = capacity_kwh
                block = []
                for begin, end, kw, tariff in run:
                    hours = min((end - begin).total_seconds() / 3600, remaining / kw)
                    amount = hours * kw
                    block.append(dict(start=begin.isoformat(),
                                      end=(begin + timedelta(hours=hours)).isoformat(),
                                      price=tariff, usable_hours=hours, charge_kwh=amount))
                    remaining -= amount
                    if remaining <= 1e-9:
                        break
                yield block
                run.pop(0)

    def _plan_charge_windows_for_horizon(
        self,
        *,
        slots: list[dict[str, Any]],
        now: datetime,
        usable_capacity_kwh: float,
        current_remaining_capacity_kwh: float,
        max_charge_kw: float,
        max_discharge_kw: float,
        battery_min_profit: float,
        charge_safety_margin: float = 0.0,
        no_charge_reserve_kwh: float = 0.0,
        loss_completion_reserve_kwh: float = 0.0,
    ) -> tuple[list[dict[str, str | float]], list[dict[str, str | float]]]:
        planned_solar_charge_windows: list[dict[str, str | float]] = []
        planned_grid_charge_windows: list[dict[str, str | float]] = []
        self._full_arbitrage_intervals = []
        future_slots = sorted((slot for slot in slots if slot["end"] > now), key=lambda item: item["start"])
        if not future_slots or usable_capacity_kwh <= 0 or max_charge_kw <= 0:
            return planned_solar_charge_windows, planned_grid_charge_windows

        current_usable_kwh = max(0.0, usable_capacity_kwh - current_remaining_capacity_kwh)
        # Reconcile elapsed execution and measured SOC just as mode construction
        # does. Raw persisted lots can already have been consumed since storage.
        admission_stock = observed_stock(getattr(self, "_battery_margin_state", None),
            now, current_usable_kwh, 0.0, battery_min_profit)
        admission_costs = [float(cost) for quantity, cost in admission_stock.lots
                           if quantity > 1e-9 and cost is not None]
        historical_price = getattr(self, "_battery_grid_charge_price", None)
        if current_usable_kwh > .01 and historical_price is not None:
            admission_costs.append(float(historical_price))
        admission_acquisition_cost = max(admission_costs, default=None)
        # The live discharge latch protects the current cycle, not the entire
        # forecast horizon. A later grid cycle can start after its depletion.
        first_grid_charge_start = now
        active_grid_cycle = (
            getattr(self, "_active_charge_phase_mode", "accu_uit") == "laden_van_net"
            and self._active_charge_phase_end is not None
            and now < self._active_charge_phase_end
        )
        # Partial cycles may reverse before empty; source eligibility is economic,
        # not derived from a historical charge/discharge latch.
        first_charge_not_before = now


        min_discharge_gap = (
            timedelta(hours=usable_capacity_kwh / max_discharge_kw)
            if max_discharge_kw > 0
            else timedelta.max
        )
        horizon_end = max(cast(datetime, slot["end"]) for slot in future_slots)
        selected_solar_charge_by_start: dict[datetime, float] = {}
        selected_grid_charge_by_start: dict[datetime, float] = {}
        eligible_grid_starts = getattr(self, '_optimizer_grid_starts', None)

        def _solar_cost(slot: dict[str, Any]) -> float:
            # Storing solar forgoes the configured export revenue. That tariff
            # already includes any deduction; never subtract tax a second time.
            return float(slot["export_price"])

        def _slot_candidates(after: datetime) -> list[dict[str, Any]]:
            candidates: list[dict[str, Any]] = []
            for slot in future_slots:
                if not slot.get("price_known", True):
                    # Estimated tariffs are evaluated only by the isolated
                    # preview, including negative estimated solar prices.
                    continue
                slot_start = cast(datetime, slot["start"])
                slot_end = cast(datetime, slot["end"])
                if slot_end <= after:
                    continue
                # A future depletion boundary must not charge before discharge
                # finishes. The current quarter, however, remains available:
                # excluding it at every refresh postpones charging indefinitely.
                if self._cycle_export_enabled and after > now and slot_start < after:
                    continue
                active_start = max(slot_start, after)
                active_hours = max((slot_end - active_start).total_seconds() / 3600, 0.0)
                if active_hours <= 0:
                    continue
                slot_hours = max(float(slot["hours"]), 1e-9)
                active_fraction = min(1.0, active_hours / slot_hours)
                slot_capacity_kwh = max_charge_kw * active_hours
                transition = slot.get("solar_transition")
                solar_kwh = min(slot_capacity_kwh, max(0.0, float(slot["net_solar_kwh"])) * active_fraction)
                negative_grid_price = slot.get("price_known", True) and float(slot["import_price"]) < 0
                if solar_kwh > 0 and not negative_grid_price and not transition:
                    candidates.append(
                        {
                            "kind": "solar",
                            "start": slot_start,
                            "end": slot_end,
                            "charge_kwh": round(solar_kwh, 6),
                            "cost": round(_solar_cost(slot), 6),
                        }
                    )
                # Grid charging means solar plus a grid top-up, sharing the
                # inverter limit. Price the solar portion at its export value;
                # only the remaining capacity is bought from the grid.
                grid_kwh = slot_capacity_kwh
                import_price = float(slot["import_price"])
                # Genuine imports remain subject to normal fill/margin checks;
                # a PV-only grid command cannot sidestep a buffered solar slot.
                if (grid_kwh > 0 and (eligible_grid_starts is None or slot_start in eligible_grid_starts)
                        and (not transition or grid_kwh - solar_kwh > 1e-6)):
                    mixed_cost = (
                        (solar_kwh * _solar_cost(slot) if solar_kwh > 0 else 0.0)
                        + max(0.0, grid_kwh - solar_kwh) * import_price
                    ) / grid_kwh
                    candidates.append(
                        {
                            "kind": "grid",
                            "start": slot_start,
                            "end": slot_end,
                            "charge_kwh": round(grid_kwh, 6),
                            "cost": mixed_cost,
                            # Cheap solar must not hide an unprofitable import.
                            "profit_cost": import_price,
                        }
                    )
            return candidates

        def _future_peak_price(after: datetime, *, known_only: bool = False) -> float | None:
            prices = [
                float(slot["import_price"])
                for slot in future_slots
                if cast(datetime, slot["start"]) >= after
                and (not known_only or slot.get("price_known", True))
            ]
            return max(prices, default=None)

        def _is_profitable_charge(candidate: dict[str, Any]) -> bool:
            if getattr(self, "_charge_session_started", False) and cycle_index == 0:
                # Admission happened before this observed partial fill. Finish
                # safely; a refreshed price must not reverse it before full.
                return True
            profit_cost = float(candidate.get("profit_cost", candidate["cost"]))
            if candidate["kind"] == "solar":
                # Surplus slots are valued at foregone export, not at an
                # import tariff the household cannot actually avoid there.
                return self._has_profitable_discharge(
                    slots=future_slots, start=candidate["end"], end=horizon_end,
                    cost=profit_cost, minimum_profit=battery_min_profit,
                )
            if profit_cost < 0:
                return True
            peak_price = _future_peak_price(
                cast(datetime, candidate["end"]), known_only=False,
            )
            return (
                peak_price is not None
                # Imports earn the configured margin as a complete weighted
                # supplement, not separately in every purchase quarter.
                and (candidate["kind"] == "grid" or peak_price - profit_cost >= battery_min_profit)
            )

        def _profitable_discharge_capacity(after: datetime, before: datetime, cost: float,
                                           *, include_estimated: bool = False) -> float:
            """Count each kWh once: house at import value, surplus at export value."""
            capacity = 0.0
            for slot in future_slots:
                if not include_estimated and not slot.get("price_known", True):
                    continue
                hours = max(0.0, (min(before, slot["end"]) - max(after, now, slot["start"])).total_seconds() / 3600)
                power_energy = max_discharge_kw * hours
                home = min(power_energy, max(0.0, -float(slot["net_solar_kwh"]))
                           * hours / max(float(slot["hours"]), 1e-9))
                home_profitable = float(slot["import_price"]) + 1e-9 >= cost + battery_min_profit
                if home_profitable:
                    capacity += home
                if (self._cycle_export_enabled and slot["start"] >= before - timedelta(hours=8)
                        and slot.get("price_known", True) and (home <= 1e-9 or home_profitable)
                        and float(slot["export_price"]) > 0
                        and float(slot["export_price"]) + 1e-9 >= cost + battery_min_profit):
                    capacity += max(0.0, power_energy - home)
            return capacity

        def _primary_charge_cost(candidates: list[dict[str, Any]]) -> float | None:
            primary_costs = [
                float(candidate["cost"])
                for candidate in candidates
                if _is_profitable_charge(candidate)
            ]
            return min(primary_costs, default=None)

        def _project_usable_energy_until(until: datetime, *, include_export: bool = False) -> float:
            projected_usable_kwh = current_usable_kwh
            selected_acquisition_cost = admission_acquisition_cost
            has_selected_charge = False
            projected_charging = bool(getattr(self, "_charge_session_started", False))
            projected_discharging = bool(getattr(self, "_discharge_session_started", False))
            for slot in future_slots:
                slot_start = cast(datetime, slot["start"])
                slot_end = cast(datetime, slot["end"])
                if slot_start >= until:
                    break
                if slot_end <= now:
                    continue
                active_start = max(slot_start, now)
                active_end = min(slot_end, until)
                active_hours = max((active_end - active_start).total_seconds() / 3600, 0.0)
                if active_hours <= 0:
                    continue
                slot_hours = max(float(slot.get("hours", active_hours)), 1e-9)
                active_fraction = min(1.0, active_hours / slot_hours)
                net_battery_demand_kwh = max(0.0, -float(slot.get("net_solar_kwh", 0.0))) * active_fraction
                discharge_kwh = min(max_discharge_kw * active_hours, net_battery_demand_kwh)

                # Admission uses the selected execution floor, but committed
                # loss completion is limited by the original higher reserve.
                # Retaining stock that execution will export undersizes the
                # refill. The household acquisition-price guard below remains
                # strict even for this explicitly committed completion.
                committed_loss_export = (
                    not has_selected_charge and not projected_charging
                    and getattr(self, "_battery_cycle_discharge_floor_kwh", None) == 0.0
                    and not getattr(self, "_battery_require_profitable_cycle", False)
                    and 0.0 < projected_usable_kwh <= loss_completion_reserve_kwh
                )
                if ((include_export or committed_loss_export or (not has_selected_charge
                        and not getattr(self, "_charge_session_started", False)
                        and getattr(self, "_battery_cycle_discharge_floor_kwh", None) == 0.0))
                        and self._cycle_export_enabled and slot.get("price_known", True)
                        and float(slot["export_price"]) > 0
                        and (committed_loss_export or selected_acquisition_cost is None
                             or float(slot["export_price"]) + 1e-9 >= selected_acquisition_cost + battery_min_profit)):
                    discharge_kwh = max_discharge_kw * active_hours
                grid_charge = selected_grid_charge_by_start.get(slot_start, 0.0)
                solar_charge = selected_solar_charge_by_start.get(slot_start, 0.0)
                selected_charge = grid_charge + solar_charge
                if (selected_charge > 0 and projected_discharging
                        and projected_usable_kwh > no_charge_reserve_kwh + 0.005):
                    selected_charge = 0.0
                if selected_charge > 0:
                    has_selected_charge = True
                    # Selected commands occupy their actual powered duration;
                    # never subtract household discharge during that command.
                    rate = max_charge_kw if grid_charge > 0 else min(
                        max_charge_kw, chargeable_solar_kwh(slot) / slot_hours)
                    charged = min(selected_charge, rate * active_hours)
                    projected_usable_kwh = min(usable_capacity_kwh, projected_usable_kwh + charged)
                    projected_charging = projected_usable_kwh < usable_capacity_kwh - 0.005
                    projected_discharging = not projected_charging
                    acquisition_cost = float(slot['import_price'] if grid_charge > 0
                                             else slot['export_price'])
                    selected_acquisition_cost = max(acquisition_cost,
                        selected_acquisition_cost if selected_acquisition_cost is not None else acquisition_cost)
                else:
                    # Do not consume a just-selected cheap fill at its purchase
                    # price in this projection, then buy dearer solar to replace
                    # it. Execution allocates stock to the higher-value demand.
                    if (net_battery_demand_kwh > 1e-9 and selected_acquisition_cost is not None
                            and float(slot['import_price']) + 1e-9
                            < selected_acquisition_cost + battery_min_profit):
                        discharge_kwh = (min(max_discharge_kw * active_hours, net_battery_demand_kwh)
                            if (getattr(self, '_battery_solar_reserve_relaxation', False)
                                and not has_selected_charge and slot.get('price_known', True)
                                and projected_usable_kwh <= loss_completion_reserve_kwh + 1e-9
                                and float(slot['import_price']) + 1e-9 >= selected_acquisition_cost)
                            else 0.0)
                    if projected_charging:
                        discharge_kwh = 0.0
                    projected_usable_kwh -= min(discharge_kwh, max(
                        0.0, projected_usable_kwh - no_charge_reserve_kwh))
                    if projected_usable_kwh <= no_charge_reserve_kwh + 0.005:
                        projected_discharging = False
            return projected_usable_kwh

        def _append_charge_candidates(
            selected_for_cycle: list[dict[str, Any]],
            candidates: list[dict[str, Any]],
            *,
            target_kwh: float,
            charged_kwh: float,
            used_starts: set[datetime] | None = None,
        ) -> float:
            remaining = list(candidates)
            while remaining and charged_kwh < target_kwh:
                choices = []
                for index, candidate in enumerate(remaining):
                    start = candidate["start"]
                    if used_starts is not None and start in used_starts:
                        continue
                    previous = next((item for item in selected_for_cycle if item["start"] == start), None)
                    old_kwh = float(previous["charge_kwh"]) if previous else 0.0
                    extra_kwh = float(candidate["charge_kwh"]) - old_kwh
                    if extra_kwh <= 1e-9:
                        continue
                    if previous and not (previous["kind"] == "solar" and candidate["kind"] == "grid"):
                        continue
                    # Replacing solar-only with mixed charging uses this slot
                    # once. Compare the extra energy and cost, retaining the
                    # solar contribution within the shared charging power.
                    cost = float(candidate["cost"])
                    if previous:
                        added_kwh = min(extra_kwh, target_kwh - charged_kwh)
                        cost = (cost * (old_kwh + added_kwh)
                                - float(previous["cost"]) * old_kwh) / added_kwh
                    choices.append((cost, index, previous, old_kwh, extra_kwh))
                if not choices:
                    break
                _, index, previous, old_kwh, extra_kwh = min(choices, key=lambda choice: (choice[0], choice[1]))
                candidate = remaining.pop(index)
                take_kwh = min(extra_kwh, target_kwh - charged_kwh)
                selected = {**candidate, "charge_kwh": round(old_kwh + take_kwh, 6)}
                if previous:
                    selected_for_cycle.remove(previous)
                selected_for_cycle.append(selected)
                charged_kwh += take_kwh
                if used_starts is not None:
                    used_starts.add(candidate["start"])
            return charged_kwh

        def _selected_solar_energy(selected):
            by_start = {slot["start"]: slot for slot in future_slots}
            return sum(float(item["charge_kwh"]) * (
                1.0 if item["kind"] == "solar" else min(1.0,
                    max(0.0, float(by_start[item["start"]]["net_solar_kwh"])) / max(
                        max_charge_kw * float(by_start[item["start"]]["hours"]), 1e-9)))
                for item in selected)

        retry_charge_deadlines: dict[datetime, datetime] = {}
        cursor = first_charge_not_before
        cycle_index = 0
        previous_charge_end: datetime | None = None
        previous_charge_shortfall_kwh = 0.0
        previous_charge_cost: float | None = None
        while cursor < horizon_end:
            selected_for_cycle: list[dict[str, Any]] = []
            charged_kwh = 0.0
            cycle_candidates = _slot_candidates(cursor)
            # Compare every eligible source by effective price. Solar availability
            # does not make a more expensive solar slot preferable to cheap grid.
            profitable_primary_candidates = [
                candidate for candidate in cycle_candidates
                if _is_profitable_charge(candidate)
                and (self._cycle_export_enabled
                     or (candidate["kind"] == "solar" and not self._discharge_session_started) or (
                    first_grid_charge_start is not None
                    and max(now, candidate["start"]) >= first_grid_charge_start
                ))
            ]
            if not profitable_primary_candidates:
                break

            best_candidate = min(
                profitable_primary_candidates,
                key=lambda item: (
                    item["start"].date(),
                    float(item["cost"]), 0 if item["kind"] == "solar" else 1, item["start"],
                ),
            )

            # Insert an earlier complete cycle only when all usable capacity
            # can earn the margin before the cheaper refill. A single peak
            # quarter cannot justify filling an entire battery.
            bridge = None
            refill_start = cast(datetime, best_candidate["start"])
            earlier_grid = [item for item in profitable_primary_candidates
                            if item["kind"] == "grid" and item["end"] < refill_start]
            for finish in sorted({item["end"] for item in earlier_grid}):
                choices = sorted((item for item in earlier_grid if item["end"] <= finish),
                                 key=lambda item: (item["cost"], item["start"]))
                needed = (current_remaining_capacity_kwh if cycle_index == 0
                          and getattr(self, "_charge_session_started", False) else usable_capacity_kwh)
                chosen = []
                for item in choices:
                    if needed <= 1e-6:
                        break
                    chosen.append(item)
                    needed -= float(item["charge_kwh"])
                if needed > 1e-6 or not chosen:
                    continue
                cost = max(float(item["profit_cost"]) for item in chosen)
                if _profitable_discharge_capacity(finish, refill_start, cost) + 1e-6 < usable_capacity_kwh:
                    continue
                score = sum(float(item["cost"]) * float(item["charge_kwh"]) for item in chosen)
                if bridge is None or score < bridge[0]:
                    bridge = (score, finish, chosen)
            if bridge is not None:
                cycle_candidates = [item for item in cycle_candidates if item["end"] <= bridge[1]]
                profitable_primary_candidates = [item for item in profitable_primary_candidates if item["end"] <= bridge[1]]
                best_candidate = min(bridge[2], key=lambda item: (item["cost"], item["start"]))

            # A daytime solar fill must finish within that day's charging
            # opportunity. Tomorrow's sunshine belongs to a later cycle:
            # otherwise it displaces today's grid supplement, then disappears
            # when the mode simulation starts the intervening discharge.
            # Grid-led overnight cycles may still cross midnight, and the
            # full horizon remains available for their economic justification.
            charge_deadline = horizon_end
            if best_candidate["kind"] == "solar":
                charge_deadline = min(horizon_end, best_candidate["start"].replace(
                    hour=0, minute=0, second=0, microsecond=0,
                ) + timedelta(days=1))
            else:
                # Overnight grid charging may cross midnight, but the next
                # solar day is a separate opportunity. Limit both solar-only
                # and mixed grid/solar candidates: their cheap solar share
                # otherwise displaces today's unfinished grid fill as well.
                charge_deadline = min((slot["start"] for slot in future_slots
                    if slot["start"].date() > best_candidate["start"].date()
                    and float(slot.get("solar_kwh", 0.0)) > 1e-9), default=horizon_end)
            selection_deadline = min(charge_deadline, retry_charge_deadlines.get(cursor, charge_deadline))
            cycle_candidates = [item for item in cycle_candidates if item["end"] <= selection_deadline]
            profitable_primary_candidates = [item for item in profitable_primary_candidates
                                             if item["end"] <= selection_deadline]

            # Do not begin a partial refill before an intervening valuable
            # discharge period. That would force existing stock out early and
            # lock charging through the peak. A full earlier cycle remains
            # eligible; this applies equally to grid-only and mixed refills.
            if (bridge is None and self._discharge_session_started
                    and current_usable_kwh > _BATTERY_DEPLETION_EPSILON_KWH
                    and previous_charge_end is None):
                anchor_import = next((float(item["profit_cost"]) for item in cycle_candidates
                    if item["kind"] == "grid" and item["start"] == best_candidate["start"]), None)
                peaks = [slot for slot in future_slots
                    if anchor_import is not None and cursor <= slot["start"] < best_candidate["start"]
                    and float(slot["net_solar_kwh"]) < -1e-9
                    and float(slot["import_price"]) >= anchor_import + battery_min_profit]
                if peaks:
                    peak_start, peak_end = peaks[0]["start"], peaks[-1]["end"]
                    def capacity_between(start, end):
                        capacity = {}
                        for item in profitable_primary_candidates:
                            if start <= item["start"] and item["end"] <= end:
                                capacity[item["start"]] = max(capacity.get(item["start"], 0.),
                                                             float(item["charge_kwh"]))
                        return sum(capacity.values())
                    if (capacity_between(cursor, peak_start) + 1e-6 < usable_capacity_kwh
                            and capacity_between(peak_end, selection_deadline) + 1e-6 >= usable_capacity_kwh):
                        cycle_candidates = [item for item in cycle_candidates if item["start"] >= peak_end]
                        profitable_primary_candidates = [item for item in profitable_primary_candidates
                                                         if item["start"] >= peak_end]

            candidates_by_start: dict[datetime, list[dict[str, Any]]] = {}
            for candidate in cycle_candidates:
                candidates_by_start.setdefault(cast(datetime, candidate["start"]), []).append(candidate)

            valley_slots = [
                slot for slot in future_slots
                if cast(datetime, slot["end"]) > cursor
                and cast(datetime, slot["end"]) <= charge_deadline
            ]
            best_index = next(
                (
                    index
                    for index, slot in enumerate(valley_slots)
                    if cast(datetime, slot["start"]) == cast(datetime, best_candidate["start"])
                ),
                None,
            )
            if best_index is None:
                break

            projected_usable_at_charge_kwh = _project_usable_energy_until(cast(datetime, best_candidate["start"]))
            # Partial cycles carry actual stock forward. An earlier partial
            # fill is not an empty battery, and a future refill never releases
            # the configured discharge reserve.
            target_kwh = max(0.0, usable_capacity_kwh - projected_usable_at_charge_kwh)
            if target_kwh <= 0.005:
                next_cursor = cast(datetime, best_candidate["end"])
                if next_cursor <= cursor:
                    next_cursor = cursor + timedelta(minutes=1)
                cursor = next_cursor
                cycle_index += 1
                continue

            valley_indexes = {best_index}
            left_index = best_index - 1
            right_index = best_index + 1
            while True:
                options: list[tuple[float, int]] = []
                if left_index >= 0:
                    left_slot = valley_slots[left_index]
                    left_next = valley_slots[left_index + 1]
                    left_start = cast(datetime, left_slot["start"])
                    if cast(datetime, left_slot["end"]) == cast(datetime, left_next["start"]):
                        left_cost = _primary_charge_cost(candidates_by_start.get(left_start, []))
                        if left_cost is not None:
                            options.append((left_cost, left_index))
                if right_index < len(valley_slots):
                    right_slot = valley_slots[right_index]
                    right_prev = valley_slots[right_index - 1]
                    right_start = cast(datetime, right_slot["start"])
                    if cast(datetime, right_prev["end"]) == cast(datetime, right_slot["start"]):
                        right_cost = _primary_charge_cost(candidates_by_start.get(right_start, []))
                        if right_cost is not None:
                            options.append((right_cost, right_index))
                if not options:
                    break
                _, selected_index = min(options, key=lambda item: (item[0], valley_slots[item[1]]["start"]))
                valley_indexes.add(selected_index)
                if selected_index == left_index:
                    left_index -= 1
                else:
                    right_index += 1

            valley_starts = {
                cast(datetime, valley_slots[index]["start"])
                for index in valley_indexes
            }
            primary_candidates = [
                candidate for candidate in profitable_primary_candidates
                if cast(datetime, candidate["start"]) in valley_starts
            ]
            charged_kwh = _append_charge_candidates(
                selected_for_cycle,
                sorted(
                    primary_candidates,
                    key=lambda item: (float(item["cost"]), 0 if item["kind"] == "solar" else 1,
                                      abs((item["start"] - best_candidate["start"]).total_seconds()),
                                      item["start"]),
                ),
                target_kwh=target_kwh,
                charged_kwh=charged_kwh,
            )

            active_charge_cycle_needs_topup = (
                getattr(self, "_charge_session_started", False)
                and self._active_charge_phase_end is not None
                and self._active_charge_phase_end > now
                and cursor <= now
            )
            projected_usable_soc_percent = (
                (projected_usable_at_charge_kwh / usable_capacity_kwh) * 100.0
                if usable_capacity_kwh > 0
                else 0.0
            )
            regular_grid_topup_allowed = (
                # Match primary eligibility: the physical simulation also
                # considers export before a refill, not just home depletion.
                (self._cycle_export_enabled or first_grid_charge_start is not None)
                and projected_usable_soc_percent < 95.0
            )
            if charged_kwh < target_kwh and (regular_grid_topup_allowed or active_charge_cycle_needs_topup):
                valley_block_slots = [
                    slot for slot in valley_slots
                    if cast(datetime, slot["start"]) in valley_starts
                ]
                valley_block_start = min(cast(datetime, slot["start"]) for slot in valley_block_slots)
                valley_block_end = max(cast(datetime, slot["end"]) for slot in valley_block_slots)
                grid_candidates = [
                    candidate for candidate in cycle_candidates
                    if candidate["kind"] == "grid" and float(candidate["cost"]) >= 0
                    and _is_profitable_charge(candidate)
                    and (self._cycle_export_enabled or (
                        first_grid_charge_start is not None
                        and max(now, cast(datetime, candidate["start"])) >= first_grid_charge_start
                    ))
                    and cast(datetime, candidate["start"]) < valley_block_end
                ]
                used_grid_starts = {
                    cast(datetime, selected["start"])
                    for selected in selected_for_cycle if selected["kind"] == "grid"
                }
                grid_anchor = min(grid_candidates, key=lambda item: (item["cost"], item["start"]))["start"] if grid_candidates else now
                charged_kwh = _append_charge_candidates(
                    selected_for_cycle,
                    sorted(grid_candidates, key=lambda item: (
                        float(item["cost"]),
                        abs((item["start"] - grid_anchor).total_seconds()),
                        item["start"],
                    )),
                    target_kwh=target_kwh,
                    charged_kwh=charged_kwh,
                    used_starts=used_grid_starts,
                )

            if charged_kwh < target_kwh and active_charge_cycle_needs_topup:
                grid_candidates = [
                    candidate for candidate in cycle_candidates
                    if candidate["kind"] == "grid"
                    and float(candidate["cost"]) >= 0
                    and _is_profitable_charge(candidate)
                    and cast(datetime, candidate["start"]) >= cursor
                ]
                used_grid_starts = {
                    cast(datetime, selected["start"])
                    for selected in selected_for_cycle
                    if selected["kind"] == "grid"
                }
                charged_kwh = _append_charge_candidates(
                    selected_for_cycle,
                    sorted(grid_candidates, key=lambda item: (float(item["cost"]), item["start"])),
                    target_kwh=target_kwh,
                    charged_kwh=charged_kwh,
                    used_starts=used_grid_starts,
                )

            if charged_kwh < target_kwh:
                # A costly gap can split a price valley without invalidating
                # cheaper, profitable slots later in the same opportunity.
                charged_kwh = _append_charge_candidates(
                    selected_for_cycle, profitable_primary_candidates,
                    target_kwh=target_kwh, charged_kwh=charged_kwh,
                )

            if not selected_for_cycle:
                break

            # Price-ranked selection can move the actual refill beyond the
            # anchor (including tomorrow). Re-size at that physical start,
            # rather than carrying today's partial-SOC top-up into a full refill.
            selected_start = min(item["start"] for item in selected_for_cycle)
            if (getattr(self, "_battery_cycle_discharge_floor_kwh", None) == 0.0
                    and selected_start > best_candidate["start"]
                    and not getattr(self, "_charge_session_started", False)
                    and _project_usable_energy_until(selected_start) < projected_usable_at_charge_kwh - 0.005):
                cursor = selected_start
                continue

            # Require enough profitable later demand for the full new import
            # supplement, after accounting for the already stored energy.
            grid_selected = [item for item in selected_for_cycle if item["kind"] == "grid"]
            if grid_selected:
                charge_end = max(cast(datetime, item["end"]) for item in selected_for_cycle)
                minimum_sale_price = max(float(item["profit_cost"]) for item in grid_selected) + battery_min_profit
                # The price anchor may be on an earlier day than the slots
                # actually selected. Assess existing supply at the selected
                # refill, after the intervening household consumption.
                selected_charge_start = min(cast(datetime, item["start"]) for item in selected_for_cycle)
                existing_energy = _project_usable_energy_until(selected_charge_start, include_export=True)
                historical_cost = admission_acquisition_cost
                if existing_energy > 0.01 and historical_cost is not None:
                    minimum_sale_price = max(minimum_sale_price, historical_cost + battery_min_profit)
                cheaper_refill = min((cast(datetime, item["start"])
                    for item in _slot_candidates(charge_end)
                    if item["start"] >= charge_end + min_discharge_gap
                    and float(item["cost"]) < min(float(g["cost"]) for g in grid_selected) - .005
                    and float(item["charge_kwh"]) > .05 and _is_profitable_charge(item)), default=None)
                if bridge is not None:
                    cheaper_refill = refill_start
                mixed_fill = any(item["kind"] == "solar" for item in selected_for_cycle) or any(
                    float(slot.get("solar_kwh", 0.0)) > .01 for slot in future_slots
                    if any(item["start"] == slot["start"] for item in grid_selected))
                if cheaper_refill is not None and not mixed_fill:
                    capacity = _profitable_discharge_capacity(charge_end, cheaper_refill,
                                                             minimum_sale_price - battery_min_profit)
                    if capacity + 1e-6 < usable_capacity_kwh and not (active_grid_cycle and cursor <= now):
                        cursor = cheaper_refill
                        continue
                    self._full_arbitrage_intervals.append((charge_end, cheaper_refill,
                                                           minimum_sale_price - battery_min_profit))
                solar_selected = [item for item in selected_for_cycle if item["kind"] == "solar"]
                # A running charge latch is not economic admission. Already
                # stored energy still consumes the same future outlets as new
                # imports; only an executable remaining supplement may continue.
                required_grid_kwh = max(0.0, target_kwh - sum(
                    float(item["charge_kwh"]) for item in solar_selected
                ))
                # A grid command includes simultaneous PV. Only its import
                # supplement has to earn the import-price margin.
                # Reserve stock and selected PV are not profit on new imports.
                # They compete for the same physical future demand.
                selected_solar_kwh = _selected_solar_energy(selected_for_cycle)
                profit = grid_supplement_profit(
                    selected=selected_for_cycle, slots=future_slots, after=charge_end,
                    before=cheaper_refill or horizon_end, max_charge_kw=max_charge_kw,
                    max_discharge_kw=max_discharge_kw, minimum_profit=battery_min_profit,
                    existing_energy=existing_energy + selected_solar_kwh, allow_export=self._cycle_export_enabled,
                    historical_cost=historical_cost,
                    discharge_budget_kwh=max(0.0, usable_capacity_kwh - no_charge_reserve_kwh),
                    retained_reserve_kwh=no_charge_reserve_kwh,
                )
                # A cheap late top-up can move completion past the valuable
                # discharge hours. Retry the same charging opportunity without
                # its last slot before rejecting the whole fill. Each retry
                # strictly shortens the selection; all profit, full-capacity and
                # physical checks below still apply to the rebuilt cycle.
                cheapest_import = min(float(item["profit_cost"]) for item in cycle_candidates
                                      if item["kind"] == "grid")
                retry_has_margin = any(
                    slot["end"] > best_candidate["end"]
                    and max(float(slot["import_price"]),
                            float(slot["export_price"]) if self._cycle_export_enabled else float("-inf"))
                        + 1e-9 >= cheapest_import + battery_min_profit
                    for slot in future_slots)
                # An expired solar phase (or restored latch without a live
                # phase) must not veto an earlier economic completion when the
                # cheapest import falls after useful demand. Keep the running
                # solar phase's existing selection while its window is live;
                # retries there can truncate its remaining committed PV fill.
                if (not profit["profitable"] and retry_has_margin
                        and not (cursor <= now and getattr(self, "_charge_session_started", False)
                                 and current_usable_kwh > _BATTERY_DEPLETION_EPSILON_KWH
                                 and self._active_charge_phase_mode != "laden_van_net"
                                 and self._active_charge_phase_end is not None
                                 and self._active_charge_phase_end > now)
                        and not all(float(item["profit_cost"]) < 0 for item in grid_selected)
                        and bridge is None):
                    earlier_end = max(item["start"] for item in selected_for_cycle)
                    if best_candidate["end"] <= earlier_end < selection_deadline:
                        retry_charge_deadlines[cursor] = earlier_end
                        continue
                # A small profitable peak cannot justify buying a full cycle.
                # The daily-mean forecast may support the economic estimate;
                # starting a new charge still requires known purchase prices.
                # A grid-mode command can be entirely supplied by PV. There
                # is then no import margin to prove; actual imports, including
                # negative-price purchases, still require executable profit.
                if (profit["net_kwh"] <= 1e-9
                        and all(float(item["profit_cost"]) < 0 for item in grid_selected)):
                    grid_budget = required_grid_kwh
                elif profit["average_profit"] + 1e-9 >= battery_min_profit:
                    # Retained reserve is stock at acquisition cost, not a sale
                    # or invented energy. Its actual purchase may accompany a
                    # useful profitable partial discharge. Do not trim it away
                    # and then replace cheap imports with dearer solar.
                    reserve_deficit = max(0.0, no_charge_reserve_kwh
                                          - existing_energy - selected_solar_kwh)
                    net_fraction = profit["net_kwh"] / max(sum(
                        float(item["charge_kwh"]) for item in grid_selected), 1e-9)
                    grid_budget = min(required_grid_kwh,
                        (profit["covered_kwh"] + reserve_deficit) / max(net_fraction, 1e-9))
                else:
                    grid_budget = 0.0
                # No discharge is executable until full charge. Reject new
                # partial grid fills, preserving any usable solar alternative.
                if (sum(float(item["charge_kwh"])
                        for item in solar_selected) + min(grid_budget, sum(
                            float(item["charge_kwh"]) for item in grid_selected))
                        + 0.005 < target_kwh):
                    grid_budget = 0.0
                if grid_budget <= 1e-9:
                    # Rejecting the import supplement must not discard its
                    # profitable solar contribution as well. Restore the
                    # solar-only command for those mixed slots.
                    for item in grid_selected:
                        solar_alternative = next((candidate for candidate in primary_candidates
                            if candidate["kind"] == "solar"
                            and candidate["start"] == item["start"]), None)
                        if solar_alternative is not None:
                            solar_selected.append({**solar_alternative, "charge_kwh": min(
                                float(solar_alternative["charge_kwh"]), float(item["charge_kwh"]),
                            )})
                # Equal-price slots nearest the cheapest slot avoid unnecessary
                # gaps without buying more energy or accepting a higher price.
                anchor = min(grid_selected, key=lambda item: (float(item["cost"]), item["start"]))["start"]
                trimmed_grid = []
                for item in sorted(grid_selected, key=lambda item: (
                    float(item["cost"]), abs((item["start"] - anchor).total_seconds()), item["start"],
                )):
                    take = min(float(item["charge_kwh"]), grid_budget)
                    if take > 1e-9:
                        trimmed_grid.append({**item, "charge_kwh": round(take, 6)})
                        grid_budget -= take
                selected_for_cycle = solar_selected + trimmed_grid
                if not selected_for_cycle:
                    cursor = charge_end
                    cycle_index += 1
                    continue


            # Consolidate energy within chronological, profitable same-source
            # runs whose TOTAL price spread is at most one cent. Do not bridge
            # unavailable slots, a source change, or a larger price excursion.
            selection_before_bundling = list(selected_for_cycle)
            for kind in ("solar", "grid"):
                same_source = [item for item in selected_for_cycle if item["kind"] == kind]
                if not same_source:
                    continue
                first_start = min(item["start"] for item in same_source)
                if (cursor <= now and getattr(self, "_charge_session_started", False)
                        and self._active_charge_phase_end is not None
                        and self._active_charge_phase_end > now
                        and self._active_charge_phase_mode == (
                            "laden_van_net" if kind == "grid" else "laden_met_zonne_energie")):
                    first_start = now
                last_end = max(item["end"] for item in same_source)
                candidates = sorted((item for item in primary_candidates
                    if item["kind"] == kind and item["end"] > first_start
                    and item["start"] < last_end
                    and not any(other["start"] == item["start"] and other["kind"] != kind
                                for other in selected_for_cycle)), key=lambda item: item["start"])
                amounts = bundle_slot_allocations(
                    [{**item, "price": item["cost"],
                      "capacity_kwh": item["charge_kwh"]} for item in candidates],
                    {item["start"]: float(item["charge_kwh"]) for item in same_source},
                )
                candidate_starts = {item["start"] for item in candidates}
                selected_for_cycle = [item for item in selected_for_cycle
                    if item["kind"] != kind or item["start"] not in candidate_starts]
                selected_for_cycle.extend({**item, "charge_kwh": round(amounts[item["start"]], 6)}
                    for item in candidates if amounts.get(item["start"], 0.) > 1e-9)

            # A complete extra arbitrage cycle has a stronger requirement than
            # an individually profitable slot: all usable energy must still be
            # sellable at its new maximum import cost before the next refill.
            bundled_grid = [item for item in selected_for_cycle if item["kind"] == "grid"]
            if bundled_grid:
                def _grid_selection_is_admissible(selection):
                    """Validate the complete candidate, including any fallback."""
                    candidate_profit = grid_supplement_profit(
                        selected=selection, slots=future_slots,
                        after=max(item["end"] for item in selection),
                        before=cheaper_refill or horizon_end, max_charge_kw=max_charge_kw,
                        max_discharge_kw=max_discharge_kw, minimum_profit=battery_min_profit,
                        existing_energy=_project_usable_energy_until(
                            min(item["start"] for item in selection), include_export=True
                        ) + _selected_solar_energy(selection),
                        allow_export=self._cycle_export_enabled,
                        historical_cost=historical_cost,
                        discharge_budget_kwh=max(0.0, usable_capacity_kwh - (
                            no_charge_reserve_kwh if cheaper_refill is None else 0.0)),
                        retained_reserve_kwh=no_charge_reserve_kwh if cheaper_refill is None else 0.0,
                    )
                    # A PV-only grid command makes no purchase. Real imports
                    # require both a complete fill and executable outlets,
                    # regardless of a persisted/observed running-charge latch.
                    return candidate_profit["net_kwh"] <= 1e-9 or (
                        candidate_profit["profitable"]
                        and sum(float(item["charge_kwh"]) for item in selection) + .005 >= target_kwh
                    )

                if not _grid_selection_is_admissible(selected_for_cycle):
                    if _grid_selection_is_admissible(selection_before_bundling):
                        selected_for_cycle = selection_before_bundling
                    else:
                        selected_for_cycle = [item for item in selection_before_bundling
                                              if item["kind"] == "solar"]
                        for item in selection_before_bundling:
                            if item["kind"] != "grid":
                                continue
                            alternative = next((candidate for candidate in primary_candidates
                                if candidate["kind"] == "solar"
                                and candidate["start"] == item["start"]), None)
                            if alternative is not None:
                                selected_for_cycle.append({**alternative, "charge_kwh": min(
                                    float(alternative["charge_kwh"]), float(item["charge_kwh"]))})
                    bundled_grid = [item for item in selected_for_cycle if item["kind"] == "grid"]
            if grid_selected and bundled_grid and cheaper_refill is not None and not mixed_fill:
                bundled_cost = max(float(item["profit_cost"]) for item in selected_for_cycle
                                   if item["kind"] == "grid")
                bundled_end = max(item["end"] for item in selected_for_cycle)
                if _profitable_discharge_capacity(bundled_end, cheaper_refill,
                                                  bundled_cost) + 1e-6 < usable_capacity_kwh:
                    selected_for_cycle = (selection_before_bundling
                        if _grid_selection_is_admissible(selection_before_bundling)
                        else [item for item in selection_before_bundling if item["kind"] == "solar"])
                else:
                    self._full_arbitrage_intervals = [
                        (period[0], period[1], max(period[2], bundled_cost))
                        if period[0] == charge_end and period[1] == cheaper_refill else period
                        for period in self._full_arbitrage_intervals]

            if not selected_for_cycle:
                # Sub-millisecond clipped remainders may round to zero kWh.
                cursor = charge_end
                cycle_index += 1
                continue
            selected_start = min(item["start"] for item in selected_for_cycle)
            if (previous_charge_end is not None and previous_charge_cost is not None
                    and previous_charge_shortfall_kwh <= .05
                    and _profitable_discharge_capacity(previous_charge_end, selected_start,
                                                       previous_charge_cost) + 1e-6 >= usable_capacity_kwh):
                self._full_arbitrage_intervals.append((previous_charge_end, selected_start, previous_charge_cost))
            previous_charge_cost = max(float(item.get("profit_cost", item["cost"]))
                                       for item in selected_for_cycle)
            for selected in selected_for_cycle:
                selected_start = cast(datetime, selected["start"])
                selected_kwh = float(selected["charge_kwh"])
                if selected["kind"] == "solar":
                    selected_solar_charge_by_start[selected_start] = round(
                        selected_solar_charge_by_start.get(selected_start, 0.0) + selected_kwh,
                        6,
                    )
                else:
                    selected_grid_charge_by_start[selected_start] = round(
                        selected_grid_charge_by_start.get(selected_start, 0.0) + selected_kwh,
                        6,
                    )

            cycle_end = max(cast(datetime, selected["end"]) for selected in selected_for_cycle)
            previous_charge_shortfall_kwh = max(0.0, target_kwh - sum(
                float(selected["charge_kwh"]) for selected in selected_for_cycle
            ))
            previous_charge_end = cycle_end
            cursor = cycle_end + min_discharge_gap
            cycle_index += 1

        selected_solar_starts = {
            start
            for start, charge_kwh in selected_solar_charge_by_start.items()
            if charge_kwh > 0
        }
        for slot in future_slots:
            slot_start = cast(datetime, slot["start"])
            slot_end = cast(datetime, slot["end"])
            charge_kwh = float(selected_solar_charge_by_start.get(slot_start, 0.0))
            if charge_kwh > 0:
                active_start = max(slot_start, now) if slot_start <= now < slot_end else slot_start
                slot_hours = max((slot_end - active_start).total_seconds() / 3600, 0.0)
                full_slot_hours = max(float(slot["hours"]), 1e-9)
                active_fraction = min(1.0, slot_hours / full_slot_hours)
                slot_solar_charge_kwh = min(
                    max_charge_kw * slot_hours,
                    chargeable_solar_kwh(slot) * active_fraction,
                )
                usable_hours = slot_hours
                charge_start = active_start
                charge_end = slot_end
                if 0 < charge_kwh < slot_solar_charge_kwh - 1e-6:
                    usable_hours = slot_hours * (charge_kwh / slot_solar_charge_kwh)
                    if slot_end in selected_solar_starts:
                        charge_start = slot_end - timedelta(hours=usable_hours)
                    charge_end = charge_start + timedelta(hours=usable_hours)
                if charge_safety_margin > 0 and usable_hours > 0:
                    buffered_hours = min(slot_hours, usable_hours * (1.0 + charge_safety_margin))
                    charge_start = max(active_start, charge_end - timedelta(hours=buffered_hours))
                    usable_hours = max((charge_end - charge_start).total_seconds() / 3600, 0.0)
                planned_solar_charge_windows.append(
                    {
                        "start": charge_start.isoformat(),
                        "end": charge_end.isoformat(),
                        "price": round(_solar_cost(slot), 6),
                        "usable_hours": round(usable_hours, 3),
                        "charge_kwh": round(charge_kwh, 6),
                    }
                )
            slot_charge_kwh = float(selected_grid_charge_by_start.get(slot_start, 0.0))
            if slot_charge_kwh > 0:
                active_start = max(slot_start, now)
                usable_hours = min((slot_end - active_start).total_seconds() / 3600, slot_charge_kwh / max_charge_kw)
                if slot_start > now and slot_end in selected_grid_charge_by_start:
                    # Align future fragments with the next slot. In the live
                    # slot execute immediately; rounded SOC must not shift a
                    # running charge a fraction of a second into the future.
                    active_start = max(active_start, slot_end - timedelta(hours=usable_hours))
                    if abs((active_start - now).total_seconds()) < 0.001:
                        active_start = now
                planned_grid_charge_windows.append(
                    {
                        "start": active_start.isoformat(),
                        "end": (active_start + timedelta(hours=usable_hours)).isoformat(),
                        "price": round(float(slot["import_price"]), 6),
                        "usable_hours": round(usable_hours, 3),
                        "charge_kwh": round(slot_charge_kwh, 6),
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
        cluster_gap = _MIN_CHARGE_WINDOW_GAP
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
        clusters: list[dict[str, datetime]] = []
        for window in normalized_windows:
            if not clusters or window["start"] >= clusters[-1]["end"] + cluster_gap:
                clusters.append(dict(window))
                continue
            clusters[-1]["end"] = max(clusters[-1]["end"], window["end"])

        # Recomputed commands determine the phase bounds. Re-injecting the old
        # end as a fictitious charge window perpetuates it on every update, even
        # after a shorter fill has reached full. The separate cycle latch still
        # protects real pauses between the selected charging commands.
        active_cluster = next((cluster for cluster in clusters
                               if cluster["start"] <= now < cluster["end"]), None)
        if active_charge_phase_end is not None:
            self._active_charge_phase_end = active_cluster["end"] if active_cluster else None
            if active_cluster is None:
                self._active_charge_phase_mode = "accu_uit"
                active_charge_phase_mode = "accu_uit"

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
        slots: list[dict[str, Any]] | None = None,
    ) -> tuple[str, float, datetime]:
        charge_start = cast(datetime, charge_window.get("start", slot["start"]))
        charge_end = cast(datetime, charge_window["end"])
        mode_start = mode_start or charge_start
        usable_hours = max((charge_end - mode_start).total_seconds() / 3600, 0.0)
        charge_kwh = min(
            float(charge_window.get("charge_kwh", 0.0)),
            max(0.0, usable_hours * max_charge_kw),
            max(0.0, usable_capacity_kwh - sim_usable_energy_kwh),
        )
        if slots is not None and charge_kwh > 0:
            # A later cycle may start with energy left over. Integrate actual
            # PV surplus so it stops at capacity rather than assuming depletion.
            stored = 0.0
            for forecast in slots:
                start = max(mode_start, forecast["start"])
                end = min(charge_end, forecast["end"])
                if end <= start:
                    continue
                rate = max_charge_kw if mode == "laden_van_net" else min(
                    max_charge_kw, chargeable_solar_kwh(forecast) / max(float(forecast["hours"]), 1e-9),
                )
                amount = rate * (end - start).total_seconds() / 3600
                if rate > 0 and stored + amount >= charge_kwh - 1e-9:
                    charge_end = start + timedelta(hours=max(0.0, charge_kwh - stored) / rate)
                    stored = charge_kwh
                    break
                stored += amount
            charge_kwh = min(charge_kwh, stored)
            usable_hours = max(0.0, (charge_end - mode_start).total_seconds() / 3600)
        if charge_kwh <= 1e-9:
            mode = "accu_uit"
        if mode == "laden_van_net" and max_charge_kw > 0 and 0 < charge_kwh < usable_hours * max_charge_kw:
            charge_end = min(charge_end, mode_start + timedelta(hours=charge_kwh / max_charge_kw))
            usable_hours = (charge_end - mode_start).total_seconds() / 3600
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

    def _estimate_forced_battery_depletion_time(
        self,
        *,
        now: datetime,
        initial_usable_energy_kwh: float,
        max_discharge_kw: float,
    ) -> datetime | None:
        if initial_usable_energy_kwh <= 0:
            return now
        if max_discharge_kw <= 0:
            return None
        return now + timedelta(hours=initial_usable_energy_kwh / max_discharge_kw)

    def _can_discharge_in_interval(self, *, slots, start, end, cost, minimum_profit):
        """Whether forecast demand or profitable export can use stored energy."""
        return any(
            slot["end"] > start and slot["start"] < end and (
                float(slot["net_solar_kwh"]) < -1e-9
                or (self._cycle_export_enabled and slot.get("price_known", True)
                    and float(slot["export_price"]) > 0
                    and (cost is None or float(slot["export_price"]) + 1e-9 >= cost + minimum_profit))
            ) for slot in slots
        )

    def _has_profitable_discharge(self, *, slots, start, end, cost, minimum_profit):
        """Distinguish a tariff margin from home-only use above the reserve."""
        return any(
            slot["end"] > start and slot["start"] < end
            and slot.get("price_known", True) and (
                (float(slot.get("net_solar_kwh", 0.0)) < -1e-9
                 and float(slot["import_price"]) + 1e-9 >= (cost or 0.0) + minimum_profit)
                or (self._cycle_export_enabled and float(slot["export_price"]) > 0
                    and float(slot["export_price"]) + 1e-9 >= (cost or 0.0) + minimum_profit)
            ) for slot in slots
        )

    def _estimate_battery_depletion_time(
        self,
        *,
        slots: list[dict[str, Any]],
        now: datetime,
        initial_usable_energy_kwh: float,
        max_discharge_kw: float,
        floor_kwh: float = _BATTERY_DEPLETION_EPSILON_KWH,
    ) -> datetime | None:
        floor_kwh = max(0.0, floor_kwh)
        if initial_usable_energy_kwh <= floor_kwh:
            return now
        if max_discharge_kw <= 0:
            return None

        usable_energy_kwh = initial_usable_energy_kwh
        for slot in sorted(slots, key=lambda item: item["start"]):
            slot_start = cast(datetime, slot["start"])
            slot_end = cast(datetime, slot["end"])
            if slot_end <= now:
                continue
            active_start = max(slot_start, now)
            active_hours = max((slot_end - active_start).total_seconds() / 3600, 0.0)
            if active_hours <= 0:
                continue
            slot_hours = max(float(slot.get("hours", active_hours)), 1e-9)
            net_battery_demand_kwh = max(0.0, -float(slot.get("net_solar_kwh", 0.0)))
            active_demand_kwh = net_battery_demand_kwh * min(1.0, active_hours / slot_hours)
            discharge_kwh = min(max_discharge_kw * active_hours, active_demand_kwh)
            if discharge_kwh <= 0:
                continue
            if usable_energy_kwh - discharge_kwh <= floor_kwh:
                needed_kwh = usable_energy_kwh - floor_kwh
                discharge_rate_kw = discharge_kwh / active_hours
                if discharge_rate_kw <= 0:
                    return active_start
                return active_start + timedelta(hours=needed_kwh / discharge_rate_kw)
            usable_energy_kwh -= discharge_kwh

        return None

    def _select_cycle_discharge_floor(self, **plan):
        """Commit one discharge boundary, preferring a feasible full cycle.

        Candidate simulations are isolated. A live boundary is retained across
        pauses, changing forecasts and restarts, until the measured SoC reaches
        it. Forecasted completion never advances the live phase.
        """
        initial = max(0.0, plan["initial_usable_energy_kwh"])
        capacity = plan["usable_capacity_kwh"]
        reserve = max(0.0, min(capacity, plan.get("no_charge_reserve_kwh", 0.0)))
        boundary = getattr(self, "_battery_cycle_discharge_floor_kwh", None)
        if getattr(self, "_charge_session_started", False):
            # This fill starts new stock; the old discharge permission ended.
            boundary = reserve
        if boundary is not None:
            boundary = reserve if boundary > 0.005 else 0.0
            if (getattr(self, "_discharge_session_started", False)
                    and initial <= boundary + 0.005):
                # The measured discharge is finished, not a new fill started.
                # Keep new purchases subject to normal economic admission.
                self._discharge_session_started = False
                self._charge_session_started = False
                boundary = None

        if (boundary is not None and boundary > 0.005
                and initial < capacity - 0.005
                and getattr(self, '_battery_charge_source', None) == 'laden_met_zonne_energie'
                and not getattr(self, '_charge_session_started', False)):
            # A committed reserve survives forecast refreshes. Only measured
            # full (or full integrated execution in an isolated successor) may
            # authorize the lower full-cycle boundary.
            return reserve
        if boundary is None or boundary > 0.005:
            trial = copy(self)
            trial._store_battery_cycle_state_snapshot = lambda timestamp: None
            trial._battery_cycle_discharge_floor_kwh = 0.0
            # Admission must prove a profitable complete cycle. The loss waiver
            # belongs only to completion of an already committed full cycle.
            solar_relaxation = getattr(self, '_battery_solar_reserve_relaxation', False)
            # The solar exception already disables loss completion. Use live
            # execution rules here so export cannot interrupt a proven refill.
            trial._battery_require_profitable_cycle = not solar_relaxation
            full_windows, _ = trial._simulate_bound_mode_windows(**plan)
            energy = initial
            emptied_before_refill = initial <= 0.005
            completes_new_refill = False
            cutoff = self._two_cycle_end(full_windows) if solar_relaxation else None
            refill_until = None
            for action in execution_actions(
                    full_windows, plan["slots"], plan["max_charge_kw"], plan["max_discharge_kw"]):
                action_start = datetime.fromisoformat(action["start"])
                action_end = datetime.fromisoformat(action["end"])
                hours = (action_end - action_start).total_seconds() / 3600
                rate = (action["solar_kw"] + action["grid_kw"] if action["mode"].startswith("laden")
                        else -action["power"] if action["mode"].startswith("ontladen") else 0.0)
                # The reserve waiver needs a single executable PV refill, not
                # pieces that happen to add up across an idle or discharge gap.
                if solar_relaxation:
                    if (cutoff is None or action_end > cutoff or action['grid_kw'] > 0
                            or (rate > 0 and (not emptied_before_refill or
                                (refill_until is not None and action_start > refill_until)))):
                        break
                    if rate > 0:
                        refill_until = action_end
                energy = min(capacity, max(0.0, energy + rate * hours))
                emptied_before_refill = emptied_before_refill or energy <= 0.005
                if rate > 0 and emptied_before_refill and energy >= capacity - 0.005:
                    completes_new_refill = True
                    break
            # Preserve existing admission for an unbound, demonstrably
            # profitable full cycle. Do not use the sunk-stock waiver to prove
            # that legacy permission, and never migrate a partial discharge.
            completes_legacy_cycle = False
            if (not solar_relaxation and not completes_new_refill and boundary is None
                    and not (getattr(self, "_discharge_session_started", False)
                             and initial < capacity - 0.005)):
                trial._battery_rank_sunk_stock = False
                legacy_windows, _ = trial._simulate_bound_mode_windows(**plan)
                energy = initial
                reached_full = initial >= capacity - 0.005
                for action in execution_actions(
                        legacy_windows, plan["slots"], plan["max_charge_kw"], plan["max_discharge_kw"]):
                    hours = (datetime.fromisoformat(action["end"]) - datetime.fromisoformat(action["start"])).total_seconds() / 3600
                    rate = (action["solar_kw"] + action["grid_kw"] if action["mode"].startswith("laden")
                            else -action["power"] if action["mode"].startswith("ontladen") else 0.0)
                    energy = min(capacity, max(0.0, energy + rate * hours))
                    reached_full = reached_full or energy >= capacity - 0.005
                    if rate < 0 and energy <= 0.005 and reached_full:
                        completes_legacy_cycle = True
                        break
            # A saved fallback is released only by a physically complete refill.
            boundary = 0.0 if completes_new_refill or completes_legacy_cycle else reserve
        return boundary

    def _build_mode_windows_from_hourly_plan(self, **plan):
        self._release_solar_charge_hold(plan.get('no_charge_reserve_kwh', 0.0))
        self._battery_cycle_discharge_floor_kwh = self._select_cycle_discharge_floor(**plan)
        return self._simulate_bound_mode_windows(**plan)

    def _simulate_bound_mode_windows(
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
        battery_soc_percent: float | None = None,
        no_charge_reserve_kwh: float = 0.0,
        battery_min_profit: float = 0.08,
    ) -> tuple[list[dict[str, str | float]], str]:
        # Only remaining demand belongs to the current decision. Allocating a
        # whole quarter at 21:58 would spend energy for minutes already passed
        # and leave unexpected energy in the battery before the next recharge.
        remaining_slots = []
        for slot in slots:
            if slot["end"] <= now:
                continue
            active_start = max(now, slot["start"])
            active_hours = (slot["end"] - active_start).total_seconds() / 3600
            fraction = active_hours / max(float(slot["hours"]), 1e-9)
            remaining_slots.append({
                **slot, "start": active_start, "hours": active_hours,
                **{key: float(slot[key]) * fraction for key in ("net_solar_kwh", "solar_kwh", "demand_kwh")
                   if key in slot},
            })
        slots = remaining_slots
        solar_charge_starts = build_charge_window_lookup(
            planned_solar_charge_windows,
            max_charge_kw=max_charge_kw,
        )
        grid_charge_starts = build_charge_window_lookup(
            planned_grid_charge_windows,
            max_charge_kw=max_charge_kw,
            trim_to_charge_amount=True,
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
        cycle_end = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        battery_is_full = (
            usable_capacity_kwh > 0
            and initial_usable_energy_kwh >= usable_capacity_kwh - 0.005
        )
        if battery_is_full:
            self._charge_session_started = False
            self._discharge_session_started = True
            solar_charge_starts = {
                start: window
                for start, window in solar_charge_starts.items()
                if not (start <= now < cast(datetime, window["end"]))
            }
            grid_charge_starts = {
                start: window
                for start, window in grid_charge_starts.items()
                if not (start <= now < cast(datetime, window["end"]))
            }
            charge_starts = {
                **solar_charge_starts,
                **grid_charge_starts,
            }
            charge_windows = [
                window
                for window in charge_windows
                if not (cast(datetime, window["start"]) <= now < cast(datetime, window["end"]))
            ]
            self._active_charge_phase_end = None
            self._active_charge_phase_mode = "accu_uit"

        charge_session_started = getattr(self, "_charge_session_started", False)
        discharge_session_started = self._discharge_session_started
        cycle_floor = max(0.0, min(usable_capacity_kwh,
            float(getattr(self, "_battery_cycle_discharge_floor_kwh", no_charge_reserve_kwh) or 0.0)))
        cycle_state_initialized = getattr(self, "_battery_cycle_state_initialized", False)
        # Only the portion below the configured higher reserve may complete
        # at a loss. A reserve cycle itself retains normal profit protection.
        loss_completion_enabled = (cycle_floor < no_charge_reserve_kwh
            and not getattr(self, "_battery_solar_reserve_relaxation", False)
            and not getattr(self, "_battery_require_profitable_cycle", False))
        usable_energy_depleted = initial_usable_energy_kwh <= 0.005
        restored_discharge_latch = (
            getattr(self, "_battery_cycle_state_restored_recent", False)
            and discharge_session_started
        )
        if cycle_state_initialized and not battery_is_full and usable_energy_depleted and not restored_discharge_latch:
            # Empty stock releases discharge, but does not start a fill.
            # Preserve a genuine accepted fill even if SOC has not risen yet;
            # otherwise only an admitted active window may set its latch.
            discharge_session_started = False
            self._discharge_session_started = False

        if charge_session_started:
            discharge_session_started = False
            self._discharge_session_started = False
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
        active_charge_phase_end = active_charge_phase["end"] if active_charge_phase is not None else None
        sim_usable_energy_kwh = max(0.0, initial_usable_energy_kwh)
        standby_authorized_kwh = 0.0
        accounted_standby_tails: set[datetime] = set()
        grid_cost_floor = getattr(self, "_battery_grid_charge_price", None)
        if initial_usable_energy_kwh <= 0.01:
            grid_cost_floor = None
            self._battery_grid_charge_price = None
        margin_stock = observed_stock(getattr(self, "_battery_margin_state", None),
            now, initial_usable_energy_kwh, 0.0, battery_min_profit)
        if grid_cost_floor is not None and not getattr(self, "_battery_margin_state", None):
            # A known historical import price must not become free stock when
            # upgrading from a version without persisted margin lots.
            margin_stock.lots = [[quantity, grid_cost_floor if cost is None else cost]
                                 for quantity, cost in margin_stock.lots]
        def discharge_profitable(slot, price_key, minimum_profit=battery_min_profit):
            # Protect every remaining acquisition lot, also for avoided imports.
            # Future revenue cannot waive this per-slot floor; tariffs are net.
            costs = [float(cost) for quantity, cost in margin_stock.lots
                     if quantity > 1e-9 and cost is not None]
            if grid_cost_floor is not None:
                costs.append(grid_cost_floor)
            return ((price_key == "import_price" or slot.get("price_known", True))
                    and (price_key != "export_price" or float(slot[price_key]) > 0)
                    and (not costs or float(slot[price_key]) + 1e-9
                         >= max(costs) + minimum_profit))

        def export_profitable(slot):
            # Export necessarily supplies any simultaneous household deficit.
            return (discharge_profitable(slot, "export_price")
                    and (float(slot["net_solar_kwh"]) >= -1e-9
                         or discharge_profitable(slot, "import_price")))

        def relaxed_solar_home(slot):
            return (getattr(self, '_battery_solar_reserve_relaxation', False)
                    and first_charge_phase_start is not None
                    and slot.get('start', first_charge_phase_start) < first_charge_phase_start
                    and slot.get('price_known', True)
                    and not discharge_profitable(slot, 'import_price')
                    and discharge_profitable(slot, 'import_price', 0.0))

        def home_profitable(slot):
            return discharge_profitable(slot, "import_price") or relaxed_solar_home(slot)

        observed_margin_state = dict(at=now.isoformat(), energy=initial_usable_energy_kwh,
            reserve=0.0, lots=[lot[:] for lot in margin_stock.lots],
            credit=margin_stock.credit,
            pending_actions=[action.copy() for action in margin_stock.pending_actions])
        hourly_modes: list[dict[str, str | float]] = []
        current_mode = "accu_uit"
        last_charge_mode = "accu_uit"
        simulated_discharge_session_started = discharge_session_started
        simulated_charge_incomplete = charge_session_started and not battery_is_full
        simulated_fill_seen = False

        # Opportunistic solar may start at the selected floor; unlike grid,
        # independently profitable PV need not reach full before discharge.
        solar_topups = {}
        if (discharge_session_started and not planned_solar_charge_windows
                and not planned_grid_charge_windows):
            for forecast in slots:
                if not forecast.get("price_known", True) or chargeable_solar_kwh(forecast) <= 0:
                    continue
                useful_demand = sum(
                    min(max_discharge_kw * float(later["hours"]),
                        max(0.0, -float(later["net_solar_kwh"])))
                    for later in slots
                    if later["start"] >= forecast["end"] and later.get("price_known", True)
                    and float(later["import_price"]) - float(forecast["export_price"])
                        >= battery_min_profit - 1e-9
                )
                amount = min(chargeable_solar_kwh(forecast),
                             max_charge_kw * float(forecast["hours"]), useful_demand)
                if amount > 1e-6:
                    solar_topups[forecast["start"]] = (amount, useful_demand)

        def profitable_solar_refill(start, energy):
            """Early reversal needs executable NEW PV and unshared outlets.

            Reuse the execution integrator: displayed window kWh alone do not
            prove a full fill. Existing stock competes for later household
            demand; neither that stock nor historical credit funds new PV.
            This grants no grid permission and changes no committed floor.
            """
            if energy >= usable_capacity_kwh - 0.005:
                return False
            first = next((w for w in charge_windows
                if w["start"] <= start < w["end"]), None)
            if first is None or first["mode"] != "laden_met_zonne_energie":
                return False
            projected = energy
            costs = []
            refill_end = None
            for window in sorted(charge_windows, key=lambda w: w["start"]):
                if window["end"] <= start:
                    continue
                if window["mode"] != "laden_met_zonne_energie":
                    break
                begin = max(start, window["start"])
                forecast = next((s for s in slots if s["start"] <= begin < s["end"]), None)
                if forecast is None:
                    return False
                _, projected, end = self._append_charge_window_mode(
                    hourly_modes=[], slot=forecast, charge_window=window,
                    mode="laden_met_zonne_energie", price_key="export_price",
                    now=now, current_mode="accu_uit", mode_start=begin,
                    sim_usable_energy_kwh=projected,
                    usable_capacity_kwh=usable_capacity_kwh,
                    max_charge_kw=max_charge_kw, slots=slots)
                for source in slots:
                    if source["end"] > begin and source["start"] < end and chargeable_solar_kwh(source) > 0:
                        if not source.get("price_known", True):
                            return False
                        costs.append(float(source["export_price"]))
                refill_end = end
                if projected >= usable_capacity_kwh - 0.005:
                    break
            if refill_end is None or not costs or projected <= energy + 1e-6:
                return False
            known_costs = [float(cost) for quantity, cost in margin_stock.lots
                           if quantity > 1e-9 and cost is not None]
            if grid_cost_floor is not None:
                known_costs.append(grid_cost_floor)
            floor = max(costs + known_costs) + battery_min_profit
            next_fill_start = min((w["start"] for w in charge_windows
                if w["start"] >= refill_end and any(
                    s["net_solar_kwh"] < 0 and s["end"] > refill_end
                    and s["start"] < w["start"] for s in slots)), default=None)
            next_fill = next_fill_start or slots[-1]["end"]
            outlets = 0.0
            for outlet in slots:
                if not outlet.get("price_known", True):
                    continue
                hours = max(0., (min(next_fill, outlet["end"])
                                - max(refill_end, outlet["start"])).total_seconds() / 3600)
                power_kwh = max_discharge_kw * hours
                home_kwh = min(power_kwh,
                    max(0., -float(outlet["net_solar_kwh"]))
                    * hours / max(float(outlet["hours"]), 1e-9))
                if float(outlet["import_price"]) + 1e-9 >= floor:
                    outlets += home_kwh
                # Reserve-mode execution can sell surplus at known export
                # tariffs. Share inverter power with home use and respect the
                # executor's imminent-refill veto; never finance both with the
                # same kWh. Strict full-cycle trials retain their narrower
                # export eligibility rather than inventing a fallback outlet.
                export_end = (min(outlet['end'], next_fill_start - timedelta(minutes=30))
                              if next_fill_start is not None else outlet['end'])
                export_hours = max(0., (export_end
                    - max(refill_end, outlet['start'])).total_seconds() / 3600)
                if (self._cycle_export_enabled
                        and cycle_floor >= no_charge_reserve_kwh - 0.005
                        and not getattr(self, '_battery_require_profitable_cycle', False)
                        and float(outlet['export_price']) > 0.
                        and float(outlet['export_price']) + 1e-9 >= floor
                        # A mixed export command supplies the home leg too;
                        # admission must not count power execution will reject.
                        and (float(outlet['net_solar_kwh']) >= -1e-9
                             or float(outlet['import_price']) + 1e-9 >= floor)):
                    outlets += max(0., power_kwh-home_kwh) * min(
                        1., export_hours/max(hours, 1e-9))
            return outlets + 1e-6 >= (projected - energy
                + max(0., energy - cycle_floor))

        slot_index = 0
        while slot_index < len(slots):
            slot = slots[slot_index]
            slot_start = slot["start"]
            slot_end = slot["end"]
            if (simulated_fill_seen and not simulated_charge_incomplete
                    and not getattr(self, "_battery_require_profitable_cycle", False)):
                # Rebind only simulated new stock; never advance the live cycle.
                successor = copy(self)
                successor._store_battery_cycle_state_snapshot = lambda timestamp: None
                successor._battery_cycle_discharge_floor_kwh = no_charge_reserve_kwh
                successor._battery_solar_reserve_relaxation = False
                successor._charge_session_started = False
                successor._discharge_session_started = True
                successor._battery_grid_charge_price = grid_cost_floor
                cycle_floor = successor._select_cycle_discharge_floor(
                    slots=slots[slot_index:], now=slot_start,
                    planned_solar_charge_windows=[w for w in planned_solar_charge_windows
                        if datetime.fromisoformat(str(w["end"])) > slot_start],
                    planned_grid_charge_windows=[w for w in planned_grid_charge_windows
                        if datetime.fromisoformat(str(w["end"])) > slot_start],
                    initial_usable_energy_kwh=sim_usable_energy_kwh,
                    usable_capacity_kwh=usable_capacity_kwh,
                    average_price=average_price, average_export_price=average_export_price,
                    max_charge_kw=max_charge_kw, max_discharge_kw=max_discharge_kw,
                    no_charge_reserve_kwh=no_charge_reserve_kwh,
                    battery_min_profit=battery_min_profit)
                loss_completion_enabled = cycle_floor < no_charge_reserve_kwh
                simulated_fill_seen = False
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
            early_solar_refill = (simulated_discharge_session_started
                and not simulated_charge_incomplete
                and sim_usable_energy_kwh > cycle_floor + 0.005
                and profitable_solar_refill(slot_start, sim_usable_energy_kwh))
            charge_blocked = (simulated_discharge_session_started
                              and sim_usable_energy_kwh > cycle_floor + 0.005
                              and not early_solar_refill)
            if charge_blocked:
                overlapping_charge_window = None
            # A proposed active phase is not evidence of an accepted fill.
            # Change the live latch only after the discharge/refill guard above.
            if (overlapping_charge_window is not None and not battery_is_full
                    and (not discharge_session_started or early_solar_refill
                         or initial_usable_energy_kwh <= no_charge_reserve_kwh + 0.005)
                    and cast(datetime, overlapping_charge_window["start"]) <= now
                    < cast(datetime, overlapping_charge_window["end"])):
                charge_session_started = True
                discharge_session_started = False
                self._charge_session_started = True
                self._discharge_session_started = False
                self._battery_cycle_state_restored_recent = False
            if (
                overlapping_charge_window is not None
                and slot_start not in charge_starts
            ):
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
                    slots=slots,
                )
                last_charge_mode = charge_mode
                charge_stock(margin_stock, slots, max(now, slot_start), charge_end, charge_mode, max_charge_kw)
                simulated_discharge_session_started = False
                # Only an integrated full fill may release the original reserve.
                cycle_floor = no_charge_reserve_kwh
                simulated_charge_incomplete = ((simulated_charge_incomplete
                    or charge_mode == "laden_van_net")
                    and sim_usable_energy_kwh < usable_capacity_kwh - 0.005)
                simulated_fill_seen = sim_usable_energy_kwh >= usable_capacity_kwh - 0.005
                if charge_mode == "laden_van_net":
                    grid_cost_floor = max(
                        ([grid_cost_floor] if grid_cost_floor is not None else [])
                        + [float(s["import_price"]) for s in slots
                           if s["end"] > max(now, slot_start) and s["start"] < charge_end]
                    )
                while slot_index < len(slots) and slots[slot_index]["start"] < overlapping_charge_window["end"]:
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
                if charge_blocked:
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
                    slots=slots,
                )
                last_charge_mode = charge_mode
                charge_stock(margin_stock, slots, max(now, slot_start), charge_end, charge_mode, max_charge_kw)
                simulated_discharge_session_started = False
                # Only an integrated full fill may release the original reserve.
                cycle_floor = no_charge_reserve_kwh
                simulated_charge_incomplete = ((simulated_charge_incomplete
                    or charge_mode == "laden_van_net")
                    and sim_usable_energy_kwh < usable_capacity_kwh - 0.005)
                simulated_fill_seen = sim_usable_energy_kwh >= usable_capacity_kwh - 0.005
                if charge_mode == "laden_van_net":
                    costs = [float(s["import_price"]) for s in slots
                             if s["end"] > max(now, slot_start) and s["start"] < charge_end]
                    grid_cost_floor = max(costs + ([grid_cost_floor] if grid_cost_floor is not None else []))
                while slot_index < len(slots) and slots[slot_index]["start"] < charge_lookup[slot_start]["end"]:
                    slot_index += 1
                charge_window_handled = True
                break
            if charge_window_handled:
                continue

            topup_amount, useful_demand = solar_topups.get(slot_start, (0.0, 0.0))
            topup_amount = min(topup_amount, max(0.0, useful_demand - max(
                0.0, sim_usable_energy_kwh - no_charge_reserve_kwh)))
            if (topup_amount > 1e-6 and sim_usable_energy_kwh <= cycle_floor + 0.005
                    and simulated_discharge_session_started
                    and not simulated_charge_incomplete
                    and sim_usable_energy_kwh < usable_capacity_kwh - 1e-6):
                current_mode, sim_usable_energy_kwh, topup_end = self._append_charge_window_mode(
                    hourly_modes=hourly_modes, slot=slot,
                    charge_window={"start": slot_start, "end": slot_end,
                                   "charge_kwh": topup_amount},
                    mode="laden_met_zonne_energie", price_key="export_price",
                    now=now, current_mode=current_mode,
                    sim_usable_energy_kwh=sim_usable_energy_kwh,
                    usable_capacity_kwh=usable_capacity_kwh,
                    max_charge_kw=max_charge_kw, slots=slots,
                )
                # An executed opportunistic fill owns the same live direction
                # latch as a selected fill. A forecast-only top-up must not.
                if slot_start <= now < topup_end:
                    self._charge_session_started = True
                    self._discharge_session_started = False
                    charge_session_started = True
                    discharge_session_started = False
                charge_stock(margin_stock, slots, max(now, slot_start), topup_end,
                             "laden_met_zonne_energie", max_charge_kw)
                simulated_discharge_session_started = False
                cycle_floor = no_charge_reserve_kwh
                simulated_charge_incomplete = False
                simulated_fill_seen = sim_usable_energy_kwh >= usable_capacity_kwh - 0.005
                if topup_end < slot_end:
                    hourly_modes.append({"start": topup_end.isoformat(), "end": slot_end.isoformat(),
                        "mode": "accu_uit", "price": float(slot["import_price"]),
                        "usable_hours": (slot_end - topup_end).total_seconds() / 3600})
                slot_index += 1
                continue

            segment_end_index = slot_index
            reachable_discharge_kwh = 0.0
            while segment_end_index < len(slots):
                segment_slot_start = slots[segment_end_index]["start"]
                segment_slot_end = slots[segment_end_index]["end"]
                if (
                    segment_end_index > slot_index
                    and (not simulated_discharge_session_started
                         or sim_usable_energy_kwh - reachable_discharge_kwh <= cycle_floor + 0.005)
                    and (
                        segment_slot_start in solar_charge_starts
                        or segment_slot_start in grid_charge_starts
                        or (segment_slot_start in solar_topups
                            and simulated_discharge_session_started
                            and not simulated_charge_incomplete)
                        or any(
                            segment_slot_end > cast(datetime, window["start"])
                            and segment_slot_start < cast(datetime, window["end"])
                            for window in charge_windows
                        )
                    )
                ):
                    break
                candidate_slot = slots[segment_end_index]
                power_kwh = max_discharge_kw * float(candidate_slot["hours"])
                home_kwh = min(power_kwh, max(0.0, -float(candidate_slot["net_solar_kwh"])))
                before_fill = (first_charge_phase_start is not None
                               and candidate_slot["start"] < first_charge_phase_start)
                imminent_fill = (before_fill and candidate_slot["end"] >=
                                 first_charge_phase_start - timedelta(minutes=30))
                if (self._cycle_export_enabled and before_fill
                        and candidate_slot["end"] == first_charge_phase_start - timedelta(minutes=30)):
                    imminent_fill = False
                loss_export = (self._cycle_export_enabled and loss_completion_enabled
                               and (not before_fill or
                                    getattr(self, '_battery_cycle_discharge_floor_kwh', None) == 0.)
                               and sim_usable_energy_kwh - reachable_discharge_kwh <= no_charge_reserve_kwh)
                # The imminent-fill veto is an execution constraint, not an
                # export price test. Only committed below-reserve completion
                # can override it, exactly as in the execution loop below.
                export_reachable = (candidate_slot.get("price_known", True)
                    and (export_profitable(candidate_slot) or loss_export)
                    and (not imminent_fill or loss_export))
                if not simulated_charge_incomplete:
                    reachable_discharge_kwh += (power_kwh if export_reachable else
                        home_kwh if home_profitable(candidate_slot) else 0.0)
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
            sunk_stock_before_refill = (getattr(self, "_battery_rank_sunk_stock", True)
                and before_first_charge_phase
                and first_charge_phase_start is not None and cycle_floor <= 0.005
                and not simulated_charge_incomplete)
            segment_reserve_kwh = cycle_floor
            if not sunk_stock_before_refill and not loss_completion_enabled and not self._has_profitable_discharge(
                slots=segment_slots, start=max(now, segment_slots[0]["start"]),
                end=segment_slots[-1]["end"], cost=grid_cost_floor,
                minimum_profit=battery_min_profit,
            ):
                # Without a profitable outlet, only home use above reserve is allowed.
                segment_reserve_kwh = max(segment_reserve_kwh, no_charge_reserve_kwh)
            discharge_budget_kwh = max(0.0, sim_usable_energy_kwh - segment_reserve_kwh)
            arbitrage = next((period for period in getattr(self, "_full_arbitrage_intervals", [])
                              if period[0] <= segment_slots[0]["start"] < period[1]), None)
            # A partial solar top-up is not a new discharge-price horizon.
            # Keep later expensive demand in the allocation on both sides of it.
            planning_slots = slots[slot_index:] if solar_topups else segment_slots
            discharge_slots = planning_slots if arbitrage is None or loss_completion_enabled else [slot for slot in planning_slots
                if slot.get("price_known", True) and float(slot["import_price"]) + 1e-9 >= arbitrage[2] + battery_min_profit]
            planned_discharge_kwh = plan_segment_discharge_kwh(
                slots=[s for s in discharge_slots if home_profitable(s)],
                available_energy_kwh=discharge_budget_kwh,
                max_discharge_kw=max_discharge_kw,
            )
            total_segment_battery_demand_kwh = sum(
                max(0.0, -float(slot.get("net_solar_kwh", 0.0))) for slot in segment_slots
            )
            # Export is only true surplus when the battery holds more energy
            # than all remaining own demand in the segment.  Prefer covering
            # cheaper home-demand hours over exporting just because the price
            # optimized discharge plan picked only the most expensive hours.
            total_planned_discharge_kwh = sum(planned_discharge_kwh.values())
            # Plan export for any genuine surplus: battery energy above what the
            # discharge plan needs.  The within_export_window gate (8 h before
            # the next charge) prevents depleting the battery too early.
            forced_export_kwh = self._plan_segment_export_kwh(
                slots=segment_slots,
                available_energy_kwh=discharge_budget_kwh,
                total_segment_demand_kwh=total_segment_battery_demand_kwh,
                max_discharge_kw=max_discharge_kw,
            )
            if arbitrage is not None:
                total_segment_battery_demand_kwh = sum(planned_discharge_kwh.values())
            has_export_surplus = discharge_budget_kwh > total_segment_battery_demand_kwh

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
            if self._cycle_export_enabled:
                forced_export_kwh = self._plan_segment_export_kwh(
                    slots=[s for s in segment_slots if (
                        _export_allowed_from is not None
                        and s["start"] >= _export_allowed_from
                        and not (before_first_charge_phase and first_charge_phase_start is not None
                                 and s["end"] > first_charge_phase_start - timedelta(minutes=30))
                        and s.get("price_known", True)
                        and export_profitable(s)
                    )],
                    available_energy_kwh=discharge_budget_kwh,
                    total_segment_demand_kwh=total_segment_battery_demand_kwh,
                    max_discharge_kw=max_discharge_kw,
                )

            reserve_fallback = (cycle_floor >= no_charge_reserve_kwh - 0.005
                                and not getattr(self, "_battery_require_profitable_cycle", False))
            if (reserve_fallback or sunk_stock_before_refill) and self._cycle_export_enabled:
                # One shared budget ranks avoidable imports and actual export
                # revenue together. Cheaper early home demand must not consume
                # stock that can earn more in a later export slot.
                offers = []
                for s in segment_slots:
                    if not s.get("price_known", True):
                        continue
                    power_kwh = max_discharge_kw * float(s["hours"])
                    home_kwh = min(power_kwh, max(0.0, -float(s["net_solar_kwh"])))
                    if home_profitable(s):
                        offers.append((float(s["import_price"]), s["start"], "home", home_kwh))
                    if self._cycle_export_enabled and export_profitable(s):
                        offers.append((float(s["export_price"]), s["start"], "export",
                                       max(0.0, power_kwh - home_kwh)))
                planned_discharge_kwh = {}
                forced_export_kwh = {}
                remaining = discharge_budget_kwh
                for _, start, kind, amount in sorted(offers, key=lambda o: (-o[0], o[1], o[2] != "home")):
                    allocated = min(remaining, amount)
                    target = planned_discharge_kwh if kind == "home" else forced_export_kwh
                    target[start] = target.get(start, 0.0) + allocated
                    remaining -= allocated

            # Compact the actual budgets, before suffix reservations and command
            # durations are computed. No chart-only filling of idle windows.
            home_bundle_candidates = [
                {**s, "price": s["import_price"], "capacity_kwh": min(
                    max_discharge_kw * float(s["hours"]),
                    max(0., -float(s["net_solar_kwh"])))}
                for s in segment_slots if home_profitable(s)
                and not any(s['end'] > w['start'] and s['start'] < w['end']
                            for w in charge_windows)]
            planned_discharge_kwh = bundle_slot_allocations(
                home_bundle_candidates, planned_discharge_kwh,
            )
            forced_export_kwh = bundle_slot_allocations(
                [{**s, "price": s["export_price"], "capacity_kwh": max(0.,
                    max_discharge_kw * float(s["hours"])
                    - min(max_discharge_kw * float(s["hours"]),
                          max(0., -float(s["net_solar_kwh"]))))}
                 for s in segment_slots if export_profitable(s)
                 and not any(s['end'] > w['start'] and s['start'] < w['end']
                             for w in charge_windows)
                 and (_export_allowed_from is None or s['start'] >= _export_allowed_from)
                 and not (before_first_charge_phase and first_charge_phase_start is not None
                          and s['end'] > first_charge_phase_start - timedelta(minutes=30))],
                forced_export_kwh,
            )

            # Precompute suffix discharge sums: remaining_planned_discharge[s] =
            # total planned discharge for all segment slots that start AFTER s.
            suffix_discharge_kwh: dict[datetime, float] = {}
            suffix_demand_kwh: dict[datetime, float] = {}
            running_suffix = 0.0
            running_demand_suffix = 0.0
            for _slot in reversed(segment_slots):
                suffix_discharge_kwh[_slot["start"]] = running_suffix
                suffix_demand_kwh[_slot["start"]] = running_demand_suffix
                running_suffix += float(planned_discharge_kwh.get(_slot["start"], 0.0))
                running_demand_suffix += max(0.0, -float(_slot.get("net_solar_kwh", 0.0)))

            # Ranking may look through a rejected proposal, but execution must
            # revisit its boundary on fresh simulated stock. Otherwise an
            # ignored proposal suppresses exports for the entire longer segment
            # and hides later legitimate charging opportunities.
            execution_end_index = next((i for i in range(slot_index + 1, segment_end_index)
                if slots[i]["start"] in solar_charge_starts
                or slots[i]["start"] in grid_charge_starts
                or slots[i]["start"] in solar_topups
                or any(slots[i]["end"] > cast(datetime, w["start"])
                       and slots[i]["start"] < cast(datetime, w["end"])
                       for w in charge_windows)),
                segment_end_index)
            for segment_slot in slots[slot_index:execution_end_index]:
                energy_before_slot = sim_usable_energy_kwh
                reserve_loss_allowed = (loss_completion_enabled
                                        and (not sunk_stock_before_refill or
                                             getattr(self, '_battery_cycle_discharge_floor_kwh', None) == 0.)
                                        and energy_before_slot <= no_charge_reserve_kwh + 1e-9)
                segment_slot_start = segment_slot["start"]
                segment_slot_end = segment_slot["end"]
                segment_discharge_kwh = float(planned_discharge_kwh.get(segment_slot_start, 0.0))
                remaining_planned_discharge_kwh = suffix_discharge_kwh[segment_slot_start]
                if reserve_loss_allowed:
                    remaining_slots = [s for s in segment_slots if s['start'] >= segment_slot_start]
                    remaining_home = plan_segment_discharge_kwh(
                        slots=[s for s in remaining_slots if home_profitable(s)],
                        available_energy_kwh=max(0.0, energy_before_slot - cycle_floor),
                        max_discharge_kw=max_discharge_kw,
                    )
                    # Keep the executable bundled suffix when it still uses the
                    # fresh physical home budget. Re-ranking that same budget
                    # would undo a bridge after its first slot has executed.
                    bundled_remaining = {s['start']: planned_discharge_kwh.get(s['start'], 0.0)
                                         for s in remaining_slots}
                    if abs(sum(bundled_remaining.values()) - sum(remaining_home.values())) <= 1e-9:
                        remaining_home = bundled_remaining
                    else:
                        remaining_home = bundle_slot_allocations(
                            [s for s in home_bundle_candidates if s['start'] >= segment_slot_start],
                            remaining_home,
                        )
                    # Replace the entire suffix, including zeroed allocations;
                    # stale reservations must not consume later export headroom.
                    planned_discharge_kwh.update({s['start']: remaining_home.get(s['start'], 0.0)
                                                  for s in remaining_slots})
                    segment_discharge_kwh = float(remaining_home.get(segment_slot_start, 0.0))
                    remaining_planned_discharge_kwh = sum(
                        value for start, value in remaining_home.items() if start > segment_slot_start)
                charge_phase_imminent = (
                    first_charge_phase_start is not None
                    and before_first_charge_phase
                    and segment_slot_start < first_charge_phase_start
                    and segment_slot_end >= first_charge_phase_start - timedelta(minutes=30)
                )
                if (self._cycle_export_enabled
                        and first_charge_phase_start is not None
                        and segment_slot_end == first_charge_phase_start - timedelta(minutes=30)):
                    charge_phase_imminent = False
                within_export_window = (
                    _export_allowed_from is not None
                    and segment_slot_start >= _export_allowed_from
                    and not charge_phase_imminent
                    and (grid_cost_floor is None or (
                        segment_slot.get("price_known", True)
                        and float(segment_slot["export_price"]) + 1e-9 >= grid_cost_floor + battery_min_profit
                    ))
                )
                if (reserve_fallback or sunk_stock_before_refill) and self._cycle_export_enabled:
                    within_export_window = (segment_slot.get("price_known", True)
                                            and not charge_phase_imminent)
                # Sunk-stock ranking cannot override the above-reserve guard.
                within_export_window = within_export_window and export_profitable(segment_slot)
                if reserve_loss_allowed and self._cycle_export_enabled:
                    # Reallocate from the actual remaining stock, not from an
                    # earlier above-reserve forecast that may have been capped.
                    remaining = max(0.0, energy_before_slot - cycle_floor
                                    - segment_discharge_kwh - remaining_planned_discharge_kwh)
                    # Loss completion has its own eligible candidates: the
                    # above-reserve profitability filter must not exclude them.
                    # Share physical power with the freshly planned household
                    # suffix, then preserve its already bundled export allocation
                    # whenever that allocation still fits the actual budget.
                    export_candidates = [
                        {**s, 'price': s['export_price'], 'capacity_kwh': max(
                            0.0, max_discharge_kw * float(s['hours'])
                            - planned_discharge_kwh.get(s['start'], 0.0))}
                        for s in remaining_slots if s.get('price_known', True)]
                    remaining_export = {}
                    for candidate in sorted(export_candidates,
                                            key=lambda s: (-float(s['export_price']), s['start'])):
                        amount = min(remaining, candidate['capacity_kwh'])
                        remaining_export[candidate['start']] = amount
                        remaining -= amount
                    bundled_export = {s['start']: forced_export_kwh.get(s['start'], 0.0)
                                      for s in remaining_slots}
                    capacities = {s['start']: s['capacity_kwh'] for s in export_candidates}
                    if (abs(sum(bundled_export.values()) - sum(remaining_export.values())) <= 1e-9
                            and all(0.0 <= amount <= capacities.get(start, 0.0) + 1e-9
                                    for start, amount in bundled_export.items())):
                        remaining_export = bundled_export
                    else:
                        remaining_export = bundle_slot_allocations(export_candidates, remaining_export)
                    # Clear ineligible/zeroed reservations as well as allocated
                    # ones, so no stale suffix can overdraw the physical stock.
                    forced_export_kwh.update({s['start']: remaining_export.get(s['start'], 0.0)
                                              for s in remaining_slots})
                    within_export_window = segment_slot.get('price_known', True)
                drain_before_charge_phase = (
                    before_first_charge_phase
                    and first_charge_phase_start is not None
                    and sim_usable_energy_kwh > 0
                    and not charge_phase_imminent
                )
                segment_export_kwh = (
                    float(forced_export_kwh.get(segment_slot_start, 0.0))
                    if (
                        (not charge_session_started
                         or (reserve_fallback and not simulated_charge_incomplete)
                         or (arbitrage is not None and
                         (sim_usable_energy_kwh >= usable_capacity_kwh - .05 or simulated_discharge_session_started)))
                        and (
                            not before_first_charge_phase
                            or drain_before_charge_phase
                            or reserve_loss_allowed
                            or reserve_fallback
                        )
                    )
                    else 0.0
                )
                within_charge_phase = (
                    any(
                        segment_slot_end > cluster["start"] and segment_slot_start < cluster["end"]
                        for cluster in charge_phase_clusters
                    )
                )
                charge_phase_mode = (
                    last_charge_mode if last_charge_mode != "accu_uit"
                    else active_charge_phase_mode
                )
                # Filling remains the chosen direction across idle price gaps.
                suppress_discharge = (simulated_charge_incomplete
                    and sim_usable_energy_kwh < usable_capacity_kwh - 0.005)
                within_charge_phase = suppress_discharge
                # Authorize against fresh lots, independently of reserve fallback,
                # bundling and credit from future sales. Export also feeds home.
                relaxed_home = relaxed_solar_home(segment_slot)
                if relaxed_home:
                    segment_export_kwh = 0.0
                    within_export_window = False
                    if energy_before_slot > no_charge_reserve_kwh + 1e-9:
                        segment_discharge_kwh = 0.0
                if not home_profitable(segment_slot):
                    segment_discharge_kwh = 0.0
                    if float(segment_slot["net_solar_kwh"]) < -1e-9:
                        segment_export_kwh = 0.0
                        within_export_window = False
                discharge_allowed_by_soc = True

                # Discharge takes priority over charging, except between charge
                # windows within today's cycle (suppress_discharge).
                mode = "accu_uit"
                if (
                    segment_discharge_kwh > 0
                    and sim_usable_energy_kwh > 0
                    and discharge_allowed_by_soc
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
                    simulated_discharge_session_started = True
                elif within_charge_phase:
                    # A charge window always beats a plain export slot.  Export
                    # that is a forced part of a segment is handled separately
                    # through segment_discharge_kwh; segment_export_kwh must
                    # never override a planned charge phase.
                    # A cycle may contain pauses between selected charge
                    # windows. Keep those pauses idle; commanding grid charge
                    # here adds unbudgeted energy without updating the simulation.
                    mode = "accu_uit"
                elif (not suppress_discharge and segment_export_kwh > 0
                      and sim_usable_energy_kwh > 0 and within_export_window):
                    mode = "ontladen_naar_net"
                    last_charge_mode = "accu_uit"
                    sim_usable_energy_kwh = max(0.0, sim_usable_energy_kwh - segment_export_kwh)
                else:
                    exportable_kwh = max(0.0, sim_usable_energy_kwh - suffix_demand_kwh[segment_slot_start])
                    slot_export_capacity_kwh = max_discharge_kw * float(segment_slot["hours"])
                    slot_solar_kwh = float(segment_slot.get("net_solar_kwh", 0))
                    if (
                        not before_first_charge_phase
                        and not charge_session_started
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

                mode_end = segment_slot_end
                remainder_mode = "accu_uit"
                reserve_kwh = cycle_floor
                if not sunk_stock_before_refill and not loss_completion_enabled and not self._has_profitable_discharge(
                    slots=(segment_slot,), start=max(now, segment_slot_start),
                    end=segment_slot_end, cost=grid_cost_floor,
                    minimum_profit=battery_min_profit,
                ):
                    # A margin in another slot cannot release this slot's reserve.
                    reserve_kwh = max(reserve_kwh, no_charge_reserve_kwh)
                if mode in ("ontladen", "ontladen_naar_net"):
                    available_kwh = max(0.0, energy_before_slot - reserve_kwh)
                    allocated_kwh = min(available_kwh, max(0.0, energy_before_slot - sim_usable_energy_kwh))
                    # Retain a reachable weighted margin for purchased FIFO
                    # stock. Earlier household use carries a debt into every
                    # later replan; PV and old stock cannot subsidize that debt.
                    home_power = min(max_discharge_kw, max(0., -float(segment_slot["net_solar_kwh"]))
                                     / max(float(segment_slot["hours"]), 1e-9))
                    sale_price = float(segment_slot["import_price"])
                    if mode == "ontladen_naar_net":
                        sale_price = (home_power*sale_price + (max_discharge_kw-home_power)
                                      * float(segment_slot["export_price"])) / max_discharge_kw
                    # A full-cycle trial may release the higher reserve only
                    # after proving its economics. The reserve fallback itself
                    # never needs historical-profit permission.
                    if (getattr(self, "_battery_require_profitable_cycle", False)
                            and not sunk_stock_before_refill):
                        future_sales = [(float(future["import_price"]),
                            min(max_discharge_kw * float(future["hours"]),
                                max(0., -float(future["net_solar_kwh"]))))
                            for future in slots if future["start"] >= segment_slot_end
                            and future.get("price_known", True)
                            and not any(w["start"] <= future["start"] < w["end"]
                                        for w in charge_windows)]
                        allocated_kwh = min(allocated_kwh, margin_stock.available(
                            sale_price, battery_min_profit, future_sales, requested=allocated_kwh))
                    # Live and forecast accounting retain acquisition costs and
                    # realized debt even when sunk-stock discharge makes a loss.
                    active_start = max(now, segment_slot_start)
                    active_hours = max(0.0, (segment_slot_end - active_start).total_seconds() / 3600)
                    discharge_kw = max_discharge_kw if mode == "ontladen_naar_net" else min(
                        max_discharge_kw,
                        max(0.0, -float(segment_slot["net_solar_kwh"])) / max(float(segment_slot["hours"]), 1e-9),
                    )
                    if allocated_kwh <= 1e-6:
                        mode = "accu_uit"
                        sim_usable_energy_kwh = energy_before_slot
                    elif discharge_kw > 0:
                        discharge_hours = min(active_hours, allocated_kwh / discharge_kw)
                        home_kw = min(
                            max_discharge_kw,
                            max(0.0, -float(segment_slot["net_solar_kwh"])) / max(float(segment_slot["hours"]), 1e-9),
                        )
                        home_kwh = home_kw * active_hours
                        if (mode == "ontladen_naar_net" and home_kw > 0
                                and segment_discharge_kwh + 1e-9 >= home_kwh
                                and allocated_kwh + 1e-9 >= home_kwh):
                            # Export mode also supplies the house. Reserve its
                            # demand for the whole slot, then spend only the
                            # surplus at the additional export power. Otherwise
                            # compressing both budgets into export leaves a false
                            # idle gap and forces the house to buy grid energy.
                            export_kw = max_discharge_kw - home_kw
                            discharge_hours = min(active_hours, max(0.0,
                                (allocated_kwh - home_kwh) / export_kw,
                            )) if export_kw > 1e-9 else 0.0
                            if discharge_hours <= 1e-9:
                                mode = "ontladen"
                                discharge_kw = home_kw
                                discharge_hours = active_hours
                            else:
                                remainder_mode = "ontladen"
                        mode_end = active_start + timedelta(hours=discharge_hours)
                        used_kwh = discharge_hours * discharge_kw
                        if remainder_mode == "ontladen":
                            used_kwh += (active_hours - discharge_hours) * home_kw
                        margin_stock.discharge(used_kwh, sale_price, battery_min_profit)
                        sim_usable_energy_kwh = max(reserve_kwh, energy_before_slot - used_kwh)

                # Standby is a household-only command, not a forecast sale.
                # Budget its worst-case use independently of the forecast and
                # of any unmeasured refill. A refresh starts at measured SOC.
                transition_discharge = False
                if (mode == "accu_uit" and segment_slot.get("solar_transition") == "tail"
                        and not suppress_discharge and segment_slot.get("price_known", True)
                        and discharge_profitable(segment_slot, "import_price")
                        and max_discharge_kw > 0):
                    active_start = max(now, segment_slot_start)
                    headroom = max(0.0, min(initial_usable_energy_kwh, sim_usable_energy_kwh)
                                   - max(reserve_kwh, no_charge_reserve_kwh) - standby_authorized_kwh)
                    tail_start = segment_slot.get("solar_transition_start", segment_slot_start)
                    if tail_start not in accounted_standby_tails:
                        accounted_standby_tails.add(tail_start)
                        # Count elapsed eligible intervals once. A cheap or
                        # unknown tariff could not have spent standby budget.
                        tail_tariffs = segment_slot.get("solar_tail_tariffs", (
                            (tail_start, now, segment_slot["import_price"]),
                        ))
                        elapsed_hours = sum(
                            (min(end, now) - start).total_seconds() / 3600
                            for start, end, price in tail_tariffs
                            if start < now and home_profitable({"import_price": price})
                        )
                        elapsed_budget = min(headroom, elapsed_hours * max_discharge_kw)
                        standby_authorized_kwh += elapsed_budget
                        headroom -= elapsed_budget
                    hours = (segment_slot_end - active_start).total_seconds() / 3600
                    budget = min(max(0.0, hours) * max_discharge_kw, headroom)
                    if budget > 1e-6:
                        mode = "ontladen"
                        mode_end = active_start + timedelta(hours=budget / max_discharge_kw)
                        transition_discharge = True
                        standby_authorized_kwh += budget

                if segment_slot["start"] <= now < segment_slot["end"]:
                    current_mode = mode
                    if not cycle_state_initialized and mode in ("laden_met_zonne_energie", "laden_van_net"):
                        charge_session_started = True
                        discharge_session_started = False
                    if mode in ("ontladen", "ontladen_naar_net") and not transition_discharge:
                        discharge_session_started = True
                        charge_session_started = False

                hourly_modes.append(
                    {
                        "start": segment_slot["start"].isoformat(),
                        "end": mode_end.isoformat(),
                        "price": round(float(segment_slot["import_price"]), 6),
                        "usable_hours": round((mode_end - segment_slot_start).total_seconds() / 3600, 3),
                        "mode": mode,
                        **({"standby": True} if transition_discharge else {}),
                    }
                )
                if mode_end < segment_slot_end:
                    hourly_modes.append({
                        "start": mode_end.isoformat(), "end": segment_slot_end.isoformat(),
                        "price": round(float(segment_slot["import_price"]), 6),
                        "usable_hours": round((segment_slot_end - mode_end).total_seconds() / 3600, 3),
                        "mode": remainder_mode,
                    })
                if sim_usable_energy_kwh <= 0.01:
                    grid_cost_floor = None

            slot_index = execution_end_index

        self._discharge_session_started = discharge_session_started
        if current_mode == "laden_van_net":
            current_price = next(float(slot["import_price"]) for slot in slots if slot["start"] <= now < slot["end"])
            previous_cost = getattr(self, "_battery_grid_charge_price", None)
            self._battery_grid_charge_price = max(current_price, previous_cost) if previous_cost is not None else current_price
        self._charge_session_started = charge_session_started and not battery_is_full
        if current_mode in ("laden_met_zonne_energie", "laden_van_net"):
            self._note_battery_charge_source(current_mode)
            if (not battery_is_full
                    and self._battery_charge_source == 'laden_met_zonne_energie'):
                self._battery_cycle_discharge_floor_kwh = no_charge_reserve_kwh
        self._battery_cycle_state_initialized = True
        self._battery_margin_state = {**observed_margin_state,
            "actions": execution_actions(hourly_modes, slots, max_charge_kw, max_discharge_kw)}
        self._store_battery_cycle_state_snapshot(now)

        self._update_active_charge_phase_state(
            now=now,
            active_charge_phase=active_charge_phase,
            current_mode=current_mode,
            last_charge_mode=last_charge_mode,
            active_charge_phase_mode=active_charge_phase_mode,
        )

        # Explicit idle remainders prevent the start-based mode schedule from
        # extending a partial charge until the next price-slot boundary.
        complete_modes: list[dict[str, str | float]] = []
        cursor = now
        for window in sorted(hourly_modes, key=lambda item: str(item["start"])):
            start = dt_util.parse_datetime(str(window["start"]))
            end = dt_util.parse_datetime(str(window["end"]))
            if start is None or end is None or end <= now:
                continue
            if start > cursor:
                if complete_modes and start - cursor <= timedelta(milliseconds=1):
                    complete_modes[-1]["end"] = start.isoformat()
                else:
                    complete_modes.append({
                        "start": cursor.isoformat(), "end": start.isoformat(),
                        "price": window["price"], "mode": "accu_uit",
                        "usable_hours": (start - cursor).total_seconds() / 3600,
                    })
            complete_modes.append({**window, "start": max(now, start).isoformat()})
            cursor = end
        if slots and cursor < slots[-1]["end"]:
            complete_modes.append({
                "start": cursor.isoformat(), "end": slots[-1]["end"].isoformat(),
                "price": float(slots[-1]["import_price"]), "mode": "accu_uit",
                "usable_hours": (slots[-1]["end"] - cursor).total_seconds() / 3600,
            })
        return self._merge_mode_windows(complete_modes), current_mode

    def _estimate_unknown_price_plan(
        self, *, slots, now, actual_windows, initial_energy, capacity,
        max_charge, max_discharge, reserve, min_profit, include_known_prices=False, preview_start=None,
    ):
        """Preview a speculative tail without changing live commands or state."""
        tomorrow = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        start = preview_start or min((s["start"] for s in slots
                     if s["start"] >= tomorrow
                     and (include_known_prices or not s.get("price_known", True))), default=None)
        if start is None or not any(s["end"] > start for s in slots):
            return []
        # Continue from the energy and cycle produced by the real-price plan.
        energy = initial_energy
        preview = copy(self)
        preview._isolated_cycle_preview = include_known_prices
        preview._store_battery_cycle_state_snapshot = lambda timestamp: None
        preview._active_charge_phase_end = None
        preview._active_charge_phase_mode = "accu_uit"
        preview._battery_cycle_state_restored_recent = False
        last_energy_mode = None
        last_energy_window_end = None
        for window in actual_windows:
            begin, end = datetime.fromisoformat(window["start"]), datetime.fromisoformat(window["end"])
            if begin >= start:
                break
            if window.get("standby"):
                continue
            mode = window["mode"]
            for slot in slots:
                hours = max(0.0, (min(start, end, slot["end"]) - max(now, begin, slot["start"])).total_seconds()/3600)
                if not hours:
                    continue
                net_kw = float(slot["net_solar_kwh"])/max(float(slot["hours"]), 1e-9)
                rate = {"laden_van_net": max_charge,
                        "laden_met_zonne_energie": min(max_charge, max(0., net_kw)),
                        "ontladen": -min(max_discharge, max(0., -net_kw)),
                        "ontladen_naar_net": -max_discharge, "accu_uit": 0.}[mode]
                energy = max(0., min(capacity, energy + hours*rate))
                if rate != 0:
                    last_energy_mode = mode
                    last_energy_window_end = end
                if rate > 0:
                    preview._battery_cycle_discharge_floor_kwh = reserve
                    preview._charge_session_started = True
                    preview._discharge_session_started = False
                    # Solar-only charging does not authorize the running-grid
                    # exception when the preview starts halfway through a fill.
                    preview._active_charge_phase_mode = mode
                    if mode == "laden_van_net":
                        preview._battery_grid_charge_price = max(
                            float(slot["import_price"]),
                            getattr(preview, "_battery_grid_charge_price", None) or float(slot["import_price"]),
                        )
                elif rate < 0:
                    preview._charge_session_started = False
                    preview._discharge_session_started = True
                if energy <= _BATTERY_DEPLETION_EPSILON_KWH:
                    preview._battery_grid_charge_price = None
        # An ended partial charge window is not a completed cycle. The mode
        # builder releases the charge latch only when simulated energy is full.
        # Treat estimates as inputs only within this isolated preview. The
        # published prices stay unknown and no live reserve is released by it.
        future = []
        for slot in slots:
            if slot["end"] <= start:
                continue
            clipped = {**slot, "price_known": True}
            if slot["start"] < start:
                fraction = (slot["end"] - start).total_seconds() / (slot["end"] - slot["start"]).total_seconds()
                clipped.update(start=start, hours=slot["hours"] * fraction)
                for key in ("solar_kwh", "demand_kwh", "net_solar_kwh"):
                    clipped[key] = slot[key] * fraction
            future.append(clipped)
        solar, grid = preview._plan_bound_charge_windows(
            slots=future, now=start, usable_capacity_kwh=capacity,
            current_remaining_capacity_kwh=max(0., capacity-energy),
            max_charge_kw=max_charge, max_discharge_kw=max_discharge, battery_min_profit=min_profit,
            no_charge_reserve_kwh=reserve,
        )
        modes, _ = preview._build_mode_windows_from_hourly_plan(
            slots=future, now=start, planned_solar_charge_windows=solar, planned_grid_charge_windows=grid,
            initial_usable_energy_kwh=energy, usable_capacity_kwh=capacity,
            average_price=sum(s["import_price"] for s in future)/len(future),
            average_export_price=sum(s["export_price"] for s in future)/len(future),
            max_charge_kw=max_charge, max_discharge_kw=max_discharge,
            no_charge_reserve_kwh=reserve, battery_min_profit=min_profit,
        )
        return [{**w, "planning_estimated": True,
                 "price_estimated": any(not s.get("price_known", True) for s in slots
                                        if s["start"] < datetime.fromisoformat(w["end"])
                                        and s["end"] > datetime.fromisoformat(w["start"]))}
                for w in modes]

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
                max(0.0, max_discharge_kw * float(slot["hours"])
                    - max(0.0, -float(slot.get("net_solar_kwh", 0.0)))),
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
        if window.get("standby"):
            serialized_window["standby"] = True
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
