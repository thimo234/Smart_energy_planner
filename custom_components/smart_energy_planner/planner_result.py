"""Planner result model shared by coordinator and sensors."""

from __future__ import annotations

from dataclasses import dataclass


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
    upcoming_energy_price_windows: list[dict[str, str | float]]
    estimated_total_home_demand_kwh: float
    estimated_hourly_home_demand: list[dict[str, str | float]]
    estimated_hourly_solar_forecast: list[dict[str, str | float]]
    projected_remaining_solar_until_sunset_kwh: float
    projected_remaining_home_demand_until_sunset_kwh: float
    projected_solar_surplus_until_sunset_kwh: float
    grid_charge_needed_until_sunset_kwh: float
    battery_charge_hours_needed_until_sunset: float
    target_battery_full_by_sunset: bool
    planned_grid_charge_windows: list[dict[str, str | float]]
    planned_solar_charge_windows: list[dict[str, str | float]]
    planned_battery_mode_schedule: list[dict[str, str]]
    planned_battery_mode_windows: list[dict[str, str | float]]
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
    cheapest_price_window: dict[str, str | float] | None
    most_expensive_price_window: dict[str, str | float] | None
    battery_min_profit_per_kwh: float
    price_resolution: str
    source_status: dict[str, str]
    source_errors: list[str]
    rationale: str
