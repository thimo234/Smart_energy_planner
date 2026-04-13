"""Constants for Smart Energy Planner."""

from __future__ import annotations

from datetime import timedelta

DOMAIN = "smart_energy_planner"

CONF_PLANNER_KIND = "planner_kind"
CONF_PRICE_SENSOR = "price_sensor"
CONF_SOLCAST_TODAY_SENSOR = "solcast_today_sensor"
CONF_TEMPERATURE_SENSOR = "temperature_sensor"
CONF_ROOM_TEMPERATURE_SENSOR = "room_temperature_sensor"
CONF_THERMOSTAT_ENTITY = "thermostat_entity"
CONF_HEATING_SWITCH_ENTITY = "heating_switch_entity"
CONF_HEATING_ENERGY_SENSOR = "heating_energy_sensor"
CONF_TOTAL_ENERGY_SENSOR = "total_energy_sensor"
CONF_HEATING_LOOKBACK_DAYS = "heating_lookback_days"
CONF_HEAT_PUMP_MAX_OFF_HOURS = "heat_pump_max_off_hours"
CONF_HEAT_PUMP_MIN_ON_HOURS = "heat_pump_min_on_hours"
CONF_THERMOSTAT_ECO_SETBACK = "thermostat_eco_setback"
CONF_PRICE_RESOLUTION = "price_resolution"
CONF_BATTERY_ENABLED = "battery_enabled"
CONF_BATTERY_CAPACITY_KWH = "battery_capacity_kwh"
CONF_BATTERY_MIN_PROFIT_PER_KWH = "battery_min_profit_per_kwh"
CONF_BATTERY_MAX_CHARGE_KW = "battery_max_charge_kw"
CONF_BATTERY_MAX_DISCHARGE_KW = "battery_max_discharge_kw"

PLANNER_KIND_COMBINED = "combined"
PLANNER_KIND_BATTERY = "battery"
PLANNER_KIND_THERMOSTAT = "thermostat"

PRICE_RESOLUTION_HOURLY = "hourly"
PRICE_RESOLUTION_QUARTER_HOURLY = "quarter_hourly"

DEFAULT_NAME = "Smart Energy Planner"
DEFAULT_PLANNER_KIND = PLANNER_KIND_COMBINED
DEFAULT_HEATING_LOOKBACK_DAYS = 5
DEFAULT_HEAT_PUMP_MAX_OFF_HOURS = 3
DEFAULT_HEAT_PUMP_MIN_ON_HOURS = 1
DEFAULT_THERMOSTAT_ECO_SETBACK = 2.0
DEFAULT_PRICE_RESOLUTION = PRICE_RESOLUTION_HOURLY
DEFAULT_BATTERY_ENABLED = False
DEFAULT_BATTERY_CAPACITY_KWH = 10.0
DEFAULT_BATTERY_MIN_PROFIT_PER_KWH = 0.08
DEFAULT_BATTERY_MAX_CHARGE_KW = 2.5
DEFAULT_BATTERY_MAX_DISCHARGE_KW = 2.5

COORDINATOR_UPDATE_INTERVAL = timedelta(minutes=15)
