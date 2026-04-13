"""Constants for Smart Energy Planner."""

from __future__ import annotations

from datetime import timedelta

DOMAIN = "smart_energy_planner"

CONF_PRICE_SENSOR = "price_sensor"
CONF_SOLCAST_TODAY_SENSOR = "solcast_today_sensor"
CONF_TEMPERATURE_SENSOR = "temperature_sensor"
CONF_HEATING_ENERGY_SENSOR = "heating_energy_sensor"
CONF_HEATING_LOOKBACK_DAYS = "heating_lookback_days"
CONF_BATTERY_ENABLED = "battery_enabled"
CONF_BATTERY_CAPACITY_KWH = "battery_capacity_kwh"
CONF_BATTERY_MAX_CHARGE_KW = "battery_max_charge_kw"
CONF_BATTERY_MAX_DISCHARGE_KW = "battery_max_discharge_kw"

DEFAULT_NAME = "Smart Energy Planner"
DEFAULT_HEATING_LOOKBACK_DAYS = 5
DEFAULT_BATTERY_ENABLED = False
DEFAULT_BATTERY_CAPACITY_KWH = 10.0
DEFAULT_BATTERY_MAX_CHARGE_KW = 2.5
DEFAULT_BATTERY_MAX_DISCHARGE_KW = 2.5

COORDINATOR_UPDATE_INTERVAL = timedelta(minutes=15)
