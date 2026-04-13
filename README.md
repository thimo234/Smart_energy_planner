# Smart Energy Planner

Smart Energy Planner is a Home Assistant custom integration that combines:

- dynamic energy prices from a Nord Pool sensor
- Solcast solar forecast for today
- outdoor temperature
- recent heating energy usage
- optional home battery capacity and charge/discharge limits

The integration exposes planner sensors that can be used in automations to shift loads to cheaper and greener moments.

## Features

- Installable through HACS as a custom repository
- Full UI configuration through a config flow
- Reads hourly Nord Pool price blocks from an existing sensor
- Estimates heating demand from outdoor temperature and recent heat pump consumption
- Adapts advice when forecast solar energy is available
- Adds optional battery-aware strategy recommendations

## Configuration

Add the integration from the Home Assistant UI and configure:

- Energy price sensor
- Solar forecast sensor
- Solcast forecast sensor for today
- Outdoor temperature sensor
- Heat pump energy sensor
- Number of historical heating days to inspect
- Optional home battery support
- Battery capacity
- Maximum battery charge power
- Maximum battery discharge power

## Exposed entities

- `sensor.smart_energy_planner_score`
- `sensor.smart_energy_planner_recommendation`
- `sensor.smart_energy_planner_battery_strategy`
- `sensor.smart_energy_planner_heating_estimate`

The sensors also expose extra attributes such as the next cheap window, the price spread for the current day, and the Solcast production forecast used by the planner.

## HACS

1. Open HACS.
2. Add this repository as a custom repository.
3. Choose category `Integration`.
4. Install `Smart Energy Planner`.
5. Restart Home Assistant.
6. Add the integration from **Settings -> Devices & services**.
