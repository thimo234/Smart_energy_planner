# Smart Energy Planner

Smart Energy Planner is a Home Assistant custom integration that combines:

- dynamic energy prices from a Nord Pool sensor
- Solcast solar forecast for today
- outdoor temperature
- optional home battery capacity and charge/discharge limits

The integration exposes planner sensors that can be used in automations to shift loads to cheaper and greener moments.
You can now add the integration multiple times and choose a separate planner type for the battery and for thermostat control.

## Features

- Installable through HACS as a custom repository
- Full UI configuration through a config flow
- Reads hourly Nord Pool price blocks from an existing sensor
- Supports Nord Pool quarter-hour price blocks from `raw_today` and `raw_tomorrow`
- Estimates heating demand from outdoor temperature and recent heat pump consumption
- Uses Solcast daily forecast and `detailedHourly` production windows
- Adds optional battery-aware strategy recommendations

## Configuration

Add the integration from the Home Assistant UI and choose one of these planner types:

- `Combined planner`
- `Battery planner`
- `Thermostat planner`

You can add the integration twice if you want one standalone battery planner and one standalone thermostat planner.
Depending on the selected planner type, configure:

- Energy price sensor
- Solcast forecast sensor for today
- Outdoor temperature sensor
- Room temperature sensor
- Room thermostat entity
- Heating control switch
- Total home energy sensor
- Number of historical heating days to inspect
- Eco setback below thermostat setpoint
- Cold tolerance
- Hot tolerance
- Minimum thermostat temperature
- Maximum thermostat temperature
- Contract price interval: hourly or quarter-hourly
- Optional home battery support
- Battery capacity
- Minimum battery profit per kWh
- Maximum battery charge power
- Maximum battery discharge power

After setup, you can open the integration settings again from Home Assistant and adjust the full configuration from the settings icon.
The UI filters the entity choices so you mainly see compatible Nord Pool, Solcast, temperature, and energy sensors.

The battery planner derives household usage from the history of your total home energy sensor.
The thermostat planner focuses on room cooling behavior and expensive hours, and now creates its own climate entity that automatically drives the linked room thermostat into eco mode during the most expensive block that the room can bridge by slowly cooling down. It also switches the heating switch on and off around the active target temperature using configurable cold and hot tolerances.

## Exposed entities

- `sensor.smart_energy_planner_score`
- `sensor.smart_energy_planner_recommendation`
- `sensor.smart_energy_planner_battery_strategy`
- `sensor.smart_energy_planner_heat_pump_strategy`
- `sensor.smart_energy_planner_estimated_home_demand_today`
- `sensor.smart_energy_planner_heating_estimate`
- `sensor.smart_energy_planner_thermostat_eco_setpoint`
- `sensor.smart_energy_planner_room_cooling_hours_to_eco`
- `climate.smart_energy_planner_planner_thermostat`

The sensors also expose extra attributes such as the next cheap window, the price spread for the current day, and the Solcast production forecast used by the planner.
The estimated home demand sensor includes `estimated_hourly_home_demand` with a per-hour forecast for today.
For thermostat planners, the entities also include `planned_eco_window_start`, `planned_eco_window_end`, `room_cooling_rate_c_per_hour`, and the current and eco thermostat setpoints.

For Solcast, the planner works best with the sensor that exposes today's forecast total plus `detailedHourly`.
Battery strategy values:

- `accu_uit`
- `ontladen`
- `laden_met_zonne_energie`
- `laden_van_net`

Heat pump strategy values:

- `normal`
- `energy_saving_on`

## HACS

1. Open HACS.
2. Add this repository as a custom repository.
3. Choose category `Integration`.
4. Install `Smart Energy Planner`.
5. Restart Home Assistant.
6. Add the integration from **Settings -> Devices & services**.
