![Smart Energy Planner](./logo.svg)

# Smart Energy Planner

Smart Energy Planner is a Home Assistant custom integration for:

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
- Uses Solcast daily forecast and `detailedHourly` production windows
- Adds optional battery-aware strategy recommendations

## Configuration

Add the integration from the Home Assistant UI and choose one of these planner types:

- `Battery planner`
- `Thermostat planner`

You can add the integration multiple times if you want one standalone battery planner and one standalone thermostat planner.
Depending on the selected planner type, configure:

- Energy price sensor
- Solcast forecast sensor for today
- Outdoor temperature sensor
- Room temperature sensor
- Heating control switch
- Total home energy sensor
- Number of historical days to inspect
- Eco setback below thermostat setpoint
- Cold tolerance
- Hot tolerance
- Minimum thermostat temperature
- Maximum thermostat temperature
- Minimum switch interval (minutes)
- Heating status check interval (minutes)
- Contract price interval: hourly or quarter-hourly
- Optional home battery support
- Battery capacity
- Minimum battery profit per kWh
- Maximum battery charge power
- Maximum battery discharge power

After setup, you can open the integration settings again from Home Assistant and adjust the full configuration from the settings icon.
The UI filters the entity choices so you mainly see compatible Nord Pool, Solcast, temperature, and energy sensors.

The battery planner derives the estimated energy for today only from the history of your total home energy sensor. It does not split heating out separately and does not require a heating energy sensor.
The thermostat planner focuses on room cooling behavior and expensive hours, and creates its own climate entity that becomes your room thermostat in Home Assistant. It automatically chooses eco mode during the most expensive block that the room can bridge by slowly cooling down, switches the heating switch with configurable hysteresis, and periodically checks whether the heating state still matches the thermostat target.

## Exposed entities

- `sensor.smart_energy_planner_score`
- `sensor.smart_energy_planner_recommendation`
- `sensor.smart_energy_planner_battery_strategy`
- `sensor.smart_energy_planner_estimated_home_demand_today`
- `sensor.smart_energy_planner_room_cooling_hours_to_eco`
- `sensor.smart_energy_planner_thermostat_eco_start_time`
- `climate.smart_energy_planner_planner_thermostat`

The sensors also expose extra attributes such as the next cheap window, the price spread for the current day, and the Solcast production forecast used by the planner.
The estimated home demand sensor includes `estimated_hourly_home_demand` with a per-hour forecast for today.
For thermostat planners, the entities also include `planned_eco_window_start`, `planned_eco_window_end`, `room_cooling_rate_c_per_hour`, and the current thermostat setpoint.

For Solcast, the planner works best with the sensor that exposes today's forecast total plus `detailedHourly`.
Battery strategy values:

- `accu_uit`
- `ontladen`
- `laden_met_zonne_energie`
- `laden_van_net`

## Battery reserve

Grid charging requires forecast home consumption at a known later tariff
at least `battery_min_profit_per_kwh` above the purchase tariff, beyond what
existing usable energy can supply. Once justified,
the cycle aims to refill the battery to 100%, using solar and eligible cheap
grid slots, including slots before a small solar window. This prioritizes a
full recharge: energy retained as reserve does not yet have a proven profitable
discharge. Equal-price grid slots nearest the cheapest block avoid idle gaps.
Charging durations account for power limits and remaining battery capacity.
A running grid refill keeps its full-charge target across updates, while still
checking known future tariffs against the minimum margin. Energy already bought
in that cycle must not cancel the remaining charge halfway through.

Both sources compete on effective price within each charge cycle. The configured
pricing model values stored solar at the export revenue it forgoes. Without a
separate export sensor (or when selecting the same sensor for both), export is
derived from import minus **Export price tax deduction (EUR/kWh)**, default 0.11.
Enter 0.11 for 11 cents, or 0 to disable the deduction. The import sensor should
include the taxes you want to subtract. A distinct export sensor is used as-is.
This is a user-configured fixed amount, not an automatically maintained tax rate
or a percentage VAT calculation. Grid charging
uses the full import tariff. Cheaper grid energy can therefore displace later,
more expensive solar, even with abundant solar forecast. At a known negative
import tariff, the requested strategy is grid charging, including when solar
surplus is present. A slot uses one charging mode and respects inverter power.
Positive effective costs require a known later tariff with the minimum margin;
unknown later prices do not justify a profitable cycle. Equal-price slots nearest
the cheapest block are preferred. Partially filled
cycles carry their remaining capacity into the next cycle's calculation.
Once discharging starts, both solar and grid refills wait until the safe minimum
is reached (within 0.05 kWh tolerance). A cheaper opportunity can be skipped if
the discharge cycle is still incomplete. Charging pauses across expensive gaps
without reversing direction while another selected charging window remains that
day. After the final profitable window, evening discharge may start even if weak
sun prevented a full charge. The additional reserve and minimum profit still apply.

**Solar forecast safety margin (%)** now reduces the solar energy used throughout
the battery plan, instead of merely padding charging duration. The default is
20%: plan with 80% of forecast solar. An explicitly saved 0% stays disabled.
The separate demand safety margin defaults to 20% extra demand. These margins
can bring charging and evening discharge forward while keeping price and cycle
constraints. They are forecast safeguards, not an instantaneous power controller;
unexpected peaks can still require grid power between updates or at power limits.

After grid charging, discharge and export also respect the minimum price
difference. The highest grid purchase price is retained across refreshes and
restarts until the usable battery is depleted. This deliberately conservative
rule also protects mixed battery energy. Historical energy with no recorded
purchase price cannot be checked retroactively. The margin is a tariff
difference per modeled kWh; conversion losses and battery wear are not modeled.

A selected solar or grid charge window releases the extra no-charge reserve.
Unselected raw solar surplus does not. There is no separate reserve for grid-only
cycles. After the last selected charging opportunity, the extra floor applies
again. It prevents further discharge;
it does not force charging if the battery is already below that floor.

Battery options include **Minimum battery state of charge without a charging
opportunity (%)**. This additional reserve applies when the remaining planning
horizon contains no selected replenishment window. It limits both home discharge and grid export;
the normal minimum state of charge always remains the absolute lower limit.
For example, with a normal minimum of 20% and this setting at 40%, the battery
stops discharging at 40% when no replenishment is forecast. A future charge
opportunity releases the extra reserve. Existing installations default to their
normal minimum until this option is changed.

The strategy sensor exposes `battery_no_charge_min_soc_percent`,
`battery_no_charge_reserve_active`, and `battery_reserved_energy_kwh` for checking
the reserve. The planner supplies a strategy; the inverter or your automation
must follow it to stop actual discharge.

## Lovelace card

When tomorrow's prices are missing, the card shows a **provisional** battery plan
through tomorrow midnight. Missing tariffs use the duration-weighted average of
the latest known 24-hour period (available observations if incomplete). These
prices retain `price_known: false`. The provisional schedule is separate from
the executable plan: it does not change today's command, cycle state or reserve.
It is recalculated and replaced when actual prices arrive. A flat estimate cannot
identify tomorrow's cheapest hour or prove profitable grid arbitrage by itself.

The strategy sensor exposes `current_export_price` and
`upcoming_export_price_windows` for the effective export tariffs, and
`estimated_battery_mode_windows` for the provisional schedule. The card marks
the latter as a forecast; automations should keep using the strategy state.

The integration includes a custom Lovelace card that shows the upcoming energy
price as a stepped graph, the expected home demand as a dotted line, and the
planned battery mode as background colors.

Add this JavaScript module as a Lovelace resource:

```yaml
url: /smart_energy_planner/smart-energy-planner-card.js
type: module
```

Then add the card:

```yaml
type: custom:smart-energy-planner-card
title: Energieprijs planning
planner_entity: sensor.smart_energy_planner_battery_strategy
```

In the visual editor you only need to select the planner sensor. The card reads
`upcoming_energy_price_windows`, `estimated_hourly_home_demand`, and
`planned_battery_mode_schedule` from that selected planner entity. By default
the graph starts one hour before the current hour and continues to the last
known energy price. Add `max_hours_to_show` only if you want to cap the visible
horizon. The integration registers the Lovelace resource automatically in
storage mode as `/smart_energy_planner/smart-energy-planner-card.js`. YAML-mode
dashboards still need a manual resource entry.

## HACS

1. Open HACS.
2. Add this repository as a custom repository.
3. Choose category `Integration`.
4. Install `Smart Energy Planner`.
5. Restart Home Assistant.
6. Add the integration from **Settings -> Devices & services**.
