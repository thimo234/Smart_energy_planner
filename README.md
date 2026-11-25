![Smart Energy Planner](./logo.svg)

# Smart Energy Planner

Smart Energy Planner is a Home Assistant custom integration for:

- dynamic energy prices from a Nord Pool sensor
- Solcast solar forecast for today
- optional home battery capacity and charge/discharge limits

The integration exposes planner sensors that can be used in automations to shift loads to cheaper and greener moments.
Create one household configuration and add battery, price-window and consumption-forecast planners as Home Assistant config subentries. Existing standalone planners continue to work until you explicitly adopt them.

## Features

- Installable through HACS as a custom repository
- Full UI configuration through a config flow
- Reads hourly Nord Pool price blocks from an existing sensor
- Supports Nord Pool quarter-hour price blocks from `raw_today` and `raw_tomorrow`
- Uses Solcast daily forecast and `detailedHourly` production windows
- Adds optional battery-aware strategy recommendations

## Configuration

Add the integration from the Home Assistant UI and create a household, for example `Energieplanner Thuis`. Configure the shared import price sensor, optional export price sensor and export tax deduction, total home energy meter, Solcast forecasts for today/tomorrow there. Optional external consumption forecasts remain battery-specific. The total home meter must measure cumulative energy in kWh, not instantaneous power in W.

From this household, add one or more config subentries:

- `Battery planner`: battery identity, capacity, SOC, power and cycle/profit policy.
- `Price window planner`: cheapest or most expensive windows, duration and peak policy. All price-window planners in the household appear together under one shared subentry, while each retains its own settings and schedule. Use the group's reconfigure action to edit, add or remove an individual planner; adding another price-window planner from the normal subentry menu also adds it to this group.
- `Consumption forecast`: device energy meters, learned/fixed/cheap-start timing and forecast settings.

Shared sources are inherited and do not appear again in the child forms. Changing household sources reloads its planners. An unavailable child does not prevent healthy siblings from starting. This is shared configuration and source acquisition, not a new joint optimizer or automatic switching of physical appliances. Consumption device rules remain under the consumption-forecast subentry; they are not standalone appliance controllers.

### Mixed heating and price response

A heat pump can run during normal/expensive hours as well as cheap hours. In the
consumption-forecast subentry's **Reconfigure** flow, choose its energy meter,
select **Start relative to cheapest block**, then set **Price-responsive fraction**
(`shiftable_fraction`) between 0 and 1. This is per device, not a hardcoded heat-pump
entity. Use 0 to retain the learned clock-time heating profile; use a value below
1 only when a partial price response is known. For example, 0.25 retains at least
75% of every learned quarter-hour's heating demand at its clock time and moves
only 25% of the learned above-standby curve. Standby itself remains at clock time.
The example is an assumption, not a recommended or measured fraction.

The background shape, standby, excess curve and daily energy are learned from
completed consumption days. The split is **explicit configuration**, not an
inference from today's prices: the captured dataset has no archived historical
prices with which to validate a causal price-response fraction. Schedule notes
report `configured_price_response` and `historical_price_response_not_fitted`.
Without a known split, choose **Learn clock time and weekly routine** or fraction
0 rather than assuming all heating can move to cheap hours.

For a partial response, the shifted excess wraps within the same day, conserving
daily energy even with fractional offsets and midnight crossings. Missing prices
and DST-transition days fall back to the learned clock profile. Weather correction
scales the **whole mixed profile uniformly per day**, preserving normal/expensive
hour heating and the mix; it never moves the base to cheap hours. This structural
conservation is regression-tested, not a claim of live intraday prediction accuracy.
The default fraction is 1 for backward compatibility with existing full-cycle
schedules; fixed starts and existing non-heating schedules are unchanged. No live
configuration or device control is changed automatically.

### Optional weather-aware consumption forecasts

The household form also accepts **Weather forecast (optional, hourly)** and
**Outdoor temperature history (optional)**. Select your own weather entity and
outdoor temperature sensor; neither has a default. Weather must support Home
Assistant's hourly `weather.get_forecasts` read API. Temperature history is read
from Recorder states and hourly mean statistics, including older observations
after detailed state history has been purged; the current sensor value is never
substituted for missing completed history. Completed observations take precedence
over overlapping provider forecasts. For the **current hour only**, a valid
current outdoor sensor reading (or the weather entity's current temperature)
can supply an explicit Celsius daily-mean proxy; it is never added to training
history or used to fill arbitrary past gaps. `demand_weather_sources.current_hour`
identifies that input; without it, current-hour provider coverage is used.
Celsius and Fahrenheit are supported. No appliances are switched, no entities
are configured automatically, and battery policies are unchanged.

Consumption forecasts continue working without either source. Historical
completed-day component consumption is compared with same-day outdoor
temperature. A bounded heating-degree correction is enabled **per component**
only after at least 14 earlier complete training days, seven chronological
held-out predictions, positive heating sensitivity, and held-out MAE reduced by
more than 10% and 0.05 kWh against the historical baseline. This does not multiply
all appliances by a household-wide temperature factor: a temperature-independent
dishwasher keeps
its own learned/scheduled forecast. Fixed/cheap-start components retain their
schedule; only a validated component's daily energy changes, by at most 50%.
Days with missing temperature coverage (including incomplete future forecasts)
use the existing component forecast, never zero demand. DST transition days are
excluded from the 24-hour daily fit rather than compressed or duplicated.

A separate intraday adjustment uses at least three **completed** observed hours,
with at least 0.3 kWh of expected consumption. It adjusts only future hours today,
by at most 20%, only for the aggregate/unassigned learned household component;
it does not rescale individually scheduled appliances. Recorder energy and
temperature caches refresh on each completed-hour boundary.

The demand sensor exposes `demand_weather_sources` and per-component
`demand_accuracy`: source availability, training/validation counts, baseline and
candidate MAE, signed bias, WAPE, activation/fallback reason, and intraday factor.
`temperature_model_status=validated` means the historical gate passed, not that
every forecast day was corrected. `temperature_days` reports each day's actual
activation or fallback; overall `temperature_status` is `active`,
`partially_active`, or `missing_forecast_coverage` after validation, and
`forecast_days_corrected` counts the days actually scaled. Configured optional
source failures appear in normal `source_status` / `source_errors` as
`ready_with_warnings`; the underlying consumption forecast remains usable.
`baseline_history` remains available without weather when energy history permits.
The validation uses only earlier days to predict each later day. Temperature
validation uses **historical observed temperatures**, not archived weather
forecasts: it measures a component relationship, not end-to-end forecast-provider
accuracy. The intraday adjustment is bounded but is not included in these daily
validation scores. No accuracy improvement on your installation is claimed until
its own sufficiently covered history passes these gates. See the
[offline captured-data validation](docs/weather-demand-validation.md) for measured
component errors and the limits of those results.


Existing price-window subentries within the same household are consolidated during setup, preserving planner identities, entity IDs, device links, individual schedules and stored runtime. Standalone entries are not silently adopted. Removing a planner in the group asks for confirmation and only cleans up that planner's entities/devices on reload; other members and historical runtime remain. Home Assistant's delete action for the entire shared subentry deletes the whole group. See [household architecture and verification](docs/household-configuration-verification.md) for migration/retry details and offline-test limitations.

### Adopt an existing planner without losing its identity

Existing standalone configurations remain supported; they are not silently merged. Create a household with matching shared sources, add a subentry of the desired type, select the existing planner, and confirm adoption. Conflicting sources abort adoption rather than silently changing the old planner. Entity IDs and persisted battery/forecast/window identities are retained. The old entry is left as an inert archive after successful adoption, so interrupted adoption can resume without two controllers. Repeat for each existing planner.

Manage shared sources through the household options and planner-specific settings through the subentry reconfigure action. Devices are grouped in the integration configuration; this does not create a physical Home Assistant device hierarchy.

Thermostat control has been removed. Before updating, move any active heating or
cooling control to another controller. Existing thermostat entries no longer load;
remove them from Home Assistant. Existing switches are not changed by this update.

The UI filters entity choices for compatible price, Solcast and energy sensors. Battery planners derive estimated demand from total-home history unless an external demand forecast is configured. Household planners coalesce matching historical reads and invalidate cached inputs on source updates; each planner keeps its own results and policy.

## Exposed entities

Price Window Start includes `next_price_window_start` and `next_price_window_end`
for the block after the selected block; these are empty when no following block
is known. Estimated Home Demand Today exposes only today's total and hourly
consumption forecast and its source diagnostics. Battery Strategy retains the
battery and planning details. The former Planner Score entity is removed on setup.

The price window planner's expensive-price mode finds successive local price
peaks. For each peak it locks the most expensive contiguous block of the configured
maximum duration around that peak. After the peak, it ends the block early at the
first known price at or below the higher surrounding valley floor plus the
configured prominence percentage of that day's price spread. The start stays
locked; the minimum off time runs from the actual earlier end. The threshold is
stored as `normal_price_threshold`, and the average price and duration describe
the shortened block. A shortened block is never extended on refresh or restart.
Equal consecutive peak prices count as one peak; lower known prices must confirm
both sides. **Minimum peak prominence (% of price spread)** filters small ripples:
the summit must rise above its surrounding valley floors by at least this
percentage of its local day's highest minus lowest known price. The default is
10%; 0 disables the filter. There is no rolling average or two-peaks-per-day limit.
When new blocks compete for the required off time, the more prominent peak wins.
A small early summit therefore cannot displace the larger evening peak. Existing
locked blocks retain their start times; this priority applies to newly found blocks.
Only complete blocks covered by known prices are selected. Peaks that cannot fit
are skipped. The cheapest-window mode keeps its existing daily selection.

**Minimum off time between price peak blocks (hours)** is separate from the block
duration: default 1 hour, configurable from 0.25 to 48 hours in quarter-hour steps.
Blocks never overlap or touch. The binary sensor turns off at the locked end time,
without waiting for another planner update. At startup, future blocks are selected
again from all available prices, then locked. This waits for complete daily prices;
until then restored blocks survive temporary price-sensor outages. Active blocks
and the actual last end remain protected so a restart cannot bypass the minimum
pause. The list is exposed as `planned_price_peak_windows`.
Small peak blocks that have not started are always removed, including
previously locked future blocks that no longer contain a sufficiently prominent
peak. The former cleanup switch has been removed; saved disabled settings are
ignored. Rechecking requires complete daily prices; missing data never cancels a
block. Active and future blocks can also end early;
an active block whose normal-price boundary has already passed stops now, without
backdating its end. Missing tariffs cannot trigger an early end. Lowering the maximum
duration can shorten blocks as before; increasing it never extends a locked block.

- `sensor.smart_energy_planner_recommendation`
- `sensor.smart_energy_planner_battery_strategy`
- `sensor.smart_energy_planner_estimated_home_demand_today`

The sensors also expose extra attributes such as the next cheap window, the price spread for the current day, and the Solcast production forecast used by the planner.
The estimated home demand sensor includes `estimated_hourly_home_demand` with a per-hour forecast for today.

For Solcast, the planner works best with the sensor that exposes today's forecast total plus `detailedHourly`.
Battery strategy values:

- `accu_uit`
- `ontladen`
- `laden_met_zonne_energie`
- `laden_van_net`

## Experimental planner v2

In the battery planner options, turn on **Experimental planner v2 (solar only)**
to compare alternative solar charging windows. It keeps the existing battery
limits and pricing rules, scores published commands under both the original and
a lower-sun forecast, and accounts for mode changes. It does not optimize grid
charging or battery export in this release. V1 remains the default and is used
whenever v2 cannot find a suitable improvement within its compute budget.
The main card and Battery Strategy attributes show the selected planner and
why it was chosen. Turn the option off to return to v1.

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
Once discharging starts, solar and grid refills wait until the selected cycle
boundary is reached. A cheaper opportunity can be skipped while discharge is
incomplete. Charging may pause across gaps but does not reverse before the full
target is reached. A simulated endpoint is not measured completion.

**Solar forecast safety margin (%)** now reduces the solar energy used throughout
the battery plan, instead of merely padding charging duration. The default is
20%: plan with 80% of forecast solar. An explicitly saved 0% stays disabled.
The separate demand safety margin defaults to 20% extra demand. These margins
can bring charging and evening discharge forward while keeping price and cycle
constraints. They are forecast safeguards, not an instantaneous power controller;
unexpected peaks can still require grid power between updates or at power limits.

New charging must meet the configured minimum tariff margin against executable
later demand or export. Existing stored energy is scheduled at the most valuable
remaining known hours regardless of historical acquisition cost or realized margin
debt. Without a profitable new charging block, discharge stops at the configured
higher reserve. Historical cost records are retained, not reset to unlock discharge.
A started fill retains its direction until full; reaching the discharge boundary
only makes a new fill eligible and never exempts it from profitability admission.
Conversion losses and battery wear are not separately modeled.

A saved higher reserve is a fallback, not an immutable discharge boundary.
If a realizable, profitable **full new refill** is selected later (including
tomorrow), the planner can upgrade a partial discharge to the normal minimum
before that refill. Existing stock is ranked at the highest remaining known
home/export opportunities without historical-cost or debt gating. A partial,
unprofitable or physically insufficient refill does not release the reserve;
raw unselected solar surplus does not count. A started fill still must reach
its full target before downstream discharge. The normal minimum, maximum,
known-tariff/export permissions and minimum-profit admission for new energy
remain in force; accounting lots and realized losses are preserved. Existing
profitable complete-cycle permissions are unchanged; without a qualifying new
refill, a saved fallback is not upgraded.

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

For a single-source dashboard, select only Battery Strategy. Configure the
consumption forecast and both solar forecast sources in the battery planner.
Battery Strategy exposes the complete price, solar and demand series used by the
plan, plus `status`, `source_entities` and `demand_forecast_source` diagnostics.
Copy its full state and attributes when sharing a planning problem.

```yaml
type: custom:smart-energy-planner-card
title: Energieprijs planning
planner_entity: sensor.smart_energy_planner_battery_strategy
```

Both visual card editors use Battery Strategy (`planner_entity`) as the single
source for prices and forecasts. Old separate price/forecast selections are
ignored on load and removed when saving the editor. The mini card retains its
actual consumption, solar and SOC meters for measured history. The card reads
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

The integration includes its icon and logo in
`custom_components/smart_energy_planner/brand/`. Home Assistant 2026.3 or newer
loads these local brand images automatically. After updating, restart Home
Assistant and refresh the browser or reopen the app if an old placeholder remains.

## Compacte kaart voor NSPanel Pro

Kies **Smart Energy Planner Mini** bij het toevoegen van een dashboardkaart.
In de visuele configuratie kies je de planner, werkelijk huisverbruik en werkelijk
zonnevermogen (sensoren in W/kW of oplopende Wh/kWh-tellers met Recorder-geschiedenis). Een aparte prijssensor
is optioneel. De bestaande frontendresource bevat ook deze kaart.

```yaml
type: custom:smart-energy-planner-mini-card
planner_entity: sensor.smart_energy_planner_battery_strategy
consumption_entity: sensor.huisvermogen
solar_entity: sensor.zonnevermogen
soc_entity: sensor.accupercentage
```

De grafiek is 150 px hoog en toont de lokale kalenderdag van 00:00 tot 23:59.
Prijs staat in afgeronde kolommen met tijdgewogen uurgemiddelden en dezelfde
groen/geel/rode prijsindeling als de gewone kaart. Verbruik is een paarse
stippellijn en zon een gele lijn. Verbruik en zon gebruiken uurgemiddelden met
vloeiende lijnen. De linker schalen zijn weggelaten; eenheden en waarden staan
in het detailvenster. Toekomstige waarden behouden dezelfde kleuren, maar zijn
licht vervaagd. Tik op een kolom om de prijs (€/kWh), verbruik en zon (gemiddeld kW)
te zien. De achtergrond toont de batterijmodus: blauw voor netladen, geel voor
zonneladen, groen voor ontladen en paars voor export. Ontbrekende meetgegevens
blijven leeg. De kaart en meetgeschiedenis verversen maximaal eens per vijf minuten,
en pauzeren als het dashboard verborgen is. Nu en gegevens kunnen daardoor vijf
minuten achterlopen; selectie reageert direct. De kaartachtergrond is transparant.
Als het detailvenster open is, sluit een tik ergens op de kaart het venster.
Kies bij **Accupercentage (%)** de SOC-sensor met Recorder-geschiedenis.
Historische details tonen de geregistreerde accustand aan het begin van het uur;
ontbrekende historie wordt als een streepje weergegeven.
Wh/kWh-tellers worden via hun stijging en meetduur omgerekend naar gemiddeld kW;
het laatste interval verschijnt zodra de volgende tellerstand beschikbaar is.
# Independent consumption forecast

Add another Smart Energy Planner integration entry and select **Consumption
forecast**. This entry needs no battery or solar sensors. Select cumulative
Wh/kWh meters (`total` or `total_increasing`), not W/kW power sensors.

- Select an optional household total and the individual appliance meters. The
  household total must include those appliances. The planner learns the
  remainder after subtracting their measured consumption; it never adds the
  full household total on top of the appliances. Without a household meter,
  the forecast covers only the selected appliances.
- For each appliance, choose learned clock-time/week patterns, a fixed start,
  or an offset from the cheapest contiguous price block. Select active weekdays
  for explicitly scheduled appliances. The price block duration and whole-hour
  starts are configurable. A price sensor is required for price-based timing.
- Example: five-hour block, dishwasher offset **0**, heat pump offset **2**,
  cooking fixed at **17:30**. This predicts consumption; it does not switch
  appliances or change their existing automations.
- Energy and the shape/duration of the active profile come from Recorder. At
  least two complete usable days are needed for every component, including
  the remainder. The previous 28 days are read once an hour; a repeated weekday
  uses up to four matching days, otherwise up to fourteen recent days are used.
  Scheduled profiles are aligned using the first active historical hour.
  Multiple cycles, changing heating demand and hourly source resolution limit
  the accuracy; quarter-hour output does not imply quarter-hour measurements.
- If future prices are unknown, the affected appliance keeps its historical
  clock-time forecast and reports this fallback in the source diagnostics.

The **Consumption Forecast** sensor reports today's expected kWh and publishes
three days of quarter-hour intervals plus the component forecasts as live
attributes. These large arrays are excluded from Recorder.

In the battery planner options, select this sensor as **Consumption forecast**.
It takes precedence over the old household meter calculation. Appliance timing
is retained; only the battery's consumption safety margin is applied, once.
An unavailable, malformed, incomplete or more than two-hour-old forecast puts
the battery planner into a waiting state instead of assuming zero consumption.
Existing battery entries keep their current internal forecast until an external
forecast sensor is selected. Installation and testing in a live Home Assistant
instance are still required.
