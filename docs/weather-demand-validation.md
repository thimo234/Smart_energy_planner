# Weather-aware demand: offline validation

## Reproducible checks

`python analysis/validate_weather_demand.py analysis/weather-demand-live.json`
reads the separate read-only HA capture and runs the production
`component_forecast` / `correct_component` helpers. It writes
`analysis/weather-demand-validation.json`; no HA access occurs during validation.
The raw capture and generated per-entity report contain private household/device
data and must remain local, as must collector captures and the live collector
script. They are excluded from the publication allowlist. Only this aggregate
summary and the generic offline validation helper are publishable; the helper
requires a separately supplied local capture. No capture or collector is part
of the integration distribution.

## Captured-data results

Capture time: **2026-11-16 19:39 UTC**, local timezone Europe/Amsterdam.
The evaluated energy series have 26 complete, non-DST training days. Weather
candidate scores below compare the same **eight** chronological held-out days;
each day's model uses strictly earlier energy and observed temperatures.

| Component | Baseline daily MAE (kWh) | Candidate daily MAE (kWh) | Baseline WAPE | Candidate WAPE | Gate |
| --- | ---: | ---: | ---: | ---: | --- |
| Heat pump | 4.927 | 2.182 | 37.51% | 16.61% | Pass |
| Whole-home meter, independently modeled | 7.707 | 5.983 | 26.15% | 20.30% | Pass |
| Outdoor Christmas lighting | 0.477 | 0.448 | 73.32% | 68.77% | Reject |
| TV cabinet | 0.199 | 0.285 | 15.29% | 21.93% | Reject |

The dishwasher, indoor Christmas lighting and remaining monitored appliance
have no sufficiently validated positive heating fit. They retain their own
baseline. Baseline-only errors remain available even without a usable weather
fit. The whole-home row is an **independent aggregate-meter experiment**, not
an aggregate evaluation of the configured component/residual forecast.

## Boundaries on these results

- Historical temperatures are observed Recorder hourly means, not archived
  weather-provider forecasts. The scores validate temperature sensitivity, not
  delivered tomorrow-weather accuracy.
- The same chronological validation scores select activation. They are not an
  untouched final test set, and future improvement is not established.
- Per-component daily energy validation does not prove aggregate hourly or
  configured fixed/cheap-start schedule accuracy. The offline experiment uses
  learned timing and disables intraday adjustments.
- No future forecast was supplied to this validation run, so
  `forecast_days_corrected=0` is intentional. Production applies a validated fit
  only on days with complete current forecast/observed-today coverage. A
  validated model is reported separately from actual per-day activation:
  `temperature_days` and `forecast_days_corrected` identify actual corrections.
  Today's current hour may use an explicitly attributed current-temperature
  proxy; completed historical gaps are never filled with it or stale forecasts.
- The separate bounded intraday adjustment has regression coverage but no
  issuance-time historical evaluation here.
- Runtime merges Recorder state observations with Celsius mean statistics;
  coverage and source failures still determine runtime activation independently.
- No live configuration, deployment, reload, restart, or battery-policy change
  was performed for this implementation or validation.
