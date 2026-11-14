# Fixed-boundary battery policy — local verification

## Scope and status

Publication candidate in `/opt/data/projects/homeserver/ha_energy_planner-rollback`,
based on `cc89399`. The user approved Git publication after the computed graph.
The other dirty parent checkout is untouched; installation, deployment and live
Home Assistant restart remain outside this approval. The independent-review
blocker and subsequent verification are recorded below.

Publication check: 39 focused bound-policy, integration, stock-migration,
complete-cycle and tomorrow-fill tests passed in this exact worktree; diff
whitespace checks passed. Historical full-suite results below are retained.

Current policy is the first section of `accuplanner-eisen.md`. Older partial-cycle
approval/status text in that file is historical, not approval of this candidate.

## Implementation

- `coordinator.py`: select the normal physical minimum when a complete profitable
  full-capacity discharge is feasible; otherwise select the configured higher
  reserve. Store the selected usable-energy boundary together with observed
  phase in `battery_cycle_state`. Restored/full and restored/reserve cycles keep
  their choice across price updates and JSON restart.
- Source selection uses that boundary before constructing charge candidates.
  Incomplete charging pauses without releasing discharge; incomplete discharge
  rejects refills even when solar or cheap import appears. Charge target remains
  the configured maximum. Live phase is never completed by a future simulation.
- Mode integration splits actual powered durations, tariff and demand/solar
  interval boundaries. It does not consume household energy simultaneously with
  a charge command or silently clamp over-capacity energy in graph integration.
  Executable windows and non-executable outlook use matching phase/energy at
  their seam. Their horizons need not be equal.
- `battery_margin.py`: migrate previously reserve-partitioned stock by promoting
  actual reserve volume into retained FIFO lots, not constructing free stock;
  preserve realized credit/debt. Initial stored-energy cost/export protection
  and independently computed imported-energy margin remain enforced.
- Completion tolerance is 0.005 kWh across live mode and source guards. The RED
  test with SOC 70.07%, a chosen 70% boundary and cheap active refill exposed a
  real loosened-tolerance/live-latch bug; the fix checks both emitted strategy
  and persistent observed phase. A separate RED test caught opportunistic solar
  at a completed reserve boundary bypassing the incomplete-refill latch: 7 kWh
  was incorrectly allowed to discharge before the 10 kWh maximum. The solar path
  now sets the same simulated fill latch and uses the same 0.005 kWh boundary.

## Regression classification — not blanket relaxation

- `test_battery_partial_cycle_policy.py`: assumptions that a partial charge can
  immediately discharge or a partial discharge can immediately refill are
  superseded. Replacement assertions independently integrate modes, include idle
  gaps, and require the fixed boundary/full charge before reversing.
- `test_battery_cycle_safety.py`: legacy partial stock is not evidence of a
  completed full cycle. Until maximum is observed/reached, wait/refill rather
  than declaring a live discharge. Persistent observed phase is checked.
- `test_battery_full_cycles.py`: expecting *every* charge source cluster to be
  independently full conflicts with a charging phase spanning multiple clusters.
  The independent full-plan trace now checks reversals across those pauses.
  Explicit pre-committed reserve regression remains a reserve cycle; another
  case independently verifies preference for normal minimum when feasible.
- `test_battery_daytime_completion.py`, `test_battery_tomorrow_cycle.py`,
  `test_battery_solar_topup_regression.py`: old exact source timing or forced
  turnover depended on a now-forbidden early reversal/partial fill. Keep physical
  bounds and independent transition checks. A pause is accepted only as waiting;
  it is not used as proof of profitability. Known export prices remain positive
  in the deterministic tomorrow fixture. The November recorded fixture starts
  at 55%, already below its configured 60% reserve, so starting a new charge is
  permitted and it must reach maximum before discharge.
- `test_battery_energy_accounting.py`: replace old 1.75/2.25 kWh assumptions that
  included simultaneous household discharge while charging with independent
  integration of accepted commands. Feedback checks every executed window
  inside an update, not just its first strategy label. A 15-minute update can
  finish charging and start discharge; asserting its *starting* energy is full
  would be an invalid test. Long feedback reaches both maximum and normal
  minimum for its selected feasible complete cycle.
- `test_battery_partial_cycle_economics.py`: the original 60% SOC / 40% reserve /
  EUR 0.15 margin fixture no longer offers a complete profitable cycle. It is
  explicitly retained as a no-command/no-spend/constant-energy regression,
  not silently changed into a vacuous margin assertion. Positive turnover is
  separately proved on a recorded September 19 fixture with 20% SOC, 20%
  reserve, EUR 0.08 configured minimum, including 114 feedback updates both
  uninterrupted and with JSON restart, FIFO imported-energy economics, actual
  demand and tariffs. Initial conditions/threshold differ and are not claimed
  as economic proof for the old fixture. Broad import-margin cases still check
  costs and independently integrated physical limits; an incomplete fill may
  remain inventory but may not claim profitable discharge.
- Other explicitly conflicting fixtures: `charge_window_stability` now measures
  above (65%), not exactly at (60%), its committed completion boundary;
  `evening_peak_regression` waits during an incomplete solar fill while preserving
  preview non-mutation; `existing_energy` rejects premature refill; `export_transition`
  permits profitable completion at normal minimum rather than forcing reserve;
  `full_profit_reserve` and `solar_preview` retain unfinished imports as inventory,
  not claimed sales; `late_topup` permits grid supplementation to maximum and
  independently reaches normal minimum in a feasible full cycle; `morning_value`
  allows retained unfinished fill instead of an exact old end-SOC; `reserve_stock`
  verifies full-depth preference and FIFO realized-prefix margin, retaining four
  unsold kWh; `unavoidable_remainder` below reserve enters a charging phase.
  Unit expectations in `test_battery_planner.py` reject early future refills and
  premature discharge while retaining no-loss export and physical energy bounds.
- Dedicated suites: `test_battery_bound_cycle_policy.py`,
  `test_battery_bound_cycle_integration.py`,
  `test_battery_bound_stock_migration.py`, and
  `test_battery_forecast_interval_bounds.py`. Full coordinator replay covers
  full vs configured **70%** reserve, forecast deterioration/improvement without
  boundary reselection, repeated refresh, JSON restore, observed-vs-simulated
  phase, executable/outlook continuity and preservation of acquisition debt.
  `bound_cycle_assertions.py` is a separate physical integration oracle.

## Actual execution

- Full command: `python -m unittest discover -s tests -p 'test_*.py'`.
- Latest complete verification after the independent-review downtime fix:
  **372 tests, OK, 299.651 s**, including the retained original partial-only
  economic fixture, opportunistic-solar incomplete-refill regression, and two
  new long-downtime coordinator regressions. Focused bound-policy/integration/
  stock-migration plus legacy stale-state test: **20 tests, OK, 0.327 s**.
  Raw logs: `previews/bound-cycles/downtime-{red,green,full-suite}.log`.
- RED reproduced the review blocker: both new tests failed (nine subtests at
  31 minutes, one day and seven days) because the selected boundary was lost.
  GREEN restores a valid explicit committed boundary and its observed phase
  together without age expiry. Full (20%) and configured reserve (70%) discharge
  remain committed above their boundary despite cheap refill forecasts; an
  unfinished charge at 90% cannot discharge. Actual measured boundary/full SOC
  still advances phase safely through normal planning. Legacy snapshots without
  an explicit committed boundary retain their existing 30-minute stale-state
  behavior, verified by the unchanged legacy regression. A valid saved timestamp
  is still required; this change does not repair malformed persistence.
- The earlier 370-test/17-test results preceded this review fix and are superseded
  by the results above. No claim is made that the three plotted preview cases
  themselves demonstrate downtime; that evidence comes from the coordinator
  regressions. Preview generation was rerun successfully after the fix.
- Source-error messages for intentionally missing price/demand sensors are
  exercised test output, not failures. No install/network fallback was used.
- `PYTHONPATH=tests python scripts/preview_bound_cycles.py`: succeeds; asserts
  independent cycle transitions, min/max, seam timestamps and imported margin;
  generated SVG is XML-parsed. No frontend source was changed. Browser screenshot
  or user visual approval has **not** been performed.
- `git diff --check`: passed at final handoff. `py_compile` passed for coordinator,
  margin accounting and the new preview generator.

## Computed standalone preview

`previews/bound-cycles/index.html` is self-contained: inline SVG/CSS, no external
assets, JS, server or network needed. Do not overwrite older preview directories.
Adjacent snapshot/plot-data/evidence JSON files retain computed inputs and output.

| Case | Chosen boundary | Independently integrated SOC | Imported-energy result |
| --- | --- | --- | --- |
| synthetic complete cycle | 20% | 20–100% | 8 kWh imported/sold; EUR 0.70/kWh margin |
| synthetic reserve cycle | 70% | 70–100% | no grid turnover; starts full, stops at reserve |
| recorded November fixture | 60% | 55–100% | 1.774 kWh imported/sold; EUR 0.1421956032/kWh margin |

The first two are clearly labelled **synthetic deterministic controls**, not live
measurements. The third is the recorded fixture, not a new live reading. Grey
pauses and translucent outlook distinguish waiting and non-executable forecasts.
Graph data is independently integrated without cosmetic clipping. Starting SOC
below reserve is preserved, not falsely drawn at reserve.

## Remaining limits

Finite forecasts cannot guarantee later real-world fulfillment/profit; a committed
cycle can wait through an adverse update without loss-making export. 0.005 kWh
completion tolerance is a local numerical tolerance, not sensor calibration.
No live timing, physical device/control behavior, performance under actual HA load,
or frontend browser appearance is certified by these offline tests. The full
suite takes approximately five minutes locally. Independent review and graph
approval must precede any publication or deployment.
