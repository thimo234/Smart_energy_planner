# Partial-cycle policy — repaired local candidate / NOT RELEASED

## Outcome

The corrected candidate passes the complete offline unittest suite: **352 tests, zero failures/errors, 255.699 seconds**; **59 focused tests pass in 84.017 seconds**. The independent-review realized-grid-margin blocker is reproduced and corrected, including 114 sequential feedback updates and JSON runtime restart. Base remains `e699535`. There was no commit, push, Home Assistant deployment, live API access, dependency installation or download. The preview still needs user review; a passing offline suite is not a claim about the live installation.

## Reproducible verification

```sh
python -m unittest discover -s tests -p 'test_*.py'
PYTHONPATH=tests python -m unittest test_battery_partial_cycle_economics test_battery_complete_cycle_policy test_battery_energy_accounting test_battery_average_profit test_battery_solar_topup_regression test_battery_morning_value
PYTHONPATH=tests python scripts/preview_partial_cycles.py
```

- Full-suite exit code: **0**, final line **OK**. Current output is `previews/partial-cycles/unittest.log`, copied from `/opt/data/cache/scratch/grid-margin-full-final.log`. Focused output is `/opt/data/cache/scratch/grid-margin-focused-final.log`; independent replay metrics are `/opt/data/cache/scratch/grid-margin-metrics.json`. `git diff --check` also passes.
- Expected missing-sensor/forecast warnings are produced by negative-path fixtures. They are not live Home Assistant errors.
- Earlier `partial-policy-*.log` files are preserved as historical evidence, **not current results**. The prior candidate had 346 tests and 96 failure entries, including subtests. Its physical and economic failures were real repair work, not all obsolete expectations.
- The earlier repair added intratariff forecast-boundary preservation and independent accounting of emitted partial-cycle imports. Four additional review regressions now cover the exact single-plan economics, 114 actual-command/SOC feedback updates, the same sequence with a JSON runtime restart, and protection against borrowing margin from an unreachable FIFO tail beyond the current command's demand budget.

## Repair and safety boundaries

1. **Physical accounting:** split tariff intervals at demand and solar forecast boundaries before scheduling. Previously an averaged demand rate could appear safe internally while independent integration crossed the real floor. `test_demand_integration` remains enforced; no physical tolerance was relaxed. The new forecast-interval regression preserves differing half-hour demand inside a one-hour tariff.
2. **Reserve:** retain the configured higher reserve through all selected partial fills. A tiny solar window is not permission to spend reserve. Discharge availability is bounded by actual measured stock minus reserve, never by inventing stock when initial SOC is below reserve.
3. **Source economics:** project actual carried stock and selected PV into future purchasing decisions. Retained reserve is stock at cost, not a fictional profitable sale. A just-selected cheap fill is not consumed at its purchase price in the projection merely to justify a more expensive later solar replacement. Negative-price and cheaper-grid source-priority regressions remain exercised.
4. **Partial grid admission:** remove the full-refill requirement, but retain the configured grid supplement margin. Limit the admitted supplement to realizable profitable demand plus physically retained reserve deficit. Simultaneous PV and pre-existing stock compete for demand; they do not subsidize a losing grid supplement. A stale active-cycle latch cannot bypass the profit threshold. Export acquisition-cost protections remain in place.
5. **Partial direction changes:** historical full-charge/discharge latches no longer independently block a new economically valid direction. Repeated updates, restart persistence, restored latch combinations, isolated/outlook planning and independent energy traces are covered by the full suite.
6. **Solar top-ups:** preserve physically possible partial surplus after a discharge without discarding higher-value demand on the other side of that top-up. A top-up does not create a new price horizon that forgets the later evening peak.

Changes are concentrated in `custom_components/smart_energy_planner/{coordinator,battery_planner,battery_forecast,battery_margin}.py`. Unrelated existing files and historical logs were preserved.

## Meaningful migration of historical strict-cycle assertions

Legacy expectations of exactly 100% before discharge, exactly the physical floor before refilling, or a mandatory complete fill have been replaced with actual emitted-energy, higher-reserve, source-price and independent-margin checks. Tests were not skipped or deleted to obtain a green suite.

Some formerly rejected incomplete fills are now legitimately profitable partial fills. For example, the low-profit replay admits a partial purchase at an EUR 0.08/kWh threshold but still rejects it at EUR 0.10/kWh. Tests explicitly cover both outcomes. A profitable partial surplus export is checked against acquisition cost rather than forbidden solely because the old cycle was incomplete.

The independent `imported_margin` test ledger tracks chronological FIFO stock, excludes PV/pre-existing stock from purchased-energy profits, holds reserve physically, includes actual home and export sales, and rejects unfunded discharge. This replaces obsolete post-last-refill-only averages that ignored valid sales between partial fills. Existing tighter source-priority and export tests still run.

## Actual computed standalone preview

- Open **`previews/partial-cycles/index.html`** directly; it uses inline SVG and no external assets.
- Source: the existing captured `tests/fixtures/battery_2026_11_12_night_solar.json`, not new live measurements.
- `computed-snapshot.json` is actual coordinator replay output. `plot-data.json` is independently integrated `scripts.visualize_battery.plot_data` output, without cosmetic SOC clamping.
- `economics.json` contains a separately integrated purchased-energy ledger: 1.764 kWh imported/sold, weighted acquisition price €0.321061224/kWh, realized margin **€0.142598073/kWh**. This fixture differs from the review's €0.15 case below.
- Current replay: **5 executable windows + 5 estimated windows**, **103 SOC integration points**, SOC **55.000–99.898%**. Initial SOC is below the fixture's 60% reserve; the trace does not invent energy to reach that reserve. Future discharge remains reserve-bounded.
- Verification performed: generated SVG parses as XML; ten combined windows match the emitted snapshot; every integrated SOC point is within the initial-stock/capacity bounds; HTML has no external asset URLs.
- Browser screenshot/visual approval was **not performed**. The preview remains explicitly marked not released. Offline safety in this fixture does not prove universal optimization or live behavior.

## Independent-review grid-margin correction

The earlier 348-test result did not establish realized profitability. Review found **€0.083370995/kWh** after 114 feedback updates against a configured **€0.15/kWh** minimum, and **€0.079316/kWh** in the single-plan replay. That was a real blocker, even though the baseline also failed. Those results are superseded by the correction below, not dismissed as baseline behavior.

The execution simulator conserves FIFO purchased lots separately from old stock and solar, charging imports at actual slot tariffs. Earlier household consumption can borrow from a later reachable peak, but that margin debt survives every replan. Remaining purchased stock and physically reachable household demand must fund the weighted margin; PV and pre-existing stock cannot subsidize it. Feasibility uses the current command's actual energy budget, not hypothetical sales of all current stock. Future recovery checks cumulative weighted margin across purchased FIFO lots, preserving valid cheaper-lot recovery rather than enforcing an unintended per-lot minimum. Export retains its separate acquisition-price floor. The JSON runtime snapshot preserves observed lots, realized credit/debt and last executable commands; isolated outlook simulation cannot enter execution history. Measured-SOC reconciliation cannot mint positive credit. Command budgets retain measured precision instead of repeatedly rounding energy up at the reserve boundary.

New regressions use `2026_09_18_evening`, starting at `2026-09-18T19:24:56.329382+02:00`, SOC 60%, reserve 40%, minimum profit €0.15/kWh, charge limit 2.5 kW, tax deduction €0.11 and isolated cycles. They independently integrate actual commands into the next SOC at each 15-minute update, rather than feeding back the planner's predicted SOC. The second sequential replay saves/restores JSON runtime state at update 57. Reviewed margin and physical bounds use **1e-6 numerical tolerances**, not the earlier millikWh/millicent allowances.

| Verification | Imports (kWh) | Purchased energy sold (kWh) | Margin (€/kWh) | Minimum physical energy (kWh) |
| --- | ---: | ---: | ---: | ---: |
| Single plan, executable + outlook | 1.875000000 | 0.878946673 | 0.149999999 | — |
| 114 sequential actual-command updates | 0.835882374 | 0.835882353 | 0.149999979 | 4.000000021 |
| Same replay with JSON runtime restart | 0.835882374 | 0.835882353 | 0.149999979 | 4.000000021 |

Sequential acquisition cost is **€0.108875591**, and physical energy stays within **4.000000021–6.000000000 kWh**. Imports and sales are both nonzero: this is not a vacuous “never use the battery” result. Single-plan unsold imports remain stock, not realized sales.

One synthetic pause/refill fixture previously advertised €0.20 proposals while its actual import slots cost €0.60, then expected €0.60 home use to satisfy a €0.10 margin. Its acquisition slots now genuinely cost €0.20; partial-cycle timing and reserve assertions are unchanged. Weighted-average profit, morning-value, partial-solar, source-protection, physical-bound and restart regressions all remain passing. No economics/safety test was skipped or removed.

The preview script now writes the independent ledger, checks SOC bounds and window counts, and parses its inline SVG as XML before reporting success. Generated artifacts and the current full-suite execution log were refreshed from actual execution.

## Publication approval

The user approved the computed preview and explicitly requested publication to main. Publication preflight reran 65 focused tests in this exact worktree: all pass. The fetched main tip still matches base `e699535`. Generated previews and execution logs remain local; this document records their provenance rather than including them in the commit. Home Assistant deployment and restart are not approved by this publication request and have not been performed. Earlier NOT RELEASED / pending-review labels above describe the pre-approval verification stage.

## Reusable workflow / parent coordination

Allow economically justified partial charge/discharge reversals, not arbitrary guard removal. Preserve configured reserve, source ordering, acquisition-cost export protection and realized grid margin. Integrate commands independently at every forecast boundary; do not clamp charts or treat below-reserve initial energy as replenished. Run the complete unittest discovery suite after meaningful legacy-assertion migrations, preserve the exact execution log, and regenerate the preview from actual replay output. The shared regression skill was not modified by this worktree-only subagent.
