#!/usr/bin/env python3
"""
Smart Energy Planner - Nachtelijke Planning Validatie

Controleert de batterij planning op logica en optimaliteit.
Wordt uitgevoerd door een Home Assistant automation elke nacht.

Gebruik:
  python3 validate_planning.py --config /config/smart_energy_planner_config.json
"""

import json
import sys
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Any


@dataclass
class PlanningIssue:
    """Een gevonden probleem in de planning."""
    severity: str  # "error", "warning", "info"
    category: str  # Categorisering van het probleem
    message: str
    suggestion: str = ""
    affected_windows: list[dict] = field(default_factory=list)


@dataclass
class PlanningValidation:
    """Resultaat van de planning validatie."""
    timestamp: str
    issues: list[PlanningIssue] = field(default_factory=list)
    is_valid: bool = True
    summary: str = ""

    def add_issue(self, issue: PlanningIssue):
        self.issues.append(issue)
        if issue.severity == "error":
            self.is_valid = False

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "is_valid": self.is_valid,
            "issue_count": len(self.issues),
            "issues": [
                {
                    "severity": i.severity,
                    "category": i.category,
                    "message": i.message,
                    "suggestion": i.suggestion,
                    "affected_windows": i.affected_windows,
                }
                for i in self.issues
            ],
            "summary": self.summary,
        }


def validate_planning(planner_data: dict[str, Any]) -> PlanningValidation:
    """Valideer de batterij planning op logica en consistentie."""
    now = datetime.now()
    validation = PlanningValidation(timestamp=now.isoformat())

    # Haal planning data op
    planned_windows = planner_data.get("planned_battery_mode_windows", [])
    planned_schedule = planner_data.get("planned_battery_mode_schedule", [])
    battery_soc = planner_data.get("battery_soc_percent", 0)
    battery_total = planner_data.get("battery_total_energy_kwh", 0)
    battery_available = planner_data.get("battery_energy_available_kwh", 0)
    battery_min_soc = planner_data.get("battery_min_soc_percent", 20)
    price_windows = planner_data.get("upcoming_energy_price_windows", [])

    # 1. Controleer of er windows zijn
    if not planned_windows:
        validation.add_issue(PlanningIssue(
            severity="warning",
            category="missing_data",
            message="Geen planning windows gevonden",
            suggestion="Controleer of alle sensoren beschikbaar zijn",
        ))
        return validation

    # 2. Controleer SOC tegen minimum
    if battery_soc <= battery_min_soc:
        # Zoek laad windows
        charge_windows = [w for w in planned_windows
                         if w.get("mode") in ("laden_van_net", "laden_met_zonne_energie")]
        if not charge_windows:
            validation.add_issue(PlanningIssue(
                severity="error",
                category="soc_violation",
                message=f"Batterij SOC ({battery_soc}%) is onder minimum ({battery_min_soc}%) "
                       f"zonder laad planning",
                suggestion="Voeg laad window toe of verlaag battery_min_soc_percent",
                affected_windows=planned_windows[:3],
            ))

    # 3. Simuleer batterij status door de planning heen
    #    Dit houdt rekening met laden EN ontladen op verschillende tijdstippen
    discharge_windows = [w for w in planned_windows
                        if w.get("mode") == "ontladen"]
    charge_windows = [w for w in planned_windows
                     if w.get("mode") in ("laden_van_net", "laden_met_zonne_energie")]

    # Sorteer alle windows op start tijd
    all_windows = sorted(planned_windows, key=lambda w: w.get("start", ""))

    # Simuleer batterij SOC door de planning
    current_kwh = battery_total * (battery_soc / 100)
    simulation_results = []
    battery_min_kwh = battery_total * (battery_min_soc / 100)

    for w in all_windows:
        mode = w.get("mode")
        start = w.get("start")
        end = w.get("end")
        if not start or not end:
            continue
        try:
            s = datetime.fromisoformat(start)
            e = datetime.fromisoformat(end)
            hours = (e - s).total_seconds() / 3600
        except (ValueError, TypeError):
            continue

        if mode == "ontladen":
            # Ontlaad met max discharge rate
            discharge_kwh = min(hours * 2.5, current_kwh - battery_min_kwh, current_kwh)
            current_kwh -= discharge_kwh
            simulation_results.append({
                "mode": mode,
                "start": start,
                "end": end,
                "kwh_change": -discharge_kwh,
                "soc_after": (current_kwh / battery_total * 100) if battery_total > 0 else 0,
            })
            if current_kwh < battery_min_kwh:
                validation.add_issue(PlanningIssue(
                    severity="error",
                    category="soc_violation_during_discharge",
                    message=f"Batterij gaat onder minimum SOC tijdens ontladen "
                           f"({current_kwh/battery_total*100:.1f}% < {battery_min_soc}%)",
                    suggestion="Verhoog battery_min_soc_percent of verlaag discharge planning",
                    affected_windows=[w],
                ))
        elif mode in ("laden_van_net", "laden_met_zonne_energie"):
            # Laad met max charge rate
            charge_kwh = min(hours * 2.5, battery_total - current_kwh)
            current_kwh += charge_kwh
            simulation_results.append({
                "mode": mode,
                "start": start,
                "end": end,
                "kwh_change": charge_kwh,
                "soc_after": (current_kwh / battery_total * 100) if battery_total > 0 else 0,
            })

    # Controleer of ontladen alleen gebeurt als er energie is (vereiste versie)
    if discharge_windows:
        total_discharge_kwh = sum(
            abs(r["kwh_change"]) for r in simulation_results
            if r["mode"] == "ontladen"
        )
        max_dischargeable = battery_total * (1 - battery_min_soc / 100)
        if total_discharge_kwh > max_dischargeable * 1.2:  # 20% marge
            validation.add_issue(PlanningIssue(
                severity="warning",
                category="over_discharge",
                message=f"Totale geplande ontlading ({total_discharge_kwh:.1f} kWh) "
                       f"exceedt beschikbare energie na laden ({max_dischargeable:.1f} kWh)",
                suggestion="Verhoog battery_min_soc_percent of verlaag discharge planning",
                affected_windows=discharge_windows[:3],
            ))

    # 4. Controleer prijs-gedreven ontlading
    if price_windows and discharge_windows:
        high_price_windows = [w for w in price_windows
                             if w.get("average_price", 0) > 0.50]
        if high_price_windows and not discharge_windows:
            validation.add_issue(PlanningIssue(
                severity="info",
                category="missed_opportunity",
                message=f"{len(high_price_windows)} hoge prijs perioden gevonden "
                       f"zonder ontlading planning",
                suggestion="Controleer battery_min_profit_per_kwh instelling",
                affected_windows=high_price_windows[:3],
            ))

    # 5. Controleer zonne-energie laden
    solar_windows = [w for w in planned_windows
                    if w.get("mode") == "laden_met_zonne_energie"]
    if solar_windows:
        # Check of er genoeg zonne-overschot is
        solar_forecast = planner_data.get("estimated_hourly_solar_forecast", [])
        if solar_forecast:
            total_solar = sum(w.get("estimated_kwh", 0) for w in solar_forecast
                            if w.get("estimated_kwh", 0) > 0)
            total_charge_in_solar = 0
            for w in solar_windows:
                start = w.get("start")
                end = w.get("end")
                if start and end:
                    try:
                        s = datetime.fromisoformat(start)
                        e = datetime.fromisoformat(end)
                        hours = (e - s).total_seconds() / 3600
                        total_charge_in_solar += hours * 2.5
                    except (ValueError, TypeError):
                        pass

            if total_charge_in_solar > total_solar * 1.5:
                validation.add_issue(PlanningIssue(
                    severity="warning",
                    category="solar_mismatch",
                    message=f"Zon laad planning ({total_charge_in_solar:.1f} kWh) "
                           f"overschrijdt voorspelling ({total_solar:.1f} kWh)",
                    suggestion="Controleer Solcast sensors of laad snelheid",
                    affected_windows=solar_windows[:3],
                ))

    # 6. Controleer planning continuïteit
    if planned_schedule:
        for i in range(1, len(planned_schedule)):
            prev = planned_schedule[i - 1]
            curr = planned_schedule[i]
            if prev.get("at") and curr.get("at"):
                try:
                    prev_time = datetime.fromisoformat(prev["at"])
                    curr_time = datetime.fromisoformat(curr["at"])
                    if curr_time < prev_time:
                        validation.add_issue(PlanningIssue(
                            severity="error",
                            category="timeline_error",
                            message=f"Planning tijdlijn fout: {curr['at']} "
                                   f"is vóór {prev['at']}",
                            suggestion="Controleer coordinator.py planning logica",
                        ))
                except (ValueError, TypeError):
                    pass

    # 7. Controleer of er geen overlapping windows zijn
    for i in range(len(planned_windows)):
        for j in range(i + 1, len(planned_windows)):
            w1 = planned_windows[i]
            w2 = planned_windows[j]
            start1 = w1.get("start")
            end1 = w1.get("end")
            start2 = w2.get("start")
            end2 = w2.get("end")
            if all([start1, end1, start2, end2]):
                try:
                    s1 = datetime.fromisoformat(start1)
                    e1 = datetime.fromisoformat(end1)
                    s2 = datetime.fromisoformat(start2)
                    e2 = datetime.fromisoformat(end2)
                    # Check overlapping
                    if s1 < e2 and s2 < e1:
                        validation.add_issue(PlanningIssue(
                            severity="error",
                            category="overlapping_windows",
                            message=f"Overlapping windows: {start1}-{end1} "
                                   f"en {start2}-{end2}",
                            suggestion="Controleer merge_windows() in battery_planner.py",
                            affected_windows=[w1, w2],
                        ))
                except (ValueError, TypeError):
                    pass

    # Samenvatting
    error_count = sum(1 for i in validation.issues if i.severity == "error")
    warning_count = sum(1 for i in validation.issues if i.severity == "warning")
    info_count = sum(1 for i in validation.issues if i.severity == "info")

    validation.summary = (
        f"Planning validatie voltooid: {error_count} errors, "
        f"{warning_count} warnings, {info_count} info"
    )

    return validation


def load_planner_data(config_path: str) -> dict:
    """Laad planner data vanuit een JSON bestand (geexporteerd uit HA)."""
    try:
        with open(config_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Fout: Bestand {config_path} niet gevonden")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Fout: Ongeldige JSON in {config_path}: {e}")
        sys.exit(1)


def main():
    """Hoofd functie."""
    import argparse

    parser = argparse.ArgumentParser(description="Smart Energy Planner Validatie")
    parser.add_argument("--config", required=True, help="Pad naar planner config JSON")
    parser.add_argument("--output", help="Pad naar output JSON (optioneel)")
    parser.add_argument("--verbose", action="store_true", help="Toon gedetailleerde output")

    args = parser.parse_args()

    # Laad data
    planner_data = load_planner_data(args.config)

    # Valideer
    result = validate_planning(planner_data)

    # Output
    result_dict = result.to_dict()

    if args.verbose:
        print(json.dumps(result_dict, indent=2, ensure_ascii=False))
    else:
        print(result.summary)
        if result.issues:
            print(f"\nGevonden issues:")
            for issue in result.issues:
                print(f"  [{issue.severity.upper()}] {issue.message}")
                if issue.suggestion:
                    print(f"    → {issue.suggestion}")

    # Schrijf output als dat is opgegeven
    if args.output:
        with open(args.output, "w") as f:
            json.dump(result_dict, f, indent=2, ensure_ascii=False)
        print(f"\nOutput geschreven naar {args.output}")

    # Return code
    sys.exit(0 if result.is_valid else 1)


if __name__ == "__main__":
    main()
