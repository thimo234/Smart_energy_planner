# Smart Energy Planner - Project Overzicht

## Project Informatie
- **Repository:** https://github.com/thimo234/Smart_energy_planner
- **Type:** Home Assistant Custom Component
- **Doel:** Batterij planning en energie-optimalisatie
- **Taal:** Python (Home Assistant Integration)
- **Update interval:** 15 minuten

## GitHub Configuratie
- **Gebruiker:** thimo234
- **Authenticatie:** Personal Access Token (ghp_tW...)
- **Git Protocol:** HTTPS
- **Branch:** main

## Hoe de Planning Wordt Gekregen

### Data Bronnen (Home Assistant Sensors)
1. **Prijs sensor** - Stroomprijzen (Nordpool/Elia)
2. **Export prijs sensor** - Terugleverprijs
3. **Solcast sensors** - Zonne-energie voorspelling (vandaag + morgen)
4. **Batterij SOC sensor** - Huidige batterij status (SoC %)
5. **Totaal energie sensor** - Huisverbruik (optioneel)

### Data Stroom
```
Home Assistant Sensors
        ↓
Coordinator (_async_update_data)
        ↓
┌─────────────────────────────────────────────────┐
│ 1. Prijs data ophalen & verwerken               │
│    - extract_price_windows()                    │
│    - select_contiguous_price_window()           │
│    - price_peaks.py (pieken detecteren)         │
├─────────────────────────────────────────────────┤
│ 2. Zonne-energie voorspelling                   │
│    - Solcast sensors                            │
│    - build_hourly_solar_forecast()              │
├─────────────────────────────────────────────────┤
│ 3. Huisverbruik voorspelling                    │
│    - Historisch verbruik (7 dagen)              │
│    - build_hourly_home_demand_forecast()        │
│    - align_price_responsive_demand()            │
├─────────────────────────────────────────────────┤
│ 4. Batterij status                              │
│    - SOC %                                      │
│    - Beschikbare capaciteit                     │
│    - Min SOC voor ontladen                      │
├─────────────────────────────────────────────────┤
│ 5. Energie balans berekenen                     │
│    - build_energy_balance_slots()               │
│    - Solar vs Verbruik vs Prijs                 │
├─────────────────────────────────────────────────┤
│ 6. Batterij planning                            │
│    - build_battery_mode_schedule()              │
│    - battery_profit.py (winst berekening)       │
│    - 4 modi: accu_uit, laden_van_net,           │
│      laden_met_zonne_energie, ontladen          │
└─────────────────────────────────────────────────┘
        ↓
PlannerResult (dataclass)
        ↓
Sensors (sensor.py)
  - sensor.battery_strategy
  - sensor.battery_profit_total_eur
  - sensor.estimated_home_demand_today
```

### Planning Modi
| Modus | Beschrijving |
|-------|-------------|
| `accu_uit` | Batterij inactief |
| `laden_van_net` | Laden van het net (goedkope prijzen) |
| `laden_met_zonne_energie` | Laden met zonne-energie |
| `ontladen` | Ontladen bij hoog verbruik/geen zon |
| `ontladen_naar_net` | Terugleveren aan het net |

### Belangrijke Configuratie Opties
- `battery_enabled` - Batterij planning aan/uit
- `battery_capacity_kwh` - Batterij capaciteit (default: 10.0)
- `battery_min_soc_percent` - Min SOC voor ontladen (default: 20%)
- `battery_no_charge_min_soc_percent` - Min SOC om NIET te laden (default: 20%)
- `battery_min_profit_per_kwh` - Min winst per kWh om te ontladen (default: 0.08)
- `battery_max_charge_kw` - Max laadsnelheid (default: 2.5)
- `battery_max_discharge_kw` - Max ontlaadsnelheid (default: 2.5)
- `battery_charge_safety_margin` - Laad veiligheidsmarge (default: 20%)
- `battery_demand_safety_margin` - Vraag veiligheidsmarge (default: 20%)
- `price_window_duration_hours` - Duur prijsvenster (default: 3)
- `price_window_type` - Prijsvenster type: `cheapest` of `most_expensive`

## Bestandsstructuur

### Kernbestanden
| Bestand | Regels | Beschrijving |
|---------|--------|-------------|
| `coordinator.py` | 4280 | Hoofdlogica: data ophalen, planning berekenen, state beheren |
| `battery_forecast.py` | 955 | Verbruik en zonne-energie voorspelling |
| `battery_planner.py` | 714 | Batterij planning algoritme (4 modi) |
| `battery_profit.py` | 64 | Winstberekening ontladen |
| `sensor.py` | 462 | Home Assistant sensor entiteiten |
| `config_flow.py` | 619 | Configuratie wizard |
| `price_helpers.py` | 351 | Prijsverwerking en venster selectie |
| `price_peaks.py` | 290 | Prijs pieken detecteren |
| `demand_forecast.py` | 280 | Verbruiksvoorspelling coordinator |
| `demand_coordinator.py` | 80 | Verbruiksdata coordinator |
| `const.py` | 70 | Constanten en defaults |
| `planner_result.py` | 92 | Resultaat dataclass |
| `binary_sensor.py` | 192 | Binary sensor entiteiten |
| `battery_models.py` | 15 | Data modellen (SolarWindow) |
| `price_models.py` | 14 | Prijs modellen (PlannerWindow) |

### Ondersteunende Bestanden
- `__init__.py` (282 regels) - Component setup
- `frontend/smart-energy-planner-card.js` - UI card
- `frontend_resource.py` (82 regels) - Frontend resources
- `brand/` - Logo's en iconen
- `translations/` - Meertalige strings (en/nl)
- `manifest.json` - Component manifest

### Scripts
- `scripts/visualize_battery.py` - Batterij visualisatie
- `scripts/analyze_demand_history.py` - Verbruik analyse
- `scripts/preview_demand_routines.py` - Verbruik routines preview
- `scripts/templates/battery_chart.html` - Batterij grafiek template

### Tests
- **48 testbestanden** in `tests/`
- Testen voor batterij planning, prijs pieken, verbruik voorspelling, etc.
- Test fixtures met realistische batterij data

## Planning Algoritme Detail

### Stap 1: Prijs Data Verwerken
- Haalt `raw_today` en `raw_tomorrow` op uit prijs sensor
- Converteert naar `PlannerWindow` objecten
- Aggregateert naar uurlijkse vensters (indien nodig)
- Detecteert prijs pieken met prominensie filtering

### Stap 2: Zonne-energie Voorspelling
- Haalt Solcast data op (vandaag + morgen)
- Bouwt 168-uurs (1 week) zonne-voorspelling
- Berekenet beschikbare ruimte in batterij

### Stap 3: Verbruik Voorspelling
- Gebruikt 7 dagen historische data
- Bouwt 168-uurs verbruiksprofiel per uur van de dag
- Past dag/nacht en weekend/werkdag aanpassingen toe
- Identificeert flexibele vs vaste vraag

### Stap 4: Energie Balans
- Per uur: Solar - Verbruik = Netto balans
- Positief = overschot (laad batterij)
- Negatief = tekort (ontlaad batterij of van net laden)

### Stap 5: Batterij Planning
- **Laden met zon:** Als er zonne-overschot is en batterij heeft ruimte
- **Laden van net:** Als prijzen laag zijn en batterij ruimte heeft
- **Ontladen:** Als prijzen hoog zijn of verbruik hoog zonder zon
- **Accu uit:** Als geen van bovenstaande geldt

### Winst Berekening
- Vergelijkt laadprijs met ontladeprijs
- Houdt rekening met verlies (round-trip efficiency)
- Minimale winstdrempel: 0.08 EUR/kWh (configurable)
- Houdt reserve aan voor onvoorspelbaar verbruik

## Huidige Planning (vandaag)
- **SOC:** 42% (2.2 kWh beschikbaar, 4.2 kWh totaal)
- **Min SOC ontladen:** 60%
- **Strategie:** `laden_met_zonne_energie`
- **Planning:**
  - Nu - 10:42: `accu_uit`
  - 10:42 - 14:30: `laden_met_zonne_energie` (Zon)
  - 14:30 - 16:00: `accu_uit`
  - 16:00 - 21:05: `ontladen` (Piekprijs €0.579/kWh)

## Bekende Issues
1. **total_energy_sensor** niet geconfigureerd - beïnvloedt zelfverbruik berekening
2. **Demand forecast** afhankelijk van historische data (minimaal 7 dagen nodig)
3. **Solcast** afhankelijk van externe API beschikbaarheid

## Toekomstige Verbeteringen
- [ ] Nachtelijke planning validatie (automatisch)
- [ ] Planning feedback loop (planning vs werkelijkheid)
- [ ] Extra visualisaties
- [ ] Meer test coverage
- [ ] Ondersteuning voor meerdere batterijen
