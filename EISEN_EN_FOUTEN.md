# Smart Energy Planner - Eisen en Fouten Log

## Eisen van de Gebruiker

### 2026-11-06
- **[EIS]** Planning elke nacht controleren op logica
- **[EIS]** Als planning niet logisch is: code aanpassen voor betere planning
- **[EIS]** Onthouden hoe planning data wordt opgehaald (Home Assistant API → battery_strategy sensor)
- **[EIS]** Altijd werken aan dit project bij "energy planner" referenties
- **[EIS]** GitHub configuratie onthouden (thimo234, PAT token)
- **[EIS]** Project specifiek geheugen bijhouden

### Bekende Issues
- **total_energy_sensor** niet geconfigureerd in Home Assistant
  - Impact: Zelfverbruik berekening is minder accuraat
  - Oplossing: Configureer `sensor.totaal_zelf_gebruikte_energie` in Home Assistant

## Fouten Log

### 2026-11-06
- **GitHub Auth:** Device flow werkte niet in headless omgeving
  - Oplossing: Personal Access Token gebruikt
- **gh CLI install:** apt-get mislukt (geen sudo)
  - Oplossing: Handmatige installatie naar ~/bin/gh

## Geplande Acties
- [ ] Nachtelijke planning validatie script
- [ ] Planning vs werkelijkheid vergelijking
- [ ] Automatische code aanpassingen bij suboptimale planning
