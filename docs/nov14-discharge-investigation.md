# Onderzoek ontbrekend ontlaadvenster, 14 november 2026

## Vastgesteld

Live uitlezing van `sensor.battery_strategy`, vastgelegd in
`tests/fixtures/battery_2026_11_14_missing_discharge.json`, toont 88% SOC,
10 kWh capaciteit, 60% no-charge-reserve, een actieve ontlaadcyclus en
uitsluitend `accu_uit`-vensters. De sensor vermeldt 2,8 kWh boven de reserve.
Dit is een echte sensoropname, geen verzonnen planneruitvoer.

Een actuele read-only entity-registrycontrole bevestigde dat alle drie
accuentiteiten al aan de huishoudentry en dezelfde accu-subentry zijn gekoppeld.
De oude unieke planneridentiteit bleef behouden. Het aparte huishoudapparaat is
het bovenliggende apparaat, niet op zichzelf bewijs van een dubbele accu.

## Grenzen van de reproductie

`test_battery_nov14_live_replay.py` reconstrueert prijzen en prognoses uit de
opname. Het gebruikt expliciete aannames voor niet-gepubliceerde instellingen
zoals laad-/ontlaadvermogen en begint met een lege kostenadministratie.
Onder die aannames plant de bestaande planner wel ontladen richting 60%.
Ook het variëren van de cyclusgrens alleen reproduceert de live all-idle-planning
niet. Dit bewijst NIET dat de live planner goed werkt en verklaart het verschil
nog niet. Een schone kostenadministratie is geen reconstructie van de echte
opgeslagen energieloten/kostprijs. De winstregels zijn daarom niet aangepast.

De prijs-/SOC-visualisatie van de opname is lokaal gegenereerd met het bestaande
`visualize_battery.py`; dit is een viewer, geen nieuwe plannerberekening. De
opgegeven vermogens (2,5/3 kW) zijn replay-aannames, niet live geverifieerd.
Gegenereerde previews en lokale testlogs worden niet gecommit.

## Diagnostiek voor de volgende stap

De nieuwe officiële HA-hook `async_get_config_entry_diagnostics` werkt voor een
huishoudentry en een zelfstandige planner. Zij exporteert alleen een whitelist
van effectieve planninginstellingen, actuele planning, runtime-cyclusvlaggen en
kostprijsadministratie. De stabiele legacy-identiteit wordt gebruikt om de echte
runtime-opslag te vinden. Geen refresh, besturing of opslagwrite wordt uitgevoerd;
entiteitsnamen, titels en willekeurige configuratievelden worden niet gedumpt.
De export bevat wel planning-/verbruiksgegevens en interne entry-identiteiten;
behandel het bestand als privédiagnostiek.

Na afzonderlijk akkoord voor installatie en herladen moet op de doelinstallatie
via de integratiepagina diagnostiek worden gedownload, terwijl het probleem nog
zichtbaar is. Vergelijk effectieve configuratie, coordinator- en runtime-cyclusgrens,
`battery_grid_charge_price` en `battery_margin_state` met de sensoropname. Maak
vervolgens een stateful replay voordat een beleidswijziging wordt geaccepteerd.
Er is nog geen bewezen reparatie voor het ontbrekende live ontlaadvenster.

## Verificatie van deze codewijziging

De volledige offline suite is geslaagd: 440 tests, 299,146 seconden. Deze bevat
onder andere menu-/overname-/lifecycle-regressies, acht interruption-subcases en
negen diagnostiektests. De overnameregressie is daarnaast uitgevoerd met de
AST-geëxtraheerde officiële HA 2026.11.1 entity-registryvalidatie en cleanup.
Dat is geen volledige draaiende HA-integratietest. Echte menuweergave,
archiefverwijdering en de diagnostiekdownload op de doelinstallatie moeten nog
worden gecontroleerd na goedgekeurde installatie.
