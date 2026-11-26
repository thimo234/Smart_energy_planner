# V2-leerproef (alleen offline)

`scripts/replay_battery_learning.py` vergelijkt de huidige vaste wolkenfactor
(0,75) met een factor per lokaal zesuursblok. De alternatieve factor komt
uit het onderste kwartiel van de dagelijkse medianen van gemeten/opgegeven
PV-opbrengst. Alleen voltooide, eerdere meetdagen uit de laatste 28 dagen
worden gebruikt. Een blok met minder dan drie waargenomen dagen houdt 0,75.
Dit is een kandidaat voor validatie, geen productie-instelling.

## Invoer

Draai vanuit de repository: `python scripts/replay_battery_learning.py /pad/naar/metingen.jsonl`.
Eén regel per lokale dag, zonder overlap. Gebruik in alle tijdstempels de
lokale UTC-offset. Bewaar de prognoses, instellingen en planvensters **op
beslismoment**, en voeg werkelijk gemeten energie pas na afloop toe. Gebruik
geen latere Solcast-updates als oorspronkelijke prognose en geen netto
netmeting als vervanging voor gemeten bruto huisverbruik of PV-opbrengst.

Velden per regel:

- `now`: ISO 8601-tijdstempel met UTC-offset.
- `initial` en `capacity`: startenergie en capaciteit boven het absolute
  SOC-minimum, in kWh; `max_charge` en `max_discharge`: kW.
- `slots`: aaneensluitende tijdvakken vanaf `now`, elk met `start`, `end`,
  `solar_kwh`, `demand_kwh`, `import_price`, `export_price`,
  `measured_solar_kwh` en `measured_demand_kwh`. Prijzen zijn €/kWh. Geef de
  originele scorerwaarden `net_solar_kwh`, `solar_transition` en `price_known`
  mee als ze afwijken van respectievelijk zon min vraag, `false` en `true`.
- `baseline`: de eerder vastgelegde en door de beleidslaag toegelaten vensters,
  als gewone kandidaat in de offline vergelijking.
- `candidates`: maximaal acht door dezelfde beleidslaag toegelaten plannen
  in de originele volgorde; elk heeft `id`, `kind` (`solar` of `grid`) en
  `windows`. Kandidaten moeten al gecontroleerd zijn op reserve, winst en
  exporttoestemming. Het offline script kan die toelating niet reconstrueren.
- Een venster heeft `start`, `end` en `mode`, en eventueel `standby`.
  Modi: `laden_met_zonne_energie`, `laden_van_net`, `ontladen`,
  `ontladen_naar_net`, `accu_uit`.

## Uitvoer en beperking

De uitvoer is JSONL met per dag de gekozen plannen, het al dan niet gebruiken
van kalibratie, kosten, netafname en teruglevering, netafname bij het hoogste
tarief van die dag, schakelingen en min/max/eindstand van de accu. Vergelijk
ook de eindstand; lagere kosten door de accu leger te eindigen kunnen misleiden.
Een wolkenscenario verandert alleen de beoordeling, niet de kandidaatgeneratie
of het beleid. De live aanroep houdt de vaste factor en het bestaande tijdsbudget.

Dit is een vergelijking bij één vast beslismoment per dag; het model simuleert
geen latere herplanning, rendement, omvormer of andere hardwarebeperkingen.
Zonder echte, gekoppelde prognoses en metingen zegt de kunstmatige regressie
alleen dat een andere plankeuze mogelijk is. Zij bewijst geen praktijkwinst.
