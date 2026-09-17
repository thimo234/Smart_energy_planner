# Controle accuplanning op de aangeleverde sensordata

## Conclusie

De eerdere code bevatte aantoonbare fouten in de energiebalans en schakeltijden.
Die zijn hersteld. De planner is echter een verzameling beslisregels, geen
optimalisatiemodel dat de goedkoopste totale energierekening berekent.
Ook mijn eerdere conclusie dat 3,2 kWh extra laden op basis van alleen het
prijsverschil rendabel zou zijn, was te stellig.

## Wat de gegevens zeggen

- Actuele prijs: €0,159/kWh; piek vanavond: €0,425/kWh.
- Accu: 68% van 10 kWh = 6,8 kWh; bij een minimum van 20% is 4,8 kWh inzetbaar.
- Vandaag blijft de voorspelde zonneproductie onder het huishoudelijke verbruik.
- Resterende bekende uren met een prijs van minstens €0,239/kWh vragen samen
  ongeveer 4,45 kWh netto uit accu of net. De bestaande 4,8 kWh kan dat al dekken.
- De tarieven na middernacht zijn in de dump `price_known: false`. €0,335/kWh
  is daar een invulprijs, geen gepubliceerd tarief.

Hieruit volgt dat de grote piek wel een gunstig prijsverschil toont, maar geen
bewijs levert voor minstens €0,08 extra winst op iedere nieuw geladen kWh.
Daarvoor moeten bestaande energie, het verbruik per uur, alternatieve
laadmomenten en verliezen gezamenlijk worden meegerekend.

## Herstelde fouten

1. Elk laadvenster zette de gesimuleerde accu automatisch op vol. De simulatie
   telt nu alleen de geselecteerde laadenergie op. Ook gedeeltelijk voltooide
   laadcycli kunnen hun werkelijk opgeslagen energie daarna gebruiken.
2. Korte pauzes werden achteraf vervangen door laden of ontladen, zonder nieuwe
   energieberekening. Die nabewerking is uit de productieplanning verwijderd.
3. Na een gedeeltelijk gebruikt laadkwartier ontbrak een expliciet uit-venster.
   De schakellijst kon daardoor laden laten doorlopen tot het volgende kwartier.
   De planning bevat nu aansluitende vensters met expliciete stoptijden.
4. Een gedeeltelijk beschikbaar ontlaadbudget werd als een volledig actief
   kwartier weergegeven. De ontlaadduur wordt nu begrensd op het toegewezen
   energievolume en de toepasselijke reserve.
5. Een goedkoop netlaadslot kon nogmaals worden gebruikt voor aanvullend laden
   wanneer de eerste selectie onvoldoende energie leverde. Dubbel tellen is geblokkeerd.
6. Onbekende tarieven verloren hun herkenbaarheid in de energiebalans.
   Die markering blijft nu behouden. Onbekende inkoopprijzen of latere
   invulprijzen mogen geen rendabele netlaadcyclus onderbouwen.
7. De actuele volle accustatus kon alle latere netlaadvensters omzetten naar
   zonneladen, ook na toekomstig ontladen. Die omzetting is verwijderd uit de
   nabewerking van de volledige planning.

## Nagespeelde planning na herstel

De bestaande, eerder gevraagde strategie om bij een gunstig prijsverschil bij
te vullen is behouden. Dit is de uitkomst onder onderstaande aannames, geen
bewijs van economische optimaliteit:

| Tijd (Nederlandse tijd) | Stand |
|---|---|
| 17 september 12:33–13:38 | Laden van net, circa 3,2 kWh |
| 13:38–17:30 | Accu uit |
| 17 september 17:30–18 september 07:58 | Ontladen |
| 07:58–08:00 | Accu uit |
| 08:00–12:30 | Laden met zonne-energie |
| 12:30–18:00 | Accu uit |
| 18:00–00:00 | Ontladen |

De periode na middernacht is voorlopig: de invulprijzen worden nog gebruikt
voor het verdelen van bestaande accu-energie over de horizon. Daardoor kan de
planner energie bewaren voor een veronderstelde nachtprijs in plaats van een
bekende middagprijs. Nieuwe echte tarieven kunnen deze verdeling wijzigen.

## Verificatie en grenzen

De controle speelt de volledige `_build_plan`-route na, inclusief de
uiteindelijke sensorvensters en schakellijst. De aangeleverde vraagprognose
wordt als bestaand invoerresultaat gebruikt; de oorspronkelijke meetgeschiedenis
en de leerstap zijn niet beschikbaar in de dump.

Een aparte berekening integreert de uitgegeven standen met het verbruik, de
zon en het vermogen, zonder de energie achteraf op de accugrenzen af te kappen.
In dit scenario blijft de berekende totale accu-energie tussen 2 en 10 kWh.
De eerdere nabewerking gaf opdrachten die boven de beschikbare capaciteit
uitkwamen. De omvormer zou die in werkelijkheid moeten begrenzen.

Aannames: maximaal laden 3 kW (niet expliciet gegeven in de dump), maximaal
ontladen 3 kW, teruglevertarief gelijk aan inkoop minus €0,10 (niet aangeleverd),
geen laad-/ontlaadverliezen. Deze ontbrekende gegevens beperken hoe precies
de laadduur en financiële opbrengst kunnen worden beoordeeld.

Er zijn 105 geslaagde Python-tests, inclusief nieuwe tests voor de volledige
sensorplanning, fysieke energiebalans, gedeeltelijk laden en ontladen,
dubbele laadcapaciteit en onbekende prijzen. Er is geen live Home Assistant
of omvormer aangestuurd.

Voor een werkelijk economische planner is een volgende inhoudelijke stap
nodig: optimaliseer marginale besparing over alle tijdvakken, met expliciet
rendement en een beleid voor onbekende prijzen. Dat zou de eerder gevraagde
voorkeur voor bijvullen kunnen veranderen en is hier niet stilzwijgend ingevoerd.

Zie `battery-replay-2026-09-17.json` voor het volledige berekende resultaat en
de onafhankelijk berekende energiereeks.
