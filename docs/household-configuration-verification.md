# Hoofdconfiguratie en subplanners: verificatie

## Gebruikersdoel en implementatie

Eén Home Assistant-configuratie bevat gedeelde inkoopprijs, optionele exportprijs
met belastingcorrectie, cumulatieve hoofdverbruiksmeter en zonneprognoses voor
vandaag en morgen. Accu-, prijsvenster- en verbruiksprognoseplanners zijn config
subentries. Externe vraagprognoses blijven accu-specifiek. Contractresolutie,
vensterduur, accubeleid en apparaatregels blijven planner-specifiek.

`household.py` centraliseert bronovererving, stabiele planneridentiteit,
platformregistratie, expliciete overname en coalescing van identieke historische
bronopvragingen. Bestaande rekenalgoritmen worden niet vervangen. Er is geen nieuwe
gezamenlijke optimalisatie of fysieke apparaatbesturing. De bestaande zes
apparaatmeters blijven regels onder de verbruiksprognoseplanner. Een huishoudelijk
hoofdapparaat wordt geregistreerd; planner-entiteiten horen bij hun config
subentry, zonder een extra fysieke subapparatenhiërarchie te beloven.

## Compatibiliteit en overname

Bestaande losse configuraties blijven werken. Overname is expliciet per planner:
maak de hoofdconfiguratie met overeenkomende bronnen, voeg een subplanner van het
juiste type toe, selecteer de bestaande planner en bevestig. Bronconflicten
blokkeren overname. De oude configuratie wordt een inactief archief; unieke
sensoridentiteit en opslagidentiteit blijven behouden. Onderbroken overname kan
idempotent worden hervat. Er worden geen configuraties, sensoren of opgeslagen
accustatussen automatisch verwijderd.

Een fout vóór overdracht mag de nog actieve oude coordinator of runtime-status
niet verwijderen. Platformregistratie vereist zowel de succesvol gestarte
subplanner in de parent-map als exact hetzelfde coordinatorobject onder de globale
planneridentiteit. Daardoor kan een mislukte overname niet alsnog de sensoren van
de oude planner registreren.

De prijsvenster-binaire sensor kan zonder eerste coordinatorresultaat worden
geplaatst. Na bronherstel wordt de tijdgrens opnieuw gepland. Defecte subplanners
blokkeren gezonde planners niet.

## Werkelijk uitgevoerde controles

- Volledige offline suite: `python -m unittest discover -s tests -p 'test_*.py'`:
  416 tests, OK. Eindlog lokaal: `household-full-suite.log`.
- Gerichte suite: `python -m unittest discover -s tests -p 'test_household*.py'`:
  29 tests, OK.
- Foutreproducties vóór herstel: behoud van actieve legacy-coordinator bij
  bronconflict/mislukte unload, eerste prijsvensterresultaat ontbreekt en
  platformregistratie na mislukte overname. Eigenaarschapreproductie gaf vier
  gefaalde subtests; na herstel slagen beide platforms voor beide foutredenen.
- `git diff --check` en Python compileall: geslaagd.
- Onafhankelijke review vond de lifecycleproblemen; na herstel is de gerichte
  eindreview akkoord, zonder resterende beveiligings- of logische blokkades.

De tests gebruiken expliciete Home Assistant-contractdoubles. Home Assistant is
hier niet geïnstalleerd; er is geen echte HA-integratietestrun of UI-test van de
nieuwe versie uitgevoerd. Live Home Assistant meldde versie 2026.11.1. De nieuwe
versie is niet live geïnstalleerd, gemigreerd of herstart.

## Read-only controle bestaande installatie

De config-entry-API toont zes geladen, niet-uitgeschakelde planners: Accu,
goedkoopste 3 uur, Goedkoopste uur, duurste uren, goedkoopste 5 uur en Home
Consumption. Het entiteitenregister plus actuele states bevestigen dat alle twaalf
bijbehorende integratie-entiteiten beschikbaar zijn. Dat bewijst bronbeschikbaarheid
en actieve berekeningen, niet dat iedere toekomstige prijskeuze economisch optimaal is.

De verbruiksprognose gebruikt zes apparaatmeters: wasmachine/droger, tv-meubel,
oven, warmtepomp, vaatwasser en netwerk, plus de hoofdverbruiksmeter. Alle geven een
numerieke cumulatieve kWh-stand met passende state_class. De prijsbron is beschikbaar.
De prognose is actief maar meldt timing-fallbacks voor warmtepomp en vaatwasser
voor een toekomstige dag; dit is terugval op historische tijden, geen volledige
bronuitval. Er is niets aan deze live configuraties gewijzigd.

## Grenzen en vervolgstap

Na installatie is een echte UI-/reload-/overnamecontrole nodig. Bestaande planners
worden niet automatisch verplaatst. Installatie, herstart en live overname vallen
buiten deze lokale wijziging/publicatie en vereisen concrete toestemming.
