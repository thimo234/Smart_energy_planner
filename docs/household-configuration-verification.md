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

## Aanvullende regressie: subentrymenu en overnamearchief

Het huishoudapparaat is bewust het bovenliggende apparaat, niet automatisch een
wees. Onderscheid het huishouden, de planner-subentry en een oude config-entry met
`_adopted_by`: alleen de laatste is een inactief overnamearchief.

Officiële HA Core **2026.11.1**-broncode read-only gecontroleerd:

- [llama.cpp strings](https://github.com/home-assistant/core/blob/2026.11.1/homeassistant/components/llama_cpp/strings.json): menulabels staan in `config_subentries.<type>.entry_type` en `initiate_flow.user`, niet in een bovenliggend `title`. De stap-titels blijven ongewijzigd.
- [Device registry](https://github.com/home-assistant/core/blob/2026.11.1/homeassistant/helpers/device_registry.py): `async_update_device(new_config_entry_id=..., new_config_subentry_id=...)` draagt bestaande apparaten over. Alleen entiteiten verplaatsen laat apparaten achter die HA bij archiefverwijdering kan opruimen.
- [Config entries](https://github.com/home-assistant/core/blob/2026.11.1/homeassistant/config_entries.py): entryverwijdering unloadt en ruimt registers op; `async_remove_subentry` wijzigt het huishouden en activeert update-listeners. Er is hier geen aparte integratiehook voor subentryverwijdering.
- [DataUpdateCoordinator](https://github.com/home-assistant/core/blob/2026.11.1/homeassistant/helpers/update_coordinator.py): shutdown wordt bij de eigenaar geregistreerd via `async_on_unload`. Ontbrekende callbacks in een testdouble bewijzen dus geen productiefout in shutdown.

Overname verplaatst nu bestaande apparaten én entiteiten, met dezelfde
legacy-planneridentiteit/opslagsleutel. Een onderbroken registeroverdracht is
hervatbaar. Een bestaande configuratieclaim blokkeert dubbel eigenaarschap, ook
bij mislukte setup of een inmiddels verwijderd archief. Een nog levende
legacy-controller blokkeert conflicterend herstel.

Een overgebleven archief met een verwijderde eigenaar kan via de normale
selectielijst opnieuw expliciet worden overgenomen. Bevestiging en controle van
gedeelde bronnen blijven verplicht. De nieuwe subentry behoudt `_legacy_entry_id`
maar krijgt geen archiefmarker `_adopted_by`. Een actief toegewezen archief wordt
niet aangeboden. Na kindverwijdering blijft het oude archief inactief; de marker
wordt niet gewist om zelfstandig accubesturing te herstarten. Archiefverwijdering
mag de actieve kindcoordinator/status niet wissen. Kindverwijdering ruimt bij
parent-reload runtime-eigenaarschap op, niet het historische opslagrecord. HA kan
bewust verwijderde kindapparaten/entiteiten wel uit registers verwijderen; dit
belooft geen herstel van verwijderde register-ID's na expliciete kindverwijdering.

Gerichte controle op deze wijziging:

```sh
PYTHONPATH=tests python -m unittest test_household test_household_flows test_household_binary_sensor test_household_lifecycle -q
```

**40 tests geslaagd**, inclusief 11 nieuwe lifecycle-testmethoden. Een geïsoleerde
kopie van baseline `db6cb10` met de nieuwe tests reproduceerde **13 gefaalde
asserties en 2 errors**. Alleen de extra device-registryparameter werd daarbij
voor de oude vierargumentfunctie genegeerd, zodat fouten het ontbrekende gedrag
aantonen in plaats van uitsluitend een gewijzigde functiehandtekening.

Dit blijft offline verificatie met HA-contractdoubles. Echte menuweergave,
verwijdering/reload en geserialiseerd herstel van actieve cycli moeten nog op de
doelinstallatie worden gecontroleerd. De registry-eventregressie hieronder is wel
lokaal uitgevoerd. Geen live wijziging, installatie, herstart, commit of push
uitgevoerd voor deze aanvullende wijziging.

### Aanvullende regressie: onderbroken eigendomsoverdracht

Een vervolgreview vond registerverlies bij de volgorde apparaten-eerst: HA's
`EntityRegistry.async_device_modified` verwijdert na een apparaatverplaatsing
entiteiten die nog bij de oude config-entry/subentry horen. Een exception tijdens
entiteitsoverdracht laat het al ingeplande cleanup-event alsnog uitvoeren; opnieuw
proberen kan de verwijderde registerrecords en gebruikersaanpassingen niet herstellen.

Overname verplaatst daarom **alle entiteiten vóór enig apparaat**, zonder `await`
tussen de registerupdates. De bestaande apparaatlinks blijven intact. De officiële
HA 2026.11.1-`_validate_item` accepteert tijdelijk afwijkend entiteit-/apparaateigendom:
het controleert de doelentry/subentry en, bij een meegegeven apparaat-ID, het bestaan
van het apparaat, niet gelijk eigenaarschap. De entiteitsupdate zelf verstuurt een
entity-registry-update, geen device-update. Bij een fout in de eerste lus is nog
geen apparaat-event ingepland; bij een fout in de tweede lus zijn alle entiteiten
al veilig overgedragen. De archiefmarker en bestaande eigenaarschapsfilters maken
herhaling een hervatting, geen nieuwe registratie.

Tests-first toegevoegd in `test_household_lifecycle.py`: acht interruptiesubcases
voor de eerste/tweede entiteits- en apparaatupdate, zowel vóór als ná de write.
Een echte event-loop-tick levert cleanup vóór de retry af. De test controleert
behoud van originele entiteit-ID's, unieke ID's, apparaatlinks, gebruikersnamen,
iconen, labels, area, opties, uitgeschakelde entiteiten en actieve runtime-status.
De retry voltooit de overdracht; nogmaals herhalen doet geen registerupdates en
unloadt het archief niet opnieuw.

- Rood vóór herstel: **7 van 8 subcases faalden** wegens daadwerkelijke
  `async_remove`-aanroepen. Alleen de fout vóór de eerste apparaatwrite was veilig.
- Groen na herstel: bovenstaande gerichte suite **41 tests, OK**.
- Onafhankelijk dezelfde interruptietest uitgevoerd met de volledige, ongewijzigde
  `_validate_item` en `async_device_modified`, via AST uit de eerder opgeslagen
  officiële HA-bron geëxtraheerd: hetzelfde rood (7 failures), daarna groen.
  Lokaal reproduceerscript: `/opt/data/cache/scratch/verify_household_actual_ha_cleanup.py`.
  De normale tests bevatten de relevante HA-eigenaarschapschecks en zijn niet
  afhankelijk van dit tijdelijke bronbestand/script of een geïnstalleerde HA.
- `git diff --check` en compilatie van de twee gewijzigde Python-bestanden: OK.

Dit is geen volledige HA-integratietest. Apparaat-/entiteitsopslag en de overige
HA-eventlisteners blijven doubles; de volledige suite wordt afzonderlijk opnieuw
uitgevoerd. Er zijn geen diagnostiek- of accutestbestanden gewijzigd.
