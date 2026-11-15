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

## Prijsblokplanners: één gedeelde subentry

Binnen één huishouden delen alle prijsblokplanners één config-subentry. De eerste
planner mag voor compatibiliteit nog de oude platte gegevensvorm gebruiken;
bij toevoegen van een tweede planner wordt diezelfde subentry een groep met
`_price_window_planners`. Deze mapping is gesleuteld op de **blijvende
planneridentiteit**, niet op de naam. Nieuwe groepsleden krijgen een eigen
willekeurige identiteit; bestaande leden behouden de oorspronkelijke subentry-ID
of `_legacy_entry_id`. Accu- en verbruiksprognose-subentries blijven ongewijzigd.

Elke prijsblokplanner behoudt zijn naam, instellingen, coordinator, planning,
service-target en runtime-opslagsleutel. De parent-runtime-map gebruikt voor
gegroepeerde leden de planneridentiteit (`runtime_key`); platformregistratie en
diagnostiek controleren precies die coordinator. Registratie bij HA gebruikt voor
alle leden dezelfde `config_subentry_id`. Entiteit- en apparaatidentiteiten,
bestaande `unique_id`/`entity_id`, apparaatlinks en registeraanpassingen worden bij
consolidatie niet opnieuw aangemaakt of hernoemd. Losse legacy-configuraties
worden nooit automatisch overgenomen.

Bij huishoudsetup consolideert `consolidate_price_windows` bestaande afzonderlijke
prijsblok-subentries onder de eerste bestaande prijsblok-subentry. Eerst worden
alle instellingen plus `_price_window_group_sources` als hervattingsjournaal in de
doelsubentry bewaard. Vervolgens worden synchroon **alle entiteiten vóór enig
apparaat** verplaatst, archiefverwijzingen bijgewerkt en pas daarna bron-subentries
verwijderd. Geen `await` tussen registerupdates. Bij een exception blijven het
journaal en de nog aanwezige bronnen staan; de volgende setup hervat. Een tweede
voltooide run wijzigt geen registers. Conflicterende dubbele identiteiten worden
niet stilzwijgend samengevoegd. Dit sluit aan op HA 2026.11.1-registercleanup.

Het configuratiemenu van de groep biedt een plannerkeuze met bewerken, toevoegen
of verwijderen. Toevoegen via het normale subentrymenu voegt eveneens aan de
bestaande groep toe. Bewerken verandert alleen het gekozen lid; gelijktijdig
verwijderde leden kunnen niet door een oude editflow terugkomen. Verwijderen
vereist bevestiging en bewaart een tombstone tot de parent is herladen. Bij die
setup worden alleen registers van de verwijderde identiteit binnen de betreffende
parent/subentry opgeruimd (entiteiten vóór apparaten); peerplanners en historische
runtime-opslag blijven behouden. Een leeg geworden groep blijft bestaan en kan
weer worden gevuld. HA's eigen verwijderactie voor de hele subentry verwijdert
uiteraard de hele groep. Een verwijderd lid kan later opnieuw expliciet uit een
inactief legacy-archief worden overgenomen, met broncontrole; dit belooft geen
behoud van register-ID's na bewuste verwijdering.

Offline regressies: `tests/test_price_window_groups.py` controleert bindings,
platformownership, setup/unload, diagnostiek, consolidatie/herhaling/interruption,
edit/add/remove, tombstones, vertaalde formuliervelden en archiefbeschikbaarheid.
De bestaande huishoud-runtimecontrole gebruikt nu vier prijsplanners onder één
subentry, met ongewijzigde accu-/vraagstatus en bronabonnementen.

Uitgevoerd op de lokale groepswijziging bovenop `c5c66d5`:

- RED tegen een geïsoleerde kopie van `c5c66d5`: alleen de nieuwe
  `test_price_window_groups.py` overgenomen; 14 tests, 11 failures en 3 errors
  (bindings/migratie/groepsflows ontbraken). Daarnaast faalde de tombstone-
  consolidatieregressie eerst met `KeyError: '_removed_price_window_planners'`.
- GREEN: `PYTHONPATH=tests python -m unittest test_price_window_groups test_household test_household_flows test_household_lifecycle test_household_binary_sensor test_diagnostics -q`
  — **64 tests geslaagd**.
- `python -m unittest discover -s tests -p 'test_*.py'` — **511 tests geslaagd**
  in 481.406 s; volledig log: `/opt/data/cache/scratch/price-group-final-full-tests.log`.
- `node tests/test_frontend_card.js` — geslaagd (kaart, editor/migratie en
  bestaande snapshot-/laadvenstertotalen).
- `git diff --check` — geslaagd.

De controles gebruiken HA-contractdoubles; echte UI-/reload-/registryopslag-
verificatie op HA blijft noodzakelijk. Voor deze wijziging is niet gedeployed of
naar HA geschreven. Accuplanningcode is niet gewijzigd.

## Werkelijk uitgevoerde controles (eerdere huishoudwijziging)

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

## Reviewherstel prijsblokgroepen: retry en verouderde formulieren

- De gepersisteerde doelgroep is na het schrijven van het retryjournal leidend.
  Een retry leest reeds gekopieerde bronleden niet opnieuw in. Regressies dienen
  echte edit-/remove-formulieren in tussen een registryfout en de retry: gewijzigde
  instellingen blijven behouden; verwijderde leden worden niet hersteld en hun
  tombstones blijven bestaan tot de gerichte entity-/device-cleanup.
- Een formulier blijft gebonden aan de oorspronkelijke groeps-subentry. Bij
  verwijdering of vervanging stopt edit/add/remove vóór iedere create/write.
  Ook de eerste reconfigure-stap controleert het context-ID vóór HA's
  `_get_reconfigure_subentry`; die lookup kan voor een verdwenen ID falen.
  De regressies dwingen daarnaast HA 2026.11.1's `SOURCE_USER`-beperking voor
  `async_create_entry` af: reconfigure mag geen nieuwe subentry aanmaken.
- Een formulier voor een bron die inmiddels in een consolidatiejournal staat,
  wordt geweigerd: alleen de doelgroep mag daarna gewijzigd worden. Gelijktijdige
  eerste add-formulieren mogen na het ontstaan van een groep geen tweede groep
  aanmaken. Succesmeldingen gebruiken het HA-vertaaldomein; actieopties gebruiken
  vertaalde EN/NL-selectorlabels.
- RED-bewijs: `/opt/data/cache/scratch/third-fix-red-retry.log`
  (edit: `ValueError`, remove: hersteld lid), `third-fix-red-stale.log`
  (illegale reconfigure-create, ontbrekende lookup en writes naar vervangende
  groep) en `third-fix-red-related.log` (verouderde bron, gelijktijdige eerste
  add, ontbrekende selectorvertaling). Alle logs staan in dezelfde scratchmap.
- De gerichte suite na de laatste productiecodewijziging slaagde: **71 tests**
  (`third-fix-focused.log`). Dit is offline contract-/doubles-verificatie, geen
  draaiende HA-integratietest. Er is niet geïnstalleerd, gedeployd of naar HA
  geschreven; accuplanningcode en live-analysebestanden zijn niet gewijzigd.
- Aanvullend zijn de ongewijzigde `async_create_entry`,
  `_get_reconfigure_subentry` en bijbehorende properties via AST uit de lokaal
  opgeslagen officiële HA Core 2026.11.1-bron geladen. De echte sourcebeperking
  en missing-subentry-exceptie zijn eerst gecontroleerd; daarna slagen alle
  **6 edit/add/remove × verwijderd/vervangen**-gevallen zonder create/write.
  Reproduceerscript: `/opt/data/cache/scratch/third_fix_actual_ha_contract.py`;
  resultaat: `/opt/data/cache/scratch/third-fix-actual-ha-contract.log`.
- Na de laatste productiecodewijziging is de volledige suite opnieuw uitgevoerd:
  `python -m unittest discover -s tests -v` — **518 tests, OK, 467.339 s**, exit 0.
  Volledig log: `/opt/data/cache/scratch/third-fix-full-unittest.log`.
  De eerste foreground-poging werd door de tooltimeout afgebroken; de genoemde
  volledige run is daarna als bijgehouden achtergrondproces voltooid.
- `node tests/test_frontend_card.js` — geslaagd, exit 0; log:
  `/opt/data/cache/scratch/third-fix-frontend.log`. De kaart-, editor/migratie-,
  snapshot- en laadvenstercontroles slagen. De zeven nieuwe regressietests
  slagen ook afzonderlijk (`third-fix-green-regressions.log` in dezelfde map).

## Tweede reviewherstel: hele groep verwijderen tijdens consolidatie

- Blokker tests-first gereproduceerd: na een fout vóór/na de eerste entity-write,
  HA-vormige `async_remove_subentry` van de doelgroep en een JSON-opslag/herstart
  werd bron `three` opnieuw een actieve planner. RED-log:
  `/opt/data/cache/scratch/group-deletion-red.log` — `Ran 1 test in 0.003s`,
  `FAILED (failures=2)`, met `['three', 'battery'] != ['battery']`.
- Het huishouden bewaart nu `_price_window_retired_sources` (bron → eigenaar)
  **vóór** de doelgroepsnapshot. Dit record verdwijnt niet met de doelgroep.
  Bindings en groepsformulieren negeren gepensioneerde bronnen. Als hun eigenaar
  is verwijderd, hervat setup uitsluitend HA's normale subentry-verwijdering;
  bronleden/registerrecords worden niet naar een nieuwe groep gekopieerd.
- Regressies serialiseren entry-/subentrydata naar JSON, controleren HA-vormige
  entity-/device-cleanup, en onderbreken vóór/ná retirement-, snapshot- en
  bronverwijderwrites. Een niet-gepersisteerde retirement mag een nog geldige
  bron niet weggooien. Een gepersisteerde retirement voorkomt herstart en
  edit/add/remove-writes naar de bron, ook vóór de volgende setup. Setup/unload
  wordt daarna echt aangeroepen met de bestaande offline runtime-contractdoubles.
- Archieven blijven inert. Alleen expliciete herovername van een verweesd archief
  zonder andere geconfigureerde claim is toegestaan. Vóór de doelgroepsnapshot
  beschermt de retirement-eigenaar de bestaande claim al. Deze extra grensgevaltest
  was eerst rood: `group-deletion-legacy-red.log` — `Ran 1 test in 0.002s`,
  `FAILED (failures=1)`, `AssertionError: True is not false`.
- Het complete eerste RED-pakket staat in `group-deletion-all-red.log`:
  `Ran 3 tests in 0.007s`, `FAILED (failures=10)`. Na herstel slagen alle vijf
  regressiemethoden: `group-deletion-green-regressions.log` —
  `Ran 5 tests in 0.046s`, `OK`, exit 0.
- Gerichte suite na de laatste productiecodewijziging:
  `PYTHONPATH=tests python -m unittest test_household test_household_flows test_household_lifecycle test_household_binary_sensor test_price_window_groups test_diagnostics -v`
  — **76 tests, OK, 0.331 s**, exit 0; `group-deletion-focused.log`.
  Eén oudere runtimedouble miste `async_update_entry`; alleen die ontbrekende
  HA-managerwrite is aan de double toegevoegd.
- Aanvullend zijn dezelfde vijf regressiemethoden uitgevoerd met HA's exacte
  `async_remove_subentry`, zonder decorators via AST geladen uit de al lokaal
  opgeslagen HA Core 2026.11.1-bron. **5 tests, OK, 0.043 s**, exit 0;
  `group-deletion-actual-ha-removal.log`. Reproduceerscript:
  `/opt/data/cache/scratch/group_deletion_actual_ha_removal.py`.
  Entryopslag en registry-cleanup blijven contractdoubles.
- Volledige Python-suite na de laatste productiecodewijziging:
  `python -m unittest discover -s tests -v` — **523 tests, OK, 469.202 s**,
  `EXIT_CODE=0`; `/opt/data/cache/scratch/group-deletion-full-unittest.log`.
- Frontend na de laatste productiecodewijziging:
  `node tests/test_frontend_card.js` — alle vier controlegroepen geslaagd,
  exit 0; `group-deletion-frontend.log`. `git diff --check` is ook geslaagd.
  Alle hier genoemde korte lognamen staan in `/opt/data/cache/scratch/`.
- Dit blijft offline contractverificatie, geen live HA-integratietest. Geen
  installatie/download, live HA, commit, push of deployment uitgevoerd;
  accuplanning en de bestaande `analysis/live-*.json` zijn niet gewijzigd.

## Laatste migratieherstel: edit tussen retirement en snapshot

- RED: `test_price_window_groups.GroupDeletionRetryTest.test_edit_before_snapshot_keeps_exclusive_claim_through_restart_and_retry`
  onderbreekt de snapshotwrite na gepersisteerde retirement, bewerkt daarna de
  bestemming via de echte flowmethoden en serialiseert entry-/subentrydata naar
  JSON vóór verdere flowinzendingen en migratieherhaling. Eén test gaf **5 failures**:
  selectie en reeds geopende bevestiging stonden overname door een ander huishouden
  toe, de geconfigureerde claim ontbrak en setup kon het archief daadwerkelijk
  opnieuw toewijzen. Log: `/opt/data/cache/scratch/pending-snapshot-red.log`.
- `legacy_claims` telt nu de nog niet gesnapshotte bronleden als claims van hun
  gepersisteerde retirement-eigenaar, zolang bron en eigenaar bestaan. Alleen het
  expliciete `_price_window_group_sources`-journaal bewijst dat de snapshot is
  opgeslagen; een groepspayload zonder dat lid bewijst geen bewuste verwijdering.
  Alle bestaande selectie-, bevestigings- en setupguards delen deze claimcontrole.
  De bestaande test voor dezelfde grens zonder edit verwacht daardoor nu de
  eerdere foutmelding `already claimed` in plaats van `already adopted`.
- GREEN: dezelfde regressie — **1 test, OK, 0.006 s**; log:
  `/opt/data/cache/scratch/pending-snapshot-green.log`. De test controleert ook dat
  migratieherhaling de edit bewaart, registers overdraagt, het archief naar de
  juiste eigenaar wijst en expliciete verwijdering ná snapshot herovername toestaat.
- Gerichte suite op deze correctie:
  `PYTHONPATH=tests python -m unittest test_household test_household_flows test_household_lifecycle test_household_binary_sensor test_price_window_groups test_diagnostics -v`
  — **77 tests, OK, 0.238 s**, exit 0; log:
  `/opt/data/cache/scratch/pending-snapshot-focused.log`. `git diff --check` geslaagd.
- Definitieve verificatie na deze correctie: volledige Python-suite **524 tests,
  OK, 469.967 s**, exit 0; log:
  `/opt/data/cache/scratch/price-group-approved-full-tests.log`.
  `node tests/test_frontend_card.js` en `git diff --check` slagen eveneens.
  De eerste voorgrondrun werd door de tooltijdslimiet afgebroken; de volledige
  achtergrondrun hierboven is gecontroleerd op exitcode en het unittest-eindresultaat.
- Onafhankelijke read-only eindcontrole: akkoord zonder security- of logicablokkers;
  77 gerichte tests en 20 extra interruptiescenario's geslaagd. Deze controle omvatte
  alle kandidaatwijzigingen tegen c5c66d5, inclusief stale flows, exclusief eigendom,
  registeroverdracht en verwijdering/herstel.
- Dit blijft offline HA-contractverificatie. Echte HA-UI-/opslag-/reloadverificatie
  blijft open. Geen installatie, download, herstart of deployment uitgevoerd;
  accuplanning en de ongetrackte analysebestanden zijn behouden.
