# Eisen aan de accuplanner

Dit is de blijvende afsprakenlijst voor werkzaamheden aan de accuplanner.
De eisen hieronder komen uit het gesprek met de gebruiker. Ze beschrijven het
gewenste gedrag; ze zijn geen verklaring dat alles al geïmplementeerd of live
geverifieerd is. Lees ook de open punten onderaan.

## 1. Prijs bepaalt het laden

- Vergelijk netladen en zonneladen op hun effectieve prijs. Zonneoverschot krijgt
  niet automatisch voorrang en is geen voorwaarde om een laadcyclus te plannen.
- Verduidelijking 26 september: `laden_van_net` gebruikt ook het zonneoverschot;
  het net vult alleen aan tot het totale laadvermogen. Reken de zonnebijdrage
  tegen de terugleverprijs en uitsluitend de aanvulling tegen het importtarief.
  Op hetzelfde tijdstip blijft zon doorgaans goedkoper. Goedkope netaanvulling
  mag duurdere latere zonnelading vervangen; goedkopere latere zon gaat voor.
  De importaanvulling moet zelfstandig aan de minimumwinst voldoen.
- Gebruik het door de gebruiker opgegeven voordeel van **€0,11/kWh** voor zon:
  standaard belastingaftrek = €0,11/kWh, nu instelbaar in EUR/kWh.
  Zonder aparte terugleversensor (of met dezelfde sensor) is de terugleverprijs
  importtarief min deze aftrek. Een aparte terugleversensor blijft ongewijzigd.
  De effectieve zonneprijs is deze terugleverprijs; trek de belasting niet dubbel
  af. 0 schakelt de aftrek uit. Dit is geen automatisch bijgewerkte fiscale regel.
- Bij een bekende negatieve importprijs wil de gebruiker **netladen**, ook
  als er tegelijkertijd zonneoverschot is. Respecteer capaciteit, vermogen en
  de geldende cyclusvergrendeling.
- Kies ook voor zonneladen de goedkoopste geschikte uren. De gebruiker vindt
  het goed dat eerdere, duurdere zonne-energie daardoor niet wordt opgeslagen.
- Op een dag zonder zon moet zelfstandig een rendabele netlaadcyclus kunnen
  worden gepland. Een klein beetje zon mag benodigd netladen niet blokkeren.
- Vermijd onnodige onderbrekingen bij gelijke prijzen. Een pauze vanwege een
  werkelijk duurder tarief kan wel logisch zijn en moet zichtbaar zijn.
- Gewijzigd op 1 oktober: bij prijsverschillen van maximaal **EUR 0,01/kWh
  (1 cent)** krijgen minder schakelingen en zoveel mogelijk aaneengesloten
  uitperiodes voorrang boven kleine tariefvoordelen. Dit geldt voor laden en
  ontladen, ook in toekomstige vensters. Dit vervangt de band van 0,5 cent
  die alleen voor een lopende laadcyclus gold.
- Bundel dezelfde hoeveelheid energie binnen geschikte prijsbanden; voeg geen
  extra lading toe. Behoud laadbron, minimumwinst, cyclusgrenzen, reserve,
  afname en vermogen. Laat kleine opeenvolgende tariefstappen niet optellen
  tot een toegestane verschuiving van meer dan 1 cent.
- Een gekozen laadvenster mag niet bij iedere herberekening vooruit schuiven
  doordat het lopende kwartier wordt overgeslagen. Veranderende SOC, tarieven
  of prognoses mogen de benodigde resterende laadtijd wel veranderen.

## 2. Rendement en volledige laadcyclus

- Akkoord 29 september: bestaande accustroom mag voor eigen huisverbruik
  worden gebruikt zonder opnieuw de minimumwinst tegenover de historische
  inkoopprijs te eisen. Veilige ondergrens, extra reserve, vermogenslimieten
  en cyclusvergrendeling blijven gelden. Nieuwe netlading moet de minimumwinst
  zelfstandig blijven halen; export houdt de bestaande winstbescherming.
- Houd rekening met de instelbare minimumwinst per kWh en bekende latere
  tarieven. Toon ook het bijbehorende latere ontladen; een laadplan zonder
  onderbouwing aan de afnamekant is onvoldoende.
- Een geldige laadcyclus streeft naar een volle accu, rekening houdend met
  verwachte ontlading vóór de cyclus, zonnebijdrage en het maximale laadvermogen.
- Verduidelijking 30 september: meerdere volledige cycli per dag zijn gewenst
  als elke extra cyclus de minimumwinst behaalt. Voorbeeld: dagladen, avondexport,
  nachtladen, ochtendexport en opnieuw dagladen. Geen bewust kleine bijlaadcycli.
- Tussen het einde van laden en de volgende laadstart is minimaal de tijd nodig
  om de bruikbare capaciteit met maximaal ontlaadvermogen af te geven. Dat is
  slechts een fysieke ondergrens: voldoende rendabele afname moet er ook zijn.
- Toets eigen verbruik tegen het importtarief en verkoop tegen de terugleverprijs.
  Deel het ontlaadvermogen tussen beide, tel energie niet dubbel en gebruik voor
  de extra cyclus uitsluitend bekende tarieven. Een losse hoge prijspiek is
  onvoldoende om een volledige laadcyclus economisch te onderbouwen.
- Ontbreekt voldoende rendabele ontlading vóór het goedkopere volgende venster,
  sla de extra cyclus over. Dagladen gevolgd door huisverbruik in de nacht en
  opnieuw dagladen blijft mogelijk. Dit vervangt de gedeeltelijke nachtcyclus.
- De bestaande uitzondering bij tegenvallende zon na het laatste rendabele
  laadvenster blijft gelden. Veilige SOC en bescherming tegen pendelen blijven.
- Stop een gestarte netlaadcyclus niet halverwege doordat de zojuist geladen
  energie bij een herberekening als al aanwezige voorraad wordt afgetrokken.
- Ga niet bij ieder toekomstig laadvenster opnieuw uit van een lege accu:
  simuleer resterende energie en respecteer de werkelijke beschikbare ruimte.
- Er is eerder besproken dat volledig bijladen ook energie kan achterlaten
  waarvan de latere winst nog niet binnen de horizon bewezen is. Presenteer
  dus geen garantie dat iedere geladen kWh de minimumwinst oplevert.
- Het huidige rekenmodel gebruikt tariefverschillen. Omzettingsverliezen en
  slijtage zijn niet daarin verwerkt; verander die definitie niet ongemerkt.

## 3. Niet pendelen tussen laden en ontladen

- Streef naar zoveel mogelijk een volledige laad- en ontlaadcyclus.
- Zodra ontladen is gestart, mag een kleine SOC-daling gevolgd door een gunstige
  laadprijs niet meteen tot opnieuw laden leiden. Blijf in de ontlaadcyclus
  richting de veilige ondergrens.
- Omgekeerd mag een begonnen laadcyclus niet na een kleine SOC-stijging meteen
  worden afgebroken voor ontladen omdat prijzen of prognoses iets veranderen.
- De nieuwste wens geldt voor beide laadrichtingen en bronnen. De eerdere
  uitzondering waarmee zon tijdens ontladen mocht bijladen is niet meer een
  vanzelfsprekend toegestaan uitgangspunt.
- `accu_uit` tijdens een noodzakelijke pauze is niet hetzelfde als de cyclus
  omkeren. Houd de cyclusstatus vast over herberekeningen en herstarts.
- Uitzondering, expliciet akkoord 2 oktober: een onvermijdelijk restant mag
  een rendabel nieuw laadvenster niet blokkeren. Probeer eerst te ontladen via
  huisverbruik of rendabele export. Kan de accu tijdens het laadvenster niet
  verder nuttig ontladen, vul dan alleen de werkelijk vrije capaciteit aan.
- De gebruiker heeft op 2 oktober bevestigd dat export de minimumwinst ten
  opzichte van de oude inkoopprijs moet blijven halen. Een goedkopere volgende
  lading is geen reden om die grens te versoepelen; laat het restant dan zitten.
- Een ontbrekende SOC tijdens opstarten betekent niet dat de accu leeg is.
  Initialiseer of wijzig geen cyclus totdat een geldige SOC beschikbaar is;
  overschrijf daarbij ook geen opgeslagen status of tijdstempel. Herstel de
  inkoopprijs voor exportbescherming samen met de cyclusstatus.
- Na een kort exportdeel direct doorgaan met normaal ontladen als daarvoor
  huisverbruik en energie zijn ingepland. Verkort zo nodig het exportdeel om
  de rest van het kwartier uit de accu te blijven leveren; voeg geen energie
  toe aan het budget. Zonder afname of bij de reservegrens blijft uit toegestaan.
- Voorkom tegelijk dat een verouderde cyclusstatus de accu eindeloos blokkeert.
  **Expliciet akkoord van de gebruiker:** na het laatste rendabele laadvenster
  van die dag mag de avondontlading beginnen, ook als tegenvallende zon een
  volledige lading onmogelijk heeft gemaakt.

## 4. Veilige ondergrens en extra reserve

- De normale minimum-SOC is de absolute veilige ondergrens. In de aangeleverde
  voorbeelden is dit **20%** bij een accu van **10 kWh**. Dit zijn voorbeeldwaarden,
  geen universele vaste instellingen.
- Als een volgend rendabel laadvenster in de uitvoerbare planning beschikbaar is, mag de extra reserve
  worden vrijgegeven en moet zoveel mogelijk bruikbare energie vóór dat venster
  worden benut, tot de veilige ondergrens waar vermogen, afname en rendement
  dat toelaten. Maak geen onrendabele export noodzakelijk om de accu leeg te krijgen.
- Is er geen geschikte aanvulling, houd dan een **instelbaar extra percentage**
  vast. In recente metingen is dit 60%.
- Alleen aanwezig zonlicht is bij de nieuwste prijsgerichte eis onvoldoende:
  een daadwerkelijk geselecteerd geschikt laadvenster moet de aanvulling dragen.
- De extra reserve voorkomt verdere ontlading; zij verplicht niet tot duur
  bijladen als de accu al onder dat percentage zit.

## 5. Onzekerheid in zon en verbruik

- Houd rekening met werkelijk gemeten verbruik en actuele SOC. Herbereken
  wanneer verbruik of prognoses veranderen.
- Bij onverwacht hoger verbruik moet rendabel aanvullend netladen kunnen
  worden gezocht in de goedkoopste nog beschikbare geschikte uren.
- De gebruiker vraagt een veiligheidsmarge waardoor laden en ontladen iets
  eerder kunnen beginnen. Voorbeeld: de zon valt rond 18:00 tegen en het verbruik
  is hoog; voorkom dat star wachten op een gepland schakelmoment onnodig tot
  dure netafname leidt.
- Uitwerking: een instelbare zonneprognosemarge verlaagt de zon in de gehele
  accuplanning (standaard 20% minder). De bestaande verbruiksmarge rekent
  standaard met 20% meer vraag. Een expliciet opgeslagen 0% blijft uit. Dit is
  geen vaste tijdverschuiving of nieuwe regeling op onmiddellijk gemeten vermogen.
- Garandeer niet dat iedere onverwachte verbruikspiek kan worden opgevangen:
  vermogen, capaciteit, beschikbare sensoren en verstreken goedkope uren tellen mee.

## 6. Controle en werkwijze

- Gebruik op verzoek van 27 september een herbruikbaar visualisatiescript:
  `scripts/visualize_battery.py`, met een vaste template. Nieuwe metingen en
  berekende vergelijkingsplannen worden als data ingelezen, niet telkens als
  nieuw geschreven grafiek. Dit script toont plannen; het berekent ze niet.
- Reproduceer fouten met de aangeleverde sensorinformatie; bewaar geschikte,
  noodzakelijke regressiedata in `tests/fixtures`.
- Test opeenvolgende herberekeningen met veranderende SOC en bewaarde
  cyclusstatus. Alleen een goed ogende eenmalige planning is onvoldoende.
- Controleer onafhankelijk de energie-inhoud uit opdrachten, duur en vermogen:
  niet onder de veilige grens, niet boven vol, geen overlappende laadopdrachten.
- Test onder andere geen zon, veel zon, weinig zon, negatieve prijzen,
  veranderend verbruik, tariefwijzigingen, dagovergangen en hervatten na herstart.
- **Vóór iedere commit/push:** toon een grafiek in de opzet van de Lovelace-card,
  met prijzen, zonne- en verbruiksprognose, laad-/ontlaadvensters en berekende SOC.
  Benoem of het echte aangeleverde prognoses of synthetische testscenario's zijn.
- Wacht op het akkoord van de gebruiker over dat concrete resultaat.
  Daarna mogen de bijbehorende wijzigingen naar `main` worden gecommit en gepusht.
- Vermeld of alleen lokaal/simulatie is getest of ook in Home Assistant.
  Gepusht betekent niet dat de draaiende integratie al is bijgewerkt.

## 7. Voorlopige planning bij ontbrekende prijzen

- Op verzoek van 25 september 2026: toon alvast morgen wanneer Nord Pool nog
  geen prijzen heeft gepubliceerd, met het gemiddelde van de laatst bekende dag.
- Gebruik het tijdgewogen gemiddelde van de laatste bekende 24 uur; bij een
  onvolledige reeks alleen de beschikbare waarnemingen. Behoud negatieve prijzen.
- Markeer tarieven en planning duidelijk als schatting. De voorlopige planning
  mag geen zekere winst suggereren of vandaag alvast de extra reserve vrijgeven.
- Bereken de schatting vanuit de energie die de echte planning overlaat, met
  dezelfde cyclusvergrendeling, veilige ondergrens en minimumwinst.
- Nieuwste afspraak van 25 september: plan **twee opeenvolgende cycli** als
  uitvoerbare planning (laden en daarna ontladen, of omgekeerd). De lopende
  cyclus telt mee. Pauzes en wisselingen tussen zonne- en netladen tellen niet
  als nieuwe cyclus. De grens mag over middernacht lopen.
- Alles na die twee cycli is een afzonderlijke, speculatieve vooruitblik, ook
  bij bekende prijzen. Dit vervangt het tussentijds besproken kalenderdagmodel.
- De vooruitblik mag de uitvoerbare opdrachten, reserve, cyclusvergrendeling
  of opgeslagen status niet wijzigen. Bekende latere tarieven blijven nodig
  als economische onderbouwing voor de minimumwinst van het uitvoerbare laden.
- Begin de vooruitblik met de energie en cyclusstatus die de uitvoerbare twee
  cycli naar verwachting overlaten. Herbereken de uitvoerbare planning steeds
  met de werkelijke SOC; neem de vooruitblik niet over als vastgezette opdracht.
- Markeer de planning als vooruitblik en maak afzonderlijk duidelijk of de
  prijzen bekend zijn of met het daggemiddelde zijn geschat.

## 8. Bekende fouten die niet mogen terugkomen

- Het huidige kwartier bij iedere update blokkeren met een oude laadstatus,
  waardoor ontladen steeds naar het volgende kwartier verschuift.
- Een laadstart door afronding steeds milliseconden na `nu` plaatsen, zodat de
  werkelijke strategie permanent uit blijft.
- Netladen alleen toestaan binnen een klein zonnevenster, of geheel blokkeren
  terwijl de accu vóór een later goedkoop venster wel leeg kan zijn.
- Een geplande zonnebijdrage als gegarandeerd beschouwen en benodigd netladen
  daardoor overslaan, of dezelfde tijd dubbel als zon- en netladen gebruiken.
- De extra reserve vasthouden ondanks een rendabel volgend laadvenster.
- Bij herberekenen al verstreken kwartierminuten opnieuw als toekomstig
  verbruik meetellen.
- Een latere zonnecyclus plannen alsof er altijd ruimte is voor een volledig
  lege accu, terwijl er nog energie van de vorige cyclus over is.
- Bij het bereiken van 100% SOC de volgende rendabele laadcyclus verliezen:
  sluit eerst de lopende laadcyclus af en plan ontladen gevolgd door laden.
  Een mogelijke aanvulling mag pas starten als de energiesimulatie bevestigt
  dat de ontlaadcyclus voltooid is. Neem uitvoerbare, rendabele export van
  werkelijk overtollige energie mee; eigen verbruik alleen kan te weinig zijn.
- Laadvensters die wegens een volle accu worden overgeslagen mogen niet
  worden geteld alsof er inmiddels een nieuwe lege accu is ontstaan.

## 9. Recorder en actuele kaartgegevens

- Uitgebreide prijs-, verbruiks-, zonne- en planningsreeksen moeten actueel
  beschikbaar blijven voor de Lovelace-card en automatiseringen.
- Sla deze reeksen niet bij iedere update opnieuw op in Recorder; behoud de
  sensorstanden en compacte attributen in de geschiedenis.
- Lokaal geïmplementeerd via `_unrecorded_attributes` op de gedeelde
  accuplannersensor. Dit geldt ook voor de strategie- en verbruikssensor.
- Lokaal gecontroleerd met prognosedata boven de 16384-bytegrens: de actuele
  reeksen blijven intact en de gefilterde attributen passen onder de grens.
  Live controle na installatie/herstart staat nog open.

## Werkstatus bij laatste bijwerking

### Goedgekeurde correctie: onvolledige daglading — 5 oktober

- Gewenste werking bevestigd: het resterende kleine zonneoverschot mag de
  benodigde rendabele netaanvulling voor de lopende laadcyclus niet blokkeren.
- De aangeleverde meting van 14:32, 83% SOC, is gereproduceerd met de echte
  import- en exportprijzen en prognoses. De selectie gebruikte zonnelading van
  6 oktober voor de lopende daglading; de uitvoerbare planning hield daarvan
  alleen 0,064 kWh op 5 oktober over, zonder netaanvulling.
- Lokaal aangepast: een door zon bepaald daglaadvenster selecteert energie
  binnen die dag. Latere tarieven blijven beschikbaar voor de winsttoets;
  door netprijzen bepaalde nachtcycli kunnen nog over middernacht lopen.
- Tijdens een lopende laadcyclus wordt geen fictieve huisontlading tijdens
  laadpauzes bij de benodigde capaciteit opgeteld. Een lopende netlaadcyclus
  blijft ook tijdens pauzes/herstart herkenbaar via de bewaarde cyclusstatus
  en inkoopprijs; minimumwinst wordt opnieuw getoetst.
- Een geselecteerde netlaadopdracht in het lopende kwartier begint direct,
  zodat afronding van SOC de strategie niet een fractie na nu op uit laat staan.
- Nieuwe regressies: vandaag vol vóór avondontlading, vermogen en onafhankelijke
  energiegrenzen, behouden volgende cyclus in de vooruitblik, opeenvolgende
  vijfminutenupdates en herstart tijdens een tariefpauze.
- Grafiek gebruikt de aangeleverde prognoses zonder nogmaals marges toe te
  passen. Aannames: 2,5 kW laden en 3 kW ontladen; 10 kWh capaciteit, veilige
  grens 20%, extra reserve 60% en minimumwinst EUR 0,08 komen uit de meting.
- Lokaal geslaagd: volledige reeks van 190 Python-tests, plus de aanvullende
  test voor netladen over middernacht, frontendtest en grafiekcontrole. De
  correctie bereikt met deze aannames 100% rond 16:45 en ontlaadt vanaf 17:00.
- De gebruiker heeft de concrete vergelijkingsgrafiek op 5 oktober goedgekeurd
  en opdracht gegeven deze correctie naar main te committen en pushen.
- Nog open: installatie en live werking in Home Assistant.

### Minikaart: zichtbare moduskleuren en historisch SOC — 5 oktober

- Nieuwe wens: achtergrondkleuren van de accuplanning beter onderscheidbaar;
  historische details moeten het accupercentage tonen in plaats van planner score.
- Lokaal aangepast: gekleurde modusbanden 28% dekking (was 12%), neutrale
  achtergrond 8%. Afgeronde hoeken en transparante kaart blijven behouden.
- Verduidelijking: moduskleuren horen uitsluitend in de achtergrond; prijsstaafjes
  mogen niet mee verkleuren. Elke prijsstaaf heeft nu een ondoorzichtige basis in
  dashboard-kaartkleur, met daarboven de eigen prijskleur en bestaande dekking
  voor historie/voorspelling. Dit voorkomt kleurmenging met de accuplanning en
  behoudt de vervaagde toekomstkleuren, zonder nieuwe CSS-browservereisten.
- Visuele configuratie heeft `soc_entity` (Accupercentage %). Recorder-geschiedenis
  van die sensor wordt in dezelfde vijfminutenaanvraag meegenomen. Historische
  details tonen de laatste geregistreerde stand op of vóór het begin van het uur,
  met expliciet tijdstip. Geen historisch SOC beschikbaar betekent een streepje;
  niet de huidige stand, planner score of een geschat percentage.
- Frontend- en browsercontroles slagen: historisch SOC, ontbrekende/ongeldige
  standen, kleurweergave op licht/donker thema en behoud van vijfminutenverversing.
  Voorbeeld gebruikt echte verbruikshistorie/prijzen, synthetische zon/prognose/SOC.
  De gebruiker heeft de gecorrigeerde grafiek op 5 oktober goedgekeurd en
  opdracht gegeven naar main te pushen. Live controle na installatie staat open.

### Gewenste minikaart NSPanel Pro — 2 oktober

- Aparte compacte Lovelace-kaart, grafiek maximaal 150 px hoog; altijd vandaag
  van 00:00 tot 23:59. Alleen stroomprijs, huisverbruik en zonneopbrengst.
- Vóór nu uitsluitend gemeten verbruik en zon; na nu voorspellingen in dezelfde
  kleuren, licht vervaagd (aanpassing op expliciet verzoek van 2 oktober).
  Het lopende kwartier wordt bij nu gesplitst. Ontbrekende metingen blijven leeg.
- Batterijmodus zichtbaar als achtergrondkleur. Selectie toont prijs, verbruik,
  zon en modus naast de kolom, rechts in de linkerhelft en links in de rechterhelft.
- Verbruiks- en zonnesensor selecteerbaar in de visuele kaartconfiguratie.
- Aanvulling 2 oktober: uiterlijk volgens het aangeleverde voorbeeld en de
  bestaande kaart: afgeronde prijskolommen met tijdgewogen uurgemiddelden,
  groen/geel/rood volgens dezelfde prijsquantielen, gele zonlijn, paarse
  stippellijn voor verbruik, dubbele schaal, Nu-lijn en witte selectierand.
  Toekomst behoudt de kleuren met lagere dekking; batterijmodus blijft een subtiele achtergrond.
- Gewijzigd 2 oktober: als details open zijn, sluit iedere tik op de kaart het
  venster en de selectierand. Bij gesloten details opent een tik op een kolom de waarden.
  Een update houdt een verborgen selectie verborgen. Pop-up behoudt de oorspronkelijke
  donkere kleur en wordt alleen transparanter; dit vervangt het verzoek om lichter.
- Lokaal geïmplementeerd als `custom:smart-energy-planner-mini-card` in de bestaande
  frontendresource. Meetreeksen uit Recorder voor W/kW-sensoren, kwartiergemiddelden;
  prognose-energie wordt naar gemiddeld kW omgerekend. Geen plannerwijziging.
- Nog te controleren: weergave en aanraken op het echte NSPanel, beschikbare
  meetgeschiedenis in Home Assistant. Voorbeeldgegevens zijn synthetisch.
- Lokaal geslaagd: frontendtests en browsercontrole op 320 en 480 px, maximaal
  150 px hoog, selectie links/rechts, sluiten via pop-up en behoud na updates.
  Voorspellingen behouden dezelfde kleuren met lagere dekking.
- De gebruiker heeft de laatste voorbeeldgrafiek op 2 oktober goedgekeurd en
  expliciet opdracht gegeven deze minikaart naar main te committen en pushen.
- Nieuwe correcties gevraagd na installatie: geen decoratieve kaartachtergrond;
  transparant met behoud van modusbanden. Schaaltekst volgt het dashboardthema.
  Eenheden mogen niet door de schaalwaarden staan. De entiteitskiezer moet bij
  Home Assistant-updates open blijven en de ingevoerde zoektekst behouden.
- Lokaal aangepast: blijvende picker-elementen in de mini-editor en extra ruimte
  boven de schalen. Live controle staat open.
- Nieuwe eis: lichte kaart voor het trage NSPanel; niet vaak verversen.
  Lokaal geïmplementeerd: maximaal één automatische verversing per vijf minuten,
  geen hertekenen bij losse hass-updates, pauze bij verborgen/losgekoppeld dashboard.
  Selectie reageert direct. Hierdoor kunnen Nu en gegevens maximaal vijf minuten
  achterlopen. Meetgeschiedenis gebruikt minimal_response en geparste meetwaarden
  worden gecachet. Aaneengesloten modusbanden delen één SVG-element.
- Lokaal gecontroleerd: frontendtests en browsercontrole van schaalruimte,
  behoud van picker-focus/zoektekst met een testpicker, en 500 hass-updates zonder
  extra hertekenen of geschiedenisaanvragen. Vijfminutenverversing en stoppen bij
  loskoppelen gecontroleerd. De echte HA-kiezer en NSPanel blijven live te testen.
- De gebruiker heeft de gecorrigeerde grafiek en lichte verversing op 2 oktober
  goedgekeurd en opdracht gegeven deze correcties naar main te pushen.
- Nieuw verzoek na installatie: geen kaartrand, kleine afgeronde modusbanden en
  vloeiende lijnen. Geïmplementeerd met rand/schaduw uit, radius 3 en kubische
  interpolatie met behoud van vlakke stukken en gaten in ontbrekende meetgegevens.
- Aangeleverde configuratie gebruikt `sensor.planner_score`,
  `sensor.zonnepanelen_pvenergytotal` en `sensor.totaal_zelf_gebruikte_energie`.
  De gebruiker bevestigde kWh: huisverbruik heeft state_class total en zon
  total_increasing. Recorder-meetreeksen zijn niet aangeleverd.
  De kaart ondersteunt nu ook Wh/kWh: tellerdelta gedeeld door meetduur levert
  gemiddeld kW; total_increasing-reset telt de nieuwe stand. Geen extrapolatie
  na de laatste tellerstand of vervanging door prognoses. Eén uur vóór middernacht
  wordt mee opgehaald om het eerste meetinterval te kunnen berekenen.
- Voor verstreken prijskolommen publiceert de planner de bekende prijzen vanaf
  middernacht in plaats van slechts het afgelopen uur. De planning zelf verandert niet.
  Live controle van deze correcties staat nog open.
- Nord Pool `sensor.nordpool_kwh_nl_eur_3_10_0` is aangeleverd. Een expliciet
  gekozen prijssensor krijgt in de minikaart voorrang voor de volledige dagprijzen;
  zonder bruikbare sensorprijzen blijft de plannerreeks de bron. Uurgemiddelden blijven.
- Lokaal geslaagd voor deze correcties: 188 Python-tests, frontendtests voor
  kWh-delta/reset/geen extrapolatie en aangeleverde Nord Pool-uurgemiddelden,
  browsercontrole voor sluiten op kolom/pop-up/as, randloze kaart, ronde modusbanden,
  kubische lijnen, lichte verversing en stabiele editor. Voorbeeldgrafiek bevat
  aangeleverde prijzen; zon en verbruik blijven synthetisch wegens ontbrekende historie.
- De gebruiker heeft op 2 oktober deze correcties goedgekeurd en gevraagd de titel
  weg te halen en daarna te pushen. Na uitleg dat alleen het testvoorbeeld een
  uitlegtekst heeft, heeft de gebruiker bevestigd dat verwijderen niet nodig is.
  De minikaart heeft geen titel. Publicatie naar main is goedgekeurd.
- Nieuwe melding 2 oktober: grote verbruikspieken in de minikaart. Concrete
  kaartfout gereproduceerd: bij state_class total werden negatieve tellerdelta's
  overgeslagen; latere stijgingen telden wel. Tijdelijke correcties in een
  berekende teller konden zo extra verbruik veroorzaken. total-delta's worden nu
  met hun teken samengenomen binnen de weergegeven periode. De bestaande
  total_increasing-resetbehandeling blijft apart. Geen pieken afkappen of
  tellerstanden willekeurig gladstrijken.
- Lokaal gecontroleerd met een synthetische total-teller: tijdelijke stijgingen
  en dalingen heffen elkaar op; 0,5 kWh per kwartier blijft 2 kW. Browservoorbeeld
  toont dezelfde invoer voor/na de correctie, met aangeleverde Nord Pool-prijzen
  en synthetische zon/verbruik. Frontend- en browsercontroles slagen.
- De aangeleverde CSV bevestigt de oorzaak voor de getoonde dag: de oude
  berekening reproduceert exact 49,7434 kW. Met dezelfde echte tellerhistorie
  bedraagt het hoogste kwartiergemiddelde 1,7207 kW. Vandaag bevat 886 metingen
  en 577 dalingen. Ook correcties met identieke tijdstempels komen voor; eerst
  wordt de laatste stand op dat tijdstip gekozen, vervolgens worden delta's
  berekend. Zo verdwijnen wijzigingen zonder duur niet uit de energiebalans.
- De regressie controleert de echte CSV, zowel het maximum als de totale
  weergegeven energie tegenover het geïnterpoleerde verschil tussen begin- en
  eindstand. De vergelijking gebruikt echte verbruiksgegevens en aangeleverde
  Nord Pool-prijzen; zon en toekomstige verbruiksprognose blijven synthetisch.
  Frontend- en browsercontroles slagen. Live controle na installatie staat open.
- De gebruiker heeft de grafiek met echte verbruikshistorie goedgekeurd en
  opdracht gegeven te pushen, na afronden van beide schalen links op één decimaal.
  De detailwaarden blijven op twee decimalen; alleen schaaltekst verandert.
- Nieuw verzoek: beide schalen links volledig verwijderen. Verbruik en zon als
  uurgemiddelden tonen, met vloeiende kubische interpolatie tussen uurpunten.
  Het huidige uur behoudt aparte gemeten en voorspelde delen op de Nu-grens.
  Detailwaarden gebruiken dezelfde uurgemiddelden. Ontbrekende data blijft leeg;
  energie-inhoud en vijfminutenverversing blijven behouden. Frontend- en
  browsercontroles slagen, inclusief energiebehoud met de aangeleverde CSV.
  De gebruiker heeft deze grafiek op 2 oktober goedgekeurd en opdracht gegeven
  naar main te pushen. Live controle na installatie staat nog open.
- Nieuw verzoek: geschiedenis en voorspelling vloeiend aan elkaar blenden.
  Lokaal aangepast: één doorlopende kubische lijn door de gemeten en voorspelde
  uurpunten, met ongewijzigde bronwaarden en detailwaarden. Gaten in ontbrekende
  gegevens blijven onderbrekingen. Vanaf Nu vervaagt de lijn geleidelijk over
  30 minuten naar de bestaande 60% dekking. Geen animatie of extra verversingen.
  De interpolatie rond Nu is een visuele verbinding tussen de twee bronnen;
  ze verandert de gemeten/voorspelde uurgemiddelden niet. Het laatste gemeten
  gemiddelde vormt het anker op Nu, waarna de lijn vloeiend naar het eerste
  bruikbare voorspelde uurpunt loopt. Een voorspeld punt binnen 15 minuten van
  Nu wordt voor de visuele verbinding overgeslagen; vlak vóór een uurgrens mag
  een paar seconden resterende voorspelling geen bijna verticale sprong veroorzaken.
  Detailwaarden en energie blijven ongewijzigd. De aangeleverde NSPanel-screenshot
  laat de oude verticale sprong in de gele lijn zien. Browsercontrole met bewust
  afwijkende synthetische zon en echte verbruikshistorie slaagt, ook voor het anker
  op Nu. Frontendtests slagen. De gebruiker heeft deze voorbeeldgrafiek op
  2 oktober goedgekeurd en opdracht gegeven naar main te pushen. Live controle
  na installatie staat nog open.


### Goedgekeurde correctie 27 september: doorladen binnen 0,5 cent prijsverschil

- De aangeleverde 53%-meting is exact gereproduceerd, inclusief de pauzes
  om 12:45 en 13:15. Zij bespaarden samen circa EUR 0,00146.
- De gebruiker heeft de band van EUR 0,005/kWh goedgekeurd. Binnen een
  aansluitende rendabele band verschuift dezelfde hoeveelheid laadenergie
  naar voren. Herberekeningen behouden een al begonnen laadfase.
- Met dezelfde prognose: zonneladen van 12:38 tot circa 15:42 in plaats
  van onderbroken tot 16:30; ontladen blijft vanaf 18:00. De prognoses zijn
  overgenomen zonder aanvullende marge; maximaal vermogen aangenomen 3 kW.
- De gebruiker heeft de grafiek en concrete wijziging op 27 september
  goedgekeurd voor commit en push naar main. Live verificatie staat nog open.
- Lokaal geslaagd: 163 Python-tests, kaarttest en browsercontrole van de
  vergelijkingsgrafiek. De tests omvatten opeenvolgende SOC-updates, grotere
  prijsverschillen en behoud van de minimumwinst bij netladen.

### Goedgekeurde correctie 26 september: netladen vult zonneladen aan

- De gebruiker bevestigde dat netladen zonne-energie niet vervangt. De oude
  selectie rekende ten onrechte het volledige laadvermogen tegen importtarief.
- Lokaal aangepast: vergelijk gemengde laadkosten en behoud de gezamenlijke
  vermogenslimiet. Controleer de minimumwinst van netaanvulling op het ongewogen
  importtarief. Bij afwijzing van de aanvulling blijft geschikte zonnelading over.
- Nieuwe controles: goedkope aanvulling boven duurdere latere zon, goedkopere
  zon behouden, minimumwinst niet verbergen met goedkope zon, opeenvolgende updates.
- De nieuwe 13:30-meting is afgebroken vóór de prognoses. De voorbeeldgrafiek
  gebruikt daarom de eerdere volledige 26-septemberprognose met 29% SOC;
  dit is geen exacte reconstructie van de nieuwste meting. Live verificatie
  staat nog open.
- Alle 160 Python-tests, de kaarttest en browsercontrole van de grafiek slagen.
  De gebruiker heeft tijdens het afronden expliciet gevraagd deze correctie
  te pushen; de voorbeeldgrafiek wordt bij de terugkoppeling meegeleverd.

### Goedgekeurde correctie 26 september: integratie-icoon

- Het bestaande icoon en logo moeten zichtbaar zijn in het integratieoverzicht.
- De bestaande PNG-bestanden zijn toegevoegd onder de door Home Assistant
  vanaf 2026.3 ondersteunde map `custom_components/smart_energy_planner/brand/`.
  Bestanden in de repositoryhoofdmap of direct naast manifest.json volstaan niet.
- PNG-formaat, leesbaarheid en gelijkheid aan de bestaande afbeeldingen zijn
  lokaal gecontroleerd. Controle in Home Assistant staat nog open.
- De gebruiker heeft op 26 september deze correctie goedgekeurd voor commit
  en push naar main.

### Goedgekeurde correctie 26 september: geen onnodige pauze na export

- Het energiebudget voor export en huisverbruik werd volledig op maximaal
  vermogen uitgevoerd, met daarna uit tot de kwartiergrens. Het budget moet
  het huis juist gedurende het hele geselecteerde kwartier kunnen bedienen.
- Lokaal aangepast: bereken de exportduur uit het overschot gedeeld door het
  vermogen boven het huisverbruik; vervolg direct met normaal ontladen.
- Gereproduceerd met de eerdere volledige-accumeting van 25 september.
  De nieuwste screenshot bevat geen sensorwaarden voor een exacte reconstructie.
- Gerichte controles voor de overgang, behoud van het energiebudget en terecht
  uitschakelen zonder huisverbruik slagen. Alle 156 Python-tests, de kaarttest
  en browsercontrole van de grafiek slagen. Live controle staat nog open.
- De gebruiker heeft de getoonde grafiek en deze correctie goedgekeurd en
  op 26 september toestemming gegeven voor commit en push naar main.

### Correctie 26 september: laadstart schuift bij updates vooruit

- Gereproduceerd met de meting van 26 september, 20% SOC: het lopende
  kwartier werd telkens uitgesloten. Een bewaarde ontlaadstatus voegde bovendien
  opnieuw 30 minuten wachttijd toe terwijl de bruikbare energie al op was.
- Lokaal aangepast: gebruik de resterende minuten van het huidige kwartier;
  pas de wachttijd voor ontladen alleen toe zolang er nog bruikbare energie is.
  De bestaande tolerantie van 0,05 kWh en de bescherming tegen pendelen blijven.
- Nieuwe regressiecontroles: zonneladen tot vol met opeenvolgende updates op
  onronde tijdstippen en berekende SOC, herstelde status bij 20%, netladen tussen
  kwartiergrenzen en niet bijladen tijdens een onvoltooide ontlaadcyclus.
- Lokaal geslaagd: 154 Python-tests, kaarttest, diffcontrole en browsercontrole
  van de vergelijkingsgrafiek op desktop- en telefoonbreedte.
- De gebruiker heeft voor deze correctie expliciet gevraagd direct na het
  oplossen en controleren te pushen; hiervoor is geen tweede akkoord nodig.
  De gebruikelijke grafiek blijft onderdeel van de terugkoppeling.
- Nog open: live verificatie na installatie in Home Assistant.

### Goedgekeurde correctie: volle accu behoudt volgende laadcyclus

- De gebruiker heeft op 25 september de reconstructie van de 100%-meting
  goedgekeurd en opdracht gegeven deze correctie naar main te pushen.
- De laadcyclus wordt vóór de nieuwe selectie afgesloten zodra de accu vol
  is. Uitvoerbare export van overtollige energie kan de volgende rendabele
  laadcyclus mogelijk maken; de energiesimulatie bewaakt de veilige ondergrens.
- Lokaal gecontroleerd: 150 Python-tests, kaarttest en browsercontrole.
  De vier cyclusregressietests zijn na uitbreiding met opeenvolgende updates
  van 99% naar 100% opnieuw geslaagd. Live verificatie staat nog open.

### Goedgekeurde wijziging: twee cycli en aparte vooruitblik

- Op 25 september heeft de gebruiker de grafiek met twee uitvoerbare cycli
  en een afzonderlijke vooruitblik goedgekeurd en commit/push naar main gevraagd.
- De lopende cyclus telt mee; pauzes en wisselingen tussen laadbronnen niet.
  De vooruitblik blijft apart, ook bij bekende prijzen, en wordt op een kopie
  van de plannerstatus berekend vanaf de verwachte energie aan de cyclusgrens.
- Lokaal gecontroleerd: 149 Python-tests, kaarttest en browsercontrole van de
  grafiek. Na de laatste grenscorrecties zijn de drie cyclusregressietests
  opnieuw geslaagd. Live verificatie na installatie staat nog open.

- Prijsgerichte bronkeuze met EUR 0,11 zonnevoordeel, negatieve netprijzen,
  reservevrijgave en cyclusvergrendeling zijn lokaal aangepast.
- Na het laatste rendabele laadvenster mag een onvolledige laadcyclus overgaan
  naar avondontlading. Minimumwinst en extra reserve blijven van toepassing.
- Beide laadbronnen wachten tijdens ontladen op de veilige ondergrens
  (bestaande tolerantie 0,05 kWh). Dit kan een goedkoop laadvenster laten vervallen.
  Ook toekomstige opdrachten controleren de daadwerkelijk gesimuleerde energie.
- De zonneprognosemarge werkt door in de gehele energiebalans. Standaard 20%;
  een expliciet opgeslagen 0% blijft uit. De bestaande vraagmarge blijft apart.
- Een rekenfout bij export is verholpen: eigen verbruik en export delen hetzelfde
  maximale ontlaadvermogen; export krijgt alleen het resterende vermogen.
- Lokaal gecontroleerd: 137 Python-tests, de frontend-cardtest, compilatie en
  diffcontrole. Tests omvatten opeenvolgende SOC-updates, de cyclusuitzondering,
  vroegere avondontlading en onafhankelijke controle van de energiegrenzen.
- De bijgewerkte grafiek bevat de aangeleverde prognose met en zonder zonmarge
  en duidelijk benoemde synthetische scenario's. Browsercontrole geslaagd.
- De gebruiker heeft op 24 september 2026 de grafiek goedgekeurd en expliciet
  opdracht gegeven deze wijzigingen naar main te committen en pushen.
  Dit document en AGENTS.md horen bij dezelfde goedgekeurde wijziging.
- **Nog open:** live verificatie in Home Assistant na installatie. De lokale
  tests en goedkeuring betekenen niet dat de draaiende integratie is bijgewerkt.

### Goedgekeurde wijziging na commit 13bce4b

- Instelbare belastingaftrek en consistente zonne-/terugleverprijs toegevoegd.
- Afzonderlijke voorlopige planning voor morgen en aanduiding in Lovelace toegevoegd.
- Lokale eindcontrole geslaagd: 145 Python-tests, frontend-cardtest, compilatie,
  diffcontrole en browsercontrole van de grafiek. De grafiek is een reconstructie
  met eerder aangeleverde data, geen nieuwe live meting.
- De gebruiker heeft op 25 september 2026 de grafiek goedgekeurd en opdracht
  gegeven deze wijzigingen naar main te committen en pushen.
- Nog open: installatie en live verificatie in Home Assistant.

### Lokale correctie 2 oktober: laadvenster bij onvermijdelijk restant

- De 26%-meting is gereproduceerd met een aangenomen historische inkoopprijs
  van EUR 0,30/kWh. Die prijs ontbreekt in de sensorinformatie; vanaf EUR 0,29
  ontstaat dezelfde ochtendplanning zonder laadvenster. Dit bevestigt een
  mogelijke oorzaak, niet de exacte opgeslagen inkoopprijs in Home Assistant.
- De zon dekt vanaf 09:00 het huisverbruik. Ongeveer 0,49 kWh boven de veilige
  grens kan daardoor vóór het zonneladen niet worden benut; export haalt de
  oude winstgrens niet. De volledige-cyclusblokkering schrapte het laadvenster.
- De geïmplementeerde uitzondering controleert de hele geselecteerde laadfase:
  geen verdere huisafname en geen bekende rendabele export. Dan mag de accu
  het restant behouden en de vrije capaciteit aanvullen. Bij mogelijke verdere
  ontlading blijft de cyclusbescherming actief. De prognose wijzigt geen
  huidige cyclusstatus vóór de daadwerkelijke laadstart.
- Een theoretische ontlaadwachttijd wordt niet telkens opnieuw vooruit gezet
  als er gedurende die wachttijd geen bruikbare afname is. Opeenvolgende
  herberekeningen behouden zo de laadstart.
- Reconstructie: huisontlading tot 09:00, circa 24,9% over, zonneladen
  11:45–15:59 tot 100%. Aannames: 10 kWh, 20% veilige grens, laden 2,5 kW,
  ontladen 3 kW, aangeleverde prognoses zonder aanvullende marges. De meting
  gebruikt EUR 0,12 verschil tussen import en export; de replay volgt dit.
- Lokaal geslaagd: volledige suite van 187 tests; daarna de vijf gerichte
  restanttests inclusief een aanvullende hersteltest (188 unieke tests).
  Kaarttest, diffcontrole en browsercontrole op desktop en mobiel geslaagd.
  De oude 75%-regressie is expliciet aangepast aan de nieuwe uitzondering:
  ook daar mag een onvermijdelijk restant de zonnecyclus niet meer blokkeren.
- De gebruiker heeft deze concrete grafiek op 2 oktober goedgekeurd en opdracht
  gegeven de correctie naar main te committen en pushen.
- Nog open: installatie en live verificatie in Home Assistant.

### Lokale correctie 27 september: rendement bij werkelijk gekozen laadstart

- Een vroeg prijsanker kon op de vorige dag liggen terwijl de geselecteerde
  laaduren pas morgen vallen. De rendementscontrole gebruikte daardoor te veel
  bestaande energie en verwierp het goedkope blok. De voorraad wordt nu op de
  daadwerkelijk geselecteerde laadstart geraamd. Minimumwinst en de controle
  op gesimuleerde ontlading blijven bestaan.
- De meting van 16:54 is gereproduceerd: laden verschuift van 15:15–18:27 naar
  11:30–14:30 en 15:00–15:12. Replay met aangeleverde prognoses, 10 kWh, veilige
  grens 20%, maximaal laden 2,5 kW (8 kWh / 3,2 uur), ontladen 3 kW.
- 165 plannertests en de kaarttest geslaagd, plus twee tests van de herbruikbare
  visualisatie. Browsercontrole met vergelijking en mobiele breedte geslaagd.
- De gebruiker heeft deze grafiek op 27 september goedgekeurd en opdracht
  gegeven de correctie en het visualisatiescript naar main te committen en pushen.
- Nog open: installatie en live verificatie in Home Assistant.

### Lokale wijziging 29 september: bestaande stroom voor eigen verbruik

- Op expliciet akkoord blokkeert de historische inkoopprijs huisverbruik niet meer.
  Nieuwe netlading en export houden de winstcontrole. Veilige ondergrens, extra
  reserve en cyclusvergrendeling blijven behouden. Tests zijn aan deze afspraak
  aangepast; blokkeren van onvoltooide ontlaadcycli blijft getest.
- Replay met 75% SOC en aangenomen historische inkoop EUR 0,322, 2,5 kW laden
  en 3 kW ontladen: huisverbruik tot de 60%-reserve. Geen laadvenster vandaag,
  wegens te weinig afname vóór de zonuren en behoud van exportbescherming.
- Een synthetisch scenario met voldoende eigen verbruik voltooit de ontlading
  wel vóór een nieuw laadvenster. Herhaalde updates en reserve zijn getest.
- Lokaal: 170 Python-tests, frontendtest en browsercontrole geslaagd.
- De gebruiker heeft de concrete grafiek op 29 september goedgekeurd en
  opdracht gegeven deze wijziging naar main te committen en pushen.
- Nog open: installatie en live verificatie in Home Assistant.


### Lokale wijziging 29 september: beperkte nachtcyclus vóór goedkoper dagladen

- Geïmplementeerd: een noodzakelijke nachtelijke netlaadcyclus koopt alleen
  voldoende voor rendabele afname vóór een goedkoper volgend laadmoment.
  Deze bewust gedeeltelijke cyclus mag daarna ontladen zonder eerst 100% te
  bereiken. De minimumwinst blijft gelden voor deze nieuwe netenergie.
- De gedeeltelijke cyclus wordt bewaard bij updates en een herstart. Een
  lopende gemengde daglading wordt hierdoor niet in kleine cycli opgesplitst.
- Replay van de meting van 29 september 18:59 (68%): bestaande energie gaat
  naar huisverbruik; geen onnodige nachtelijke aankoop. Op 30 september volgt
  zonneladen vanaf circa 09:16, zon met netaanvulling 11:45–13:30 en vervolgens
  zonneladen tot 16:00. Aannames: 10 kWh, veilige grens 20%, laden 2,5 kW,
  ontladen 3 kW; geen historische inkoopprijs in de aangeleverde meting.
- Synthetisch met lege bruikbare accu: beperkte nachtlading, rendabele
  ochtendontlading en opnieuw dagladen. Ook met vijfminutenupdates en herstel
  van opgeslagen cyclusgegevens getest; de energie blijft binnen de grenzen.
- Lokaal geslaagd: 173 Python-tests, frontendtest en browsercontrole van de
  vergelijking, inclusief wisselen van planning en mobiele breedte.
- De gebruiker heeft deze concrete grafiek op 29 september goedgekeurd en
  opdracht gegeven de wijziging naar main te committen en pushen.
- Nog open: installatie en live verificatie in Home Assistant.


### Lokale wijziging 30 september: volledige rendabele cycli

- De uitzondering voor gedeeltelijke nachtlading uit 846c3d8 is vervangen.
  Oude opgeslagen partial_grid_cycle-velden worden niet meer toegepast.
- Extra laadcycli vóór een goedkopere aanvulling moeten voldoende bekende,
  rendabele ontlaadcapaciteit hebben voor de volledige bruikbare accu.
  Eigen verbruik gebruikt het importtarief, export het teruglevertarief;
  beide delen het maximale ontlaadvermogen. Een enkele prijspiek volstaat niet.
- Een eerdere volledige nachtcyclus kan nu vóór het goedkopere dagminimum
  worden geselecteerd wanneer rendabele ochtendafname/export de accu kan legen.
  Voltooide cycli mogen ook op dezelfde dag opvolgen; de simulatie controleert
  bij elke laadstart de werkelijk resterende energie. Meer dan twee richtingen
  blijft als afzonderlijke vooruitblik weergegeven volgens de bestaande afspraak.
- De eerdere volledige meting van 29 september 18:59, 68% SOC, geeft geen
  rendabele extra volledige nachtcyclus. Dagladen blijft behouden. De nieuwste
  screenshot bevat onvoldoende invoer voor een exacte reconstructie daarvan.
- Synthetisch met een ochtendimportprijs van EUR 0,60 (export EUR 0,49): volledig
  nachtladen, rendabele ochtendontlading tot 20% en opnieuw dagladen tot 100%.
  Ook dagladen/avondontlading/nachtladen/ochtendontlading/dagladen is getest.
- Lokaal: 176 Python-tests en de kaarttest geslaagd. De cycluscontroles omvatten
  vijfminutenupdates, herstart, oude gedeeltelijke status, een te korte prijspiek,
  minimumwinst bij export en onafhankelijke energiegrenzen. Browsercontrole van
  beide grafieken en mobiele breedte geslaagd. Grafieken gebruiken 10 kWh,
  20% veilige grens, 2,5 kW laden en 3 kW ontladen, zonder extra prognosemarge.
- De gebruiker heeft beide grafieken en deze concrete wijziging op 30 september
  goedgekeurd en opdracht gegeven naar main te committen en pushen.
- Nog open: installatie en live verificatie in Home Assistant.


### Lokale correctie 1 oktober: opstarten zonder geldige SOC

- Gereproduceerd: meerdere updates zonder SOC initialiseren een lege accu en
  activeren vervolgens de laadvergrendeling. Bij terugkomst van 39% SOC blijft
  de ochtendontlading daardoor geblokkeerd. Met de aangeleverde meting en een
  behouden ontlaadstatus is ontladen wel mogelijk. Zonder opstartlog is niet
  bewezen dat precies deze volgorde in de draaiende installatie is opgetreden.
- Zonder SOC geeft de planner nu een wachtresultaat met accu_uit en geen
  opdrachten. Cyclusstatus en het opgeslagen tijdstempel worden niet gewijzigd.
  Na terugkomst van de sensor wordt opnieuw met de echte SOC gerekend.
- Ook herstelt de integratie nu de opgeslagen battery_grid_charge_price vanuit
  de opslag bij setup; dit veld werd eerder wel opgeslagen maar niet ingelezen.
- Nieuwe regressies: herhaalde ontbrekende SOC bij koude start, behouden laad-
  en ontlaadrichting, en JSON-opslag gevolgd door een nieuw runtime-object met
  de echte setup-veldtoewijzing. Bij gelijke invoer is het volledige uitvoerbare
  plan plus vooruitblik gelijk vóór en na herstel, inclusief inkoopprijs.
- De meting van 1 oktober 08:34, 39%: herstelde ontlading tot 11:45, daarna
  netladen tot vol en avondontlading in de vooruitblik. Grafiek rekent met
  10 kWh, veilige grens 20%, 2,5 kW laden, 3 kW ontladen, EUR 0,11 aftrek en
  aangeleverde prognoses zonder extra marge. Historische inkoopprijs ontbreekt
  in de meting; voor deze grafiek is geen historische exportprijsgrens aangenomen.
- Lokaal: 179 Python-tests, kaarttest en browsercontrole van vergelijking en
  mobiele breedte geslaagd.
- De gebruiker heeft deze concrete grafiek op 1 oktober goedgekeurd en opdracht
  gegeven de correctie naar main te committen en pushen.
- Nog open: installatie en live verificatie in Home Assistant.


### Lokale wijziging 1 oktober: minder schakelen binnen één cent

- Laden bundelt geplande energie naar voren binnen aaneengesloten rendabele
  blokken met dezelfde bron en hoogstens 1 cent totale prijsvariatie. Dit
  gebeurt ook bij toekomstige cycli en bij meerdere prijsbanden in één cyclus.
- Huisontlading gebruikt binnen 1 cent van elke resterende prijspiek eerst
  eerdere afname. Daardoor blijft de noodzakelijke uitperiode meer bijeen.
  Geen extra export om een pauze cosmetisch weg te werken. Grotere verschillen,
  ontbrekende afname, winstgrenzen en veilige SOC kunnen pauzes blijven vereisen.
- De extra volledige arbitragecyclus wordt na bundeling opnieuw gecontroleerd
  op voldoende rendabele afname tegen de nieuwe maximale importprijs.
- Reconstructie met de meting van 1 oktober 08:34, 39% SOC: netladen wordt
  teruggebracht van zes naar drie blokken; het eerste loopt 11:45 tot circa
  13:11. Zelfde aannames als de herstartgrafiek: 10 kWh, veilige grens 20%,
  2,5 kW laden, 3 kW ontladen, EUR 0,11 aftrek, geen extra prognosemarge en
  geen aangeleverde historische inkoopprijs.
- Lokaal geslaagd: 183 Python-tests, kaarttest, diffcontrole en browsercontrole
  op desktop- en telefoonbreedte. Inclusief toekomstige netlading, exact 1 cent,
  grotere verschillen, winstgrens, herhaalde herberekeningen en energiebalans.
- De gebruiker heeft deze concrete grafiek op 1 oktober goedgekeurd en opdracht
  gegeven de wijziging naar main te committen en pushen.
- Nog open: installatie en live verificatie in Home Assistant.
