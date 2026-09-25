# Eisen aan de accuplanner

Dit is de blijvende afsprakenlijst voor werkzaamheden aan de accuplanner.
De eisen hieronder komen uit het gesprek met de gebruiker. Ze beschrijven het
gewenste gedrag; ze zijn geen verklaring dat alles al geïmplementeerd of live
geverifieerd is. Lees ook de open punten onderaan.

## 1. Prijs bepaalt het laden

- Vergelijk netladen en zonneladen op hun effectieve prijs. Zonneoverschot krijgt
  niet automatisch voorrang en is geen voorwaarde om een laadcyclus te plannen.
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

## 2. Rendement en volledige laadcyclus

- Houd rekening met de instelbare minimumwinst per kWh en bekende latere
  tarieven. Toon ook het bijbehorende latere ontladen; een laadplan zonder
  onderbouwing aan de afnamekant is onvoldoende.
- Een geldige laadcyclus streeft naar een volle accu, rekening houdend met
  verwachte ontlading vóór de cyclus, zonnebijdrage en het maximale laadvermogen.
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
- Voorkom tegelijk dat een verouderde cyclusstatus de accu eindeloos blokkeert.
  **Expliciet akkoord van de gebruiker:** na het laatste rendabele laadvenster
  van die dag mag de avondontlading beginnen, ook als tegenvallende zon een
  volledige lading onmogelijk heeft gemaakt.

## 4. Veilige ondergrens en extra reserve

- De normale minimum-SOC is de absolute veilige ondergrens. In de aangeleverde
  voorbeelden is dit **20%** bij een accu van **10 kWh**. Dit zijn voorbeeldwaarden,
  geen universele vaste instellingen.
- Als een volgend rendabel laadvenster beschikbaar is, mag de extra reserve
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
- Vervang de voorlopige planning zodra echte prijzen beschikbaar komen.

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
