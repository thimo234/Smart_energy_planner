# Accuplanning visualiseren

Gebruik steeds hetzelfde script en dezelfde grafiek; alleen de invoer verandert.
Het script leest een volledige Home Assistant-sensor als YAML/TXT of JSON.
Een JSON-object met alleen de attributen werkt ook. YAML vereist `PyYAML`.

```powershell
python scripts/visualize_battery.py sensor.txt --charge-kw 2.5 --discharge-kw 3 --output planning.html
python scripts/visualize_battery.py nieuw.json --compare sensor.txt --charge-kw 2.5 --discharge-kw 3 --output vergelijking.html
```

De uitvoer is een herbruikbaar inline grafiekfragment voor de chat. De ingebouwde
visualize-renderer kan dit desgewenst omzetten naar een zelfstandige browserpagina.
De template staat in `scripts/templates/battery_chart.html`; verander de template
niet voor iedere nieuwe meting. Bewaar gegenereerde grafieken buiten Git.

Het script toont prijzen, zon, verbruik, modusvensters en onafhankelijk berekende
SOC. Het voert **geen nieuwe plannerberekening** uit. Voor een codewijziging levert
de regressiereplay de nieuwe modusvensters aan als JSON, met dezelfde prognoses.
De uitvoerbare planning en de aansluitende vooruitblik worden beide ingelezen;
de start van de vooruitblik staat onder de grafiek.

Geef de werkelijk ingestelde vermogens op. Netladen omvat zon plus netaanvulling
binnen hetzelfde laadvermogen. Prognoses worden gebruikt zoals aangeleverd, zonder
nogmaals veiligheidsmarges toe te passen. SOC wordt niet afgeknipt op 0 of 100%,
zodat een onmogelijke planning zichtbaar blijft. De berekening modelleert dezelfde
tarief-/energiesystematiek als de planner, zonder omzettingsverliezen.

Ontbrekende prognose-uren of overlappende modusvensters geven een foutmelding in
plaats van stilzwijgend een verzonnen planning. Bij 0% SOC moet de invoer ook
`battery_capacity_kwh` bevatten. De grafiek gebruikt D3 vanaf een CDN.
