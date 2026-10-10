# Werkinstructies voor deze repository

Lees bij iedere taak aan de accuplanner eerst `docs/accuplanner-eisen.md`.
Dit document bevat de door de gebruiker afgesproken werking, eerdere fouten en
de verplichte controle vóór publicatie. Pas bestaande eisen niet stilzwijgend
aan om een nieuwe wijziging of test te laten passen.

Werk het document bij wanneer de gebruiker een eis toevoegt of wijzigt. Maak
onderscheid tussen gewenste werking, geïmplementeerde werking en nog te testen
punten. Een nieuw expliciet gebruikersverzoek gaat voor op het document.

Vóór committen of pushen: toon de gebruiker de berekende planning als grafiek
in de opzet van de Lovelace-card en wacht op akkoord over de concrete wijziging.
Toestemming voor een eerder voorstel geldt niet automatisch voor later gewijzigde
werking. Commit en push na akkoord naar `main`, tenzij de gebruiker anders vraagt.
Neem geen ongerelateerde wijzigingen mee.

Houd controles compact (gebruikersverzoek 9 oktober): draai eerst alleen de
regressies voor het gewijzigde gedrag. Draai de volledige suite alleen bij brede
plannerwijzigingen of concrete twijfel over neveneffecten, niet opnieuw vóór
een push als de gecontroleerde code gelijk is gebleven. Bewaar uitgebreide logs
in een bestand en toon alleen aantallen en fouten. Hergebruik
`scripts/visualize_battery.py` en de vaste template; schrijf geen nieuwe grafiek
per meting. Herhaal frontend-/browser-/mobiele controles alleen als de kaart,
template of grafiekgenerator verandert, of als er een concrete weergavefout is.
