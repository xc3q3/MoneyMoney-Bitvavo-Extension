# 💱 MoneyMoney Bitvavo Extension

[MoneyMoney](https://moneymoney-app.com/)-Erweiterung zur Anbindung deines [Bitvavo](https://bitvavo.com/de/)-Kontos.

> **Read-only** – die Erweiterung liest nur. Keine Trades, keine Transfers, kein Risiko.

---

## ✨ Funktionsumfang

- **Verrechnungskonto (EUR)** – zeigt alle EUR-wirksamen Buchungen: Käufe, Verkäufe, Ein- und Auszahlungen
- **Depot** – zeigt deine aktuellen Krypto-Bestände mit aktuellem EUR-Kurs
- **Automatischer Abgleich** – inkrementelle Sync-Updates bei jedem Refresh

---

## 📋 Voraussetzungen

- [MoneyMoney](https://moneymoney-app.com/) für macOS
- Ein [Bitvavo](https://bitvavo.com/de/)-Konto
- Ein Bitvavo API Key mit **ausschließlich Leserechten**

---

## 🔑 API Key erstellen

1. In Bitvavo einloggen und zu **Einstellungen → API** navigieren.
2. Einen neuen API Key erstellen.
3. **Nur folgende Berechtigung aktivieren: `Lesen`** – alle anderen Berechtigungen deaktiviert lassen.
4. API Key und API Secret notieren (das Secret wird nur einmal angezeigt).

> ⚠️ **Sicherheitshinweis:** Vergib dem Key keine Trade- oder Withdrawal-Rechte. Die Erweiterung benötigt ausschließlich Lesezugriff.

---

## 🛠️ Installation

1. [`bitvavo.lua`](bitvavo.lua) herunterladen.
2. In MoneyMoney: `Hilfe` → `Zeige Datenbank im Finder`.
3. Datei in den Ordner **`Extensions`** kopieren.
4. Falls eine Fehlermeldung zu unsignierten Erweiterungen erscheint: `Einstellungen` → `Extensions` → Signaturprüfung für unsignierte Erweiterungen deaktivieren.

---

## ⚙️ Konto einrichten

1. In MoneyMoney: `Konto` → `Konto hinzufügen`.
2. Im Eingabefenster unter **Andere** den Eintrag `Bitvavo` auswählen.
3. Zugangsdaten eingeben:

   | Feld | Wert |
   |---|---|
   | **Benutzername** | Dein Bitvavo API Key |
   | **Passwort** | Dein Bitvavo API Secret |

4. Konto speichern – es erscheinen automatisch zwei Konten: **Verrechnungskonto** und **Depot**.

---

## 📊 Was du siehst

### Verrechnungskonto
Alle Buchungen, bei denen EUR fließt:

| Buchungstyp | Beispiel |
|---|---|
| Kauf | `Kauf BTC` – 0,005 BTC (Kurs: 85.000 EUR, Gebühr: 1,25 EUR) |
| Verkauf | `Verkauf ETH` – 1,2 ETH (Kurs: 2.300 EUR, Gebühr: 1,10 EUR) |
| Einzahlung | `Einzahlung` – 500,00 EUR |
| Auszahlung | `Auszahlung` – 250,00 EUR |

Reine Krypto-zu-Krypto-Transaktionen erscheinen hier **nicht** – sie haben keinen EUR-Effekt.

### Depot
Alle Krypto-Positionen mit aktuellem EUR-Kurs:
- Menge (verfügbar + in offenen Orders)
- Aktueller Marktpreis in EUR (live von Bitvavo)
- Gesamtwert in EUR

Die EUR-Bewertung setzt einen aktiven `<SYMBOL>-EUR`-Markt auf Bitvavo voraus.

---

## ⏱️ Erstabgleich

Beim ersten Abgleich wird die gesamte Bitvavo-Historie geladen. Das kann je nach Kontoalter **einige Minuten** dauern – Bitvavo limitiert API-Anfragen, und die Erweiterung wartet automatisch zwischen den Anfragen, um Rate-Limits nicht zu überschreiten.

Um Laufzeit und API-Last zu begrenzen, werden beim Erstabgleich standardmäßig maximal **12 EUR-Märkte** berücksichtigt. Priorität haben dabei:

1. `BTC-EUR`
2. `ETH-EUR`
3. `SOL-EUR`
4. Weitere Märkte aus deinen Beständen und deiner Historie (alphabetisch)

Falls der Saldo nach dem Erstabgleich nicht exakt mit Bitvavo übereinstimmt, korrigiert die Erweiterung die Differenz automatisch mit einer **`Eröffnungsabgleich`**-Buchung. Das ist kein Fehler – der angezeigte EUR-Saldo entspricht damit immer dem tatsächlichen Bitvavo-Saldo.

---

## 🔧 Erweiterte Einstellungen

Diese Parameter befinden sich direkt am Anfang der `bitvavo.lua` und können bei Bedarf angepasst werden. Für den normalen Funktion ist das **nicht notwendig**.

### `MAX_BACKFILL_MARKETS`

```lua
local MAX_BACKFILL_MARKETS = 12
```

Begrenzt beim Erstabgleich die Anzahl der EUR-Märkte, deren Handelshistorie abgerufen wird. Höhere Werte erfassen mehr Trades lückenlos, verlängern aber die Sync-Dauer entsprechend. Werte zwischen `10` und `20` sind für die meisten Konten sinnvoll.

### `FORCE_FULL_SYNC`

```lua
local FORCE_FULL_SYNC = false
```

Erzwingt beim nächsten Abgleich eine vollständige Neuladung der gesamten Historie – so als würde die Erweiterung zum ersten Mal eingerichtet. Sinnvoll, wenn der Kontostand nicht stimmt oder nach einer Änderung von `MAX_BACKFILL_MARKETS`.

> ⚠️ Nach dem erzwungenen Sync unbedingt wieder auf `false` zurücksetzen – sonst wird bei **jedem** Abgleich die komplette Historie neu geladen.

---

## 🔒 Datenschutz & Sicherheit

- Die Erweiterung kommuniziert **ausschließlich** mit `api.bitvavo.com`.
- API Key und Secret werden von MoneyMoney lokal verschlüsselt gespeichert.
- Es werden keine Daten an Dritte übermittelt.
- Jede API-Anfrage wird mit HMAC-SHA256 signiert (Bitvavo-Standard).

---

## ☕ Unterstützung

Falls dir die Erweiterung einen Mehrwert bietet und du Danke sagen möchtest, freue ich mich über eine kleine Spende.

- **Bitcoin (BTC)**: `bc1qvhlfxeu5ehyck7eek4nlfwd7sxx279a0l2g5l7`
- **Ethereum (ETH)**: `0xBFd4eB9019c1DeF66B76cfeF2E6805dD3DD7B772`
- **Solana (SOL)**: `FnTU57Mk3cbdZ5zy4fKR8k314c95KyDLDqYcWiHkRgyc`

---

## 📜 Lizenz

Dieses Projekt steht unter der [MIT License](LICENSE).

---