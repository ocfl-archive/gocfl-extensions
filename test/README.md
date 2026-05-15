# Test-Infrastruktur

Dieses Verzeichnis enthält Hilfsmittel und Konfigurationen für die Durchführung von Tests innerhalb des `gocfl-extensions` Projekts.

## Inhalt

- `helper.go`: Enthält Funktionen zum Aufsetzen von Testumgebungen, einschließlich In-Memory-Dateisystemen (VFS) und OCFL-Storage-Roots.
    - `SetupTestEnv`: Initialisiert eine einfache Testumgebung mit Logger und VFS.
    - `SetupFullTestEnv`: Erstellt eine komplette OCFL-Testumgebung inklusive konfigurierbarer Erweiterungsmanager für Storage-Roots und Objekte.
    - `CreateTestObject`: Hilfsfunktion zum Erstellen eines neuen OCFL-Objekts innerhalb der Testumgebung.
    - `ReloadObject`: Hilfsfunktion zum Laden eines existierenden OCFL-Objekts zu Testzwecken.
- `defaultconfig/`: Enthält Standard-Konfigurationsdateien (JSON) für die Initialisierung von OCFL-Strukturen in Tests.
    - `object/`: Standardkonfigurationen für OCFL-Objekte.
    - `storageroot/`: Standardkonfigurationen für OCFL-Storage-Roots.
    - `embed.go`: Stellt die Konfigurationen via `go:embed` für die Verwendung in Go-Tests bereit.

## Verwendung in Tests

Die Hilfsfunktionen in `helper.go` sind darauf ausgelegt, die Erstellung von Integrationstests zu vereinfachen, die das Zusammenspiel verschiedener OCFL-Erweiterungen verifizieren.

Beispiel für die Initialisierung einer Testumgebung:

```go
func TestMeineErweiterung(t *testing.T) {
    env := test.SetupFullTestEnv(t, nil, nil, nil, nil)
    // ... Testlogik ...
}
```
