# gocfl-extensions

Dieses Repository enthält verschiedene Implementierungen von OCFL-Erweiterungen (Oxford Common File Layout) für die Go-Bibliothek [gocfl](https://github.com/ocfl-archive/gocfl).

## Übersicht

Die Erweiterungen sind in diesem Projekt modular aufgebaut und bieten zusätzliche Funktionalitäten für OCFL-Objekte und Storage-Roots, wie zum Beispiel:

- Spezielle Speicherlayouts (Storage Layouts)
- Unterstützung für zusätzliche Digest-Algorithmen
- Generierung von Metadaten (METS, PREMIS, EAD3)
- Automatisierte Workflows wie Vorschaubilderstellung oder Formatmigration

## Projektstruktur

- `pkg/extension`: Das Hauptpaket, das die Infrastruktur für Erweiterungen und die einzelnen Implementierungen enthält. Eine detaillierte Übersicht aller verfügbaren Erweiterungen finden Sie in der dortigen [README.md](pkg/extension/README.md).
- `test`: Enthält Hilfsmittel und Konfigurationen für Tests. Eine Beschreibung der Test-Infrastruktur finden Sie in der dortigen [README.md](test/README.md).

## Verwendung

Um dieses Modul in Ihrem Go-Projekt zu verwenden:

```bash
go get github.com/ocfl-archive/gocfl-extensions
```

## Dokumentation

Jede Erweiterung ist dokumentiert. Eine Liste aller registrierten und unregistrierten Erweiterungen sowie Verweise auf deren spezifische Dokumentation finden Sie unter:

👉 [**Detaillierte Übersicht der Erweiterungen**](pkg/extension/README.md)

## Lizenz

Dieses Projekt steht unter der [Apache License, Version 2.0](LICENSE).
