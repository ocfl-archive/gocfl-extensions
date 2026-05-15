# OCFL Extensions

Dieses Paket enthält verschiedene Erweiterungen für den GOCFL-Lade- und Verwaltungsprozess.

Es wird zwischen registrierten Erweiterungen, die im [OCFL Extensions Repository](https://ocfl.github.io/extensions/) gelistet sind, und unregistrierten (lokalen oder experimentellen) Erweiterungen unterschieden.

## Übersicht der Erweiterungen

Jede Erweiterung befindet sich in einem eigenen Unterverzeichnis und implementiert spezifische Funktionalitäten wie Speicherlayouts, Digest-Algorithmen oder Metadaten-Erzeugung.

### Registrierte Erweiterungen

Diese Erweiterungen folgen der offiziellen OCFL-Nummerierung und sind unter [https://ocfl.github.io/extensions/](https://ocfl.github.io/extensions/) dokumentiert.

- [0001-digest-algorithms](ext_0001_digest_algorithms/README.md)
- [0002-flat-direct-storage-layout](ext_0002_flat_direct_storage_layout/README.md)
- [0003-hash-and-id-n-tuple-storage-layout](ext_0003_hash_and_id_n_tuple_storage_layout/README.md)
- [0004-hashed-n-tuple-storage-layout](ext_0004_hashed_n_tuple_storage_layout/README.md)
- [0006-flat-omit-prefix-storage-layout](ext_0006_flat_omit_prefix_storage_layout/README.md)
- [0007-n-tuple-omit-prefix-storage-layout](ext_0007_n_tuple_omit_prefix_storage_layout/README.md)
- [0009-digest-algorithms](ext_0009_digest_algorithms/README.md)
- [0010-differential-n-tuple-omit-prefix-storage-layout](ext_0010_differential_n_tuple_omit_prefix_storage_layout/README.md)
- [0011-direct-clean-path-layout](ext_0011_direct_clean_path_layout/README.md)
- [0012-hash-and-no-prefix-id-n-tuple-storage-layout](ext_0012_hash_and_no_prefix_id_n_tuple_storage_layout/README.md)

### Unregistrierte Erweiterungen

Diese Erweiterungen sind (noch) nicht offiziell registriert und verwenden den Platzhalter `NNNN` in ihrer Bezeichnung.

- [NNNN-content-subpath](ext_NNNN_content_subpath/README.md)
- [NNNN-filesystem](ext_NNNN_filesystem/README.md)
- [NNNN-indexer](ext_NNNN_indexer/README.md)
- [NNNN-metafile](ext_NNNN_metafile/README.md)
- [NNNN-mets](ext_NNNN_mets/README.md)
- [NNNN-migration](ext_NNNN_migration/README.md)
- [NNNN-pairtree-storage-layout](ext_NNNN_pairtree_storage_layout/README.md)
- [NNNN-thumbnail](ext_NNNN_thumbnail/README.md)
- [NNNN-timestamp](ext_NNNN_timestamp/README.md)

## Initialisierung komplexer Erweiterungen

Einige Erweiterungen sind komplexer Natur und erfordern zusätzliche Konfigurationsdaten (z. B. für die METS-Erzeugung, Bildkonvertierung oder Metadaten-Extraktion). Wenn diese Erweiterungen verwendet werden, müssen sie oft manuell initialisiert und konfiguriert werden, bevor sie in den OCFL-Prozess integriert werden können.

Dies geschieht in der Regel durch Aufruf einer `Init`-Funktion der Erweiterung, die die notwendigen Laufzeitdaten (wie Logger oder Dateisystem-Referenzen) setzt.

### Beispiel: Initialisierung der Migration- und Thumbnail-Erweiterung

Das folgende Beispiel zeigt, wie Erweiterungen im `gocfl-cli` Projekt vor der Verwendung initialisiert werden:

```go
// Initialisierung der Migration-Erweiterung mit Konfiguration und Quell-Dateisystem
ext_NNNN_migration.Init(&conf.Migration, sourceFS, logger)

// Initialisierung der Thumbnail-Erweiterung
ext_NNNN_thumbnail.Init(conf.Thumbnail, sourceFS, logger)

// Initialisierung der Indexer-Erweiterung
ext_NNNN_indexer.Init(addr, conf.Indexer, localCache, logger)
```

Durch diese `Init`-Aufrufe registrieren sich die Erweiterungen selbst mit der notwendigen Laufzeitkonfiguration im globalen OCFL-Erweiterungs-Manager, sodass sie während des Ingest-Prozesses korrekt aufgerufen werden können.
