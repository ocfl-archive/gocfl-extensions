package ext_NNNN_indexer

import (
	"encoding/json"
	"io/fs"
	"path"
	"testing"

	"github.com/ocfl-archive/filesystem/pkg/aferoFS"
	"github.com/ocfl-archive/filesystem/pkg/writefs"
	"github.com/ocfl-archive/gocfl-extensions/test"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl"
	ironmaiden "github.com/ocfl-archive/indexer/v3/pkg/indexer"
	"github.com/rs/zerolog"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func TestIndexer_AddObject(t *testing.T) {
	logger := new(zerolog.New(zerolog.NewConsoleWriter()))
	configFS, err := aferoFS.NewFS(afero.NewMemMapFs(), logger)
	require.NoError(t, err)

	// Konfiguration wie in ../gocfl-cli/internal/extensions/object/NNNN-indexer/config.json
	type indexerConfigInternal struct {
		ExtensionName string   `json:"extensionName"`
		StorageType   string   `json:"storageType"`
		StorageName   string   `json:"storageName"`
		Actions       []string `json:"actions"`
		Compress      string   `json:"compress"`
	}
	cfgData := indexerConfigInternal{
		ExtensionName: IndexerName,
		StorageType:   "extension",
		StorageName:   "metadata",
		Actions:       []string{"siegfried"},
		Compress:      "none", // "none" für einfachere Verifikation im ersten Schritt
	}

	configData, err := json.MarshalIndent(cfgData, "", "  ")
	require.NoError(t, err)
	_, err = writefs.WriteFile(configFS, path.Join("object", IndexerName, "config.json"), configData)
	require.NoError(t, err)

	// Testdaten erstellen
	testContent := "This is a test file content."
	_, err = writefs.WriteFile(configFS, "temp/test.txt", []byte(testContent))
	require.NoError(t, err)

	objectExtensionFS, err := fs.Sub(configFS, "object")
	require.NoError(t, err)
	tempFS, err := fs.Sub(configFS, "temp")
	require.NoError(t, err)

	indexerConf := ironmaiden.GetDefaultConfig()
	// Wir nutzen einen OCFLLogger für die Initialisierung
	ocflLogger := ocfl.NewOCFLLogger(t.Context(), logger, nil, "1.1", nil)

	Init(indexerConf, false, ocflLogger)

	env := test.SetupFullTestEnv(t, nil, tempFS, nil, objectExtensionFS)

	objID := "test-object-indexer"
	obj, objFS := test.CreateTestObject(t, env, objID)

	versionWriter, err := obj.StartUpdate("initial index", "John Doe", "mailto:john@example.com", false)
	require.NoError(t, err)

	err = versionWriter.AddFile(tempFS, "test.txt", false, "test.txt", false, false)
	require.NoError(t, err)

	err = versionWriter.Close()
	require.NoError(t, err)

	// Verifikation: Prüfen ob Indexer-Datei existiert
	indexerPath := "extensions/NNNN-indexer/metadata/indexer_v1.jsonl"
	data, err := fs.ReadFile(objFS, indexerPath)
	require.NoError(t, err, "Indexer output file should exist at %s", indexerPath)

	// Prüfen ob Inhalt gültiges JSONL ist und den Pfad enthält
	require.Contains(t, string(data), "test.txt")
}
