package ext_NNNN_metafile

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"path"
	"testing"

	"github.com/je4/filesystem/v4/pkg/aferoFS"
	"github.com/je4/filesystem/v4/pkg/writefs"
	"github.com/ocfl-archive/gocfl-extensions/test"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/extension"
	"github.com/rs/zerolog"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

//go:embed gocfl-info-1.0.json
var metafileSchema []byte

func TestMetaFile_AddObject(t *testing.T) {
	// Content for the server to serve valid JSON when needed
	infoContent := `{
		"signature": "test-sig",
		"organisation_id": "test-org-id",
		"organisation": "test-org",
		"organisation_address": "test-org-addr",
		"title": "Test Object",
		"user": "test-user",
		"address": "test-addr",
		"created": "2026-05-06T12:00:00Z",
		"last_changed": "2026-05-06T12:00:00Z"
	}`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if path.Base(r.URL.Path) == "hello.txt" {
			fmt.Fprint(w, "Hello World!")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, infoContent)
	}))
	defer server.Close()

	logger := new(zerolog.New(zerolog.NewConsoleWriter()))
	configFS, err := aferoFS.NewFS(afero.NewMemMapFs(), logger)
	require.NoError(t, err)
	metafileConfig := &MetaFileConfig{
		ExtensionConfig: &extension.ExtensionConfig{ExtensionName: MetaFileName},
		StorageType:     "extension",
		StorageName:     "",
		MetaName:        "info.json",
		MetaSchema:      "gocfl-info-1.0.json",
		MetaSchemaUrl:   "https://raw.githubusercontent.com/ocfl-archive/gocfl/main/gocfl-info-1.0.json",
	}
	metafileConfigData, _ := json.MarshalIndent(metafileConfig, "", "  ")
	_, err = writefs.WriteFile(configFS, path.Join("extensions", MetaFileName, "config.json"), metafileConfigData)
	require.NoError(t, err)
	_, err = writefs.WriteFile(configFS, path.Join("extensions", MetaFileName, "gocfl-info-1.0.json"), metafileSchema)
	require.NoError(t, err)

	// Temporäre Metadaten-Datei erstellen (muss gegen gocfl-info-1.0.json validieren)
	_, err = writefs.WriteFile(configFS, "temp/info.json", []byte(infoContent))
	require.NoError(t, err)
	_, err = writefs.WriteFile(configFS, "temp/data.txt", []byte("some data"))
	require.NoError(t, err)

	objectExtensionFS, err := fs.Sub(configFS, "extensions")
	require.NoError(t, err)
	tempFS, err := fs.Sub(configFS, "temp")
	require.NoError(t, err)
	env := test.SetupFullTestEnv(t, map[string]string{"ext-NNNN-metafile-source": server.URL + "/info.json"}, tempFS, nil, objectExtensionFS)

	// 4. Test-Objekt erstellen
	objID := "test-object"
	obj, objFS := test.CreateTestObject(t, env, objID)
	versionWriter, err := obj.StartUpdate(objFS, "initial data", "Jane Doe", "mailto:dummy", false)
	require.NoError(t, err)
	err = versionWriter.AddFile(configFS, "temp/data.txt", false, "content", false, false)
	require.NoError(t, err)
	err = versionWriter.Close()
	require.NoError(t, err)

	// Prüfen, ob "extensions/NNNN-metafile/info.json" in objFS existiert und korrekt ist
	metaPath := path.Join("extensions", MetaFileName, "info.json")
	data, err := fs.ReadFile(objFS, metaPath)
	require.NoError(t, err, "file %s should exist in objFS", metaPath)

	var result map[string]any
	err = json.Unmarshal(data, &result)
	require.NoError(t, err, "should be valid JSON")

	var expected map[string]any
	err = json.Unmarshal([]byte(infoContent), &expected)
	require.NoError(t, err)

	require.Equal(t, expected, result, "content of %s should match infoContent", metaPath)
	/*
		fs.WalkDir(objFS, ".", func(path string, d fs.DirEntry, err error) error {
			logger.Info().Str("path", path).Msg("path")
			return nil
		})
	*/

}
