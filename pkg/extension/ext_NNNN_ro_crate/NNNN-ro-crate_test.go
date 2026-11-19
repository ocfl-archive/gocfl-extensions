package ext_NNNN_ro_crate

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"io/fs"
	"path"
	"testing"

	"github.com/ocfl-archive/filesystem/pkg/aferoFS"
	"github.com/ocfl-archive/filesystem/pkg/writefs"
	"github.com/ocfl-archive/gocfl-extensions/test"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/extension"
	"github.com/rs/zerolog"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

const sampleROCrateMetadata = `{
  "@context": "https://w3id.org/ro/crate/1.1/context",
  "@graph": [
    {
      "@type": "CreativeWork",
      "@id": "ro-crate-metadata.json",
      "conformsTo": {
        "@id": "https://w3id.org/ro/crate/1.1"
      },
      "about": {
        "@id": "./"
      }
    },
    {
      "@id": "./",
      "@type": [
        "Dataset",
        "LearningResource"
      ],
      "name": "Demo Crate",
      "description": "a demo crate for Galaxy training",
      "datePublished": "2024-03-08",
      "license": {
        "@id": "https://spdx.org/licenses/CC-BY-NC-SA-4.0.html"
      },
      "keywords": [
        "training",
        "galaxy"
      ]
    }
  ]
}`

func TestROCrate_AddObject(t *testing.T) {
	logger := new(zerolog.New(zerolog.NewConsoleWriter()))
	configFS, err := aferoFS.NewFS(afero.NewMemMapFs(), logger)
	require.NoError(t, err)

	rcConfig := &ROCrateFileConfig{
		ExtensionConfig: &extension.ExtensionConfig{ExtensionName: ROCrateFileName},
		StorageType:     "extension",
		StorageName:     "",
		MetaName:        "info.json",
	}
	rcConfigData, err := json.Marshal(rcConfig, jsontext.WithIndent("  "))
	require.NoError(t, err)

	_, err = writefs.WriteFile(configFS, path.Join("extensions", ROCrateFileName, "config.json"), rcConfigData)
	require.NoError(t, err)

	_, err = writefs.WriteFile(configFS, "temp/ro-crate-metadata.json", []byte(sampleROCrateMetadata))
	require.NoError(t, err)
	_, err = writefs.WriteFile(configFS, "temp/data.txt", []byte("some data"))
	require.NoError(t, err)

	objectExtensionFS, err := fs.Sub(configFS, "extensions")
	require.NoError(t, err)
	tempFS, err := fs.Sub(configFS, "temp")
	require.NoError(t, err)

	params := map[string]string{
		ParamROCrateEnabled(): "true",
		paramROCrateOrg():     "My University",
		paramROCrateOrgID():   "https://ror.org/12345",
		paramROCrateUser():    "Jane Doe",
		paramROCrateAddress(): "Campus Street 1",
	}

	env := test.SetupFullTestEnv(t, params, tempFS, nil, objectExtensionFS)

	objID := "test-rocrate-object"
	obj, objFS := test.CreateTestObject(t, env, objID)
	versionWriter, err := obj.StartUpdate("initial rocrate ingest", "Jane Doe", "mailto:jane@example.com", false)
	require.NoError(t, err)

	err = versionWriter.AddFile(configFS, "temp/ro-crate-metadata.json", false, "content", false, false)
	require.NoError(t, err)
	err = versionWriter.AddFile(configFS, "temp/data.txt", false, "content", false, false)
	require.NoError(t, err)

	err = versionWriter.Close()
	require.NoError(t, err)

	metaPath := path.Join("extensions", ROCrateFileName, "info.json")
	data, err := fs.ReadFile(objFS, metaPath)
	require.NoError(t, err, "file %s should exist in objFS", metaPath)

	var result map[string]any
	err = json.Unmarshal(data, &result)
	require.NoError(t, err, "should be valid JSON")

	require.Equal(t, "./", result["signature"])
	require.Equal(t, "Demo Crate", result["title"])
	require.Equal(t, "a demo crate for Galaxy training", result["description"])
	require.Equal(t, "2024-03-08", result["created"])
	require.Equal(t, "Dataset; LearningResource", result["sets"])
	require.Equal(t, "training; galaxy", result["keywords"])
	require.Equal(t, "https://spdx.org/licenses/CC-BY-NC-SA-4.0.html", result["licenses"])
	require.Equal(t, "My University", result["organisation"])
	require.Equal(t, "https://ror.org/12345", result["organisation_id"])
	require.Equal(t, "Jane Doe", result["user"])
	require.Equal(t, "Campus Street 1", result["address"])

	// Test GetMetadata on object
	extManager := obj.GetExtensionManager()
	require.NotNil(t, extManager)
	meta, err := extManager.GetMetadata(objFS, obj)
	require.NoError(t, err)
	require.NotEmpty(t, meta)
}
