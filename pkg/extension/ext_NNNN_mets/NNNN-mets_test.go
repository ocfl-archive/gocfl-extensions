package ext_NNNN_mets

import (
	"encoding/json"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/je4/filesystem/v4/pkg/vfsrw"
	"github.com/je4/filesystem/v4/pkg/writefs"
	"github.com/je4/utils/v2/pkg/zLogger"
	extcontent "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_content_subpath"
	"github.com/ocfl-archive/gocfl-extensions/test"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/extension"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestMETSIntegration_OnRealObject(t *testing.T) {
	// eigenes Config-FS im Speicher aufbauen
	cfgVFS := vfsrw.Config{
		"config": &vfsrw.VFS{
			Name:  "config",
			Type:  "afero",
			Afero: &vfsrw.Afero{BaseDir: "mem://"},
		},
	}
	var _zlogger zLogger.ZLogger = new(zerolog.New(os.Stderr))
	vfs, err := vfsrw.NewFS(cfgVFS, _zlogger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = vfs.Close() })
	configFS, err := writefs.Sub(vfs, "vfs://config")
	require.NoError(t, err)

	// Content-Subpath-Konfiguration (Areas content/metadata)
	cspCfg := &extcontent.ContentSubPathConfig{
		ExtensionConfig: &extension.ExtensionConfig{ExtensionName: extcontent.ContentSubPathName},
		Paths: map[string]extcontent.ContentSubPathEntry{
			"content":  {Path: "content/data", Description: "Primary content area"},
			"metadata": {Path: "metadata/files", Description: "Metadata area"},
		},
	}
	data, err := json.MarshalIndent(cspCfg, "", "  ")
	require.NoError(t, err)
	require.NoError(t, writefs.MkDir(configFS, extcontent.ContentSubPathName))
	_, err = writefs.WriteFile(configFS, path.Join(extcontent.ContentSubPathName, "config.json"), data)
	require.NoError(t, err)

	// METS-Konfiguration: Ablage in Area "metadata"
	metsCfg := &MetsConfig{
		ExtensionConfig:            &extension.ExtensionConfig{ExtensionName: METSName},
		StorageType:                "area",
		StorageName:                "metadata",
		PrimaryDescriptiveMetadata: "JSON:metadata:info.json",
		MetsFile:                   "mets.xml",
		PremisFile:                 "premis.xml",
	}
	data, err = json.MarshalIndent(metsCfg, "", "  ")
	require.NoError(t, err)
	require.NoError(t, writefs.MkDir(configFS, METSName))
	_, err = writefs.WriteFile(configFS, path.Join(METSName, "config.json"), data)
	require.NoError(t, err)

	// Test-Umgebung mit unseren Objekt-Extensions starten
	env := test.SetupFullTestEnv(t, nil, nil, nil, configFS)

	// Echtes Objekt anlegen und befüllen
	const objID = "obj-mets"
	obj, _ := test.CreateTestObject(t, env, objID)

	vw, err := obj.StartUpdate("v1", "Tester", "tester@example.org", false)
	require.NoError(t, err)

	// Dateien in verschiedenen Areas ablegen
	require.NoError(t, vw.AddData([]byte("hello content"), "doc.txt", true, "content", false, false))
	// Primäre descriptive metadata, auf die METS verweisen darf
	require.NoError(t, vw.AddData([]byte(`{"title":"Demo"}`), "info.json", true, "metadata", false, false))

	require.NoError(t, vw.Close())

	// Objekt neu laden und Inventory prüfen
	loaded, _ := test.ReloadObject(t, env, objID)
	inv := loaded.GetInventory()

	var foundContent, foundMets, foundPremis bool
	err = inv.IterateFiles(inv.GetHead(), func(internal []string, external []string, digest string) error {
		for i, ext := range external {
			// content-Datei prüfen (extern kann mit oder ohne Area-Präfix auftreten)
			if ext == "doc.txt" || ext == "content/data/doc.txt" {
				require.Contains(t, internal[i], "content/data/doc.txt")
				foundContent = true
			}
			// METS/PREMIS über den internen Pfad verifizieren (robust ggü. externem Namen)
			if strings.Contains(internal[i], "metadata/files/mets.xml") {
				foundMets = true
			}
			if strings.Contains(internal[i], "metadata/files/premis.xml") {
				foundPremis = true
			}
		}
		return nil
	})
	require.NoError(t, err)

	require.True(t, foundContent, "content file not found")
	require.True(t, foundMets, "mets.xml not found")
	require.True(t, foundPremis, "premis.xml not found")
}
