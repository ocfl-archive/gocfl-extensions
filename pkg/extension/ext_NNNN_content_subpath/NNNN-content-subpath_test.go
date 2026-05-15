package ext_NNNN_content_subpath

import (
	"encoding/json"
	"os"
	"path"
	"testing"

	"github.com/je4/filesystem/v4/pkg/vfsrw"
	"github.com/je4/filesystem/v4/pkg/writefs"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/ocfl-archive/gocfl-extensions/test"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/extension"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestContentSubPath(t *testing.T) {
	// Setup custom configuration FS
	cfgVFS := vfsrw.Config{
		"config": &vfsrw.VFS{
			Name: "config",
			Type: "afero",
			Afero: &vfsrw.Afero{
				BaseDir: "mem://",
			},
		},
	}
	var _zlogger zLogger.ZLogger = new(zerolog.New(os.Stderr))
	vfs, err := vfsrw.NewFS(cfgVFS, _zlogger)
	require.NoError(t, err)
	configFS, err := writefs.Sub(vfs, "vfs://config")
	require.NoError(t, err)

	config := &ContentSubPathConfig{
		ExtensionConfig: &extension.ExtensionConfig{
			ExtensionName: ContentSubPathName,
		},
		Paths: map[string]ContentSubPathEntry{
			"content": {
				Path:        "content/data",
				Description: "Primary content area",
			},
			"metadata": {
				Path:        "metadata/files",
				Description: "Metadata area",
			},
		},
	}

	data, err := json.MarshalIndent(config, "", "  ")
	require.NoError(t, err)
	err = writefs.MkDir(configFS, ContentSubPathName)
	require.NoError(t, err)
	configPath := path.Join(ContentSubPathName, "config.json")
	_, err = writefs.WriteFile(configFS, configPath, data)
	require.NoError(t, err)

	env := test.SetupFullTestEnv(t, nil, nil, nil, configFS)

	t.Run("IntegrationWithObject", func(t *testing.T) {
		objID := "test-object"
		obj, _ := test.CreateTestObject(t, env, objID)

		vw, err := obj.StartUpdate("initial version", "Junie", "junie@jetbrains.com", false)
		require.NoError(t, err)

		// Test file in default 'content' area
		err = vw.AddData([]byte("content data"), "file1.txt", true, "content", false, false)
		require.NoError(t, err)

		// Test file in 'metadata' area
		err = vw.AddData([]byte("metadata data"), "meta1.json", true, "metadata", false, false)
		require.NoError(t, err)

		// Test file in 'full' area (no subpath)
		err = vw.AddData([]byte("root data"), "root.txt", true, "full", false, false)
		require.NoError(t, err)

		err = vw.Close()
		require.NoError(t, err)

		// Reload and verify inventory paths
		loadedObj, _ := test.ReloadObject(t, env, objID)
		inv := loadedObj.GetInventory()

		foundContent := false
		foundMetadata := false
		foundFull := false

		err = inv.IterateFiles(inv.GetHead(), func(internal []string, external []string, digest string) error {
			for i, ext := range external {
				switch ext {
				case "file1.txt", "content/data/file1.txt":
					require.Contains(t, internal[i], "content/data/file1.txt")
					foundContent = true
				case "meta1.json", "metadata/files/meta1.json":
					require.Contains(t, internal[i], "metadata/files/meta1.json")
					foundMetadata = true
				case "root.txt":
					require.Contains(t, internal[i], "root.txt")
					require.NotContains(t, internal[i], "content/data/")
					foundFull = true
				}
			}
			return nil
		})
		require.NoError(t, err)
		require.True(t, foundContent, "file1.txt not found")
		require.True(t, foundMetadata, "meta1.json not found")
		require.True(t, foundFull, "root.txt not found")

		// Verify README.md exists and contains the expected content
		foundREADME := false
		err = inv.IterateFiles(inv.GetHead(), func(internal []string, external []string, digest string) error {
			for i, ext := range external {
				if ext == "README.md" {
					foundREADME = true
					// Check if it's in the root (no subpath)
					require.NotContains(t, internal[i], "content/data/")
					require.NotContains(t, internal[i], "metadata/files/")
				}
			}
			return nil
		})
		require.NoError(t, err)
		require.True(t, foundREADME, "README.md not found in inventory")
	})

	// Keep existing unit tests logic using the extension manager from env
	extManager := env.ObjectExtensionManager

	t.Run("BuildObjectManifestPath", func(t *testing.T) {
		testCases := []struct {
			name         string
			originalPath string
			area         string
			expected     string
			expectError  bool
		}{
			{"Default area (content)", "file.txt", "", "content/data/file.txt", false},
			{"Explicit content area", "file.txt", "content", "content/data/file.txt", false},
			{"Explicit metadata area", "doc.pdf", "metadata", "metadata/files/doc.pdf", false},
			{"Full area (no prefix)", "anything.txt", "full", "anything.txt", false},
			{"Invalid area", "file.txt", "invalid", "", true},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				p, err := extManager.BuildObjectManifestPath(tc.originalPath, tc.area)
				if tc.expectError {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					require.Equal(t, tc.expected, p)
				}
			})
		}
	})

	t.Run("BuildObjectStatePath", func(t *testing.T) {
		p, err := extManager.BuildObjectStatePath("file.txt", "metadata")
		require.NoError(t, err)
		require.Equal(t, "metadata/files/file.txt", p)
	})

	t.Run("BuildObjectExtractPath", func(t *testing.T) {
		testCases := []struct {
			name         string
			originalPath string
			area         string
			expected     string
			expectError  bool
		}{
			{"Extract from content", "content/data/file.txt", "content", "file.txt", false},
			{"Extract from metadata", "metadata/files/doc.pdf", "metadata", "doc.pdf", false},
			{"Extract full", "any/path/file.txt", "full", "any/path/file.txt", false},
			{"Wrong area prefix", "content/data/file.txt", "metadata", "", true},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				p, err := extManager.BuildObjectExtractPath(tc.originalPath, tc.area)
				if tc.expectError {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					require.Equal(t, tc.expected, p)
				}
			})
		}
	})

	t.Run("GetAreaPath", func(t *testing.T) {
		path, err := extManager.GetAreaPath("content")
		require.NoError(t, err)
		require.Equal(t, "content/data", path)

		_, err = extManager.GetAreaPath("invalid")
		require.Error(t, err)
	})
}
