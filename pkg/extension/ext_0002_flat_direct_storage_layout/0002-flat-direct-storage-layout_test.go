package ext_0002_flat_direct_storage_layout

import (
	"encoding/json"
	"path"
	"testing"

	"github.com/ocfl-archive/filesystem/pkg/writefs"
	"github.com/ocfl-archive/gocfl-extensions/test"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/extension"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/extension/extensionimpl"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/storageroot"
	"github.com/stretchr/testify/require"
)

func TestNewStorageLayoutFlatDirect(t *testing.T) {
	env := test.SetupTestEnv(t)
	storageLayoutConfig := &StorageLayoutFlatDirectConfig{
		ExtensionConfig: &extension.ExtensionConfig{ExtensionName: StorageLayoutFlatDirectName},
	}
	data, _ := json.MarshalIndent(storageLayoutConfig, "", "  ")
	_, err := writefs.WriteFile(env.ConfigFS, path.Join(StorageLayoutFlatDirectName, "config.json"), data)
	require.NoError(t, err)

	extensionFactory, err := extensionimpl.NewFactory[storageroot.ExtensionManager](nil, env.Logger)
	require.NoError(t, err)

	sl, err := extensionFactory.LoadExtensionManager(env.ConfigFS)
	require.NoError(t, err)

	testCases := []struct {
		id   string
		path string
	}{
		{"object-01", "object-01"},
		{"..hor_rib:lé-$id", "..hor_rib:lé-$id"},
		{"info:fedora/object-01", "info:fedora/object-01"},
		{"abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij", "abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij"},
	}

	for _, tc := range testCases {
		t.Run(tc.id, func(t *testing.T) {
			path, err := sl.BuildStorageRootPath(nil, tc.id)
			require.NoError(t, err)
			require.Equal(t, tc.path, path)
		})
	}
}
