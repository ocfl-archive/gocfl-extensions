package ext_0002_flat_direct_storage_layout

import (
	"encoding/json"
	"path"
	"testing"

	"github.com/je4/filesystem/v3/pkg/writefs"
	"github.com/ocfl-archive/gocfl-extensions/test"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/extension"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/extension/extensionimpl"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/storageroot"
	"github.com/stretchr/testify/assert"
)

func TestNewStorageLayoutFlatDirect(t *testing.T) {
	env := test.SetupTestEnv(t)
	storageLayoutConfig := &StorageLayoutFlatDirectConfig{
		ExtensionConfig: &extension.ExtensionConfig{ExtensionName: StorageLayoutFlatDirectName},
	}
	data, _ := json.MarshalIndent(storageLayoutConfig, "", "  ")
	_, err := writefs.WriteFile(env.ConfigFS, path.Join(StorageLayoutFlatDirectName, "config.json"), data)
	assert.NoError(t, err)

	extensionFactory, err := extensionimpl.NewFactory(nil, env.Logger)
	assert.NoError(t, err)

	genericExtensionManager, err := extensionFactory.LoadExtensionManager(env.ConfigFS)
	assert.NoError(t, err)

	sl, ok := genericExtensionManager.(storageroot.ExtensionStorageRootPath)
	assert.True(t, ok, "Extension manager should implement storageroot.ExtensionStorageRootPath interface")

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
			assert.NoError(t, err)
			assert.Equal(t, tc.path, path)
		})
	}
}
