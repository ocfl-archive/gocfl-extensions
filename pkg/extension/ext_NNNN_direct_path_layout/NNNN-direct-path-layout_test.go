package ext_NNNN_direct_path_layout

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"path"
	"testing"

	"github.com/ocfl-archive/filesystem/pkg/writefs"
	"github.com/ocfl-archive/gocfl-extensions/test"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/extension"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/extension/extensionimpl"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/storageroot"
	"github.com/stretchr/testify/require"
)

func TestNewDirectPathLayout(t *testing.T) {
	env := test.SetupTestEnv(t)
	storageLayoutConfig := &DirectPathLayoutConfig{
		ExtensionConfig: &extension.ExtensionConfig{ExtensionName: DirectPathLayoutName},
	}
	data, err := json.Marshal(storageLayoutConfig, jsontext.WithIndent("  "))
	require.NoError(t, err)
	_, err = writefs.WriteFile(env.ConfigFS, path.Join(DirectPathLayoutName, "config.json"), data)
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
		{"coll-a/sub-b/item-01", "coll-a/sub-b/item-01"},
		{"dept-archives/2024/doc-1234", "dept-archives/2024/doc-1234"},
		{"..hor_rib:lé-$id", "..hor_rib:lé-$id"},
		{"info:fedora/object-01", "info:fedora/object-01"},
	}

	for _, tc := range testCases {
		t.Run(tc.id, func(t *testing.T) {
			p, err := sl.BuildStorageRootPath(nil, tc.id)
			require.NoError(t, err)
			require.Equal(t, tc.path, p)
		})
	}
}

func TestDirectPathLayoutBasics(t *testing.T) {
	ext, err := NewDirectPathLayout()
	require.NoError(t, err)
	require.NotNil(t, ext)
	require.Equal(t, DirectPathLayoutName, ext.GetName())
	require.False(t, ext.IsRegistered())
	require.NotEmpty(t, DirectPathLayoutDoc)
	require.Contains(t, DirectPathLayoutDoc, "OCFL Community Extension NNNN: Direct Path Layout")
	require.NoError(t, ext.Terminate())
	require.NoError(t, ext.SetParams(nil))
}
