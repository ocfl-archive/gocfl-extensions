package ext_0006_flat_omit_prefix_storage_layout

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

func TestNewFlatOmitPrefixStorageLayout(t *testing.T) {
	env := test.SetupTestEnv(t)

	type testCase struct {
		id   string
		path string
	}

	runTest := func(name string, config *FlatOmitPrefixStorageLayoutConfig, cases []testCase) {
		t.Run(name, func(t *testing.T) {
			data, _ := json.MarshalIndent(config, "", "  ")
			configPath := path.Join(FlatOmitPrefixStorageLayoutName, "config.json")
			_, err := writefs.WriteFile(env.ConfigFS, configPath, data)
			require.NoError(t, err)

			extensionFactory, err := extensionimpl.NewFactory[storageroot.ExtensionManager](nil, env.Logger)
			require.NoError(t, err)

			sl, err := extensionFactory.LoadExtensionManager(env.ConfigFS)
			require.NoError(t, err)

			for _, tc := range cases {
				t.Run(tc.id, func(t *testing.T) {
					p, err := sl.BuildStorageRootPath(nil, tc.id)
					require.NoError(t, err)
					require.Equal(t, tc.path, p)
				})
			}
		})
	}

	// Example 1: delimiter ":"
	runTest("Example1", &FlatOmitPrefixStorageLayoutConfig{
		ExtensionConfig: &extension.ExtensionConfig{ExtensionName: FlatOmitPrefixStorageLayoutName},
		Delimiter:       ":",
	}, []testCase{
		{"namespace:12887296", "12887296"},
		{"urn:uuid:6e8bc430-9c3a-11d9-9669-0800200c9a66", "6e8bc430-9c3a-11d9-9669-0800200c9a66"},
	})

	// Example 2: delimiter "edu/"
	runTest("Example2", &FlatOmitPrefixStorageLayoutConfig{
		ExtensionConfig: &extension.ExtensionConfig{ExtensionName: FlatOmitPrefixStorageLayoutName},
		Delimiter:       "edu/",
	}, []testCase{
		{"https://institution.edu/3448793", "3448793"},
		{"https://institution.edu/abc/edu/f8.05v", "f8.05v"},
	})

	// Example 3: delimiter "info:"
	runTest("Example3", &FlatOmitPrefixStorageLayoutConfig{
		ExtensionConfig: &extension.ExtensionConfig{ExtensionName: FlatOmitPrefixStorageLayoutName},
		Delimiter:       "info:",
	}, []testCase{
		{"info:fedora/object-01", "fedora/object-01"},
		{"https://example.org/info:/12345/x54xz321/s3/f8.05v", "/12345/x54xz321/s3/f8.05v"},
	})

	// Additional Test: No delimiter present
	runTest("NoDelimiter", &FlatOmitPrefixStorageLayoutConfig{
		ExtensionConfig: &extension.ExtensionConfig{ExtensionName: FlatOmitPrefixStorageLayoutName},
		Delimiter:       ":",
	}, []testCase{
		{"object-01", "object-01"},
	})
}
