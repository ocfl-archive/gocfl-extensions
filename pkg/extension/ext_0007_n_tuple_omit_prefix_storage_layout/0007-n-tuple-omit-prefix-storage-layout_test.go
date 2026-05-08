package ext_0007_n_tuple_omit_prefix_storage_layout

import (
	"encoding/json"
	"path"
	"testing"

	"github.com/je4/filesystem/v3/pkg/writefs"
	"github.com/ocfl-archive/gocfl-extensions/test"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/extension"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/extension/extensionimpl"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/storageroot"
	"github.com/stretchr/testify/require"
)

func TestNTupleOmitPrefixStorageLayout(t *testing.T) {
	env := test.SetupTestEnv(t)

	type testCase struct {
		id   string
		path string
	}

	runTest := func(name string, config *NTupleOmitPrefixStorageLayoutConfig, cases []testCase) {
		t.Run(name, func(t *testing.T) {
			data, _ := json.MarshalIndent(config, "", "  ")
			configPath := path.Join(NTupleOmitPrefixStorageLayoutName, "config.json")
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

	// Example 1: delimiter ":", tupleSize 4, numberOfTuples 2, zeroPadding "left", reverseObjectRoot true
	runTest("Example1", &NTupleOmitPrefixStorageLayoutConfig{
		ExtensionConfig:   &extension.ExtensionConfig{ExtensionName: NTupleOmitPrefixStorageLayoutName},
		Delimiter:         ":",
		TupleSize:         4,
		NumberOfTuples:    2,
		ZeroPadding:       "left",
		ReverseObjectRoot: true,
	}, []testCase{
		{"namespace:12887296", "6927/8821/12887296"},
		{"urn:uuid:6e8bc430-9c3a-11d9-9669-0800200c9a66", "66a9/c002/6e8bc430-9c3a-11d9-9669-0800200c9a66"},
		{"abc123", "321c/ba00/abc123"},
	})

	// Example 2: delimiter "edu/", tupleSize 3, numberOfTuples 3, zeroPadding "right", reverseObjectRoot false
	runTest("Example2", &NTupleOmitPrefixStorageLayoutConfig{
		ExtensionConfig:   &extension.ExtensionConfig{ExtensionName: NTupleOmitPrefixStorageLayoutName},
		Delimiter:         "edu/",
		TupleSize:         3,
		NumberOfTuples:    3,
		ZeroPadding:       "right",
		ReverseObjectRoot: false,
	}, []testCase{
		{"https://institution.edu/3448793", "344/879/300/3448793"},
		{"https://institution.edu/abc/edu/f8.05v", "f8./05v/000/f8.05v"},
	})

	// Additional Test: No delimiter present
	runTest("NoDelimiter", &NTupleOmitPrefixStorageLayoutConfig{
		ExtensionConfig:   &extension.ExtensionConfig{ExtensionName: NTupleOmitPrefixStorageLayoutName},
		Delimiter:         ":",
		TupleSize:         3,
		NumberOfTuples:    3,
		ZeroPadding:       "left",
		ReverseObjectRoot: false,
	}, []testCase{
		{"object-01", "obj/ect/-01/object-01"},
	})
}
