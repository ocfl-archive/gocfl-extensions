package ext_0010_differential_n_tuple_omit_prefix_storage_layout

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

func TestDifferentialNTupleOmitPrefixStorageLayout(t *testing.T) {
	env := test.SetupTestEnv(t)

	type testCase struct {
		id   string
		path string
	}

	runTest := func(name string, config *DifferentialNTupleOmitPrefixStorageLayoutConfig, cases []testCase) {
		t.Run(name, func(t *testing.T) {
			data, _ := json.MarshalIndent(config, "", "  ")
			configPath := path.Join(DifferentialNTupleOmitPrefixStorageLayoutName, "config.json")
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

	runTest("Example 1", &DifferentialNTupleOmitPrefixStorageLayoutConfig{
		ExtensionConfig:            &extension.ExtensionConfig{ExtensionName: DifferentialNTupleOmitPrefixStorageLayoutName},
		Delimiter:                  ":",
		TupleSegmentSizes:          []int{2, 3, 2, 4},
		FullIdentifierAsObjectRoot: false,
	}, []testCase{
		{"druid:gh875jh5489", "gh/875/jh/5489"},
		{"namespace:11887296672", "11/887/29/6672"},
		{"urn:nbn:fi:111-0023815", "11/1-0/02/3815"},
		{"abc123xyz89", "ab/c12/3x/yz89"},
	})

	runTest("Example 2", &DifferentialNTupleOmitPrefixStorageLayoutConfig{
		ExtensionConfig:            &extension.ExtensionConfig{ExtensionName: DifferentialNTupleOmitPrefixStorageLayoutName},
		Delimiter:                  "edu/",
		TupleSegmentSizes:          []int{3, 4},
		FullIdentifierAsObjectRoot: true,
	}, []testCase{
		{"https://institution.edu/3448793", "344/8793/3448793"},
		{"https://institution.edu/abc/edu/f8a905v", "f8a/905v/f8a905v"},
	})

	t.Run("Error Cases", func(t *testing.T) {
		config := &DifferentialNTupleOmitPrefixStorageLayoutConfig{
			ExtensionConfig:            &extension.ExtensionConfig{ExtensionName: DifferentialNTupleOmitPrefixStorageLayoutName},
			Delimiter:                  ":",
			TupleSegmentSizes:          []int{2, 2},
			FullIdentifierAsObjectRoot: false,
		}
		data, _ := json.MarshalIndent(config, "", "  ")
		configPath := path.Join(DifferentialNTupleOmitPrefixStorageLayoutName, "config.json")
		_, err := writefs.WriteFile(env.ConfigFS, configPath, data)
		require.NoError(t, err)

		extensionFactory, err := extensionimpl.NewFactory[storageroot.ExtensionManager](nil, env.Logger)
		require.NoError(t, err)

		sl, err := extensionFactory.LoadExtensionManager(env.ConfigFS)
		require.NoError(t, err)

		testCases := []string{
			"too:short",    // length 5, expected 4
			"too:longlong", // length 8, expected 4
			"end:",         // delimiter at end
		}

		for _, tc := range testCases {
			t.Run(tc, func(t *testing.T) {
				_, err := sl.BuildStorageRootPath(nil, tc)
				require.Error(t, err)
			})
		}
	})
}
