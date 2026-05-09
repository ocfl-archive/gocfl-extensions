package ext_0003_hash_and_id_n_tuple_storage_layout

import (
	"encoding/json"
	"path"
	"testing"

	"github.com/je4/filesystem/v4/pkg/writefs"
	"github.com/ocfl-archive/gocfl-extensions/test"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/extension"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/extension/extensionimpl"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/storageroot"
	"github.com/stretchr/testify/require"
)

func TestNewStorageLayoutHashAndIdNTuple(t *testing.T) {
	env := test.SetupTestEnv(t)

	type testCase struct {
		id   string
		path string
	}

	runTest := func(name string, config *StorageLayoutHashAndIdNTupleConfig, cases []testCase) {
		t.Run(name, func(t *testing.T) {
			data, _ := json.MarshalIndent(config, "", "  ")
			configPath := path.Join(StorageLayoutHashAndIdNTupleName, "config.json")
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

	// Example 1: Default configuration
	runTest("Example1_Default", &StorageLayoutHashAndIdNTupleConfig{
		ExtensionConfig: &extension.ExtensionConfig{ExtensionName: StorageLayoutHashAndIdNTupleName},
		DigestAlgorithm: "sha256",
		TupleSize:       3,
		NumberOfTuples:  3,
	}, []testCase{
		{"object-01", "3c0/ff4/240/object-01"},
		{"..hor/rib:le-$id", "487/326/d8c/%2e%2ehor%2frib%3ale-%24id"},
	})

	// Example 2: MD5, 2/15
	runTest("Example2_MD5", &StorageLayoutHashAndIdNTupleConfig{
		ExtensionConfig: &extension.ExtensionConfig{ExtensionName: StorageLayoutHashAndIdNTupleName},
		DigestAlgorithm: "md5",
		TupleSize:       2,
		NumberOfTuples:  15,
	}, []testCase{
		{"object-01", "ff/75/53/44/92/48/5e/ab/b3/9f/86/35/67/28/88/object-01"},
		{"..hor/rib:le-$id", "08/31/97/66/fb/6c/29/35/dd/17/5b/94/26/77/17/%2e%2ehor%2frib%3ale-%24id"},
	})

	// Example 3: Edge case 0/0
	runTest("Example3_EdgeCase", &StorageLayoutHashAndIdNTupleConfig{
		ExtensionConfig: &extension.ExtensionConfig{ExtensionName: StorageLayoutHashAndIdNTupleName},
		DigestAlgorithm: "sha256",
		TupleSize:       0,
		NumberOfTuples:  0,
	}, []testCase{
		{"object-01", "object-01"},
		{"..hor/rib:le-$id", "%2e%2ehor%2frib%3ale-%24id"},
	})

	// Long ID test (from Python code in MD)
	longID101 := "abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghija"
	longID101Digest := "5cc73e648fbcff136510e330871180922ddacf193b68fdeff855683a01464220"
	runTest("LongID", &StorageLayoutHashAndIdNTupleConfig{
		ExtensionConfig: &extension.ExtensionConfig{ExtensionName: StorageLayoutHashAndIdNTupleName},
		DigestAlgorithm: "sha256",
		TupleSize:       3,
		NumberOfTuples:  3,
	}, []testCase{
		{longID101, "5cc/73e/648/abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij-" + longID101Digest},
	})
}
