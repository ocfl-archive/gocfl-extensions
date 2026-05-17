package ext_0004_hashed_n_tuple_storage_layout

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

func TestNewStorageLayoutHashedNTuple(t *testing.T) {
	env := test.SetupTestEnv(t)

	type testCase struct {
		id   string
		path string
	}

	runTest := func(name string, config *StorageLayoutHashedNTupleConfig, cases []testCase) {
		t.Run(name, func(t *testing.T) {
			data, _ := json.MarshalIndent(config, "", "  ")
			configPath := path.Join(StorageLayoutHashedNTupleName, "config.json")
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
	runTest("Example1_Default", &StorageLayoutHashedNTupleConfig{
		ExtensionConfig: &extension.ExtensionConfig{ExtensionName: StorageLayoutHashedNTupleName},
		DigestAlgorithm: "sha256",
		TupleSize:       3,
		NumberOfTuples:  3,
		ShortObjectRoot: false,
	}, []testCase{
		{"object-01", "3c0/ff4/240/3c0ff4240c1e116dba14c7627f2319b58aa3d77606d0d90dfc6161608ac987d4"},
		{"..hor/rib:le-$id", "487/326/d8c/487326d8c2a3c0b885e23da1469b4d6671fd4e76978924b4443e9e3c316cda6d"},
	})

	// Example 2: MD5, 2/15, shortObjectRoot: true
	runTest("Example2_MD5_Short", &StorageLayoutHashedNTupleConfig{
		ExtensionConfig: &extension.ExtensionConfig{ExtensionName: StorageLayoutHashedNTupleName},
		DigestAlgorithm: "md5",
		TupleSize:       2,
		NumberOfTuples:  15,
		ShortObjectRoot: true,
	}, []testCase{
		{"object-01", "ff/75/53/44/92/48/5e/ab/b3/9f/86/35/67/28/88/4e"},
		{"..hor/rib:le-$id", "08/31/97/66/fb/6c/29/35/dd/17/5b/94/26/77/17/e0"},
	})

	// Example 3: Edge case 0/0
	runTest("Example3_EdgeCase", &StorageLayoutHashedNTupleConfig{
		ExtensionConfig: &extension.ExtensionConfig{ExtensionName: StorageLayoutHashedNTupleName},
		DigestAlgorithm: "sha256",
		TupleSize:       0,
		NumberOfTuples:  0,
		ShortObjectRoot: false,
	}, []testCase{
		{"object-01", "3c0ff4240c1e116dba14c7627f2319b58aa3d77606d0d90dfc6161608ac987d4"},
		{"..hor/rib:le-$id", "487326d8c2a3c0b885e23da1469b4d6671fd4e76978924b4443e9e3c316cda6d"},
	})
}
