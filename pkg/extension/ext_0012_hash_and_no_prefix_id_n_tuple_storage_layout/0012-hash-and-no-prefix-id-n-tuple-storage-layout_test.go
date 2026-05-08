package ext_0012_hash_and_no_prefix_id_n_tuple_storage_layout_test

import (
	"encoding/json"
	"path"
	"testing"

	"github.com/je4/filesystem/v3/pkg/writefs"
	"github.com/ocfl-archive/gocfl-extensions/test"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/extension/extensionimpl"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/storageroot"
	"github.com/stretchr/testify/require"
)

func TestStorageLayoutHashAndNoPrefixIdNTuple(t *testing.T) {
	env := test.SetupTestEnv(t)

	type testCase struct {
		name            string
		digestAlgorithm string
		tupleSize       int
		numberOfTuples  int
		delimiters      []string
		objectID        string
		expectedPath    string
	}

	testCases := []testCase{
		// Example 1
		{
			name:            "Example 1: object-01",
			digestAlgorithm: "sha256",
			tupleSize:       3,
			numberOfTuples:  3,
			delimiters:      []string{},
			objectID:        "object-01",
			expectedPath:    "3c0/ff4/240/object-01",
		},
		{
			name:            "Example 1: ..hor/rib:le-$id",
			digestAlgorithm: "sha256",
			tupleSize:       3,
			numberOfTuples:  3,
			delimiters:      []string{},
			objectID:        "..hor/rib:le-$id",
			expectedPath:    "487/326/d8c/%2e%2ehor%2frib%3ale-%24id",
		},
		// Example 2
		{
			name:            "Example 2: object-01",
			digestAlgorithm: "md5",
			tupleSize:       2,
			numberOfTuples:  15,
			delimiters:      []string{"/"},
			objectID:        "object-01",
			expectedPath:    "ff/75/53/44/92/48/5e/ab/b3/9f/86/35/67/28/88/object-01",
		},
		{
			name:            "Example 2: ..hor/rib:le-$id",
			digestAlgorithm: "md5",
			tupleSize:       2,
			numberOfTuples:  15,
			delimiters:      []string{"/"},
			objectID:        "..hor/rib:le-$id",
			expectedPath:    "5d/6e/4e/8c/b5/cd/0c/7a/8f/bf/65/c1/29/51/27/rib%3ale-%24id",
		},
		// Example 3
		{
			name:            "Example 3: object-01",
			digestAlgorithm: "sha256",
			tupleSize:       0,
			numberOfTuples:  0,
			delimiters:      []string{"/"},
			objectID:        "object-01",
			expectedPath:    "object-01",
		},
		{
			name:            "Example 3: ..hor/rib:le-$id",
			digestAlgorithm: "sha256",
			tupleSize:       0,
			numberOfTuples:  0,
			delimiters:      []string{"/"},
			objectID:        "..hor/rib:le-$id",
			expectedPath:    "rib%3ale-%24id",
		},
		// Additional Python code tests
		{
			name:            "Python test: object-01 with '-' delimiter",
			digestAlgorithm: "sha256",
			tupleSize:       3,
			numberOfTuples:  3,
			delimiters:      []string{"-"},
			objectID:        "object-01",
			expectedPath:    "938/db8/c9f/01",
		},
		{
			name:            "Python test: md5 defaults",
			digestAlgorithm: "md5",
			tupleSize:       3,
			numberOfTuples:  3,
			delimiters:      []string{},
			objectID:        "object-01",
			expectedPath:    "ff7/553/449/object-01",
		},
		{
			name:            "Python test: md5 5/2",
			digestAlgorithm: "md5",
			tupleSize:       5,
			numberOfTuples:  2,
			delimiters:      []string{},
			objectID:        "object-01",
			expectedPath:    "ff755/34492/object-01",
		},
		{
			name:            "Python test: special characters",
			digestAlgorithm: "sha256",
			tupleSize:       3,
			numberOfTuples:  3,
			delimiters:      []string{},
			objectID:        "..Hor/rib:lè-$id",
			expectedPath:    "373/529/21a/%2e%2eHor%2frib%3al%c3%a8-%24id",
		},
		{
			name:            "Truncation test: long ID",
			digestAlgorithm: "sha256",
			tupleSize:       3,
			numberOfTuples:  3,
			delimiters:      []string{},
			objectID:        "abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghija",
			expectedPath:    "5cc/73e/648/abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij-5cc73e648fbcff136510e330871180922ddacf193b68fdeff855683a01464220",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			conf := struct {
				ExtensionName   string   `json:"extensionName"`
				DigestAlgorithm string   `json:"digestAlgorithm"`
				TupleSize       int      `json:"tupleSize"`
				NumberOfTuples  int      `json:"numberOfTuples"`
				Delimiters      []string `json:"delimiters"`
			}{
				ExtensionName:   "0012-hash-and-no-prefix-id-n-tuple-storage-layout",
				DigestAlgorithm: tc.digestAlgorithm,
				TupleSize:       tc.tupleSize,
				NumberOfTuples:  tc.numberOfTuples,
				Delimiters:      tc.delimiters,
			}

			data, _ := json.MarshalIndent(conf, "", "  ")
			configPath := path.Join("0012-hash-and-no-prefix-id-n-tuple-storage-layout", "config.json")
			_, err := writefs.WriteFile(env.ConfigFS, configPath, data)
			require.NoError(t, err)

			extensionFactory, err := extensionimpl.NewFactory[storageroot.ExtensionManager](nil, env.Logger)
			require.NoError(t, err)

			sl, err := extensionFactory.LoadExtensionManager(env.ConfigFS)
			require.NoError(t, err)

			path, err := sl.BuildStorageRootPath(nil, tc.objectID)
			require.NoError(t, err)
			require.Equal(t, tc.expectedPath, path)
		})
	}
}
