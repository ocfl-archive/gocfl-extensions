package ext_0011_direct_clean_path_layout

import (
	"encoding/json"
	"path"
	"testing"

	"github.com/je4/filesystem/v4/pkg/writefs"
	"github.com/ocfl-archive/gocfl-extensions/test"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/extension"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/extension/extensionimpl"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/object"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/storageroot"
	"github.com/stretchr/testify/require"
)

func doTestDirectClean(t *testing.T, extensionName string) {
	env := test.SetupTestEnv(t)

	type testCase struct {
		id   string
		path string
	}

	runTest := func(name string, config *DirectCleanConfig, cases []testCase) {
		t.Run(name, func(t *testing.T) {
			data, err := json.MarshalIndent(config, "", "  ")
			require.NoError(t, err)
			configPath := path.Join(extensionName, "config.json")
			_, err = writefs.WriteFile(env.ConfigFS, configPath, data)
			require.NoError(t, err)

			srExtensionFactory, err := extensionimpl.NewFactory[storageroot.ExtensionManager](nil, env.Logger)
			require.NoError(t, err)

			sl, err := srExtensionFactory.LoadExtensionManager(env.ConfigFS)
			require.NoError(t, err)

			clExtensionFactory, err := extensionimpl.NewFactory[object.ExtensionManager](nil, env.Logger)
			require.NoError(t, err)
			cl, err := clExtensionFactory.LoadExtensionManager(env.ConfigFS)
			require.NoError(t, err)

			for _, tc := range cases {
				t.Run(tc.id, func(t *testing.T) {
					p, err := sl.BuildStorageRootPath(nil, tc.id)
					require.NoError(t, err)
					require.Equal(t, tc.path, p)

					p2, err := cl.BuildObjectManifestPath(tc.id, "")
					require.NoError(t, err)
					require.Equal(t, tc.path, p2)
				})
			}
		})
	}

	// Example #1: encodeUTF == false
	runTest("Example1_False", &DirectCleanConfig{
		ExtensionConfig:             &extension.ExtensionConfig{ExtensionName: extensionName},
		MaxPathnameLen:              32000,
		MaxPathSegmentLen:           127,
		UTFEncode:                   false,
		ReplacementString:           "_",
		WhitespaceReplacementString: " ",
		FallbackDigestAlgorithm:     "md5",
		FallbackFolder:              "fallback",
		NumberOfFallbackTuples:      2,
		FallbackTupleSize:           1,
	}, []testCase{
		{"..hor_rib:lé-$id", "..hor_rib_lé-$id"},
		{"info:fedora/object-01", "info_fedora/object-01"},
		{"~ info:fedora/-obj#ec@t-\"01 ", "info_fedora/obj_ec_t-_01"},
		{"/test/ ~/.../blah", "test/_../blah"},
		{"https://hdl.handle.net/XXXXX/test/bl ah", "https_/hdl.handle.net/XXXXX/test/bl ah"},
		{"abcdefghijabcdefghij abcdefghijabcdefghij abcdefghijabcdefghij abcdefghijabcdefghij abcdefghijabcdefghij abcdefghijabcdefghij abcdefghijabcdefghij abcdefghijabcdefghij abcdefghijabcdefghij abcdefghijabcdefghij abcdefghijabcdefghij abcdefghijabcdefghij abcdefghijabcdefghij", "fallback/0/e/0eafabb38fa7f1583d1461afe980ebdc"},
	})

	// Example #2: encodeUTF == true
	runTest("Example2_True", &DirectCleanConfig{
		ExtensionConfig:             &extension.ExtensionConfig{ExtensionName: extensionName},
		MaxPathnameLen:              32000,
		MaxPathSegmentLen:           127,
		UTFEncode:                   true,
		ReplacementString:           "_",
		WhitespaceReplacementString: " ",
		FallbackDigestAlgorithm:     "sha512",
		FallbackFolder:              "fallback",
		NumberOfFallbackTuples:      2,
		FallbackTupleSize:           1,
	}, []testCase{
		{"..hor_rib:lé-$id", "..hor_rib=u003Alé-$id"},
		{"object=u123a-01", "object=u003Du123a-01"},
		{"object=u13a-01", "object=u13a-01"},
		{"info:fedora/object-01", "info=u003Afedora/object-01"},
		{"~ info:fedora/-obj#ec@t-\"01 ", "=u007E=u0020info=u003Afedora/-obj=u0023ec=u0040t-=u002201=u0020"},
		{"/test/ ~/.../blah", "test/=u0020~/=u002E../blah"},
		{"https://hdl.handle.net/XXXXX/test/bl ah", "https=u003A/hdl.handle.net/XXXXX/test/bl=u0020ah"},
		{"abcdefghijabcdefghij abcdefghijabcdefghij abcdefghijabcdefghij abcdefghijabcdefghij abcdefghijabcdefghij abcdefghijabcdefghij abcdefghijabcdefghij abcdefghijabcdefghij abcdefghijabcdefghij abcdefghijabcdefghij abcdefghijabcdefghij abcdefghijabcdefghij abcdefghijabcdefghij", "fallback/b/8/b8acda4abac53237afa03d6bbb078e1bf46b40438bb256df79b8d9ff0e57b32a688156ad21755363ea19953c160c4dd6d4db175b71e9aa87d68937181a9f69d/9"},
	})
}

func TestDirectCleanPathLayout(t *testing.T) {
	doTestDirectClean(t, DirectCleanName)
}
