package ext_0009_digest_algorithms

import (
	"encoding/json"
	"path"
	"testing"

	"github.com/ocfl-archive/filesystem/pkg/writefs"
	"github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_0001_digest_algorithms"
	"github.com/ocfl-archive/gocfl-extensions/test"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/extension"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/extension/extensionimpl"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/object"
	"github.com/stretchr/testify/require"
)

func TestNewDigestAlgorithms(t *testing.T) {
	env := test.SetupTestEnv(t)
	digestAlgorithmsConfig := &ext_0001_digest_algorithms.DigestAlgorithmsConfig{
		ExtensionConfig: &extension.ExtensionConfig{ExtensionName: DigestAlgorithmsName},
	}
	data, err := json.MarshalIndent(digestAlgorithmsConfig, "", "  ")
	require.NoError(t, err)
	_, err = writefs.WriteFile(env.ConfigFS, path.Join(DigestAlgorithmsName, "config.json"), data)
	require.NoError(t, err)
	extensionFactory, err := extensionimpl.NewFactory[object.ExtensionManager](nil, env.Logger)
	require.NoError(t, err)
	testFixityDigest, err := extensionFactory.LoadExtensionManager(env.ConfigFS)
	require.NoError(t, err)

	digests := testFixityDigest.GetFixityDigests()
	expectedDigests := []string{
		"blake2b-160",
		"blake2b-256",
		"blake2b-384",
		"blake2b-512",
		"md5",
		"sha512",
		"sha256",
		"sha1",
		"size",
	}
	var digestStrings []string
	for _, d := range digests {
		digestStrings = append(digestStrings, string(d))
	}
	for _, expected := range expectedDigests {
		require.Contains(t, digestStrings, expected)
	}
	require.Equal(t, len(expectedDigests), len(digestStrings), "Digests should match the ones in 0009-digest-algorithms.md")
}
