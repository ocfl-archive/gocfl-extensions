package ext_0009_digest_algorithms

import (
	"encoding/json"
	"path"
	"testing"

	"github.com/je4/filesystem/v3/pkg/writefs"
	"github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_0001_digest_algorithms"
	"github.com/ocfl-archive/gocfl-extensions/test"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/extension"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/extension/extensionimpl"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/object"
	"github.com/stretchr/testify/assert"
)

func TestNewDigestAlgorithms(t *testing.T) {
	env := test.SetupTestEnv(t)
	digestAlgorithmsConfig := &ext_0001_digest_algorithms.DigestAlgorithmsConfig{
		ExtensionConfig: &extension.ExtensionConfig{ExtensionName: DigestAlgorithmsName},
	}
	data, _ := json.MarshalIndent(digestAlgorithmsConfig, "", "  ")
	_, err := writefs.WriteFile(env.ConfigFS, path.Join(DigestAlgorithmsName, "config.json"), data)
	assert.NoError(t, err)
	extensionFactory, err := extensionimpl.NewFactory(nil, env.Logger)
	assert.NoError(t, err)
	genericExtensionManager, err := extensionFactory.LoadExtensionManager(env.ConfigFS)
	assert.NoError(t, err)
	testFixityDigest, ok := genericExtensionManager.(object.ExtensionFixityDigest)
	assert.True(t, ok, "Extension manager should implement ExtensionFixityDigest interface")

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
		assert.Contains(t, digestStrings, expected)
	}
	assert.Equal(t, len(expectedDigests), len(digestStrings), "Digests should match the ones in 0009-digest-algorithms.md")
}
