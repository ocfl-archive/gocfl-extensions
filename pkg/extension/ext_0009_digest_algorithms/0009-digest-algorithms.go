package ext_0009_digest_algorithms

import (
	_ "embed"

	"github.com/je4/utils/v2/pkg/checksum"
	"github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_0001_digest_algorithms"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/extension"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/object"
)

const DigestAlgorithmsName = "0009-digest-algorithms"
const DigestAlgorithmsDescription = "controlled vocabulary of digest algorithm names that may be used to indicate the given algorithm in fixity blocks of OCFL Objects"

//go:embed 0009-digest-algorithms.md
var DigestAlgorithmsDoc string

func init() {
	extension.RegisterExtension(DigestAlgorithmsName, NewDigestAlgorithms, nil)
}

var algorithms = []checksum.DigestAlgorithm{
	checksum.DigestBlake2b160,
	checksum.DigestBlake2b256,
	checksum.DigestBlake2b384,
	checksum.DigestBlake2b512,
	checksum.DigestMD5,
	checksum.DigestSHA512,
	checksum.DigestSHA256,
	checksum.DigestSHA1,
	checksum.DigestSize,
}

func NewDigestAlgorithms() (extension.Extension, error) {
	var config = &ext_0001_digest_algorithms.DigestAlgorithmsConfig{
		ExtensionConfig: &extension.ExtensionConfig{
			ExtensionName: DigestAlgorithmsName,
		},
	}
	sl := &DigestAlgorithms{
		&ext_0001_digest_algorithms.DigestAlgorithms{DigestAlgorithmsConfig: config},
	}
	return sl, nil
}

type DigestAlgorithms struct {
	*ext_0001_digest_algorithms.DigestAlgorithms
}

func (sl *DigestAlgorithms) GetDocumentation() string { return DigestAlgorithmsDoc }
func (sl *DigestAlgorithms) GetDescription() string   { return DigestAlgorithmsDescription }

func (sl *DigestAlgorithms) IsRegistered() bool {
	return true
}

func (sl *DigestAlgorithms) GetFixityDigests() []checksum.DigestAlgorithm {
	return algorithms
}

func (sl *DigestAlgorithms) GetName() string { return DigestAlgorithmsName }

// check interface satisfaction
var (
	_ extension.Extension          = &DigestAlgorithms{}
	_ object.ExtensionFixityDigest = &DigestAlgorithms{}
)
