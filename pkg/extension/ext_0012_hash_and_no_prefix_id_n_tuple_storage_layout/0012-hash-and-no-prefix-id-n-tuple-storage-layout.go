package ext_0012_hash_and_no_prefix_id_n_tuple_storage_layout

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"strings"

	"emperror.dev/errors"
	"github.com/je4/filesystem/v3/pkg/writefs"
	"github.com/je4/utils/v2/pkg/checksum"
	"github.com/ocfl-archive/gocfl/v3/pkg/appendfs"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/extension"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/storageroot"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfllogger"
)

const StorageLayoutHashAndNoPrefixIdNTupleName = "0012-hash-and-no-prefix-id-n-tuple-storage-layout"
const StorageLayoutHashAndNoPrefixIdNTupleDescription = "Hashed Truncated N-tuple Trees with Non-prefixed Object ID Encapsulating Directory for OCFL Storage Hierarchies"

//go:embed 0012-hash-and-no-prefix-id-n-tuple-storage-layout.md
var StorageLayoutHashAndNoPrefixIdNTupleDoc string

func init() {
	extension.RegisterExtensionStorageRoot(StorageLayoutHashAndNoPrefixIdNTupleName, NewStorageLayoutHashAndNoPrefixIdNTuple, nil, &StorageLayoutHashAndNoPrefixIdNTupleDoc)
}

func NewStorageLayoutHashAndNoPrefixIdNTuple() (extension.Extension, error) {
	config := &StorageLayoutHashAndNoPrefixIdNTupleConfig{
		ExtensionConfig: &extension.ExtensionConfig{ExtensionName: StorageLayoutHashAndNoPrefixIdNTupleName},
		DigestAlgorithm: string(checksum.DigestSHA256),
		TupleSize:       3,
		NumberOfTuples:  3,
		Delimiters:      []string{},
	}
	sl := &StorageLayoutHashAndNoPrefixIdNTuple{StorageLayoutHashAndNoPrefixIdNTupleConfig: config}
	return sl, nil
}

func (sl *StorageLayoutHashAndNoPrefixIdNTuple) WithLogger(logger ocfllogger.OCFLLogger) extension.Extension {
	sl.logger = logger.With("extension", StorageLayoutHashAndNoPrefixIdNTupleName)
	return sl
}

func (sl *StorageLayoutHashAndNoPrefixIdNTuple) Load(data json.RawMessage, extFS fs.FS) error {
	if err := json.Unmarshal(data, sl.StorageLayoutHashAndNoPrefixIdNTupleConfig); err != nil {
		return errors.Wrapf(err, "cannot unmarshal StorageLayoutHashAndNoPrefixIdNTupleConfig '%s'", string(data))
	}
	if sl.NumberOfTuples > 32 {
		sl.NumberOfTuples = 32
	}
	if sl.TupleSize > 32 {
		sl.TupleSize = 32
	}
	if (sl.TupleSize == 0 && sl.NumberOfTuples != 0) || (sl.TupleSize != 0 && sl.NumberOfTuples == 0) {
		sl.NumberOfTuples = 0
		sl.TupleSize = 0
	}
	return nil
}

type StorageLayoutHashAndNoPrefixIdNTupleConfig struct {
	*extension.ExtensionConfig
	DigestAlgorithm string   `json:"digestAlgorithm"`
	TupleSize       int      `json:"tupleSize"`
	NumberOfTuples  int      `json:"numberOfTuples"`
	Delimiters      []string `json:"delimiters"`
}

type StorageLayoutHashAndNoPrefixIdNTuple struct {
	*StorageLayoutHashAndNoPrefixIdNTupleConfig
	logger ocfllogger.OCFLLogger
}

func (sl *StorageLayoutHashAndNoPrefixIdNTuple) Terminate() error {
	return nil
}

func (sl *StorageLayoutHashAndNoPrefixIdNTuple) GetConfig() any {
	return sl.StorageLayoutHashAndNoPrefixIdNTupleConfig
}

func (sl *StorageLayoutHashAndNoPrefixIdNTuple) IsRegistered() bool {
	return true
}

func (sl *StorageLayoutHashAndNoPrefixIdNTuple) GetName() string {
	return StorageLayoutHashAndNoPrefixIdNTupleName
}

func (sl *StorageLayoutHashAndNoPrefixIdNTuple) SetParams(map[string]string) error {
	return nil
}

func (sl *StorageLayoutHashAndNoPrefixIdNTuple) WriteConfig(fsys appendfs.FS) error {
	configWriter, err := writefs.Create(fsys, "config.json")
	if err != nil {
		return errors.Wrap(err, "cannot open config.json")
	}
	defer configWriter.Close()
	jenc := json.NewEncoder(configWriter)
	jenc.SetIndent("", "   ")
	if err := jenc.Encode(sl.StorageLayoutHashAndNoPrefixIdNTupleConfig); err != nil {
		return errors.Wrapf(err, "cannot encode config to file")
	}
	return nil
}

func removePrefixes(objectID string, delimiters []string) string {
	rightmostIdx := -1
	for _, delimiter := range delimiters {
		if len(delimiter) > 0 {
			// Find delimiter in objectID, ignoring one at the very end
			idx := strings.LastIndex(objectID[0:len(objectID)-1], delimiter)
			if idx >= 0 {
				if idx+len(delimiter) > rightmostIdx {
					rightmostIdx = idx + len(delimiter)
				}
			}
		}
	}
	if rightmostIdx > 0 {
		return objectID[rightmostIdx:]
	}
	return objectID
}

func isSafe(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_'
}

func percentEncode(str string) string {
	var result strings.Builder
	for i := 0; i < len(str); i++ {
		c := str[i]
		if isSafe(c) {
			result.WriteByte(c)
		} else {
			result.WriteString(fmt.Sprintf("%%%02x", c))
		}
	}
	return result.String()
}

func getEncapsulationDirectory(objectIDWithNoPrefix string, digest string) string {
	encoded := percentEncode(objectIDWithNoPrefix)
	if len(encoded) > 100 {
		return encoded[:100] + "-" + digest
	}
	return encoded
}

func (sl *StorageLayoutHashAndNoPrefixIdNTuple) BuildStorageRootPath(storageRoot storageroot.StorageRoot, id string) (string, error) {
	idNoPrefix := removePrefixes(id, sl.Delimiters)

	h, err := checksum.GetHash(checksum.DigestAlgorithm(sl.DigestAlgorithm))
	if err != nil {
		return "", errors.Wrapf(err, "cannot get hash for %s", sl.DigestAlgorithm)
	}
	if _, err := h.Write([]byte(idNoPrefix)); err != nil {
		return "", errors.Wrapf(err, "cannot hash %s", idNoPrefix)
	}
	digestBytes := h.Sum(nil)
	digest := fmt.Sprintf("%x", digestBytes)

	if len(digest) < sl.TupleSize*sl.NumberOfTuples {
		return "", errors.Errorf("digest %s too short for %v tuples of %v chars", sl.DigestAlgorithm, sl.NumberOfTuples, sl.TupleSize)
	}

	var dirparts []string
	for i := 0; i < sl.NumberOfTuples; i++ {
		dirparts = append(dirparts, digest[i*sl.TupleSize:(i+1)*sl.TupleSize])
	}

	encapsulationDir := getEncapsulationDirectory(idNoPrefix, digest)
	dirparts = append(dirparts, encapsulationDir)

	return strings.Join(dirparts, "/"), nil
}

func (sl *StorageLayoutHashAndNoPrefixIdNTuple) WriteLayout(fsys appendfs.FS) error {
	configWriter, err := writefs.Create(fsys, "ocfl_layout.json")
	if err != nil {
		return errors.Wrap(err, "cannot open ocfl_layout.json")
	}
	defer func(configWriter writefs.FileWrite) {
		err := configWriter.Close()
		if err != nil {
			sl.logger.Error().Err(err).Msg("failed to close configWriter")
		}
	}(configWriter)
	jenc := json.NewEncoder(configWriter)
	jenc.SetIndent("", "   ")
	if err := jenc.Encode(struct {
		Extension   string `json:"extension"`
		Description string `json:"description"`
	}{
		Extension:   StorageLayoutHashAndNoPrefixIdNTupleName,
		Description: StorageLayoutHashAndNoPrefixIdNTupleDescription,
	}); err != nil {
		return errors.Wrapf(err, "cannot encode config to file")
	}
	return nil
}

// check interface satisfaction
var (
	_ extension.Extension                  = &StorageLayoutHashAndNoPrefixIdNTuple{}
	_ storageroot.ExtensionStorageRootPath = &StorageLayoutHashAndNoPrefixIdNTuple{}
)
