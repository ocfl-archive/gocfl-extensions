package ext_0010_differential_n_tuple_omit_prefix_storage_layout

import (
	_ "embed"
	"encoding/json"
	"io"
	"strings"

	"emperror.dev/errors"
	"github.com/je4/filesystem/v3/pkg/writefs"
	"github.com/ocfl-archive/gocfl/v3/pkg/appendfs"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/extension"
	extensiontypes "github.com/ocfl-archive/gocfl/v3/pkg/ocfl/extension"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/object"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/storageroot"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfllogger"
)

const DifferentialNTupleOmitPrefixStorageLayoutName = "0010-differential-n-tuple-omit-prefix-storage-layout"
const DifferentialNTupleOmitPrefixStorageLayoutDescription = "pairtree-inspired root directory structure intended to support identifiers with differential tuple sizes"

//go:embed 0010-differential-n-tuple-omit-prefix-storage-layout.md
var DifferentialNTupleOmitPrefixStorageLayoutDoc string

func init() {
	extension.RegisterExtension(DifferentialNTupleOmitPrefixStorageLayoutName, NewDifferentialNTupleOmitPrefixStorageLayout, nil, &DifferentialNTupleOmitPrefixStorageLayoutDoc)
}

func NewDifferentialNTupleOmitPrefixStorageLayout() (extensiontypes.Extension, error) {
	config := &DifferentialNTupleOmitPrefixStorageLayoutConfig{
		ExtensionConfig:            &extensiontypes.ExtensionConfig{ExtensionName: DifferentialNTupleOmitPrefixStorageLayoutName},
		Delimiter:                  ":",
		TupleSegmentSizes:          []int{2, 3, 2, 4},
		FullIdentifierAsObjectRoot: false,
	}
	sl := &DifferentialNTupleOmitPrefixStorageLayout{DifferentialNTupleOmitPrefixStorageLayoutConfig: config}
	return sl, nil
}

func (sl *DifferentialNTupleOmitPrefixStorageLayout) WithLogger(logger ocfllogger.OCFLLogger) extensiontypes.Extension {
	sl.logger = logger.With("extension", DifferentialNTupleOmitPrefixStorageLayoutName)
	return sl
}

func (sl *DifferentialNTupleOmitPrefixStorageLayout) Load(data json.RawMessage) error {
	if err := json.Unmarshal(data, sl.DifferentialNTupleOmitPrefixStorageLayoutConfig); err != nil {
		return errors.Wrapf(err, "cannot unmarshal DifferentialNTupleOmitPrefixStorageLayoutConfig '%s'", string(data))
	}
	if sl.Delimiter == "" {
		sl.Delimiter = ":"
	}
	return nil
}

type DifferentialNTupleOmitPrefixStorageLayoutConfig struct {
	*extensiontypes.ExtensionConfig
	Delimiter                  string `json:"delimiter"`
	TupleSegmentSizes          []int  `json:"tupleSegmentSizes"`
	FullIdentifierAsObjectRoot bool   `json:"fullIdentifierAsObjectRoot"`
}

type DifferentialNTupleOmitPrefixStorageLayout struct {
	*DifferentialNTupleOmitPrefixStorageLayoutConfig
	logger ocfllogger.OCFLLogger
}

func (sl *DifferentialNTupleOmitPrefixStorageLayout) Terminate() error {
	return nil
}

func (sl *DifferentialNTupleOmitPrefixStorageLayout) GetConfig() any {
	return sl.DifferentialNTupleOmitPrefixStorageLayoutConfig
}

func (sl *DifferentialNTupleOmitPrefixStorageLayout) IsRegistered() bool {
	return true
}

func (sl *DifferentialNTupleOmitPrefixStorageLayout) Stat(w io.Writer, statInfo []object.StatInfo) error {
	return nil
}

func (sl *DifferentialNTupleOmitPrefixStorageLayout) SetParams(map[string]string) error {
	return nil
}

func (sl *DifferentialNTupleOmitPrefixStorageLayout) GetName() string {
	return DifferentialNTupleOmitPrefixStorageLayoutName
}

func (sl *DifferentialNTupleOmitPrefixStorageLayout) WriteConfig(fsys appendfs.FS) error {
	configWriter, err := writefs.Create(fsys, "config.json")
	if err != nil {
		return errors.Wrap(err, "cannot open config.json")
	}
	defer configWriter.Close()
	jenc := json.NewEncoder(configWriter)
	jenc.SetIndent("", "   ")
	if err := jenc.Encode(sl.ExtensionConfig); err != nil {
		return errors.Wrapf(err, "cannot encode config to file")
	}
	return nil
}

func (sl *DifferentialNTupleOmitPrefixStorageLayout) WriteLayout(fsys appendfs.FS) error {
	configWriter, err := writefs.Create(fsys, "ocfl_layout.json")
	if err != nil {
		return errors.Wrap(err, "cannot open ocfl_layout.json")
	}
	defer configWriter.Close()
	jenc := json.NewEncoder(configWriter)
	jenc.SetIndent("", "   ")
	if err := jenc.Encode(struct {
		Extension   string `json:"extension"`
		Description string `json:"description"`
	}{
		Extension:   DifferentialNTupleOmitPrefixStorageLayoutName,
		Description: DifferentialNTupleOmitPrefixStorageLayoutDescription,
	}); err != nil {
		return errors.Wrapf(err, "cannot encode config to file")
	}
	return nil
}

func (sl *DifferentialNTupleOmitPrefixStorageLayout) BuildStorageRootPath(storageRoot storageroot.StorageRoot, id string) (string, error) {
	/*
		1. Remove the prefix, which is everything to the left of the right-most instance of the delimiter, as well as the delimiter.
		   If there is no delimiter, the whole id is used; if the delimiter is found at the end, an error is thrown.
	*/
	prefixOmittedID := id
	if sl.Delimiter != "" {
		last := strings.LastIndex(id, sl.Delimiter)
		if last >= 0 {
			if last == len(id)-len(sl.Delimiter) {
				return "", errors.Errorf("delimiter '%s' found at the end of ID '%s'", sl.Delimiter, id)
			}
			prefixOmittedID = id[last+len(sl.Delimiter):]
		}
	}

	/*
		2. Starting at the leftmost character of the resulting id and working right, divide the id into segments, where the number of segments is equal to the number of elements in the tupleSegmentSizes parameter array and the character size of each segment from left to right equals the corresponding integer value in the tupleSegmentSizes array.
		   If the length of the identifier does not equal the sum of the tupleSegmentSizes, an error is thrown.
	*/
	totalSize := 0
	for _, size := range sl.TupleSegmentSizes {
		totalSize += size
	}
	if len(prefixOmittedID) != totalSize {
		return "", errors.Errorf("length of prefix-omitted ID '%s' (%d) does not equal sum of tupleSegmentSizes (%d)", prefixOmittedID, len(prefixOmittedID), totalSize)
	}

	// ASCII subset check (0x20 to 0x7F)
	for i := 0; i < len(prefixOmittedID); i++ {
		if prefixOmittedID[i] < 0x20 || prefixOmittedID[i] > 0x7F {
			return "", errors.Errorf("ID '%s' contains non-ASCII character 0x%02x", prefixOmittedID, prefixOmittedID[i])
		}
	}

	var pathComponents []string
	currentPos := 0
	for _, size := range sl.TupleSegmentSizes {
		pathComponents = append(pathComponents, prefixOmittedID[currentPos:currentPos+size])
		currentPos += size
	}

	/*
		3. Create the start of the object root path by joining the tuples, in order, using the filesystem path separator.
	*/
	/*
		4. Optionally, if fullIdentifierAsObjectRoot is true, complete the object root path by joining the prefix-omitted id (from step 1) onto the end after another filesystem path separator.
	*/
	if sl.FullIdentifierAsObjectRoot {
		pathComponents = append(pathComponents, prefixOmittedID)
	}

	result := strings.Join(pathComponents, "/")
	return result, nil
}

// check interface satisfaction
var (
	_ extensiontypes.Extension             = &DifferentialNTupleOmitPrefixStorageLayout{}
	_ storageroot.ExtensionStorageRootPath = &DifferentialNTupleOmitPrefixStorageLayout{}
)
