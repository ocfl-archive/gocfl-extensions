package ext_NNNN_direct_path_layout

import (
	_ "embed"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"io/fs"

	"emperror.dev/errors"
	"github.com/ocfl-archive/filesystem/pkg/appendfs"
	"github.com/ocfl-archive/filesystem/pkg/writefs"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/extension"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/storageroot"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfllogger"
)

const DirectPathLayoutName = "NNNN-direct-path-layout"
const DirectPathLayoutDescription = "direct path storage layout"

//go:embed NNNN-direct-path-layout.md
var DirectPathLayoutDoc string

func init() {
	extension.RegisterExtensionStorageRoot(DirectPathLayoutName, NewDirectPathLayout, nil, &DirectPathLayoutDoc)
}

type DirectPathLayoutConfig struct {
	*extension.ExtensionConfig
}

type DirectPathLayout struct {
	*DirectPathLayoutConfig
	logger ocfllogger.OCFLLogger
}

func NewDirectPathLayout() (extension.Extension, error) {
	var config = &DirectPathLayoutConfig{
		ExtensionConfig: &extension.ExtensionConfig{
			ExtensionName: DirectPathLayoutName,
		},
	}
	sl := &DirectPathLayout{
		DirectPathLayoutConfig: config,
	}
	return sl, nil
}

func (sl *DirectPathLayout) Load(data jsontext.Value, extFS fs.FS) error {
	if err := json.Unmarshal(data, sl.DirectPathLayoutConfig); err != nil {
		return errors.Wrapf(err, "cannot unmarshal DirectPathLayoutConfig '%s'", string(data))
	}
	return nil
}

func (sl *DirectPathLayout) GetConfig() any {
	return sl.DirectPathLayoutConfig
}

func (sl *DirectPathLayout) IsRegistered() bool {
	return false
}

func (sl *DirectPathLayout) WithLogger(logger ocfllogger.OCFLLogger) extension.Extension {
	sl.logger = logger.With("extension", DirectPathLayoutName)
	return sl
}

func (sl *DirectPathLayout) Terminate() error {
	return nil
}

func (sl *DirectPathLayout) SetParams(params map[string]string) error {
	return nil
}

func (sl *DirectPathLayout) GetName() string { return DirectPathLayoutName }

func (sl *DirectPathLayout) GetConfigString() string {
	str, _ := json.Marshal(sl.DirectPathLayoutConfig, jsontext.WithIndent("  "))
	return string(str)
}

func (sl *DirectPathLayout) WriteLayout(fsys appendfs.FS) error {
	configWriter, err := writefs.Create(fsys, "ocfl_layout.json")
	if err != nil {
		return errors.Wrap(err, "cannot open ocfl_layout.json")
	}
	defer configWriter.Close()
	if err := json.MarshalWrite(configWriter, struct {
		Extension   string `json:"extension"`
		Description string `json:"description"`
	}{
		Extension:   DirectPathLayoutName,
		Description: DirectPathLayoutDescription,
	}, jsontext.WithIndent("   ")); err != nil {
		return errors.Wrapf(err, "cannot encode config to file")
	}
	return nil
}

func (sl *DirectPathLayout) WriteConfig(fsys appendfs.FS) error {
	configWriter, err := writefs.Create(fsys, "config.json")
	if err != nil {
		return errors.Wrap(err, "cannot open config.json")
	}
	defer configWriter.Close()
	if err := json.MarshalWrite(configWriter, sl.ExtensionConfig, jsontext.WithIndent("   ")); err != nil {
		return errors.Wrapf(err, "cannot encode config to file")
	}
	return nil
}

func (sl *DirectPathLayout) BuildStorageRootPath(storageRoot storageroot.StorageRoot, id string) (string, error) {
	return id, nil
}

// check interface satisfaction
var (
	_ extension.Extension                  = &DirectPathLayout{}
	_ storageroot.ExtensionStorageRootPath = &DirectPathLayout{}
)
