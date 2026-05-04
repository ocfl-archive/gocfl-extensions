package extension

import (
	"encoding/json"
	"io/fs"
	"os"
	"path"
	"testing"

	"github.com/je4/filesystem/v3/pkg/vfsrw"
	"github.com/je4/filesystem/v3/pkg/writefs"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/ocfl-archive/gocfl/v3/pkg/extensions/ext_NNNN_gocfl_extension_manager"
	"github.com/ocfl-archive/gocfl/v3/pkg/extensions/ext_initial"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/extension"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/version"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfllogger"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

type TestEnv struct {
	ConfigFS fs.FS
	Logger   *ocfllogger.OCFLLoggerImpl
}

func SetupTestEnv(t *testing.T) *TestEnv {
	ctx := t.Context()
	out := zerolog.ConsoleWriter{Out: os.Stderr}
	zlogger := zerolog.New(out)
	var _zlogger zLogger.ZLogger = &zlogger
	logger := ocfllogger.NewOCFLLogger(ctx, &zlogger, nil, version.Version1_1, nil)

	cfg := vfsrw.Config{
		"extensionconfig": &vfsrw.VFS{
			Name: "extensionconfig",
			Type: "afero",
			Afero: &vfsrw.Afero{
				BaseDir: "mem://",
			},
		},
	}

	vfs, err := vfsrw.NewFS(cfg, _zlogger)
	assert.NoError(t, err)
	extensionConfigFS, err := writefs.Sub(vfs, "vfs://extensionconfig")
	assert.NoError(t, err)
	extensionManagerConfig := &extension.ManagerConfig{
		ExtensionConfig: &extension.ExtensionConfig{ExtensionName: ext_NNNN_gocfl_extension_manager.GOCFLExtensionManagerName},
		Sort:            nil,
		Exclusion:       nil,
	}
	data, _ := json.MarshalIndent(extensionManagerConfig, "", "  ")
	_, err = writefs.WriteFile(extensionConfigFS, path.Join(ext_NNNN_gocfl_extension_manager.GOCFLExtensionManagerName, "config.json"), data)
	assert.NoError(t, err)

	initialConfig := ext_initial.InitialConfig{
		ExtensionConfig: &extension.ExtensionConfig{ExtensionName: ext_initial.InitialName},
		Extension:       ext_NNNN_gocfl_extension_manager.GOCFLExtensionManagerName,
	}
	data, _ = json.MarshalIndent(initialConfig, "", "  ")
	_, err = writefs.WriteFile(extensionConfigFS, path.Join(ext_initial.InitialName, "config.json"), data)
	assert.NoError(t, err)

	return &TestEnv{
		ConfigFS: extensionConfigFS,
		Logger:   logger,
	}
}
