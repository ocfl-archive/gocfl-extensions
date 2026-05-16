package test

import (
	"encoding/json"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/je4/filesystem/v4/pkg/appendfs"
	"github.com/je4/filesystem/v4/pkg/vfsrw"
	"github.com/je4/filesystem/v4/pkg/writefs"
	"github.com/je4/utils/v2/pkg/checksum"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/ocfl-archive/gocfl-extensions/test/defaultconfig"
	"github.com/ocfl-archive/gocfl/v3/pkg/extensions/ext_NNNN_gocfl_extension_manager"
	"github.com/ocfl-archive/gocfl/v3/pkg/extensions/ext_initial"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/extension"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/initocfl"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/object"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/storageroot"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/version"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfllogger"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func copyRecursive(srcFS, dstFS fs.FS, src, dst string) error {
	src = path.Clean(filepath.ToSlash(src))
	dst = path.Clean(filepath.ToSlash(dst))
	return fs.WalkDir(srcFS, src, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		relDst := strings.TrimPrefix(filepath.ToSlash(p), src)
		if d.IsDir() {
			if relDst == "" {
				return nil
			}
			// Verzeichnis im Ziel erstellen
			return writefs.MkDir(dstFS, relDst)
		}

		// Datei kopieren
		return copyFile(srcFS, dstFS, p, path.Join(dst, relDst))
	})
}

func copyFile(srcFS, dstFS fs.FS, src, dest string) error {
	sourceFile, err := srcFS.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	// Zieldatei erstellen
	destFile, err := writefs.Create(dstFS, dest)
	if err != nil {
		return err
	}
	defer destFile.Close()

	// Inhalt kopieren
	if _, err := io.Copy(destFile, sourceFile); err != nil {
		return err
	}
	return nil
}

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
	require.NoError(t, err)
	extensionConfigFS, err := writefs.Sub(vfs, "vfs://extensionconfig")
	require.NoError(t, err)
	extensionManagerConfig := &extension.ManagerConfig{
		ExtensionConfig: &extension.ExtensionConfig{ExtensionName: ext_NNNN_gocfl_extension_manager.GOCFLExtensionManagerName},
		Sort:            nil,
		Exclusion:       nil,
	}
	data, _ := json.MarshalIndent(extensionManagerConfig, "", "  ")
	_, err = writefs.WriteFile(extensionConfigFS, path.Join(ext_NNNN_gocfl_extension_manager.GOCFLExtensionManagerName, "config.json"), data)
	require.NoError(t, err)

	initialConfig := ext_initial.InitialConfig{
		ExtensionConfig: &extension.ExtensionConfig{ExtensionName: ext_initial.InitialName},
		Extension:       ext_NNNN_gocfl_extension_manager.GOCFLExtensionManagerName,
	}
	data, _ = json.MarshalIndent(initialConfig, "", "  ")
	_, err = writefs.WriteFile(extensionConfigFS, path.Join(ext_initial.InitialName, "config.json"), data)
	require.NoError(t, err)

	return &TestEnv{
		ConfigFS: extensionConfigFS,
		Logger:   logger,
	}
}

// FullTestEnv contains all components required for a complete OCFL test.
type FullTestEnv struct {
	DestFS                      vfsrw.VFSRW                  // The underlying virtual read/write file system.
	TargetFS                    appendfs.FS                  // File system for write access (append support).
	ReadSRFS                    fs.FS                        // Read-only file system for the storage root.
	OCFLFactorySR               storageroot.Factory          // Factory for OCFL storage roots.
	StorageRoot                 storageroot.StorageRoot      // The initialized OCFL storage root.
	OCFLLogger                  ocfllogger.OCFLLogger        // OCFL-specific logger.
	StorageRootExtensionManager storageroot.ExtensionManager // manager for storage root extensions.
	ObjectExtensionManager      object.ExtensionManager      // manager for object extensions.
}

// SetupFullTestEnv initializes a complete test environment including an in-memory file system and OCFL storage root.
func SetupFullTestEnv(t *testing.T, extensionParams map[string]string, tempFS, storageRootExtensionFS, objectExtensionFS fs.FS) *FullTestEnv {
	ctx := t.Context()
	// Logger setup
	out := zerolog.ConsoleWriter{Out: os.Stderr}
	zlogger := zerolog.New(out)
	var _zlogger zLogger.ZLogger = &zlogger
	logger := ocfllogger.NewOCFLLogger(ctx, &zlogger, nil, version.Version1_1, nil)

	// In-memory VFS configuration (afero mem://)
	cfg := vfsrw.Config{
		"testmem": &vfsrw.VFS{
			Name: "testmem",
			Type: "afero",
			Afero: &vfsrw.Afero{
				BaseDir: "mem://",
			},
		},
	}

	// Initialize VFS
	vfs, err := vfsrw.NewFS(cfg, _zlogger)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = vfs.Close()
	})

	// Create directory structure in VFS
	err = writefs.MkDir(vfs, "vfs://testmem/storageroot")
	require.NoError(t, err)
	err = writefs.MkDir(vfs, "vfs://testmem/extensionconfig")
	require.NoError(t, err)
	err = writefs.MkDir(vfs, "vfs://testmem/temp")
	require.NoError(t, err)

	extConfigFS, err := writefs.Sub(vfs, "vfs://testmem/extensionconfig")
	require.NoError(t, err)

	// Copy default configurations
	err = copyRecursive(defaultconfig.DefaultConfig, extConfigFS, "", "")
	require.NoError(t, err)

	// copy custom storage root extension configurations
	if storageRootExtensionFS != nil {
		err = copyRecursive(storageRootExtensionFS, extConfigFS, "", "storageroot")
		require.NoError(t, err)
	}
	storageRootExtensionFS, err = fs.Sub(extConfigFS, "storageroot")
	require.NoError(t, err)

	// copy custom object extension configurations
	if objectExtensionFS != nil {
		err = copyRecursive(objectExtensionFS, extConfigFS, "", "object")
		require.NoError(t, err)
	}
	objectExtensionFS, err = fs.Sub(extConfigFS, "object")
	require.NoError(t, err)

	// Copy additional temporary files
	if tempFS != nil {
		err = copyRecursive(tempFS, vfs, "", "vfs://testmem/temp")
		require.NoError(t, err)
	}

	// Create writable file system used for initialization
	destFS, err := appendfs.New(vfs)
	require.NoError(t, err)

	// storage root subdirectory for initialization
	srFS, err := appendfs.Sub(destFS, "vfs://testmem/storageroot")
	require.NoError(t, err)

	ocflVer := version.Version1_1

	// Initialize extension factory and managers
	storageRootExtManager, _, err := initocfl.SetupExtensionManager[storageroot.ExtensionManager](extensionParams, storageRootExtensionFS, logger)
	require.NoError(t, err)
	objectExtManager, _, err := initocfl.SetupExtensionManager[object.ExtensionManager](extensionParams, objectExtensionFS, logger)
	require.NoError(t, err)

	// Initialize storage root
	sr, err := initocfl.InitStorageRoot(ctx, srFS, nil, ocflVer, checksum.DigestSHA512, nil, logger)
	require.NoError(t, err)

	if storageRootExtManager != nil {
		sr.WithExtensionManager(storageRootExtManager)
	}

	// Reload read-only file system for storage root (as fs.FS)
	// We use the original destFS here as it's fine for in-memory VFS.
	// In real scenarios (ZIP), the file system would need to be reopened.
	readSRFS_append, err := appendfs.Sub(destFS, "vfs://testmem/storageroot")
	require.NoError(t, err)
	readSRFS := fs.FS(readSRFS_append)

	return &FullTestEnv{
		DestFS:                      vfs,
		TargetFS:                    srFS,
		ReadSRFS:                    readSRFS,
		OCFLFactorySR:               nil, // not used anymore but kept for struct compatibility
		StorageRoot:                 sr,
		OCFLLogger:                  logger,
		StorageRootExtensionManager: storageRootExtManager,
		ObjectExtensionManager:      objectExtManager,
	}
}

func CreateTestObject(t *testing.T, env *FullTestEnv, objID string) (object.Object, appendfs.FS) {
	objFolder, err := env.StorageRoot.IdToFolder(objID)
	require.NoError(t, err)

	objFS, err := appendfs.Sub(env.TargetFS, objFolder)
	require.NoError(t, err)

	obj, err := initocfl.InitObject(t.Context(), objFS, nil, version.Version1_1, objID, checksum.DigestSHA512, nil, env.OCFLLogger)
	require.NoError(t, err)

	if env.ObjectExtensionManager != nil {
		obj.WithExtensionManager(env.ObjectExtensionManager)
	}

	return obj, objFS
}

func ReloadObject(t *testing.T, env *FullTestEnv, objID string) (object.Object, fs.FS) {
	objFolder, err := env.StorageRoot.IdToFolder(objID)
	require.NoError(t, err)

	// Wir nutzen env.ReadSRFS als Lese-Dateisystem
	objFS, err := fs.Sub(env.ReadSRFS, objFolder)
	require.NoError(t, err)

	loadedObj, _, err := initocfl.LoadObject(t.Context(), objFS, nil, env.OCFLLogger)
	require.NoError(t, err)

	if env.ObjectExtensionManager != nil {
		loadedObj.WithExtensionManager(env.ObjectExtensionManager)
	}

	return loadedObj, objFS
}
