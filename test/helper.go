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

	"github.com/je4/filesystem/v3/pkg/vfsrw"
	"github.com/je4/filesystem/v3/pkg/writefs"
	"github.com/je4/utils/v2/pkg/checksum"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/ocfl-archive/gocfl-extensions/test/defaultconfig"
	"github.com/ocfl-archive/gocfl/v3/pkg/appendfs"
	"github.com/ocfl-archive/gocfl/v3/pkg/extensions/ext_NNNN_gocfl_extension_manager"
	"github.com/ocfl-archive/gocfl/v3/pkg/extensions/ext_initial"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/extension"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/extension/extensionimpl"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/factory"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/factory/factoryimpl"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/object"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/storageroot"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/storageroot/storagerootimpl"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/version"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfllogger"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
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

type FullTestEnv struct {
	DestFS                             vfsrw.VFSRW
	SourceFS                           appendfs.FS
	ReadSRFS                           fs.FS
	ExtFactory                         extension.Factory
	OCFLFactory                        factory.Factory
	StorageRoot                        storageroot.StorageRoot
	Logger                             zLogger.ZLogger
	OCFLLogger                         ocfllogger.OCFLLogger
	DefaultStorageRootExtensionManager storageroot.ExtensionManager
	DefaultObjectExtensionManager      object.ExtensionManager
}

func SetupFullTestEnv(t *testing.T, extensionParams map[string]string, tempFS, storageRootExtensionFS, objectExtensionFS fs.FS) *FullTestEnv {
	ctx := t.Context()
	out := zerolog.ConsoleWriter{Out: os.Stderr}
	zlogger := zerolog.New(out)
	var _zlogger zLogger.ZLogger = &zlogger
	logger := ocfllogger.NewOCFLLogger(ctx, &zlogger, nil, version.Version1_1, nil)

	cfg := vfsrw.Config{
		"testmem": &vfsrw.VFS{
			Name: "testmem",
			Type: "afero",
			Afero: &vfsrw.Afero{
				BaseDir: "mem://",
			},
		},
	}

	vfs, err := vfsrw.NewFS(cfg, _zlogger)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = vfs.Close()
	})
	err = writefs.MkDir(vfs, "vfs://testmem/storageroot")
	require.NoError(t, err)
	err = writefs.MkDir(vfs, "vfs://testmem/extensionconfig")
	require.NoError(t, err)
	err = writefs.MkDir(vfs, "vfs://testmem/temp")
	require.NoError(t, err)

	extConfigFS, err := writefs.Sub(vfs, "vfs://testmem/extensionconfig")
	require.NoError(t, err)

	err = copyRecursive(defaultconfig.DefaultConfig, extConfigFS, "", "")
	require.NoError(t, err)
	if storageRootExtensionFS != nil {
		err = copyRecursive(storageRootExtensionFS, extConfigFS, "", "storageroot")
		require.NoError(t, err)
	}
	storageRootExtensionFS, err = fs.Sub(extConfigFS, "storageroot")
	require.NoError(t, err)
	if objectExtensionFS != nil {
		err = copyRecursive(objectExtensionFS, extConfigFS, "", "object")
		require.NoError(t, err)
	}
	objectExtensionFS, err = fs.Sub(extConfigFS, "object")
	require.NoError(t, err)

	if tempFS != nil {
		err = copyRecursive(tempFS, vfs, "", "vfs://testmem/temp")
		require.NoError(t, err)
	}

	destFS := appendfs.FS(vfs)

	srFS, err := appendfs.Sub(destFS, "vfs://testmem/storageroot")
	require.NoError(t, err)

	extFactory, err := extensionimpl.NewFactory(extensionParams, logger)
	require.NoError(t, err)
	storageRootExtManager0, err := extFactory.LoadExtensionManager(storageRootExtensionFS)
	require.NoError(t, err)
	storageRootExtManager, ok := storageRootExtManager0.(storageroot.ExtensionManager)
	assert.True(t, ok, "extension manager should implement storageroot.ExtensionManager")
	objectExtManager0, err := extFactory.LoadExtensionManager(objectExtensionFS)
	require.NoError(t, err)
	objectExtManager, ok := objectExtManager0.(object.ExtensionManager)
	assert.True(t, ok, "extension manager should implement object.ExtensionManager")

	ocflVer := version.Version1_1
	fact := factoryimpl.NewFactory(ocflVer, extFactory, logger)

	sr := storagerootimpl.NewStorageRootBase(ctx, fact, ocflVer, extFactory, logger)
	sr.WithWriteFS(srFS)
	sr.WithDigestAlgorithm(checksum.DigestSHA512)

	sr.WithExtensionManager(storageRootExtManager)

	initializer := sr.GetInitializer()
	assert.NotNil(t, initializer)
	initializer.WithFS(srFS)

	err = initializer.Init()
	require.NoError(t, err)

	// Lese-Dateisystem für Storage Root (als fs.FS) neu laden
	// Wir nutzen hier das ursprüngliche destFS, da es für In-Memory VFS okay ist.
	// In echten Szenarien (ZIP) müsste das Dateisystem neu geöffnet werden.
	readSRFS_append, err := appendfs.Sub(destFS, "vfs://testmem/storageroot")
	require.NoError(t, err)
	readSRFS := fs.FS(readSRFS_append)

	return &FullTestEnv{
		DestFS:                             vfs,
		SourceFS:                           srFS,
		ReadSRFS:                           readSRFS,
		ExtFactory:                         extFactory,
		OCFLFactory:                        fact,
		StorageRoot:                        sr,
		Logger:                             _zlogger,
		OCFLLogger:                         logger,
		DefaultStorageRootExtensionManager: storageRootExtManager,
		DefaultObjectExtensionManager:      objectExtManager,
	}
}

func CreateTestObject(t *testing.T, env *FullTestEnv, objID string) (object.Object, appendfs.FS) {
	objFolder, err := env.StorageRoot.IdToFolder(objID)
	require.NoError(t, err)

	objFS, err := appendfs.Sub(env.SourceFS, objFolder)
	require.NoError(t, err)
	obj, err := env.StorageRoot.CreateObject(objID, checksum.DigestSHA512, []checksum.DigestAlgorithm{}, env.DefaultObjectExtensionManager)
	require.NoError(t, err)

	objInit := obj.GetInitializer(objFS)
	err = objInit.Init(objID, checksum.DigestSHA512, []checksum.DigestAlgorithm{})
	require.NoError(t, err)

	return obj, objFS
}

func ReloadObject(t *testing.T, env *FullTestEnv, objID string) (object.Object, fs.FS) {
	objFolder, err := env.StorageRoot.IdToFolder(objID)
	require.NoError(t, err)

	// Wir nutzen env.ReadSRFS als Lese-Dateisystem
	objFS, err := fs.Sub(env.ReadSRFS, objFolder)
	require.NoError(t, err)

	loadedObj := env.OCFLFactory.NewObject(t.Context())
	loader := loadedObj.GetLoader(objFS, env.ExtFactory)
	err = loader.Load()
	require.NoError(t, err)

	return loadedObj, objFS
}
