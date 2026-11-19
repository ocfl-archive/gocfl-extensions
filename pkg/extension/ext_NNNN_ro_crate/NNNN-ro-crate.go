// Package ext_NNNN_ro_crate implements the OCFL extension NNNN-ro-crate.
//
// This extension processes RO-Crate metadata (ro-crate-metadata.json) and maps
// descriptive metadata into a GOCFL-compatible metadata summary file.
package ext_NNNN_ro_crate

import (
	"bytes"
	_ "embed"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"io"
	"io/fs"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"emperror.dev/errors"
	"github.com/ocfl-archive/filesystem/pkg/appendfs"
	"github.com/ocfl-archive/filesystem/pkg/writefs"
	ext "github.com/ocfl-archive/gocfl-extensions/pkg/extension"
	"github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_ro_crate/rocrate"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/extension"
	extensiontypes "github.com/ocfl-archive/gocfl/v3/pkg/ocfl/extension"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/inventory"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/object"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfllogger"
)

const ROCrateFileName = "NNNN-ro-crate"
const ROCrateAliasName = "NNNN-rocrate"
const ROCrateEnabled = "enabled"

// ext prefix.
const extPrefix = "ext"

// Additional parameters for RO-CRATE metafile.
const rOCrateParamOrg = "organisation"
const rOCrateParamOrgID = "organisationID"
const rOCrateParamUser = "user"
const rOCrateParamAddress = "address"

const ROCrateFileDescription = "Description for RO-Crate extension"

//go:embed NNNN-ro-crate.md
var ROCrateDoc string

func init() {
	extension.RegisterExtensionObject(ROCrateFileName, func() (extensiontypes.Extension, error) {
		return NewROCrate(nil, nil)
	}, GetROCrateParams, &ROCrateDoc)
	extension.RegisterExtensionObject(ROCrateAliasName, func() (extensiontypes.Extension, error) {
		return NewROCrate(nil, nil)
	}, GetROCrateParams, &ROCrateDoc)
}

func Init(fSys fs.FS, logger ocfllogger.OCFLLogger) {
	extension.RegisterExtensionObject(ROCrateFileName, func() (extensiontypes.Extension, error) {
		return NewROCrate(fSys, logger)
	}, GetROCrateParams, &ROCrateDoc)
	extension.RegisterExtensionObject(ROCrateAliasName, func() (extensiontypes.Extension, error) {
		return NewROCrate(fSys, logger)
	}, GetROCrateParams, &ROCrateDoc)
}

func GetROCrateParams() ([]*extension.ExternalParam, error) {
	return []*extension.ExternalParam{
		{
			ExtensionName: ROCrateFileName,
			Functions:     []string{"add", "create"},
			Param:         ROCrateEnabled,
			Description:   "replace metafile extension functionality if enabled and map RO-CRATE metadata",
			Default:       "false",
		},
		{
			ExtensionName: ROCrateFileName,
			Functions:     []string{"add", "create"},
			Param:         rOCrateParamOrg,
			Description:   "provide institutional organisation to the RO-CRATE extension metadata",
			Default:       "",
		},
		{
			ExtensionName: ROCrateFileName,
			Functions:     []string{"add", "create"},
			Param:         rOCrateParamOrgID,
			Description:   "provide institutional organisation ID to the RO-CRATE extension metadata",
			Default:       "",
		},
		{
			ExtensionName: ROCrateFileName,
			Functions:     []string{"add", "create"},
			Param:         rOCrateParamUser,
			Description:   "provide institutional user to the RO-CRATE extension metadata",
			Default:       "",
		},
		{
			ExtensionName: ROCrateFileName,
			Functions:     []string{"add", "create"},
			Param:         rOCrateParamAddress,
			Description:   "provide user address to the RO-CRATE extension metadata",
			Default:       "",
		},
	}, nil
}

// ROCrateFileConfig contains the serializable configuration of the extension.
type ROCrateFileConfig struct {
	*extensiontypes.ExtensionConfig
	StorageType string `json:"storageType"`
	StorageName string `json:"storageName"`
	MetaName    string `json:"metaFileName,omitempty"`
}

// userConfig stores the additional metadata a user provides.
type userConfig struct {
	organisation   string
	organisationID string
	user           string
	address        string
}

// ROCrateFile represents an instance of the RO-Crate extension.
type ROCrateFile struct {
	*ROCrateFileConfig
	fSys       fs.FS
	info       map[string][]byte
	enabled    bool
	userConfig *userConfig
	logger     ocfllogger.OCFLLogger
}

// NewROCrate creates a new ROCrateFile instance.
func NewROCrate(fSys fs.FS, logger ocfllogger.OCFLLogger) (*ROCrateFile, error) {
	var config = &ROCrateFileConfig{
		ExtensionConfig: &extensiontypes.ExtensionConfig{
			ExtensionName: ROCrateFileName,
		},
		StorageType: "extension",
		StorageName: "",
		MetaName:    "info.json",
	}
	rc := &ROCrateFile{
		ROCrateFileConfig: config,
		fSys:              fSys,
		logger:            logger,
		info:              map[string][]byte{},
		userConfig:        &userConfig{},
	}
	return rc, nil
}

func (rc *ROCrateFile) WithLogger(logger ocfllogger.OCFLLogger) extensiontypes.Extension {
	rc.logger = logger.With("extension", ROCrateFileName)
	return rc
}

func (rc *ROCrateFile) GetName() string {
	return ROCrateFileName
}

func (rc *ROCrateFile) Load(data jsontext.Value, extFS fs.FS) error {
	if err := json.Unmarshal(data, rc.ROCrateFileConfig); err != nil {
		return errors.Wrapf(err, "cannot unmarshal ROCrateFileConfig '%s'", string(data))
	}
	if rc.MetaName == "" {
		rc.MetaName = "info.json"
	}
	if rc.StorageType == "" {
		rc.StorageType = "extension"
	}
	return nil
}

func (rc *ROCrateFile) Terminate() error {
	return nil
}

func (rc *ROCrateFile) GetConfig() any {
	return rc.ROCrateFileConfig
}

func (rc *ROCrateFile) IsRegistered() bool {
	return false
}

// ParamROCrateEnabled returns the full parameter key for enabled flag.
func ParamROCrateEnabled() string {
	return fmt.Sprintf("%s-%s-%s", extPrefix, ROCrateFileName, ROCrateEnabled)
}

func paramROCrateOrg() string {
	return fmt.Sprintf("%s-%s-%s", extPrefix, ROCrateFileName, rOCrateParamOrg)
}

func paramROCrateOrgID() string {
	return fmt.Sprintf("%s-%s-%s", extPrefix, ROCrateFileName, rOCrateParamOrgID)
}

func paramROCrateUser() string {
	return fmt.Sprintf("%s-%s-%s", extPrefix, ROCrateFileName, rOCrateParamUser)
}

func paramROCrateAddress() string {
	return fmt.Sprintf("%s-%s-%s", extPrefix, ROCrateFileName, rOCrateParamAddress)
}

func (rc *ROCrateFile) SetParams(params map[string]string) error {
	if params == nil {
		return nil
	}
	if rc.userConfig == nil {
		rc.userConfig = &userConfig{}
	}
	if enabled, ok := params[ParamROCrateEnabled()]; ok {
		if strings.ToLower(enabled) == "true" {
			rc.enabled = true
		} else if strings.ToLower(enabled) == "false" {
			rc.enabled = false
		}
	}
	if org, ok := params[paramROCrateOrg()]; ok {
		rc.userConfig.organisation = org
	}
	if orgID, ok := params[paramROCrateOrgID()]; ok {
		rc.userConfig.organisationID = orgID
	}
	if u, ok := params[paramROCrateUser()]; ok {
		rc.userConfig.user = u
	}
	if addr, ok := params[paramROCrateAddress()]; ok {
		rc.userConfig.address = addr
	}
	return nil
}

func (rc *ROCrateFile) WriteConfig(fsys appendfs.FS) error {
	configWriter, err := writefs.Create(fsys, "config.json")
	if err != nil {
		return errors.Wrap(err, "cannot open config.json")
	}
	defer configWriter.Close()
	if err := json.MarshalWrite(configWriter, rc.ROCrateFileConfig, jsontext.WithIndent("   ")); err != nil {
		return errors.Wrapf(err, "cannot encode config to file")
	}
	return nil
}

func (rc *ROCrateFile) UpdateObjectBefore(object.VersionWriter) error {
	return nil
}

func (rc *ROCrateFile) UpdateObjectAfter(object.VersionWriter) error {
	return nil
}

func (rc *ROCrateFile) findROCrateMeta(stateFiles []string) bool {
	for _, f := range stateFiles {
		fClean := strings.TrimPrefix(filepath.ToSlash(f), "/")
		if fClean == "data/ro-crate-metadata.json" || fClean == "ro-crate-metadata.json" || path.Base(fClean) == "ro-crate-metadata.json" {
			return true
		}
	}
	return false
}

func copyStream(reader io.Reader) (io.Reader, error) {
	buf := &bytes.Buffer{}
	_, err := io.Copy(buf, reader)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func infoJSONExists(obj object.Object, metaFileName string) bool {
	inv := obj.GetInventory()
	if inv == nil {
		return false
	}
	manifest := inv.GetManifest()
	if manifest == nil {
		return false
	}
	for _, names := range manifest.Iterate() {
		for _, name := range names {
			s := strings.Split(name, "/")
			if s[len(s)-1] == metaFileName {
				return true
			}
		}
	}
	return false
}

func (rc *ROCrateFile) writeMetafile(obj object.VersionWriter, rcMeta string) error {
	if !rc.enabled {
		return nil
	}
	if infoJSONExists(obj.GetObject(), rc.MetaName) {
		return fmt.Errorf("%s exists, ensure metafile extension is not configured as on", rc.MetaName)
	}
	data := []byte(rcMeta)
	switch strings.ToLower(rc.StorageType) {
	case "area":
		targetname := strings.TrimLeft(rc.MetaName, "/")
		if _, err := obj.AddReader(io.NopCloser(bytes.NewBuffer(data)), []string{targetname}, rc.StorageName, true, false); err != nil {
			return errors.Wrapf(err, "cannot write '%s'", targetname)
		}
	case "path":
		pathName, err := obj.GetObject().GetExtensionManager().GetAreaPath("content")
		if err != nil {
			return errors.Wrapf(err, "cannot get area path for '%s'", "content")
		}
		targetname := strings.TrimLeft(filepath.ToSlash(filepath.Join(pathName, rc.StorageName, rc.MetaName)), "/")
		if _, err := obj.AddReader(io.NopCloser(bytes.NewBuffer(data)), []string{targetname}, "", true, false); err != nil {
			return errors.Wrapf(err, "cannot write '%s'", targetname)
		}
	case "extension":
		targetname := strings.TrimLeft(filepath.ToSlash(filepath.Join("extensions", rc.GetName(), rc.StorageName, rc.MetaName)), "/")
		if _, err := writefs.WriteFile(obj.GetObject().GetWriteFS(), targetname, data); err != nil {
			return errors.Wrapf(err, "cannot write file '%v/%s'", obj.GetObject().GetWriteFS(), targetname)
		}
	default:
		return errors.Errorf("unsupported storage type '%s'", rc.StorageType)
	}
	return nil
}

func (rc *ROCrateFile) writeConfigValues(rcMeta *rocrate.GocflSummary) {
	if rc.userConfig != nil {
		rcMeta.Organisation = rc.userConfig.organisation
		rcMeta.OrganisationID = rc.userConfig.organisationID
		rcMeta.User = rc.userConfig.user
		rcMeta.Address = rc.userConfig.address
	}
}

func (rc *ROCrateFile) StreamObject(
	obj object.VersionWriter,
	reader io.Reader,
	stateFiles []string,
	dest string,
) error {
	if !rc.enabled {
		return nil
	}
	if !rc.findROCrateMeta(stateFiles) {
		return nil
	}
	inventory := obj.GetObject().GetInventory()
	if inventory == nil {
		return errors.New("no inventory available")
	}
	metaCopy, err := copyStream(reader)
	if err != nil {
		return err
	}
	processed, err := rocrate.ProcessMetadataStream(metaCopy)
	if err != nil {
		return errors.Wrapf(err, "cannot process RO-CRATE metadata")
	}
	rcMeta, err := processed.GOCFLSummary()
	if err != nil {
		return errors.Wrapf(err, "cannot generate GOCFL summary from RO-CRATE metadata")
	}
	rc.writeConfigValues(&rcMeta)
	metaBytes, err := rcMeta.ToJSON()
	if err != nil {
		return errors.Wrap(err, "problem creating GOCFL metadata JSON")
	}
	if err := rc.writeMetafile(obj, string(metaBytes)); err != nil {
		return errors.Wrapf(err, "cannot write metafile")
	}
	head := inventory.GetHead()
	if head.IsValid() {
		rc.info[head.String()] = metaBytes
	}
	return nil
}

func (rc *ROCrateFile) GetMetadata(sourceFS fs.FS, obj object.Object) (map[string]any, error) {
	if !rc.enabled {
		return nil, nil
	}
	var err error
	var result = map[string]any{}
	inv := obj.GetInventory()
	if inv == nil {
		return nil, errors.New("no inventory available")
	}

	var versions []*inventory.VersionNumber
	for v := range inv.GetVersions().GetVersionNumbers() {
		versions = append(versions, v)
	}
	slices.SortFunc(versions, func(a, b *inventory.VersionNumber) int {
		if a.Equal(b) {
			return 0
		}
		if a.Less(b) {
			return 1
		}
		return -1
	})

	var metadata []byte
	var found bool
	for _, ver := range versions {
		var ok bool
		if metadata, ok = rc.info[ver.String()]; ok {
			found = true
			break
		}
		if metadata, err = ext.ReadFile(rc.GetName(), sourceFS, obj, rc.MetaName, ver, rc.StorageType, rc.StorageName); err == nil {
			found = true
			break
		}
	}
	if metadata == nil {
		if found {
			return result, nil
		}
		return nil, errors.Wrapf(err, "cannot read %s", rc.MetaName)
	}
	var metaStruct = map[string]any{}
	if err := json.Unmarshal(metadata, &metaStruct); err != nil {
		return nil, errors.Wrapf(err, "cannot unmarshal '%s'", rc.MetaName)
	}
	result[""] = metaStruct
	return result, nil
}

// Interface assertions
var (
	_ extensiontypes.Extension     = &ROCrateFile{}
	_ object.ExtensionObjectChange = &ROCrateFile{}
	_ object.ExtensionStream       = &ROCrateFile{}
	_ object.ExtensionMetadata     = &ROCrateFile{}
)
