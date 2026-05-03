package ext_0011_direct_clean_path_layout

import (
	"emperror.dev/errors"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfl/extension"
)

// fallback for object with unregigered naming

const LegacyDirectCleanName = "NNNN-direct-clean-path-layout"
const LegacyDirectCleanDescription = "Maps OCFL object identifiers to storage paths or as an object extension that maps logical paths to content paths. This is done by replacing or removing \"dangerous characters\" from names"

func init() {
	extension.RegisterExtension(LegacyDirectCleanName, NewLegacyDirectClean, nil, &DirectCleanDoc)
}

func NewLegacyDirectClean() (extension.Extension, error) {
	sl, err := NewDirectClean()
	if err == nil {
		return &LegacyDirectClean{sl.(*DirectClean)}, nil
	}
	return nil, errors.WithStack(err)
}

type LegacyDirectClean struct {
	*DirectClean
}

func (sl *LegacyDirectClean) IsRegistered() bool {
	return false
}
func (sl *LegacyDirectClean) GetName() string { return LegacyDirectCleanName }
func (sl *LegacyDirectClean) GetConfig() any {
	return sl.DirectCleanConfig
}
