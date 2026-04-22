package thumbnaildata

import (
	"embed"
	_ "embed"
)

//go:embed thumbnail.toml
var ThumbnailConfigString string

//go:embed scripts/pdf2thumb.ps1 scripts/pdf2thumb.sh scripts/video2thumb.ps1 scripts/video2thumb.sh
var ThumbnailFS embed.FS
