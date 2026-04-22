package thumbnaildata

import (
	"embed"
	_ "embed"
)

//go:embed thumbnail.toml
var ThumbnailConfigString string

//go:embed scripts/*
var ThumbnailFS embed.FS
