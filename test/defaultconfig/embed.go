package defaultconfig

import (
	"embed"
)

//go:embed object/*/*  storageroot/*/*
var DefaultConfig embed.FS
