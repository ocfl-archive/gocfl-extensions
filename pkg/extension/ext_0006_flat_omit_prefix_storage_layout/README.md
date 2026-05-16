# 0006-flat-omit-prefix-storage-layout

This OCFL extension provides support for a flat omit prefix storage layout.

Further information can be found in [0006-flat-omit-prefix-storage-layout.md](0006-flat-omit-prefix-storage-layout.md).

## Usage

### Activation

Extensions must be explicitly activated by importing them for their side effects (using the blank identifier `_`). This registers the extension within the `gocfl` library.

```go
import _ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_0006_flat_omit_prefix_storage_layout"
```
