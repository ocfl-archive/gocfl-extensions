# 0002-flat-direct-storage-layout

This OCFL extension provides support for a flat direct storage layout.

Further information can be found in [0002-flat-direct-storage-layout.md](0002-flat-direct-storage-layout.md).

## Usage

### Activation

Extensions must be explicitly activated by importing them for their side effects (using the blank identifier `_`). This registers the extension within the `gocfl` library.

```go
import _ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_0002_flat_direct_storage_layout"
```
