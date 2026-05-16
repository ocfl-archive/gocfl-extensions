# NNNN-filesystem

This OCFL extension provides support for filesystem interactions.

Further information can be found in [NNNN-filesystem.md](NNNN-filesystem.md).

## Usage

### Activation

Extensions must be explicitly activated by importing them for their side effects (using the blank identifier `_`). This registers the extension within the `gocfl` library.

```go
import _ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_filesystem"
```
