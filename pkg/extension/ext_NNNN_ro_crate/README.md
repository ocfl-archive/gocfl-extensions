# NNNN-ro-crate

This OCFL extension provides support for extracting and mapping metadata from RO-Crate packages.

Further information can be found in [NNNN-ro-crate.md](NNNN-ro-crate.md).

## Usage

### Activation

Extensions must be explicitly activated by importing them for their side effects (using the blank identifier `_`). This registers the extension within the `gocfl` library.

```go
import _ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_ro_crate"
```

### Manual Initialization

For active use, this extension can be manually initialized with a filesystem and a logger:

```go
ext_NNNN_ro_crate.Init(fSys, logger)
```
