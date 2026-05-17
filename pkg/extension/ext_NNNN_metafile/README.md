# NNNN-metafile

This OCFL extension provides support for managing metadata files.

Further information can be found in [NNNN-metafile.md](NNNN-metafile.md).

## Usage

### Activation

Extensions must be explicitly activated by importing them for their side effects (using the blank identifier `_`). This registers the extension within the `gocfl` library.

```go
import _ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_metafile"
```

### Manual Initialization

For active use, this extension must be manually initialized.

```go
// Initialize the metafile extension with a filesystem and a logger
ext_NNNN_metafile.Init(fSys, logger)
```
