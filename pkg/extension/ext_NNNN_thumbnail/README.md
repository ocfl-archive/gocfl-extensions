# NNNN-thumbnail

This OCFL extension provides support for the automatic generation of preview images (thumbnails).

Further information can be found in [NNNN-thumbnail.md](NNNN-thumbnail.md).

## Usage

### Activation

Extensions must be explicitly activated by importing them for their side effects (using the blank identifier `_`). This registers the extension within the `gocfl` library.

```go
import _ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_thumbnail"
```

### Manual Initialization

For active use, this extension must be manually initialized.

```go
// Initialize the thumbnail extension with configuration and source file system
ext_NNNN_thumbnail.Init(conf.Thumbnail, sourceFS, logger)
```
