# NNNN-indexer

This OCFL extension provides support for indexing content.

Further information can be found in [NNNN-indexer.md](NNNN-indexer.md).

## Usage

### Activation

Extensions must be explicitly activated by importing them for their side effects (using the blank identifier `_`). This registers the extension within the `gocfl` library.

```go
import _ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_indexer"
```

### Manual Initialization

For active use, this extension must be manually initialized.

```go
// Initialize the indexer extension with service address, configuration, local cache and logger
ext_NNNN_indexer.Init(addr, conf.Indexer, localCache, logger)
```
