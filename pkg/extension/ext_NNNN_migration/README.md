# NNNN-migration

This OCFL extension provides support for migrating file formats.

Further information can be found in [NNNN-migration.md](NNNN-migration.md).

## Usage

### Activation

Extensions must be explicitly activated by importing them for their side effects (using the blank identifier `_`). This registers the extension within the `gocfl` library.

```go
import _ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_migration"
```

### Manual Initialization

For active use, this extension must be manually initialized.

```go
// Initialize the migration extension with configuration and source file system
ext_NNNN_migration.Init(&conf.Migration, sourceFS, logger)
```
