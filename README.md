# gocfl-extensions

This repository contains various implementations of OCFL (Oxford Common File Layout) extensions for the [gocfl](https://github.com/ocfl-archive/gocfl) Go library.

## Overview

The extensions in this project are modular and provide additional functionalities for OCFL objects and storage roots, such as:

- Special storage layouts
- Support for additional digest algorithms
- Metadata generation (METS, PREMIS, EAD3)
- Automated workflows like thumbnail creation or format migration

## Project Structure

- `pkg/extension`: The main package containing the infrastructure for extensions and individual implementations. A detailed overview of all available extensions can be found in its [README.md](pkg/extension/README.md).
- `test`: Contains tools and configurations for testing. A description of the test infrastructure can be found in its [README.md](test/README.md).

## Usage

To use this module in your Go project:

```bash
go get github.com/ocfl-archive/gocfl-extensions
```

### Activating Extensions

Extensions must be explicitly activated by importing them for their side effects (using the blank identifier `_`). This registers the extension within the `gocfl` library.

**Note:** For simple extensions (like digest algorithms or storage layouts) and for using complex extensions in **read-only** mode (e.g. extraction, validation), the side-effect import is usually sufficient. However, for **active** use (e.g. `init`, `add`, `update`), complex extensions require manual registration and initialization.

To activate a specific extension (e.g., the 0009 Digest Algorithms extension):

```go
import _ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_0009_digest_algorithms"
```

To activate **all** available extensions from this repository, you can import them like this:

```go
import (
	_ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_0001_digest_algorithms"
	_ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_0002_flat_direct_storage_layout"
	_ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_0003_hash_and_id_n_tuple_storage_layout"
	_ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_0004_hashed_n_tuple_storage_layout"
	_ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_0006_flat_omit_prefix_storage_layout"
	_ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_0007_n_tuple_omit_prefix_storage_layout"
	_ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_0009_digest_algorithms"
	_ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_0010_differential_n_tuple_omit_prefix_storage_layout"
	_ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_0011_direct_clean_path_layout"
	_ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_0012_hash_and_no_prefix_id_n_tuple_storage_layout"
	_ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_content_subpath"
	_ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_filesystem"
	_ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_indexer"
	_ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_metafile"
	_ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_mets"
	_ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_migration"
	_ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_pairtree_storage_layout"
	_ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_thumbnail"
	_ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_timestamp"
)
```

#### Initialization of Complex Extensions

For **active** use, complex extensions must be manually initialized. The following examples show how this is done:

##### Indexer Extension
```go
// Initialize the indexer extension with service address, configuration, local cache and logger
ext_NNNN_indexer.Init(addr, conf.Indexer, localCache, logger)
```

##### Migration Extension
```go
// Initialize the migration extension with configuration and source file system
ext_NNNN_migration.Init(&conf.Migration, sourceFS, logger)
```

##### Thumbnail Extension
```go
// Initialize the thumbnail extension with configuration and source file system
ext_NNNN_thumbnail.Init(conf.Thumbnail, sourceFS, logger)
```

## Documentation

Each extension is documented. A list of all registered and unregistered extensions and references to their specific documentation can be found at:

👉 [**Detailed Overview of Extensions**](pkg/extension/README.md)

## License

This project is licensed under the [Apache License, Version 2.0](LICENSE).
