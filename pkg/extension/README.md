# OCFL Extensions

This package contains various extensions for the GOCFL loading and management process.

A distinction is made between registered extensions, which are listed in the [OCFL Extensions Repository](https://ocfl.github.io/extensions/), and unregistered (local or experimental) extensions.

## Overview of Extensions

Each extension is located in its own subdirectory and implements specific functionalities such as storage layouts, digest algorithms, or metadata generation.

### Registered Extensions

These extensions follow the official OCFL numbering and are documented at [https://ocfl.github.io/extensions/](https://ocfl.github.io/extensions/).

- [0001-digest-algorithms](ext_0001_digest_algorithms/README.md)
- [0002-flat-direct-storage-layout](ext_0002_flat_direct_storage_layout/README.md)
- [0003-hash-and-id-n-tuple-storage-layout](ext_0003_hash_and_id_n_tuple_storage_layout/README.md)
- [0004-hashed-n-tuple-storage-layout](ext_0004_hashed_n_tuple_storage_layout/README.md)
- [0006-flat-omit-prefix-storage-layout](ext_0006_flat_omit_prefix_storage_layout/README.md)
- [0007-n-tuple-omit-prefix-storage-layout](ext_0007_n_tuple_omit_prefix_storage_layout/README.md)
- [0009-digest-algorithms](ext_0009_digest_algorithms/README.md)
- [0010-differential-n-tuple-omit-prefix-storage-layout](ext_0010_differential_n_tuple_omit_prefix_storage_layout/README.md)
- [0011-direct-clean-path-layout](ext_0011_direct_clean_path_layout/README.md)
- [0012-hash-and-no-prefix-id-n-tuple-storage-layout](ext_0012_hash_and_no_prefix_id_n_tuple_storage_layout/README.md)

### Unregistered Extensions

These extensions are not (yet) officially registered and use the placeholder `NNNN` in their designation.

- [NNNN-content-subpath](ext_NNNN_content_subpath/README.md)
- [NNNN-filesystem](ext_NNNN_filesystem/README.md)
- [NNNN-indexer](ext_NNNN_indexer/README.md)
- [NNNN-metafile](ext_NNNN_metafile/README.md)
- [NNNN-mets](ext_NNNN_mets/README.md)
- [NNNN-migration](ext_NNNN_migration/README.md)
- [NNNN-pairtree-storage-layout](ext_NNNN_pairtree_storage_layout/README.md)
- [NNNN-thumbnail](ext_NNNN_thumbnail/README.md)
- [NNNN-timestamp](ext_NNNN_timestamp/README.md)

## Initialization of Complex Extensions

Some extensions are complex in nature and require additional configuration data (e.g., for METS generation, image conversion, or metadata extraction). 

### Registration vs. Initialization

1.  **Simple Extensions & Read-Only Mode:**
    For simple extensions (like storage layouts or digest algorithms) and for complex extensions used only in **read-only** mode (e.g., when validating or extracting an existing OCFL object), a side-effect import is sufficient for registration:
    ```go
    import _ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_indexer"
    ```

2.  **Active Usage (init, add, update):**
    For operations that modify or create OCFL structures (like `init`, `add`, or `update`), complex extensions must be manually initialized and configured before they can be used. This ensures they have access to required resources like loggers, file systems, or specific service addresses.

This is usually done by calling an `Init` function of the extension, which sets the necessary runtime data and registers it in the global OCFL extension manager.

### Examples: Initializing Complex Extensions

The following examples show how complex extensions are initialized (as seen in the `gocfl-cli` project):

#### Indexer Extension
The indexer extension extracts metadata from files and stores it in a search index.
```go
// Initialize the indexer extension with service address, configuration, local cache and logger
ext_NNNN_indexer.Init(addr, conf.Indexer, localCache, logger)
```

#### Migration Extension
The migration extension handles format migrations during the ingest process.
```go
// Initialize the migration extension with configuration and source file system
ext_NNNN_migration.Init(&conf.Migration, sourceFS, logger)
```

#### Thumbnail Extension
The thumbnail extension automatically generates preview images for supported file types.
```go
// Initialize the thumbnail extension with configuration and source file system
ext_NNNN_thumbnail.Init(conf.Thumbnail, sourceFS, logger)
```

Through these `Init` calls, the extensions register themselves with the necessary runtime configuration in the global OCFL extension manager, so that they can be correctly called during the ingest process.
