# 0012-hash-and-no-prefix-id-n-tuple-storage-layout

This OCFL extension provides support for a hash and no prefix ID n-tuple storage layout.

Further information can be found in [0012-hash-and-no-prefix-id-n-tuple-storage-layout.md](0012-hash-and-no-prefix-id-n-tuple-storage-layout.md).

## Usage

### Activation

Extensions must be explicitly activated by importing them for their side effects (using the blank identifier `_`). This registers the extension within the `gocfl` library.

```go
import _ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_0012_hash_and_no_prefix_id_n_tuple_storage_layout"
```
