# NNNN-mets

This OCFL extension provides support for the generation of METS and PREMIS metadata.

Further information can be found in [NNNN-mets.md](NNNN-mets.md).

## Usage

### Activation

Extensions must be explicitly activated by importing them for their side effects (using the blank identifier `_`). This registers the extension within the `gocfl` library.

```go
import _ "github.com/ocfl-archive/gocfl-extensions/pkg/extension/ext_NNNN_mets"
```
