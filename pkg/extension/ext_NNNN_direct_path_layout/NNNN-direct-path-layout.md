# OCFL Community Extension NNNN: Direct Path Layout

* **Extension Name:** `NNNN-direct-path-layout`
* **Authors:** Jürgen Enge (Basel)
* **Minimum OCFL Version:** 1.0
* **OCFL Community Extensions Version:** 1.0
* **Obsoletes:** n/a
* **Obsoleted by:** n/a

## Overview

This storage root extension describes a direct path OCFL storage layout. OCFL object identifiers are mapped directly to directory paths within the OCFL storage root directory without any modifications or transformations.

Unlike flat storage layouts (such as `0002-flat-direct-storage-layout`), this extension permits object identifiers to include forward slashes (`/`), thereby creating hierarchical multi-level directory structures within the storage root.

### Usage Scenario

This layout is intended for repositories where object identifiers naturally conform to a filesystem-friendly hierarchy (e.g., `collection/subcollection/item-001` or `department/archive/record-42`) and where preserving this direct hierarchical structure in the underlying storage root is desired without requiring hashing or tuple subdivision.

## Parameters

This extension has no parameters.

## Procedure

The OCFL object identifier is used, without any changes, as the object's relative root path within the OCFL storage root.

## Caveats

### Object Containment / Identifier Prefixes
When using this extension, no object identifier may be a path prefix of another object identifier. For example, having both `records/finance` and `records/finance/2023` is invalid because the root directory of the second object would be located inside the root directory of the first object, which violates OCFL specification requirements.

### Filesystem Restrictions & Dangerous Characters
- Identifiers must not begin with a leading slash (`/`) or contain relative traversal segments such as `.` or `..`.
- Object identifiers must only contain characters and path segment lengths that are valid on the underlying filesystem (e.g., avoiding reserved characters like `:`, `\`, `*`, `?`, `"`, `<`, `>`, `|` on Windows systems, and observing maximum segment / filename length limits).

## Examples

### Example 1: Valid Hierarchical Mappings

#### Mappings

| Object ID | Object Root Path |
| --- | --- |
| `object-01` | `object-01` |
| `coll-a/sub-b/item-01` | `coll-a/sub-b/item-01` |
| `dept-archives/2024/doc-1234` | `dept-archives/2024/doc-1234` |

#### Storage Hierarchy

```
[storage_root]/
├── 0=ocfl_1.0
├── ocfl_layout.json
├── coll-a/
│   └── sub-b/
│       └── item-01/
│           ├── 0=ocfl_object_1.0
│           ├── inventory.json
│           ├── inventory.json.sha512
│           └── v1/
├── dept-archives/
│   └── 2024/
│       └── doc-1234/
│           ├── 0=ocfl_object_1.0
│           ├── inventory.json
│           ├── inventory.json.sha512
│           └── v1/
└── object-01/
    ├── 0=ocfl_object_1.0
    ├── inventory.json
    ├── inventory.json.sha512
    └── v1/
```

### Example 2: Invalid Mappings

#### Mappings

| Object ID | Issue / Reason |
| --- | --- |
| `collection/item` and `collection/item/sub` | Nested object collision (one object root is inside another) |
| `../escape-root` | Path traversal attempting to escape the storage root |
| `/absolute/path/object` | Leading slash creating absolute path rather than relative path |
