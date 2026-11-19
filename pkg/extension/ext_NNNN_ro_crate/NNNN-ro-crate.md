# OCFL Community Extension NNNN: RO-Crate

* **Extension Name:** NNNN-ro-crate
* **Authors:** Ross Spencer, Jürgen Enge
* **Minimum OCFL Version:** 1.0
* **OCFL Community Extensions Version:** 1.0
* **Obsoletes:** n/a
* **Obsoleted by:** n/a

## Overview

This object extension processes RO-Crate metadata (`ro-crate-metadata.json`) present in an OCFL object's content during ingest or streaming, extracts semantic metadata from the root entity, and generates a GOCFL-compatible metadata summary file (by default `info.json`).

### Usage Scenario

When storing research data packages that follow the [RO-Crate specification](https://www.researchobject.org/ro-crate/), this extension extracts key descriptive metadata (such as title, description, keywords, license, authors, and publication date) from `ro-crate-metadata.json` and writes a summary metadata file (`info.json`) that can be consumed by external repository viewers or archive management tools.

## Parameters

### Summary

* **Name:** `storageType`
    * **Description:** Location type where the generated metadata file is stored. Possible values are `area`, `path`, or `extension`.
        * **area:** within an `area` defined by [NNNN-content-subpath](NNNN-content-subpath.md)
        * **path:** directly within content folder
        * **extension:** within the extension subfolder
    * **Type:** string
    * **Default:** `extension`

* **Name:** `storageName`
    * **Description:** Subdirectory or area name within the specified storage type
        * **area:** area name
        * **path:** subfolder within content folder
        * **extension:** subfolder within extension folder
    * **Type:** string
    * **Default:** `""`

* **Name:** `metaFileName`
    * **Description:** Name of the generated metadata file.
    * **Type:** string
    * **Default:** `info.json`

## External Parameters

The following parameters can be supplied via configuration or CLI flags:

* `ext-NNNN-ro-crate-enabled`: enable or disable the RO-Crate metadata processing (default: `false`).
* `ext-NNNN-ro-crate-organisation`: institutional organisation name.
* `ext-NNNN-ro-crate-organisationID`: institutional organisation identifier.
* `ext-NNNN-ro-crate-user`: user name.
* `ext-NNNN-ro-crate-address`: user address.

## Examples

### Configuration

```json
{
  "extensionName": "NNNN-ro-crate",
  "storageType": "extension",
  "storageName": "",
  "metaFileName": "info.json"
}
```
