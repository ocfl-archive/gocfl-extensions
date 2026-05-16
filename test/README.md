# Test Infrastructure

This directory contains tools and configurations for performing tests within the `gocfl-extensions` project.

## Content

- `helper.go`: Contains functions for setting up test environments, including in-memory file systems (VFS) and OCFL storage roots.
    - `SetupTestEnv`: Initializes a simple test environment with a logger and VFS.
    - `SetupFullTestEnv`: Creates a complete OCFL test environment including configurable extension managers for storage roots and objects.
    - `CreateTestObject`: Helper function to create a new OCFL object within the test environment.
    - `ReloadObject`: Helper function to load an existing OCFL object for testing purposes.
- `defaultconfig/`: Contains default configuration files (JSON) for initializing OCFL structures in tests.
    - `object/`: Default configurations for OCFL objects.
    - `storageroot/`: Default configurations for OCFL storage roots.
    - `embed.go`: Provides the configurations via `go:embed` for use in Go tests.

## Usage in Tests

The helper functions in `helper.go` are designed to simplify the creation of integration tests that verify the interaction of various OCFL extensions.

Example of initializing a test environment:

```go
func TestMyExtension(t *testing.T) {
    env := test.SetupFullTestEnv(t, nil, nil, nil, nil)
    // ... test logic ...
}
```
