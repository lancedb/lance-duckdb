# Development Guide

## Architecture

This extension bridges DuckDB and Lance format, allowing SQL queries on Lance datasets.

### Key Components

- **lib.rs**: Extension entry point, registers all functions
- **lance_scan.rs**: Implements the `lance_scan` table function
- **types.rs**: Arrow to DuckDB type conversions
- **replacement_scan.rs**: Future implementation for automatic `.lance` file detection

## Extension vs Package Naming

- **Extension Name**: `lance` (what users see in DuckDB)
- **Rust Package Name**: `lance_duckdb` (avoids conflict with lance crate)

This is configured in:
- `Makefile`: Sets `EXTENSION_NAME=lance` and `RUST_CRATE_NAME=lance_duckdb`
- `Cargo.toml`: Package name is `lance_duckdb`

## Building

```bash
# One-time setup
git submodule update --init --recursive
make configure

# Build
make release         # Release build
make debug          # Debug build
make clean          # Clean build artifacts
```

## Testing

The project uses DuckDB's sqllogictest framework for testing. Test files are located in `test/sql/` and use the `.test` extension.

```bash
# Run all tests (builds and tests)
make test

# Run tests with debug build
make test_debug

# Run tests with release build
make test_release
```

### Test File Format

Tests use the sqllogictest format:
- `statement ok` - Statement should execute without error
- `statement error` - Statement should fail with specified error
- `query <types>` - Query with expected results
- `require <extension>` - Load an extension

See `test/sql/lance.test` for examples.

## Current Implementation Status

### Working
- ✅ Basic table function `lance_scan(path)`
- ✅ Arrow to DuckDB type mapping
- ✅ Actual Lance dataset reading
- ✅ Extension loading and registration
- ✅ Basic data type conversion (Int64, Float64, Utf8)

### TODO
- [ ] S3/cloud storage support (Lance supports it natively)
- [ ] Predicate pushdown
- [ ] Projection pushdown
- [ ] Replacement scan for automatic `.lance` file handling (requires duckdb-rs API enhancement)
- [ ] Proper Arrow data copying for all types (currently simplified conversion)
- [ ] Streaming reads instead of loading all data at once

## Code Style

- Run `cargo clippy --all-targets --all-features` before committing
- Follow Rust idioms and conventions
- Add safety documentation for unsafe functions

## Debugging

If the extension fails to load:
1. Check you're using `-unsigned` flag with duckdb
2. Verify the extension was built: `ls build/release/lance.duckdb_extension`
3. Check for version compatibility with DuckDB

## Contributing

1. Keep the MVP approach - get basic functionality working first
2. Add tests for new features
3. Update documentation when adding new functionality
4. Ensure clippy passes without warnings
