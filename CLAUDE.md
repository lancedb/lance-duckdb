# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a DuckDB extension written in Rust that enables native SQL querying of Lance format datasets. Lance is a modern columnar data format optimized for ML/AI workloads with native cloud storage support.

## Essential Commands

### Building
```bash
# Initial setup (only needed once)
git submodule update --init --recursive
make configure

# Build commands
make release         # Production build → build/release/lance.duckdb_extension
make debug          # Debug build → build/debug/lance.duckdb_extension
make clean          # Clean build artifacts
make clean_all      # Clean everything including configure

# Quick Rust checks (without full build)
cargo check
cargo clippy --all-targets --all-features
```

### Testing
```bash
# Run all tests (builds release and runs sqllogictest)
make test

# Run with specific build
make test_debug     # Test with debug build
make test_release   # Test with release build

# Run DuckDB with extension for manual testing
duckdb -unsigned -c "LOAD 'build/release/lance.duckdb_extension'; SELECT * FROM lance_scan('test/test_data.lance');"
```

### Development Iteration
```bash
# Fast iteration cycle
cargo build --release && make test_release

# Check for issues without full build
cargo clippy --all-targets --all-features
```

## Architecture & Key Design Decisions

### Extension Architecture

The extension follows a three-layer architecture:

1. **Entry Layer** (`src/lib.rs`)
   - Defines `lance_init_c_api` entry point using `duckdb_entrypoint_c_api` macro
   - Registers table functions with DuckDB

2. **Table Function Layer** (`src/lance_scan.rs`)
   - Implements `LanceScanVTab` using DuckDB's VTab trait
   - `bind()`: Opens Lance dataset, extracts schema, registers output columns
   - `init()`: Loads all data batches into memory (current implementation)
   - `func()`: Serves data from memory to DuckDB

3. **Type Mapping Layer** (`src/types.rs`)
   - Maps Arrow types to DuckDB logical types
   - Currently simplified: converts most types to strings for MVP

### Critical Implementation Details

#### Naming Strategy
The extension uses different names to avoid conflicts:
- **Extension name**: `lance` (what users see)
- **Rust crate name**: `lance_duckdb` (avoids crate conflict)
- **Entry point**: `lance_init_c_api` (generated from extension name)

This is controlled in `Makefile`:
```makefile
EXTENSION_NAME=lance
RUST_CRATE_NAME=lance_duckdb
```

#### Async Bridge Pattern
Lance uses async APIs while DuckDB extensions are synchronous:
```rust
// Create runtime in init
let runtime = Arc::new(Runtime::new()?);

// Block on async operations
let dataset = runtime.block_on(async {
    Dataset::open(&path).await
})?;
```

#### Current Data Loading Strategy
**Important**: Currently loads ALL data into memory during `init()`:
```rust
// In LanceScanInitData
batches: Arc<Mutex<Vec<RecordBatch>>>,  // All data loaded here
```

This works for small-medium datasets but needs streaming for production.

### Dependency Version Constraints

**Critical**: Arrow versions MUST match exactly between Lance and DuckDB:
- Lance 0.32.1 → Arrow 55.1
- No version ranges allowed (use exact versions)

### Known Limitations

1. **Replacement Scan**: Not implemented due to `duckdb-rs` API limitations
   - Users must use `lance_scan('file.lance')` instead of `FROM 'file.lance'`
   - Requires access to raw database handle not exposed by duckdb-rs

2. **Type Conversion**: Currently simplified to strings
   - Production needs direct Arrow→DuckDB memory mapping

3. **Memory Usage**: Loads entire dataset into memory
   - Needs streaming implementation for large datasets

## Test Data & Testing

### Test Dataset
Location: `test/test_data.lance`
- 5 records: id (1-5), name (Alice-Eve), age (25-45), score (78.5-95.5)
- Created by: `cargo run --example create_test_data`

### Test Format
Uses DuckDB's sqllogictest format in `test/sql/`:
- `statement ok/error`: Test statement execution
- `query <types>`: Test query with expected results (I=int, T=text, R=real)
- `require lance`: Load the extension

## Common Issues & Solutions

### Build Failures
1. **Cargo hangs**: Kill with `pkill -9 cargo rustc`, then `make clean`
2. **Version mismatch**: Check `TARGET_DUCKDB_VERSION=v1.3.2` in Makefile
3. **Missing symbols**: Ensure `USE_UNSTABLE_C_API=1` is set

### Extension Loading
```sql
-- Always use -unsigned flag for local builds
duckdb -unsigned
LOAD 'build/release/lance.duckdb_extension';
```

### Type Errors
Current implementation converts to strings. If seeing type mismatches, check:
1. Arrow schema extraction in `bind()`
2. Type mapping in `types.rs`
3. Data conversion in `func()`

## Future Improvements Priority

1. **High Priority**
   - Streaming reads (replace Vec<RecordBatch> with iterator)
   - Proper Arrow→DuckDB type mapping
   - Predicate pushdown to Lance

2. **Medium Priority**
   - Replacement scan when API available
   - Projection pushdown
   - Better error messages

3. **Low Priority**
   - Write support (COPY TO)
   - Vector index integration
   - Statistics for query optimization