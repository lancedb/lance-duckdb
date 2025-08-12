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
GEN=ninja make release         # Production build → build/release/lance.duckdb_extension
GEN=ninja make debug          # Debug build → build/debug/lance.duckdb_extension
GEN=ninja make clean          # Clean build artifacts
GEN=ninja make clean_all      # Clean everything including configure

# Quick Rust checks (without full build)
cargo check
cargo clippy --all-targets --all-features
```

### Testing

release build can be slow, use `test_debug` for quick test.

```bash
# Run all tests (builds release and runs sqllogictest)
GEN=ninja make test

# Run with specific build
GEN=ninja make test_debug     # Test with debug build
GEN=ninja make test_release   # Test with release build

# Run DuckDB with extension for manual testing
duckdb -unsigned -c "LOAD 'build/release/lance.duckdb_extension'; SELECT * FROM lance_scan('test/test_data.lance');"
```

### Development Iteration
```bash
# Fast iteration cycle
cargo build --release && make test_debug

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

## Test Data & Testing

### Test Dataset
Location: `test/test_data.lance`

### Test Format
Uses DuckDB's sqllogictest format in `test/sql/`:
- `statement ok/error`: Test statement execution
- `query <types>`: Test query with expected results (I=int, T=text, R=real)
- `require lance`: Load the extension

## Common Issues & Solutions

### Extension Loading
```sql
-- Always use -unsigned flag for local builds
duckdb -unsigned
LOAD 'build/release/lance.duckdb_extension';
```
