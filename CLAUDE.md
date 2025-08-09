# Lance DuckDB Extension Development Notes

## Project Overview

This project implements a DuckDB extension that enables native querying of Lance format datasets directly from SQL. Lance is a modern columnar data format optimized for ML/AI workloads with native cloud storage support.

## Key Development Decisions

### 1. Architecture Approach

**Decision**: Use Lance library directly instead of reimplementing the format reader.

**Rationale**:
- Lance already handles S3/cloud storage natively
- Avoids reimplementing complex format details
- Focuses effort on DuckDB integration layer
- Ensures compatibility with Lance ecosystem

### 2. Extension Naming Strategy

**Decision**: Use different names for extension vs Rust package
- Extension name: `lance` (what users see in DuckDB)
- Rust package name: `lance_duckdb` (avoids crate name conflict)

**Implementation**:
```makefile
EXTENSION_NAME=lance
RUST_CRATE_NAME=lance_duckdb
```

**Rationale**: Provides clean user experience while avoiding naming conflicts in Rust ecosystem.

## Testing Strategy

### Test Data Setup
- Created small test dataset: `test/test_data.lance`
- 5 records with id, name, age, score columns
- Covers basic data types: Int64, Utf8, Float64

### Test Coverage
1. Extension loading verification
2. Basic data reading
3. Projections and filters
4. Aggregations (COUNT, AVG, MIN, MAX, SUM)
5. Sorting and limits
6. Combined predicates

### Test Framework
- Using DuckDB's sqllogictest format
- All tests in `test/sql/` directory
- Run with `make test`

## Dependencies Version Locking

**Critical**: Arrow version must match between Lance and DuckDB bindings.
- Lance 0.32.1 requires Arrow 55.1
- Must use exact versions, no ranges

## Build Configuration

### Key Makefile Variables
- `USE_UNSTABLE_C_API=1`: Required for duckdb-rs
- `TARGET_DUCKDB_VERSION=v1.3.2`: Tested DuckDB version

### Entry Point Naming
- Macro generates: `lance_init_c_api` (not `lance_duckdb_init_c_api`)
- Controlled by `ext_name = "lance"` attribute

## Lessons Learned

1. **Start Simple**: Mock data → Real Lance reading was correct progression
2. **Type Systems Matter**: Arrow/DuckDB type mapping is complex, string conversion works for MVP
3. **API Limitations**: Some features blocked by binding limitations, document and move on
4. **Test Early**: sqllogictest format excellent for SQL extension testing
5. **Version Alignment**: Dependency version conflicts are painful, lock versions early

## Development Commands

```bash
# Build
make clean && make release

# Test
make test

# Quick iteration
cargo check
cargo clippy --all-targets --all-features

# Create test data
cargo run --example create_test_data
```

## References

- [Lance Format](https://github.com/lancedb/lance)
- [DuckDB Extension API](https://duckdb.org/docs/api/c/extension)
- [Arrow Columnar Format](https://arrow.apache.org/docs/format/Columnar.html)
- [duckdb-rs](https://github.com/duckdb/duckdb-rs)
