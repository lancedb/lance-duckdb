# Lance DuckDB Extension

A DuckDB extension that enables native querying of Lance format datasets, including support for cloud storage (S3, Azure, GCS).

## Features

- **Native Lance Support**: Query `.lance` files directly using SQL
- **Cloud Storage**: Seamlessly query Lance datasets on S3, Azure Blob Storage, and Google Cloud Storage
- **Table Function**: Use `lance_scan()` to read Lance datasets
- **Arrow Integration**: Automatic Arrow to DuckDB type conversion
- **Performance**: Optimized for analytical workloads with Lance's columnar format

## Building

### Prerequisites

- Rust toolchain
- DuckDB development files
- Make

### Build Steps

```bash
# Initialize submodules
git submodule update --init --recursive

# Configure the build
make configure

# Build the extension
make release
```

The built extension will be available at `build/release/lance.duckdb_extension`.

## Installation

```sql
-- Load the extension (with -unsigned flag for local builds)
LOAD 'path/to/lance.duckdb_extension';
```

## Usage

### Basic Query

```sql
-- Query a local Lance file
SELECT * FROM lance_scan('path/to/dataset.lance');

-- Query from S3
SELECT * FROM lance_scan('s3://bucket/path/dataset.lance');

-- Aggregations
SELECT COUNT(*), AVG(value) 
FROM lance_scan('data.lance') 
WHERE category = 'A';
```

### With Filters and Projections

```sql
-- Select specific columns
SELECT id, name, timestamp 
FROM lance_scan('dataset.lance')
WHERE timestamp >= '2024-01-01'
ORDER BY timestamp DESC
LIMIT 100;
```

## Development

### Project Structure

```
├── src/
│   ├── lib.rs           # Extension entry point
│   ├── lance_scan.rs    # Lance scan table function
│   ├── replacement_scan.rs # Replacement scan (future)
│   └── types.rs         # Arrow to DuckDB type mapping
├── test/
│   ├── sql/            # SQL test files
│   └── data/           # Test data
└── examples/           # Usage examples
```

### Running Tests

```bash
# Run all tests (builds release and runs SQL tests)
make test

# Run debug tests
make test_debug

# Run quick smoke test
make test_quick

# Run Rust tests
cargo test
```

### Running Clippy

```bash
cargo clippy --all-targets --all-features
```

## Roadmap

- [x] Basic `lance_scan` table function
- [x] Arrow to DuckDB type mapping
- [x] MVP with demo data
- [ ] Full Lance dataset reading
- [ ] Predicate pushdown
- [ ] Projection pushdown
- [ ] Replacement scan for `.lance` files
- [ ] Write support (COPY TO)
- [ ] Vector index support

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

Apache 2.0

## Acknowledgments

Built on top of:
- [Lance](https://github.com/lancedb/lance) - Modern columnar data format
- [DuckDB](https://duckdb.org) - In-process analytical database
- [Apache Arrow](https://arrow.apache.org) - Columnar memory format