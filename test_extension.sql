-- Test the Lance DuckDB extension

-- Load the extension
LOAD 'build/release/lance.duckdb_extension';

-- Test 1: Hello function (should work)
SELECT * FROM hello_lance('Test');

-- Test 2: Lance scan function with demo data
SELECT * FROM lance_scan('s3://test-bucket/data.lance');

-- Test 3: Count rows
SELECT COUNT(*) FROM lance_scan('test/data/sample.lance');

-- Test 4: Select specific columns
SELECT id, name FROM lance_scan('test/data/sample.lance') LIMIT 3;
