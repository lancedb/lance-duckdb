-- Example usage of lance-duckdb extension

-- Load the extension
LOAD 'lance_duckdb';

-- Or use INSTALL/LOAD pattern
-- INSTALL lance_duckdb;
-- LOAD lance_duckdb;

-- 1. Basic usage with lance_scan function
SELECT * FROM lance_scan('test/data/sample.lance') LIMIT 10;

-- 2. Using replacement scan with .lance files
-- This automatically uses lance_scan internally
SELECT * FROM 'test/data/sample.lance' LIMIT 10;

-- 3. Query S3 hosted Lance datasets
-- Lance handles S3 authentication and access internally
SELECT count(*) FROM 's3://bucket/path/dataset.lance';

-- 4. Column projection - only specified columns are read
SELECT id, value, category 
FROM 'test/data/sample.lance' 
WHERE value > 0.5 
LIMIT 100;

-- 5. Aggregations
SELECT 
    category, 
    COUNT(*) as count,
    AVG(value) as avg_value,
    MAX(value) as max_value
FROM 'test/data/sample.lance'
GROUP BY category;

-- 6. Joins with regular DuckDB tables
CREATE TABLE categories AS 
SELECT 'A' as category, 'Alpha' as name UNION ALL
SELECT 'B', 'Beta' UNION ALL
SELECT 'C', 'Charlie' UNION ALL
SELECT 'D', 'Delta';

SELECT 
    l.id,
    l.value,
    c.name as category_name
FROM 'test/data/sample.lance' l
JOIN categories c ON l.category = c.category
LIMIT 10;

-- 7. Export Lance data to Parquet
COPY (SELECT * FROM 'test/data/sample.lance') 
TO 'output.parquet' (FORMAT PARQUET);

-- 8. Complex queries with window functions
SELECT 
    id,
    value,
    category,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY value DESC) as rank
FROM 'test/data/sample.lance'
WHERE value > 0
LIMIT 20;

-- 9. Explain plan to see optimization
EXPLAIN SELECT id, value 
FROM 's3://bucket/dataset.lance' 
WHERE id BETWEEN 1000 AND 2000;

-- 10. Create a view over Lance data
CREATE VIEW lance_view AS 
SELECT * FROM 'test/data/sample.lance';

SELECT * FROM lance_view WHERE category = 'A' LIMIT 5;