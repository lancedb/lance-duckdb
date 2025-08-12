# This file is included by DuckDB's build system to configure the extension

# Extension from this repo
duckdb_extension_load(lance
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR}/include
    LOAD_TESTS
)