#define DUCKDB_EXTENSION_MAIN

#include "lance_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/main/config.hpp"

// Forward declarations for functions defined in other files
namespace duckdb {
    void RegisterLanceScan(DatabaseInstance &db);
    void RegisterLanceCopy(DatabaseInstance &db);
    void RegisterLanceReplacement(DBConfig &config);
}

namespace duckdb {

static void LoadInternal(DatabaseInstance &instance) {
    // Register table function for lance_scan
    RegisterLanceScan(instance);
    
    // Register copy function for COPY TO/FROM
    RegisterLanceCopy(instance);
}

void LanceExtension::Load(DuckDB &db) {
    LoadInternal(*db.instance);
    
    // Register replacement scan
    auto &config = DBConfig::GetConfig(*db.instance);
    RegisterLanceReplacement(config);
}

std::string LanceExtension::Name() {
    return "lance";
}

std::string LanceExtension::Version() const {
#ifdef EXT_VERSION_LANCE
    return EXT_VERSION_LANCE;
#else
    return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void lance_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::LanceExtension>();
}

DUCKDB_EXTENSION_API const char *lance_version() {
    return duckdb::DuckDB::LibraryVersion();
}

}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif