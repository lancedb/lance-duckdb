#define DUCKDB_EXTENSION_MAIN

#include "lance_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"

namespace duckdb {

// Forward declaration
void RegisterLanceScan(DatabaseInstance &instance);

static void LoadInternal(DatabaseInstance &instance) {
	// Register the lance_scan table function
	RegisterLanceScan(instance);
}

void LanceExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
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