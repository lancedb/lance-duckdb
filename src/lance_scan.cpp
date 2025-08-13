#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

struct LanceScanBindData : public TableFunctionData {
	string path;
};

static unique_ptr<FunctionData> LanceScanBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<LanceScanBindData>();
	
	if (input.inputs.empty()) {
		throw InvalidInputException("lance_scan requires at least one argument (file path)");
	}
	
	result->path = input.inputs[0].GetValue<string>();
	
	// Return a dummy schema for now
	return_types.push_back(LogicalType::INTEGER);
	names.push_back("dummy_column");
	
	return std::move(result);
}

static void LanceScanFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	// Do nothing - just return empty chunk
	output.SetCardinality(0);
}

void RegisterLanceScan(DatabaseInstance &instance) {
	TableFunction lance_scan("lance_scan", {LogicalType::VARCHAR}, LanceScanFunc, LanceScanBind);
	ExtensionUtil::RegisterFunction(instance, lance_scan);
}

} // namespace duckdb