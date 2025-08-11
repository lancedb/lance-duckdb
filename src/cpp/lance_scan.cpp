#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
// Arrow integration headers would go here when available

#include <memory>
#include <mutex>

// FFI declarations for Rust functions
extern "C" {
    // Dataset operations
    void* lance_open_dataset(const char* path);
    void lance_close_dataset(void* dataset);
    
    // Schema operations
    void* lance_get_schema(void* dataset);
    void lance_free_schema(void* schema);
    int64_t lance_schema_num_fields(void* schema);
    const char* lance_schema_field_name(void* schema, int64_t index);
    const char* lance_schema_field_type(void* schema, int64_t index);
    
    // Data reading
    void* lance_read_batch(void* dataset);
    void lance_free_batch(void* batch);
    int64_t lance_batch_num_rows(void* batch);
    void* lance_batch_to_arrow(void* batch);
}

namespace duckdb {

struct LanceScanBindData : public TableFunctionData {
    string file_path;
    vector<string> column_names;
    vector<LogicalType> column_types;
    void* dataset = nullptr;
    
    ~LanceScanBindData() {
        if (dataset) {
            lance_close_dataset(dataset);
        }
    }
};

struct LanceScanGlobalState : public GlobalTableFunctionState {
    mutex lock;
    bool finished = false;
    void* current_batch = nullptr;
    idx_t batch_index = 0;
    
    ~LanceScanGlobalState() {
        if (current_batch) {
            lance_free_batch(current_batch);
        }
    }
};

struct LanceScanLocalState : public LocalTableFunctionState {};

static unique_ptr<FunctionData> LanceScanBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
    auto bind_data = make_uniq<LanceScanBindData>();
    
    // Get the file path from the input
    if (input.inputs.empty() || input.inputs[0].IsNull()) {
        throw InvalidInputException("lance_scan requires a file path");
    }
    bind_data->file_path = input.inputs[0].GetValue<string>();
    
    // Open the Lance dataset
    bind_data->dataset = lance_open_dataset(bind_data->file_path.c_str());
    if (!bind_data->dataset) {
        throw IOException("Failed to open Lance dataset: " + bind_data->file_path);
    }
    
    // Get schema from the dataset
    void* schema = lance_get_schema(bind_data->dataset);
    if (!schema) {
        throw IOException("Failed to get schema from Lance dataset");
    }
    
    // Extract column names and types
    int64_t num_fields = lance_schema_num_fields(schema);
    for (int64_t i = 0; i < num_fields; i++) {
        const char* field_name = lance_schema_field_name(schema, i);
        const char* field_type = lance_schema_field_type(schema, i);
        
        bind_data->column_names.push_back(string(field_name));
        
        // Map Lance/Arrow types to DuckDB types
        // For now, use simplified mapping
        string type_str(field_type);
        LogicalType duckdb_type;
        
        if (type_str == "int32" || type_str == "int64") {
            duckdb_type = LogicalType::BIGINT;
        } else if (type_str == "float" || type_str == "double") {
            duckdb_type = LogicalType::DOUBLE;
        } else if (type_str == "string" || type_str == "utf8") {
            duckdb_type = LogicalType::VARCHAR;
        } else if (type_str == "bool") {
            duckdb_type = LogicalType::BOOLEAN;
        } else {
            // Default to VARCHAR for unknown types
            duckdb_type = LogicalType::VARCHAR;
        }
        
        bind_data->column_types.push_back(duckdb_type);
    }
    
    lance_free_schema(schema);
    
    // Set return types and names
    return_types = bind_data->column_types;
    names = bind_data->column_names;
    
    return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> LanceScanInit(ClientContext &context, TableFunctionInitInput &input) {
    return make_uniq<LanceScanGlobalState>();
}

static unique_ptr<LocalTableFunctionState> LanceScanLocalInit(ExecutionContext &context, TableFunctionInitInput &input,
                                                              GlobalTableFunctionState *global_state) {
    return make_uniq<LanceScanLocalState>();
}

static void LanceScanFunc(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &bind_data = data.bind_data->Cast<LanceScanBindData>();
    auto &global_state = data.global_state->Cast<LanceScanGlobalState>();
    
    lock_guard<mutex> lock(global_state.lock);
    
    if (global_state.finished) {
        return;
    }
    
    // Read next batch if needed
    if (!global_state.current_batch) {
        global_state.current_batch = lance_read_batch(bind_data.dataset);
        if (!global_state.current_batch) {
            global_state.finished = true;
            return;
        }
    }
    
    // Convert Arrow batch to DuckDB DataChunk
    void* arrow_batch = lance_batch_to_arrow(global_state.current_batch);
    if (!arrow_batch) {
        throw IOException("Failed to convert Lance batch to Arrow format");
    }
    
    // For now, simplified conversion - this will need proper implementation
    idx_t num_rows = lance_batch_num_rows(global_state.current_batch);
    output.SetCardinality(num_rows);
    
    // TODO: Implement proper Arrow to DuckDB conversion here
    
    // Mark batch as consumed
    lance_free_batch(global_state.current_batch);
    global_state.current_batch = nullptr;
    
    // Check if we've read all data
    void* next_batch = lance_read_batch(bind_data.dataset);
    if (!next_batch) {
        global_state.finished = true;
    } else {
        global_state.current_batch = next_batch;
    }
}

void RegisterLanceScan(DatabaseInstance &db) {
    TableFunction lance_scan("lance_scan", {LogicalType::VARCHAR}, LanceScanFunc, LanceScanBind, 
                            LanceScanInit, LanceScanLocalInit);
    
    lance_scan.name = "lance_scan";
    lance_scan.projection_pushdown = false;  // TODO: implement later
    lance_scan.filter_pushdown = false;      // TODO: implement later
    
    ExtensionUtil::RegisterFunction(db, lance_scan);
}

} // namespace duckdb