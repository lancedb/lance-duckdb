use anyhow::anyhow;
use arrow_array::RecordBatch;
use arrow_schema::{self, Schema as ArrowSchema};
use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection,
};
use lance::Dataset;
use std::{
    ffi::CString,
    sync::{Arc, Mutex},
};
use tokio::runtime::Runtime;

use crate::types::arrow_to_duckdb_type;

#[repr(C)]
pub struct LanceScanBindData {
    pub path: String,
    pub schema: Arc<ArrowSchema>,
    pub batch_size: usize,
}

#[repr(C)]
pub struct LanceScanInitData {
    path: String,
    runtime: Arc<Runtime>,
    batches: Arc<Mutex<Vec<RecordBatch>>>,
    current_batch_idx: Arc<Mutex<usize>>,
    current_row_idx: Arc<Mutex<usize>>,
}

pub struct LanceScanVTab;

impl VTab for LanceScanVTab {
    type InitData = LanceScanInitData;
    type BindData = LanceScanBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        // Get path parameter
        let path = bind.get_parameter(0).to_string();
        
        // Get optional batch size parameter
        let batch_size = if bind.get_parameter_count() > 1 {
            bind.get_parameter(1).to_int64() as usize
        } else {
            8192 // Default batch size
        };

        // Open the Lance dataset to get schema
        let runtime = Runtime::new()?;
        let dataset = runtime.block_on(async {
            Dataset::open(&path).await
        })?;
        
        // Get the Arrow schema from Lance dataset
        let lance_schema = dataset.schema();
        // Convert Lance schema to Arrow schema
        let arrow_schema: ArrowSchema = lance_schema.into();
        let arrow_schema = Arc::new(arrow_schema);
        
        // Register output columns in DuckDB
        for field in arrow_schema.fields() {
            let logical_type = arrow_to_duckdb_type(field.data_type())?;
            bind.add_result_column(field.name(), logical_type);
        }
        
        Ok(LanceScanBindData {
            path,
            schema: arrow_schema,
            batch_size,
        })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let bind_data: *const LanceScanBindData = init.get_bind_data();
        let path = unsafe { (*bind_data).path.clone() };
        
        // Create a new runtime for this scan
        let runtime = Arc::new(Runtime::new()?);
        
        // Read all batches from the dataset
        let batches = runtime.block_on(async {
            let dataset = Dataset::open(&path).await?;
            let scanner = dataset.scan().try_into_stream().await?;
            
            use futures::StreamExt;
            let mut scanner = Box::pin(scanner);
            let mut batches = Vec::new();
            
            while let Some(batch_result) = scanner.next().await {
                match batch_result {
                    Ok(batch) => batches.push(batch),
                    Err(e) => return Err(e.into()),
                }
            }
            
            Ok::<Vec<RecordBatch>, Box<dyn std::error::Error>>(batches)
        })?;
        
        Ok(LanceScanInitData {
            path,
            runtime,
            batches: Arc::new(Mutex::new(batches)),
            current_batch_idx: Arc::new(Mutex::new(0)),
            current_row_idx: Arc::new(Mutex::new(0)),
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        
        let batches = init_data.batches.lock().unwrap();
        let mut batch_idx = init_data.current_batch_idx.lock().unwrap();
        let mut row_idx = init_data.current_row_idx.lock().unwrap();
        
        // Check if we've exhausted all batches
        if *batch_idx >= batches.len() {
            output.set_len(0);
            return Ok(());
        }
        
        let current_batch = &batches[*batch_idx];
        let batch_rows = current_batch.num_rows();
        
        // Calculate how many rows we can output
        let rows_remaining = batch_rows - *row_idx;
        let rows_to_output = std::cmp::min(rows_remaining, 2048);
        
        // Output data
        if rows_to_output > 0 {
            // For each column, copy data to DuckDB vectors
            for (col_idx, column) in current_batch.columns().iter().enumerate() {
                let vector = output.flat_vector(col_idx);
                
                // Convert Arrow data to DuckDB format
                // For MVP, convert everything to string
                for i in 0..rows_to_output {
                    let actual_row = *row_idx + i;
                    
                    // Get string representation of the value
                    let value_str = if column.is_null(actual_row) {
                        String::new()
                    } else {
                        // Simplified conversion - in production handle each type properly
                        match column.data_type() {
                            arrow_schema::DataType::Int64 => {
                                use arrow_array::cast::AsArray;
                                let array = column.as_primitive::<arrow_array::types::Int64Type>();
                                array.value(actual_row).to_string()
                            },
                            arrow_schema::DataType::Float64 => {
                                use arrow_array::cast::AsArray;
                                let array = column.as_primitive::<arrow_array::types::Float64Type>();
                                array.value(actual_row).to_string()
                            },
                            arrow_schema::DataType::Utf8 => {
                                use arrow_array::cast::AsArray;
                                let array = column.as_string::<i32>();
                                array.value(actual_row).to_string()
                            },
                            _ => format!("unsupported_type")
                        }
                    };
                    
                    let c_value = CString::new(value_str)?;
                    vector.insert(i, c_value);
                }
            }
            
            output.set_len(rows_to_output);
            *row_idx += rows_to_output;
            
            // Move to next batch if current is exhausted
            if *row_idx >= batch_rows {
                *batch_idx += 1;
                *row_idx = 0;
            }
        } else {
            output.set_len(0);
        }
        
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // path (required)
        ])
    }
}

pub fn register_lance_scan(con: &Connection) -> anyhow::Result<()> {
    con.register_table_function::<LanceScanVTab>("lance_scan")
        .map_err(|e| anyhow!("Failed to register lance_scan: {}", e))?;
    Ok(())
}