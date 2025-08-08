use anyhow::anyhow;
use arrow_array::{RecordBatch, StringArray, Int64Array, Float64Array};
use arrow_schema::{Schema as ArrowSchema, Field, DataType};
use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection,
};
use std::{
    ffi::CString,
    sync::{Arc, Mutex},
};

use crate::types::arrow_to_duckdb_type;

#[repr(C)]
pub struct LanceScanBindData {
    pub path: String,
    pub schema: Arc<ArrowSchema>,
    pub batch_size: usize,
}

#[repr(C)]
pub struct LanceScanInitData {
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

        // For MVP, create a demo schema
        // TODO: In production, open Lance dataset and get actual schema
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        
        // Register output columns in DuckDB
        for field in schema.fields() {
            let logical_type = arrow_to_duckdb_type(field.data_type())?;
            bind.add_result_column(field.name(), logical_type);
        }
        
        Ok(LanceScanBindData {
            path,
            schema,
            batch_size,
        })
    }

    fn init(_init: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        // Create demo data
        // TODO: In production, read actual Lance data
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        
        let id_array = Int64Array::from(vec![1, 2, 3, 4, 5]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie", "David", "Eve"]);
        let value_array = Float64Array::from(vec![1.1, 2.2, 3.3, 4.4, 5.5]);
        
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(value_array),
            ],
        )?;
        
        Ok(LanceScanInitData {
            batches: Arc::new(Mutex::new(vec![batch])),
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
            for (col_idx, _column) in current_batch.columns().iter().enumerate() {
                let vector = output.flat_vector(col_idx);
                
                // Simple string conversion for all types (MVP)
                for i in 0..rows_to_output {
                    let actual_row = *row_idx + i;
                    let value = format!("row_{actual_row}_col_{col_idx}");
                    let c_value = CString::new(value)?;
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