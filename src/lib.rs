use std::ffi::{c_char, c_void, CStr, CString};
use std::ptr;
use std::sync::Arc;

use arrow::array::{make_array, Array, RecordBatch, StructArray};
use arrow::datatypes::{Schema, Field};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use lance::Dataset;
use tokio::runtime::Runtime;

mod reader;
mod writer;
mod types;

use reader::LanceReader;
use writer::LanceWriter;

// Dataset operations
#[no_mangle]
pub extern "C" fn lance_open_dataset(path: *const c_char) -> *mut c_void {
    if path.is_null() {
        return ptr::null_mut();
    }
    
    let path_str = unsafe {
        match CStr::from_ptr(path).to_str() {
            Ok(s) => s,
            Err(_) => return ptr::null_mut(),
        }
    };
    
    let runtime = match Runtime::new() {
        Ok(rt) => rt,
        Err(_) => return ptr::null_mut(),
    };
    
    let dataset = match runtime.block_on(Dataset::open(path_str)) {
        Ok(ds) => ds,
        Err(_) => return ptr::null_mut(),
    };
    
    let reader = Box::new(LanceReader {
        dataset: Arc::new(dataset),
        runtime: Arc::new(runtime),
    });
    
    Box::into_raw(reader) as *mut c_void
}

#[no_mangle]
pub extern "C" fn lance_close_dataset(dataset: *mut c_void) {
    if !dataset.is_null() {
        unsafe {
            let _ = Box::from_raw(dataset as *mut LanceReader);
        }
    }
}

// Schema operations
#[no_mangle]
pub extern "C" fn lance_get_schema(dataset: *mut c_void) -> *mut c_void {
    if dataset.is_null() {
        return ptr::null_mut();
    }
    
    let reader = unsafe { &*(dataset as *const LanceReader) };
    let schema = reader.dataset.schema();
    
    let arrow_schema: Schema = match schema.try_into() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };
    
    Box::into_raw(Box::new(Arc::new(arrow_schema))) as *mut c_void
}

#[no_mangle]
pub extern "C" fn lance_free_schema(schema: *mut c_void) {
    if !schema.is_null() {
        unsafe {
            let _ = Box::from_raw(schema as *mut Arc<Schema>);
        }
    }
}

#[no_mangle]
pub extern "C" fn lance_schema_num_fields(schema: *mut c_void) -> i64 {
    if schema.is_null() {
        return 0;
    }
    
    let schema = unsafe { &*(schema as *const Arc<Schema>) };
    schema.fields().len() as i64
}

#[no_mangle]
pub extern "C" fn lance_schema_field_name(schema: *mut c_void, index: i64) -> *const c_char {
    if schema.is_null() || index < 0 {
        return ptr::null();
    }
    
    let schema = unsafe { &*(schema as *const Arc<Schema>) };
    let fields = schema.fields();
    
    if index as usize >= fields.len() {
        return ptr::null();
    }
    
    let field = &fields[index as usize];
    match CString::new(field.name().as_str()) {
        Ok(c_str) => {
            let ptr = c_str.as_ptr();
            std::mem::forget(c_str);
            ptr
        }
        Err(_) => ptr::null(),
    }
}

#[no_mangle]
pub extern "C" fn lance_schema_field_type(schema: *mut c_void, index: i64) -> *const c_char {
    if schema.is_null() || index < 0 {
        return ptr::null();
    }
    
    let schema = unsafe { &*(schema as *const Arc<Schema>) };
    let fields = schema.fields();
    
    if index as usize >= fields.len() {
        return ptr::null();
    }
    
    let field = &fields[index as usize];
    let type_str = types::arrow_type_to_string(field.data_type());
    
    match CString::new(type_str) {
        Ok(c_str) => {
            let ptr = c_str.as_ptr();
            std::mem::forget(c_str);
            ptr
        }
        Err(_) => ptr::null(),
    }
}

// Data reading
#[no_mangle]
pub extern "C" fn lance_read_batch(dataset: *mut c_void) -> *mut c_void {
    if dataset.is_null() {
        return ptr::null_mut();
    }
    
    let reader = unsafe { &*(dataset as *const LanceReader) };
    
    match reader.read_next_batch() {
        Some(batch) => Box::into_raw(Box::new(batch)) as *mut c_void,
        None => ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "C" fn lance_free_batch(batch: *mut c_void) {
    if !batch.is_null() {
        unsafe {
            let _ = Box::from_raw(batch as *mut RecordBatch);
        }
    }
}

#[no_mangle]
pub extern "C" fn lance_batch_num_rows(batch: *mut c_void) -> i64 {
    if batch.is_null() {
        return 0;
    }
    
    let batch = unsafe { &*(batch as *const RecordBatch) };
    batch.num_rows() as i64
}

// Structure to hold Arrow C Data Interface arrays and schema
#[repr(C)]
pub struct ArrowArrayStream {
    array: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
}

#[no_mangle]
pub extern "C" fn lance_batch_to_arrow_stream(
    batch: *mut c_void,
    out_array: *mut FFI_ArrowArray,
    out_schema: *mut FFI_ArrowSchema,
) -> i32 {
    // For now, return error - proper implementation needs more work
    // The issue is that Arrow-rs FFI support is evolving
    // We'll use a simpler approach with direct data access
    return -1;
}

// Get column as int64 array
#[no_mangle]
pub extern "C" fn lance_batch_get_int64_column(
    batch: *mut c_void,
    col_idx: i64,
    out_data: *mut i64,
) -> i64 {
    if batch.is_null() || out_data.is_null() {
        return -1;
    }
    
    let batch = unsafe { &*(batch as *const RecordBatch) };
    if col_idx < 0 || col_idx as usize >= batch.num_columns() {
        return -1;
    }
    
    let column = batch.column(col_idx as usize);
    
    // Try to get as Int64Array
    use arrow::array::Int64Array;
    if let Some(array) = column.as_any().downcast_ref::<Int64Array>() {
        let out_slice = unsafe { std::slice::from_raw_parts_mut(out_data, array.len()) };
        for (i, value) in array.iter().enumerate() {
            out_slice[i] = value.unwrap_or(0);
        }
        return array.len() as i64;
    }
    
    -1
}

// Get column as float64 array
#[no_mangle]
pub extern "C" fn lance_batch_get_float64_column(
    batch: *mut c_void,
    col_idx: i64,
    out_data: *mut f64,
) -> i64 {
    if batch.is_null() || out_data.is_null() {
        return -1;
    }
    
    let batch = unsafe { &*(batch as *const RecordBatch) };
    if col_idx < 0 || col_idx as usize >= batch.num_columns() {
        return -1;
    }
    
    let column = batch.column(col_idx as usize);
    
    // Try to get as Float64Array
    use arrow::array::Float64Array;
    if let Some(array) = column.as_any().downcast_ref::<Float64Array>() {
        let out_slice = unsafe { std::slice::from_raw_parts_mut(out_data, array.len()) };
        for (i, value) in array.iter().enumerate() {
            out_slice[i] = value.unwrap_or(0.0);
        }
        return array.len() as i64;
    }
    
    -1
}

// Get string value from column
#[no_mangle]
pub extern "C" fn lance_batch_get_string_value(
    batch: *mut c_void,
    col_idx: i64,
    row_idx: i64,
) -> *const c_char {
    if batch.is_null() {
        return ptr::null();
    }
    
    let batch = unsafe { &*(batch as *const RecordBatch) };
    if col_idx < 0 || col_idx as usize >= batch.num_columns() {
        return ptr::null();
    }
    if row_idx < 0 || row_idx as usize >= batch.num_rows() {
        return ptr::null();
    }
    
    let column = batch.column(col_idx as usize);
    
    // Try to get as StringArray
    use arrow::array::StringArray;
    if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
        if !array.is_null(row_idx as usize) {
            let value = array.value(row_idx as usize);
            match CString::new(value) {
                Ok(c_str) => {
                    let ptr = c_str.as_ptr();
                    std::mem::forget(c_str);
                    return ptr;
                }
                Err(_) => return ptr::null(),
            }
        }
    }
    
    ptr::null()
}

// Writer operations
#[no_mangle]
pub extern "C" fn lance_create_writer(path: *const c_char, arrow_schema: *mut c_void) -> *mut c_void {
    if path.is_null() || arrow_schema.is_null() {
        return ptr::null_mut();
    }
    
    let path_str = unsafe {
        match CStr::from_ptr(path).to_str() {
            Ok(s) => s,
            Err(_) => return ptr::null_mut(),
        }
    };
    
    let schema = unsafe { &*(arrow_schema as *const Arc<Schema>) };
    
    match LanceWriter::new(path_str, schema.as_ref().clone()) {
        Ok(writer) => Box::into_raw(Box::new(writer)) as *mut c_void,
        Err(_) => ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "C" fn lance_write_batch(writer: *mut c_void, arrow_batch: *mut c_void) {
    if writer.is_null() || arrow_batch.is_null() {
        return;
    }
    
    let writer = unsafe { &mut *(writer as *mut LanceWriter) };
    let batch = unsafe { &*(arrow_batch as *const RecordBatch) };
    
    let _ = writer.write_batch(batch.clone());
}

#[no_mangle]
pub extern "C" fn lance_finish_writer(writer: *mut c_void) {
    if writer.is_null() {
        return;
    }
    
    let writer = unsafe { &mut *(writer as *mut LanceWriter) };
    let _ = writer.finish();
}

#[no_mangle]
pub extern "C" fn lance_close_writer(writer: *mut c_void) {
    if !writer.is_null() {
        unsafe {
            let _ = Box::from_raw(writer as *mut LanceWriter);
        }
    }
}

// Schema conversion
#[no_mangle]
pub extern "C" fn lance_duckdb_to_arrow_schema(
    names: *const *const c_char,
    types: *const *const c_char,
    num_fields: i64,
) -> *mut c_void {
    if names.is_null() || types.is_null() || num_fields <= 0 {
        return ptr::null_mut();
    }
    
    let mut fields = Vec::new();
    
    for i in 0..num_fields as usize {
        let name = unsafe {
            let name_ptr = *names.add(i);
            match CStr::from_ptr(name_ptr).to_str() {
                Ok(s) => s.to_string(),
                Err(_) => return ptr::null_mut(),
            }
        };
        
        let type_str = unsafe {
            let type_ptr = *types.add(i);
            match CStr::from_ptr(type_ptr).to_str() {
                Ok(s) => s,
                Err(_) => return ptr::null_mut(),
            }
        };
        
        let arrow_type = types::string_to_arrow_type(type_str);
        fields.push(arrow::datatypes::Field::new(name, arrow_type, true));
    }
    
    let schema = Arc::new(Schema::new(fields));
    Box::into_raw(Box::new(schema)) as *mut c_void
}