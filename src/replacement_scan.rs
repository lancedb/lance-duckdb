use libduckdb_sys as ffi;
use std::ffi::{c_void, CStr, CString};

/// The callback function for Lance replacement scan
///
/// This function is called by DuckDB when it encounters a table reference
/// that might be a Lance dataset. If the path ends with .lance, we tell
/// DuckDB to replace it with a call to lance_scan function.
unsafe extern "C" fn lance_replacement_scan_callback(
    info: ffi::duckdb_replacement_scan_info,
    table_name: *const std::os::raw::c_char,
    _extra_data: *mut c_void,
) {
    // Parse the table name
    let table_name_cstr = CStr::from_ptr(table_name);
    let table_name_str = match table_name_cstr.to_str() {
        Ok(s) => s,
        Err(_) => return, // Invalid UTF-8, skip
    };

    // Check if this is a Lance dataset (ends with .lance)
    // Lance datasets are directories, so both /path/to/dataset.lance
    // and dataset.lance should be recognized
    if !table_name_str.ends_with(".lance") {
        return; // Not a Lance file, let DuckDB continue trying other replacement scans
    }

    // Set the replacement function name to lance_scan
    let function_name = CString::new("lance_scan").unwrap();
    ffi::duckdb_replacement_scan_set_function_name(info, function_name.as_ptr());

    // Add the file path as a parameter to the lance_scan function
    let path_value = ffi::duckdb_create_varchar(table_name);
    if !path_value.is_null() {
        ffi::duckdb_replacement_scan_add_parameter(info, path_value);
        ffi::duckdb_destroy_value(&mut (path_value as *mut _));
    }
}

/// Register the replacement scan for .lance files with DuckDB
///
/// This function registers a callback that will be invoked whenever DuckDB
/// encounters a table reference. If the reference ends with .lance,
/// it will be replaced with a call to lance_scan.
pub unsafe fn register_replacement_scan_internal(db: ffi::duckdb_database) -> anyhow::Result<()> {
    // Register the replacement scan callback
    ffi::duckdb_add_replacement_scan(
        db,
        Some(lance_replacement_scan_callback),
        std::ptr::null_mut(), // No extra data needed
        None,                 // No delete callback needed
    );

    Ok(())
}