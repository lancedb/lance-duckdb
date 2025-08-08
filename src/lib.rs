use duckdb::Connection;
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
use libduckdb_sys as ffi;
use std::error::Error;

mod lance_scan;
mod replacement_scan;
mod types;

/// Entry point for the Lance DuckDB extension
///
/// # Safety
///
/// This function is called by DuckDB's C API when loading the extension.
/// The caller must ensure that the connection is valid and properly initialized.
#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    // Register lance_scan table function
    lance_scan::register_lance_scan(&con)?;
    
    // Register replacement scan for .lance files
    replacement_scan::register_replacement_scan(&con)?;
    
    Ok(())
}