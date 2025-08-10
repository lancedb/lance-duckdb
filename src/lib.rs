use duckdb::Connection;
use libduckdb_sys as ffi;
use std::ffi::CString;

mod lance_scan;
mod replacement_scan;
mod types;

/// Custom entry point for the Lance DuckDB extension
///
/// This custom entry point gives us direct access to the database handle,
/// which is necessary for registering replacement scans.
///
/// # Safety
///
/// This function is called by DuckDB's C API when loading the extension.
/// The caller must ensure that the info and access pointers are valid.
#[no_mangle]
pub unsafe extern "C" fn lance_init_c_api(
    info: ffi::duckdb_extension_info,
    access: *const ffi::duckdb_extension_access,
) -> bool {
    // Initialize the API
    let have_api = match ffi::duckdb_rs_extension_api_init(info, access, "v1.3.2") {
        Ok(api) => api,
        Err(_) => {
            return false;
        }
    };

    if !have_api {
        // API version mismatch
        return false;
    }

    // Get the database handle from the extension access
    let get_database_fn = match (*access).get_database {
        Some(f) => f,
        None => {
            let error_msg = CString::new("Failed to get database access function").unwrap();
            if let Some(set_error) = (*access).set_error {
                set_error(info, error_msg.as_ptr());
            }
            return false;
        }
    };

    let db: ffi::duckdb_database = *(get_database_fn)(info);

    // Create a connection from the database handle
    let connection = match Connection::open_from_raw(db.cast()) {
        Ok(conn) => conn,
        Err(e) => {
            let error_msg = CString::new(format!("Failed to create connection: {e}")).unwrap();
            if let Some(set_error) = (*access).set_error {
                set_error(info, error_msg.as_ptr());
            }
            return false;
        }
    };

    // Register the lance_scan table function
    if let Err(e) = lance_scan::register_lance_scan(&connection) {
        let error_msg = CString::new(format!("Failed to register lance_scan: {e}")).unwrap();
        if let Some(set_error) = (*access).set_error {
            set_error(info, error_msg.as_ptr());
        }
        return false;
    }

    // Register the replacement scan for .lance files
    if let Err(e) = replacement_scan::register_replacement_scan_internal(db) {
        let error_msg = CString::new(format!("Failed to register replacement scan: {e}")).unwrap();
        if let Some(set_error) = (*access).set_error {
            set_error(info, error_msg.as_ptr());
        }
        return false;
    }

    true
}
