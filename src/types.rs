use anyhow::{anyhow, Result};
use arrow_schema::DataType;
use duckdb::core::{LogicalTypeHandle, LogicalTypeId};

/// Convert Arrow DataType to DuckDB LogicalType
pub fn arrow_to_duckdb_type(arrow_type: &DataType) -> Result<LogicalTypeHandle> {
    let logical_type = match arrow_type {
        // Numeric types
        DataType::Boolean => LogicalTypeHandle::from(LogicalTypeId::Boolean),
        DataType::Int8 => LogicalTypeHandle::from(LogicalTypeId::Tinyint),
        DataType::Int16 => LogicalTypeHandle::from(LogicalTypeId::Smallint),
        DataType::Int32 => LogicalTypeHandle::from(LogicalTypeId::Integer),
        DataType::Int64 => LogicalTypeHandle::from(LogicalTypeId::Bigint),
        DataType::UInt8 => LogicalTypeHandle::from(LogicalTypeId::UTinyint),
        DataType::UInt16 => LogicalTypeHandle::from(LogicalTypeId::USmallint),
        DataType::UInt32 => LogicalTypeHandle::from(LogicalTypeId::UInteger),
        DataType::UInt64 => LogicalTypeHandle::from(LogicalTypeId::UBigint),
        DataType::Float32 => LogicalTypeHandle::from(LogicalTypeId::Float),
        DataType::Float64 => LogicalTypeHandle::from(LogicalTypeId::Double),
        
        // String types
        DataType::Utf8 | DataType::LargeUtf8 => LogicalTypeHandle::from(LogicalTypeId::Varchar),
        
        // Binary types
        DataType::Binary | DataType::LargeBinary => LogicalTypeHandle::from(LogicalTypeId::Blob),
        
        // Date/Time types
        DataType::Date32 | DataType::Date64 => LogicalTypeHandle::from(LogicalTypeId::Date),
        DataType::Time32(_) | DataType::Time64(_) => LogicalTypeHandle::from(LogicalTypeId::Time),
        DataType::Timestamp(_, _) => LogicalTypeHandle::from(LogicalTypeId::TimestampMs),
        
        // Complex types - temporarily map to VARCHAR
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
            // TODO: Implement proper list type mapping
            LogicalTypeHandle::from(LogicalTypeId::Varchar)
        }
        DataType::Struct(_) => {
            // TODO: Implement proper struct type mapping
            LogicalTypeHandle::from(LogicalTypeId::Varchar)
        }
        DataType::Map(_, _) => {
            // TODO: Implement proper map type mapping
            LogicalTypeHandle::from(LogicalTypeId::Varchar)
        }
        
        // Decimal type
        DataType::Decimal128(_precision, _scale) | DataType::Decimal256(_precision, _scale) => {
            // TODO: Create decimal type with proper precision and scale
            LogicalTypeHandle::from(LogicalTypeId::Double)
        }
        
        // Null type - map to VARCHAR for now
        DataType::Null => LogicalTypeHandle::from(LogicalTypeId::Varchar),
        
        _ => {
            return Err(anyhow!("Unsupported Arrow type: {:?}", arrow_type));
        }
    };
    
    Ok(logical_type)
}