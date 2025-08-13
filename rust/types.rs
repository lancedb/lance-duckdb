use arrow::datatypes::DataType;

pub fn arrow_type_to_string(data_type: &DataType) -> String {
    match data_type {
        DataType::Boolean => "bool".to_string(),
        DataType::Int8 => "int8".to_string(),
        DataType::Int16 => "int16".to_string(),
        DataType::Int32 => "int32".to_string(),
        DataType::Int64 => "int64".to_string(),
        DataType::UInt8 => "uint8".to_string(),
        DataType::UInt16 => "uint16".to_string(),
        DataType::UInt32 => "uint32".to_string(),
        DataType::UInt64 => "uint64".to_string(),
        DataType::Float32 => "float".to_string(),
        DataType::Float64 => "double".to_string(),
        DataType::Utf8 => "utf8".to_string(),
        DataType::LargeUtf8 => "string".to_string(),
        DataType::Binary => "binary".to_string(),
        DataType::Date32 => "date32".to_string(),
        DataType::Date64 => "date64".to_string(),
        DataType::Timestamp(_, _) => "timestamp".to_string(),
        DataType::Decimal128(_, _) => "decimal128".to_string(),
        DataType::List(_) => "list".to_string(),
        DataType::Struct(_) => "struct".to_string(),
        _ => "unknown".to_string(),
    }
}

pub fn string_to_arrow_type(type_str: &str) -> DataType {
    match type_str {
        "bool" => DataType::Boolean,
        "int8" => DataType::Int8,
        "int16" => DataType::Int16,
        "int32" => DataType::Int32,
        "int64" => DataType::Int64,
        "uint8" => DataType::UInt8,
        "uint16" => DataType::UInt16,
        "uint32" => DataType::UInt32,
        "uint64" => DataType::UInt64,
        "float" => DataType::Float32,
        "double" => DataType::Float64,
        "utf8" | "string" => DataType::Utf8,
        "binary" => DataType::Binary,
        "date32" => DataType::Date32,
        "date64" => DataType::Date64,
        _ => DataType::Utf8, // Default to string
    }
}
