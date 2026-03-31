// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::error::client_error;
use crate::fcore;

/// Extract an i64 from a serde_json::Value.
///
/// napi-rs stores JavaScript numbers > u32::MAX as serde_json's Float variant,
/// and `Value::as_i64()` returns None for Float values. This helper falls back
/// to checking whether the f64 value represents an exact integer.
fn value_as_i64(value: &serde_json::Value) -> Option<i64> {
    value.as_i64().or_else(|| {
        value.as_f64().and_then(|f| {
            let n = f as i64;
            if n as f64 == f {
                Some(n)
            } else {
                None
            }
        })
    })
}

/// Convert a serde_json::Value to a Rust Datum based on the expected data type.
pub fn json_value_to_datum(
    value: &serde_json::Value,
    data_type: &fcore::metadata::DataType,
) -> napi::Result<fcore::row::Datum<'static>> {
    if value.is_null() {
        return Ok(fcore::row::Datum::Null);
    }

    use fcore::metadata::DataType;
    match data_type {
        DataType::Boolean(_) => {
            let b = value
                .as_bool()
                .ok_or_else(|| client_error("Expected boolean value"))?;
            Ok(fcore::row::Datum::Bool(b))
        }
        DataType::TinyInt(_) => {
            let n = value
                .as_i64()
                .ok_or_else(|| client_error("Expected integer value for TinyInt"))?;
            let v = i8::try_from(n).map_err(|_| {
                client_error(format!("Value {n} out of range for TinyInt [-128, 127]"))
            })?;
            Ok(fcore::row::Datum::Int8(v))
        }
        DataType::SmallInt(_) => {
            let n = value
                .as_i64()
                .ok_or_else(|| client_error("Expected integer value for SmallInt"))?;
            let v = i16::try_from(n).map_err(|_| {
                client_error(format!(
                    "Value {n} out of range for SmallInt [-32768, 32767]"
                ))
            })?;
            Ok(fcore::row::Datum::Int16(v))
        }
        DataType::Int(_) => {
            let n = value
                .as_i64()
                .ok_or_else(|| client_error("Expected integer value for Int"))?;
            let v = i32::try_from(n).map_err(|_| {
                client_error(format!(
                    "Value {n} out of range for Int [-2147483648, 2147483647]"
                ))
            })?;
            Ok(fcore::row::Datum::Int32(v))
        }
        DataType::BigInt(_) => {
            // Accept i64 number or string representation for large values
            if let Some(n) = value_as_i64(value) {
                Ok(fcore::row::Datum::Int64(n))
            } else if let Some(s) = value.as_str() {
                let n: i64 = s
                    .parse()
                    .map_err(|e| client_error(format!("Invalid BigInt string: {e}")))?;
                Ok(fcore::row::Datum::Int64(n))
            } else {
                Err(client_error(
                    "Expected integer or string for BigInt column",
                ))
            }
        }
        DataType::Float(_) => {
            let n = value
                .as_f64()
                .ok_or_else(|| client_error("Expected number value for Float"))?;
            Ok(fcore::row::Datum::Float32((n as f32).into()))
        }
        DataType::Double(_) => {
            let n = value
                .as_f64()
                .ok_or_else(|| client_error("Expected number value for Double"))?;
            Ok(fcore::row::Datum::Float64(n.into()))
        }
        DataType::String(_) | DataType::Char(_) => {
            let s = value
                .as_str()
                .ok_or_else(|| client_error("Expected string value"))?;
            Ok(fcore::row::Datum::String(s.to_string().into()))
        }
        DataType::Bytes(_) | DataType::Binary(_) => {
            // Accept array of byte numbers [72, 101, 108, ...]
            if let Some(arr) = value.as_array() {
                let bytes: Vec<u8> = arr
                    .iter()
                    .map(|v| {
                        v.as_u64()
                            .and_then(|n| u8::try_from(n).ok())
                            .ok_or_else(|| {
                                client_error("Expected byte value (0-255) in array")
                            })
                    })
                    .collect::<napi::Result<Vec<u8>>>()?;
                Ok(fcore::row::Datum::Blob(bytes.into()))
            } else if let Some(s) = value.as_str() {
                // Accept raw string as UTF-8 bytes
                Ok(fcore::row::Datum::Blob(s.as_bytes().to_vec().into()))
            } else {
                Err(client_error(
                    "Expected byte array or string for Bytes/Binary column",
                ))
            }
        }
        DataType::Decimal(dt) => {
            // Accept string "123.45" or number
            let precision = dt.precision();
            let scale = dt.scale();
            if let Some(s) = value.as_str() {
                // Parse string as decimal
                let bd: bigdecimal::BigDecimal = s
                    .parse()
                    .map_err(|e| client_error(format!("Invalid decimal string: {e}")))?;
                let dec = fcore::row::Decimal::from_big_decimal(bd, precision, scale)
                    .map_err(|e| client_error(e.to_string()))?;
                Ok(fcore::row::Datum::Decimal(dec))
            } else if let Some(n) = value.as_i64() {
                // Treat integer as unscaled value with scale 0
                let bd = bigdecimal::BigDecimal::from(n);
                let dec = fcore::row::Decimal::from_big_decimal(bd, precision, scale)
                    .map_err(|e| client_error(e.to_string()))?;
                Ok(fcore::row::Datum::Decimal(dec))
            } else if let Some(n) = value.as_f64() {
                let s = format!("{n}");
                let bd: bigdecimal::BigDecimal = s
                    .parse()
                    .map_err(|e| client_error(format!("Invalid decimal number: {e}")))?;
                let dec = fcore::row::Decimal::from_big_decimal(bd, precision, scale)
                    .map_err(|e| client_error(e.to_string()))?;
                Ok(fcore::row::Datum::Decimal(dec))
            } else {
                Err(client_error(
                    "Expected string or number for Decimal column",
                ))
            }
        }
        DataType::Date(_) => {
            // Accept epoch days (number) or ISO date string "YYYY-MM-DD"
            if let Some(n) = value.as_i64() {
                let v = i32::try_from(n)
                    .map_err(|_| client_error(format!("Date epoch days {n} out of i32 range")))?;
                Ok(fcore::row::Datum::Date(fcore::row::Date::new(v)))
            } else if let Some(s) = value.as_str() {
                // Parse "YYYY-MM-DD" to epoch days
                let date = parse_date_string(s)?;
                Ok(fcore::row::Datum::Date(date))
            } else {
                Err(client_error(
                    "Expected epoch days (number) or ISO date string for Date column",
                ))
            }
        }
        DataType::Time(_) => {
            // Accept millis since midnight (number)
            if let Some(n) = value.as_i64() {
                let v = i32::try_from(n).map_err(|_| {
                    client_error(format!("Time millis {n} out of i32 range"))
                })?;
                Ok(fcore::row::Datum::Time(fcore::row::Time::new(v)))
            } else {
                Err(client_error(
                    "Expected milliseconds since midnight (number) for Time column",
                ))
            }
        }
        DataType::Timestamp(_) => {
            // TimestampNtz: accept epoch millis (number)
            if let Some(n) = value_as_i64(value) {
                Ok(fcore::row::Datum::TimestampNtz(
                    fcore::row::TimestampNtz::new(n),
                ))
            } else {
                Err(client_error(
                    "Expected epoch milliseconds (number) for Timestamp column",
                ))
            }
        }
        DataType::TimestampLTz(_) => {
            // TimestampLtz: accept epoch millis (number)
            if let Some(n) = value_as_i64(value) {
                Ok(fcore::row::Datum::TimestampLtz(
                    fcore::row::TimestampLtz::new(n),
                ))
            } else {
                Err(client_error(
                    "Expected epoch milliseconds (number) for TimestampLTz column",
                ))
            }
        }
        _ => Err(client_error(format!(
            "Unsupported data type for conversion: {data_type}"
        ))),
    }
}

/// Parse "YYYY-MM-DD" date string to Date (epoch days).
fn parse_date_string(s: &str) -> napi::Result<fcore::row::Date> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 3 {
        return Err(client_error(format!(
            "Invalid date format: '{s}'. Expected YYYY-MM-DD"
        )));
    }
    let year: i32 = parts[0]
        .parse()
        .map_err(|_| client_error(format!("Invalid year in date: '{s}'")))?;
    let month: u32 = parts[1]
        .parse()
        .map_err(|_| client_error(format!("Invalid month in date: '{s}'")))?;
    let day: u32 = parts[2]
        .parse()
        .map_err(|_| client_error(format!("Invalid day in date: '{s}'")))?;

    // Calculate epoch days from 1970-01-01
    let epoch_days = date_to_epoch_days(year, month, day)?;
    Ok(fcore::row::Date::new(epoch_days))
}

/// Convert year/month/day to epoch days since 1970-01-01.
fn date_to_epoch_days(year: i32, month: u32, day: u32) -> napi::Result<i32> {
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return Err(client_error(format!(
            "Invalid date: year={year}, month={month}, day={day}"
        )));
    }

    // Days from year 0 to the start of the given year (before any days in that year)
    fn days_from_year(y: i32) -> i64 {
        let y = y as i64;
        365 * y + (y - 1) / 4 - (y - 1) / 100 + (y - 1) / 400
    }

    let month_days: [u32; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let is_leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;

    let mut total_days: i64 = days_from_year(year) - days_from_year(1970);
    for m in 0..(month - 1) as usize {
        total_days += month_days[m] as i64;
        if m == 1 && is_leap {
            total_days += 1;
        }
    }
    total_days += (day - 1) as i64;

    Ok(total_days as i32)
}

/// Convert a Rust InternalRow field to a serde_json::Value.
pub fn datum_to_json_value(
    row: &dyn fcore::row::InternalRow,
    pos: usize,
    data_type: &fcore::metadata::DataType,
) -> napi::Result<serde_json::Value> {
    if row
        .is_null_at(pos)
        .map_err(|e| client_error(e.to_string()))?
    {
        return Ok(serde_json::Value::Null);
    }

    use fcore::metadata::DataType;
    match data_type {
        DataType::Boolean(_) => {
            let v = row
                .get_boolean(pos)
                .map_err(|e| client_error(e.to_string()))?;
            Ok(serde_json::Value::Bool(v))
        }
        DataType::TinyInt(_) => {
            let v = row.get_byte(pos).map_err(|e| client_error(e.to_string()))?;
            Ok(serde_json::json!(v))
        }
        DataType::SmallInt(_) => {
            let v = row
                .get_short(pos)
                .map_err(|e| client_error(e.to_string()))?;
            Ok(serde_json::json!(v))
        }
        DataType::Int(_) => {
            let v = row.get_int(pos).map_err(|e| client_error(e.to_string()))?;
            Ok(serde_json::json!(v))
        }
        DataType::BigInt(_) => {
            let v = row.get_long(pos).map_err(|e| client_error(e.to_string()))?;
            // Return string for values outside JS Number.MAX_SAFE_INTEGER range
            // to avoid silent precision loss
            const MAX_SAFE_INT: i64 = 9_007_199_254_740_991;
            if v > MAX_SAFE_INT || v < -MAX_SAFE_INT {
                Ok(serde_json::Value::String(v.to_string()))
            } else {
                Ok(serde_json::json!(v))
            }
        }
        DataType::Float(_) => {
            let v = row
                .get_float(pos)
                .map_err(|e| client_error(e.to_string()))?;
            Ok(serde_json::json!(v))
        }
        DataType::Double(_) => {
            let v = row
                .get_double(pos)
                .map_err(|e| client_error(e.to_string()))?;
            Ok(serde_json::json!(v))
        }
        DataType::String(_) => {
            let v = row
                .get_string(pos)
                .map_err(|e| client_error(e.to_string()))?;
            Ok(serde_json::Value::String(v.to_string()))
        }
        DataType::Char(ct) => {
            let v = row
                .get_char(pos, ct.length() as usize)
                .map_err(|e| client_error(e.to_string()))?;
            Ok(serde_json::Value::String(v.to_string()))
        }
        DataType::Bytes(_) => {
            let v = row
                .get_bytes(pos)
                .map_err(|e| client_error(e.to_string()))?;
            let arr: Vec<serde_json::Value> = v.iter().map(|&b| serde_json::json!(b)).collect();
            Ok(serde_json::Value::Array(arr))
        }
        DataType::Binary(bt) => {
            let v = row
                .get_binary(pos, bt.length() as usize)
                .map_err(|e| client_error(e.to_string()))?;
            let arr: Vec<serde_json::Value> = v.iter().map(|&b| serde_json::json!(b)).collect();
            Ok(serde_json::Value::Array(arr))
        }
        DataType::Decimal(dt) => {
            let v = row
                .get_decimal(pos, dt.precision() as usize, dt.scale() as usize)
                .map_err(|e| client_error(e.to_string()))?;
            Ok(serde_json::Value::String(v.to_big_decimal().to_string()))
        }
        DataType::Date(_) => {
            let v = row.get_date(pos).map_err(|e| client_error(e.to_string()))?;
            // Return epoch days as number
            Ok(serde_json::json!(v.get_inner()))
        }
        DataType::Time(_) => {
            let v = row.get_time(pos).map_err(|e| client_error(e.to_string()))?;
            // Return millis since midnight
            Ok(serde_json::json!(v.get_inner()))
        }
        DataType::Timestamp(tt) => {
            let v = row
                .get_timestamp_ntz(pos, tt.precision())
                .map_err(|e| client_error(e.to_string()))?;
            // Return epoch millis
            Ok(serde_json::json!(v.get_millisecond()))
        }
        DataType::TimestampLTz(tt) => {
            let v = row
                .get_timestamp_ltz(pos, tt.precision())
                .map_err(|e| client_error(e.to_string()))?;
            // Return epoch millis
            Ok(serde_json::json!(v.get_epoch_millisecond()))
        }
        _ => Err(client_error(format!(
            "Unsupported data type for conversion: {data_type}"
        ))),
    }
}

/// Convert a JSON object to a GenericRow (full-width row).
pub fn json_to_generic_row(
    row: &serde_json::Value,
    table_info: &fcore::metadata::TableInfo,
) -> napi::Result<fcore::row::GenericRow<'static>> {
    let schema = table_info.get_schema();
    let columns = schema.columns();
    let obj = row
        .as_object()
        .ok_or_else(|| client_error("Row must be a plain object with column names as keys"))?;

    // Validate unknown field names
    for key in obj.keys() {
        if !columns.iter().any(|c| c.name() == key) {
            return Err(client_error(format!("Unknown column: '{key}'")));
        }
    }

    let mut generic_row = fcore::row::GenericRow::new(columns.len());
    for (i, col) in columns.iter().enumerate() {
        if let Some(value) = obj.get(col.name()) {
            let datum = json_value_to_datum(value, col.data_type())?;
            generic_row.set_field(i, datum);
        }
    }
    Ok(generic_row)
}

/// Convert a JSON object to a sparse GenericRow for partial updates.
pub fn json_to_sparse_generic_row(
    row: &serde_json::Value,
    table_info: &fcore::metadata::TableInfo,
    target_indices: &[usize],
) -> napi::Result<fcore::row::GenericRow<'static>> {
    let schema = table_info.get_schema();
    let columns = schema.columns();
    let obj = row
        .as_object()
        .ok_or_else(|| client_error("Row must be a plain object with column names as keys"))?;

    let mut generic_row = fcore::row::GenericRow::new(columns.len());
    for &idx in target_indices {
        let col = &columns[idx];
        let value = obj.get(col.name()).ok_or_else(|| {
            client_error(format!("Required field '{}' is missing", col.name()))
        })?;
        let datum = json_value_to_datum(value, col.data_type())?;
        generic_row.set_field(idx, datum);
    }
    Ok(generic_row)
}

/// Convert a JSON object to a dense GenericRow for PK lookup.
pub fn json_to_dense_generic_row(
    row: &serde_json::Value,
    table_info: &fcore::metadata::TableInfo,
    target_indices: &[usize],
) -> napi::Result<fcore::row::GenericRow<'static>> {
    let schema = table_info.get_schema();
    let columns = schema.columns();
    let obj = row
        .as_object()
        .ok_or_else(|| client_error("Row must be a plain object with column names as keys"))?;

    let mut generic_row = fcore::row::GenericRow::new(target_indices.len());
    for (dense_idx, &schema_idx) in target_indices.iter().enumerate() {
        let col = &columns[schema_idx];
        let value = obj.get(col.name()).ok_or_else(|| {
            client_error(format!("Required field '{}' is missing", col.name()))
        })?;
        let datum = json_value_to_datum(value, col.data_type())?;
        generic_row.set_field(dense_idx, datum);
    }
    Ok(generic_row)
}

/// Convert an InternalRow to a JSON object.
pub fn internal_row_to_json(
    row: &dyn fcore::row::InternalRow,
    table_info: &fcore::metadata::TableInfo,
) -> napi::Result<serde_json::Value> {
    let schema = table_info.get_schema();
    let columns = schema.columns();
    let mut map = serde_json::Map::new();

    for (i, col) in columns.iter().enumerate() {
        let value = datum_to_json_value(row, i, col.data_type())?;
        map.insert(col.name().to_string(), value);
    }

    Ok(serde_json::Value::Object(map))
}

/// Convert an InternalRow to a JSON object using a specific RowType.
pub fn internal_row_to_json_with_row_type(
    row: &dyn fcore::row::InternalRow,
    row_type: &fcore::metadata::RowType,
) -> napi::Result<serde_json::Value> {
    let fields = row_type.fields();
    let mut map = serde_json::Map::new();

    for (i, field) in fields.iter().enumerate() {
        let value = datum_to_json_value(row, i, field.data_type())?;
        map.insert(field.name().to_string(), value);
    }

    Ok(serde_json::Value::Object(map))
}
