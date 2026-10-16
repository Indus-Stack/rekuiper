use anyhow::{Context, Result};
use arrow::array::{ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use rekuiper_core::model::StreamRecord;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Infers an Arrow schema and writes a batch of [`StreamRecord`]s to a Parquet file.
/// If the file already exists and its schema matches the incoming batch, existing Arrow
/// [`RecordBatch`]es are copied directly without decoding to [`StreamRecord`] maps;
/// if the schema evolved, existing rows are merged and widened. Writes go to a temporary
/// file and atomically replace `path`.
pub fn write_records_to_parquet(records: &[StreamRecord], path: &Path) -> Result<()> {
    if records.is_empty() {
        return Ok(());
    }

    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create parent directory for {:?}", path))?;
        }
    }

    let new_batch = records_to_record_batch(records)?;

    let batches_to_write: Vec<RecordBatch> = if path.exists()
        && std::fs::metadata(path)
            .map(|m| m.len() > 0)
            .unwrap_or(false)
    {
        match read_raw_record_batches(path) {
            Ok(existing_batches) if !existing_batches.is_empty() => {
                if existing_batches[0].schema() == new_batch.schema() {
                    let mut all = Vec::with_capacity(existing_batches.len() + 1);
                    all.extend(existing_batches);
                    all.push(new_batch);
                    all
                } else {
                    let mut all_records = Vec::new();
                    for b in &existing_batches {
                        all_records.extend(record_batch_to_records(b)?);
                    }
                    all_records.extend(records.iter().cloned());
                    vec![records_to_record_batch(&all_records)?]
                }
            }
            _ => vec![new_batch],
        }
    } else {
        vec![new_batch]
    };

    let schema = batches_to_write[0].schema();
    let tmp_path = path.with_extension(format!("parquet.{}.tmp", uuid::Uuid::new_v4().simple()));

    let write_res = (|| -> Result<()> {
        let file = File::create(&tmp_path)
            .with_context(|| format!("Failed to create temp Parquet file at {:?}", tmp_path))?;

        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::SNAPPY)
            .build();

        let mut writer = ArrowWriter::try_new(file, schema, Some(props))
            .context("Failed to initialize ArrowWriter for Parquet")?;

        for batch in &batches_to_write {
            writer
                .write(batch)
                .context("Failed to write RecordBatch to Parquet file")?;
        }

        writer
            .close()
            .context("Failed to close Parquet ArrowWriter")?;
        Ok(())
    })();

    if let Err(e) = write_res {
        let _ = std::fs::remove_file(&tmp_path);
        return Err(e);
    }

    std::fs::rename(&tmp_path, path).with_context(|| {
        let _ = std::fs::remove_file(&tmp_path);
        format!("Failed to atomically rename {:?} to {:?}", tmp_path, path)
    })?;

    Ok(())
}

/// Offloads [`write_records_to_parquet`] to Tokio's blocking thread pool.
pub async fn write_records_to_parquet_async(
    records: Vec<StreamRecord>,
    path: PathBuf,
) -> Result<()> {
    tokio::task::spawn_blocking(move || write_records_to_parquet(&records, &path))
        .await
        .context("Parquet write blocking task panicked")?
}

/// Appends a single [`StreamRecord`] to the Parquet file at `path`.
pub fn append_record_to_parquet(record: &StreamRecord, path: &Path) -> Result<()> {
    write_records_to_parquet(std::slice::from_ref(record), path)
}

/// Offloads [`append_record_to_parquet`] to Tokio's blocking thread pool.
pub async fn append_record_to_parquet_async(record: StreamRecord, path: PathBuf) -> Result<()> {
    tokio::task::spawn_blocking(move || append_record_to_parquet(&record, &path))
        .await
        .context("Parquet append blocking task panicked")?
}

/// Reads raw Arrow [`RecordBatch`]es from a Parquet file, closing the file handle before returning.
fn read_raw_record_batches(path: &Path) -> Result<Vec<RecordBatch>> {
    let file =
        File::open(path).with_context(|| format!("Failed to open Parquet file at {:?}", path))?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("Failed to read Parquet metadata from {:?}", path))?;

    let reader = builder
        .build()
        .context("Failed to build ParquetRecordBatchReader")?;

    let mut batches = Vec::new();
    for batch_result in reader {
        batches.push(batch_result.context("Failed reading RecordBatch from Parquet")?);
    }
    Ok(batches)
}

/// Reads all [`StreamRecord`] rows from a Parquet file.
pub fn read_parquet_file(path: &Path) -> Result<Vec<StreamRecord>> {
    let batches = read_raw_record_batches(path)?;
    let mut records = Vec::new();
    for batch in &batches {
        records.extend(record_batch_to_records(batch)?);
    }
    Ok(records)
}

/// Offloads [`read_parquet_file`] to Tokio's blocking thread pool.
pub async fn read_parquet_file_async(path: PathBuf) -> Result<Vec<StreamRecord>> {
    tokio::task::spawn_blocking(move || read_parquet_file(&path))
        .await
        .context("Parquet read blocking task panicked")?
}

/// Convert a slice of StreamRecords into an Arrow RecordBatch.
fn records_to_record_batch(records: &[StreamRecord]) -> Result<RecordBatch> {
    // 1. Collect all distinct fields in sorted order for consistent schema
    let mut field_types: BTreeMap<String, DataType> = BTreeMap::new();
    for r in records {
        for (k, v) in &r.data {
            if k == "__meta__" || k.starts_with("__") {
                continue;
            }
            let candidate_type = match v {
                Value::Bool(_) => DataType::Boolean,
                Value::Number(n) if n.is_i64() || n.is_u64() => DataType::Int64,
                Value::Number(_) => DataType::Float64,
                _ => DataType::Utf8,
            };
            field_types
                .entry(k.clone())
                .and_modify(|existing| {
                    // Upgrade integer to float if mixed
                    if *existing == DataType::Int64 && candidate_type == DataType::Float64 {
                        *existing = DataType::Float64;
                    } else if *existing != candidate_type {
                        *existing = DataType::Utf8;
                    }
                })
                .or_insert(candidate_type);
        }
    }

    let mut fields = Vec::with_capacity(field_types.len());
    for (name, dt) in &field_types {
        fields.push(Field::new(name, dt.clone(), true));
    }
    let schema: SchemaRef = Arc::new(Schema::new(fields));

    // 2. Build column arrays
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(field_types.len());
    for (name, dt) in &field_types {
        let array: ArrayRef = match dt {
            DataType::Boolean => {
                let mut builder = BooleanBuilder::with_capacity(records.len());
                for r in records {
                    match r.data.get(name) {
                        Some(Value::Bool(b)) => builder.append_value(*b),
                        _ => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Int64 => {
                let mut builder = Int64Builder::with_capacity(records.len());
                for r in records {
                    match r.data.get(name) {
                        Some(Value::Number(n)) => {
                            if let Some(i) = n.as_i64() {
                                builder.append_value(i);
                            } else if let Some(u) = n.as_u64() {
                                builder.append_value(u as i64);
                            } else {
                                builder.append_null();
                            }
                        }
                        _ => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Float64 => {
                let mut builder = Float64Builder::with_capacity(records.len());
                for r in records {
                    match r.data.get(name) {
                        Some(Value::Number(n)) => {
                            if let Some(f) = n.as_f64() {
                                builder.append_value(f);
                            } else {
                                builder.append_null();
                            }
                        }
                        _ => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            _ => {
                let mut builder = StringBuilder::with_capacity(records.len(), records.len() * 16);
                for r in records {
                    match r.data.get(name) {
                        Some(Value::String(s)) => builder.append_value(s),
                        Some(other) if !other.is_null() => {
                            builder.append_value(serde_json::to_string(other).unwrap_or_default())
                        }
                        _ => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
        };
        columns.push(array);
    }

    RecordBatch::try_new(schema, columns).context("Failed to construct Arrow RecordBatch")
}

/// Convert an Arrow RecordBatch into a Vec of StreamRecords.
fn record_batch_to_records(batch: &RecordBatch) -> Result<Vec<StreamRecord>> {
    let num_rows = batch.num_rows();
    let num_cols = batch.num_columns();
    let schema = batch.schema();
    let mut records = Vec::with_capacity(num_rows);

    for row_idx in 0..num_rows {
        let mut row_data = HashMap::with_capacity(num_cols);
        for col_idx in 0..num_cols {
            let col = batch.column(col_idx);
            let field = schema.field(col_idx);
            let field_name = field.name().clone();

            if col.is_null(row_idx) {
                continue;
            }

            let value = match field.data_type() {
                DataType::Boolean => {
                    use arrow::array::AsArray;
                    let bool_arr = col.as_boolean();
                    Value::Bool(bool_arr.value(row_idx))
                }
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                    use arrow::array::AsArray;
                    let num = match field.data_type() {
                        DataType::Int8 => col
                            .as_primitive::<arrow::datatypes::Int8Type>()
                            .value(row_idx) as i64,
                        DataType::Int16 => col
                            .as_primitive::<arrow::datatypes::Int16Type>()
                            .value(row_idx) as i64,
                        DataType::Int32 => col
                            .as_primitive::<arrow::datatypes::Int32Type>()
                            .value(row_idx) as i64,
                        DataType::Int64 => col
                            .as_primitive::<arrow::datatypes::Int64Type>()
                            .value(row_idx),
                        _ => unreachable!(),
                    };
                    Value::Number(num.into())
                }
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                    use arrow::array::AsArray;
                    let num = match field.data_type() {
                        DataType::UInt8 => col
                            .as_primitive::<arrow::datatypes::UInt8Type>()
                            .value(row_idx) as u64,
                        DataType::UInt16 => col
                            .as_primitive::<arrow::datatypes::UInt16Type>()
                            .value(row_idx) as u64,
                        DataType::UInt32 => col
                            .as_primitive::<arrow::datatypes::UInt32Type>()
                            .value(row_idx) as u64,
                        DataType::UInt64 => col
                            .as_primitive::<arrow::datatypes::UInt64Type>()
                            .value(row_idx),
                        _ => unreachable!(),
                    };
                    Value::Number(num.into())
                }
                DataType::Float32 => {
                    use arrow::array::AsArray;
                    let f = col
                        .as_primitive::<arrow::datatypes::Float32Type>()
                        .value(row_idx);
                    serde_json::Number::from_f64(f as f64)
                        .map(Value::Number)
                        .unwrap_or(Value::Null)
                }
                DataType::Float64 => {
                    use arrow::array::AsArray;
                    let f = col
                        .as_primitive::<arrow::datatypes::Float64Type>()
                        .value(row_idx);
                    serde_json::Number::from_f64(f)
                        .map(Value::Number)
                        .unwrap_or(Value::Null)
                }
                DataType::Utf8 => {
                    use arrow::array::AsArray;
                    let s = col.as_string::<i32>().value(row_idx);
                    if let Ok(v) = serde_json::from_str::<Value>(s) {
                        if v.is_object() || v.is_array() {
                            v
                        } else {
                            Value::String(s.to_string())
                        }
                    } else {
                        Value::String(s.to_string())
                    }
                }
                DataType::LargeUtf8 => {
                    use arrow::array::AsArray;
                    let s = col.as_string::<i64>().value(row_idx);
                    if let Ok(v) = serde_json::from_str::<Value>(s) {
                        if v.is_object() || v.is_array() {
                            v
                        } else {
                            Value::String(s.to_string())
                        }
                    } else {
                        Value::String(s.to_string())
                    }
                }
                _ => {
                    use arrow::array::AsArray;
                    if let Some(s) = col.as_string_opt::<i32>() {
                        Value::String(s.value(row_idx).to_string())
                    } else {
                        Value::Null
                    }
                }
            };

            if !value.is_null() {
                row_data.insert(field_name, value);
            }
        }
        records.push(StreamRecord::new(row_data));
    }

    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parquet_roundtrip() {
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join(format!("test_parquet_{}.parquet", uuid::Uuid::new_v4()));

        let mut rec1 = HashMap::new();
        rec1.insert("device_id".to_string(), Value::String("turbine-01".into()));
        rec1.insert("temp".to_string(), serde_json::json!(23.5));
        rec1.insert("rpm".to_string(), serde_json::json!(1500));
        rec1.insert("active".to_string(), Value::Bool(true));

        let mut rec2 = HashMap::new();
        rec2.insert("device_id".to_string(), Value::String("turbine-02".into()));
        rec2.insert("temp".to_string(), serde_json::json!(28.1));
        rec2.insert("rpm".to_string(), serde_json::json!(1850));
        rec2.insert("active".to_string(), Value::Bool(false));

        let records = vec![StreamRecord::new(rec1), StreamRecord::new(rec2)];

        write_records_to_parquet(&records, &test_file).expect("write parquet failed");

        let read_back = read_parquet_file(&test_file).expect("read parquet failed");
        assert_eq!(read_back.len(), 2);
        assert_eq!(read_back[0].data.get("device_id").unwrap(), "turbine-01");
        assert_eq!(read_back[0].data.get("active").unwrap(), &Value::Bool(true));
        assert_eq!(read_back[1].data.get("device_id").unwrap(), "turbine-02");

        let _ = std::fs::remove_file(&test_file);
    }

    #[test]
    fn test_parquet_filters_internal_meta_keys() {
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join(format!(
            "test_parquet_meta_{}.parquet",
            uuid::Uuid::new_v4()
        ));

        let mut rec = HashMap::new();
        rec.insert("sensor_id".to_string(), Value::String("s-100".into()));
        rec.insert("val".to_string(), serde_json::json!(42.0));
        rec.insert("__meta__".to_string(), serde_json::json!({"topic": "demo"}));
        rec.insert("__internal_batch__".to_string(), Value::Bool(true));

        let records = vec![StreamRecord::new(rec)];
        write_records_to_parquet(&records, &test_file).expect("write parquet failed");

        let read_back = read_parquet_file(&test_file).expect("read parquet failed");
        assert_eq!(read_back.len(), 1);
        assert_eq!(read_back[0].data.get("sensor_id").unwrap(), "s-100");
        assert_eq!(
            read_back[0].data.get("val").unwrap(),
            &serde_json::json!(42.0)
        );
        assert!(read_back[0].data.get("__meta__").is_none());
        assert!(read_back[0].data.get("__internal_batch__").is_none());

        let _ = std::fs::remove_file(&test_file);
    }
}
