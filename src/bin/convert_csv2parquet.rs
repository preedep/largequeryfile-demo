use arrow::array::{ArrayRef, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use csv::ReaderBuilder;
use log::info;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde_json::json;
use std::error::Error;
use std::fs::{self, File};
use std::io::Write;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn read_csv_generic(input_file: &str) -> Result<(Vec<Vec<String>>, Vec<String>), Box<dyn Error>> {
    info!("Reading CSV file: {}", input_file);
    
    let file = File::open(input_file)?;
    let mut csv_reader = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(file);

    // Get headers
    let headers = csv_reader.headers()?.iter().map(|h| h.to_string()).collect::<Vec<_>>();
    let num_columns = headers.len();
    
    // Initialize column vectors
    let mut columns: Vec<Vec<String>> = vec![Vec::new(); num_columns];

    let mut count = 0;
    for result in csv_reader.records() {
        let record = result?;
        for (i, field) in record.iter().enumerate() {
            columns[i].push(field.to_string());
        }

        count += 1;
        if count % 50_000 == 0 {
            info!("Processed {} rows...", count);
        }
    }

    info!("Total rows read: {}", count);
    Ok((columns, headers))
}

fn create_schema(headers: &[String], column_types: &[DataType]) -> Arc<Schema> {
    let fields: Vec<Field> = headers
        .iter()
        .zip(column_types.iter())
        .map(|(name, dtype)| Field::new(name, dtype.clone(), false))
        .collect();
    Arc::new(Schema::new(fields))
}

fn infer_column_types(headers: &[String], first_row: &[String]) -> Vec<DataType> {
    headers.iter().zip(first_row.iter()).map(|(header, value)| {
        // Try to parse as integer for salary column
        if header == "salary" {
            if value.parse::<i32>().is_ok() {
                return DataType::Int32;
            }
        }
        DataType::Utf8
    }).collect()
}

fn create_arrow_arrays(columns: &[Vec<String>], column_types: &[DataType]) -> Vec<ArrayRef> {
    columns.iter().zip(column_types.iter()).map(|(col, dtype)| {
        match dtype {
            DataType::Int32 => {
                let int_values: Vec<i32> = col.iter()
                    .map(|s| s.parse::<i32>().unwrap_or(0))
                    .collect();
                Arc::new(Int32Array::from(int_values)) as ArrayRef
            },
            _ => Arc::new(StringArray::from(col.clone())) as ArrayRef,
        }
    }).collect()
}

fn calculate_column_stats(
    columns: &[Vec<String>],
    headers: &[String],
    column_types: &[DataType],
) -> serde_json::Value {
    let num_records = columns[0].len();
    
    let mut min_values = serde_json::Map::new();
    let mut max_values = serde_json::Map::new();
    let mut null_count = serde_json::Map::new();
    
    for (i, (header, dtype)) in headers.iter().zip(column_types.iter()).enumerate() {
        match dtype {
            DataType::Int32 => {
                // For integer columns
                let values: Vec<i32> = columns[i].iter()
                    .map(|s| s.parse::<i32>().unwrap_or(0))
                    .collect();
                
                if let (Some(&min), Some(&max)) = (values.iter().min(), values.iter().max()) {
                    min_values.insert(header.clone(), json!(min));
                    max_values.insert(header.clone(), json!(max));
                }
            },
            _ => {
                // For string columns - lexicographic min/max
                if !columns[i].is_empty() {
                    if let (Some(min), Some(max)) = (
                        columns[i].iter().min(),
                        columns[i].iter().max()
                    ) {
                        min_values.insert(header.clone(), json!(min));
                        max_values.insert(header.clone(), json!(max));
                    }
                }
            }
        }
        
        // Count nulls (currently always 0 since we don't have nullable data)
        null_count.insert(header.clone(), json!(0));
    }
    
    json!({
        "numRecords": num_records,
        "minValues": min_values,
        "maxValues": max_values,
        "nullCount": null_count
    })
}

fn write_single_parquet(
    output_file: &str,
    columns: Vec<Vec<String>>,
    headers: Vec<String>,
) -> Result<(), Box<dyn Error>> {
    info!("Writing single Parquet file: {}", output_file);
    
    if columns.is_empty() {
        return Err("No data to write".into());
    }
    
    let total_rows = columns[0].len();
    
    // Infer column types from first row
    let first_row: Vec<String> = columns.iter().map(|col| col[0].clone()).collect();
    let column_types = infer_column_types(&headers, &first_row);
    let schema = create_schema(&headers, &column_types);

    // Create Arrow arrays based on inferred types
    let arrays = create_arrow_arrays(&columns, &column_types);

    // Create record batch
    let batch = RecordBatch::try_new(schema.clone(), arrays)?;

    // Write to Parquet with compression
    let file = File::create(output_file)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    
    writer.write(&batch)?;
    writer.close()?;

    info!("Successfully wrote {} rows to {}", total_rows, output_file);
    Ok(())
}

fn write_partitioned_parquet(
    output_dir: &str,
    columns: Vec<Vec<String>>,
    headers: Vec<String>,
    num_partitions: usize,
) -> Result<(), Box<dyn Error>> {
    info!("Writing partitioned Parquet files to: {}", output_dir);
    info!("Number of partitions: {}", num_partitions);
    
    if columns.is_empty() {
        return Err("No data to write".into());
    }
    
    // Create output directory
    fs::create_dir_all(output_dir)?;
    
    let total_rows = columns[0].len();
    let rows_per_partition = (total_rows + num_partitions - 1) / num_partitions;
    
    // Infer column types from first row
    let first_row: Vec<String> = columns.iter().map(|col| col[0].clone()).collect();
    let column_types = infer_column_types(&headers, &first_row);
    let schema = create_schema(&headers, &column_types);

    for partition_idx in 0..num_partitions {
        let start_idx = partition_idx * rows_per_partition;
        let end_idx = ((partition_idx + 1) * rows_per_partition).min(total_rows);
        
        if start_idx >= total_rows {
            break;
        }

        info!("Writing partition {} (rows {}-{})...", partition_idx, start_idx, end_idx - 1);

        // Slice data for this partition
        let partition_columns: Vec<Vec<String>> = columns.iter()
            .map(|col| col[start_idx..end_idx].to_vec())
            .collect();

        // Create Arrow arrays based on inferred types
        let arrays = create_arrow_arrays(&partition_columns, &column_types);

        // Create record batch
        let batch = RecordBatch::try_new(schema.clone(), arrays)?;

        // Write partition file
        let partition_file = format!("{}/part-{:04}.parquet", output_dir, partition_idx);
        let file = File::create(&partition_file)?;
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;
        
        writer.write(&batch)?;
        writer.close()?;

        info!("Wrote {} rows to {}", end_idx - start_idx, partition_file);
    }

    info!("Successfully wrote {} total rows to {} partitions", total_rows, num_partitions);
    Ok(())
}

fn convert_csv_to_parquet(
    input_file: &str,
    single_output: &str,
    partitioned_output: &str,
    num_partitions: usize,
) -> Result<(), Box<dyn Error>> {
    info!("\n{}", "=".repeat(60));
    info!("Processing: {}", input_file);
    info!("{}", "=".repeat(60));

    // Read CSV data once
    let (columns, headers) = read_csv_generic(input_file)?;

    // Write single file
    info!("\n=== Creating Single Parquet File ===");
    write_single_parquet(
        single_output,
        columns.clone(),
        headers.clone(),
    )?;

    // Write partitioned files
    info!("\n=== Creating Partitioned Parquet Files ===");
    write_partitioned_parquet(
        partitioned_output,
        columns,
        headers,
        num_partitions,
    )?;

    info!("\n=== Conversion Complete for {} ===", input_file);
    info!("Single file: {}", single_output);
    info!("Partitioned files: {}/part-*.parquet", partitioned_output);

    Ok(())
}

// Convert CSV to Delta Lake format (manual implementation)
fn convert_csv_to_delta(
    input_file: &str,
    delta_table_path: &str,
) -> Result<(), Box<dyn Error>> {
    info!("\n{}", "=".repeat(60));
    info!("Converting to Delta Lake: {}", input_file);
    info!("{}", "=".repeat(60));

    // Read CSV data
    let (columns, headers) = read_csv_generic(input_file)?;
    
    if columns.is_empty() {
        return Err("No data to convert".into());
    }

    let total_rows = columns[0].len();
    info!("Total rows to convert: {}", total_rows);

    // Infer column types
    let first_row: Vec<String> = columns.iter().map(|col| col[0].clone()).collect();
    let column_types = infer_column_types(&headers, &first_row);

    // Create Arrow schema
    let arrow_schema = create_schema(&headers, &column_types);

    // Create Delta table directory structure
    fs::create_dir_all(delta_table_path)?;
    let delta_log_path = format!("{}/_delta_log", delta_table_path);
    fs::create_dir_all(&delta_log_path)?;

    info!("Creating Delta table at: {}", delta_table_path);
    info!("Writing data to Parquet files...");

    // Create Arrow arrays
    let arrays = create_arrow_arrays(&columns, &column_types);

    // Create record batch
    let batch = RecordBatch::try_new(arrow_schema.clone(), arrays)?;

    // Write Parquet file
    let parquet_file_name = "part-00000-data.snappy.parquet";
    let parquet_path = format!("{}/{}", delta_table_path, parquet_file_name);
    let file = File::create(&parquet_path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    let file_size = fs::metadata(&parquet_path)?.len();
    info!("Wrote Parquet file: {} ({} bytes)", parquet_file_name, file_size);

    // Create Delta Log transaction files
    create_delta_log_files(
        &delta_log_path,
        input_file,
        &columns,
        &headers,
        &column_types,
        parquet_file_name,
        file_size,
        total_rows,
    )?;

    info!("Successfully created Delta table with {} rows", total_rows);
    info!("Delta table location: {}", delta_table_path);
    info!("Transaction log: {}/_delta_log/", delta_table_path);

    // Display Delta table info
    display_delta_table_info(delta_table_path, &headers, total_rows)?;

    Ok(())
}

// Convert CSV to Delta Lake format with partitioning
fn convert_csv_to_delta_partitioned(
    input_file: &str,
    delta_table_path: &str,
    num_partitions: usize,
) -> Result<(), Box<dyn Error>> {
    info!("\n{}", "=".repeat(60));
    info!("Converting to Delta Lake (Partitioned): {}", input_file);
    info!("{}", "=".repeat(60));

    // Read CSV data
    let (columns, headers) = read_csv_generic(input_file)?;
    
    if columns.is_empty() {
        return Err("No data to convert".into());
    }

    let total_rows = columns[0].len();
    info!("Total rows to convert: {}", total_rows);
    info!("Number of partitions: {}", num_partitions);

    // Infer column types
    let first_row: Vec<String> = columns.iter().map(|col| col[0].clone()).collect();
    let column_types = infer_column_types(&headers, &first_row);

    // Create Arrow schema
    let arrow_schema = create_schema(&headers, &column_types);

    // Create Delta table directory structure
    fs::create_dir_all(delta_table_path)?;
    let delta_log_path = format!("{}/_delta_log", delta_table_path);
    fs::create_dir_all(&delta_log_path)?;

    info!("Creating Delta table at: {}", delta_table_path);
    info!("Writing data to {} Parquet partitions...", num_partitions);

    let rows_per_partition = (total_rows + num_partitions - 1) / num_partitions;
    let mut partition_files = Vec::new();
    let mut total_size = 0u64;

    // Write partitioned Parquet files
    for partition_idx in 0..num_partitions {
        let start_idx = partition_idx * rows_per_partition;
        let end_idx = ((partition_idx + 1) * rows_per_partition).min(total_rows);
        
        if start_idx >= total_rows {
            break;
        }

        info!("Writing partition {} (rows {}-{})...", partition_idx, start_idx, end_idx - 1);

        // Slice data for this partition
        let partition_columns: Vec<Vec<String>> = columns.iter()
            .map(|col| col[start_idx..end_idx].to_vec())
            .collect();

        // Create Arrow arrays
        let arrays = create_arrow_arrays(&partition_columns, &column_types);

        // Create record batch
        let batch = RecordBatch::try_new(arrow_schema.clone(), arrays)?;

        // Write partition file
        let parquet_file_name = format!("part-{:04}-data.snappy.parquet", partition_idx);
        let parquet_path = format!("{}/{}", delta_table_path, parquet_file_name);
        let file = File::create(&parquet_path)?;
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        let file_size = fs::metadata(&parquet_path)?.len();
        total_size += file_size;
        partition_files.push((parquet_file_name, file_size, partition_columns));
        
        info!("  Wrote {} ({} bytes, {} rows)", 
              partition_files.last().unwrap().0,
              file_size,
              end_idx - start_idx);
    }

    // Create Delta Log transaction files for partitioned data
    create_delta_log_files_partitioned(
        &delta_log_path,
        input_file,
        &headers,
        &column_types,
        &partition_files,
        total_rows,
    )?;

    info!("Successfully created Delta table with {} rows in {} partitions", 
          total_rows, partition_files.len());
    info!("Delta table location: {}", delta_table_path);
    info!("Transaction log: {}/_delta_log/", delta_table_path);
    info!("Total size: {} bytes", total_size);

    // Display Delta table info
    display_delta_table_info(delta_table_path, &headers, total_rows)?;

    Ok(())
}

// Create Delta Log transaction files manually
fn create_delta_log_files(
    delta_log_path: &str,
    table_name: &str,
    columns: &[Vec<String>],
    headers: &[String],
    column_types: &[DataType],
    parquet_file: &str,
    file_size: u64,
    num_records: usize,
) -> Result<(), Box<dyn Error>> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis() as i64;

    // Transaction 0: Protocol + Metadata (CREATE TABLE)
    let protocol = json!({
        "protocol": {
            "minReaderVersion": 1,
            "minWriterVersion": 2
        }
    });

    let schema_fields: Vec<serde_json::Value> = headers
        .iter()
        .zip(column_types.iter())
        .map(|(name, dtype)| {
            let type_str = match dtype {
                DataType::Int32 => "integer",
                DataType::Int64 => "long",
                DataType::Float32 => "float",
                DataType::Float64 => "double",
                DataType::Boolean => "boolean",
                _ => "string",
            };
            json!({
                "name": name,
                "type": type_str,
                "nullable": false,
                "metadata": {}
            })
        })
        .collect();

    let metadata = json!({
        "metaData": {
            "id": format!("delta-{}", uuid::Uuid::new_v4()),
            "name": table_name.trim_end_matches(".csv"),
            "description": format!("Delta table created from {}", table_name),
            "format": {
                "provider": "parquet",
                "options": {}
            },
            "schemaString": json!({
                "type": "struct",
                "fields": schema_fields
            }).to_string(),
            "partitionColumns": [],
            "configuration": {
                "delta.enableChangeDataFeed": "true",
                "delta.checkpoint.writeStatsAsJson": "true"
            },
            "createdTime": timestamp
        }
    });

    let txn0_path = format!("{}/00000000000000000000.json", delta_log_path);
    let mut txn0_file = File::create(&txn0_path)?;
    writeln!(txn0_file, "{}", serde_json::to_string_pretty(&protocol)?)?;
    writeln!(txn0_file, "{}", serde_json::to_string_pretty(&metadata)?)?;
    info!("Created transaction log: 00000000000000000000.json (CREATE TABLE)");

    // Transaction 1: Add file (INSERT)
    let commit_info = json!({
        "commitInfo": {
            "timestamp": timestamp + 1000,
            "operation": "WRITE",
            "operationParameters": {
                "mode": "Append",
                "partitionBy": "[]"
            },
            "readVersion": 0,
            "isolationLevel": "Serializable",
            "isBlindAppend": true,
            "operationMetrics": {
                "numFiles": "1",
                "numOutputRows": num_records.to_string(),
                "numOutputBytes": file_size.to_string()
            },
            "engineInfo": "Rust-Manual-Delta-1.0"
        }
    });

    let add_file = json!({
        "add": {
            "path": parquet_file,
            "partitionValues": {},
            "size": file_size,
            "modificationTime": timestamp + 1000,
            "dataChange": true,
            "stats": calculate_column_stats(columns, headers, column_types).to_string()
        }
    });

    let txn1_path = format!("{}/00000000000000000001.json", delta_log_path);
    let mut txn1_file = File::create(&txn1_path)?;
    writeln!(txn1_file, "{}", serde_json::to_string_pretty(&commit_info)?)?;
    writeln!(txn1_file, "{}", serde_json::to_string_pretty(&add_file)?)?;
    info!("Created transaction log: 00000000000000000001.json (INSERT)");

    Ok(())
}

// Create Delta Log transaction files for partitioned data
fn create_delta_log_files_partitioned(
    delta_log_path: &str,
    table_name: &str,
    headers: &[String],
    column_types: &[DataType],
    partition_files: &[(String, u64, Vec<Vec<String>>)], // (filename, size, partition_columns)
    total_records: usize,
) -> Result<(), Box<dyn Error>> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis() as i64;

    // Transaction 0: Protocol + Metadata (CREATE TABLE)
    let protocol = json!({
        "protocol": {
            "minReaderVersion": 1,
            "minWriterVersion": 2
        }
    });

    let schema_fields: Vec<serde_json::Value> = headers
        .iter()
        .zip(column_types.iter())
        .map(|(name, dtype)| {
            let type_str = match dtype {
                DataType::Int32 => "integer",
                DataType::Int64 => "long",
                DataType::Float32 => "float",
                DataType::Float64 => "double",
                DataType::Boolean => "boolean",
                _ => "string",
            };
            json!({
                "name": name,
                "type": type_str,
                "nullable": false,
                "metadata": {}
            })
        })
        .collect();

    let metadata = json!({
        "metaData": {
            "id": format!("delta-{}", uuid::Uuid::new_v4()),
            "name": table_name.trim_end_matches(".csv"),
            "description": format!("Delta table (partitioned) created from {}", table_name),
            "format": {
                "provider": "parquet",
                "options": {}
            },
            "schemaString": json!({
                "type": "struct",
                "fields": schema_fields
            }).to_string(),
            "partitionColumns": [],
            "configuration": {
                "delta.enableChangeDataFeed": "true",
                "delta.checkpoint.writeStatsAsJson": "true"
            },
            "createdTime": timestamp
        }
    });

    let txn0_path = format!("{}/00000000000000000000.json", delta_log_path);
    let mut txn0_file = File::create(&txn0_path)?;
    writeln!(txn0_file, "{}", serde_json::to_string_pretty(&protocol)?)?;
    writeln!(txn0_file, "{}", serde_json::to_string_pretty(&metadata)?)?;
    info!("Created transaction log: 00000000000000000000.json (CREATE TABLE)");

    // Transaction 1: Add files (INSERT) - multiple files
    let total_size: u64 = partition_files.iter().map(|(_, size, _)| size).sum();
    
    let commit_info = json!({
        "commitInfo": {
            "timestamp": timestamp + 1000,
            "operation": "WRITE",
            "operationParameters": {
                "mode": "Append",
                "partitionBy": "[]"
            },
            "readVersion": 0,
            "isolationLevel": "Serializable",
            "isBlindAppend": true,
            "operationMetrics": {
                "numFiles": partition_files.len().to_string(),
                "numOutputRows": total_records.to_string(),
                "numOutputBytes": total_size.to_string()
            },
            "engineInfo": "Rust-Manual-Delta-1.0"
        }
    });

    let txn1_path = format!("{}/00000000000000000001.json", delta_log_path);
    let mut txn1_file = File::create(&txn1_path)?;
    writeln!(txn1_file, "{}", serde_json::to_string_pretty(&commit_info)?)?;
    
    // Write add action for each partition file
    for (filename, file_size, partition_columns) in partition_files {
        let stats = calculate_column_stats(partition_columns, headers, column_types);
        let add_file = json!({
            "add": {
                "path": filename,
                "partitionValues": {},
                "size": file_size,
                "modificationTime": timestamp + 1000,
                "dataChange": true,
                "stats": stats.to_string()
            }
        });
        writeln!(txn1_file, "{}", serde_json::to_string_pretty(&add_file)?)?;
    }
    
    info!("Created transaction log: 00000000000000000001.json (INSERT {} files)", partition_files.len());

    Ok(())
}

// Display Delta table information
fn display_delta_table_info(
    delta_table_path: &str,
    headers: &[String],
    total_rows: usize,
) -> Result<(), Box<dyn Error>> {
    info!("\n{}", "=".repeat(60));
    info!("DELTA TABLE INFORMATION");
    info!("{}", "=".repeat(60));
    
    info!("Version: 1");
    info!("Schema: {:?}", headers);
    info!("Total rows: {}", total_rows);
    
    // List data files
    let data_files: Vec<_> = fs::read_dir(delta_table_path)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry.path().extension()
                .and_then(|s| s.to_str())
                .map(|s| s == "parquet")
                .unwrap_or(false)
        })
        .collect();
    
    info!("Number of data files: {}", data_files.len());
    if !data_files.is_empty() {
        info!("\nData files:");
        for (i, entry) in data_files.iter().enumerate() {
            let metadata = entry.metadata()?;
            info!("  {}. {} ({} bytes)", 
                  i + 1, 
                  entry.file_name().to_string_lossy(),
                  metadata.len());
        }
    }

    // List transaction log files
    let log_path = format!("{}/_delta_log", delta_table_path);
    let log_files: Vec<_> = fs::read_dir(&log_path)?
        .filter_map(|entry| entry.ok())
        .collect();
    
    info!("\nTransaction log files:");
    for entry in log_files {
        info!("  - {}", entry.file_name().to_string_lossy());
    }

    info!("\nConfiguration:");
    info!("  delta.enableChangeDataFeed: true");
    info!("  delta.checkpoint.writeStatsAsJson: true");

    info!("{}", "=".repeat(60));
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init();

    info!("Starting CSV to Parquet and Delta Lake conversion for multiple datasets...");

    // ========== PARQUET CONVERSION ==========
    info!("\n{}", "#".repeat(60));
    info!("PHASE 1: PARQUET CONVERSION");
    info!("{}", "#".repeat(60));

    // Dataset 1: mock_data.csv
    convert_csv_to_parquet(
        "mock_data.csv",
        "mock_data.parquet",
        "mock_data_partitioned",
        4,
    )?;

    // Dataset 2: mock_data_salary.csv
    convert_csv_to_parquet(
        "mock_data_salary.csv",
        "mock_data_salary.parquet",
        "mock_data_salary_partitioned",
        4,
    )?;

    info!("\n{}", "=".repeat(60));
    info!("PARQUET CONVERSIONS COMPLETE");
    info!("{}", "=".repeat(60));
    info!("\nGenerated Parquet files:");
    info!("  - mock_data.parquet");
    info!("  - mock_data_partitioned/part-*.parquet");
    info!("  - mock_data_salary.parquet");
    info!("  - mock_data_salary_partitioned/part-*.parquet");

    // ========== DELTA LAKE CONVERSION (SINGLE FILE) ==========
    info!("\n{}", "#".repeat(60));
    info!("PHASE 2: DELTA LAKE CONVERSION (SINGLE FILE)");
    info!("{}", "#".repeat(60));

    // Dataset 1: mock_data.csv -> Delta Lake
    convert_csv_to_delta(
        "mock_data.csv",
        "mock_data_delta",
    )?;

    // Dataset 2: mock_data_salary.csv -> Delta Lake
    convert_csv_to_delta(
        "mock_data_salary.csv",
        "mock_data_salary_delta",
    )?;

    info!("\n{}", "=".repeat(60));
    info!("DELTA LAKE (SINGLE FILE) CONVERSIONS COMPLETE");
    info!("{}", "=".repeat(60));

    // ========== DELTA LAKE CONVERSION (PARTITIONED) ==========
    info!("\n{}", "#".repeat(60));
    info!("PHASE 3: DELTA LAKE CONVERSION (PARTITIONED)");
    info!("{}", "#".repeat(60));

    // Dataset 1: mock_data.csv -> Delta Lake Partitioned
    convert_csv_to_delta_partitioned(
        "mock_data.csv",
        "mock_data_delta_partitioned",
        4,
    )?;

    // Dataset 2: mock_data_salary.csv -> Delta Lake Partitioned
    convert_csv_to_delta_partitioned(
        "mock_data_salary.csv",
        "mock_data_salary_delta_partitioned",
        4,
    )?;

    info!("\n{}", "=".repeat(60));
    info!("DELTA LAKE (PARTITIONED) CONVERSIONS COMPLETE");
    info!("{}", "=".repeat(60));

    info!("\n{}", "=".repeat(60));
    info!("ALL CONVERSIONS COMPLETE!");
    info!("{}", "=".repeat(60));
    info!("\nGenerated files:");
    info!("\n📦 Parquet:");
    info!("  - mock_data.parquet");
    info!("  - mock_data_partitioned/ (4 partitions)");
    info!("  - mock_data_salary.parquet");
    info!("  - mock_data_salary_partitioned/ (4 partitions)");
    info!("\n🔺 Delta Lake (Single):");
    info!("  - mock_data_delta/");
    info!("    ├── _delta_log/ (2 transactions)");
    info!("    └── part-00000-data.snappy.parquet");
    info!("  - mock_data_salary_delta/");
    info!("    ├── _delta_log/ (2 transactions)");
    info!("    └── part-00000-data.snappy.parquet");
    info!("\n🔺 Delta Lake (Partitioned):");
    info!("  - mock_data_delta_partitioned/");
    info!("    ├── _delta_log/ (2 transactions)");
    info!("    └── part-0000 to part-0003 (4 files)");
    info!("  - mock_data_salary_delta_partitioned/");
    info!("    ├── _delta_log/ (2 transactions)");
    info!("    └── part-0000 to part-0003 (4 files)");
    info!("\n✨ You can now:");
    info!("  1. Query Parquet files directly");
    info!("  2. Query Delta Lake tables (single or partitioned)");
    info!("  3. Compare performance between single vs partitioned");
    info!("  4. Set up Delta Sharing for remote access");
    info!("  5. Run performance benchmarks");

    Ok(())
}