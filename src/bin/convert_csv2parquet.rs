use arrow::array::{ArrayRef, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use csv::ReaderBuilder;
use log::info;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::error::Error;
use std::fs::{self, File};
use std::sync::Arc;

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
    let arrays: Vec<ArrayRef> = columns.iter().zip(column_types.iter()).map(|(col, dtype)| {
        match dtype {
            DataType::Int32 => {
                let int_values: Vec<i32> = col.iter()
                    .map(|s| s.parse::<i32>().unwrap_or(0))
                    .collect();
                Arc::new(Int32Array::from(int_values)) as ArrayRef
            },
            _ => Arc::new(StringArray::from(col.clone())) as ArrayRef,
        }
    }).collect();

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
        let arrays: Vec<ArrayRef> = partition_columns.iter().zip(column_types.iter()).map(|(col, dtype)| {
            match dtype {
                DataType::Int32 => {
                    let int_values: Vec<i32> = col.iter()
                        .map(|s| s.parse::<i32>().unwrap_or(0))
                        .collect();
                    Arc::new(Int32Array::from(int_values)) as ArrayRef
                },
                _ => Arc::new(StringArray::from(col.clone())) as ArrayRef,
            }
        }).collect();

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

fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init();

    info!("Starting CSV to Parquet conversion for multiple datasets...");

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
    info!("ALL CONVERSIONS COMPLETE");
    info!("{}", "=".repeat(60));
    info!("\nGenerated files:");
    info!("  - mock_data.parquet");
    info!("  - mock_data_partitioned/part-*.parquet");
    info!("  - mock_data_salary.parquet");
    info!("  - mock_data_salary_partitioned/part-*.parquet");

    Ok(())
}