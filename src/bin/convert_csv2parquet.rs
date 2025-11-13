use arrow::array::{ArrayRef, StringArray};
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

fn read_csv_data(input_file: &str) -> Result<(Vec<String>, Vec<String>, Vec<String>, Vec<String>), Box<dyn Error>> {
    info!("Reading CSV file: {}", input_file);
    
    let file = File::open(input_file)?;
    let mut csv_reader = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(file);

    let mut first_names = Vec::new();
    let mut last_names = Vec::new();
    let mut citizen_ids = Vec::new();
    let mut addresses = Vec::new();

    let mut count = 0;
    for result in csv_reader.records() {
        let record = result?;
        first_names.push(record[0].to_string());
        last_names.push(record[1].to_string());
        citizen_ids.push(record[2].to_string());
        addresses.push(record[3].to_string());

        count += 1;
        if count % 50_000 == 0 {
            info!("Processed {} rows...", count);
        }
    }

    info!("Total rows read: {}", count);
    Ok((first_names, last_names, citizen_ids, addresses))
}

fn get_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("first_name", DataType::Utf8, false),
        Field::new("last_name", DataType::Utf8, false),
        Field::new("citizen_id", DataType::Utf8, false),
        Field::new("address", DataType::Utf8, false),
    ]))
}

fn write_single_parquet(
    output_file: &str,
    first_names: Vec<String>,
    last_names: Vec<String>,
    citizen_ids: Vec<String>,
    addresses: Vec<String>,
) -> Result<(), Box<dyn Error>> {
    info!("Writing single Parquet file: {}", output_file);
    
    let schema = get_schema();
    let total_rows = first_names.len();

    // Create Arrow arrays
    let first_name_array: ArrayRef = Arc::new(StringArray::from(first_names));
    let last_name_array: ArrayRef = Arc::new(StringArray::from(last_names));
    let citizen_id_array: ArrayRef = Arc::new(StringArray::from(citizen_ids));
    let address_array: ArrayRef = Arc::new(StringArray::from(addresses));

    // Create record batch
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![first_name_array, last_name_array, citizen_id_array, address_array],
    )?;

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
    first_names: Vec<String>,
    last_names: Vec<String>,
    citizen_ids: Vec<String>,
    addresses: Vec<String>,
    num_partitions: usize,
) -> Result<(), Box<dyn Error>> {
    info!("Writing partitioned Parquet files to: {}", output_dir);
    info!("Number of partitions: {}", num_partitions);
    
    // Create output directory
    fs::create_dir_all(output_dir)?;
    
    let schema = get_schema();
    let total_rows = first_names.len();
    let rows_per_partition = (total_rows + num_partitions - 1) / num_partitions;

    for partition_idx in 0..num_partitions {
        let start_idx = partition_idx * rows_per_partition;
        let end_idx = ((partition_idx + 1) * rows_per_partition).min(total_rows);
        
        if start_idx >= total_rows {
            break;
        }

        info!("Writing partition {} (rows {}-{})...", partition_idx, start_idx, end_idx - 1);

        // Slice data for this partition
        let partition_first_names: Vec<String> = first_names[start_idx..end_idx].to_vec();
        let partition_last_names: Vec<String> = last_names[start_idx..end_idx].to_vec();
        let partition_citizen_ids: Vec<String> = citizen_ids[start_idx..end_idx].to_vec();
        let partition_addresses: Vec<String> = addresses[start_idx..end_idx].to_vec();

        // Create Arrow arrays
        let first_name_array: ArrayRef = Arc::new(StringArray::from(partition_first_names));
        let last_name_array: ArrayRef = Arc::new(StringArray::from(partition_last_names));
        let citizen_id_array: ArrayRef = Arc::new(StringArray::from(partition_citizen_ids));
        let address_array: ArrayRef = Arc::new(StringArray::from(partition_addresses));

        // Create record batch
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![first_name_array, last_name_array, citizen_id_array, address_array],
        )?;

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

fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init();

    let input_file = "mock_data.csv";
    let single_output = "mock_data.parquet";
    let partitioned_output = "mock_data_partitioned";
    let num_partitions = 4;

    info!("Starting CSV to Parquet conversion...");
    info!("Reading from: {}", input_file);

    // Read CSV data once
    let (first_names, last_names, citizen_ids, addresses) = read_csv_data(input_file)?;

    // Write single file
    info!("\n=== Creating Single Parquet File ===");
    write_single_parquet(
        single_output,
        first_names.clone(),
        last_names.clone(),
        citizen_ids.clone(),
        addresses.clone(),
    )?;

    // Write partitioned files
    info!("\n=== Creating Partitioned Parquet Files ===");
    write_partitioned_parquet(
        partitioned_output,
        first_names,
        last_names,
        citizen_ids,
        addresses,
        num_partitions,
    )?;

    info!("\n=== Conversion Complete ===");
    info!("Single file: {}", single_output);
    info!("Partitioned files: {}/part-*.parquet", partitioned_output);

    Ok(())
}