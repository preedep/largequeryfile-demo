use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use csv::ReaderBuilder;
use log::info;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::error::Error;
use std::fs::File;
use std::sync::Arc;

fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init();

    let input_file = "mock_data.csv";
    let output_file = "mock_data.parquet";

    info!("Starting CSV to Parquet conversion...");
    info!("Reading from: {}", input_file);
    info!("Writing to: {}", output_file);

    // Define schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("first_name", DataType::Utf8, false),
        Field::new("last_name", DataType::Utf8, false),
        Field::new("citizen_id", DataType::Utf8, false),
        Field::new("address", DataType::Utf8, false),
    ]));

    // Read CSV file
    let file = File::open(input_file)?;
    let mut csv_reader = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(file);

    // Collect data
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

    info!("Writing Parquet file with Snappy compression...");

    // Write to Parquet with compression
    let file = File::create(output_file)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    
    writer.write(&batch)?;
    writer.close()?;

    info!("Successfully converted {} to {}", input_file, output_file);
    info!("Total rows written: {}", count);

    Ok(())
}