use arrow::array::{Array, AsArray, GenericByteArray};
use arrow::datatypes::GenericStringType;
use csv::ReaderBuilder;
use log::info;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::error::Error;
use std::fs::File;
use std::time::Instant;

#[derive(Debug)]
struct QueryResult {
    total_rows: usize,
    matching_rows: usize,
    duration_ms: u128,
    file_size_mb: f64,
}

fn query_csv(file_path: &str, search_pattern: &str) -> Result<QueryResult, Box<dyn Error>> {
    info!("Starting CSV query...");
    let start = Instant::now();

    let file = File::open(file_path)?;
    let file_size_mb = file.metadata()?.len() as f64 / 1_024_000.0;

    let mut csv_reader = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(file);

    let mut total_rows = 0;
    let mut matching_rows = 0;

    for result in csv_reader.records() {
        let record = result?;
        total_rows += 1;

        // Search in all fields
        for field in record.iter() {
            if field.contains(search_pattern) {
                matching_rows += 1;
                break;
            }
        }
    }

    let duration_ms = start.elapsed().as_millis();

    Ok(QueryResult {
        total_rows,
        matching_rows,
        duration_ms,
        file_size_mb,
    })
}

fn search_in_batch(batch: &arrow::record_batch::RecordBatch, search_pattern: &str) -> usize {
    let mut matching_rows = 0;
    let num_rows = batch.num_rows();

    for col_idx in 0..batch.num_columns() {
        let column = batch.column(col_idx);
        if let Some(string_array) = column.as_string_opt::<i32>() {
            matching_rows += count_matches_in_column(string_array, search_pattern, num_rows);
        }
    }

    matching_rows
}

fn count_matches_in_column(
    string_array: &GenericByteArray<GenericStringType<i32>>,
    search_pattern: &str,
    num_rows: usize,
) -> usize {
    let mut matches = 0;

    for row_idx in 0..num_rows {
        if !string_array.is_null(row_idx) && string_array.value(row_idx).contains(search_pattern) {
            matches += 1;
            break;
        }
    }

    matches
}

fn query_parquet(file_path: &str, search_pattern: &str) -> Result<QueryResult, Box<dyn Error>> {
    info!("Starting Parquet query...");
    let start = Instant::now();

    let file = File::open(file_path)?;
    let file_size_mb = file.metadata()?.len() as f64 / 1_024_000.0;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mut reader = builder.build()?;

    let mut total_rows = 0;
    let mut matching_rows = 0;

    while let Some(batch) = reader.next() {
        let batch = batch?;
        total_rows += batch.num_rows();
        matching_rows += search_in_batch(&batch, search_pattern);
    }

    let duration_ms = start.elapsed().as_millis();

    Ok(QueryResult {
        total_rows,
        matching_rows,
        duration_ms,
        file_size_mb,
    })
}

fn print_report(csv_result: &QueryResult, parquet_result: &QueryResult, search_pattern: &str) {
    info!("\n{}", "=".repeat(80));
    info!("📊 PERFORMANCE COMPARISON REPORT");
    info!("{}", "=".repeat(80));
    info!("\n🔍 Search Pattern: \"{}\"", search_pattern);
    
    info!("\n{}", "-".repeat(80));
    info!("📄 CSV FILE RESULTS");
    info!("{}", "-".repeat(80));
    info!("  File Size:      {:.2} MB", csv_result.file_size_mb);
    info!("  Total Rows:     {}", csv_result.total_rows);
    info!("  Matching Rows:  {}", csv_result.matching_rows);
    info!("  Query Time:     {} ms", csv_result.duration_ms);
    info!("  Throughput:     {:.2} MB/s", 
             csv_result.file_size_mb / (csv_result.duration_ms as f64 / 1000.0));

    info!("\n{}", "-".repeat(80));
    info!("📦 PARQUET FILE RESULTS");
    info!("{}", "-".repeat(80));
    info!("  File Size:      {:.2} MB", parquet_result.file_size_mb);
    info!("  Total Rows:     {}", parquet_result.total_rows);
    info!("  Matching Rows:  {}", parquet_result.matching_rows);
    info!("  Query Time:     {} ms", parquet_result.duration_ms);
    info!("  Throughput:     {:.2} MB/s", 
             parquet_result.file_size_mb / (parquet_result.duration_ms as f64 / 1000.0));

    info!("\n{}", "-".repeat(80));
    info!("⚡ PERFORMANCE COMPARISON");
    info!("{}", "-".repeat(80));
    
    let speedup = csv_result.duration_ms as f64 / parquet_result.duration_ms as f64;
    let size_reduction = (1.0 - (parquet_result.file_size_mb / csv_result.file_size_mb)) * 100.0;
    
    info!("  File Size Reduction: {:.1}%", size_reduction);
    info!("  Speed Improvement:   {:.2}x", speedup);
    
    if speedup > 1.0 {
        info!("  Winner:              🏆 Parquet is {:.2}x FASTER!", speedup);
    } else {
        info!("  Winner:              🏆 CSV is {:.2}x FASTER!", 1.0 / speedup);
    }
    
    info!("  Time Saved:          {} ms ({:.1}%)", 
             csv_result.duration_ms.saturating_sub(parquet_result.duration_ms),
             ((csv_result.duration_ms.saturating_sub(parquet_result.duration_ms)) as f64 
              / csv_result.duration_ms as f64) * 100.0);

    info!("\n{}", "=".repeat(80));
}

fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init();

    let csv_file = "mock_data.csv";
    let parquet_file = "mock_data.parquet";
    let search_pattern = "Smith"; // Search for common last name

    info!("\n🚀 Starting Performance Query Test...\n");

    // Query CSV
    let csv_result = query_csv(csv_file, search_pattern)?;
    info!("CSV query completed in {} ms", csv_result.duration_ms);

    // Query Parquet
    let parquet_result = query_parquet(parquet_file, search_pattern)?;
    info!("Parquet query completed in {} ms", parquet_result.duration_ms);

    // Print comparison report
    print_report(&csv_result, &parquet_result, search_pattern);

    Ok(())
}