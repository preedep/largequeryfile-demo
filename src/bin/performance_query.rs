use arrow::array::{Array, AsArray};
use csv::ReaderBuilder;
use log::info;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rayon::prelude::*;
use std::error::Error;
use std::fs::{self, File};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
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
    let num_rows = batch.num_rows();
    let num_cols = batch.num_columns();
    
    // Count rows where pattern appears in ANY column (not sum of all columns)
    (0..num_rows)
        .into_par_iter()
        .filter(|&row_idx| {
            // Check if this row has the pattern in any column
            for col_idx in 0..num_cols {
                let column = batch.column(col_idx);
                if let Some(string_array) = column.as_string_opt::<i32>() {
                    if !string_array.is_null(row_idx) && string_array.value(row_idx).contains(search_pattern) {
                        return true; // Found in this row, count it once
                    }
                }
            }
            false
        })
        .count()
}

// Removed - no longer needed

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

// Optimized version with tuning
fn query_parquet_optimized(file_path: &str, search_pattern: &str) -> Result<QueryResult, Box<dyn Error>> {
    info!("Starting Optimized Parquet query...");
    let start = Instant::now();

    let file = File::open(file_path)?;
    let file_size_mb = file.metadata()?.len() as f64 / 1_024_000.0;

    // Optimization 1: Larger batch size for better throughput
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder
        .with_batch_size(8192) // Increase from default 1024
        .build()?;

    let total_rows = AtomicUsize::new(0);
    let matching_rows = AtomicUsize::new(0);

    // Optimization 2: Collect batches first, then process in parallel
    let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>()?;
    
    // Optimization 3: Parallel batch processing
    batches.par_iter().for_each(|batch| {
        total_rows.fetch_add(batch.num_rows(), Ordering::Relaxed);
        let matches = search_in_batch(batch, search_pattern);
        matching_rows.fetch_add(matches, Ordering::Relaxed);
    });

    let duration_ms = start.elapsed().as_millis();

    Ok(QueryResult {
        total_rows: total_rows.load(Ordering::Relaxed),
        matching_rows: matching_rows.load(Ordering::Relaxed),
        duration_ms,
        file_size_mb,
    })
}

// Query partitioned Parquet files
fn query_parquet_partitioned(partition_dir: &str, search_pattern: &str) -> Result<QueryResult, Box<dyn Error>> {
    info!("Starting Partitioned Parquet query...");
    let start = Instant::now();

    // Get all partition files
    let partition_path = Path::new(partition_dir);
    if !partition_path.exists() {
        return Err(format!("Partition directory not found: {}", partition_dir).into());
    }

    let mut partition_files: Vec<_> = fs::read_dir(partition_path)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry.path().extension()
                .and_then(|s| s.to_str())
                .map(|s| s == "parquet")
                .unwrap_or(false)
        })
        .map(|entry| entry.path())
        .collect();

    partition_files.sort();
    
    let num_partitions = partition_files.len();
    info!("Found {} partition files", num_partitions);

    // Calculate total file size
    let total_file_size: u64 = partition_files.iter()
        .filter_map(|path| fs::metadata(path).ok())
        .map(|metadata| metadata.len())
        .sum();
    let file_size_mb = total_file_size as f64 / 1_024_000.0;

    let total_rows = AtomicUsize::new(0);
    let matching_rows = AtomicUsize::new(0);

    // Process all partitions in parallel
    partition_files.par_iter().for_each(|partition_file| {
        if let Ok(file) = File::open(partition_file) {
            if let Ok(builder) = ParquetRecordBatchReaderBuilder::try_new(file) {
                if let Ok(reader) = builder.with_batch_size(8192).build() {
                    // Process all batches in this partition
                    if let Ok(batches) = reader.collect::<Result<Vec<_>, _>>() {
                        batches.iter().for_each(|batch| {
                            total_rows.fetch_add(batch.num_rows(), Ordering::Relaxed);
                            let matches = search_in_batch(batch, search_pattern);
                            matching_rows.fetch_add(matches, Ordering::Relaxed);
                        });
                    }
                }
            }
        }
    });

    let duration_ms = start.elapsed().as_millis();

    Ok(QueryResult {
        total_rows: total_rows.load(Ordering::Relaxed),
        matching_rows: matching_rows.load(Ordering::Relaxed),
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
    let parquet_partitioned_dir = "mock_data_partitioned";
    let search_pattern = "Smith"; // Search for common last name

    info!("\n🚀 Starting Performance Query Test...\n");

    // Query CSV
    let csv_result = query_csv(csv_file, search_pattern)?;
    info!("CSV query completed in {} ms", csv_result.duration_ms);

    // Query Parquet (Standard)
    let parquet_result = query_parquet(parquet_file, search_pattern)?;
    info!("Parquet query completed in {} ms", parquet_result.duration_ms);

    // Query Parquet (Optimized)
    let parquet_optimized_result = query_parquet_optimized(parquet_file, search_pattern)?;
    info!("Parquet OPTIMIZED query completed in {} ms", parquet_optimized_result.duration_ms);

    // Query Parquet (Partitioned)
    let parquet_partitioned_result = query_parquet_partitioned(parquet_partitioned_dir, search_pattern)?;
    info!("Parquet PARTITIONED query completed in {} ms", parquet_partitioned_result.duration_ms);

    // Print comparison report
    print_report(&csv_result, &parquet_result, search_pattern);

    info!("\n{}", "=".repeat(80));
    info!("🚀 OPTIMIZED PARQUET RESULTS");
    info!("{}", "=".repeat(80));
    info!("  Query Time:         {} ms", parquet_optimized_result.duration_ms);
    info!("  vs Standard:        {:.2}x faster", 
          parquet_result.duration_ms as f64 / parquet_optimized_result.duration_ms as f64);
    info!("  vs CSV:             {:.2}x {}", 
          if csv_result.duration_ms > parquet_optimized_result.duration_ms {
              csv_result.duration_ms as f64 / parquet_optimized_result.duration_ms as f64
          } else {
              parquet_optimized_result.duration_ms as f64 / csv_result.duration_ms as f64
          },
          if csv_result.duration_ms > parquet_optimized_result.duration_ms { "FASTER" } else { "SLOWER" });
    info!("{}", "=".repeat(80));

    info!("\n{}", "=".repeat(80));
    info!("📂 PARTITIONED PARQUET RESULTS");
    info!("{}", "=".repeat(80));
    info!("  File Size:          {:.2} MB", parquet_partitioned_result.file_size_mb);
    info!("  Total Rows:         {}", parquet_partitioned_result.total_rows);
    info!("  Matching Rows:      {}", parquet_partitioned_result.matching_rows);
    info!("  Query Time:         {} ms", parquet_partitioned_result.duration_ms);
    info!("  Throughput:         {:.2} MB/s", 
          parquet_partitioned_result.file_size_mb / (parquet_partitioned_result.duration_ms as f64 / 1000.0));
    info!("\n  Performance:");
    info!("  vs CSV:             {:.2}x {}", 
          if csv_result.duration_ms > parquet_partitioned_result.duration_ms {
              csv_result.duration_ms as f64 / parquet_partitioned_result.duration_ms as f64
          } else {
              parquet_partitioned_result.duration_ms as f64 / csv_result.duration_ms as f64
          },
          if csv_result.duration_ms > parquet_partitioned_result.duration_ms { "FASTER ⚡" } else { "SLOWER" });
    info!("  vs Single Parquet:  {:.2}x faster", 
          parquet_result.duration_ms as f64 / parquet_partitioned_result.duration_ms as f64);
    info!("  vs Optimized:       {:.2}x {}", 
          if parquet_optimized_result.duration_ms > parquet_partitioned_result.duration_ms {
              parquet_optimized_result.duration_ms as f64 / parquet_partitioned_result.duration_ms as f64
          } else {
              parquet_partitioned_result.duration_ms as f64 / parquet_optimized_result.duration_ms as f64
          },
          if parquet_optimized_result.duration_ms > parquet_partitioned_result.duration_ms { "FASTER 🚀" } else { "SLOWER" });
    info!("{}", "=".repeat(80));

    Ok(())
}