use arrow::array::{Array, AsArray};
use csv::ReaderBuilder;
use log::info;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use polars::prelude::*;
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

// Helper: Get sorted list of parquet files from directory
fn get_partition_files(partition_dir: &str) -> Result<Vec<std::path::PathBuf>, Box<dyn Error>> {
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
    Ok(partition_files)
}

// Helper: Calculate total file size
fn calculate_total_file_size(files: &[std::path::PathBuf]) -> f64 {
    let total_bytes: u64 = files.iter()
        .filter_map(|path| fs::metadata(path).ok())
        .map(|metadata| metadata.len())
        .sum();
    total_bytes as f64 / 1_024_000.0
}

// Helper: Process a single partition file
fn process_partition_file(
    partition_file: &std::path::Path,
    search_pattern: &str,
    total_rows: &AtomicUsize,
    matching_rows: &AtomicUsize,
) {
    if let Ok(file) = File::open(partition_file) {
        if let Ok(builder) = ParquetRecordBatchReaderBuilder::try_new(file) {
            if let Ok(reader) = builder.with_batch_size(8192).build() {
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
}

// Query partitioned Parquet files
fn query_parquet_partitioned(partition_dir: &str, search_pattern: &str) -> Result<QueryResult, Box<dyn Error>> {
    info!("Starting Partitioned Parquet query...");
    let start = Instant::now();

    // Get partition files
    let partition_files = get_partition_files(partition_dir)?;
    info!("Found {} partition files", partition_files.len());

    // Calculate total file size
    let file_size_mb = calculate_total_file_size(&partition_files);

    // Process all partitions in parallel
    let total_rows = AtomicUsize::new(0);
    let matching_rows = AtomicUsize::new(0);

    partition_files.par_iter().for_each(|partition_file| {
        process_partition_file(partition_file, search_pattern, &total_rows, &matching_rows);
    });

    let duration_ms = start.elapsed().as_millis();

    Ok(QueryResult {
        total_rows: total_rows.load(Ordering::Relaxed),
        matching_rows: matching_rows.load(Ordering::Relaxed),
        duration_ms,
        file_size_mb,
    })
}

// Query CSV using Polars
fn query_csv_polars(file_path: &str, search_pattern: &str) -> Result<QueryResult, Box<dyn Error>> {
    info!("Starting CSV Polars query...");
    let start = Instant::now();

    let file_size_mb = fs::metadata(file_path)?.len() as f64 / 1_024_000.0;

    // Read CSV with Polars
    let df = CsvReadOptions::default()
        .try_into_reader_with_file_path(Some(file_path.into()))?
        .finish()?;

    let total_rows = df.height();

    // Filter rows containing the search pattern
    let mask = df.column("first_name")?.str()?.contains_literal(search_pattern)?
        | df.column("last_name")?.str()?.contains_literal(search_pattern)?
        | df.column("citizen_id")?.str()?.contains_literal(search_pattern)?
        | df.column("address")?.str()?.contains_literal(search_pattern)?;

    let matching_rows = mask.sum().unwrap_or(0) as usize;
    let duration_ms = start.elapsed().as_millis();

    Ok(QueryResult {
        total_rows,
        matching_rows,
        duration_ms,
        file_size_mb,
    })
}

// Query Parquet using Polars
fn query_parquet_polars(file_path: &str, search_pattern: &str) -> Result<QueryResult, Box<dyn Error>> {
    info!("Starting Parquet Polars query...");
    let start = Instant::now();

    let file_size_mb = fs::metadata(file_path)?.len() as f64 / 1_024_000.0;

    // Read Parquet with Polars
    let df = ParquetReader::new(std::fs::File::open(file_path)?).finish()?;

    let total_rows = df.height();

    // Filter rows containing the search pattern
    let mask = df.column("first_name")?.str()?.contains_literal(search_pattern)?
        | df.column("last_name")?.str()?.contains_literal(search_pattern)?
        | df.column("citizen_id")?.str()?.contains_literal(search_pattern)?
        | df.column("address")?.str()?.contains_literal(search_pattern)?;

    let matching_rows = mask.sum().unwrap_or(0) as usize;
    let duration_ms = start.elapsed().as_millis();

    Ok(QueryResult {
        total_rows,
        matching_rows,
        duration_ms,
        file_size_mb,
    })
}

// Query Partitioned Parquet using Polars
fn query_parquet_partitioned_polars(partition_dir: &str, search_pattern: &str) -> Result<QueryResult, Box<dyn Error>> {
    info!("Starting Partitioned Parquet Polars query...");
    let start = Instant::now();

    // Get partition files
    let partition_files = get_partition_files(partition_dir)?;
    info!("Found {} partition files", partition_files.len());

    let file_size_mb = calculate_total_file_size(&partition_files);

    // Read and process all partitions
    let mut total_rows = 0;
    let mut matching_rows = 0;

    for partition_file in &partition_files {
        let df = ParquetReader::new(std::fs::File::open(partition_file)?).finish()?;
        total_rows += df.height();

        // Filter rows containing the search pattern
        let mask = df.column("first_name")?.str()?.contains_literal(search_pattern)?
            | df.column("last_name")?.str()?.contains_literal(search_pattern)?
            | df.column("citizen_id")?.str()?.contains_literal(search_pattern)?
            | df.column("address")?.str()?.contains_literal(search_pattern)?;

        matching_rows += mask.sum().unwrap_or(0) as usize;
    }
    let duration_ms = start.elapsed().as_millis();

    Ok(QueryResult {
        total_rows,
        matching_rows,
        duration_ms,
        file_size_mb,
    })
}

fn print_professional_report(
    csv_result: &QueryResult,
    csv_polars_result: &QueryResult,
    parquet_result: &QueryResult,
    parquet_optimized_result: &QueryResult,
    parquet_partitioned_result: &QueryResult,
    parquet_polars_result: &QueryResult,
    parquet_partitioned_polars_result: &QueryResult,
    search_pattern: &str,
) {
    println!("\n");
    println!("╔═══════════════════════════════════════════════════════════════════════════════╗");
    println!("║                     📊 PERFORMANCE BENCHMARK REPORT                          ║");
    println!("╚═══════════════════════════════════════════════════════════════════════════════╝");
    println!("\n🔍 Search Pattern: \"{}\"", search_pattern);
    println!("📅 Test Date: {}", chrono::Local::now().format("%Y-%m-%d %H:%M:%S"));
    
    // Summary Table
    println!("\n┌─────────────────────────┬──────────────┬──────────────┬──────────────┬──────────────┐");
    println!("│ Method                  │ Query Time   │ Throughput   │ File Size    │ Speedup      │");
    println!("├─────────────────────────┼──────────────┼──────────────┼──────────────┼──────────────┤");
    
    let baseline = csv_result.duration_ms as f64;
    
    println!("│ CSV (Baseline)          │ {:>8} ms │ {:>8.2} MB/s │ {:>8.2} MB │     1.00x    │",
        csv_result.duration_ms,
        csv_result.file_size_mb / (csv_result.duration_ms as f64 / 1000.0),
        csv_result.file_size_mb);
    
    println!("│ CSV Polars              │ {:>8} ms │ {:>8.2} MB/s │ {:>8.2} MB │ {:>8.2}x 📊 │",
        csv_polars_result.duration_ms,
        csv_polars_result.file_size_mb / (csv_polars_result.duration_ms as f64 / 1000.0),
        csv_polars_result.file_size_mb,
        baseline / csv_polars_result.duration_ms as f64);
    
    println!("│ Parquet Single          │ {:>8} ms │ {:>8.2} MB/s │ {:>8.2} MB │ {:>8.2}x    │",
        parquet_result.duration_ms,
        parquet_result.file_size_mb / (parquet_result.duration_ms as f64 / 1000.0),
        parquet_result.file_size_mb,
        baseline / parquet_result.duration_ms as f64);
    
    println!("│ Parquet Optimized       │ {:>8} ms │ {:>8.2} MB/s │ {:>8.2} MB │ {:>8.2}x 🚀 │",
        parquet_optimized_result.duration_ms,
        parquet_optimized_result.file_size_mb / (parquet_optimized_result.duration_ms as f64 / 1000.0),
        parquet_optimized_result.file_size_mb,
        baseline / parquet_optimized_result.duration_ms as f64);
    
    println!("│ Parquet Partitioned     │ {:>8} ms │ {:>8.2} MB/s │ {:>8.2} MB │ {:>8.2}x ⚡ │",
        parquet_partitioned_result.duration_ms,
        parquet_partitioned_result.file_size_mb / (parquet_partitioned_result.duration_ms as f64 / 1000.0),
        parquet_partitioned_result.file_size_mb,
        baseline / parquet_partitioned_result.duration_ms as f64);
    
    println!("│ Parquet Polars          │ {:>8} ms │ {:>8.2} MB/s │ {:>8.2} MB │ {:>8.2}x 🔥 │",
        parquet_polars_result.duration_ms,
        parquet_polars_result.file_size_mb / (parquet_polars_result.duration_ms as f64 / 1000.0),
        parquet_polars_result.file_size_mb,
        baseline / parquet_polars_result.duration_ms as f64);
    
    println!("│ Parquet Part. Polars    │ {:>8} ms │ {:>8.2} MB/s │ {:>8.2} MB │ {:>8.2}x 💎 │",
        parquet_partitioned_polars_result.duration_ms,
        parquet_partitioned_polars_result.file_size_mb / (parquet_partitioned_polars_result.duration_ms as f64 / 1000.0),
        parquet_partitioned_polars_result.file_size_mb,
        baseline / parquet_partitioned_polars_result.duration_ms as f64);
    
    println!("└─────────────────────────┴──────────────┴──────────────┴──────────────┴──────────────┘");
    
    // Results Summary
    println!("\n┌─────────────────────────────────────────────────────────────────────────────────┐");
    println!("│ 📈 RESULTS SUMMARY                                                              │");
    println!("├─────────────────────────────────────────────────────────────────────────────────┤");
    println!("│ Total Rows Scanned:     {:>10}                                              │", csv_result.total_rows);
    println!("│ Matching Rows Found:    {:>10}                                              │", csv_result.matching_rows);
    println!("│ Match Rate:             {:>9.2}%                                              │", 
        (csv_result.matching_rows as f64 / csv_result.total_rows as f64) * 100.0);
    println!("└─────────────────────────────────────────────────────────────────────────────────┘");
    
    // Winner Analysis
    let results = [
        ("CSV", csv_result.duration_ms),
        ("CSV Polars", csv_polars_result.duration_ms),
        ("Parquet Single", parquet_result.duration_ms),
        ("Parquet Optimized", parquet_optimized_result.duration_ms),
        ("Parquet Partitioned", parquet_partitioned_result.duration_ms),
        ("Parquet Polars", parquet_polars_result.duration_ms),
        ("Parquet Partitioned Polars", parquet_partitioned_polars_result.duration_ms),
    ];
    let fastest = results.iter().min_by_key(|x| x.1).unwrap();
    
    println!("\n┌─────────────────────────────────────────────────────────────────────────────────┐");
    println!("│ 🏆 WINNER: {:60} │", fastest.0);
    println!("├─────────────────────────────────────────────────────────────────────────────────┤");
    println!("│ Best Query Time:        {:>8} ms                                              │", fastest.1);
    println!("│ Performance Gain:       {:>8.2}x faster than CSV baseline                     │", 
        baseline / fastest.1 as f64);
    println!("│ Time Saved:             {:>8} ms ({:.1}% reduction)                          │",
        csv_result.duration_ms - fastest.1,
        ((csv_result.duration_ms - fastest.1) as f64 / csv_result.duration_ms as f64) * 100.0);
    println!("└─────────────────────────────────────────────────────────────────────────────────┘");
    
    // Storage Efficiency
    let size_reduction = (1.0 - (parquet_result.file_size_mb / csv_result.file_size_mb)) * 100.0;
    println!("\n┌─────────────────────────────────────────────────────────────────────────────────┐");
    println!("│ 💾 STORAGE EFFICIENCY                                                           │");
    println!("├─────────────────────────────────────────────────────────────────────────────────┤");
    println!("│ CSV Size:               {:>8.2} MB                                            │", csv_result.file_size_mb);
    println!("│ Parquet Size:           {:>8.2} MB                                            │", parquet_result.file_size_mb);
    println!("│ Space Saved:            {:>8.2} MB ({:.1}% reduction)                        │",
        csv_result.file_size_mb - parquet_result.file_size_mb,
        size_reduction);
    println!("└─────────────────────────────────────────────────────────────────────────────────┘");
    println!("\n");
}

fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init();

    let csv_file = "mock_data.csv";
    let parquet_file = "mock_data.parquet";
    let parquet_partitioned_dir = "mock_data_partitioned";
    let search_pattern = "Smith"; // Search for common last name

    println!("\n🚀 Starting Performance Query Test...\n");

    println!("⏳ Running benchmarks...");
    
    println!("\n[1/7] Testing CSV query...");
    let csv_result = query_csv(csv_file, search_pattern)?;
    println!("      ✓ Completed in {} ms", csv_result.duration_ms);

    println!("\n[2/7] Testing CSV Polars query...");
    let csv_polars_result = query_csv_polars(csv_file, search_pattern)?;
    println!("      ✓ Completed in {} ms", csv_polars_result.duration_ms);

    println!("\n[3/7] Testing Parquet Single File query...");
    let parquet_result = query_parquet(parquet_file, search_pattern)?;
    println!("      ✓ Completed in {} ms", parquet_result.duration_ms);

    println!("\n[4/7] Testing Parquet Optimized query...");
    let parquet_optimized_result = query_parquet_optimized(parquet_file, search_pattern)?;
    println!("      ✓ Completed in {} ms", parquet_optimized_result.duration_ms);

    println!("\n[5/7] Testing Parquet Partitioned query...");
    let parquet_partitioned_result = query_parquet_partitioned(parquet_partitioned_dir, search_pattern)?;
    println!("      ✓ Completed in {} ms", parquet_partitioned_result.duration_ms);

    println!("\n[6/7] Testing Parquet Polars query...");
    let parquet_polars_result = query_parquet_polars(parquet_file, search_pattern)?;
    println!("      ✓ Completed in {} ms", parquet_polars_result.duration_ms);

    println!("\n[7/7] Testing Parquet Partitioned Polars query...");
    let parquet_partitioned_polars_result = query_parquet_partitioned_polars(parquet_partitioned_dir, search_pattern)?;
    println!("      ✓ Completed in {} ms", parquet_partitioned_polars_result.duration_ms);

    // Print professional report
    print_professional_report(
        &csv_result,
        &csv_polars_result,
        &parquet_result,
        &parquet_optimized_result,
        &parquet_partitioned_result,
        &parquet_polars_result,
        &parquet_partitioned_polars_result,
        search_pattern,
    );

    Ok(())
}