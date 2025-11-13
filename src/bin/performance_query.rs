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
use sysinfo::{System, Pid};

#[derive(Debug)]
struct QueryResult {
    total_rows: usize,
    matching_rows: usize,
    duration_ms: u128,
    file_size_mb: f64,
    memory_used_mb: f64,
    cpu_usage_percent: f32,
    avg_salary: Option<f64>,
    min_salary: Option<i32>,
    max_salary: Option<i32>,
}

struct ResourceMonitor {
    system: System,
    pid: Pid,
    start_memory: u64,
}

impl ResourceMonitor {
    fn new() -> Self {
        let pid = sysinfo::get_current_pid().unwrap();
        let mut system = System::new();
        system.refresh_all();
        std::thread::sleep(std::time::Duration::from_millis(100));
        system.refresh_all();
        
        let start_memory = system.process(pid).map(|p| p.memory()).unwrap_or(0);
        
        Self {
            system,
            pid,
            start_memory,
        }
    }
    
    fn measure(&mut self) -> (f64, f32) {
        self.system.refresh_all();
        
        if let Some(process) = self.system.process(self.pid) {
            let current_memory = process.memory();
            let cpu_usage = process.cpu_usage();
            
            let memory_used_mb = (current_memory.saturating_sub(self.start_memory)) as f64 / (1024.0 * 1024.0);
            
            (memory_used_mb.max(0.0), cpu_usage)
        } else {
            (0.0, 0.0)
        }
    }
}

fn query_csv(file_path: &str, search_pattern: &str) -> Result<QueryResult, Box<dyn Error>> {
    info!("Starting CSV query...");
    let mut monitor = ResourceMonitor::new();
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
    let (_memory_used_mb, _cpu_usage_percent) = monitor.measure();
    let (memory_used_mb, cpu_usage_percent) = monitor.measure();

    Ok(QueryResult {
        total_rows,
        matching_rows,
        duration_ms,
        file_size_mb,
        memory_used_mb,
        cpu_usage_percent,
        avg_salary: None,
        min_salary: None,
        max_salary: None,
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
    let mut monitor = ResourceMonitor::new();
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
    let (memory_used_mb, cpu_usage_percent) = monitor.measure();

    Ok(QueryResult {
        total_rows,
        matching_rows,
        duration_ms,
        file_size_mb,
        memory_used_mb,
        cpu_usage_percent,
        avg_salary: None,
        min_salary: None,
        max_salary: None,
    })
}

// Optimized version with tuning
fn query_parquet_optimized(file_path: &str, search_pattern: &str) -> Result<QueryResult, Box<dyn Error>> {
    info!("Starting Optimized Parquet query...");
    let mut monitor = ResourceMonitor::new();
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
    let (memory_used_mb, cpu_usage_percent) = monitor.measure();

    Ok(QueryResult {
        total_rows: total_rows.load(Ordering::Relaxed),
        matching_rows: matching_rows.load(Ordering::Relaxed),
        duration_ms,
        file_size_mb,
        memory_used_mb,
        cpu_usage_percent,
        avg_salary: None,
        min_salary: None,
        max_salary: None,
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
    let mut monitor = ResourceMonitor::new();
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
    let (memory_used_mb, cpu_usage_percent) = monitor.measure();

    Ok(QueryResult {
        total_rows: total_rows.load(Ordering::Relaxed),
        matching_rows: matching_rows.load(Ordering::Relaxed),
        duration_ms,
        file_size_mb,
        memory_used_mb,
        cpu_usage_percent,
        avg_salary: None,
        min_salary: None,
        max_salary: None,
    })
}

// Query CSV using Polars (optimized with chunked arrays)
fn query_csv_polars(file_path: &str, search_pattern: &str) -> Result<QueryResult, Box<dyn Error>> {
    info!("Starting CSV Polars query...");
    let mut monitor = ResourceMonitor::new();
    let start = Instant::now();

    let file_size_mb = fs::metadata(file_path)?.len() as f64 / 1_024_000.0;

    // Read CSV with Polars
    let df = CsvReadOptions::default()
        .try_into_reader_with_file_path(Some(file_path.into()))?
        .finish()?;

    let total_rows = df.height();

    // Get columns as ChunkedArrays for faster iteration
    let first_names = df.column("first_name")?.str()?;
    let last_names = df.column("last_name")?.str()?;
    let citizen_ids = df.column("citizen_id")?.str()?;
    let addresses = df.column("address")?.str()?;

    // Parallel search using iterators
    let matching_rows = (0..total_rows)
        .into_par_iter()
        .filter(|&i| {
            let first = first_names.get(i).unwrap_or("");
            let last = last_names.get(i).unwrap_or("");
            let citizen = citizen_ids.get(i).unwrap_or("");
            let addr = addresses.get(i).unwrap_or("");
            
            first.contains(search_pattern)
                || last.contains(search_pattern)
                || citizen.contains(search_pattern)
                || addr.contains(search_pattern)
        })
        .count();

    let duration_ms = start.elapsed().as_millis();
    let (memory_used_mb, cpu_usage_percent) = monitor.measure();

    Ok(QueryResult {
        total_rows,
        matching_rows,
        duration_ms,
        file_size_mb,
        memory_used_mb,
        cpu_usage_percent,
        avg_salary: None,
        min_salary: None,
        max_salary: None,
    })
}

// Query Parquet using Polars (optimized with chunked arrays)
fn query_parquet_polars(file_path: &str, search_pattern: &str) -> Result<QueryResult, Box<dyn Error>> {
    info!("Starting Parquet Polars query...");
    let mut monitor = ResourceMonitor::new();
    let start = Instant::now();

    let file_size_mb = fs::metadata(file_path)?.len() as f64 / 1_024_000.0;

    // Read Parquet with Polars
    let df = ParquetReader::new(std::fs::File::open(file_path)?).finish()?;

    let total_rows = df.height();

    // Get columns as ChunkedArrays for faster iteration
    let first_names = df.column("first_name")?.str()?;
    let last_names = df.column("last_name")?.str()?;
    let citizen_ids = df.column("citizen_id")?.str()?;
    let addresses = df.column("address")?.str()?;

    // Parallel search using iterators
    let matching_rows = (0..total_rows)
        .into_par_iter()
        .filter(|&i| {
            let first = first_names.get(i).unwrap_or("");
            let last = last_names.get(i).unwrap_or("");
            let citizen = citizen_ids.get(i).unwrap_or("");
            let addr = addresses.get(i).unwrap_or("");
            
            first.contains(search_pattern)
                || last.contains(search_pattern)
                || citizen.contains(search_pattern)
                || addr.contains(search_pattern)
        })
        .count();

    let duration_ms = start.elapsed().as_millis();
    let (memory_used_mb, cpu_usage_percent) = monitor.measure();

    Ok(QueryResult {
        total_rows,
        matching_rows,
        duration_ms,
        file_size_mb,
        memory_used_mb,
        cpu_usage_percent,
        avg_salary: None,
        min_salary: None,
        max_salary: None,
    })
}

// Query Parquet with JOIN to salary dataset using Polars
fn query_parquet_polars_with_join(
    main_file: &str,
    salary_file: &str,
    search_pattern: &str,
) -> Result<QueryResult, Box<dyn Error>> {
    info!("Starting Parquet Polars query with JOIN...");
    let mut monitor = ResourceMonitor::new();
    let start = Instant::now();

    let main_size = fs::metadata(main_file)?.len() as f64 / 1_024_000.0;
    let salary_size = fs::metadata(salary_file)?.len() as f64 / 1_024_000.0;
    let file_size_mb = main_size + salary_size;

    // Read both Parquet files
    let df_main = ParquetReader::new(std::fs::File::open(main_file)?).finish()?;
    let df_salary = ParquetReader::new(std::fs::File::open(salary_file)?).finish()?;

    // Join on citizen_id
    let df_joined = df_main.join(
        &df_salary,
        ["citizen_id"],
        ["citizen_id"],
        JoinArgs::new(JoinType::Inner),
    )?;

    let total_rows = df_joined.height();

    // Get columns for search
    let first_names = df_joined.column("first_name")?.str()?;
    let last_names = df_joined.column("last_name")?.str()?;
    let citizen_ids = df_joined.column("citizen_id")?.str()?;
    let addresses = df_joined.column("address")?.str()?;
    let salaries = df_joined.column("salary")?.i32()?;

    // Filter rows matching search pattern and collect salary stats
    let matching_rows = (0..total_rows)
        .into_par_iter()
        .filter_map(|i| {
            let first = first_names.get(i).unwrap_or("");
            let last = last_names.get(i).unwrap_or("");
            let citizen = citizen_ids.get(i).unwrap_or("");
            let addr = addresses.get(i).unwrap_or("");

            let matches = first.contains(search_pattern)
                || last.contains(search_pattern)
                || citizen.contains(search_pattern)
                || addr.contains(search_pattern);

            if matches {
                salaries.get(i)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    let matching_count = matching_rows.len();
    
    // Calculate salary statistics
    let (avg_salary, min_salary, max_salary) = if !matching_rows.is_empty() {
        let sum: i64 = matching_rows.iter().sum::<i32>() as i64;
        let avg = sum as f64 / matching_rows.len() as f64;
        let min = *matching_rows.iter().min().unwrap();
        let max = *matching_rows.iter().max().unwrap();
        (Some(avg), Some(min), Some(max))
    } else {
        (None, None, None)
    };

    let duration_ms = start.elapsed().as_millis();
    let (memory_used_mb, cpu_usage_percent) = monitor.measure();

    Ok(QueryResult {
        total_rows,
        matching_rows: matching_count,
        duration_ms,
        file_size_mb,
        memory_used_mb,
        cpu_usage_percent,
        avg_salary,
        min_salary,
        max_salary,
    })
}

// Query Partitioned Parquet with JOIN to salary dataset using Polars
fn query_parquet_partitioned_polars_with_join(
    main_partition_dir: &str,
    salary_partition_dir: &str,
    search_pattern: &str,
) -> Result<QueryResult, Box<dyn Error>> {
    info!("Starting Partitioned Parquet Polars query with JOIN...");
    let mut monitor = ResourceMonitor::new();
    let start = Instant::now();

    // Get partition files
    let main_files = get_partition_files(main_partition_dir)?;
    let salary_files = get_partition_files(salary_partition_dir)?;
    info!("Found {} main partition files and {} salary partition files", 
          main_files.len(), salary_files.len());

    let file_size_mb = calculate_total_file_size(&main_files) + calculate_total_file_size(&salary_files);

    // Read all partitions and concatenate using vstack
    let mut df_main = ParquetReader::new(std::fs::File::open(&main_files[0])?).finish()?;
    for file in &main_files[1..] {
        let df = ParquetReader::new(std::fs::File::open(file)?).finish()?;
        df_main.vstack_mut(&df)?;
    }

    let mut df_salary = ParquetReader::new(std::fs::File::open(&salary_files[0])?).finish()?;
    for file in &salary_files[1..] {
        let df = ParquetReader::new(std::fs::File::open(file)?).finish()?;
        df_salary.vstack_mut(&df)?;
    }

    // Join on citizen_id
    let df_joined = df_main.join(
        &df_salary,
        ["citizen_id"],
        ["citizen_id"],
        JoinArgs::new(JoinType::Inner),
    )?;

    let total_rows = df_joined.height();

    // Get columns for search
    let first_names = df_joined.column("first_name")?.str()?;
    let last_names = df_joined.column("last_name")?.str()?;
    let citizen_ids = df_joined.column("citizen_id")?.str()?;
    let addresses = df_joined.column("address")?.str()?;
    let salaries = df_joined.column("salary")?.i32()?;

    // Filter rows matching search pattern and collect salary stats
    let matching_rows = (0..total_rows)
        .into_par_iter()
        .filter_map(|i| {
            let first = first_names.get(i).unwrap_or("");
            let last = last_names.get(i).unwrap_or("");
            let citizen = citizen_ids.get(i).unwrap_or("");
            let addr = addresses.get(i).unwrap_or("");

            let matches = first.contains(search_pattern)
                || last.contains(search_pattern)
                || citizen.contains(search_pattern)
                || addr.contains(search_pattern);

            if matches {
                salaries.get(i)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    let matching_count = matching_rows.len();
    
    // Calculate salary statistics
    let (avg_salary, min_salary, max_salary) = if !matching_rows.is_empty() {
        let sum: i64 = matching_rows.iter().sum::<i32>() as i64;
        let avg = sum as f64 / matching_rows.len() as f64;
        let min = *matching_rows.iter().min().unwrap();
        let max = *matching_rows.iter().max().unwrap();
        (Some(avg), Some(min), Some(max))
    } else {
        (None, None, None)
    };

    let duration_ms = start.elapsed().as_millis();
    let (memory_used_mb, cpu_usage_percent) = monitor.measure();

    Ok(QueryResult {
        total_rows,
        matching_rows: matching_count,
        duration_ms,
        file_size_mb,
        memory_used_mb,
        cpu_usage_percent,
        avg_salary,
        min_salary,
        max_salary,
    })
}

// Query Partitioned Parquet using Polars
fn query_parquet_partitioned_polars(partition_dir: &str, search_pattern: &str) -> Result<QueryResult, Box<dyn Error>> {
    info!("Starting Partitioned Parquet Polars query...");
    let mut monitor = ResourceMonitor::new();
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
        let partition_rows = df.height();
        total_rows += partition_rows;

        // Get columns as ChunkedArrays for faster iteration
        let first_names = df.column("first_name")?.str()?;
        let last_names = df.column("last_name")?.str()?;
        let citizen_ids = df.column("citizen_id")?.str()?;
        let addresses = df.column("address")?.str()?;

        // Parallel search within partition
        let partition_matches = (0..partition_rows)
            .into_par_iter()
            .filter(|&i| {
                let first = first_names.get(i).unwrap_or("");
                let last = last_names.get(i).unwrap_or("");
                let citizen = citizen_ids.get(i).unwrap_or("");
                let addr = addresses.get(i).unwrap_or("");
                
                first.contains(search_pattern)
                    || last.contains(search_pattern)
                    || citizen.contains(search_pattern)
                    || addr.contains(search_pattern)
            })
            .count();
        
        matching_rows += partition_matches;
    }
    let duration_ms = start.elapsed().as_millis();
    let (memory_used_mb, cpu_usage_percent) = monitor.measure();

    Ok(QueryResult {
        total_rows,
        matching_rows,
        duration_ms,
        file_size_mb,
        memory_used_mb,
        cpu_usage_percent,
        avg_salary: None,
        min_salary: None,
        max_salary: None,
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
    println!("\n┌─────────────────────────┬──────────────┬──────────────┬──────────────┬──────────────┬──────────────┬──────────────┐");
    println!("│ Method                  │ Query Time   │ Throughput   │ File Size    │ Memory (MB)  │ CPU Usage    │ Speedup      │");
    println!("├─────────────────────────┼──────────────┼──────────────┼──────────────┼──────────────┼──────────────┼──────────────┤");
    
    let baseline = csv_result.duration_ms as f64;
    
    println!("│ CSV (Baseline)          │ {:>8} ms │ {:>8.2} MB/s │ {:>8.2} MB │ {:>8.2} MB │ {:>8.1}%   │     1.00x    │",
        csv_result.duration_ms,
        csv_result.file_size_mb / (csv_result.duration_ms as f64 / 1000.0),
        csv_result.file_size_mb,
        csv_result.memory_used_mb,
        csv_result.cpu_usage_percent);
    
    println!("│ CSV Polars              │ {:>8} ms │ {:>8.2} MB/s │ {:>8.2} MB │ {:>8.2} MB │ {:>8.1}%   │ {:>8.2}x 📊 │",
        csv_polars_result.duration_ms,
        csv_polars_result.file_size_mb / (csv_polars_result.duration_ms as f64 / 1000.0),
        csv_polars_result.file_size_mb,
        csv_polars_result.memory_used_mb,
        csv_polars_result.cpu_usage_percent,
        baseline / csv_polars_result.duration_ms as f64);
    
    println!("│ Parquet Single          │ {:>8} ms │ {:>8.2} MB/s │ {:>8.2} MB │ {:>8.2} MB │ {:>8.1}%   │ {:>8.2}x    │",
        parquet_result.duration_ms,
        parquet_result.file_size_mb / (parquet_result.duration_ms as f64 / 1000.0),
        parquet_result.file_size_mb,
        parquet_result.memory_used_mb,
        parquet_result.cpu_usage_percent,
        baseline / parquet_result.duration_ms as f64);
    
    println!("│ Parquet Optimized       │ {:>8} ms │ {:>8.2} MB/s │ {:>8.2} MB │ {:>8.2} MB │ {:>8.1}%   │ {:>8.2}x 🚀 │",
        parquet_optimized_result.duration_ms,
        parquet_optimized_result.file_size_mb / (parquet_optimized_result.duration_ms as f64 / 1000.0),
        parquet_optimized_result.file_size_mb,
        parquet_optimized_result.memory_used_mb,
        parquet_optimized_result.cpu_usage_percent,
        baseline / parquet_optimized_result.duration_ms as f64);
    
    println!("│ Parquet Partitioned     │ {:>8} ms │ {:>8.2} MB/s │ {:>8.2} MB │ {:>8.2} MB │ {:>8.1}%   │ {:>8.2}x ⚡ │",
        parquet_partitioned_result.duration_ms,
        parquet_partitioned_result.file_size_mb / (parquet_partitioned_result.duration_ms as f64 / 1000.0),
        parquet_partitioned_result.file_size_mb,
        parquet_partitioned_result.memory_used_mb,
        parquet_partitioned_result.cpu_usage_percent,
        baseline / parquet_partitioned_result.duration_ms as f64);
    
    println!("│ Parquet Polars          │ {:>8} ms │ {:>8.2} MB/s │ {:>8.2} MB │ {:>8.2} MB │ {:>8.1}%   │ {:>8.2}x 🔥 │",
        parquet_polars_result.duration_ms,
        parquet_polars_result.file_size_mb / (parquet_polars_result.duration_ms as f64 / 1000.0),
        parquet_polars_result.file_size_mb,
        parquet_polars_result.memory_used_mb,
        parquet_polars_result.cpu_usage_percent,
        baseline / parquet_polars_result.duration_ms as f64);
    
    println!("│ Parquet Part. Polars    │ {:>8} ms │ {:>8.2} MB/s │ {:>8.2} MB │ {:>8.2} MB │ {:>8.1}%   │ {:>8.2}x 💎 │",
        parquet_partitioned_polars_result.duration_ms,
        parquet_partitioned_polars_result.file_size_mb / (parquet_partitioned_polars_result.duration_ms as f64 / 1000.0),
        parquet_partitioned_polars_result.file_size_mb,
        parquet_partitioned_polars_result.memory_used_mb,
        parquet_partitioned_polars_result.cpu_usage_percent,
        baseline / parquet_partitioned_polars_result.duration_ms as f64);
    
    println!("└─────────────────────────┴──────────────┴──────────────┴──────────────┴──────────────┴──────────────┴──────────────┘");
    
    // Results Summary
    println!("\n┌─────────────────────────────────────────────────────────────────────────────────┐");
    println!("│ 📈 RESULTS SUMMARY                                                              │");
    println!("├─────────────────────────────────────────────────────────────────────────────────┤");
    println!("│ Total Rows Scanned:     {:>10}                                              │", csv_result.total_rows);
    println!("│ Matching Rows Found:    {:>10}                                              │", csv_result.matching_rows);
    println!("│ Match Rate:             {:>9.2}%                                              │", 
        (csv_result.matching_rows as f64 / csv_result.total_rows as f64) * 100.0);
    println!("└─────────────────────────────────────────────────────────────────────────────────┘");
    
    // Salary statistics if available
    if let Some(avg) = csv_result.avg_salary {
        println!("\n┌─────────────────────────────────────────────────────────────────────────────────┐");
        println!("│ 💰 SALARY STATISTICS (for matching rows)                                       │");
        println!("├─────────────────────────────────────────────────────────────────────────────────┤");
        println!("│ Average Salary:         {:>10.2} THB                                        │", avg);
        println!("│ Minimum Salary:         {:>10} THB                                        │", csv_result.min_salary.unwrap_or(0));
        println!("│ Maximum Salary:         {:>10} THB                                        │", csv_result.max_salary.unwrap_or(0));
        println!("└─────────────────────────────────────────────────────────────────────────────────┘");
    }
    
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
    let salary_parquet_file = "mock_data_salary.parquet";
    let salary_partitioned_dir = "mock_data_salary_partitioned";
    let search_pattern = "Smith"; // Search for common last name

    println!("\n🚀 Starting Performance Query Test...\n");

    println!("⏳ Running benchmarks...");
    
    println!("\n[1/9] Testing CSV query...");
    let csv_result = query_csv(csv_file, search_pattern)?;
    println!("      ✓ Completed in {} ms", csv_result.duration_ms);

    println!("\n[2/9] Testing CSV Polars query...");
    let csv_polars_result = query_csv_polars(csv_file, search_pattern)?;
    println!("      ✓ Completed in {} ms", csv_polars_result.duration_ms);

    println!("\n[3/9] Testing Parquet Single File query...");
    let parquet_result = query_parquet(parquet_file, search_pattern)?;
    println!("      ✓ Completed in {} ms", parquet_result.duration_ms);

    println!("\n[4/9] Testing Parquet Optimized query...");
    let parquet_optimized_result = query_parquet_optimized(parquet_file, search_pattern)?;
    println!("      ✓ Completed in {} ms", parquet_optimized_result.duration_ms);

    println!("\n[5/9] Testing Parquet Partitioned query...");
    let parquet_partitioned_result = query_parquet_partitioned(parquet_partitioned_dir, search_pattern)?;
    println!("      ✓ Completed in {} ms", parquet_partitioned_result.duration_ms);

    println!("\n[6/9] Testing Parquet Polars query...");
    let parquet_polars_result = query_parquet_polars(parquet_file, search_pattern)?;
    println!("      ✓ Completed in {} ms", parquet_polars_result.duration_ms);

    println!("\n[7/9] Testing Parquet Partitioned Polars query...");
    let parquet_partitioned_polars_result = query_parquet_partitioned_polars(parquet_partitioned_dir, search_pattern)?;
    println!("      ✓ Completed in {} ms", parquet_partitioned_polars_result.duration_ms);

    println!("\n[8/9] Testing Parquet Polars with JOIN query...");
    let parquet_join_result = query_parquet_polars_with_join(parquet_file, salary_parquet_file, search_pattern)?;
    println!("      ✓ Completed in {} ms", parquet_join_result.duration_ms);
    if let Some(avg) = parquet_join_result.avg_salary {
        println!("      💰 Avg Salary: {:.2} THB (Min: {}, Max: {})", 
                 avg, 
                 parquet_join_result.min_salary.unwrap_or(0),
                 parquet_join_result.max_salary.unwrap_or(0));
    }

    println!("\n[9/9] Testing Parquet Partitioned Polars with JOIN query...");
    let parquet_partitioned_join_result = query_parquet_partitioned_polars_with_join(
        parquet_partitioned_dir, 
        salary_partitioned_dir, 
        search_pattern
    )?;
    println!("      ✓ Completed in {} ms", parquet_partitioned_join_result.duration_ms);
    if let Some(avg) = parquet_partitioned_join_result.avg_salary {
        println!("      💰 Avg Salary: {:.2} THB (Min: {}, Max: {})", 
                 avg, 
                 parquet_partitioned_join_result.min_salary.unwrap_or(0),
                 parquet_partitioned_join_result.max_salary.unwrap_or(0));
    }

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

    // Print JOIN results separately
    println!("\n╔═══════════════════════════════════════════════════════════════════════════════╗");
    println!("║                     🔗 JOIN QUERY PERFORMANCE REPORT                         ║");
    println!("╚═══════════════════════════════════════════════════════════════════════════════╝");
    println!("\n🔍 Search Pattern: \"{}\"", search_pattern);
    println!("\n┌─────────────────────────────────┬──────────────┬──────────────┬──────────────┐");
    println!("│ Method                          │ Query Time   │ Matches      │ Avg Salary   │");
    println!("├─────────────────────────────────┼──────────────┼──────────────┼──────────────┤");
    println!("│ Parquet Polars JOIN             │ {:>8} ms │ {:>8}     │ {:>10.2}   │",
        parquet_join_result.duration_ms,
        parquet_join_result.matching_rows,
        parquet_join_result.avg_salary.unwrap_or(0.0));
    println!("│ Parquet Partitioned Polars JOIN │ {:>8} ms │ {:>8}     │ {:>10.2}   │",
        parquet_partitioned_join_result.duration_ms,
        parquet_partitioned_join_result.matching_rows,
        parquet_partitioned_join_result.avg_salary.unwrap_or(0.0));
    println!("└─────────────────────────────────┴──────────────┴──────────────┴──────────────┘");
    
    if parquet_join_result.avg_salary.is_some() {
        println!("\n💰 Salary Range for matches: {} - {} THB",
                 parquet_join_result.min_salary.unwrap_or(0),
                 parquet_join_result.max_salary.unwrap_or(0));
    }
    println!();

    Ok(())
}