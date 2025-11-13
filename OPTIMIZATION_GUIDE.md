# Parquet Query Optimization Guide

## Optimizations Implemented

### 1. **Larger Batch Size** 
```rust
.with_batch_size(8192) // Increased from default 1024
```
- **Why**: Reduces overhead of batch processing
- **Impact**: Better memory locality, fewer iterations
- **Trade-off**: Higher memory usage

### 2. **Parallel Batch Processing**
```rust
batches.par_iter().for_each(|batch| { ... })
```
- **Why**: Utilize multiple CPU cores
- **Impact**: Near-linear speedup with core count
- **Requires**: `rayon` crate

### 3. **Parallel Column Search**
```rust
(0..batch.num_columns()).into_par_iter().map(...)
```
- **Why**: Search multiple columns simultaneously
- **Impact**: 2-4x faster on multi-core systems
- **Best for**: Wide tables with many columns

### 4. **Atomic Counters**
```rust
AtomicUsize for thread-safe counting
```
- **Why**: Lock-free concurrent updates
- **Impact**: Minimal synchronization overhead

## Additional Optimization Strategies

### 5. **Column Projection** (Not yet implemented)
```rust
// Only read specific columns
.with_projection(vec![1, 2]) // Only last_name and citizen_id
```
- **Why**: Skip unnecessary column reads
- **Impact**: 50-75% faster for selective queries

### 6. **Row Group Filtering** (Not yet implemented)
```rust
// Use Parquet statistics to skip row groups
.with_row_filter(predicate)
```
- **Why**: Skip entire row groups without reading
- **Impact**: 10-100x faster with good predicates

### 7. **Better Compression** (Already implemented in convert)
```rust
Compression::ZSTD // Better than SNAPPY for strings
```
- **Why**: Smaller files = less I/O
- **Impact**: 20-40% size reduction

### 8. **Memory-Mapped I/O** (Advanced)
```rust
// Use mmap for large files
let mmap = unsafe { MmapOptions::new().map(&file)? };
```
- **Why**: Let OS handle caching
- **Impact**: Faster repeated queries

## When Each Optimization Helps

| Optimization | Best For | Speedup |
|--------------|----------|---------|
| Larger Batch Size | All queries | 1.2-1.5x |
| Parallel Batches | Large files (>100MB) | 2-4x |
| Parallel Columns | Wide tables (>10 cols) | 2-3x |
| Column Projection | Selective queries | 2-10x |
| Row Group Filter | Filtered queries | 10-100x |
| Better Compression | Large files | 1.3-2x |

## Benchmark Results

Run the optimized version:
```bash
RUST_LOG=info cargo run --bin performance_query
```

Expected improvements:
- **Standard Parquet**: ~578ms
- **Optimized Parquet**: ~200-300ms (2-3x faster)
- **vs CSV**: Should now be competitive or faster

## Tips for Maximum Performance

1. **Use column projection** when you don't need all columns
2. **Increase batch size** for large files (try 16384 or 32768)
3. **Use ZSTD compression** for better compression ratio
4. **Enable row group filtering** with predicates
5. **Consider partitioning** very large datasets

## Trade-offs

- **Memory**: Larger batches = more memory
- **Parallelism**: More threads = more CPU usage
- **Complexity**: More optimizations = harder to debug

## Next Steps

To further optimize:
1. Implement column projection for specific queries
2. Add row group statistics filtering
3. Use memory-mapped I/O for very large files
4. Consider using DataFusion for complex queries
