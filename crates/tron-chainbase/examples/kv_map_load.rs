//! File-only pooled-map load. This binary neither imports nor opens RocksDB.
#[path = "kv_map_estimate/model.rs"]
mod model;
#[allow(dead_code)]
#[path = "kv_map_estimate/sample.rs"]
mod sample;
use model::{KeyStorage, Map, ValueAllocator};
use sample::{process_memory, projection_with_keys, save, Input};
use serde_json::json;
use std::{fs, io::BufReader, path::PathBuf, time::Instant};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
fn main() {
    if let Err(e) = run() {
        eprintln!("file-only KV map load failed: {e}");
        std::process::exit(1);
    }
}
fn run() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let mut input_path = PathBuf::new();
    let mut work = PathBuf::new();
    let mut cells = 1_000_000u32;
    let mut cutoff = 4096usize;
    let mut pool_step = 0usize;
    let mut shards = 100u64;
    let mut key_storage = KeyStorage::Arena;
    let mut allocator = ValueAllocator::Pooled;
    while let Some(arg) = args.next() {
        if arg == "--help" {
            println!("kv_map_load --input SAMPLE --work-dir NEW_DIR [--cells-per-pool 1000000] [--large-cutoff 4096] [--pool-step 0] [--project-shards 100] [--value-allocator pooled|system] [--key-storage arena|inline]");
            return Ok(());
        }
        let v = args.next().ok_or("missing argument value")?;
        match arg.as_str() {
            "--input" => input_path = v.into(),
            "--work-dir" => work = v.into(),
            "--cells-per-pool" => cells = v.parse()?,
            "--large-cutoff" => cutoff = v.parse()?,
            "--pool-step" => pool_step = v.parse()?,
            "--project-shards" => shards = v.parse()?,
            "--key-storage" => {
                key_storage = match v.as_str() {
                    "arena" => KeyStorage::Arena,
                    "inline" => KeyStorage::Inline,
                    _ => return Err("key-storage must be arena or inline".into()),
                }
            }
            "--value-allocator" => {
                allocator = match v.as_str() {
                    "pooled" => ValueAllocator::Pooled,
                    "system" => ValueAllocator::System,
                    _ => return Err("value-allocator must be pooled or system".into()),
                }
            }
            _ => return Err(format!("unknown argument {arg}").into()),
        }
    }
    if input_path.as_os_str().is_empty()
        || work.as_os_str().is_empty()
        || cells == 0
        || cells > i32::MAX as u32
        || cutoff < 16
        || !cutoff.is_power_of_two()
        || cutoff > 32768
        || (pool_step != 0
            && (pool_step < 16 || !pool_step.is_power_of_two() || cutoff % pool_step != 0))
        || shards == 0
    {
        return Err("invalid arguments".into());
    }
    fs::create_dir(&work)?;
    let mut input = Input::new(BufReader::with_capacity(
        1024 * 1024,
        fs::File::open(&input_path)?,
    ))?;
    let mut stores = input.header.stores.clone();
    let basis_points = input.header.basis_points;
    // Parse file metadata before the baseline. No table, key arena or value pools
    // have been created yet; file buffering is bounded, never whole-file mmap/read.
    unsafe {
        libc::malloc_trim(0);
    }
    let before = process_memory();
    let started = Instant::now();
    let mut map = Map::new(cells, cutoff, 16 * 1024 * 1024);
    map.set_key_storage(key_storage);
    map.set_value_allocator(allocator);
    map.set_pool_step(pool_step);
    for (id, s) in stores.iter().enumerate() {
        let name = s.name.rsplit('/').next().unwrap_or("");
        if ["block", "trans", "trans-cache", "transactionHistory"].contains(&name) {
            map.mark_prehashed_store(id as u16);
        }
    }
    while let Some(record) = input.next_record()? {
        map.put(record.store, record.key, record.value)?;
        if map.get(record.store, record.key) != Some(record.value) {
            return Err("map lookup verification failed".into());
        }
        stores[record.store as usize].observe_with_step(
            record.key,
            record.value,
            cutoff,
            pool_step,
        );
    }
    let records = input.count;
    let checksum = input.checksum;
    drop(input); // drops the read buffer and transient reusable key/value buffers
    if map.len() as u64 != records || map.validate() != checksum {
        return Err("retained map count/checksum mismatch".into());
    }
    unsafe {
        libc::malloc_trim(0);
    }
    let after = process_memory();
    let mut delta = serde_json::Map::new();
    for name in [
        "Rss",
        "VmRSS",
        "Pss",
        "Anonymous",
        "VmSize",
        "VmSwap",
        "Swap",
        "VmHWM",
    ] {
        let a = after.get(name).and_then(|v| v.as_u64()).unwrap_or(0) as i64;
        let b = before.get(name).and_then(|v| v.as_u64()).unwrap_or(0) as i64;
        delta.insert(name.into(), json!(a - b));
    }
    let stats = map.stats();
    let raw_keys: u64 = stores.iter().map(|s| s.raw_key_bytes).sum();
    let raw_values: u64 = stores.iter().map(|s| s.raw_value_bytes).sum();
    let big: u64 = stores.iter().map(|s| s.large_count).sum();
    let maximum = stores.iter().map(|s| s.max_value_bytes).max().unwrap_or(0);
    let pool_reserved: u64 = stats.values.classes.iter().map(|s| s.reserved_bytes).sum();
    let pool_touched: u64 = stats
        .values
        .classes
        .iter()
        .map(|s| s.touched_page_span_bytes)
        .sum();
    let pool_projection = if allocator == ValueAllocator::Pooled {
        projection_with_keys(&stores, cells, shards, key_storage)
    } else {
        serde_json::Value::Null
    };
    let report = json!({
        "schema":6,"key_storage":key_storage,"pool_step_bytes":pool_step,"value_allocator":allocator,"input_file":input_path,"source_database_opened":false,
        "basis_points":basis_points,"sample_entries":records,"store_count":stores.len(),
        "raw_key_bytes":raw_keys,"raw_value_bytes":raw_values,"max_value_bytes":maximum,"large_value_count":big,
        "cells_per_pool":cells,"large_cutoff":cutoff,"reserved_value_pool_bytes":pool_reserved,
        "touched_value_pool_page_span_bytes":pool_touched,"map":stats,
        "memory_before_map_creation":before,"memory_after_load_and_validation":after,"memory_delta_bytes":delta,
        "elapsed_load_and_validation_seconds":started.elapsed().as_secs_f64(),"all_pairs_verified":true,
        "allocator_capacity_note":"malloc_usable_size excludes some chunk headers and arena fragmentation; use measured RSS delta for total footprint",
        "retained_checksum":checksum,"projection":pool_projection,"stores":stores,
        "measurement":"separate process; file-only bounded streaming input; delta measured before map creation and after closing input buffers and verifying all retained bytes"
    });
    save(&work.join("report.json"), &report)?;
    println!(
        "{}",
        json!({"phase":"complete","pool_step_bytes":pool_step,"value_allocator":allocator,"report":work.join("report.json"),"sample_entries":records,
        "raw_key_bytes":raw_keys,"raw_value_bytes":raw_values,"max_value_bytes":maximum,"large_value_count":big,
        "memory_before":before,"memory_after":after,"memory_delta_bytes":delta,"projection":report["projection"]})
    );
    Ok(())
}
