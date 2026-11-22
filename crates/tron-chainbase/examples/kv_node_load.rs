//! File-only estimate of pooled intrusive nodes with co-located key/value bytes.
#[path = "kv_map_estimate/intrusive.rs"]
mod intrusive;
#[allow(dead_code)]
#[path = "kv_map_estimate/model.rs"]
mod model;
#[path = "kv_map_estimate/parallel.rs"]
mod parallel;
#[path = "kv_map_estimate/progress.rs"]
mod progress;
#[allow(dead_code)]
#[path = "kv_map_estimate/sample.rs"]
mod sample;
use intrusive::{Map, HEADER};
use progress::{footer_count, Progress, Queries};
use sample::{process_memory, save, Input};
use serde_json::json;
use std::{collections::BTreeMap, fs, io::BufReader, path::PathBuf, time::Instant};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
fn main() {
    if let Err(e) = run() {
        eprintln!("intrusive-node load failed: {e}");
        std::process::exit(1);
    }
}
fn open(path: &PathBuf) -> Result<Input<BufReader<fs::File>>> {
    Input::new(BufReader::with_capacity(1024 * 1024, fs::File::open(path)?))
}
fn run() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let mut input_path = PathBuf::new();
    let mut work = PathBuf::new();
    let mut lock_buckets = false;
    let mut exit_after_report = false;
    let mut skip_full_verification = false;
    let mut random_reads = 100_000u64;
    let mut threads = 1usize;
    let mut cutoff = 256usize;
    let mut block_bytes = 65536usize;
    while let Some(arg) = args.next() {
        if arg == "--help" {
            println!("kv_node_load --input SAMPLE --work-dir NEW_DIR [--large-cutoff 256] [--pool-block-bytes 65536] [--lock-buckets] [--random-reads 100000] [--exit-after-report] [--threads 1]");
            return Ok(());
        }
        if arg == "--skip-full-verification" {
            skip_full_verification = true;
            continue;
        }
        if arg == "--exit-after-report" {
            exit_after_report = true;
            continue;
        }
        if arg == "--lock-buckets" {
            lock_buckets = true;
            continue;
        }
        let value = args.next().ok_or("missing argument value")?;
        match arg.as_str() {
            "--input" => input_path = value.into(),
            "--work-dir" => work = value.into(),
            "--large-cutoff" => cutoff = value.parse()?,
            "--random-reads" => random_reads = value.parse()?,
            "--threads" => threads = value.parse()?,
            "--pool-block-bytes" => block_bytes = value.parse()?,
            _ => return Err(format!("unknown argument {arg}").into()),
        }
    }
    if input_path.as_os_str().is_empty()
        || work.as_os_str().is_empty()
        || threads == 0
        || threads > 64
        || random_reads == 0
        || random_reads % threads as u64 != 0
        || cutoff > 32768
        || block_bytes < HEADER
        || block_bytes > 64 * 1024 * 1024
    {
        return Err("invalid arguments".into());
    }
    fs::create_dir(&work)?;
    let expected = footer_count(&input_path)?;
    let mut input = open(&input_path)?;
    let mut stores = input.header.stores.clone();
    let basis_points = input.header.basis_points;
    let mut class_counts: Vec<BTreeMap<usize, u64>> = vec![BTreeMap::new(); stores.len()];
    let mut large_node_bytes = vec![0u64; stores.len()];
    let mut minimum = usize::MAX;
    let mut maximum = 0usize;
    // No map, bucket arrays or pool blocks exist at this baseline.
    unsafe {
        libc::malloc_trim(0);
    }
    let before = process_memory();
    let started = Instant::now();
    let mut queries = Queries::new(expected, random_reads);
    let mut load_progress = Progress::new("load", expected)?;
    let mut loaded = 0u64;
    let mut map = Map::new(cutoff, block_bytes);
    map.set_lock_buckets(lock_buckets);
    for (store, s) in stores.iter().enumerate() {
        let name = s.name.rsplit('/').next().unwrap_or("");
        if ["block", "trans", "trans-cache", "transactionHistory"].contains(&name) {
            map.mark_prehashed_store(store as u16);
        }
    }
    while let Some(r) = input.next_record()? {
        map.put(r.store, r.key, r.value)?;
        queries.observe(loaded, r.store, r.key, r.value);
        loaded += 1;
        let raw = HEADER + r.key.len() + r.value.len();
        minimum = minimum.min(raw);
        maximum = maximum.max(raw);
        let id = r.store as usize;
        if r.value.len() > cutoff {
            large_node_bytes[id] += raw as u64;
        } else {
            *class_counts[id]
                .entry(intrusive::slot_size(r.key.len(), r.value.len()))
                .or_default() += 1;
        }
        // Historical Store.classes is only a value-size histogram, not node-pool geometry.
        stores[id].observe(r.key, r.value, cutoff);
        load_progress.update(loaded)?;
    }
    let load_rate = load_progress.finish(loaded)?;
    let records = input.count;
    let checksum = input.checksum;
    drop(input);
    if records != expected || map.len() as u64 != records {
        return Err("retained count/checksum mismatch".into());
    }
    // Optional full sweep is separate from the requested bounded query benchmark.
    // Loading still validates the complete file footer/count/checksum in both modes.
    let mut verified = 0u64;
    let verify_rate = if skip_full_verification {
        None
    } else {
        // Verify every lookup after all table growth; a byte-sum alone cannot detect swaps.
        let mut verify = open(&input_path)?;
        let mut read_progress = Progress::new("verify_read", records)?;
        while let Some(r) = verify.next_record()? {
            if map.get(r.store, r.key) != Some(r.value) {
                return Err("final byte-for-byte lookup mismatch".into());
            }
            verified += 1;
            read_progress.update(verified)?;
        }
        if verified != records || verify.checksum != checksum {
            return Err("verification stream mismatch".into());
        }
        drop(verify);
        Some(read_progress.finish(verified)?)
    };
    let mut random_read_report = if threads == 1 {
        queries.run(&map)?
    } else {
        queries.run_parallel(&map, threads)?
    }; // Consumes/frees bounded query storage.
    random_read_report["full_verification_performed_before_reads"] = json!(!skip_full_verification);
    unsafe {
        libc::malloc_trim(0);
    }
    let after = process_memory();
    let mut delta = serde_json::Map::new();
    for name in [
        "Rss",
        "VmRSS",
        "VmLck",
        "Locked",
        "Pss",
        "Anonymous",
        "VmSize",
        "VmSwap",
        "Swap",
        "VmHWM",
    ] {
        let a = after[name].as_i64().unwrap_or(0);
        let b = before[name].as_i64().unwrap_or(0);
        delta.insert(name.into(), json!(a - b));
    }
    let stats = map.stats();
    if lock_buckets && after["VmLck"].as_u64() != Some(stats.locked_bucket_bytes) {
        return Err("kernel VmLck does not match the isolated bucket mappings".into());
    }
    let raw_keys: u64 = stores.iter().map(|s| s.raw_key_bytes).sum();
    let raw_values: u64 = stores.iter().map(|s| s.raw_value_bytes).sum();
    if stats.pooled_payload_bytes + stats.standalone_requested_bytes
        != raw_keys + raw_values + records * HEADER as u64
    {
        return Err("node accounting mismatch".into());
    }
    let mut projected_classes: BTreeMap<usize, f64> = BTreeMap::new();
    let mut projected_buckets = 0u64;
    let mut projected_large = 0f64;
    let mut projected_raw_keys = 0f64;
    let mut projected_raw_values = 0f64;
    let mut estimated_entries = 0u64;
    for (id, s) in stores.iter().enumerate() {
        if s.sampled == 0 {
            continue;
        }
        let weight = s.estimated_keys as f64 / s.sampled as f64;
        estimated_entries += s.estimated_keys;
        projected_raw_keys += s.raw_key_bytes as f64 * weight;
        projected_raw_values += s.raw_value_bytes as f64 * weight;
        projected_large += large_node_bytes[id] as f64 * weight;
        for (size, count) in &class_counts[id] {
            *projected_classes.entry(*size).or_default() += *count as f64 * weight;
        }
        for count in s.key_lengths.values() {
            let n = (*count as f64 * weight).ceil() as u64;
            if n > 0 {
                projected_buckets += n.max(4).next_power_of_two();
            }
        }
    }
    let projected_used = projected_classes
        .iter()
        .map(|(size, n)| *size as f64 * n)
        .sum::<f64>();
    let projection = json!({
        "estimated_live_pairs":estimated_entries,
        "raw_key_bytes":projected_raw_keys.round() as u64,
        "raw_value_bytes":projected_raw_values.round() as u64,
        "bucket_pointer_bytes":projected_buckets*8,
        "pooled_node_bytes_in_use":projected_used.ceil() as u64,
        "standalone_node_requested_bytes":projected_large.ceil() as u64,
        "occupied_model_bytes":projected_buckets*8+projected_used.ceil() as u64+projected_large.ceil() as u64,
        "weighted_node_classes":projected_classes,
        "limits":"initial live layout only; prefix-biased sample and old supplied/estimated counts; excludes ordinary allocator overhead, future retained freed cells, pool tail slack and production runtime; no speed claim"
    });
    let report = json!({
        "schema":5,"full_verification_performed":!skip_full_verification,"query_threads":threads,"lock_buckets":lock_buckets,"layout":"intrusive_chains_combined_nodes","input_file":input_path,
        "source_database_opened":false,"sample_entries":records,"basis_points":basis_points,
        "raw_key_bytes":raw_keys,"raw_value_bytes":raw_values,"header_bytes_per_node":HEADER,
        "minimum_observed_node_bytes":if records==0 {0}else{minimum},"maximum_observed_node_bytes":maximum,
        "large_cutoff_value_bytes":cutoff,"pool_block_target_bytes":block_bytes,
        "value_capacity_rule":"0..16 exact; larger capacities rounded upward to multiples of 16",
        "pool_lifetime":"free cells enter global size-class lists; blocks retained until map/process destruction",
        "namespace_and_key_length_location":"table metadata; absent from nodes",
        "allocator_kind":"derived from actual value_size and fixed cutoff, no sign bit or flag",
        "map":stats,"projection":projection,"stores":stores,
        "load_rate_including_file_decode_and_accounting":load_rate,
        "verify_rate_including_file_decode_and_exact_compare":verify_rate,
        "random_read_benchmark":random_read_report,
        "exit_after_report":exit_after_report,
        "memory_before_map_creation":before,"memory_after_load_and_validation":after,
        "memory_delta_bytes":delta,"retained_checksum":checksum,"all_pairs_verified":!skip_full_verification,
        "query_values_verified":random_read_report["rate"]["operations"],
        "final_exact_lookup_count":verified,"elapsed_load_and_validation_seconds":started.elapsed().as_secs_f64(),
        "measurement":if skip_full_verification {"file-only separate process; complete stream count/footer/checksum validated while loading; no full lookup sweep; timed sampled lookups verify full value length and digest; RSS before destruction"} else {"file-only separate process; baseline before map allocation; both input streams closed; every final lookup compared byte-for-byte; RSS before destruction"},
        "standalone_capacity_note":"malloc_usable_size covers whole nodes, excludes some chunk metadata and fragmentation"
    });
    save(&work.join("report.json"), &report)?;
    println!(
        "{}",
        json!({"phase":"complete","report":work.join("report.json"),"sample_entries":records,
        "raw_key_bytes":raw_keys,"raw_value_bytes":raw_values,"memory_delta_bytes":delta,
        "bucket_bytes":stats.bucket_allocation_bytes,"pool_reserved_bytes":stats.pool_reserved_bytes,
        "pool_padding_bytes":stats.pooled_padding_bytes,"free_pool_cell_bytes":stats.free_pool_cell_bytes,
        "standalone_count":stats.standalone_count,"node_block_allocations":stats.node_pool_block_allocation_calls,
        "load_rate":load_rate,"verify_rate":verify_rate,"random_read":random_read_report})
    );
    if exit_after_report {
        // Requested validation completed and the report is durable. Kernel process
        // teardown reclaims the mappings; omit a benchmark-only random Drop walk.
        // Library/unit-test maps retain ordinary destruction and release checks.
        std::mem::forget(map);
    }
    Ok(())
}
