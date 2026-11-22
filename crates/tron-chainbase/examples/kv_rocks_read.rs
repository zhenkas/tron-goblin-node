//! Fixed-partition concurrent point reads for the intrusive benchmark's exact selected keys.
#[allow(dead_code)]
#[path = "kv_map_estimate/intrusive.rs"]
mod intrusive;
#[allow(dead_code)]
#[path = "kv_map_estimate/model.rs"]
mod model;
#[path = "kv_map_estimate/parallel.rs"]
mod parallel;
#[allow(dead_code)]
#[path = "kv_map_estimate/progress.rs"]
mod progress;
#[allow(dead_code)]
#[path = "kv_map_estimate/sample.rs"]
mod sample;
use progress::{digest, footer_count, Progress, Queries};
use rocksdb::{BlockBasedOptions, Cache, Options, DB};
use sample::{process_memory, save, Input};
use serde_json::json;
use std::{
    collections::BTreeMap,
    fs,
    io::{BufReader, Write},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    time::Instant,
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
fn open_store(
    path: &Path,
    logs: &Path,
    cache: &Cache,
    buffer_mb: usize,
    max_files: i32,
) -> Result<DB> {
    fs::create_dir_all(logs)?;
    let mut opts = Options::default();
    opts.create_if_missing(false);
    opts.set_paranoid_checks(true);
    opts.set_max_open_files(max_files);
    opts.set_write_buffer_size(buffer_mb * 1024 * 1024);
    opts.set_max_write_buffer_number(4);
    opts.set_db_log_dir(logs);
    let mut table = BlockBasedOptions::default();
    table.set_block_cache(cache);
    table.set_bloom_filter(10.0, false);
    table.set_cache_index_and_filter_blocks(true);
    table.set_pin_l0_filter_and_index_blocks_in_cache(true);
    table.set_pin_top_level_index_and_filter(true);
    opts.set_block_based_table_factory(&table);
    if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
        if let Some((name, compare)) = tron_chainbase::comparator_for_store(name) {
            opts.set_comparator(name, Box::new(compare));
        }
    }
    Ok(DB::open_for_read_only(&opts, path, false)?)
}
fn owner(path: &Path) -> Result<()> {
    let uid = unsafe { libc::geteuid() };
    if uid == 0 || fs::metadata(path)?.uid() != uid {
        return Err("run as the non-root database owner".into());
    }
    Ok(())
}
fn run() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let mut threads = 1usize;
    let (mut source, mut input, mut work) = (PathBuf::new(), PathBuf::new(), PathBuf::new());
    let (mut cache_mb, mut buffer_mb, mut max_files, mut reads) =
        (4096usize, 16usize, 128i32, 100000u64);
    while let Some(arg) = args.next() {
        if arg == "--help" {
            println!("kv_rocks_read --source DATA --input SAMPLE --work-dir NEW_DIR [--block-cache-mb 4096] [--write-buffer-mb 16] [--max-open-files 128] [--random-reads 100000] [--threads 1]");
            return Ok(());
        }
        let v = args.next().ok_or("missing argument")?;
        match arg.as_str() {
            "--source" => source = v.into(),
            "--input" => input = v.into(),
            "--work-dir" => work = v.into(),
            "--block-cache-mb" => cache_mb = v.parse()?,
            "--write-buffer-mb" => buffer_mb = v.parse()?,
            "--max-open-files" => max_files = v.parse()?,
            "--random-reads" => reads = v.parse()?,
            "--threads" => threads = v.parse()?,
            _ => return Err(format!("unknown argument {arg}").into()),
        }
    }
    if source.as_os_str().is_empty()
        || input.as_os_str().is_empty()
        || work.as_os_str().is_empty()
        || threads == 0
        || threads > 64
        || reads == 0
        || reads % threads as u64 != 0
        || cache_mb == 0
        || cache_mb > 65536
        || buffer_mb == 0
        || buffer_mb > 4096
        || max_files < 1
    {
        return Err("invalid arguments".into());
    }
    let source = source.canonicalize()?;
    owner(&source)?;
    let parent = work.parent().ok_or("work parent missing")?.canonicalize()?;
    let work = parent.join(work.file_name().ok_or("work filename missing")?);
    if work.starts_with(&source) || source.starts_with(&work) {
        return Err("work must be outside source".into());
    }
    fs::create_dir(&work)?;
    let prepare = Instant::now();
    let total = footer_count(&input)?;
    let mut stream = Input::new(BufReader::with_capacity(
        1024 * 1024,
        fs::File::open(&input)?,
    ))?;
    let stores = stream.header.stores.clone();
    let mut queries = Queries::new(total, reads);
    let mut selected = 0u64;
    let mut progress = Progress::new("select_keys", total)?;
    while let Some(r) = stream.next_record()? {
        queries.observe(selected, r.store, r.key, r.value);
        selected += 1;
        progress.update(selected)?;
    }
    progress.finish(selected)?;
    if selected != total {
        return Err("sample count changed".into());
    }
    drop(stream);
    let queries = queries.into_shuffled()?;
    let prepare_seconds = prepare.elapsed().as_secs_f64();
    let before_open = process_memory();
    let cache = Cache::new_hyper_clock_cache(cache_mb * 1024 * 1024, 0);
    let mut dbs = BTreeMap::new();
    let opening = Instant::now();
    for query in &queries {
        if dbs.contains_key(&query.store) {
            continue;
        }
        let path = source
            .join(&stores[query.store as usize].name)
            .canonicalize()?;
        if !path.starts_with(&source) {
            return Err("store escapes source".into());
        }
        owner(&path)?;
        let db = open_store(
            &path,
            &work.join("logs").join(query.store.to_string()),
            &cache,
            buffer_mb,
            max_files,
        )?;
        dbs.insert(query.store, db);
        println!(
            "{}",
            json!({"phase":"open_store","store":stores[query.store as usize].name,"opened":dbs.len()})
        );
        std::io::stdout().flush()?;
    }
    let open_seconds = opening.elapsed().as_secs_f64();
    let before_reads = process_memory();
    let query_fingerprint = queries.iter().fold(0u64, |h, q| {
        h.rotate_left(7) ^ digest(&q.key) ^ (q.store as u64)
    });
    let summary = parallel::run("rocksdb_random_read", &queries, threads, |q| {
        if let Some(value) = dbs[&q.store].get(&q.key)? {
            let checksum = digest(&value);
            let exact = value.len() == q.value_len && checksum == q.value_digest;
            Ok(parallel::Outcome {
                bytes: value.len() as u64,
                exact: exact as u64,
                changed: (!exact) as u64,
                checksum,
                ..parallel::Outcome::default()
            })
        } else {
            Ok(parallel::Outcome {
                missing: 1,
                ..parallel::Outcome::default()
            })
        }
    })?;
    let rate = &summary.rate;
    let (missing, changed, exact, bytes) = (
        summary.outcome.missing,
        summary.outcome.changed,
        summary.outcome.exact,
        summary.outcome.bytes,
    );
    let found = rate.operations - missing;
    let report = json!({"schema":2,"query_threads":threads,"parallel":summary,"phase":"complete","backend":"RocksDB read-only node block-cache profile",
        "single_query_thread":threads==1,"input":input,"source":source,"sample_records":total,
        "queries":rate.operations,"rate":rate,"found":found,"missing":missing,"changed_since_sample":changed,"exact_values":exact,"value_bytes_consumed":bytes,
        "query_order_fingerprint":query_fingerprint,"cache_bytes":cache_mb*1024*1024,"cache_kind":"HyperClockCache shared across queried stores","max_open_files_per_store":max_files,"write_buffer_mb":buffer_mb,
        "bloom_bits_per_key":10,"cache_index_and_filter_blocks":true,"pin_l0_and_top_level_index_filters":true,
        "queried_stores":dbs.len(),"prepare_seconds":prepare_seconds,"open_seconds":open_seconds,
        "memory_before_open":before_open,"memory_before_reads":before_reads,"memory_after_reads":process_memory(),
        "measurement":"same deterministic stratified keys and shuffle as kv_node_load; fresh RocksDB block cache; timed DB.get plus full value digest/compare, progress; file decoding and DB opening excluded; OS page cache left intact",
        "consistency":"node must remain stopped; source may have advanced after original sample, so changed/missing versions are counted explicitly"});
    save(&work.join("report.json"), &report)?;
    println!("{}", report);
    Ok(())
}
fn main() {
    if let Err(e) = run() {
        eprintln!("rocksdb read benchmark failed: {e}");
        std::process::exit(1);
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn tuned_reader_gets_bytes_and_refuses_writes_without_changing_primary() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("db");
        let db = DB::open_default(&path).unwrap();
        for n in 0..100u64 {
            db.put(n.to_le_bytes(), vec![n as u8; 33]).unwrap();
        }
        db.flush_wal(true).unwrap();
        drop(db);
        let cache = Cache::new_hyper_clock_cache(1024 * 1024, 0);
        let db = open_store(&path, &temp.path().join("logs"), &cache, 16, 128).unwrap();
        for n in 0..100u64 {
            assert_eq!(db.get(n.to_le_bytes()).unwrap().unwrap(), vec![n as u8; 33]);
        }
        assert!(db.put(b"no", b"write").is_err());
        drop(db);
        let db = DB::open_default(&path).unwrap();
        assert!(db.get(b"no").unwrap().is_none());
    }
}
