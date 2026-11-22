//! Read-only first-prefix export of live RocksDB key/value pairs.
//! Run as the database owner. Secondary metadata/reporting live outside the primary.
//! This is a footprint estimator, not a block checkpoint or a production KV backend.
#[allow(dead_code)]
#[path = "kv_map_estimate/model.rs"]
mod model;
#[allow(dead_code)]
#[path = "kv_map_estimate/sample.rs"]
mod sample;
use rocksdb::{BlockBasedOptions, Cache, Options, ReadOptions, DB};
use sample::{save, target, Store};
use serde_json::json;
use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
#[derive(Clone)]
struct Config {
    source: PathBuf,
    work: PathBuf,
    bps: u64,
    cutoff: usize,
    export_file: Option<PathBuf>,
    counts_file: Option<PathBuf>,
}

fn config() -> Result<Config> {
    let mut args = std::env::args().skip(1);
    let mut c = Config {
        source: PathBuf::new(),
        work: PathBuf::new(),
        bps: 100,
        cutoff: 4096,
        export_file: None,
        counts_file: None,
    };
    while let Some(arg) = args.next() {
        if arg == "--help" {
            println!("kv_map_estimate --source DATA --work-dir NEW_DIR --export-file SAMPLE [--basis-points 100] [--large-cutoff 4096] [--counts-file JSON]");
            std::process::exit(0);
        }
        let value = args.next().ok_or("argument missing value")?;
        match arg.as_str() {
            "--source" => c.source = value.into(),
            "--export-file" => c.export_file = Some(value.into()),
            "--counts-file" => c.counts_file = Some(value.into()),
            "--work-dir" => c.work = value.into(),
            "--basis-points" => c.bps = value.parse()?,
            "--large-cutoff" => c.cutoff = value.parse()?,
            _ => return Err(format!("unknown argument {arg}").into()),
        }
    }
    if c.source.as_os_str().is_empty() || c.work.as_os_str().is_empty() || c.export_file.is_none() {
        return Err("source, work-dir and export-file are required".into());
    }
    if c.bps == 0
        || c.bps > 10_000
        || c.cutoff < 16
        || !c.cutoff.is_power_of_two()
        || c.cutoff > 32768
    {
        return Err("invalid sampling/pool parameters".into());
    }
    Ok(c)
}
fn discover(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    if dir.join("CURRENT").is_file() {
        out.push(dir.to_path_buf());
        return Ok(());
    }
    for item in fs::read_dir(dir)? {
        let item = item?;
        if item.file_type()?.is_dir() {
            discover(&item.path(), out)?;
        }
    }
    Ok(())
}
fn owner_guard(source: &Path, paths: &[PathBuf]) -> Result<()> {
    let uid = unsafe { libc::geteuid() };
    if uid == 0 {
        return Err(
            "run as the non-root database owner; primary files must not be opened as root".into(),
        );
    }
    for path in std::iter::once(source).chain(paths.iter().map(PathBuf::as_path)) {
        if fs::metadata(path)?.uid() != uid {
            return Err(format!("database owner mismatch for {}", path.display()).into());
        }
    }
    Ok(())
}
fn secondary(source: &Path, metadata: &Path, cache: &Cache) -> Result<DB> {
    fs::create_dir_all(metadata)?;
    let mut opts = Options::default();
    opts.create_if_missing(false);
    opts.set_paranoid_checks(true);
    // RocksDB secondaries must keep SSTs open while the primary compacts/deletes them.
    opts.set_max_open_files(-1);
    opts.set_db_log_dir(metadata);
    let mut block = BlockBasedOptions::default();
    block.set_block_cache(cache);
    opts.set_block_based_table_factory(&block);
    if let Some(name) = source.file_name().and_then(|x| x.to_str()) {
        if let Some((name, compare)) = tron_chainbase::comparator_for_store(name) {
            opts.set_comparator(name, Box::new(compare));
        }
    }
    let db = DB::open_as_secondary(&opts, source, metadata)?;
    db.try_catch_up_with_primary()?;
    Ok(db)
}

fn export_pairs(
    c: &Config,
    source: &Path,
    work: &Path,
    paths: &[PathBuf],
    stores: &mut [Store],
    cache: &Cache,
) -> Result<()> {
    let output = c.export_file.as_ref().unwrap();
    let parent = fs::canonicalize(output.parent().ok_or("export parent missing")?)?;
    if parent.starts_with(source) {
        return Err("export file must be outside primary".into());
    }
    if output.exists() {
        return Err("export file already exists".into());
    }
    let partial = output.with_extension("partial");
    let file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&partial)?;
    let mut writer = std::io::BufWriter::with_capacity(8 * 1024 * 1024, file);
    sample::write_header(&mut writer, c.bps, stores)?;
    let mut count = 0u64;
    let mut checksum = 0u64;
    for (id, (path, s)) in paths.iter().zip(stores.iter_mut()).enumerate() {
        if s.target == 0 {
            continue;
        }
        let db = secondary(
            path,
            &work.join("secondary-export").join(id.to_string()),
            cache,
        )?;
        s.sequence = db.latest_sequence_number();
        let mut options = ReadOptions::default();
        options.fill_cache(false);
        options.set_readahead_size(1024 * 1024);
        let mut iter = db.raw_iterator_opt(options);
        iter.seek_to_first();
        while iter.valid() && s.sampled < s.target {
            let (k, v) = (
                iter.key().ok_or("key missing")?,
                iter.value().ok_or("value missing")?,
            );
            sample::write_pair(&mut writer, id as u16, k, v)?;
            checksum = k
                .iter()
                .chain(v)
                .fold(checksum, |a, b| a.wrapping_add(*b as u64));
            count += 1;
            s.observe(k, v, c.cutoff);
            iter.next();
        }
        iter.status()?;
        println!(
            "{}",
            json!({"phase":"export","store":s.name,"sampled":s.sampled,"total":count})
        );
    }
    sample::write_footer(&mut writer, count, checksum)?;
    writer.flush()?;
    writer.get_ref().sync_all()?;
    drop(writer);
    fs::rename(&partial, output)?;
    let report = json!({"phase":"export_complete","file":output,"records":count,"checksum":checksum,"file_bytes":fs::metadata(output)?.len(),"stores":stores,"basis_points":c.bps});
    save(&work.join("export-report.json"), &report)?;
    println!("{report}");
    Ok(())
}

fn run(c: Config) -> Result<()> {
    let source = fs::canonicalize(&c.source)?;
    let parent = fs::canonicalize(c.work.parent().ok_or("work parent missing")?)?;
    let work = parent.join(c.work.file_name().ok_or("work filename missing")?);
    if work.starts_with(&source) || source.starts_with(&work) {
        return Err("work directory must be outside the primary".into());
    }
    let mut paths = Vec::new();
    discover(&source, &mut paths)?;
    paths.sort();
    if paths.is_empty() || paths.len() >= u16::MAX as usize {
        return Err("no stores or too many stores".into());
    }
    owner_guard(&source, &paths)?;
    fs::create_dir(&work)?; // refuse reuse/overwriting an earlier measurement
    let cache = Cache::new_lru_cache(32 * 1024 * 1024);
    let overrides: BTreeMap<String, u64> = match &c.counts_file {
        Some(path) => serde_json::from_slice(&fs::read(path)?)?,
        None => BTreeMap::new(),
    };
    for name in overrides.keys() {
        if !paths
            .iter()
            .any(|p| p.strip_prefix(&source).ok().and_then(Path::to_str) == Some(name.as_str()))
        {
            return Err(format!("count override has unknown store {name}").into());
        }
    }
    let mut stores = Vec::new();
    for (id, path) in paths.iter().enumerate() {
        let db = secondary(
            path,
            &work.join("secondary-inventory").join(id.to_string()),
            &cache,
        )?;
        let mut estimated = db
            .property_int_value("rocksdb.estimate-num-keys")?
            .ok_or("estimate-num-keys unavailable")?;
        let mut iter = db.raw_iterator();
        iter.seek_to_first();
        if iter.valid() {
            estimated = estimated.max(1);
        } else {
            iter.status()?;
            estimated = 0;
        }
        let name = path.strip_prefix(&source)?.to_string_lossy().to_string();
        let rocksdb_estimated_keys = estimated;
        let count_source = if let Some(count) = overrides.get(&name) {
            // An explicitly supplied earlier verified count remains approximate for
            // today's live DB, but gives reproducible quotas despite SST sampling bias.
            estimated = if iter.valid() { (*count).max(1) } else { 0 };
            "provided reference count; may be an older snapshot"
        } else {
            "rocksdb.estimate-num-keys; sampled estimate"
        };
        let s = Store {
            name,
            rocksdb_estimated_keys: Some(rocksdb_estimated_keys),
            count_source: count_source.into(),
            estimated_keys: estimated,
            target: target(estimated, c.bps),
            sequence: db.latest_sequence_number(),
            sampled: 0,
            raw_key_bytes: 0,
            raw_value_bytes: 0,
            inline_count: 0,
            inline_bytes: 0,
            max_value_bytes: 0,
            large_count: 0,
            large_bytes: 0,
            classes: BTreeMap::new(),
            value_lengths: BTreeMap::new(),
            key_lengths: BTreeMap::new(),
        };
        println!(
            "{}",
            json!({"phase":"inventory","store":s.name,"estimated_keys":estimated,"sample_target":s.target})
        );
        stores.push(s);
    }
    save(&work.join("inventory.json"), &stores)?;
    export_pairs(&c, &source, &work, &paths, &mut stores, &cache)
}

fn main() {
    if let Err(error) = config().and_then(run) {
        eprintln!("kv-map estimate failed: {error}");
        std::process::exit(1);
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use model::Map;
    #[test]
    fn fractional_targets_include_small_stores_without_multiplying_their_weight() {
        assert_eq!(target(0, 100), 0);
        assert_eq!(target(1, 100), 1);
        assert_eq!(target(100, 100), 1);
        assert_eq!(target(101, 100), 2);
    }
    #[test]
    fn secondary_sampling_does_not_write_the_primary() {
        let temp = tempfile::tempdir().unwrap();
        let primary = temp.path().join("primary");
        let db = DB::open_default(&primary).unwrap();
        for n in 0..200u32 {
            db.put(n.to_be_bytes(), [n as u8; 32]).unwrap();
        }
        db.flush_wal(true).unwrap();
        let sequence = db.latest_sequence_number();
        let cache = Cache::new_lru_cache(1024 * 1024);
        let reader = secondary(&primary, &temp.path().join("secondary"), &cache).unwrap();
        assert_eq!(
            reader
                .property_int_value("rocksdb.estimate-num-keys")
                .unwrap(),
            Some(200)
        );
        let mut it = reader.raw_iterator();
        it.seek_to_first();
        let mut map = Map::new(4, 4096, 128);
        for _ in 0..2 {
            let (k, v) = (it.key().unwrap(), it.value().unwrap());
            map.put(0, k, v).unwrap();
            assert_eq!(map.get(0, k), Some(v));
            it.next();
        }
        assert_eq!(map.len(), 2);
        assert_eq!(db.latest_sequence_number(), sequence);
        assert_eq!(
            db.get(199u32.to_be_bytes()).unwrap().unwrap(),
            vec![199u8; 32]
        );
        assert!(reader.put(b"forbidden", b"write").is_err());
    }
}
