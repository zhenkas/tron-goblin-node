//! Single-pass TidesDB cache/read experiments on a stopped isolated data directory.
//! Preparation selects local persisted keys; no large database is copied.
use rand::{rngs::StdRng, seq::SliceRandom, RngCore, SeedableRng};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    os::unix::fs::MetadataExt,
    path::{Component, Path},
    time::Instant,
};
use tidesdb::{Config, LogLevel, SyncMode, TidesDB};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
#[derive(Clone, Serialize, Deserialize)]
struct Row {
    store: String,
    key_hex: String,
    gets: usize,
}
#[derive(Serialize, Deserialize)]
struct Trace {
    schema: u8,
    kind: String,
    rows: Vec<Row>,
}
fn io() -> Result<BTreeMap<String, u64>> {
    match fs::read_to_string("/proc/self/io") {
        Ok(text) => text
            .lines()
            .map(|l| {
                let (k, v) = l.split_once(':').ok_or("invalid IO counter")?;
                Ok((k.to_owned(), v.trim().parse()?))
            })
            .collect(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Some kernels omit task IO accounting. Use only our own cgroup;
            // this includes sibling processes, so the experiment scope must be idle.
            let cgroups = fs::read_to_string("/proc/self/cgroup")?;
            let name = cgroups
                .lines()
                .find_map(|l| l.strip_prefix("0::"))
                .ok_or("requires unified cgroup IO accounting")?;
            if name == "/"
                || Path::new(name)
                    .components()
                    .any(|c| matches!(c, Component::ParentDir))
            {
                return Err("requires an isolated cgroup for IO fallback".into());
            }
            let path = Path::new("/sys/fs/cgroup")
                .join(name.trim_start_matches('/'))
                .join("io.stat");
            let text = fs::read_to_string(path)?;
            let mut result =
                BTreeMap::from([("read_bytes".to_owned(), 0), ("write_bytes".to_owned(), 0)]);
            for line in text.lines() {
                for word in line.split_whitespace().skip(1) {
                    let Some((key, value)) = word.split_once('=') else {
                        continue;
                    };
                    let label = match key {
                        "rbytes" => "read_bytes",
                        "wbytes" => "write_bytes",
                        _ => continue,
                    };
                    *result.get_mut(label).unwrap() += value.parse::<u64>()?;
                }
            }
            Ok(result)
        }
        Err(e) => Err(e.into()),
    }
}

fn delta(a: &BTreeMap<String, u64>, b: &BTreeMap<String, u64>) -> BTreeMap<String, u64> {
    b.iter()
        .map(|(k, v)| (k.clone(), v.saturating_sub(*a.get(k).unwrap_or(&0))))
        .collect()
}
fn config(path: &Path, cache_mb: usize, max_open: usize) -> Config {
    Config::new(path)
        .num_flush_threads(1)
        .num_compaction_threads(0)
        .memtable_write_buffer_size(16 * 1024 * 1024)
        .block_cache_size(cache_mb * 1024 * 1024)
        .max_open_sstables(max_open)
        .memtable_sync_mode(SyncMode::None)
        .memtable_idle_flush_seconds(0)
        .log_level(LogLevel::Error)
        .log_to_file(false)
}
fn open(root: &Path, store: &str, cache_mb: usize, max_open: usize) -> Result<TidesDB> {
    if Path::new(store).components().count() != 1
        || !matches!(
            Path::new(store).components().next(),
            Some(Component::Normal(_))
        )
    {
        return Err("invalid store name".into());
    }
    let path = root.join(store);
    if !path.is_dir() || path.join("CURRENT").exists() || fs::read_dir(&path)?.next().is_none() {
        return Err("requires an existing stopped TidesDB store".into());
    }
    let owner = fs::metadata(&path)?;
    // Native open/close can create WAL/VLog files even without application puts.
    if owner.uid() != unsafe { libc::geteuid() } || owner.gid() != unsafe { libc::getegid() } {
        return Err("run with the database owner UID and primary GID".into());
    }
    let db = TidesDB::open(config(&path, cache_mb, max_open))?;
    db.get_column_family("state")?;
    Ok(db)
}
fn get(db: &TidesDB, key: &[u8]) -> Result<Option<Vec<u8>>> {
    let cf = db.get_column_family(if key.is_empty() {
        "_empty_key"
    } else {
        "state"
    })?;
    let txn = db.begin_transaction()?;
    match txn.get_notrack(&cf, if key.is_empty() { b"value" } else { key }) {
        Ok(v) => Ok(Some(v)),
        Err(e) if e.is_not_found() => Ok(None),
        Err(e) => Err(e.into()),
    }
}
fn prepare(root: &Path, output: &Path, n: usize) -> Result<()> {
    if output.exists() {
        return Err("trace output already exists".into());
    }
    if !(1..=2048).contains(&n) {
        return Err("sample size out of range".into());
    }
    let mut rng = StdRng::seed_from_u64(0x52afb298);
    let mut rows = Vec::new();
    for name in ["account", "storage-row", "account-asset", "properties"] {
        let db = open(root, name, 8, 8192)?;
        let cf = db.get_column_family("state")?;
        let txn = db.begin_transaction()?;
        let mut it = txn.new_iterator(&cf)?;
        it.seek_to_first()?;
        if !it.is_valid() {
            return Err("sample store is empty".into());
        }
        let first = it.key()?;
        if name == "properties" {
            for _ in 0..32 {
                if !it.is_valid() {
                    break;
                }
                rows.push(Row {
                    store: name.into(),
                    key_hex: hex::encode(it.key()?),
                    gets: 512,
                });
                it.next()?;
            }
        } else {
            let mut keys = BTreeSet::new();
            for _ in 0..n * 16 {
                let mut key = vec![0; first.len()];
                rng.fill_bytes(&mut key);
                if name != "storage-row" && key.first().is_some() {
                    key[0] = first[0];
                }
                it.seek(&key)?;
                if !it.is_valid() {
                    it.seek_to_first()?;
                }
                keys.insert(it.key()?);
                if keys.len() == n {
                    break;
                }
            }
            if keys.len() != n {
                return Err("could not select enough distinct local keys".into());
            }
            rows.extend(keys.into_iter().map(|k| Row {
                store: name.into(),
                key_hex: hex::encode(k),
                gets: 2,
            }));
        }
    }
    let trace = Trace {
        schema: 1,
        kind: "synthetic random point reads on local persisted state, with repeated properties"
            .into(),
        rows,
    };
    fs::write(output, serde_json::to_vec(&trace)?)?;
    println!(
        "{}",
        json!({"prepared":true,"unique_keys":trace.rows.len(),"requests":trace.rows.iter().map(|r|r.gets).sum::<usize>(),"trace_sha256":hex::encode(tron_crypto::hash::sha256(&fs::read(output)?))})
    );
    Ok(())
}
fn run(
    root: &Path,
    trace_path: &Path,
    output: &Path,
    cache_mb: usize,
    max_open: usize,
) -> Result<()> {
    if output.exists() {
        return Err("report output already exists".into());
    }
    if !(1..=2048).contains(&cache_mb) || !(32..=65536).contains(&max_open) {
        return Err("resource setting out of range".into());
    }
    let trace: Trace = serde_json::from_slice(&fs::read(trace_path)?)?;
    if trace.schema != 1 || trace.rows.is_empty() {
        return Err("trace schema".into());
    }
    let mut points = Vec::new();
    for row in &trace.rows {
        if row.gets == 0
            || row.gets > 1_000_000
            || points.len() > 1_000_000usize.saturating_sub(row.gets)
        {
            return Err("trace request bound".into());
        }
        let key = hex::decode(&row.key_hex)?;
        for _ in 0..row.gets {
            points.push((row.store.clone(), key.clone()));
        }
    }
    points.shuffle(&mut StdRng::seed_from_u64(0x52afb298));
    let before_open = io()?;
    let start = Instant::now();
    let mut dbs = BTreeMap::new();
    for name in trace
        .rows
        .iter()
        .map(|r| r.store.clone())
        .collect::<BTreeSet<_>>()
    {
        dbs.insert(name.clone(), open(root, &name, cache_mb, max_open)?);
    }
    let open_seconds = start.elapsed().as_secs_f64();
    let mut cache_before = BTreeMap::new();
    for (name, db) in &dbs {
        let s = db.get_cache_stats()?;
        cache_before.insert(name.clone(), (s.hits, s.misses));
    }
    let before_reads = io()?;
    let timer = Instant::now();
    let mut digest = Vec::new();
    let mut hits = 0;
    let mut payload = 0;
    for (store, key) in &points {
        let value = get(&dbs[store], key)?;
        digest.extend_from_slice(store.as_bytes());
        digest.push(0);
        digest.extend_from_slice(&(key.len() as u64).to_le_bytes());
        digest.extend_from_slice(key);
        match value {
            Some(value) => {
                hits += 1;
                payload += value.len();
                digest.push(1);
                digest.extend_from_slice(&(value.len() as u64).to_le_bytes());
                digest.extend_from_slice(&value);
            }
            None => digest.push(0),
        }
    }
    let seconds = timer.elapsed().as_secs_f64();
    let after_reads = io()?;
    let read_io = delta(&before_reads, &after_reads);
    let mut caches = Vec::new();
    for (name, db) in &dbs {
        let s = db.get_cache_stats()?;
        let old = cache_before[name];
        caches.push(json!({"store":name,"hits":s.hits-old.0,"misses":s.misses-old.1,"entries":s.total_entries,"bytes":s.total_bytes,"native_io":format!("{:?}",db.get_io_stats()?)}));
    }
    let rss = fs::read_to_string("/proc/self/status")?
        .lines()
        .find(|l| l.starts_with("VmHWM:"))
        .unwrap_or("")
        .to_owned();
    let before_close = io()?;
    drop(dbs);
    let after_close = io()?;
    let result = json!({"schema":1,"kind":trace.kind,"requests":points.len(),"unique_keys":trace.rows.len(),"hits":hits,"misses":points.len()-hits,"returned_value_bytes":payload,"cache_mb_per_store":cache_mb,"max_open_sstables_per_store":max_open,
        "source_open_seconds":open_seconds,"source_open_io":delta(&before_open,&before_reads),"read_seconds":seconds,"read_io":read_io,
        "physical_read_MB":read_io["read_bytes"] as f64/1e6,"physical_read_bytes_per_get":read_io["read_bytes"] as f64/points.len().max(1) as f64,
        "logical_read_MB":read_io.get("rchar").map(|v|*v as f64/1e6),"io_counter_scope":if fs::metadata("/proc/self/io").is_ok() {"process"} else {"own_cgroup"},"close_io":delta(&before_close,&after_close),"cache_stats":caches,"rss_high":rss,
        "result_sha256":hex::encode(tron_crypto::hash::sha256(&digest)),"trace_sha256":hex::encode(tron_crypto::hash::sha256(&fs::read(trace_path)?)),
        "warmup_passes":0,"passes":1,"application_writes":0,"native_read_only_mode":false,
        "limitations":"Synthetic access order/frequencies, not contract replay. Native TidesDB opens exclusively and may recover or flush at open/close. Background compaction/GC disabled. Open and close IO reported separately. Compare output digests and source head across runs."});
    fs::write(output, serde_json::to_vec_pretty(&result)?)?;
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}
fn main() -> Result<()> {
    if tidesdb::raise_open_file_limit(65536) < 65536 {
        return Err("requires nofile >=65536".into());
    }
    let a = std::env::args().skip(1).collect::<Vec<_>>();
    if a.len() < 4 {
        return Err("usage: tides_read_probe prepare DATABASE_DIR NEW_TRACE N; or run DATABASE_DIR TRACE NEW_REPORT CACHE_MB MAX_OPEN_SSTABLES".into());
    }
    let root = fs::canonicalize(&a[1])?;
    let output = Path::new(if a[0] == "prepare" {
        &a[2]
    } else {
        a.get(3).ok_or("missing output")?
    });
    if fs::canonicalize(
        output
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
    )?
    .starts_with(&root)
    {
        return Err("output must be outside the database directory".into());
    }
    match a[0].as_str() {
        "prepare" if a.len() == 4 => prepare(&root, Path::new(&a[2]), a[3].parse()?),
        "run" if a.len() == 6 => run(
            &root,
            Path::new(&a[2]),
            Path::new(&a[3]),
            a[4].parse()?,
            a[5].parse()?,
        ),
        _ => Err("invalid arguments".into()),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use tidesdb::ColumnFamilyConfig;
    #[test]
    fn point_reader_preserves_empty_keys_values_and_absence() {
        let temp = tempfile::tempdir().unwrap();
        let db = TidesDB::open(config(temp.path(), 4, 64)).unwrap();
        db.create_column_family("state", ColumnFamilyConfig::default())
            .unwrap();
        db.create_column_family("_empty_key", ColumnFamilyConfig::default())
            .unwrap();
        let cf = db.get_column_family("state").unwrap();
        let empty = db.get_column_family("_empty_key").unwrap();
        let mut txn = db.begin_transaction().unwrap();
        txn.put(&cf, b"k", b"").unwrap();
        txn.put(&empty, b"value", b"empty-key").unwrap();
        txn.commit().unwrap();
        assert_eq!(get(&db, b"k").unwrap(), Some(vec![]));
        assert_eq!(get(&db, b"").unwrap(), Some(b"empty-key".to_vec()));
        assert!(get(&db, b"missing").unwrap().is_none());
        assert!(open(temp.path(), "missing", 4, 64).is_err());
        assert!(!temp.path().join("missing").exists());
    }
}
