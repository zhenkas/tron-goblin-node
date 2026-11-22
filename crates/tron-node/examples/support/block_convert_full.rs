//! Streaming full-state import. Private benchmark data only; source RocksDB is read-only.
//! Link with -Wl,--wrap=engine_compaction_init (pinned TidesDB 10.2.1).
use super::*;
use ::rocksdb::{Direction, IteratorMode, Options, ReadOptions, DB};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicU64, Ordering};
use tidesdb::{ColumnFamilyConfig, Config, LogLevel, SyncMode, TidesDB};

static DISABLED_STARTS: AtomicU64 = AtomicU64::new(0);

/// This pinned native initializer only creates the compaction pool, backstop ticker
/// and queue. The engine is zero-initialized; its wake/drain paths tolerate NULL.
/// Flush, WAL, manifest, transactions and the on-disk format are unchanged.
/// The wrapper is inert without the explicit linker flag; open_target fails closed.
#[no_mangle]
pub extern "C" fn __wrap_engine_compaction_init(_db: *mut std::ffi::c_void) -> i32 {
    DISABLED_STARTS.fetch_add(1, Ordering::SeqCst);
    0
}

const BATCH_BYTES: usize = 4 * 1024 * 1024;
const FREE_FLOOR: u64 = 64 * 1024 * 1024 * 1024;
const CURSOR_KEY: &[u8] = b"cursor";

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
struct Cursor {
    rows: u64,
    logical_bytes: u64,
    last_key: Option<String>,
    digest: String,
    copied: bool,
}
impl Default for Cursor {
    fn default() -> Self {
        Self {
            rows: 0,
            logical_bytes: 0,
            last_key: None,
            digest: hex::encode([0u8; 32]),
            copied: false,
        }
    }
}
fn free_bytes(path: &Path) -> Result<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let p = CString::new(path.as_os_str().as_bytes())?;
    let mut st = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    require(
        unsafe { libc::statvfs(p.as_ptr(), st.as_mut_ptr()) } == 0,
        "statvfs failed",
    )?;
    let st = unsafe { st.assume_init() };
    Ok(st.f_bavail as u64 * st.f_frsize as u64)
}
fn fold(digest: &mut Vec<u8>, k: &[u8], v: &[u8]) {
    let mut bytes = Vec::with_capacity(48 + k.len() + v.len());
    bytes.extend_from_slice(digest);
    bytes.extend_from_slice(&(k.len() as u64).to_le_bytes());
    bytes.extend_from_slice(k);
    bytes.extend_from_slice(&(v.len() as u64).to_le_bytes());
    bytes.extend_from_slice(v);
    *digest = tron_crypto::hash::sha256(&bytes).to_vec();
}
fn open_target(path: &Path, buffer: usize) -> Result<TidesDB> {
    // A full un-compacted iterator pins thousands of SST files at once. Raise
    // the process limit BEFORE engine open; a live raise is too late to repair
    // an iterator whose descriptor acquisition already failed.
    let nofile = tidesdb::raise_open_file_limit(65536);
    require(
        nofile >= 65536,
        "full import needs an open-file limit of at least 65536",
    )?;
    std::fs::create_dir_all(path)?;
    let before = DISABLED_STARTS.load(Ordering::SeqCst);
    let config = Config::new(path)
        .num_flush_threads(1)
        .num_compaction_threads(1)
        .memtable_write_buffer_size(buffer)
        .block_cache_size(8 * 1024 * 1024)
        .max_open_sstables(8192)
        .memtable_sync_mode(SyncMode::None)
        .memtable_idle_flush_seconds(0)
        .log_level(LogLevel::Error)
        .log_to_file(true);
    let db = TidesDB::open(config)?;
    require(DISABLED_STARTS.load(Ordering::SeqCst) > before,
        "compaction initializer was not intercepted; build using cargo rustc with -C link-arg=-Wl,--wrap=engine_compaction_init")?;
    for name in ["state", "_import", tides::EMPTY_KEY_CF] {
        if !db.list_column_families()?.iter().any(|n| n == name) {
            db.create_column_family(
                name,
                ColumnFamilyConfig::default()
                    .encoding_pipeline(vec![])
                    .l1_file_count_trigger(0)
                    .tombstone_density_trigger(0.),
            )?;
        }
        let st = db.get_column_family(name)?.get_stats()?;
        require(
            st.config.encoding_pipeline.is_empty(),
            "compression must remain disabled",
        )?;
    }
    checked_stats(&db)?;
    Ok(db)
}
fn checked_stats(db: &TidesDB) -> Result<Value> {
    let st = db.get_db_stats()?;
    require(
        st.compaction_count == 0
            && st.compaction_bytes_written == 0
            && st.compaction_bytes_read == 0
            && st.compaction_pending_count == 0,
        "unexpected compaction activity",
    )?;
    Ok(json!({"compaction_count":st.compaction_count,
        "compaction_bytes_read":st.compaction_bytes_read,
        "compaction_bytes_written":st.compaction_bytes_written,
        "compaction_pending_count":st.compaction_pending_count,
        "flush_count":st.flush_count,"flush_bytes_written":st.flush_bytes_written,
        "sstable_count":st.total_sstable_count,"vlog_file_size":st.vlog_file_size}))
}
fn read_cursor(db: &TidesDB) -> Result<Cursor> {
    let cf = db.get_column_family("_import")?;
    match db.begin_transaction()?.get(&cf, CURSOR_KEY) {
        Ok(v) => Ok(serde_json::from_slice(&v)?),
        Err(e) if e.is_not_found() => Ok(Cursor::default()),
        Err(e) => Err(e.into()),
    }
}
fn commit(db: &TidesDB, rows: &[(Box<[u8]>, Box<[u8]>)], cursor: &Cursor) -> Result<()> {
    let state = db.get_column_family("state")?;
    let meta = db.get_column_family("_import")?;
    let mut tx = db.begin_transaction()?;
    for (k, v) in rows {
        if k.is_empty() {
            let empty = db.get_column_family(tides::EMPTY_KEY_CF)?;
            tx.put(&empty, tides::EMPTY_KEY_SLOT, v)?;
        } else {
            tx.put(&state, k, v)?;
        }
    }
    // Cursor and data share the SAME native atomic transaction/WAL record.
    tx.put(&meta, CURSOR_KEY, &serde_json::to_vec(cursor)?)?;
    tx.commit()?;
    Ok(())
}
fn verify(
    path: &Path,
    expected: &Cursor,
    buffer: usize,
    log: &mut File,
    name: &str,
) -> Result<Value> {
    let db = open_target(path, buffer)?;
    require(
        read_cursor(&db)? == *expected,
        "recovered import cursor differs",
    )?;
    let cf = db.get_column_family("state")?;
    let tx = db.begin_transaction()?;
    let empty_cf = db.get_column_family(tides::EMPTY_KEY_CF)?;
    let empty_value = match tx.get(&empty_cf, tides::EMPTY_KEY_SLOT) {
        Ok(v) => Some(v),
        Err(e) if e.is_not_found() => None,
        Err(e) => return Err(e.into()),
    };
    writeln!(
        log,
        "{}",
        json!({"phase":"verify_iterator_open","store":name,
        "expected_rows":expected.rows,"nofile":tidesdb::raise_open_file_limit(0),
        "native":checked_stats(&db)?})
    )?;
    log.flush()?;
    // Discover the actual extrema once, including a possible empty key. Check
    // the maximum against the source cursor before bounding the final range.
    let mut extrema = tx.new_iterator(&cf)?;
    extrema.seek_to_first()?;
    let physical_first = if extrema.is_valid() {
        Some(extrema.key()?)
    } else {
        None
    };
    let first = if empty_value.is_some() {
        Some(vec![])
    } else {
        physical_first
    };
    extrema.seek_to_last()?;
    let last = if extrema.is_valid() {
        Some(extrema.key()?)
    } else if empty_value.is_some() {
        Some(vec![])
    } else {
        None
    };
    require(
        last == expected.last_key.as_ref().map(hex::decode).transpose()?,
        "target maximum key differs from source",
    )?;
    drop(extrema);
    let mut count = 0u64;
    let mut logical = 0u64;
    let mut digest = vec![0u8; 32];
    // The logical empty key sorts before all native nonempty state keys.
    // Its value shares the import transaction with data and the durable cursor.
    if let Some(value) = empty_value.as_ref() {
        fold(&mut digest, &[], value);
        count += 1;
        logical += value.len() as u64;
    }
    let started = Instant::now();
    let mut progress = Instant::now();
    if let (Some(first), Some(last)) = (first, last) {
        let common = first.iter().zip(&last).take_while(|(a, b)| a == b).count();
        let mut cuts = vec![Vec::new()];
        for b in 0u16..=255 {
            let mut cut = first[..common].to_vec();
            cut.push(b as u8);
            if cut > *cuts.last().unwrap() && cut <= last {
                cuts.push(cut);
            }
        }
        let mut end = last.clone();
        end.push(0);
        cuts.push(end);
        writeln!(
            log,
            "{}",
            json!({"phase":"verify_ranges","store":name,
            "ranges":cuts.len()-1,"common_prefix_bytes":common,"expected_rows":expected.rows})
        )?;
        log.flush()?;
        for (range_index, bounds) in cuts.windows(2).enumerate() {
            let (lower, upper) = (&bounds[0], &bounds[1]);
            // Native bounds select overlapping SSTs; they are NOT an output
            // fence. Enforce [lower,upper) ourselves to avoid duplicates.
            let mut iter = tx.new_range_iterator(&cf, lower, upper)?;
            if lower.is_empty() {
                iter.seek_to_first()?;
            } else {
                iter.seek(lower)?;
            }
            while iter.is_valid() {
                let (k, v) = iter.key_value()?;
                if k >= *upper {
                    break;
                }
                require(
                    k >= *lower,
                    "iterator returned a key before its lower bound",
                )?;
                fold(&mut digest, &k, &v);
                logical += (k.len() + v.len()) as u64;
                count += 1;
                iter.next()?;
                if count % 4096 == 0 && progress.elapsed().as_secs() >= 30 {
                    writeln!(
                        log,
                        "{}",
                        json!({"phase":"verify","store":name,"rows":count,
                        "range_index":range_index,"ranges":cuts.len()-1,
                        "expected_rows":expected.rows,"elapsed_seconds":started.elapsed().as_secs_f64()})
                    )?;
                    log.flush()?;
                    progress = Instant::now();
                }
            }
        }
    }
    require(
        count == expected.rows
            && logical == expected.logical_bytes
            && hex::encode(digest) == expected.digest,
        "full reopened state differs from source stream",
    )?;
    checked_stats(&db)
}
fn copy_store(
    source: &Path,
    target: &Path,
    buffer: usize,
    stop_after_batches: Option<usize>,
    log: &mut File,
    name: &str,
) -> Result<Option<Value>> {
    let mut options = Options::default();
    options.set_max_open_files(64);
    options.set_paranoid_checks(true);
    let rocks = DB::open_for_read_only(&options, source, false)?;
    let db = open_target(target, buffer)?;
    let mut cursor = read_cursor(&db)?;
    let initial_rows = cursor.rows;
    let start = Instant::now();
    let mut last_progress = Instant::now();
    if !cursor.copied {
        let last = cursor.last_key.as_ref().map(hex::decode).transpose()?;
        let mode = last
            .as_ref()
            .map(|k| IteratorMode::From(k, Direction::Forward))
            .unwrap_or(IteratorMode::Start);
        let mut opts = ReadOptions::default();
        opts.fill_cache(false);
        opts.set_verify_checksums(true);
        opts.set_readahead_size(1024 * 1024);
        let mut digest = hex::decode(&cursor.digest)?;
        let mut rows = Vec::new();
        let mut nbytes = 0usize;
        let mut batches = 0usize;
        for item in rocks.iterator_opt(mode, opts) {
            let (k, v) = item?;
            if last
                .as_ref()
                .map(|l| k.as_ref() <= l.as_slice())
                .unwrap_or(false)
            {
                continue;
            }
            fold(&mut digest, &k, &v);
            cursor.rows += 1;
            cursor.logical_bytes += (k.len() + v.len()) as u64;
            cursor.last_key = Some(hex::encode(&k));
            nbytes += k.len() + v.len();
            rows.push((k, v));
            if nbytes >= BATCH_BYTES {
                require(
                    free_bytes(target)? > FREE_FLOOR,
                    "free space reserve reached; committed cursor retained",
                )?;
                cursor.digest = hex::encode(&digest);
                commit(&db, &rows, &cursor)?;
                rows.clear();
                nbytes = 0;
                batches += 1;
                if last_progress.elapsed().as_secs() >= 30 {
                    writeln!(
                        log,
                        "{}",
                        json!({"phase":"copy","store":name,"rows":cursor.rows,
                        "logical_bytes":cursor.logical_bytes,"elapsed_seconds":start.elapsed().as_secs_f64(),
                        "native":checked_stats(&db)?})
                    )?;
                    log.flush()?;
                    last_progress = Instant::now();
                }
                if stop_after_batches.map(|n| batches >= n).unwrap_or(false) {
                    db.checkpoint()?;
                    return Ok(None);
                }
            }
        }
        cursor.digest = hex::encode(digest);
        cursor.copied = true;
        commit(&db, &rows, &cursor)?;
    }
    writeln!(
        log,
        "{}",
        json!({"phase":"checkpoint","store":name,"rows":cursor.rows,
        "logical_bytes":cursor.logical_bytes})
    )?;
    log.flush()?;
    db.checkpoint()?; // Engine flush/durability barrier, never a compaction.
    let native = checked_stats(&db)?;
    let import_seconds = start.elapsed().as_secs_f64();
    drop(db);
    drop(rocks);
    let verifying = Instant::now();
    writeln!(
        log,
        "{}",
        json!({"phase":"verify_open","store":name,"expected_rows":cursor.rows})
    )?;
    log.flush()?;
    let reopen_native = verify(target, &cursor, buffer, log, name)?;
    Ok(Some(
        json!({"store":name,"rows":cursor.rows,"logical_key_value_bytes":cursor.logical_bytes,
        "sha256_chain":cursor.digest,"resumed_from_rows":initial_rows,
        "import_and_checkpoint_seconds":import_seconds,"verify_seconds":verifying.elapsed().as_secs_f64(),
        "reopened_full_digest_verified":true,"native":native,"reopen_native":reopen_native}),
    ))
}
fn fingerprint(source: &Path) -> Result<String> {
    // SST identities/sizes and cryptographic hashes of ALL mutable source files.
    // No source write is permitted. Covers all 27 native stores and clone provenance.
    let mut entries = vec![];
    for (_, name) in STORES {
        let dir = source.join("database").join(name);
        for item in std::fs::read_dir(&dir)? {
            let p = item?.path();
            require(p.is_file(), "unexpected nested source entry")?;
            let m = p.metadata()?;
            let rel = p.strip_prefix(source)?.to_string_lossy().into_owned();
            let hash = if p.extension().and_then(|x| x.to_str()) == Some("sst") {
                None
            } else {
                Some(hex::encode(tron_crypto::hash::sha256(&std::fs::read(&p)?)))
            };
            entries.push((
                rel,
                m.len(),
                m.modified()?
                    .duration_since(std::time::UNIX_EPOCH)?
                    .as_nanos(),
                hash,
            ));
        }
    }
    entries.sort();
    Ok(hex::encode(tron_crypto::hash::sha256(&serde_json::to_vec(
        &entries,
    )?)))
}
pub(super) fn run(source: &Path, out: &Path, resume: bool) -> Result<()> {
    let provenance = full::guard(source)?;
    run_inner(
        source,
        out,
        resume,
        json!({"kind":"frozen-clone","receipt":provenance}),
    )
}

/// An external operator receipt is required to read the actual stopped runtime
/// directly. Never create a fake benchmark-clone marker in production data.
fn stopped_guard(source: &Path, receipt: &Path) -> Result<Value> {
    let bytes = std::fs::read(receipt)?;
    let proof: Value = serde_json::from_slice(&bytes)?;
    let recorded = proof["source"].as_str().ok_or("stopped source missing")?;
    require(
        source.canonicalize()? == Path::new(recorded).canonicalize()?,
        "receipt belongs to another source",
    )?;
    require(
        proof["schema"] == 1
            && proof["source_stopped"] == true
            && proof["source_read_only"] == true,
        "explicit stopped read-only receipt required",
    )?;
    for role in ["node", "indexer"] {
        let service = &proof[role];
        require(
            service["state"] == "stopped"
                && service["desired"] == "stopped"
                && service["exit_code"] == 0
                && service["forced"] == false
                && service["reason"] == "requested_stop"
                && service["id"]
                    .as_str()
                    .map(|s| s.starts_with("service_"))
                    .unwrap_or(false)
                && service["last_run_id"].as_str().is_some(),
            "both service scopes must have stopped cleanly",
        )?;
    }
    let cp = source.join("checkpoint");
    require(
        cp.is_dir() && std::fs::read_dir(cp)?.next().is_none(),
        "pending cross-store checkpoint; do not import inconsistent state",
    )?;
    Ok(json!({"kind":"stopped-runtime-read-only",
        "receipt_sha256":hex::encode(tron_crypto::hash::sha256(&bytes)),
        "receipt":proof}))
}
pub(super) fn run_stopped(source: &Path, out: &Path, receipt: &Path, resume: bool) -> Result<()> {
    let proof = stopped_guard(source, receipt)?;
    run_inner(source, out, resume, proof)
}
fn run_inner(source: &Path, out: &Path, resume: bool, provenance: Value) -> Result<()> {
    let source = source.canonicalize()?;
    require(
        resume || !out.exists(),
        "output exists; explicit resume is required",
    )?;
    std::fs::create_dir_all(out)?;
    let out = out.canonicalize()?;
    require(
        !out.starts_with(&source) && !source.starts_with(&out),
        "source/target paths overlap",
    )?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(out.join(".import.lock"))?;
    require(
        unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0,
        "another importer owns target",
    )?;
    let identity = fingerprint(&source)?;
    let manifest_path = out.join("import.json");
    let mut manifest = if resume {
        let m: Value = serde_json::from_slice(&std::fs::read(&manifest_path)?)?;
        require(
            m["source"] == source.to_string_lossy().as_ref() && m["source_identity"] == identity,
            "source changed since import began",
        )?;
        require(
            m["mode"] == "direct-no-compression-no-compaction-v1",
            "different import settings",
        )?;
        require(
            m.get("source_provenance")
                .map(|p| p == &provenance)
                .unwrap_or(true),
            "source provenance changed since import began",
        )?;
        m
    } else {
        let m = json!({"schema":1,"mode":"direct-no-compression-no-compaction-v1","source":source,
            "source_provenance":provenance,
            "source_identity":identity,"target":out,"native_version":"10.2.1",
            "compression":false,"compaction_scheduler":false,"external_staging":false,
            "batch_bytes":BATCH_BYTES,"memtable_bytes":64*1024*1024,
            "source_open_mode":"read-only","complete":false});
        std::fs::write(&manifest_path, serde_json::to_vec_pretty(&m)?)?;
        m
    };
    manifest["empty_key_encoding"] = json!({
        "column_family":tides::EMPTY_KEY_CF,
        "slot_hex":hex::encode(tides::EMPTY_KEY_SLOT),
        "logical_key_hex":"",
        "nonempty_keys":"unchanged",
        "atomic_with_state_and_cursor":true
    });
    std::fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?)?;
    require(free_bytes(&out)? > FREE_FLOOR, "keep at least 64 GiB free")?;
    let mut log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(out.join("progress.jsonl"))?;
    writeln!(
        log,
        "{}",
        json!({"phase":"start","resume":resume,"source":source,"free_bytes":free_bytes(&out)?})
    )?;
    log.flush()?;
    let started = Instant::now();
    let mut reports = vec![];
    for (_, name) in STORES {
        writeln!(log, "{}", json!({"phase":"store_start","store":name}))?;
        log.flush()?;
        // Re-verify completed stores on resume; no blind trust in a previous report.
        let report = copy_store(
            &source.join("database").join(name),
            &out.join("database").join(name),
            64 * 1024 * 1024,
            None,
            &mut log,
            name,
        )?
        .ok_or("unexpected bounded import")?;
        std::fs::write(
            out.join(format!("verified-{name}.json")),
            serde_json::to_vec_pretty(&report)?,
        )?;
        writeln!(log, "{}", json!({"phase":"store_verified","report":report}))?;
        log.flush()?;
        reports.push(report);
    }
    require(
        fingerprint(&source)? == identity,
        "source changed during conversion",
    )?;
    manifest["complete"] = json!(true);
    manifest["stores"] = json!(reports);
    manifest["elapsed_seconds"] = json!(started.elapsed().as_secs_f64());
    manifest["free_bytes_after"] = json!(free_bytes(&out)?);
    manifest["source_unchanged"] = json!(true);
    std::fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?)?;
    writeln!(
        log,
        "{}",
        json!({"phase":"complete","elapsed_seconds":started.elapsed().as_secs_f64()})
    )?;
    log.flush()?;
    println!("{}", manifest);
    Ok(())
}
pub(super) fn self_test(out: &Path) -> Result<()> {
    require(!out.exists(), "self-test output exists")?;
    std::fs::create_dir_all(out)?;
    let source = out.join("source");
    let target = out.join("target");
    let mut opts = Options::default();
    opts.create_if_missing(true);
    let rock = DB::open(&opts, &source)?;
    let mut batch = ::rocksdb::WriteBatch::default();
    for i in 0u32..32768 {
        let key = i.to_be_bytes();
        let value = vec![
            (i % 251) as u8;
            if i % 13 == 0 {
                0
            } else {
                2048 + (i % 67) as usize
            }
        ];
        batch.put(key, value);
    }
    batch.put([], []);
    rock.write(batch)?;
    rock.flush()?;
    drop(rock);
    let mut log = File::create(out.join("progress.jsonl"))?;
    require(
        copy_store(&source, &target, 256 * 1024, Some(2), &mut log, "test")?.is_none(),
        "bounded import did not stop",
    )?;
    let resumed = copy_store(&source, &target, 256 * 1024, None, &mut log, "test")?
        .ok_or("resume incomplete")?;
    require(
        resumed["rows"] == 32769 && resumed["resumed_from_rows"].as_u64().unwrap_or(0) > 0,
        "resume did not recover committed cursor",
    )?;
    // Compare every source record independently against target after close/reopen.
    let mut ro = Options::default();
    ro.set_max_open_files(64);
    let source = DB::open_for_read_only(&ro, &source, false)?;
    let target = open_target(&target, 256 * 1024)?;
    let tx = target.begin_transaction()?;
    for row in source.iterator(IteratorMode::Start) {
        let (k, v) = row?;
        let (target_cf, key) = if k.is_empty() {
            (
                target.get_column_family(tides::EMPTY_KEY_CF)?,
                tides::EMPTY_KEY_SLOT,
            )
        } else {
            (target.get_column_family("state")?, k.as_ref())
        };
        require(
            tx.get(&target_cf, key)? == v.as_ref(),
            "self-test value mismatch",
        )?;
    }
    let proof = checked_stats(&target)?;
    require(
        proof["sstable_count"].as_i64().unwrap_or(0) > 4,
        "self-test did not exercise multi-SST state",
    )?;
    let empty_cases = empty_key_cases(out)?;
    let report = json!({"self_test":"passed","rows":32769,"empty_key_cases":empty_cases,
        "resumed":resumed,"native":proof,
        "wrapper_invocations":DISABLED_STARTS.load(Ordering::SeqCst)});
    std::fs::write(out.join("result.json"), serde_json::to_vec_pretty(&report)?)?;
    println!("{}", report);
    Ok(())
}

// Degenerate source stores must distinguish absent, empty-valued and nonempty-
// valued logical empty keys, including when the native state CF has no rows.
fn empty_key_cases(out: &Path) -> Result<Vec<Value>> {
    let mut reports = vec![];
    for (name, value, ordinary) in [
        ("only-empty-key", Some(vec![42u8]), false),
        ("empty-key-empty-value", Some(vec![]), false),
        ("empty-store", None, false),
        ("no-empty-key", None, true),
    ] {
        let source = out.join(format!("{name}-source"));
        let target = out.join(format!("{name}-target"));
        let mut options = Options::default();
        options.create_if_missing(true);
        let rock = DB::open(&options, &source)?;
        if let Some(v) = value.as_ref() {
            rock.put([], v)?;
        }
        if ordinary {
            rock.put([0], [55])?;
        }
        rock.flush()?;
        drop(rock);
        let mut log = File::create(out.join(format!("{name}-progress.jsonl")))?;
        let report = copy_store(&source, &target, 64 * 1024, None, &mut log, name)?
            .ok_or("empty-key case incomplete")?;
        require(
            report["rows"] == u64::from(value.is_some()) + u64::from(ordinary),
            "empty-key case row count differs",
        )?;
        let db = open_target(&target, 64 * 1024)?;
        let cf = db.get_column_family(tides::EMPTY_KEY_CF)?;
        let tx = db.begin_transaction()?;
        let found = match tx.get(&cf, tides::EMPTY_KEY_SLOT) {
            Ok(v) => Some(v),
            Err(e) if e.is_not_found() => None,
            Err(e) => return Err(e.into()),
        };
        require(found == value, "empty-key case value differs")?;
        reports.push(report);
    }
    Ok(reports)
}

#[cfg(test)]
mod stopped_guard_tests {
    use super::*;
    fn setup() -> (tempfile::TempDir, PathBuf, PathBuf, Value) {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("runtime");
        std::fs::create_dir_all(source.join("checkpoint")).unwrap();
        let receipt = dir.path().join("stop.json");
        let service = json!({"state":"stopped","desired":"stopped","exit_code":0,
            "forced":false,"reason":"requested_stop","id":"service_test","last_run_id":"test-run"});
        let proof = json!({"schema":1,"source":source,"source_stopped":true,
            "source_read_only":true,"node":service,"indexer":service});
        (dir, source, receipt, proof)
    }
    #[test]
    fn direct_runtime_requires_a_matching_clean_stop_receipt() {
        let (_dir, source, receipt, mut proof) = setup();
        assert!(stopped_guard(&source, &receipt).is_err());
        std::fs::write(&receipt, serde_json::to_vec(&proof).unwrap()).unwrap();
        assert!(stopped_guard(&source, &receipt).is_ok());
        proof["node"]["state"] = json!("running");
        std::fs::write(&receipt, serde_json::to_vec(&proof).unwrap()).unwrap();
        assert!(stopped_guard(&source, &receipt).is_err());
    }
    #[test]
    fn forced_stop_and_pending_cross_store_state_are_rejected() {
        let (_dir, source, receipt, mut proof) = setup();
        proof["node"]["forced"] = json!(true);
        std::fs::write(&receipt, serde_json::to_vec(&proof).unwrap()).unwrap();
        assert!(stopped_guard(&source, &receipt).is_err());
        proof["node"]["forced"] = json!(false);
        std::fs::write(&receipt, serde_json::to_vec(&proof).unwrap()).unwrap();
        std::fs::create_dir(source.join("checkpoint/123")).unwrap();
        assert!(stopped_guard(&source, &receipt).is_err());
    }
    #[test]
    fn receipt_for_a_different_runtime_is_rejected() {
        let (dir, source, receipt, mut proof) = setup();
        let other = dir.path().join("other");
        std::fs::create_dir(&other).unwrap();
        proof["source"] = json!(other);
        std::fs::write(&receipt, serde_json::to_vec(&proof).unwrap()).unwrap();
        assert!(stopped_guard(&source, &receipt).is_err());
    }
}

/// Reproduce the large-store descriptor requirement with small, un-compacted
/// files. This runs in its own process and deliberately starts at nofile=1024.
pub(super) fn fd_self_test(out: &Path) -> Result<()> {
    require(!out.exists(), "descriptor self-test output exists")?;
    std::fs::create_dir_all(out)?;
    let mut limit = std::mem::MaybeUninit::<libc::rlimit>::uninit();
    require(
        unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, limit.as_mut_ptr()) } == 0,
        "getrlimit failed",
    )?;
    let mut limit = unsafe { limit.assume_init() };
    require(limit.rlim_max >= 65536, "test hard limit below 65536")?;
    limit.rlim_cur = 1024;
    require(
        unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) } == 0,
        "test setrlimit failed",
    )?;
    let path = out.join("target");
    let db = open_target(&path, 64 * 1024)?;
    let mut cursor = Cursor::default();
    let mut digest = vec![0u8; 32];
    let mut keys: Vec<Vec<u8>> = (0u32..1100).map(|i| i.to_be_bytes().to_vec()).collect();
    keys.extend([
        vec![],
        vec![0],
        vec![0, 0],
        vec![0, 0, 0, 0, 0],
        vec![255],
        vec![255, 255],
    ]);
    keys.sort();
    keys.dedup();
    for (i, key) in keys.into_iter().enumerate() {
        let key = key.into_boxed_slice();
        let value = vec![(i % 251) as u8; 64].into_boxed_slice();
        fold(&mut digest, &key, &value);
        cursor.rows += 1;
        cursor.logical_bytes += (key.len() + value.len()) as u64;
        cursor.last_key = Some(hex::encode(&key));
        cursor.digest = hex::encode(&digest);
        commit(&db, &[(key, value)], &cursor)?;
        db.flush_memtable()?;
    }
    cursor.copied = true;
    commit(&db, &[], &cursor)?;
    db.checkpoint()?;
    let native = checked_stats(&db)?;
    require(
        native["sstable_count"].as_i64().unwrap_or(0) > 1024,
        "test did not exceed the old descriptor ceiling",
    )?;
    drop(db);
    let mut log = File::create(out.join("progress.jsonl"))?;
    let reopened = verify(&path, &cursor, 64 * 1024, &mut log, "fd-regression")?;
    let report = json!({"self_test":"passed","started_nofile":1024,
        "effective_nofile":tidesdb::raise_open_file_limit(0),"rows":cursor.rows,
        "native":native,"reopened":reopened,"full_digest_verified":true});
    std::fs::write(out.join("result.json"), serde_json::to_vec_pretty(&report)?)?;
    println!("{}", report);
    Ok(())
}
