//! Small writable RocksDB replay of the same canonical read witness.
use super::*;
use memory::Scan;
use std::sync::{
    atomic::{AtomicBool, AtomicU64},
    Mutex,
};
use tron_chainbase::{KvError, WriteOp};

type Rows = Vec<(Vec<u8>, Vec<u8>)>;

pub(super) trait BenchBackend: KvBackend {
    fn flush_and_sync(&self) -> std::result::Result<(), KvError>;
}
impl BenchBackend for RocksDbBackend {
    fn flush_and_sync(&self) -> std::result::Result<(), KvError> {
        RocksDbBackend::flush_and_sync(self).map_err(|e| KvError::Backend(e.to_string()))
    }
}
#[derive(Clone, Copy, PartialEq)]
pub(super) enum Engine {
    Rocks,
    #[cfg(feature = "block-tides-bench")]
    Tides,
}
impl Engine {
    fn name(self) -> &'static str {
        match self {
            Self::Rocks => "rocksdb",
            #[cfg(feature = "block-tides-bench")]
            Self::Tides => "tidesdb",
        }
    }
    fn open(self, path: &Path, _create: bool) -> Result<Arc<dyn BenchBackend>> {
        match self {
            Self::Rocks => Ok(Arc::new(RocksDbBackend::open_tuned(path, 4, 64)?)),
            #[cfg(feature = "block-tides-bench")]
            Self::Tides => Ok(Arc::new(super::tides::TidesBackend::open(path, _create)?)),
        }
    }
    fn reopen(self, path: &Path) -> Result<Arc<dyn BenchBackend>> {
        match self {
            Self::Rocks => Ok(Arc::new(RocksDbBackend::open_read_only(path)?)),
            #[cfg(feature = "block-tides-bench")]
            Self::Tides => self.open(path, false),
        }
    }
}
/// No application value cache: each read delegates to its selected native engine.
pub(super) struct MeteredBackend {
    pub(super) db: Arc<dyn BenchBackend>,
    synchronous: AtomicBool,
    reads: AtomicU64,
    scans: AtomicU64,
    batches: AtomicU64,
    writes: AtomicU64,
    write_ns: AtomicU64,
    read_ns: AtomicU64,
    scan_ns: AtomicU64,
    point_value_bytes: AtomicU64,
    scan_key_value_bytes: AtomicU64,
    scan_rows: AtomicU64,
    get_counts: Option<Mutex<BTreeMap<Vec<u8>, u64>>>,
}
impl MeteredBackend {
    pub(super) fn new(db: Arc<dyn BenchBackend>) -> Self {
        Self {
            db,
            synchronous: AtomicBool::new(false),
            reads: AtomicU64::new(0),
            scans: AtomicU64::new(0),
            batches: AtomicU64::new(0),
            writes: AtomicU64::new(0),
            write_ns: AtomicU64::new(0),
            read_ns: AtomicU64::new(0),
            scan_ns: AtomicU64::new(0),
            point_value_bytes: AtomicU64::new(0),
            scan_key_value_bytes: AtomicU64::new(0),
            scan_rows: AtomicU64::new(0),
            get_counts: None,
        }
    }
    pub(super) fn reset_counts(&self, sync: bool) {
        self.synchronous.store(sync, Ordering::Relaxed);
        for count in [
            &self.reads,
            &self.scans,
            &self.batches,
            &self.writes,
            &self.write_ns,
            &self.read_ns,
            &self.scan_ns,
            &self.point_value_bytes,
            &self.scan_key_value_bytes,
            &self.scan_rows,
        ] {
            count.store(0, Ordering::Relaxed);
        }
    }
    fn batch(&self, ops: &[WriteOp], sync: bool) -> std::result::Result<(), KvError> {
        if ops.is_empty() {
            return Ok(());
        }
        let t = Instant::now();
        let result = if sync {
            self.db.write_batch_sync(ops)
        } else {
            self.db.write_batch(ops)
        };
        self.write_ns.fetch_add(
            t.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
        self.batches.fetch_add(1, Ordering::Relaxed);
        self.writes.fetch_add(ops.len() as u64, Ordering::Relaxed);
        result
    }
}
impl KvBackend for MeteredBackend {
    fn get(&self, key: &[u8]) -> std::result::Result<Option<Vec<u8>>, KvError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        if let Some(counts) = &self.get_counts {
            *counts.lock().unwrap().entry(key.to_vec()).or_default() += 1;
        }
        let t = Instant::now();
        let result = self.db.get(key);
        if let Ok(Some(value)) = &result {
            self.point_value_bytes
                .fetch_add(value.len() as u64, Ordering::Relaxed);
        }
        self.read_ns.fetch_add(
            t.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
        result
    }
    fn put(&self, key: &[u8], value: &[u8]) -> std::result::Result<(), KvError> {
        self.write_batch(&[WriteOp::Put(key.to_vec(), value.to_vec())])
    }
    fn delete(&self, key: &[u8]) -> std::result::Result<(), KvError> {
        self.write_batch(&[WriteOp::Delete(key.to_vec())])
    }
    fn write_batch(&self, ops: &[WriteOp]) -> std::result::Result<(), KvError> {
        self.batch(ops, self.synchronous.load(Ordering::Relaxed))
    }
    fn write_batch_sync(&self, ops: &[WriteOp]) -> std::result::Result<(), KvError> {
        self.batch(ops, true)
    }
    fn sync_wal(&self) -> std::result::Result<(), KvError> {
        self.db.sync_wal()
    }
    fn scan_all(&self) -> std::result::Result<Rows, KvError> {
        self.scans.fetch_add(1, Ordering::Relaxed);
        let t = Instant::now();
        let result = self.db.scan_all();
        if let Ok(rows) = &result {
            self.scan_key_value_bytes.fetch_add(
                rows.iter().map(|(k, v)| (k.len() + v.len()) as u64).sum(),
                Ordering::Relaxed,
            );
            self.scan_rows
                .fetch_add(rows.len() as u64, Ordering::Relaxed);
        }
        self.scan_ns.fetch_add(
            t.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
        result
    }
    fn scan_from(&self, key: &[u8], limit: usize) -> std::result::Result<Rows, KvError> {
        self.scans.fetch_add(1, Ordering::Relaxed);
        let t = Instant::now();
        let result = self.db.scan_from(key, limit);
        if let Ok(rows) = &result {
            self.scan_key_value_bytes.fetch_add(
                rows.iter().map(|(k, v)| (k.len() + v.len()) as u64).sum(),
                Ordering::Relaxed,
            );
            self.scan_rows
                .fetch_add(rows.len() as u64, Ordering::Relaxed);
        }
        self.scan_ns.fetch_add(
            t.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
        result
    }
    fn scan_prefix(&self, key: &[u8]) -> std::result::Result<Rows, KvError> {
        self.scans.fetch_add(1, Ordering::Relaxed);
        let t = Instant::now();
        let result = self.db.scan_prefix(key);
        if let Ok(rows) = &result {
            self.scan_key_value_bytes.fetch_add(
                rows.iter().map(|(k, v)| (k.len() + v.len()) as u64).sum(),
                Ordering::Relaxed,
            );
            self.scan_rows
                .fetch_add(rows.len() as u64, Ordering::Relaxed);
        }
        self.scan_ns.fetch_add(
            t.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
        result
    }
    fn scan_back_from(&self, key: &[u8], limit: usize) -> std::result::Result<Rows, KvError> {
        self.scans.fetch_add(1, Ordering::Relaxed);
        let t = Instant::now();
        let result = self.db.scan_back_from(key, limit);
        if let Ok(rows) = &result {
            self.scan_key_value_bytes.fetch_add(
                rows.iter().map(|(k, v)| (k.len() + v.len()) as u64).sum(),
                Ordering::Relaxed,
            );
            self.scan_rows
                .fetch_add(rows.len() as u64, Ordering::Relaxed);
        }
        self.scan_ns.fetch_add(
            t.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
        result
    }
}

pub(super) fn rows(reads: &Reads) -> Result<BTreeMap<Vec<u8>, Vec<u8>>> {
    let mut result = reads
        .values
        .iter()
        .filter_map(|(k, v)| v.as_ref().map(|v| (k.clone(), v.clone())))
        .collect::<BTreeMap<_, _>>();
    for entries in reads.scans.values() {
        for (key, value) in entries {
            if let Some(point) = reads.values.get(key) {
                require(
                    point.as_ref() == Some(value),
                    "conflicting point/scan witness",
                )?;
            }
            if let Some(old) = result.insert(key.clone(), value.clone()) {
                require(old == *value, "conflicting scan witnesses")?;
            }
        }
    }
    Ok(result)
}
pub(super) fn verify_seed<B: KvBackend + ?Sized>(db: &B, reads: &Reads) -> Result<()> {
    for (key, value) in &reads.values {
        require(db.get(key)? == *value, "RocksDB point prestate mismatch")?;
    }
    for (query, expected) in &reads.scans {
        let actual = match query {
            Scan::All => db.scan_all()?,
            Scan::From(key, n) => db.scan_from(key, *n)?,
            Scan::Prefix(key) => db.scan_prefix(key)?,
            Scan::Back(key, n) => db.scan_back_from(key, *n)?,
        };
        require(actual == *expected, "RocksDB scan prestate mismatch")?;
    }
    Ok(())
}
fn verify_final(
    dbs: &BTreeMap<u8, Arc<MeteredBackend>>,
    expected: &BTreeMap<u8, BTreeMap<Vec<u8>, Vec<u8>>>,
) -> Result<()> {
    for (id, rows) in expected {
        let actual = dbs[id]
            .db
            .scan_all()?
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        require(actual == *rows, "persisted RocksDB poststate mismatch")?;
    }
    Ok(())
}
pub(super) fn stats(dbs: &BTreeMap<u8, Arc<MeteredBackend>>) -> Value {
    let sum = |f: fn(&MeteredBackend) -> &AtomicU64| {
        dbs.values()
            .map(|b| f(b).load(Ordering::Relaxed))
            .sum::<u64>()
    };
    json!({
        "point_reads":sum(|b|&b.reads),
        "point_value_bytes":sum(|b|&b.point_value_bytes),
        "scan_key_value_bytes":sum(|b|&b.scan_key_value_bytes),
        "scan_rows":sum(|b|&b.scan_rows),
        "scans":sum(|b|&b.scans),
        "write_batches":sum(|b|&b.batches),
        "written_operations":sum(|b|&b.writes),
        "write_batch_ms":sum(|b|&b.write_ns) as f64 / 1e6,
        "point_read_ms":sum(|b|&b.read_ns) as f64 / 1e6,
        "scan_ms":sum(|b|&b.scan_ns) as f64 / 1e6
    })
}
fn directory_bytes(path: &Path) -> Result<u64> {
    let mut bytes = 0;
    for item in std::fs::read_dir(path)? {
        let item = item?;
        let metadata = item.metadata()?;
        bytes += if metadata.is_dir() {
            directory_bytes(&item.path())?
        } else {
            metadata.len()
        };
    }
    Ok(bytes)
}

pub fn run(input: &Path, out: &Path, repeats: usize) -> Result<()> {
    require((3..=100).contains(&repeats), "repeats must be 3..100")?;
    require(
        !out.exists(),
        "RocksDB output already exists; choose a new directory",
    )?;
    let fixture_bytes = std::fs::read(input.join("fixture.json"))?;
    let fixture: Fixture = serde_json::from_slice(&fixture_bytes)?;
    require(fixture.schema == 1, "fixture schema")?;
    let raw = hex::decode(&fixture.raw_block)?;
    let block = Block::decode(raw.as_slice())?;
    verify_tx_trie_root_raw(&block, &raw)?;
    require(
        hex::encode(block_id_from_block(&block)?.as_bytes()) == fixture.block_id,
        "fixture block id",
    )?;
    let stored = TransactionRet::decode(hex::decode(&fixture.receipts)?.as_slice())?;
    let witnesses = fixture
        .stores
        .into_iter()
        .map(|(id, name, d)| Ok((id, (name, Reads::restore(d)?))))
        .collect::<Result<BTreeMap<_, _>>>()?;
    require(witnesses.len() == STORES.len(), "missing fixture stores")?;
    let mut prestate = BTreeMap::new();
    for (id, (_, reads)) in &witnesses {
        prestate.insert(*id, rows(reads)?);
    }
    let mut poststate = prestate.clone();
    let mut restore = BTreeMap::<u8, Vec<WriteOp>>::new();
    for (id, key, value) in &fixture.canonical_writes {
        let key = hex::decode(key)?;
        let before = witnesses[id]
            .1
            .values
            .get(&key)
            .ok_or("write preimage missing from witness")?;
        restore.entry(*id).or_default().push(match before {
            Some(v) => WriteOp::Put(key.clone(), v.clone()),
            None => WriteOp::Delete(key.clone()),
        });
        match value {
            Some(value) => {
                poststate
                    .get_mut(id)
                    .ok_or("unknown written store")?
                    .insert(key, hex::decode(value)?);
            }
            None => {
                poststate
                    .get_mut(id)
                    .ok_or("unknown written store")?
                    .remove(&key);
            }
        }
    }
    std::fs::create_dir_all(out)?;
    let mut dbs = BTreeMap::new();
    for (id, (name, reads)) in &witnesses {
        require(
            STORES.iter().any(|(i, n)| i == id && n == name),
            "unknown fixture store",
        )?;
        let path = out.join("database").join(name);
        let db = Arc::new(RocksDbBackend::open_tuned(&path, 4, 64)?);
        let ops = prestate[id]
            .iter()
            .map(|(k, v)| WriteOp::Put(k.clone(), v.clone()))
            .collect::<Vec<_>>();
        db.write_batch(&ops)?;
        db.flush_and_sync()?; // Initial import is SST-backed before measurement.
        verify_seed(db.as_ref(), reads)?;
        dbs.insert(*id, Arc::new(MeteredBackend::new(db)));
    }
    let switch = Arc::new(SwitchBackend(RwLock::new(dbs[&251].clone())));
    tron_chainbase::set_account_asset_backend(switch.clone());
    let view = state(&dbs);
    let mut modes = vec![];
    for synchronous in [false, true] {
        let name = if synchronous {
            "sync_wal_per_written_store"
        } else {
            "buffered_wal"
        };
        let mut full_ms = vec![];
        let mut write_ms = vec![];
        let mut counters = vec![];
        for pass in 0..=repeats {
            for (id, ops) in &restore {
                dbs[id].db.write_batch(ops)?;
                dbs[id].db.flush_and_sync()?; // Reset is outside timer.
            }
            for (id, (_, reads)) in &witnesses {
                verify_seed(dbs[id].db.as_ref(), reads)?;
            }
            for db in dbs.values() {
                db.reset_counts(synchronous);
            }
            let t = Instant::now();
            let result = execute(&view, &block, &raw, pass == 0)?;
            let elapsed = t.elapsed().as_secs_f64() * 1000.;
            let counts = stats(&dbs); // Excludes validation reads/scans below.
            check_receipts(&result, &stored)?;
            if pass == 0 {
                check_writes(&result, &fixture.canonical_writes)?;
            }
            require(
                normalized(&result) == fixture.reference,
                "RocksDB replay outcome mismatch",
            )?;
            require(
                counts["written_operations"].as_u64()
                    == Some(fixture.canonical_writes.len() as u64),
                "writes did not reach RocksDB",
            )?;
            verify_final(&dbs, &poststate)?;
            if pass > 0 {
                full_ms.push(elapsed);
                write_ms.push(
                    counts["write_batch_ms"]
                        .as_f64()
                        .ok_or("missing write time")?,
                );
                counters.push(counts);
            }
        }
        modes.push(json!({"mode":name,"full_block_ms":median(&full_ms),"full_block_samples_ms":full_ms,
            "write_batch_ms":median(&write_ms),"write_batch_samples_ms":write_ms,"io_counts":counters,
            "canonical_receipts_verified":true,"canonical_write_set_verified":true,"entire_db_poststate_verified_each_pass":true}));
    }
    // Release the global immutable-asset handle before close/reopen verification.
    *switch.0.write().unwrap() =
        Arc::new(WitnessBackend::sealed("account-asset", Reads::default()));
    drop(view);
    for db in dbs.values() {
        db.db.flush_and_sync()?;
    }
    drop(dbs);
    for (id, (name, _)) in &witnesses {
        let db = RocksDbBackend::open_read_only(out.join("database").join(name))?;
        require(
            db.scan_all()?.into_iter().collect::<BTreeMap<_, _>>() == poststate[id],
            "poststate changed across RocksDB reopen",
        )?;
    }
    let cpu = std::fs::read_to_string("/proc/cpuinfo")?
        .lines()
        .find(|l| l.starts_with("model name"))
        .unwrap_or("")
        .to_owned();
    let report = json!({"schema":1,"passed":true,"height":fixture.height,"block_id":fixture.block_id,
        "fixture_sha256":hex::encode(tron_crypto::hash::sha256(&fixture_bytes)),
        "transactions":block.transactions.len(),"total_energy":stored.transactioninfo.iter().filter_map(|t|t.receipt.as_ref()).map(|r|r.energy_usage_total).sum::<i64>(),
        "seed_rows":prestate.values().map(BTreeMap::len).sum::<usize>(),"logical_seed_bytes":prestate.values().flat_map(BTreeMap::iter).map(|(k,v)|k.len()+v.len()).sum::<usize>(),
        "database_stores":witnesses.len(),"database_directory_bytes":directory_bytes(&out.join("database"))?,
        "reopened_poststate_verified":true,"cpu":cpu,"modes":modes,
        "execution":{"serial_transactions":true,"signature_validation":true,"native_rocksdb_reads_and_writes":true,
            "initial_sst_import_outside_timer":true,"reset_and_validation_outside_timer":true,
            "profile_hooks_enabled":false,"read_cache":"warm RocksDB block cache and OS page cache; not a cold-disk benchmark",
            "write_buffer_mb_per_store":4,"max_open_files_per_store":64,"WAL_enabled":true,"undo":"in-memory, same as RAM baseline",
            "cross_store_checkpoint_manifest":false,"sync_mode":"write_batch_sync for every mutated store batch",
            "validation":"receipts and write set, full DB contents after every pass, close/reopen final DB contents",
            "timer_includes":"original full-block executor plus its direct RocksDB state commit, including selected WAL sync",
            "post_run_flush":"outside timer, then close/reopen verification; buffered mode is not power-loss durable per block"}});
    std::fs::write(out.join("report.json"), serde_json::to_vec_pretty(&report)?)?;
    println!("{}", report);
    Ok(())
}

// Preparation and first execution intentionally run in different OS processes.
pub(super) fn first_fixture(
    input: &Path,
) -> Result<(Vec<u8>, Fixture, BTreeMap<u8, (String, Reads)>)> {
    let bytes = std::fs::read(input.join("fixture.json"))?;
    let mut fixture: Fixture = serde_json::from_slice(&bytes)?;
    require(fixture.schema == 1, "fixture schema")?;
    let witnesses = std::mem::take(&mut fixture.stores)
        .into_iter()
        .map(|(id, name, d)| Ok((id, (name, Reads::restore(d)?))))
        .collect::<Result<BTreeMap<_, _>>>()?;
    require(witnesses.len() == STORES.len(), "missing fixture stores")?;
    for (id, (name, _)) in &witnesses {
        require(
            STORES.iter().any(|(i, n)| i == id && n == name),
            "unknown fixture store",
        )?;
    }
    Ok((bytes, fixture, witnesses))
}

pub fn prepare_first(input: &Path, out: &Path) -> Result<()> {
    prepare_engine(input, out, Engine::Rocks)
}
pub(super) fn prepare_engine(input: &Path, out: &Path, engine: Engine) -> Result<()> {
    require(!out.exists(), "first-pass database already exists")?;
    let (bytes, fixture, witnesses) = first_fixture(input)?;
    std::fs::create_dir_all(out)?;
    let mut seed_rows = 0usize;
    for (_, (name, reads)) in &witnesses {
        let db = engine.open(&out.join("database").join(name), true)?;
        let values = rows(reads)?;
        seed_rows += values.len();
        let ops = values
            .into_iter()
            .map(|(k, v)| WriteOp::Put(k, v))
            .collect::<Vec<_>>();
        db.write_batch(&ops)?;
        db.flush_and_sync()?;
        verify_seed(db.as_ref(), reads)?;
        // Drop the handle here. No execution occurs in the preparation process.
    }
    let marker = json!({"schema":1,"engine":engine.name(),"fixture_sha256":hex::encode(tron_crypto::hash::sha256(&bytes)),
        "height":fixture.height,"path":std::fs::canonicalize(out)?,"seed_rows":seed_rows,
        "prestate_verified":true,"execution_count":0});
    std::fs::write(
        out.join("first-pass-prepared.json"),
        serde_json::to_vec_pretty(&marker)?,
    )?;
    println!("{}", marker);
    Ok(())
}

pub fn run_first(
    input: &Path,
    database: &Path,
    out: &Path,
    synchronous: bool,
    count_gets: bool,
    properties_ram: bool,
    full_size: bool,
    block_cache: bool,
) -> Result<()> {
    run_engine(
        input,
        database,
        out,
        synchronous,
        count_gets,
        properties_ram,
        full_size,
        block_cache,
        Engine::Rocks,
    )
}
pub(super) fn run_engine(
    input: &Path,
    database: &Path,
    out: &Path,
    synchronous: bool,
    count_gets: bool,
    properties_ram: bool,
    full_size: bool,
    block_cache: bool,
    engine: Engine,
) -> Result<()> {
    require(engine == Engine::Rocks || (!full_size && !synchronous && !properties_ram && !block_cache && !count_gets),
        "TidesDB experiment requires small buffered replay without application caches or frequency counting")?;
    require(
        !block_cache || (!synchronous && properties_ram),
        "block-cache requires RAM properties and buffered WAL",
    )?;
    if full_size {
        super::full::guard(database)?;
    }
    require(!out.exists(), "first-pass report directory already exists")?;
    let (bytes, fixture, witnesses) = first_fixture(input)?;
    let hash = hex::encode(tron_crypto::hash::sha256(&bytes));
    let marker: Value =
        serde_json::from_slice(&std::fs::read(database.join("first-pass-prepared.json"))?)?;
    require(
        marker
            .get("engine")
            .and_then(Value::as_str)
            .unwrap_or("rocksdb")
            == engine.name(),
        "prepared database engine mismatch",
    )?;
    require(
        marker["fixture_sha256"] == hash,
        "first-pass fixture hash mismatch",
    )?;
    require(
        marker["path"] == json!(std::fs::canonicalize(database)?),
        "first-pass database path mismatch",
    )?;
    require(
        marker["execution_count"] == 0 && marker["prestate_verified"] == true,
        "database is not an unused prepared first-pass database",
    )?;
    for (_, (name, _)) in &witnesses {
        require(
            database.join("database").join(name).is_dir()
                && (engine != Engine::Rocks
                    || database
                        .join("database")
                        .join(name)
                        .join("CURRENT")
                        .is_file()),
            "prepared store missing",
        )?;
    }
    let raw = hex::decode(&fixture.raw_block)?;
    let block = Block::decode(raw.as_slice())?;
    verify_tx_trie_root_raw(&block, &raw)?;
    require(
        hex::encode(block_id_from_block(&block)?.as_bytes()) == fixture.block_id,
        "fixture block id",
    )?;
    let stored = TransactionRet::decode(hex::decode(&fixture.receipts)?.as_slice())?;
    let mut poststate = witnesses
        .iter()
        .map(|(id, (_, reads))| Ok((*id, rows(reads)?)))
        .collect::<Result<BTreeMap<_, _>>>()?;
    let witness_seed_rows = poststate.values().map(BTreeMap::len).sum::<usize>();
    let witness_seed_key_value_bytes = poststate
        .values()
        .flat_map(BTreeMap::iter)
        .map(|(k, v)| k.len() + v.len())
        .sum::<usize>();
    for (id, key, value) in &fixture.canonical_writes {
        let state = poststate.get_mut(id).ok_or("unknown written store")?;
        let key = hex::decode(key)?;
        if let Some(value) = value {
            state.insert(key, hex::decode(value)?);
        } else {
            state.remove(&key);
        }
    }
    std::fs::create_dir_all(out)?;
    // Atomic claim: a failed attempt is retained and cannot silently be replayed.
    let mut claim = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(database.join("first-pass-started"))?;
    use std::io::Write;
    claim.write_all(
        b"Single execution claimed; never reuse this database for another first pass.\n",
    )?;
    drop(claim);
    let open_probe_before = super::probe::sample();
    let open_cpu_started = super::probe::process_cpu_ns()?;
    let open_started = Instant::now();
    let mut dbs = BTreeMap::new();
    for (id, (name, _)) in &witnesses {
        let db = engine.open(&database.join("database").join(name), false)?;
        let mut metered = MeteredBackend::new(db);
        if count_gets {
            metered.get_counts = Some(Mutex::new(BTreeMap::new()));
        }
        let metered = Arc::new(metered);
        metered.reset_counts(synchronous);
        dbs.insert(*id, metered);
    }
    let open_ms = open_started.elapsed().as_secs_f64() * 1000.;
    let open_cpu_elapsed = super::probe::process_cpu_ns()?.saturating_sub(open_cpu_started);
    let open_probe_after = super::probe::sample();
    let open_process_probe = super::probe::report(
        &open_probe_before,
        &open_probe_after,
        open_cpu_elapsed,
        open_ms,
    );
    let switch = Arc::new(SwitchBackend(RwLock::new(dbs[&251].clone())));
    tron_chainbase::set_account_asset_backend(switch.clone());
    let mut view = state(&dbs);
    let properties = if properties_ram {
        let cache = Arc::new(super::properties::PropertiesMemory::load(dbs[&5].clone())?);
        view.dyn_props = cache.clone();
        dbs[&5].reset_counts(synchronous); // Startup scan is reported separately.
        Some(cache)
    } else {
        None
    };
    let block_caches = if block_cache {
        let mut caches = BTreeMap::new();
        for (id, db) in &dbs {
            let parent: Arc<dyn KvBackend> = if *id == 5 {
                properties
                    .as_ref()
                    .ok_or("properties mirror missing")?
                    .clone()
            } else {
                db.clone()
            };
            caches.insert(*id, Arc::new(super::block_cache::BlockCache::new(parent)));
        }
        view = state(&caches);
        *switch.0.write().unwrap() = caches[&251].clone();
        Some(caches)
    } else {
        None
    };
    let before = stats(&dbs);
    require(
        before["point_reads"] == 0 && before["scans"] == 0 && before["write_batches"] == 0,
        "database access before the first timed execution",
    )?;
    bench_profile::set_enabled(false);
    let probe_before = super::probe::sample();
    let cpu_started = super::probe::process_cpu_ns()?;
    let started = Instant::now();
    let result = execute(&view, &block, &raw, true)?;
    let execution_ms = started.elapsed().as_secs_f64() * 1000.;
    let writeback_started = Instant::now();
    if let Some(caches) = &block_caches {
        for cache in caches.values() {
            cache.flush()?;
        }
    }
    let writeback_ms = writeback_started.elapsed().as_secs_f64() * 1000.;
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.;
    let cpu_elapsed = super::probe::process_cpu_ns()?.saturating_sub(cpu_started);
    let probe_after = super::probe::sample();
    let process_probe = super::probe::report(&probe_before, &probe_after, cpu_elapsed, elapsed_ms);
    let block_cache_stats = block_caches.as_ref().map(|caches| {
        caches.iter().map(|(id,c)|json!({
        "store_id":id,"store":STORES.iter().find(|(i,_)|i==id).unwrap().1,"cache":c.stats()
    })).collect::<Vec<_>>()
    });
    let io = stats(&dbs);
    let properties_stats = properties.as_ref().map(|p| p.stats());
    let frequencies = if count_gets {
        let (summary, all) = get_frequency_report(&dbs)?;
        require(
            summary["total_gets"] == io["point_reads"],
            "get frequency total mismatch",
        )?;
        std::fs::write(
            out.join("get-frequencies.json"),
            serde_json::to_vec_pretty(&all)?,
        )?;
        Some(summary)
    } else {
        None
    };
    // All validation is after the only timed execution.
    check_receipts(&result, &stored)?;
    check_writes(&result, &fixture.canonical_writes)?;
    require(
        normalized(&result) == fixture.reference,
        "first-pass outcome mismatch",
    )?;
    require(
        io["written_operations"].as_u64() == Some(fixture.canonical_writes.len() as u64),
        "first-pass writes did not reach native backend",
    )?;
    if full_size {
        for (id, key, value) in &fixture.canonical_writes {
            require(
                dbs[id].db.get(&hex::decode(key)?)?
                    == value.as_ref().map(hex::decode).transpose()?,
                "full first-pass postimage mismatch",
            )?;
        }
    } else {
        verify_final(&dbs, &poststate)?;
    }
    if let Some(cache) = &properties {
        cache.verify()?;
    }
    *switch.0.write().unwrap() =
        Arc::new(WitnessBackend::sealed("account-asset", Reads::default()));
    drop(view);
    drop(block_caches);
    drop(properties);
    for db in dbs.values() {
        db.db.flush_and_sync()?;
    }
    drop(dbs);
    for (id, (name, _)) in &witnesses {
        let db = engine.reopen(&database.join("database").join(name))?;
        if full_size {
            for (store, key, value) in fixture
                .canonical_writes
                .iter()
                .filter(|(store, _, _)| store == id)
            {
                let _ = store;
                require(
                    db.get(&hex::decode(key)?)? == value.as_ref().map(hex::decode).transpose()?,
                    "full first-pass postimage changed after reopen",
                )?;
            }
        } else {
            require(
                db.scan_all()?.into_iter().collect::<BTreeMap<_, _>>() == poststate[id],
                "first-pass poststate changed after reopen",
            )?;
        }
    }
    let mut report = json!({"schema":1,"passed":true,"height":fixture.height,"block_id":fixture.block_id,
        "fixture_sha256":hash,"transactions":block.transactions.len(),
        "canonical_written_keys":fixture.canonical_writes.len(),
        "mode":if synchronous {"sync_wal_per_written_store"} else {"buffered_wal"},
        "full_block_ms":elapsed_ms,"database_open_ms":open_ms,"io":io,
        "pre_execution_io":before,"execution_count":1,"warmup_executions":0,
        "get_frequencies":frequencies,"latency_benchmark_valid":!count_gets,
        "canonical_receipts_verified":true,"canonical_write_set_verified":true,
        "entire_db_poststate_verified":!full_size,"all_written_postimages_verified":true,"reopened_poststate_verified":true,
        "properties_ram":properties_stats,"full_size":full_size,
        "block_cache":block_cache_stats,"execution_before_final_writeback_ms":execution_ms,"final_writeback_ms":writeback_ms,
        "database_directory_bytes":directory_bytes(&database.join("database"))?,
        "execution":{"fresh_process_required":true,"prestate_verification":"separate preparation process only",
            "pre_execution_key_reads":0,"pre_execution_scans":0,"poststate_validation":"after timer",
            "capture_state_deltas":true,"vm_profile_hooks":false,"serial_transactions":true,
            "signature_validation":true,"undo":"in-memory","checkpoint":false,
            "os_page_cache":"external per-file eviction evidence required; no global cache drop",
            "timer_includes":"original full-block executor and direct native backend commit; open and final flush excluded"}});
    report["witness_seed_rows"] = json!(witness_seed_rows);
    report["witness_seed_key_value_bytes"] = json!(witness_seed_key_value_bytes);
    report["open_process_probe"] = open_process_probe;
    report["engine"] = json!(engine.name());
    report["application_caches"] =
        json!({"properties_ram":properties_ram,"block_cache":block_cache});
    report["engine_layout"] = json!("one native database per state store");
    report["native_block_cache"] =
        json!("enabled inside native engines, fresh at open; no application value caches");
    report["process_probe"] = process_probe;
    std::fs::write(out.join("report.json"), serde_json::to_vec_pretty(&report)?)?;
    println!("{}", report);
    Ok(())
}

fn get_frequency_report(dbs: &BTreeMap<u8, Arc<MeteredBackend>>) -> Result<(Value, Value)> {
    let mut stores = vec![];
    let mut all = vec![];
    let mut total = 0u64;
    let mut unique = 0usize;
    let mut repeated_keys = 0usize;
    for (id, db) in dbs {
        let Some(counts) = &db.get_counts else {
            continue;
        };
        let counts = counts.lock().unwrap();
        let store_total = counts.values().sum::<u64>();
        let store_repeated = counts.values().filter(|n| **n > 1).count();
        let name = STORES
            .iter()
            .find(|(i, _)| i == id)
            .ok_or("unknown counted store")?
            .1;
        total += store_total;
        unique += counts.len();
        repeated_keys += store_repeated;
        stores.push(json!({"store_id":id,"store":name,"gets":store_total,"unique_keys":counts.len(),
            "repeat_gets":store_total-counts.len() as u64,"keys_read_more_than_once":store_repeated}));
        for (key, calls) in counts.iter() {
            all.push(json!({"store_id":id,"store":name,"key_hex":hex::encode(key),"gets":calls}));
        }
    }
    all.sort_by(|a, b| b["gets"].as_u64().cmp(&a["gets"].as_u64()));
    stores.sort_by(|a, b| b["repeat_gets"].as_u64().cmp(&a["repeat_gets"].as_u64()));
    let summary = json!({"key_identity":"store ID plus raw key bytes","total_gets":total,
        "unique_store_keys":unique,"repeat_gets":total-unique as u64,
        "repeat_get_percent":if total==0 {0.0} else {100.0*(total-unique as u64) as f64/total as f64},
        "keys_read_more_than_once":repeated_keys,"keys_read_once":unique-repeated_keys,
        "max_gets_one_store_key":all.first().map(|x|x["gets"].clone()),
        "top_10":all.iter().take(10).collect::<Vec<_>>(),"by_store":stores,
        "scope":"point get calls during the single execute call; scans, preparation and validation excluded"});
    Ok((summary, json!(all)))
}
