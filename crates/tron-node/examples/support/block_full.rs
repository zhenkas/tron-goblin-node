//! Replay against a local, full-size clone. Never accepts an unmarked runtime path.
use super::rocks::{stats, verify_seed, MeteredBackend};
use super::*;
use tron_chainbase::{CheckPointV2, WriteOp};

pub(super) fn guard(data: &Path) -> Result<Value> {
    let marker: Value =
        serde_json::from_slice(&std::fs::read(data.join(".benchmark-clone.json"))?)?;
    let source = marker["source"].as_str().ok_or("clone source missing")?;
    require(
        data.canonicalize()? != Path::new(source).canonicalize()?,
        "refusing writes to original runtime",
    )?;
    require(
        marker["source_stopped"] == true,
        "clone was not made from a stopped writer",
    )?;
    require(
        marker["immutable_files_hardlinked"] == true,
        "clone provenance missing",
    )?;
    Ok(marker)
}
fn open(data: &Path) -> Result<BTreeMap<u8, Arc<MeteredBackend>>> {
    STORES
        .iter()
        .map(|(id, name)| {
            Ok((
                *id,
                Arc::new(MeteredBackend::new(Arc::new(RocksDbBackend::open_tuned(
                    data.join("database").join(name),
                    4,
                    64,
                )?))),
            ))
        })
        .collect()
}
fn height(dbs: &BTreeMap<u8, Arc<MeteredBackend>>) -> Result<i64> {
    DynamicPropertiesStore::new(dbs[&5].clone())
        .latest_block_header_number()
        .ok_or("missing clone head".into())
}
fn fixture(input: &Path) -> Result<(Fixture, Vec<u8>)> {
    let bytes = std::fs::read(input.join("fixture.json"))?;
    let f: Fixture = serde_json::from_slice(&bytes)?;
    require(f.schema == 1, "fixture schema")?;
    Ok((f, bytes))
}
fn witnesses(f: &mut Fixture) -> Result<BTreeMap<u8, (String, Reads)>> {
    std::mem::take(&mut f.stores)
        .into_iter()
        .map(|(id, name, d)| Ok((id, (name, Reads::restore(d)?))))
        .collect()
}
pub fn setup(data: &Path, input: &Path) -> Result<()> {
    let provenance = guard(data)?;
    require(!input.exists(), "fixture output exists")?;
    let dbs = open(data)?;
    let before = height(&dbs)?;
    let cp = CheckPointV2::new(data);
    let recovered = tron_executor::replay_pending_checkpoints(&state(&dbs), &cp)?;
    let recovered_head = height(&dbs)?;
    for db in dbs.values() {
        db.db.flush_and_sync()?;
    }
    drop(dbs);
    // Capture a canonical block from this machine, before changing the clone's tip.
    prepare(data, input, None)?;
    let (mut f, bytes) = fixture(input)?;
    let reads = witnesses(&mut f)?;
    let dbs = open(data)?;
    let view = state(&dbs);
    let undo = BlockUndoStore::new(Arc::new(RocksDbBackend::open_tuned(
        data.join("database/block-undo"),
        4,
        64,
    )?));
    let tip = height(&dbs)?;
    require(
        tip >= f.height && tip - f.height <= 1000,
        "unexpected rollback distance",
    )?;
    // Verify the complete undo range exists before applying any rollback.
    for h in f.height..=tip {
        require(undo.get(h)?.is_some(), "missing prestate undo record")?;
    }
    let mut restored = 0usize;
    for h in (f.height..=tip).rev() {
        restored += tron_executor::rollback_block(&view, h, &undo)?;
    }
    require(
        height(&dbs)? == f.height - 1,
        "rollback did not reach parent",
    )?;
    for (id, (_, r)) in &reads {
        verify_seed(dbs[id].db.as_ref(), r)?;
    }
    for db in dbs.values() {
        db.db.flush_and_sync()?;
    }
    let ready = json!({"schema":1,"original_head":before,"recovered_head":recovered_head,"replayed_checkpoint_counts":recovered,
        "height":f.height,"prestate_height":f.height-1,"fixture_sha256":hex::encode(tron_crypto::hash::sha256(&bytes)),
        "rolled_back_blocks":tip-f.height+1,"restored_undo_entries":restored,"all_witness_point_and_scan_values_verified":true,
        "provenance":provenance});
    std::fs::write(
        data.join(".benchmark-ready.json"),
        serde_json::to_vec_pretty(&ready)?,
    )?;
    std::fs::write(
        input.join("full-setup.json"),
        serde_json::to_vec_pretty(&ready)?,
    )?;
    println!("{}", ready);
    Ok(())
}
fn execute_checkpoint(
    view: &StateBackends,
    block: &Block,
    raw: &[u8],
    capture: bool,
    undo: &BlockUndoStore,
    cp: &CheckPointV2,
) -> Result<BlockExecutionReport> {
    let wire = tx_wire_infos_from_block_bytes(raw).ok_or("wire facts")?;
    let parent = block
        .block_header
        .as_ref()
        .and_then(|h| h.raw_data.as_ref())
        .ok_or("block header")?
        .parent_hash
        .as_slice();
    let config = ExecConfig {
        parallel_exec: false,
        capture_state_deltas: capture,
        verify_contract_ret: true,
        verify_tx_trie: false,
        ..Default::default()
    };
    Ok(
        tron_executor::execute_block_with_undo_checkpoint_and_config(
            view,
            block,
            Some(BlockId::from_raw(parent.try_into()?)),
            undo,
            cp,
            &config,
            Some(&wire),
        )?,
    )
}
fn verify_post(dbs: &BTreeMap<u8, Arc<MeteredBackend>>, f: &Fixture) -> Result<()> {
    for (id, key, value) in &f.canonical_writes {
        require(
            dbs[id].db.get(&hex::decode(key)?)? == value.as_ref().map(hex::decode).transpose()?,
            "full database postimage differs",
        )?;
    }
    Ok(())
}
pub fn run(data: &Path, input: &Path, out: &Path, repeats: usize) -> Result<()> {
    let provenance = guard(data)?;
    require((3..=100).contains(&repeats), "repeats must be 3..100")?;
    require(!out.exists(), "full output already exists")?;
    let ready: Value = serde_json::from_slice(&std::fs::read(data.join(".benchmark-ready.json"))?)?;
    let (mut f, bytes) = fixture(input)?;
    let digest = hex::encode(tron_crypto::hash::sha256(&bytes));
    require(
        ready["fixture_sha256"] == digest,
        "clone was prepared for another fixture",
    )?;
    let reads = witnesses(&mut f)?;
    let raw = hex::decode(&f.raw_block)?;
    let block = Block::decode(raw.as_slice())?;
    verify_tx_trie_root_raw(&block, &raw)?;
    require(
        hex::encode(block_id_from_block(&block)?.as_bytes()) == f.block_id,
        "fixture block id",
    )?;
    let stored = TransactionRet::decode(hex::decode(&f.receipts)?.as_slice())?;
    let dbs = open(data)?;
    let mut restore = BTreeMap::<u8, Vec<WriteOp>>::new();
    for (id, key, _) in &f.canonical_writes {
        let key = hex::decode(key)?;
        let before = reads[id]
            .1
            .values
            .get(&key)
            .ok_or("missing canonical write preimage")?;
        restore.entry(*id).or_default().push(match before {
            Some(v) => WriteOp::Put(key, v.clone()),
            None => WriteOp::Delete(key),
        });
    }
    let switch = Arc::new(SwitchBackend(RwLock::new(dbs[&251].clone())));
    tron_chainbase::set_account_asset_backend(switch.clone());
    let view = state(&dbs);
    let undo_db = Arc::new(RocksDbBackend::open_tuned(
        data.join("database/block-undo"),
        4,
        64,
    )?);
    let undo = BlockUndoStore::new(undo_db.clone());
    let cp = CheckPointV2::new(data);
    require(cp.list()?.is_empty(), "unrecovered checkpoint in clone")?;
    std::fs::create_dir_all(out)?;
    let mut modes = vec![];
    for mode in [
        "buffered_wal",
        "sync_wal_per_written_store",
        "checkpoint_and_disk_undo",
    ] {
        let mut timings = vec![];
        let mut counts = vec![];
        let mut first = Value::Null;
        for pass in 0..=repeats {
            if height(&dbs)? == f.height {
                for (id, ops) in &restore {
                    dbs[id].db.write_batch(ops)?;
                    dbs[id].db.sync_wal()?;
                }
            }
            require(
                height(&dbs)? == f.height - 1,
                "clone prestate head mismatch",
            )?;
            for b in dbs.values() {
                b.reset_counts(mode == "sync_wal_per_written_store");
            }
            bench_profile::take_samples();
            bench_profile::set_enabled(pass == 0);
            let t = Instant::now();
            let report = if mode == "checkpoint_and_disk_undo" {
                execute_checkpoint(&view, &block, &raw, pass == 0, &undo, &cp)?
            } else {
                execute(&view, &block, &raw, pass == 0)?
            };
            let ms = t.elapsed().as_secs_f64() * 1000.;
            bench_profile::set_enabled(false);
            let samples = bench_profile::take_samples();
            let io = stats(&dbs);
            check_receipts(&report, &stored)?;
            if pass == 0 {
                check_writes(&report, &f.canonical_writes)?;
            }
            require(
                normalized(&report) == f.reference,
                "full database outcome mismatch",
            )?;
            require(
                io["written_operations"].as_u64() == Some(f.canonical_writes.len() as u64),
                "unexpected full database write count",
            )?;
            verify_post(&dbs, &f)?;
            if pass == 0 {
                first = json!({"full_block_ms":ms,"io":io,"top_level_vm_ms":samples.iter().map(|s|s.nanoseconds as f64/1e6).sum::<f64>(),
                    "capture_deltas_and_vm_profile_enabled":true});
            } else {
                timings.push(ms);
                counts.push(io);
            }
        }
        let med = |field: &str| {
            median(
                &counts
                    .iter()
                    .map(|s| s[field].as_f64().unwrap())
                    .collect::<Vec<_>>(),
            )
        };
        let result = json!({"mode":mode,"full_block_ms":median(&timings),"full_block_samples_ms":timings,
            "point_read_ms":med("point_read_ms"),"scan_ms":med("scan_ms"),"write_batch_ms":med("write_batch_ms"),
            "io_counts":counts,"initial_validation_pass":first,
            "canonical_receipts_verified":true,"canonical_full_write_set_verified":true,"all_written_postimages_verified_each_pass":true});
        println!("{}", json!({"mode_completed":result}),);
        modes.push(result);
    }
    for b in dbs.values() {
        b.db.sync_wal()?;
    }
    undo_db.sync_wal()?;
    let cpu = std::fs::read_to_string("/proc/cpuinfo")?
        .lines()
        .find(|l| l.starts_with("model name"))
        .unwrap_or("")
        .to_owned();
    let result = json!({"schema":1,"passed":true,"height":f.height,"block_id":f.block_id,"fixture_sha256":digest,
        "transactions":block.transactions.len(),"vm_transactions":block.transactions.iter().filter(|t|t.raw_data.as_ref().map_or(false,|r|r.contract.iter().any(|c|c.r#type==30||c.r#type==31))).count(),
        "written_keys":f.canonical_writes.len(),"cpu":cpu,"modes":modes,"setup":ready,"provenance":provenance,
        "execution":{"backing":"full-size cloned RocksDB stores with original SST files; no historical archive backend or witness value cache in timed path",
            "serial_transactions":true,"signature_validation":true,"reset_outside_timer":true,"postimage_validation_outside_timer":true,
            "RocksDB_block_cache":"fresh process for first validation; OS page cache not dropped; later samples warm",
            "first_validation_has_extra_instrumentation":true,"point_and_scan_read_timing_overhead_included":true,
            "write_buffer_mb_per_store":4,"max_open_files_per_store":64,
            "undo":"RAM for first two modes; real cloned block-undo RocksDB for checkpoint mode",
            "checkpoint":"real CheckPointV2 manifest and per-store WAL fsync in checkpoint mode",
            "original_runtime_modified":false}});
    std::fs::write(out.join("report.json"), serde_json::to_vec_pretty(&result)?)?;
    println!("{}", result);
    Ok(())
}

#[cfg(test)]
mod guard_tests {
    use super::*;
    #[test]
    fn original_runtime_path_is_rejected_even_with_a_marker() {
        let dir = tempfile::tempdir().unwrap();
        let marker = json!({"source":dir.path().to_str().unwrap(),"source_stopped":true,"immutable_files_hardlinked":true});
        std::fs::write(
            dir.path().join(".benchmark-clone.json"),
            serde_json::to_vec(&marker).unwrap(),
        )
        .unwrap();
        assert!(guard(dir.path())
            .unwrap_err()
            .to_string()
            .contains("original runtime"));
    }
    #[test]
    fn unmarked_directory_is_rejected_without_creating_files() {
        let dir = tempfile::tempdir().unwrap();
        assert!(guard(dir.path()).is_err());
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }
}

pub fn prepare_first(data: &Path, input: &Path) -> Result<()> {
    guard(data)?;
    require(
        !data.join("first-pass-prepared.json").exists()
            && !data.join("first-pass-started").exists(),
        "clone already claimed or prepared",
    )?;
    let ready: Value = serde_json::from_slice(&std::fs::read(data.join(".benchmark-ready.json"))?)?;
    let (bytes, f, reads) = super::rocks::first_fixture(input)?;
    let digest = hex::encode(tron_crypto::hash::sha256(&bytes));
    require(ready["fixture_sha256"] == digest, "clone fixture mismatch")?;
    require(
        CheckPointV2::new(data).list()?.is_empty(),
        "unrecovered checkpoint in clone",
    )?;
    let dbs = open(data)?;
    let head = height(&dbs)?;
    require(
        head == f.height || head == f.height - 1,
        "unexpected first-pass clone head",
    )?;
    let mut restore = BTreeMap::<u8, Vec<WriteOp>>::new();
    for (id, key, _) in &f.canonical_writes {
        let key = hex::decode(key)?;
        let value = reads[id]
            .1
            .values
            .get(&key)
            .ok_or("missing restore preimage")?;
        restore.entry(*id).or_default().push(match value {
            Some(v) => WriteOp::Put(key, v.clone()),
            None => WriteOp::Delete(key),
        });
    }
    for (id, ops) in &restore {
        dbs[id].db.write_batch(ops)?;
    }
    require(
        height(&dbs)? == f.height - 1,
        "first-pass reset height mismatch",
    )?;
    for (id, (_, r)) in &reads {
        verify_seed(dbs[id].db.as_ref(), r)?;
    }
    for db in dbs.values() {
        db.db.flush_and_sync()?;
    }
    drop(dbs);
    let marker = json!({"schema":1,"fixture_sha256":digest,"height":f.height,
        "path":data.canonicalize()?,"prestate_verified":true,"execution_count":0,
        "full_size":true,"parent_height":f.height-1});
    std::fs::write(
        data.join("first-pass-prepared.json"),
        serde_json::to_vec_pretty(&marker)?,
    )?;
    println!("{}", marker);
    Ok(())
}
