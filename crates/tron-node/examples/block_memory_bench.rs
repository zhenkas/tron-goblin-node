//! Real-block replay over an exported, sealed in-memory read witness.
//! Never opens a writable production database. Requires --features block-memory-bench.
#[path = "support/block_cache.rs"]
mod block_cache;
#[cfg(feature = "block-tides-bench")]
#[path = "support/block_convert_probe.rs"]
mod convert_probe;
#[cfg(feature = "block-tides-bench")]
#[path = "support/block_convert_full.rs"]
mod convert_full;
#[path = "support/block_full.rs"]
mod full;
#[path = "support/block_memory.rs"]
mod memory;
#[path = "support/block_probe.rs"]
mod probe;
#[path = "support/block_properties.rs"]
mod properties;
#[path = "support/block_rocks.rs"]
mod rocks;
#[cfg(feature = "block-tides-bench")]
#[path = "support/block_tides.rs"]
mod tides;
use memory::{Dump, HistoricalBackend, Reads, SwitchBackend, WitnessBackend};
use prost::Message;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{atomic::Ordering, Arc, RwLock},
    time::{Duration, Instant},
};
use tron_chainbase::{
    BlockIndexStore, BlockStore, BlockUndoStore, DynamicPropertiesStore, KvBackend, MemBackend,
    RocksDbBackend, SessionBackend, TransactionRetStore, UndoStoreId,
};
use tron_executor::{
    bench_profile, execute_block_with_undo_and_config, BlockExecutionReport, ExecConfig,
    StateBackends,
};
use tron_index::archive::{ArchiveAtBackend, ArchiveReader};
use tron_proto::{Block, TransactionRet};
use tron_types::{
    block_id_from_block, tx_wire_infos_from_block_bytes, verify_tx_trie_root_raw, BlockId,
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const STORES: &[(u8, &str)] = &[
    (0, "account"),
    (1, "witness"),
    (2, "votes"),
    (3, "delegation"),
    (4, "DelegatedResource"),
    (5, "properties"),
    (6, "proposal"),
    (7, "accountid-index"),
    (8, "account-index"),
    (9, "asset-issue"),
    (10, "asset-issue-v2"),
    (11, "contract"),
    (12, "abi"),
    (13, "exchange"),
    (14, "exchange-v2"),
    (15, "market_order"),
    (16, "nullifier"),
    (17, "IncrementalMerkleTree"),
    (18, "code"),
    (19, "storage-row"),
    (20, "contract-state"),
    (21, "block-index"),
    (22, "witness_schedule"),
    (23, "DelegatedResourceAccountIndex"),
    (24, "market_account"),
    (250, "reward-vi"),
    (251, "account-asset"),
];
fn require(b: bool, s: &str) -> Result<()> {
    if b {
        Ok(())
    } else {
        Err(s.into())
    }
}
static PREPARING: std::sync::OnceLock<()> = std::sync::OnceLock::new();
fn read_only(p: impl AsRef<Path>) -> Result<Arc<dyn KvBackend>> {
    require(
        PREPARING.get().is_some(),
        "source databases may only open during preparation",
    )?;
    let mut last = None;
    for _ in 0..10 {
        match RocksDbBackend::open_read_only(p.as_ref()) {
            Ok(db) => return Ok(Arc::new(db)),
            Err(e) => {
                last = Some(e);
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
    Err(last.unwrap().into())
}

fn state<B: KvBackend + 'static>(s: &BTreeMap<u8, Arc<B>>) -> StateBackends {
    let b = |id| s[&id].clone() as Arc<dyn KvBackend>;
    StateBackends {
        accounts: b(0),
        witnesses: b(1),
        votes: b(2),
        delegation: b(3),
        delegated_resources: b(4),
        delegated_resource_account_index: Some(b(23)),
        dyn_props: b(5),
        proposals: b(6),
        name_index: b(7),
        id_index: b(8),
        asset_v1: b(9),
        asset_v2: b(10),
        contracts: b(11),
        abi: b(12),
        exchange_v1: b(13),
        exchange_v2: b(14),
        market_orders: b(15),
        market_account: b(24),
        nullifiers: b(16),
        merkle_trees: Some(b(17)),
        code: Some(b(18)),
        storage_row: Some(b(19)),
        contract_state: Some(b(20)),
        block_index: Some(b(21)),
        witness_schedule: Some(b(22)),
        reward_vi: Some(b(250)),
        account_asset: None,
    }
}
fn sessions(w: &BTreeMap<u8, Arc<WitnessBackend>>) -> BTreeMap<u8, Arc<SessionBackend>> {
    w.iter()
        .map(|(id, b)| (*id, Arc::new(SessionBackend::new(b.clone()))))
        .collect()
}
fn execute(
    s: &StateBackends,
    block: &Block,
    raw: &[u8],
    capture: bool,
) -> Result<BlockExecutionReport> {
    let wire = tx_wire_infos_from_block_bytes(raw)
        .ok_or("cannot derive original transaction wire facts")?;
    require(
        wire.len() == block.transactions.len(),
        "wire transaction count",
    )?;
    let parent = block
        .block_header
        .as_ref()
        .and_then(|h| h.raw_data.as_ref())
        .ok_or("header")?
        .parent_hash
        .as_slice();
    let config = ExecConfig {
        parallel_exec: false,
        capture_state_deltas: capture,
        verify_contract_ret: true,
        verify_tx_trie: false,
        ..Default::default()
    };
    Ok(execute_block_with_undo_and_config(
        s,
        block,
        Some(BlockId::from_raw(parent.try_into()?)),
        &BlockUndoStore::new(Arc::new(MemBackend::new())),
        &config,
        Some(&wire),
    )?)
}
fn normalized(r: &BlockExecutionReport) -> Value {
    json!(r.tx_results.iter().map(|t|json!({
  "id":hex::encode(t.tx_id),"type":format!("{:?}",t.contract_type),"outcome":format!("{:?}",t.outcome),
  "receipt":[t.receipt.energy_usage,t.receipt.energy_fee,t.receipt.origin_energy_usage,t.receipt.energy_usage_total,
   t.receipt.net_usage,t.receipt.net_fee,t.receipt.result as i64,t.receipt.energy_penalty_total],
  "fee":t.actuator_fee+t.receipt.energy_fee+t.receipt.net_fee+t.receipt.multi_sign_fee+t.receipt.memo_fee,
  "return":hex::encode(&t.vm_return_data),
  "logs":t.vm_logs.iter().map(|l|json!({"address":hex::encode(l.address),"topics":l.topics.iter().map(hex::encode).collect::<Vec<_>>(),"data":hex::encode(&l.data)})).collect::<Vec<_>>()
 })).collect::<Vec<_>>())
}
fn check_receipts(r: &BlockExecutionReport, stored: &TransactionRet) -> Result<()> {
    require(
        r.tx_results.len() == stored.transactioninfo.len(),
        "stored receipt count",
    )?;
    for (t, c) in r.tx_results.iter().zip(&stored.transactioninfo) {
        require(t.tx_id.as_slice() == c.id, "receipt tx id")?;
        let a = &t.receipt;
        let b = c.receipt.as_ref().ok_or("stored receipt missing")?;
        require(
            [
                a.energy_usage,
                a.energy_fee,
                a.origin_energy_usage,
                a.energy_usage_total,
                a.net_usage,
                a.net_fee,
                a.result as i64,
                a.energy_penalty_total,
            ] == [
                b.energy_usage,
                b.energy_fee,
                b.origin_energy_usage,
                b.energy_usage_total,
                b.net_usage,
                b.net_fee,
                b.result as i64,
                b.energy_penalty_total,
            ],
            &format!(
                "receipt differs for {}: actual {:?}, expected {:?}",
                hex::encode(t.tx_id),
                [
                    a.energy_usage,
                    a.energy_fee,
                    a.origin_energy_usage,
                    a.energy_usage_total,
                    a.net_usage,
                    a.net_fee,
                    a.result as i64,
                    a.energy_penalty_total
                ],
                [
                    b.energy_usage,
                    b.energy_fee,
                    b.origin_energy_usage,
                    b.energy_usage_total,
                    b.net_usage,
                    b.net_fee,
                    b.result as i64,
                    b.energy_penalty_total
                ]
            ),
        )?;
        require(
            t.actuator_fee + a.energy_fee + a.net_fee + a.multi_sign_fee + a.memo_fee == c.fee,
            "total fee differs",
        )?;
        require(
            t.vm_return_data.as_slice()
                == c.contract_result.first().map(Vec::as_slice).unwrap_or(&[]),
            "return data differs",
        )?;
        require(t.vm_logs.len() == c.log.len(), "log count differs")?;
        for (x, y) in t.vm_logs.iter().zip(&c.log) {
            require(
                x.address.as_slice() == y.address
                    && x.data == y.data
                    && x.topics.iter().map(|x| x.to_vec()).collect::<Vec<_>>() == y.topics,
                "log differs",
            )?;
        }
    }
    Ok(())
}
#[derive(Serialize, Deserialize)]
struct Fixture {
    schema: u8,
    height: i64,
    block_id: String,
    raw_block: String,
    receipts: String,
    canonical_writes: Vec<(u8, String, Option<String>)>,
    stores: Vec<(u8, String, Dump)>,
    reference: Value,
    preparation: Value,
}
fn check_writes(r: &BlockExecutionReport, expected: &[(u8, String, Option<String>)]) -> Result<()> {
    let mut actual = r
        .state_deltas
        .as_ref()
        .ok_or("missing deltas")?
        .iter()
        .map(|d| {
            (
                d.store as u8,
                hex::encode(&d.key),
                d.after.as_ref().map(hex::encode),
            )
        })
        .collect::<Vec<_>>();
    let mut expected = expected.to_vec();
    actual.sort();
    expected.sort();
    if actual != expected {
        let a = actual
            .iter()
            .cloned()
            .map(|(s, k, v)| ((s, k), v))
            .collect::<BTreeMap<_, _>>();
        let b = expected
            .iter()
            .cloned()
            .map(|(s, k, v)| ((s, k), v))
            .collect::<BTreeMap<_, _>>();
        let differences = a
            .iter()
            .filter(|(k, v)| b.get(*k) != Some(*v))
            .take(8)
            .map(|(k, _)| k.clone())
            .collect::<Vec<_>>();
        return Err(format!(
            "canonical write-set differs: actual {}, expected {}, differing keys {:?}",
            actual.len(),
            expected.len(),
            differences
        )
        .into());
    }
    Ok(())
}
fn prepare(data: &Path, out: &Path, requested: Option<i64>) -> Result<()> {
    require(
        !out.exists(),
        "output already exists; keep previous evidence and choose a new directory",
    )?;
    std::fs::create_dir_all(out)?;
    PREPARING
        .set(())
        .map_err(|_| "preparation already started")?;
    let db = data.join("database");
    let props = read_only(db.join("properties"))?;
    let start = DynamicPropertiesStore::new(props.clone())
        .latest_block_header_number()
        .ok_or("missing frozen head")?;
    let solid = DynamicPropertiesStore::new(props.clone())
        .latest_solidified_block_num()
        .ok_or("solid height missing")?;
    let mut live = BTreeMap::new();
    live.insert(5, props);
    for (id, name) in STORES {
        if *id != 5 {
            live.insert(*id, read_only(db.join(name))?);
        }
    }
    let blocks = BlockStore::new(read_only(db.join("block"))?);
    let receipts = TransactionRetStore::new(read_only(db.join("transactionRetStore"))?);
    let end = DynamicPropertiesStore::new(read_only(db.join("properties"))?)
        .latest_block_header_number()
        .ok_or("missing frozen head")?;
    let mut archive = None;
    for _ in 0..100 {
        let reader = ArchiveReader::new(read_only(data.join("archive/db"))?);
        if reader.coverage()?.map_or(false, |(_, head)| head >= end) {
            archive = Some(reader);
            break;
        }
        std::thread::sleep(Duration::from_millis(300));
    }
    let archive = archive.ok_or("archive did not cover all frozen live views")?;
    let (base, head) = archive.coverage()?.ok_or("archive coverage missing")?;
    let index = BlockIndexStore::new(live[&21].clone());
    let heights = if let Some(h) = requested {
        vec![h]
    } else {
        {
            let high = (start - 20).min(solid);
            (high - 11..=high).collect()
        }
    };
    let mut candidates = vec![];
    for h in heights {
        require(
            h > base && h < start - 1 && h <= solid,
            "candidate outside historical coverage or too recent",
        )?;
        let id = index.get(h)?;
        let raw = blocks.get_raw(&id)?;
        let block = Block::decode(raw.as_slice())?;
        let vm = block
            .transactions
            .iter()
            .filter(|t| {
                t.raw_data.as_ref().map_or(false, |r| {
                    r.contract.iter().any(|c| c.r#type == 31 || c.r#type == 30)
                })
            })
            .count();
        candidates.push((vm, block.transactions.len(), h, raw, block));
    }
    candidates.sort_by_key(|x| (x.0, x.1, x.2));
    let selection = candidates
        .iter()
        .map(|(vm, tx, h, _, _)| json!({"height":h,"transactions":tx,"vm_transactions":vm}))
        .collect::<Vec<_>>();
    let (_, _, height, raw, block) = candidates.pop().ok_or("no block candidate")?;
    require(!block.transactions.is_empty(), "empty block")?;
    verify_tx_trie_root_raw(&block, &raw)?;
    let canonical = archive
        .write_set_at(height)?
        .ok_or("canonical archive write-set unavailable")?;
    let canonical_writes = canonical
        .iter()
        .map(|(s, k, v)| (*s as u8, hex::encode(k), v.as_ref().map(hex::encode)))
        .collect::<Vec<_>>();
    let stored = receipts
        .get(height)?
        .ok_or("canonical transaction receipts unavailable")?;
    let mut witnesses = BTreeMap::new();
    for (id, name) in STORES {
        let source = if let Some(store) = UndoStoreId::from_u8(*id) {
            Arc::new(HistoricalBackend(Arc::new(ArchiveAtBackend::new(
                live[id].clone(),
                archive.clone(),
                store,
                height - 1,
            )))) as Arc<dyn KvBackend>
        } else {
            live[id].clone()
        };
        witnesses.insert(*id, Arc::new(WitnessBackend::record(name, source)));
    }
    let switch = Arc::new(SwitchBackend(RwLock::new(witnesses[&251].clone())));
    tron_chainbase::set_account_asset_backend(switch.clone());
    let ss = sessions(&witnesses);
    let t = Instant::now();
    let report = execute(&state(&ss), &block, &raw, true)?;
    let capture_seconds = t.elapsed().as_secs_f64();
    check_receipts(&report, &stored)?;
    std::fs::create_dir_all(out)?;
    let mut final_mismatches = vec![];
    let mut redundant = 0usize;
    for (store, key, value) in &canonical {
        let id = *store as u8;
        let actual = ss[&id].get(key)?;
        let before = ArchiveAtBackend::new(live[&id].clone(), archive.clone(), *store, height - 1)
            .get(key)?;
        if actual != *value {
            let decoded = if id == 0 {
                json!({"actual":actual.as_ref().and_then(|x|tron_proto::Account::decode(x.as_slice()).ok()).map(|x|format!("{:?}",x)),
                      "expected":value.as_ref().and_then(|x|tron_proto::Account::decode(x.as_slice()).ok()).map(|x|format!("{:?}",x))})
            } else {
                Value::Null
            };
            final_mismatches.push(json!({"store":id,"key":hex::encode(key),"before":before.as_ref().map(hex::encode),"actual":actual.as_ref().map(hex::encode),"expected":value.as_ref().map(hex::encode),"decoded":decoded}));
        }
        if before == *value {
            redundant += 1;
        }
    }
    let diag = json!({"height":height,"block_id":hex::encode(block_id_from_block(&block)?.as_bytes()),"transactions":block.transactions.len(),
       "canonical_keys":canonical.len(),"actual_keys":report.state_deltas.as_ref().map_or(0,Vec::len),"redundant_canonical_writes":redundant,
       "final_state_mismatches":final_mismatches,"candidates":selection,"head_start":start,"head_end":end,"archive_head":head});
    std::fs::write(
        out.join("diagnostic.json"),
        serde_json::to_vec_pretty(&diag)?,
    )?;
    println!(
        "{}",
        json!({"height":height,"receipt_validation":true,"final_state_mismatches":final_mismatches.len(),"redundant_canonical_writes":redundant})
    );
    check_writes(&report, &canonical_writes)?;
    // account-asset and reward-vi are immutable imported auxiliary tables.
    // Asset mutations live in versioned account rows; production call sites
    // only read AccountAssetStore. Capture their frozen lookups as well.
    let stats=witnesses.iter().map(|(id,w)|{let d=w.snapshot();json!({"store":id,"name":STORES.iter().find(|x|x.0==*id).unwrap().1,"point_keys":d.values.len(),"known_absent":d.values.values().filter(|v|v.is_none()).count(),"scan_queries":d.scans.len(),"value_bytes":d.bytes()})}).collect::<Vec<_>>();
    let fixture = Fixture {
        schema: 1,
        height,
        block_id: hex::encode(block_id_from_block(&block)?.as_bytes()),
        raw_block: hex::encode(raw),
        receipts: hex::encode(stored.encode_to_vec()),
        canonical_writes,
        stores: witnesses
            .iter()
            .map(|(id, w)| {
                (
                    *id,
                    STORES.iter().find(|x| x.0 == *id).unwrap().1.into(),
                    w.snapshot().dump(),
                )
            })
            .collect(),
        reference: normalized(&report),
        preparation: json!({"live_start_head":start,"live_end_head":end,"archive_base":base,"archive_head":head,"prestate_height":height-1,
   "candidate_policy":"most VM transactions among 12 blocks at head-31..head-20","candidates":selection,"capture_seconds":capture_seconds,
   "store_stats":stats,"canonical_receipts_verified":true,"canonical_write_set_verified":true,"source_read_only":true,"reader":"frozen RocksDB open_read_only handles, including WAL","solid_height":solid,"immutable_auxiliary_stores":["account-asset","reward-vi"]}),
    };
    std::fs::create_dir_all(out)?;
    std::fs::write(out.join("fixture.json"), serde_json::to_vec(&fixture)?)?;
    println!(
        "{}",
        json!({"prepared":true,"height":height,"block_id":fixture.block_id,"transactions":block.transactions.len(),
  "canonical_writes":fixture.canonical_writes.len(),"capture_seconds":capture_seconds,"store_stats":stats})
    );
    // Drop every database and historical source before a replay is even constructed.
    drop(ss);
    drop(witnesses);
    drop(live);
    drop(archive);
    drop(blocks);
    drop(receipts);
    *switch.0.write().unwrap() =
        Arc::new(WitnessBackend::sealed("account-asset", Reads::default()));
    Ok(())
}

fn median(v: &[f64]) -> f64 {
    let mut a = v.to_vec();
    a.sort_by(f64::total_cmp);
    a[a.len() / 2]
}
fn run(input: &Path, repeats: usize) -> Result<()> {
    require((3..=100).contains(&repeats), "repeats must be 3..100")?;
    let bytes = std::fs::read(input.join("fixture.json"))?;
    let fixture: Fixture = serde_json::from_slice(&bytes)?;
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
        .map(|(id, name, d)| {
            Ok((
                id,
                Arc::new(WitnessBackend::sealed(&name, Reads::restore(d)?)),
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    let switch = Arc::new(SwitchBackend(RwLock::new(witnesses[&251].clone())));
    tron_chainbase::set_account_asset_backend(switch);
    let ss = sessions(&witnesses);
    let validated = execute(&state(&ss), &block, &raw, true)?;
    check_receipts(&validated, &stored)?;
    check_writes(&validated, &fixture.canonical_writes)?;
    require(
        normalized(&validated) == fixture.reference,
        "RAM replay differs from archived-state reference",
    )?;
    let mut vm_samples = BTreeMap::<[u8; 32], Vec<f64>>::new();
    let mut vm_totals = vec![];
    for _ in 0..3 {
        let ss = sessions(&witnesses);
        bench_profile::take_samples();
        bench_profile::set_enabled(true);
        let r = execute(&state(&ss), &block, &raw, false)?;
        bench_profile::set_enabled(false);
        let samples = bench_profile::take_samples();
        vm_totals.push(
            samples
                .iter()
                .map(|x| x.nanoseconds as f64 / 1e6)
                .sum::<f64>(),
        );
        for s in samples {
            vm_samples
                .entry(s.tx_id)
                .or_default()
                .push(s.nanoseconds as f64 / 1e6);
        }
        require(
            normalized(&r) == fixture.reference,
            "profile replay differs",
        )?;
    }
    let mut block_ms = vec![];
    let mut reset_ms = vec![];
    for _ in 0..repeats {
        let t = Instant::now();
        let ss = sessions(&witnesses);
        let view = state(&ss);
        reset_ms.push(t.elapsed().as_secs_f64() * 1000.);
        let t = Instant::now();
        let report = execute(&view, &block, &raw, false)?;
        block_ms.push(t.elapsed().as_secs_f64() * 1000.);
        require(
            normalized(&report) == fixture.reference,
            "timed replay differs",
        )?;
    }
    let misses = witnesses
        .values()
        .map(|w| w.misses.load(Ordering::Relaxed))
        .sum::<u64>();
    require(misses == 0, "RAM witness had misses")?;
    let mut expensive = vm_samples
        .into_iter()
        .map(|(id, ms)| {
            json!({"tx_id":hex::encode(id),"vm_ms":median(&ms),"samples_ms":ms,
  "energy":validated.tx_results.iter().find(|t|t.tx_id==id).map(|t|t.receipt.energy_usage_total)})
        })
        .collect::<Vec<_>>();
    expensive.sort_by(|a, b| {
        b["vm_ms"]
            .as_f64()
            .unwrap()
            .total_cmp(&a["vm_ms"].as_f64().unwrap())
    });
    let bytes_in_witness = witnesses
        .values()
        .map(|w| w.snapshot().bytes())
        .sum::<usize>();
    let known_keys = witnesses
        .values()
        .map(|w| w.snapshot().values.len())
        .sum::<usize>();
    let cpu = std::fs::read_to_string("/proc/cpuinfo")?
        .lines()
        .find(|l| l.starts_with("model name"))
        .unwrap_or("")
        .to_owned();
    let report = json!({"schema":1,"passed":true,"height":fixture.height,"block_id":fixture.block_id,"fixture_sha256":hex::encode(tron_crypto::hash::sha256(&bytes)),
  "transactions":block.transactions.len(),"vm_transactions":expensive.len(),"total_energy":validated.tx_results.iter().map(|t|t.receipt.energy_usage_total).sum::<i64>(),
  "full_block_ms":median(&block_ms),"full_block_samples_ms":block_ms,"vm_total_ms":median(&vm_totals),"vm_total_samples_ms":vm_totals,
  "reset_ms":median(&reset_ms),"reset_samples_ms":reset_ms,"most_expensive_vm":expensive.into_iter().take(10).collect::<Vec<_>>(),
  "known_prestate_keys":known_keys,"witness_value_bytes":bytes_in_witness,"witness_misses":misses,"canonical_written_keys":fixture.canonical_writes.len(),
  "canonical_receipts_verified":true,"canonical_full_write_set_verified":true,"all_repeat_outcomes_equal":true,"cpu":cpu,
  "execution":{"transaction_order":"serial canonical order","signature_validation":true,"raw_tx_wire_and_trie_verified":true,"undo":"in-memory","checkpoint":false,
   "input_reset_outside_timer":true,"fixture_load_outside_timer":true,"source_backends_present_during_replay":false,
   "VM_profile":"three separate instrumented passes around complete top-level TVM entry, including nested calls/precompiles/backend access; excludes external fee settlement/signature checking",
   "full_block_profile_hooks":"compiled in but disabled for wall-time samples"},
  "preparation":fixture.preparation});
    std::fs::write(
        input.join("report.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    println!("{}", report);
    Ok(())
}
fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    match args.get(1).map(String::as_str) {
  Some("prepare") if (4..=5).contains(&args.len())=>prepare(&PathBuf::from(&args[2]),&PathBuf::from(&args[3]),args.get(4).map(|s|s.parse()).transpose()?),
  Some("run") if (3..=4).contains(&args.len())=>run(&PathBuf::from(&args[2]),args.get(3).map(|s|s.parse()).transpose()?.unwrap_or(9)),
      Some("rocks") if (4..=5).contains(&args.len())=>rocks::run(&PathBuf::from(&args[2]),&PathBuf::from(&args[3]),args.get(4).map(|s|s.parse()).transpose()?.unwrap_or(9)),
      #[cfg(feature = "block-tides-bench")]
      Some("tides-convert-stopped") if (5..=6).contains(&args.len()) && (args.len()==5 || args[5]=="resume")=>convert_full::run_stopped(&PathBuf::from(&args[2]),&PathBuf::from(&args[3]),&PathBuf::from(&args[4]),args.len()==6),
      #[cfg(feature = "block-tides-bench")]
      Some("tides-convert-full") if (4..=5).contains(&args.len()) && (args.len()==4 || args[4]=="resume")=>convert_full::run(&PathBuf::from(&args[2]),&PathBuf::from(&args[3]),args.len()==5),
      #[cfg(feature = "block-tides-bench")]
      Some("tides-convert-fd-self-test") if args.len()==3=>convert_full::fd_self_test(&PathBuf::from(&args[2])),
      #[cfg(feature = "block-tides-bench")]
      Some("tides-convert-self-test") if args.len()==3=>convert_full::self_test(&PathBuf::from(&args[2])),
      #[cfg(feature = "block-tides-bench")]
      Some("tides-convert-probe") if args.len()==5=>convert_probe::run(&PathBuf::from(&args[2]),&PathBuf::from(&args[3]),args[4].parse()?),
      #[cfg(feature = "block-tides-bench")]
      Some("tides-first-prepare") if args.len()==4=>rocks::prepare_engine(&PathBuf::from(&args[2]),&PathBuf::from(&args[3]),rocks::Engine::Tides),
      #[cfg(feature = "block-tides-bench")]
      Some("tides-first") if args.len()==5=>rocks::run_engine(&PathBuf::from(&args[2]),&PathBuf::from(&args[3]),&PathBuf::from(&args[4]),false,false,false,false,false,rocks::Engine::Tides),
      Some("rocks-first-prepare") if args.len()==4=>rocks::prepare_first(&PathBuf::from(&args[2]),&PathBuf::from(&args[3])),
      Some("rocks-first" | "full-first") if (6..=9).contains(&args.len()) && matches!(args[5].as_str(), "buffered" | "sync") && args[6..].iter().all(|x|matches!(x.as_str(),"count-gets"|"properties-ram"|"block-cache"))=>rocks::run_first(&PathBuf::from(&args[2]),&PathBuf::from(&args[3]),&PathBuf::from(&args[4]),args[5]=="sync",args[6..].iter().any(|x|x=="count-gets"),args[6..].iter().any(|x|x=="properties-ram"||x=="block-cache"),args[1]=="full-first",args[6..].iter().any(|x|x=="block-cache")),
      Some("full-first-prepare") if args.len()==4=>full::prepare_first(&PathBuf::from(&args[2]),&PathBuf::from(&args[3])) ,
      Some("full-setup") if args.len()==4=>full::setup(&PathBuf::from(&args[2]),&PathBuf::from(&args[3])),
      Some("full") if (5..=6).contains(&args.len())=>full::run(&PathBuf::from(&args[2]),&PathBuf::from(&args[3]),&PathBuf::from(&args[4]),args.get(5).map(|s|s.parse()).transpose()?.unwrap_or(9)),
  _=>Err("usage: block_memory_bench prepare DATA_DIR NEW_OUTPUT [HEIGHT] | run FIXTURE_DIRECTORY [REPEATS] | rocks FIXTURE_DIRECTORY NEW_OUTPUT [REPEATS] | tides-convert-stopped STOPPED_RUNTIME FINAL_OUTPUT STOP_RECEIPT [resume] | tides-convert-full FULL_CLONE FINAL_OUTPUT [resume] | tides-convert-fd-self-test NEW_OUTPUT | tides-convert-self-test NEW_OUTPUT | tides-convert-probe FULL_CLONE NEW_OUTPUT SAMPLE_MIB | tides-first-prepare FIXTURE NEW_DATABASE | tides-first FIXTURE PREPARED_DATABASE NEW_OUTPUT | rocks-first-prepare FIXTURE NEW_DATABASE | rocks-first FIXTURE PREPARED_DATABASE NEW_OUTPUT buffered|sync [count-gets] [properties-ram] [block-cache] | full-first-prepare CLONE FIXTURE | full-first FIXTURE CLONE NEW_OUTPUT buffered|sync [properties-ram] [block-cache] | full-setup CLONE_DATA NEW_FIXTURE | full CLONE_DATA FIXTURE NEW_OUTPUT [REPEATS]".into())
 }
}
