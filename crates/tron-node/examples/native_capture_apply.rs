//! Offline application of a native_capture_audit JSONL plan.
//! Usage: native_capture_apply DATA_DIR PLAN_JSONL NEW_BACKUP_DIR --apply
//! Requires exclusive block-undo/receipt DB locks. All replacements and backups
//! are prepared before the first mutation; original logs/sidecars are retained.
use prost::Message;
use serde::Deserialize;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};
use tron_chainbase::{BlockIndexStore, BlockUndoStore, KvBackend, RocksDbBackend, WriteOp};
use tron_grpc::firehose_proto as fh;
use tron_index::archive::ArchiveReader;
#[allow(dead_code)]
#[path = "native_capture_audit.rs"]
mod audit;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Row {
    seq: u64,
    height: i64,
    block_id: String,
    old_payload: String,
    new_payload: String,
    sidecar_key: String,
    old_sidecar: String,
    new_sidecar: String,
}
fn require(ok: bool, message: &str) -> Result<()> {
    if !ok {
        return Err(message.into());
    }
    Ok(())
}
fn crc(bytes: &[u8]) -> u32 {
    let mut c = flate2::Crc::new();
    c.update(bytes);
    c.sum()
}
fn validate_replacement(old: &[u8], new: &[u8], seq: u64) -> Result<fh::BlockApplied> {
    let mut a = fh::Entry::decode(old)?;
    let b = fh::Entry::decode(new)?;
    require(
        a.encode_to_vec() == old && b.encode_to_vec() == new,
        "unknown or noncanonical wire fields",
    )?;
    require(a.seq == seq && b.seq == seq, "plan sequence differs")?;
    let Some(fh::entry::Event::Apply(ref mut original)) = a.event else {
        return Err("plan is not APPLY".into());
    };
    let Some(fh::entry::Event::Apply(ref replacement)) = b.event else {
        return Err("replacement is not APPLY".into());
    };
    require(
        !original.trx_balances_complete && replacement.trx_balances_complete,
        "invalid completeness transition",
    )?;
    let mut previous: Option<&[u8]> = None;
    for c in &replacement.trx_balance_changes {
        require(
            c.address.len() == 21 && c.address[0] == 0x41,
            "invalid replacement address",
        )?;
        require(
            previous.map_or(true, |p| p < c.address.as_slice()),
            "unsorted or duplicate replacement accounts",
        )?;
        require(
            c.balance_before != c.balance_after,
            "net-zero replacement row",
        )?;
        previous = Some(&c.address);
    }
    original.trx_balance_changes = replacement.trx_balance_changes.clone();
    original.trx_balances_complete = true;
    require(a == b, "replacement changes non-native entry facts")?;
    Ok(replacement.clone())
}
fn rewrite(
    bytes: &[u8],
    plan: &BTreeMap<u64, (Vec<u8>, Vec<u8>)>,
    from: u64,
) -> Result<(Vec<u8>, usize)> {
    require(bytes.starts_with(b"TRNFH001"), "bad segment magic")?;
    let mut out = b"TRNFH001".to_vec();
    let mut offset = 8;
    let mut replaced = 0;
    while offset < bytes.len() {
        require(bytes.len() - offset >= 8, "torn segment header")?;
        let len = u32::from_le_bytes(bytes[offset..offset + 4].try_into()?) as usize;
        let checksum = u32::from_le_bytes(bytes[offset + 4..offset + 8].try_into()?);
        require(
            len <= 64 * 1024 * 1024 && bytes.len() - offset - 8 >= len,
            "torn or oversized frame",
        )?;
        let old = &bytes[offset + 8..offset + 8 + len];
        require(crc(old) == checksum, "bad frame CRC")?;
        let e = fh::Entry::decode(old)?;
        let payload = if let Some((expected, new)) = plan.get(&e.seq) {
            require(old == expected, "current frame differs from audit")?;
            validate_replacement(old, new, e.seq)?;
            replaced += 1;
            new.as_slice()
        } else {
            if e.seq >= from {
                if let Some(fh::entry::Event::Apply(ref a)) = e.event {
                    require(
                        a.trx_balances_complete,
                        "unplanned incomplete frame; regenerate audit",
                    )?;
                }
            }
            old
        };
        out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        out.extend_from_slice(&crc(payload).to_le_bytes());
        out.extend_from_slice(payload);
        offset += 8 + len;
    }
    Ok((out, replaced))
}
fn write_new(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut f = OpenOptions::new().write(true).create_new(true).open(path)?;
    f.set_permissions(fs::Permissions::from_mode(0o600))?;
    f.write_all(bytes)?;
    f.sync_all()?;
    Ok(())
}
fn main() -> Result<()> {
    run(&std::env::args().collect::<Vec<_>>())
}
fn run(args: &[String]) -> Result<()> {
    require(
        args.len() == 5 && args[4] == "--apply",
        "usage: native_capture_apply DATA_DIR PLAN_JSONL NEW_BACKUP_DIR --apply",
    )?;
    let data = fs::canonicalize(&args[1])?;
    let plan_path = fs::canonicalize(&args[2])?;
    let output = std::path::absolute(&args[3])?;
    require(
        !output.starts_with(&data) && !output.exists(),
        "backup directory must be new and outside data",
    )?;
    let db = data.join("database");
    for name in ["block-undo", "block-index", "transactionRetStore"] {
        require(
            db.join(name).join("CURRENT").is_file(),
            "missing source store",
        )?;
    }
    // A running source owns this primary lock; fail before creating output.
    require(data.join("archive/db/CURRENT").is_file(), "missing archive")?;
    let undo_lock: Arc<dyn KvBackend> = Arc::new(RocksDbBackend::open(db.join("block-undo"))?);
    let undo = BlockUndoStore::new(undo_lock);
    let archive = ArchiveReader::new(Arc::new(RocksDbBackend::open(data.join("archive/db"))?));
    let index = BlockIndexStore::new(Arc::new(RocksDbBackend::open(db.join("block-index"))?));
    let receipts = RocksDbBackend::open(db.join("transactionRetStore"))?;
    require(
        fs::metadata(&plan_path)?.len() <= 256 * 1024 * 1024,
        "plan too large",
    )?;
    let input = fs::read_to_string(&plan_path)?;
    let rows = input
        .lines()
        .map(serde_json::from_str::<Row>)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    require(
        !rows.is_empty() && rows.len() <= 10000,
        "empty or oversized repair",
    )?;
    let mut replacements = BTreeMap::new();
    let mut writes = Vec::new();
    let mut last_height = 0;
    for row in &rows {
        require(row.height > last_height, "repair heights must increase")?;
        last_height = row.height;
        let old = hex::decode(&row.old_payload)?;
        let new = hex::decode(&row.new_payload)?;
        let apply = validate_replacement(&old, &new, row.seq)?;
        require(
            apply.height == row.height && hex::encode(&apply.block_id) == row.block_id,
            "plan block differs",
        )?;
        require(
            hex::encode(index.get(row.height)?.as_bytes()) == row.block_id,
            "plan is no longer canonical",
        )?;
        let record = undo.get(row.height)?.ok_or("undo unavailable")?;
        let (_, fresh) = audit::reconstruct(&record, &archive, row.height)?;
        require(
            fresh == apply.trx_balance_changes,
            "repair differs from current undo/archive reconstruction",
        )?;
        let key = hex::decode(&row.sidecar_key)?;
        let mut expected_key = row.height.to_be_bytes().to_vec();
        expected_key.push(b'B');
        require(key == expected_key, "sidecar key differs")?;
        let old_sidecar = hex::decode(&row.old_sidecar)?;
        let new_sidecar = hex::decode(&row.new_sidecar)?;
        require(
            receipts.get(&key)?.as_deref() == Some(old_sidecar.as_slice()),
            "sidecar changed since audit",
        )?;
        let sidecar = fh::BlockApplied::decode(new_sidecar.as_slice())?;
        let expected = fh::BlockApplied {
            block_id: apply.block_id,
            trx_balance_changes: apply.trx_balance_changes,
            trx_balances_complete: true,
            ..Default::default()
        };
        require(
            sidecar == expected,
            "replacement sidecar differs from frame",
        )?;
        require(
            replacements.insert(row.seq, (old, new)).is_none(),
            "duplicate repair sequence",
        )?;
        writes.push(WriteOp::Put(key, new_sidecar));
    }
    let first = *replacements.keys().next().unwrap();
    let mut files = fs::read_dir(data.join("firehose"))?
        .map(|x| x.map(|v| v.path()))
        .collect::<std::io::Result<Vec<PathBuf>>>()?;
    files.retain(|p| p.extension().and_then(|s| s.to_str()) == Some("fhlog"));
    files.sort();
    let starts = files
        .iter()
        .map(|p| {
            p.file_stem()
                .unwrap()
                .to_str()
                .unwrap()
                .strip_prefix("firehose-")
                .unwrap()
                .parse::<u64>()
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut prepared = Vec::new();
    let mut count = 0;
    for (i, path) in files.iter().enumerate() {
        if starts.get(i + 1).map_or(false, |next| *next <= first) {
            continue;
        }
        let bytes = fs::read(path)?;
        let (new, n) = rewrite(&bytes, &replacements, first)?;
        count += n;
        if n > 0 {
            prepared.push((path.clone(), bytes, new));
        }
    }
    require(
        count == rows.len(),
        "not every repair frame was found exactly once",
    )?;
    fs::create_dir(&output)?;
    fs::set_permissions(&output, fs::Permissions::from_mode(0o700))?;
    write_new(&output.join("audit-plan.jsonl"), input.as_bytes())?;
    let mut staged = Vec::new();
    for (path, old, new) in &prepared {
        let name = path.file_name().unwrap().to_str().unwrap();
        write_new(&output.join(format!("{name}.original")), old)?;
        let stage = output.join(format!("{name}.repaired"));
        write_new(&stage, new)?;
        let metadata = fs::metadata(path)?;
        fs::set_permissions(&stage, metadata.permissions())?;
        std::os::unix::fs::chown(&stage, Some(metadata.uid()), Some(metadata.gid()))?;
        staged.push((path.clone(), stage));
    }
    File::open(&output)?.sync_all()?;
    // Sidecars first: a crash can leave an incomplete frame, never a complete
    // frame backed by the old incomplete sidecar. Originals remain in the plan.
    receipts.write_batch_sync(&writes)?;
    for (path, stage) in staged {
        fs::rename(stage, path)?;
    }
    File::open(data.join("firehose"))?.sync_all()?;
    write_new(
        &output.join("applied.json"),
        serde_json::to_string(&serde_json::json!({
            "repaired":count,"segments":prepared.len(),"firstSeq":first,"consensusModified":false
        }))?
        .as_bytes(),
    )?;
    println!(
        "{}",
        serde_json::json!({"repaired":count,"segments":prepared.len(),"backup":output,"consensusModified":false})
    );
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    fn entry(complete: bool) -> Vec<u8> {
        fh::Entry {
            seq: 7,
            event: Some(fh::entry::Event::Apply(fh::BlockApplied {
                height: 10,
                block_id: tron_types::BlockId::from_hash_and_num(&[1; 32], 10)
                    .as_bytes()
                    .to_vec(),
                trx_balances_complete: complete,
                trx_balance_changes: if complete {
                    vec![fh::TrxBalanceChange {
                        address: [vec![0x41], vec![1; 20]].concat(),
                        balance_before: -10,
                        balance_after: -1,
                    }]
                } else {
                    vec![]
                },
                ..Default::default()
            })),
        }
        .encode_to_vec()
    }
    fn frame(payload: &[u8]) -> Vec<u8> {
        let mut v = b"TRNFH001".to_vec();
        v.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        v.extend_from_slice(&crc(payload).to_le_bytes());
        v.extend_from_slice(payload);
        v
    }
    #[test]
    fn signed_repair_preserves_sequence_and_facts() {
        let old = entry(false);
        let new = entry(true);
        validate_replacement(&old, &new, 7).unwrap();
        let plan = BTreeMap::from([(7, (old.clone(), new.clone()))]);
        let (fixed, n) = rewrite(&frame(&old), &plan, 7).unwrap();
        assert_eq!(n, 1);
        assert_eq!(fixed, frame(&new));
        let mut wrong = fh::Entry::decode(new.as_slice()).unwrap();
        if let Some(fh::entry::Event::Apply(ref mut a)) = wrong.event {
            a.timestamp_ms = 1;
        }
        assert!(validate_replacement(&old, &wrong.encode_to_vec(), 7).is_err());
    }
    #[test]
    fn corruption_stale_plan_and_missing_rows_fail() {
        let old = entry(false);
        let new = entry(true);
        let plan = BTreeMap::from([(7, (old.clone(), new))]);
        let mut bad = frame(&old);
        bad[12] ^= 1;
        assert!(rewrite(&bad, &plan, 7).is_err());
        assert!(rewrite(&frame(&old)[..10], &plan, 7).is_err());
        assert!(rewrite(&frame(&old), &BTreeMap::new(), 7).is_err());
        let stale = BTreeMap::from([(7, (vec![0], entry(true)))]);
        assert!(rewrite(&frame(&old), &stale, 7).is_err());
    }
    #[test]
    fn offline_repair_rechecks_archive_preserves_backups_and_reopens() {
        use tron_chainbase::{BlockUndoRecord, UndoEntry, UndoStoreId};
        use tron_index::archive::{ArchiveWriter, DeltaRef};
        let root = std::env::temp_dir().join(format!(
            "native-repair-e2e-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let data = root.join("data");
        let db = data.join("database");
        fs::create_dir_all(&db).unwrap();
        fs::create_dir_all(data.join("firehose")).unwrap();
        fs::create_dir_all(data.join("archive")).unwrap();
        let key = [vec![0x41], vec![1; 20]].concat();
        let image = |balance| {
            tron_proto::Account {
                address: key.clone(),
                balance,
                ..Default::default()
            }
            .encode_to_vec()
        };
        let before = image(-10);
        let after = image(-1);
        let old = entry(false);
        let new = entry(true);
        let block_id = tron_types::BlockId::from_hash_and_num(&[1; 32], 10)
            .as_bytes()
            .to_vec();
        let old_sidecar = fh::BlockApplied {
            block_id: block_id.clone(),
            ..Default::default()
        }
        .encode_to_vec();
        let apply = validate_replacement(&old, &new, 7).unwrap();
        let new_sidecar = fh::BlockApplied {
            block_id: block_id.clone(),
            trx_balance_changes: apply.trx_balance_changes,
            trx_balances_complete: true,
            ..Default::default()
        }
        .encode_to_vec();
        let mut sidecar_key = 10i64.to_be_bytes().to_vec();
        sidecar_key.push(b'B');
        {
            let undo = BlockUndoStore::new(Arc::new(
                RocksDbBackend::open(db.join("block-undo")).unwrap(),
            ));
            undo.put(
                10,
                &BlockUndoRecord {
                    entries: vec![UndoEntry {
                        store: UndoStoreId::Accounts,
                        key: key.clone(),
                        before: Some(before.clone()),
                    }],
                },
            )
            .unwrap();
            let index = RocksDbBackend::open(db.join("block-index")).unwrap();
            index.put(&10i64.to_be_bytes(), &block_id).unwrap();
            let receipts = RocksDbBackend::open(db.join("transactionRetStore")).unwrap();
            receipts.put(&sidecar_key, &old_sidecar).unwrap();
            let archive = ArchiveWriter::new(
                Arc::new(RocksDbBackend::open(data.join("archive/db")).unwrap()),
                None,
                vec![],
            );
            archive.check_or_init().unwrap();
            archive
                .on_block_applied(
                    10,
                    Some(&[DeltaRef {
                        store: UndoStoreId::Accounts,
                        key: &key,
                        before: Some(&before),
                        after: Some(&after),
                    }]),
                )
                .unwrap();
        }
        let name = "firehose-00000000000000000007.fhlog";
        let segment = data.join("firehose").join(name);
        fs::write(&segment, frame(&old)).unwrap();
        fs::set_permissions(&segment, fs::Permissions::from_mode(0o640)).unwrap();
        let plan = root.join("plan.jsonl");
        let row = serde_json::json!({"seq":7,"height":10,"blockId":hex::encode(&block_id),
            "oldPayload":hex::encode(&old),"newPayload":hex::encode(&new),
            "sidecarKey":hex::encode(&sidecar_key),"oldSidecar":hex::encode(&old_sidecar),
            "newSidecar":hex::encode(&new_sidecar)});
        fs::write(&plan, format!("{row}\n")).unwrap();
        let backup = root.join("backup");
        let args = vec![
            "apply".to_owned(),
            data.display().to_string(),
            plan.display().to_string(),
            backup.display().to_string(),
            "--apply".to_owned(),
        ];
        run(&args).unwrap();
        assert_eq!(fs::read(&segment).unwrap(), frame(&new));
        assert_eq!(
            fs::read(backup.join(format!("{name}.original"))).unwrap(),
            frame(&old)
        );
        assert_eq!(
            fs::metadata(&segment).unwrap().permissions().mode() & 0o777,
            0o640
        );
        {
            let receipts = RocksDbBackend::open(db.join("transactionRetStore")).unwrap();
            assert_eq!(receipts.get(&sidecar_key).unwrap(), Some(new_sidecar));
        }
        assert!(backup.join("applied.json").is_file());
        assert!(
            run(&args).is_err(),
            "an existing output cannot silently repeat a repair"
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn active_primary_cannot_be_locked_for_repair() {
        let p = std::env::temp_dir().join(format!(
            "native-capture-lock-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let first = RocksDbBackend::open(&p).unwrap();
        assert!(RocksDbBackend::open(&p).is_err());
        drop(first);
        fs::remove_dir_all(p).unwrap();
    }
}
