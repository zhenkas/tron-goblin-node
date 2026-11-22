//! Read-only reconstruction of incomplete native Firehose captures.
//! Usage: native_capture_audit DATA_DIR SCRATCH_DIR FROM_SEQ MAX_ENTRIES
//! Opens RocksDB secondaries only; writes no consensus, sidecar or Firehose data.
//! stdout is JSONL and may be large: redirect it to private operational storage.
use prost::Message;
use serde_json::json;
use std::{collections::BTreeMap, path::Path, sync::Arc};
use tron_chainbase::{BlockIndexStore, BlockUndoStore, KvBackend, RocksDbBackend, UndoStoreId};
use tron_grpc::firehose_proto as fh;
use tron_index::{
    archive::{ArchiveReader, AtHeight},
    FirehoseLogReader,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn require(ok: bool, message: &str) -> Result<()> {
    if !ok {
        return Err(message.into());
    }
    Ok(())
}
fn account_balance(key: &[u8], image: Option<&[u8]>) -> Result<i64> {
    let Some(image) = image else {
        return Ok(0);
    };
    let a = tron_proto::Account::decode(image)?;
    require(a.address == key, "account image/key mismatch")?;
    Ok(a.balance)
}
fn secondary(primary: &Path, scratch: &Path, name: &str) -> Result<Arc<dyn KvBackend>> {
    require(primary.join("CURRENT").is_file(), "missing source RocksDB")?;
    std::fs::create_dir_all(scratch)?;
    Ok(Arc::new(RocksDbBackend::open_as_secondary(
        primary,
        scratch.join(name),
    )?))
}
pub(crate) fn reconstruct(
    record: &tron_chainbase::BlockUndoRecord,
    archive: &ArchiveReader,
    height: i64,
) -> Result<(usize, Vec<fh::TrxBalanceChange>)> {
    let (base, head) = archive.coverage()?.ok_or("missing archive coverage")?;
    require(
        (base..=head).contains(&height),
        "repair outside archive coverage",
    )?;
    let mut balances = BTreeMap::new();
    for change in record
        .entries
        .iter()
        .filter(|c| c.store == UndoStoreId::Accounts)
    {
        require(
            change.key.len() == 21 && change.key[0] == 0x41,
            "invalid account key",
        )?;
        let before = account_balance(&change.key, change.before.as_deref())?;
        let after_image = match archive.value_at(UndoStoreId::Accounts, &change.key, height)? {
            AtHeight::Value(bytes) => Some(bytes),
            AtHeight::Deleted => None,
            AtHeight::NotCovered => return Err("modified account missing from archive".into()),
        };
        let after = account_balance(&change.key, after_image.as_deref())?;
        require(
            balances
                .insert(change.key.clone(), (before, after))
                .is_none(),
            "duplicate undo account",
        )?;
    }
    let touched = balances.len();
    let changes = balances
        .into_iter()
        .filter(|(_, (a, b))| a != b)
        .map(
            |(address, (balance_before, balance_after))| fh::TrxBalanceChange {
                address,
                balance_before,
                balance_after,
            },
        )
        .collect();
    Ok((touched, changes))
}

fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    require(
        args.len() == 5,
        "usage: native_capture_audit DATA_DIR SCRATCH_DIR FROM_SEQ MAX_ENTRIES",
    )?;
    let data = std::fs::canonicalize(&args[1])?;
    let scratch = std::path::absolute(&args[2])?;
    require(
        !scratch.starts_with(&data),
        "scratch must be outside source data",
    )?;
    let from_seq = args[3].parse::<u64>()?;
    let limit = args[4].parse::<usize>()?;
    require(
        from_seq > 0 && (1..=10000).contains(&limit),
        "invalid bounded range",
    )?;
    let undo = BlockUndoStore::new(secondary(
        &data.join("database/block-undo"),
        &scratch,
        "undo",
    )?);
    let index = BlockIndexStore::new(secondary(
        &data.join("database/block-index"),
        &scratch,
        "index",
    )?);
    let txret = secondary(
        &data.join("database/transactionRetStore"),
        &scratch,
        "receipts",
    )?;
    let archive = ArchiveReader::new(secondary(&data.join("archive/db"), &scratch, "archive")?);
    let (base, head) = archive.coverage()?.ok_or("missing archive coverage")?;
    let entries = FirehoseLogReader::new(data.join("firehose")).read_from(from_seq, limit)?;
    let mut incomplete = 0usize;
    for (seq, payload) in &entries {
        let mut entry = fh::Entry::decode(payload.as_slice())?;
        let Some(fh::entry::Event::Apply(ref mut apply)) = entry.event else {
            continue;
        };
        if apply.trx_balances_complete {
            continue;
        }
        require(
            apply.height >= base && apply.height <= head,
            "incomplete block outside archive",
        )?;
        require(
            index.get(apply.height)?.as_bytes().as_slice() == apply.block_id,
            "noncanonical incomplete entry",
        )?;
        let record = undo.get(apply.height)?.ok_or("undo unavailable")?;
        let (touched, changes) = reconstruct(&record, &archive, apply.height)?;
        apply.trx_balance_changes = changes;
        apply.trx_balances_complete = true;
        let height = apply.height;
        let mut sidecar_key = height.to_be_bytes().to_vec();
        sidecar_key.push(b'B');
        let old_sidecar = txret
            .get(&sidecar_key)?
            .ok_or("missing incomplete sidecar")?;
        let stored = fh::BlockApplied::decode(old_sidecar.as_slice())?;
        require(
            stored.block_id == apply.block_id && !stored.trx_balances_complete,
            "sidecar differs from incomplete entry",
        )?;
        let sidecar = fh::BlockApplied {
            block_id: apply.block_id.clone(),
            trx_balance_changes: apply.trx_balance_changes.clone(),
            trx_balances_complete: true,
            ..Default::default()
        };
        println!(
            "{}",
            json!({
                "seq":seq,"height":height,"blockId":hex::encode(&apply.block_id),
                "accountsTouched":touched,"balanceChanges":apply.trx_balance_changes.len(),
                "oldPayload":hex::encode(payload),"newPayload":hex::encode(entry.encode_to_vec()),
                "sidecarKey":hex::encode(sidecar_key),"oldSidecar":hex::encode(old_sidecar),
                "newSidecar":hex::encode(sidecar.encode_to_vec())
            })
        );
        incomplete += 1;
    }
    eprintln!(
        "{}",
        json!({"mode":"read_only_secondary","entries":entries.len(),"incomplete":incomplete,
        "archiveBase":base,"archiveHead":head,"sourceModified":false})
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn signed_account_images_remain_exact_and_wrong_addresses_fail() {
        let key = [vec![0x41], vec![1; 20]].concat();
        for balance in [i64::MIN, -9222875799431851942, -1, 0, i64::MAX] {
            let image = tron_proto::Account {
                address: key.clone(),
                balance,
                ..Default::default()
            }
            .encode_to_vec();
            assert_eq!(account_balance(&key, Some(&image)).unwrap(), balance);
            assert!(account_balance(&[0x41; 21], Some(&image)).is_err());
        }
        assert_eq!(account_balance(&key, None).unwrap(), 0);
        assert!(account_balance(&key, Some(&[0xff])).is_err());
    }
}
