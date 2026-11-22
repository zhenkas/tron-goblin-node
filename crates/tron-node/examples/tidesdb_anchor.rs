//! Attach verified TRON mainnet genesis and canonical head blocks to a converted execution-state dataset.
//! Run only while the target node is stopped; the source is a read-only gRPC peer.
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tron_proto::Block;
const MAINNET_GENESIS: &str = "00000000000000001ebf88508a03865c71d452e25f4d51194196a1d22b6653dc";

fn index_bootstrap_ops(rows: Vec<(Vec<u8>, Vec<u8>)>, floor: i64, genesis: &[u8]) -> Vec<WriteOp> {
    let mut ops: Vec<_> = rows
        .into_iter()
        .filter(|(k, _)| {
            k.len() == 8
                && k.as_slice() != 0i64.to_be_bytes()
                && k.as_slice() < floor.to_be_bytes().as_slice()
        })
        .map(|(k, _)| WriteOp::Delete(k))
        .collect();
    // TVM derives CHAINID from block-index[0], including signature domains.
    ops.push(WriteOp::Put(0i64.to_be_bytes().to_vec(), genesis.to_vec()));
    ops
}

use tron_chainbase::{
    BlockIndexStore, BlockStore, DynamicPropertiesStore, KvEngine, KvProvider, RocksDbBackend,
    WriteOp,
};
// The RocksDB source must be a stopped/sealed snapshot, never a live primary.
fn frozen_rocks_blocks(
    data: &Path,
    height: i64,
) -> Result<(Block, Block), Box<dyn std::error::Error>> {
    let root = data.join("database");
    let index = BlockIndexStore::new(Arc::new(RocksDbBackend::open_read_only(
        root.join("block-index"),
    )?));
    let blocks = BlockStore::new(Arc::new(RocksDbBackend::open_read_only(
        root.join("block"),
    )?));
    Ok((
        blocks.get(&index.get(height)?)?,
        blocks.get(&index.get(0)?)?,
    ))
}
async fn bootstrap_blocks(
    source: &str,
    height: i64,
) -> Result<(Block, Block), Box<dyn std::error::Error>> {
    if let Some(path) = source.strip_prefix("rocks:") {
        return frozen_rocks_blocks(Path::new(path), height);
    }
    let mut client =
        tron_grpc::proto::wallet_client::WalletClient::connect(source.to_owned()).await?;
    let head = client
        .get_block_by_num(tron_proto::protocol::NumberMessage { num: height })
        .await?
        .into_inner();
    let genesis = client
        .get_block_by_num(tron_proto::protocol::NumberMessage { num: 0 })
        .await?
        .into_inner();
    Ok((head, genesis))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 4 {
        return Err(
            "usage: tidesdb_anchor DATA_DIR GRPC_URL_OR_rocks:SEALED_DATA_DIR REPORT_JSON".into(),
        );
    }
    let data = PathBuf::from(&args[1]);
    let report = PathBuf::from(&args[3]);
    if report.exists() {
        return Err(
            "report already exists; inspect the completed operation before retrying".into(),
        );
    }
    let import: serde_json::Value =
        serde_json::from_slice(&std::fs::read(data.join("import.json"))?)?;
    if import["complete"] != true || import["source_unchanged"] != true {
        return Err("execution conversion was not fully verified".into());
    }
    let provider = KvProvider {
        engine: KvEngine::Tidesdb,
        tidesdb: tron_chainbase::TidesDbOptions {
            compaction_threads: 0,
            ..Default::default()
        },
        ..Default::default()
    };
    let root = data.join("database");
    let props = provider.open(&root.join("properties"), "properties")?;
    let dp = DynamicPropertiesStore::new(props);
    let height = dp
        .latest_block_header_number()
        .ok_or("state has no head number")?;
    let expected = dp
        .latest_block_header_hash()?
        .ok_or("state has no head hash")?;
    let index_be = provider.open(&root.join("block-index"), "block-index")?;
    let index = BlockIndexStore::new(index_be.clone());
    let indexed = index.get(height)?;
    if indexed.as_bytes() != &expected {
        return Err("converted block-index disagrees with state head".into());
    }
    let (block, genesis) = bootstrap_blocks(&args[2], height).await?;
    let actual = tron_types::block_id_from_block(&block)?;
    if actual.as_bytes() != &expected {
        return Err("source block hash does not match converted state".into());
    }
    let genesis_id = tron_types::block_id_from_block(&genesis)?;
    if hex::encode(genesis_id.as_bytes()) != MAINNET_GENESIS {
        return Err("source genesis does not match TRON mainnet".into());
    }
    if let Some(existing) = index_be.get(&0i64.to_be_bytes())? {
        if existing.as_slice() != genesis_id.as_bytes() {
            return Err("converted genesis disagrees with verified source".into());
        }
    }
    // Retain the genesis identity and full TAPOS window. Older locator entries
    // may have been pruned by the serving peer. CHAINID still needs height zero.
    let floor = (height - 65_535).max(0);
    let old = index_be.scan_all()?;
    let ops = index_bootstrap_ops(old, floor, genesis_id.as_bytes());
    let removed = ops
        .iter()
        .filter(|op| matches!(op, WriteOp::Delete(_)))
        .count();
    // All validation above precedes any write to the dataset.
    let blocks = provider.open(&root.join("block"), "block")?;
    let block_store = BlockStore::new(blocks.clone());
    block_store.put(&genesis_id, &genesis)?;
    block_store.put(&actual, &block)?;
    blocks.sync_wal()?;
    index_be.write_batch_sync(&ops)?;
    let result = serde_json::json!({"schema":1,"head":height,"hash":hex::encode(expected),
        "tapos_floor":floor,"old_index_rows_removed":removed,
        "genesis":hex::encode(genesis_id.as_bytes()),"chain_id":728126428u64,
        "source":args[2],"head_block_verified":true,"execution_state_unchanged":true});
    std::fs::write(&report, serde_json::to_vec_pretty(&result)?)?;
    println!("{}", serde_json::to_string(&result)?);
    // Drop all native handles before another process opens this directory.
    drop(index);
    drop(index_be);
    drop(blocks);
    drop(dp);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tron_chainbase::{KvBackend, MemBackend};

    #[test]
    fn pruning_locator_history_preserves_chain_identity_and_tapos() {
        for missing_genesis in [false, true] {
            let db = MemBackend::new();
            let genesis = hex::decode(MAINNET_GENESIS).unwrap();
            if !missing_genesis {
                db.put(&0i64.to_be_bytes(), &genesis).unwrap();
            }
            for n in [1i64, 99, 100, 65_635] {
                db.put(&n.to_be_bytes(), &[n as u8; 32]).unwrap();
            }
            db.write_batch(&index_bootstrap_ops(db.scan_all().unwrap(), 100, &genesis))
                .unwrap();
            assert_eq!(db.get(&0i64.to_be_bytes()).unwrap(), Some(genesis.clone()));
            assert_eq!(
                u32::from_be_bytes(genesis[28..].try_into().unwrap()),
                728126428
            );
            assert!(db.get(&99i64.to_be_bytes()).unwrap().is_none());
            assert!(db.get(&100i64.to_be_bytes()).unwrap().is_some());
            assert!(db.get(&65_635i64.to_be_bytes()).unwrap().is_some());
            assert_eq!(db.scan_all().unwrap().len(), 3);
        }
    }
    #[test]
    fn sealed_rocks_bootstrap_reads_exact_blocks_without_changing_source() {
        fn fingerprint(data: &Path) -> std::collections::BTreeMap<PathBuf, Vec<u8>> {
            let mut files = std::collections::BTreeMap::new();
            for store in ["block", "block-index"] {
                for entry in std::fs::read_dir(data.join("database").join(store)).unwrap() {
                    let path = entry.unwrap().path();
                    if path.is_file() {
                        files.insert(
                            path.strip_prefix(data).unwrap().to_owned(),
                            std::fs::read(path).unwrap(),
                        );
                    }
                }
            }
            files
        }
        let dir = tempfile::tempdir().unwrap();
        {
            let root = dir.path().join("database");
            std::fs::create_dir_all(&root).unwrap();
            let index = BlockIndexStore::new(Arc::new(
                RocksDbBackend::open(root.join("block-index")).unwrap(),
            ));
            let blocks =
                BlockStore::new(Arc::new(RocksDbBackend::open(root.join("block")).unwrap()));
            for n in [0, 7] {
                let block = Block {
                    block_header: Some(tron_proto::BlockHeader {
                        raw_data: Some(tron_proto::block_header::Raw {
                            number: n,
                            timestamp: 17,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                };
                let id = tron_types::block_id_from_block(&block).unwrap();
                index.put(&id).unwrap();
                blocks.put(&id, &block).unwrap();
            }
        }
        let before = fingerprint(dir.path());
        let (head, genesis) = frozen_rocks_blocks(dir.path(), 7).unwrap();
        assert_eq!(head.block_header.unwrap().raw_data.unwrap().number, 7);
        assert_eq!(genesis.block_header.unwrap().raw_data.unwrap().number, 0);
        assert_eq!(
            fingerprint(dir.path()),
            before,
            "read-only bootstrap changed its source"
        );
    }
}
