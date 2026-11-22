//! Provider integration at the node store boundary, including checkpoint/reorg.
use tron_chainbase::{DynamicPropertiesStore, KvEngine, KvProvider, WriteOp};
use tron_node::{OpenedStores, StorageError};
fn provider(engine: KvEngine) -> KvProvider {
    let mut p = KvProvider {
        engine,
        rocks_tuning: Some((1, 32)),
        ..Default::default()
    };
    p.tidesdb.write_buffer_size_mb = 1;
    p.tidesdb.block_cache_mb = 1;
    p.tidesdb.max_open_sstables = 64;
    p
}
#[test]
fn both_engines_recover_checkpoints_revoke_and_reopen() {
    for engine in [KvEngine::Rocksdb, KvEngine::Tidesdb] {
        let tmp = tempfile::tempdir().unwrap();
        let p = provider(engine);
        let stores = OpenedStores::open_with_provider(tmp.path(), &p).unwrap();
        stores.accounts.put(b"account", b"parent").unwrap();
        let failed: Result<(), String> = stores.snapshots.apply_block(1, || {
            stores
                .accounts
                .write_batch(&[
                    WriteOp::Put(b"account".to_vec(), b"tentative".to_vec()),
                    WriteOp::Put(b"new".to_vec(), b"value".to_vec()),
                ])
                .unwrap();
            assert_eq!(
                stores.accounts.get(b"account").unwrap(),
                Some(b"tentative".to_vec())
            );
            Err("reject block".into())
        });
        assert!(failed.is_err());
        assert_eq!(stores.snapshots.depth(), 0);
        assert_eq!(
            stores.accounts.get(b"account").unwrap(),
            Some(b"parent".to_vec())
        );
        assert_eq!(stores.accounts.get(b"new").unwrap(), None);
        let cp = tron_chainbase::CheckPointV2::new(tmp.path());
        cp.write(&[
            tron_chainbase::CheckpointEntry {
                db_name: "account".into(),
                key: b"account".to_vec(),
                value: Some(b"recovered".to_vec()),
            },
            tron_chainbase::CheckpointEntry {
                db_name: "storage-row".into(),
                key: vec![],
                value: Some(vec![42]),
            },
        ])
        .unwrap();
        assert_eq!(stores.snapshots.recover_from_checkpoints(&cp).unwrap(), 2);
        assert!(cp.list().unwrap().is_empty());
        stores.accounts.sync_wal().unwrap();
        stores.storage_row.sync_wal().unwrap();
        drop(stores);
        let reopened = OpenedStores::open_with_provider(tmp.path(), &p).unwrap();
        assert_eq!(
            reopened.accounts.get(b"account").unwrap(),
            Some(b"recovered".to_vec())
        );
        assert_eq!(reopened.storage_row.get(&[]).unwrap(), Some(vec![42]));
    }
}
#[test]
fn execution_only_conversion_is_rejected_before_creating_missing_stores() {
    for engine in [KvEngine::Rocksdb, KvEngine::Tidesdb] {
        let tmp = tempfile::tempdir().unwrap();
        let p = provider(engine);
        let path = tmp.path().join("database/properties");
        let props = p.open(&path, "properties").unwrap();
        DynamicPropertiesStore::new(props.clone()).save_latest_block_header_number(87_000_000);
        props.sync_wal().unwrap();
        drop(props);
        let result = OpenedStores::open_with_provider(tmp.path(), &p);
        assert!(matches!(result, Err(StorageError::Incomplete(_))));
        assert!(!tmp.path().join("database/block").exists());
        assert!(!tmp.path().join("database/account").exists());
    }
}
