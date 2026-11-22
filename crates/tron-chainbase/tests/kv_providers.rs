//! Provider contract tests run against both persistent engines.
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};
use tron_chainbase::{
    KvBackend, KvEngine, KvProvider, MemBackend, SessionBackend, SnapshotKvBackend, WriteOp,
};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "tron-kv-provider-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&p).unwrap();
        Self(p)
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
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
fn engines() -> [KvEngine; 2] {
    [KvEngine::Rocksdb, KvEngine::Tidesdb]
}
#[test]
fn point_batch_scan_sync_and_reopen_parity() {
    for engine in engines() {
        let dir = Dir::new();
        let p = provider(engine);
        let path = dir.0.join("state");
        let db = p.open(&path, "account").unwrap();
        let mem = MemBackend::new();
        let ops = vec![
            WriteOp::Put(vec![], vec![]),
            WriteOp::Put(vec![0], vec![1]),
            WriteOp::Put(vec![0, 0], vec![2]),
            WriteOp::Put(vec![0, 255], vec![3]),
            WriteOp::Put(vec![1], vec![4]),
            WriteOp::Put(vec![255], vec![5]),
            WriteOp::Put(vec![255, 255], vec![6]),
            WriteOp::Put(b"value".to_vec(), vec![9; 8192]),
        ];
        db.write_batch_sync(&ops).unwrap();
        mem.write_batch(&ops).unwrap();
        for start in [
            vec![],
            vec![0],
            vec![0, 0],
            vec![0, 1],
            vec![1],
            vec![2],
            vec![255],
            vec![255, 255],
            vec![255, 255, 255],
        ] {
            assert_eq!(
                db.get(&start).unwrap(),
                mem.get(&start).unwrap(),
                "{engine:?} point"
            );
            assert_eq!(
                db.scan_prefix(&start).unwrap(),
                mem.scan_prefix(&start).unwrap(),
                "{engine:?} prefix"
            );
            for limit in [0, 1, 2, 32] {
                assert_eq!(
                    db.scan_from(&start, limit).unwrap(),
                    mem.scan_from(&start, limit).unwrap(),
                    "{engine:?} forward"
                );
                assert_eq!(
                    db.scan_back_from(&start, limit).unwrap(),
                    mem.scan_back_from(&start, limit).unwrap(),
                    "{engine:?} backward"
                );
            }
        }
        let keys: &[&[u8]] = &[b"value", b"", b"missing", &[0], b"value"];
        assert_eq!(db.multi_get(keys).unwrap(), mem.multi_get(keys).unwrap());
        assert!(db.multi_get(&[]).unwrap().is_empty());
        db.write_batch(&[
            WriteOp::Delete(vec![]),
            WriteOp::Put(vec![], b"new".to_vec()),
            WriteOp::Delete(vec![0, 0]),
        ])
        .unwrap();
        mem.write_batch(&[
            WriteOp::Delete(vec![]),
            WriteOp::Put(vec![], b"new".to_vec()),
            WriteOp::Delete(vec![0, 0]),
        ])
        .unwrap();
        db.sync_wal().unwrap();
        drop(db);
        let reopened = p.open(&path, "account").unwrap();
        assert_eq!(
            reopened.scan_all().unwrap(),
            mem.scan_all().unwrap(),
            "{engine:?} reopen"
        );
    }
}
#[test]
fn batches_are_atomic_across_empty_key_and_regular_state_cf() {
    for engine in engines() {
        let dir = Dir::new();
        let db = provider(engine).open(&dir.0.join("db"), "state").unwrap();
        db.write_batch(&[
            WriteOp::Put(vec![], vec![0]),
            WriteOp::Put(b"left".to_vec(), vec![0]),
            WriteOp::Put(b"right".to_vec(), vec![0]),
        ])
        .unwrap();
        let writer = db.clone();
        let done = Arc::new(AtomicBool::new(false));
        let flag = done.clone();
        let thread = std::thread::spawn(move || {
            for i in 1..=250u16 {
                let v = i.to_be_bytes().to_vec();
                writer
                    .write_batch(&[
                        WriteOp::Put(vec![], v.clone()),
                        WriteOp::Put(b"left".to_vec(), v.clone()),
                        WriteOp::Put(b"right".to_vec(), v),
                    ])
                    .unwrap();
            }
            flag.store(true, Ordering::Release);
        });
        let mut reads = 0;
        while !done.load(Ordering::Acquire) || reads < 50 {
            let values = db.multi_get(&[b"left", b"", b"right", b"left"]).unwrap();
            assert!(
                values.iter().all(|v| v == &values[0]),
                "{engine:?} torn batch: {values:?}"
            );
            reads += 1;
        }
        thread.join().unwrap();
    }
}
#[test]
fn wrong_engine_rejects_existing_data_without_converting_it() {
    for engine in engines() {
        let dir = Dir::new();
        let path = dir.0.join("db");
        let selected = provider(engine);
        let db = selected.open(&path, "state").unwrap();
        db.put(b"key", b"value").unwrap();
        db.sync_wal().unwrap();
        drop(db);
        let wrong = provider(if engine == KvEngine::Rocksdb {
            KvEngine::Tidesdb
        } else {
            KvEngine::Rocksdb
        });
        assert!(wrong.open(&path, "state").is_err());
        assert_eq!(
            selected.open(&path, "state").unwrap().get(b"key").unwrap(),
            Some(b"value".to_vec())
        );
    }
}
#[test]
fn market_price_order_identity_and_native_seeks_match_rocksdb() {
    let root = Dir::new();
    let mut outputs = vec![];
    fn key(sell: i64, buy: i64) -> Vec<u8> {
        let mut k = vec![1; 38];
        k.extend_from_slice(&sell.to_be_bytes());
        k.extend_from_slice(&buy.to_be_bytes());
        k
    }
    for engine in engines() {
        let path = root.0.join(format!("{engine:?}"));
        let db = provider(engine)
            .open(&path, "market_pair_price_to_order")
            .unwrap();
        for (s, b, v) in [(0, 0, 0), (1, 2, 1), (20, 10, 2), (10, 15, 3), (2, 4, 4)] {
            db.put(&key(s, b), &[v]).unwrap();
        }
        assert_eq!(db.get(&key(1, 2)).unwrap(), Some(vec![4]));
        let rows = db.scan_prefix(&[1; 38]).unwrap();
        let from = db.scan_from(&key(10, 10), 2).unwrap();
        let back = db.scan_back_from(&key(1, 2), 2).unwrap();
        db.sync_wal().unwrap();
        drop(db);
        let reopened = provider(engine)
            .open(&path, "market_pair_price_to_order")
            .unwrap();
        assert_eq!(rows, reopened.scan_all().unwrap());
        outputs.push((rows, from, back));
    }
    assert_eq!(outputs[0], outputs[1]);
}
#[test]
#[ignore]
fn durable_child() {
    let path = PathBuf::from(std::env::var_os("TRON_KV_CRASH_PATH").unwrap());
    let engine = if std::env::var("TRON_KV_CRASH_ENGINE").unwrap() == "tidesdb" {
        KvEngine::Tidesdb
    } else {
        KvEngine::Rocksdb
    };
    let db = provider(engine).open(&path, "state").unwrap();
    db.write_batch_sync(&[
        WriteOp::Put(vec![], b"empty".to_vec()),
        WriteOp::Put(b"left".to_vec(), b"durable".to_vec()),
        WriteOp::Put(b"right".to_vec(), vec![7; 8192]),
    ])
    .unwrap();
    std::process::exit(37); // No Rust/native destructors: recover committed WAL.
}
#[test]
fn synchronized_batch_recovers_after_abrupt_process_exit() {
    for engine in engines() {
        let dir = Dir::new();
        let path = dir.0.join("db");
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--ignored", "--exact", "durable_child", "--nocapture"])
            .env("TRON_KV_CRASH_PATH", &path)
            .env(
                "TRON_KV_CRASH_ENGINE",
                if engine == KvEngine::Tidesdb {
                    "tidesdb"
                } else {
                    "rocksdb"
                },
            )
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(37));
        let db = provider(engine).open(&path, "state").unwrap();
        assert_eq!(
            db.multi_get(&[b"", b"left", b"right"]).unwrap(),
            vec![
                Some(b"empty".to_vec()),
                Some(b"durable".to_vec()),
                Some(vec![7; 8192])
            ]
        );
    }
}
#[test]
fn overlays_forward_one_parent_batch_and_preserve_tombstones() {
    use std::sync::atomic::AtomicUsize;
    struct Count {
        mem: MemBackend,
        batches: AtomicUsize,
        points: AtomicUsize,
        barriers: AtomicUsize,
    }
    impl KvBackend for Count {
        fn get(&self, k: &[u8]) -> Result<Option<Vec<u8>>, tron_chainbase::KvError> {
            self.points.fetch_add(1, Ordering::Relaxed);
            self.mem.get(k)
        }
        fn multi_get(&self, k: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, tron_chainbase::KvError> {
            self.batches.fetch_add(1, Ordering::Relaxed);
            self.mem.multi_get(k)
        }
        fn put(&self, k: &[u8], v: &[u8]) -> Result<(), tron_chainbase::KvError> {
            self.mem.put(k, v)
        }
        fn delete(&self, k: &[u8]) -> Result<(), tron_chainbase::KvError> {
            self.mem.delete(k)
        }
        fn sync_wal(&self) -> Result<(), tron_chainbase::KvError> {
            self.barriers.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }
    let root = Arc::new(Count {
        mem: MemBackend::new(),
        batches: AtomicUsize::new(0),
        points: AtomicUsize::new(0),
        barriers: AtomicUsize::new(0),
    });
    root.put(b"a", b"root").unwrap();
    root.put(b"b", b"root").unwrap();
    root.put(b"c", b"root").unwrap();
    let snap = Arc::new(SnapshotKvBackend::new(root.clone()));
    snap.advance();
    snap.delete(b"a").unwrap();
    snap.put(b"b", b"snap").unwrap();
    let session = SessionBackend::new(snap.clone());
    session.put(b"b", b"session").unwrap();
    assert_eq!(
        session
            .multi_get(&[b"a", b"b", b"c", b"missing", b"c"])
            .unwrap(),
        vec![
            None,
            Some(b"session".to_vec()),
            Some(b"root".to_vec()),
            None,
            Some(b"root".to_vec())
        ]
    );
    assert_eq!(root.batches.load(Ordering::Relaxed), 1);
    assert_eq!(root.points.load(Ordering::Relaxed), 0);
    session.sync_wal().unwrap();
    assert_eq!(root.barriers.load(Ordering::Relaxed), 1);
    session.revert();
    snap.revoke();
    root.batches.store(0, Ordering::Relaxed);
    root.points.store(0, Ordering::Relaxed);
    session.put(b"c", b"new").unwrap();
    session.put(b"missing", b"created").unwrap();
    let (ops, undo) = session.drain_pending_with_undo().unwrap();
    assert_eq!(ops.len(), 2);
    assert!(undo.contains(&(b"c".to_vec(), Some(b"root".to_vec()))));
    assert!(undo.contains(&(b"missing".to_vec(), None)));
    assert_eq!(root.batches.load(Ordering::Relaxed), 1);
    assert_eq!(root.points.load(Ordering::Relaxed), 0);
}

#[test]
fn raw_rocksdb_open_refuses_tidesdb_without_creating_current() {
    let dir = Dir::new();
    let path = dir.0.join("state");
    let p = provider(KvEngine::Tidesdb);
    let db = p.open(&path, "state").unwrap();
    db.put(b"key", b"value").unwrap();
    drop(db);
    assert!(tron_chainbase::RocksDbBackend::open(&path).is_err());
    assert!(!path.join("CURRENT").exists());
    assert_eq!(
        p.open(&path, "state").unwrap().get(b"key").unwrap(),
        Some(b"value".to_vec())
    );
}

#[test]
fn disabled_background_compaction_flushes_reopens_and_remains_writable() {
    let dir = Dir::new();
    let path = dir.0.join("state");
    let mut p = provider(KvEngine::Tidesdb);
    p.tidesdb.compaction_threads = 0;
    for pass in 0u8..3 {
        let db = p.open(&path, "state").unwrap();
        for i in 0u16..256 {
            db.write_batch(&[
                WriteOp::Put(i.to_be_bytes().to_vec(), vec![pass; 8192]),
                WriteOp::Put(vec![], vec![pass]),
            ])
            .unwrap();
        }
        db.checkpoint().unwrap();
        assert_eq!(db.get(&[]).unwrap(), Some(vec![pass]));
        assert_eq!(db.get(&[0, 1]).unwrap(), Some(vec![pass; 8192]));
        assert_eq!(db.scan_all().unwrap().len(), 257);
    }
}
