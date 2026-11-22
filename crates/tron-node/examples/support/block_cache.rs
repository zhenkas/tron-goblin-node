//! Benchmark-only block-scoped read memoization and deferred writeback.
use super::*;
use memory::Scan;
use std::sync::Mutex;
use tron_chainbase::{KvError, WriteOp};
type Rows = Vec<(Vec<u8>, Vec<u8>)>;
#[derive(Default)]
struct Cache {
    points: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    ranges: BTreeMap<Scan, Rows>,
    pending: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    gets: u64,
    point_hits: u64,
    negative_hits: u64,
    pending_hits: u64,
    base_gets: u64,
    scans: u64,
    scan_hits: u64,
    base_scans: u64,
    write_calls: u64,
    logical_writes: u64,
    flushed_keys: u64,
    flush_batches: u64,
}
pub(super) struct BlockCache {
    base: Arc<dyn KvBackend>,
    cache: Mutex<Cache>,
}
impl BlockCache {
    pub(super) fn new(base: Arc<dyn KvBackend>) -> Self {
        Self {
            base,
            cache: Mutex::new(Cache::default()),
        }
    }
    fn base_scan(&self, c: &mut Cache, q: Scan) -> std::result::Result<Rows, KvError> {
        if let Some(rows) = c.ranges.get(&q) {
            c.scan_hits += 1;
            return Ok(rows.clone());
        }
        let rows = match &q {
            Scan::All => self.base.scan_all()?,
            Scan::From(k, n) => self.base.scan_from(k, *n)?,
            Scan::Prefix(k) => self.base.scan_prefix(k)?,
            Scan::Back(k, n) => self.base.scan_back_from(k, *n)?,
        };
        c.base_scans += 1;
        // Positive scan rows are also valid immutable-base point-read results.
        // Non-returned rows are never assumed absent from a bounded scan.
        for (k, v) in &rows {
            c.points.entry(k.clone()).or_insert_with(|| Some(v.clone()));
        }
        c.ranges.insert(q, rows.clone());
        Ok(rows)
    }
    fn scan(&self, q: Scan) -> std::result::Result<Rows, KvError> {
        let mut c = self.cache.lock().unwrap();
        c.scans += 1;
        let relevant = |k: &[u8]| match &q {
            Scan::All => true,
            Scan::Prefix(p) => k.starts_with(p),
            Scan::From(start, _) => k >= start.as_slice(),
            Scan::Back(before, _) => k < before.as_slice(),
        };
        let deletes = c
            .pending
            .iter()
            .filter(|(k, v)| v.is_none() && relevant(k))
            .count();
        let fetch = match &q {
            Scan::From(k, n) => Scan::From(k.clone(), n.saturating_add(deletes)),
            Scan::Back(k, n) => Scan::Back(k.clone(), n.saturating_add(deletes)),
            other => other.clone(),
        };
        if matches!(&q, Scan::From(_, 0) | Scan::Back(_, 0)) {
            return Ok(vec![]);
        }
        let mut rows = self
            .base_scan(&mut c, fetch)?
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        for (k, v) in &c.pending {
            if !relevant(k) {
                continue;
            }
            if let Some(v) = v {
                rows.insert(k.clone(), v.clone());
            } else {
                rows.remove(k);
            }
        }
        Ok(match q {
            Scan::From(_, n) => rows.into_iter().take(n).collect(),
            Scan::Back(_, n) => rows.into_iter().rev().take(n).collect(),
            _ => rows.into_iter().collect(),
        })
    }
    pub(super) fn flush(&self) -> std::result::Result<(), KvError> {
        let mut c = self.cache.lock().unwrap();
        if c.pending.is_empty() {
            return Ok(());
        }
        let ops = c
            .pending
            .iter()
            .map(|(k, v)| match v {
                Some(v) => WriteOp::Put(k.clone(), v.clone()),
                None => WriteOp::Delete(k.clone()),
            })
            .collect::<Vec<_>>();
        // On failure preserve pending writes; the benchmark stops and does not retry.
        self.base.write_batch(&ops)?;
        let pending = std::mem::take(&mut c.pending);
        c.flushed_keys += pending.len() as u64;
        c.flush_batches += 1;
        for (k, v) in pending {
            c.points.insert(k, v);
        }
        // The persistent base changed; old scan windows no longer describe it.
        c.ranges.clear();
        Ok(())
    }
    pub(super) fn stats(&self) -> Value {
        let c = self.cache.lock().unwrap();
        let point_bytes = c
            .points
            .iter()
            .map(|(k, v)| k.len() + v.as_ref().map_or(0, Vec::len))
            .sum::<usize>();
        json!({"logical_gets":c.gets,"point_cache_hits":c.point_hits,"negative_cache_hits":c.negative_hits,
            "pending_write_hits":c.pending_hits,"base_gets":c.base_gets,
            "logical_scans":c.scans,"scan_cache_hits":c.scan_hits,"base_scans":c.base_scans,
            "write_calls":c.write_calls,"logical_write_operations":c.logical_writes,
            "flushed_keys":c.flushed_keys,"flush_batches":c.flush_batches,"pending_keys":c.pending.len(),
            "cached_point_keys_after_flush":c.points.len(),"cached_point_logical_bytes_after_flush":point_bytes})
    }
}
impl KvBackend for BlockCache {
    fn get(&self, k: &[u8]) -> std::result::Result<Option<Vec<u8>>, KvError> {
        let mut c = self.cache.lock().unwrap();
        c.gets += 1;
        if let Some(v) = c.pending.get(k).cloned() {
            c.pending_hits += 1;
            return Ok(v);
        }
        if let Some(v) = c.points.get(k).cloned() {
            c.point_hits += 1;
            if v.is_none() {
                c.negative_hits += 1
            }
            return Ok(v);
        }
        let v = self.base.get(k)?;
        c.base_gets += 1;
        c.points.insert(k.to_vec(), v.clone());
        Ok(v)
    }
    fn put(&self, k: &[u8], v: &[u8]) -> std::result::Result<(), KvError> {
        self.write_batch(&[WriteOp::Put(k.to_vec(), v.to_vec())])
    }
    fn delete(&self, k: &[u8]) -> std::result::Result<(), KvError> {
        self.write_batch(&[WriteOp::Delete(k.to_vec())])
    }
    fn write_batch(&self, ops: &[WriteOp]) -> std::result::Result<(), KvError> {
        let mut c = self.cache.lock().unwrap();
        c.write_calls += 1;
        c.logical_writes += ops.len() as u64;
        for op in ops {
            match op {
                WriteOp::Put(k, v) => {
                    c.pending.insert(k.clone(), Some(v.clone()));
                }
                WriteOp::Delete(k) => {
                    c.pending.insert(k.clone(), None);
                }
            }
        }
        Ok(())
    }
    fn write_batch_sync(&self, _: &[WriteOp]) -> std::result::Result<(), KvError> {
        Err(KvError::Backend(
            "block-cache experiment only supports buffered writeback".into(),
        ))
    }
    fn sync_wal(&self) -> std::result::Result<(), KvError> {
        Err(KvError::Backend(
            "block-cache experiment does not fsync".into(),
        ))
    }
    fn scan_all(&self) -> std::result::Result<Rows, KvError> {
        self.scan(Scan::All)
    }
    fn scan_from(&self, k: &[u8], n: usize) -> std::result::Result<Rows, KvError> {
        self.scan(Scan::From(k.to_vec(), n))
    }
    fn scan_prefix(&self, k: &[u8]) -> std::result::Result<Rows, KvError> {
        self.scan(Scan::Prefix(k.to_vec()))
    }
    fn scan_back_from(&self, k: &[u8], n: usize) -> std::result::Result<Rows, KvError> {
        self.scan(Scan::Back(k.to_vec(), n))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicU64};
    struct Disk {
        mem: MemBackend,
        gets: AtomicU64,
        scans: AtomicU64,
        fail: AtomicBool,
    }
    impl KvBackend for Disk {
        fn get(&self, k: &[u8]) -> std::result::Result<Option<Vec<u8>>, KvError> {
            self.gets.fetch_add(1, Ordering::Relaxed);
            self.mem.get(k)
        }
        fn put(&self, k: &[u8], v: &[u8]) -> std::result::Result<(), KvError> {
            self.mem.put(k, v)
        }
        fn delete(&self, k: &[u8]) -> std::result::Result<(), KvError> {
            self.mem.delete(k)
        }
        fn scan_all(&self) -> std::result::Result<Rows, KvError> {
            self.scans.fetch_add(1, Ordering::Relaxed);
            self.mem.scan_all()
        }
        fn scan_from(&self, k: &[u8], n: usize) -> std::result::Result<Rows, KvError> {
            self.scans.fetch_add(1, Ordering::Relaxed);
            self.mem.scan_from(k, n)
        }
        fn scan_back_from(&self, k: &[u8], n: usize) -> std::result::Result<Rows, KvError> {
            self.scans.fetch_add(1, Ordering::Relaxed);
            self.mem.scan_back_from(k, n)
        }
        fn scan_prefix(&self, k: &[u8]) -> std::result::Result<Rows, KvError> {
            self.scans.fetch_add(1, Ordering::Relaxed);
            self.mem.scan_prefix(k)
        }
        fn write_batch(&self, ops: &[WriteOp]) -> std::result::Result<(), KvError> {
            if self.fail.load(Ordering::Relaxed) {
                return Err(KvError::Backend("injected failure".into()));
            }
            self.mem.write_batch(ops)
        }
    }
    fn disk() -> Arc<Disk> {
        let d = Arc::new(Disk {
            mem: MemBackend::new(),
            gets: AtomicU64::new(0),
            scans: AtomicU64::new(0),
            fail: AtomicBool::new(false),
        });
        for k in [b"a", b"b", b"c", b"d", b"e"] {
            d.put(k, k).unwrap()
        }
        d
    }
    #[test]
    fn positive_negative_cache_and_writeback_are_coherent() {
        let d = disk();
        let c = BlockCache::new(d.clone());
        for _ in 0..3 {
            assert_eq!(c.get(b"a").unwrap(), Some(b"a".to_vec()));
            assert_eq!(c.get(b"missing").unwrap(), None);
        }
        assert_eq!(d.gets.load(Ordering::Relaxed), 2);
        c.put(b"a", b"new").unwrap();
        c.put(b"missing", b"created").unwrap();
        c.delete(b"b").unwrap();
        assert_eq!(c.get(b"a").unwrap(), Some(b"new".to_vec()));
        assert_eq!(c.get(b"missing").unwrap(), Some(b"created".to_vec()));
        assert_eq!(c.get(b"b").unwrap(), None);
        assert_eq!(d.mem.get(b"a").unwrap(), Some(b"a".to_vec()));
        assert_eq!(d.mem.get(b"missing").unwrap(), None);
        c.flush().unwrap();
        assert_eq!(c.get(b"a").unwrap(), d.mem.get(b"a").unwrap());
        assert_eq!(c.get(b"b").unwrap(), None);
        assert_eq!(c.stats()["flushed_keys"], 3);
    }
    #[test]
    fn cached_windows_merge_deletes_and_new_rows_in_both_directions() {
        let d = disk();
        let c = BlockCache::new(d.clone());
        assert_eq!(
            c.scan_from(b"a", 2).unwrap(),
            d.mem.scan_from(b"a", 2).unwrap()
        );
        assert_eq!(
            c.scan_from(b"a", 2).unwrap(),
            d.mem.scan_from(b"a", 2).unwrap()
        );
        assert_eq!(d.scans.load(Ordering::Relaxed), 1);
        assert_eq!(c.get(b"b").unwrap(), Some(b"b".to_vec()));
        assert_eq!(d.gets.load(Ordering::Relaxed), 0);
        c.delete(b"a").unwrap();
        c.delete(b"c").unwrap();
        c.put(b"bb", b"x").unwrap();
        assert_eq!(
            c.scan_from(b"a", 2)
                .unwrap()
                .iter()
                .map(|(k, _)| k.clone())
                .collect::<Vec<_>>(),
            vec![b"b".to_vec(), b"bb".to_vec()]
        );
        assert_eq!(
            c.scan_back_from(b"f", 3)
                .unwrap()
                .iter()
                .map(|(k, _)| k.clone())
                .collect::<Vec<_>>(),
            vec![b"e".to_vec(), b"d".to_vec(), b"bb".to_vec()]
        );
        assert_eq!(c.scan_prefix(b"b").unwrap().len(), 2);
        c.flush().unwrap();
        assert_eq!(c.scan_all().unwrap(), d.mem.scan_all().unwrap());
    }
    #[test]
    fn failed_flush_preserves_pending_and_disk_prestate() {
        let d = disk();
        let c = BlockCache::new(d.clone());
        c.put(b"a", b"new").unwrap();
        d.fail.store(true, Ordering::Relaxed);
        assert!(c.flush().is_err());
        assert_eq!(c.get(b"a").unwrap(), Some(b"new".to_vec()));
        assert_eq!(d.mem.get(b"a").unwrap(), Some(b"a".to_vec()));
        assert_eq!(c.stats()["pending_keys"], 1);
    }
    #[test]
    fn reverted_transaction_does_not_pollute_block_cache() {
        let d = disk();
        let c = Arc::new(BlockCache::new(d.clone()));
        let tx = SessionBackend::new(c.clone());
        assert_eq!(tx.get(b"a").unwrap(), Some(b"a".to_vec()));
        tx.put(b"a", b"reverted").unwrap();
        tx.delete(b"b").unwrap();
        tx.revert();
        c.flush().unwrap();
        assert_eq!(c.get(b"a").unwrap(), Some(b"a".to_vec()));
        assert_eq!(c.get(b"b").unwrap(), Some(b"b".to_vec()));
        assert_eq!(c.stats()["flushed_keys"], 0);
    }
}
