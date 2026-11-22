//! Benchmark-only complete properties mirror with write-through persistence.
use super::*;
use std::sync::atomic::AtomicU64;
use tron_chainbase::{KvError, WriteOp};
type Rows = Vec<(Vec<u8>, Vec<u8>)>;

pub(super) struct PropertiesMemory {
    disk: Arc<dyn KvBackend>,
    memory: MemBackend,
    reads: AtomicU64,
    scans: AtomicU64,
    writes: AtomicU64,
    preload: Value,
}
impl PropertiesMemory {
    pub(super) fn load(disk: Arc<dyn KvBackend>) -> Result<Self> {
        let started = Instant::now();
        let rows = disk.scan_all()?;
        let bytes = rows.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>();
        let count = rows.len();
        let memory = MemBackend::new();
        memory.write_batch(
            &rows
                .into_iter()
                .map(|(k, v)| WriteOp::Put(k, v))
                .collect::<Vec<_>>(),
        )?;
        let preload = json!({"rows":count,"logical_bytes":bytes,"milliseconds":started.elapsed().as_secs_f64()*1000.,
            "disk_scans":1,"outside_block_timer":true});
        Ok(Self {
            disk,
            memory,
            reads: AtomicU64::new(0),
            scans: AtomicU64::new(0),
            writes: AtomicU64::new(0),
            preload,
        })
    }
    pub(super) fn stats(&self) -> Value {
        json!({"preload":self.preload,"point_gets":self.reads.load(Ordering::Relaxed),
            "scans":self.scans.load(Ordering::Relaxed),"written_operations":self.writes.load(Ordering::Relaxed),
            "read_fallback_to_disk":false,"persistence":"write-through at normal block commit"})
    }
    pub(super) fn verify(&self) -> Result<()> {
        require(
            self.memory.scan_all()? == self.disk.scan_all()?,
            "properties memory/disk poststate mismatch",
        )
    }
}
impl KvBackend for PropertiesMemory {
    fn get(&self, k: &[u8]) -> std::result::Result<Option<Vec<u8>>, KvError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.memory.get(k)
    }
    fn put(&self, k: &[u8], v: &[u8]) -> std::result::Result<(), KvError> {
        self.write_batch(&[WriteOp::Put(k.to_vec(), v.to_vec())])
    }
    fn delete(&self, k: &[u8]) -> std::result::Result<(), KvError> {
        self.write_batch(&[WriteOp::Delete(k.to_vec())])
    }
    fn write_batch(&self, ops: &[WriteOp]) -> std::result::Result<(), KvError> {
        self.disk.write_batch(ops)?;
        self.memory.write_batch(ops)?;
        self.writes.fetch_add(ops.len() as u64, Ordering::Relaxed);
        Ok(())
    }
    fn write_batch_sync(&self, ops: &[WriteOp]) -> std::result::Result<(), KvError> {
        self.disk.write_batch_sync(ops)?;
        self.memory.write_batch(ops)?;
        self.writes.fetch_add(ops.len() as u64, Ordering::Relaxed);
        Ok(())
    }
    fn sync_wal(&self) -> std::result::Result<(), KvError> {
        self.disk.sync_wal()
    }
    fn scan_all(&self) -> std::result::Result<Rows, KvError> {
        self.scans.fetch_add(1, Ordering::Relaxed);
        self.memory.scan_all()
    }
    fn scan_from(&self, k: &[u8], n: usize) -> std::result::Result<Rows, KvError> {
        self.scans.fetch_add(1, Ordering::Relaxed);
        self.memory.scan_from(k, n)
    }
    fn scan_prefix(&self, k: &[u8]) -> std::result::Result<Rows, KvError> {
        self.scans.fetch_add(1, Ordering::Relaxed);
        self.memory.scan_prefix(k)
    }
    fn scan_back_from(&self, k: &[u8], n: usize) -> std::result::Result<Rows, KvError> {
        self.scans.fetch_add(1, Ordering::Relaxed);
        self.memory.scan_back_from(k, n)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    struct Disk {
        memory: MemBackend,
        reads: AtomicU64,
        fail: AtomicBool,
    }
    impl KvBackend for Disk {
        fn get(&self, k: &[u8]) -> std::result::Result<Option<Vec<u8>>, KvError> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            self.memory.get(k)
        }
        fn put(&self, k: &[u8], v: &[u8]) -> std::result::Result<(), KvError> {
            self.write_batch(&[WriteOp::Put(k.to_vec(), v.to_vec())])
        }
        fn delete(&self, k: &[u8]) -> std::result::Result<(), KvError> {
            self.write_batch(&[WriteOp::Delete(k.to_vec())])
        }
        fn scan_all(&self) -> std::result::Result<Rows, KvError> {
            self.memory.scan_all()
        }
        fn write_batch(&self, ops: &[WriteOp]) -> std::result::Result<(), KvError> {
            if self.fail.load(Ordering::Relaxed) {
                return Err(KvError::Backend("injected failure".into()));
            }
            self.memory.write_batch(ops)
        }
    }
    fn disk() -> Arc<Disk> {
        let d = Arc::new(Disk {
            memory: MemBackend::new(),
            reads: AtomicU64::new(0),
            fail: AtomicBool::new(false),
        });
        d.put(b"a", b"1").unwrap();
        d.put(b"b", b"2").unwrap();
        d
    }
    #[test]
    fn reads_and_missing_keys_stay_in_memory_and_writes_persist() {
        let d = disk();
        let c = PropertiesMemory::load(d.clone()).unwrap();
        assert_eq!(c.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(c.get(b"missing").unwrap(), None);
        assert_eq!(d.reads.load(Ordering::Relaxed), 0);
        c.write_batch(&[
            WriteOp::Put(b"a".to_vec(), b"3".to_vec()),
            WriteOp::Delete(b"b".to_vec()),
        ])
        .unwrap();
        assert_eq!(c.get(b"a").unwrap(), Some(b"3".to_vec()));
        assert_eq!(c.get(b"b").unwrap(), None);
        c.verify().unwrap();
        let reopened = PropertiesMemory::load(d).unwrap();
        assert_eq!(reopened.get(b"a").unwrap(), Some(b"3".to_vec()));
        assert_eq!(reopened.get(b"b").unwrap(), None);
    }
    #[test]
    fn failed_disk_batch_does_not_advance_memory() {
        let d = disk();
        let c = PropertiesMemory::load(d.clone()).unwrap();
        d.fail.store(true, Ordering::Relaxed);
        assert!(c.put(b"a", b"changed").is_err());
        assert_eq!(c.get(b"a").unwrap(), Some(b"1".to_vec()));
        c.verify().unwrap();
    }
}
