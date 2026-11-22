//! Native TidesDB provider for chainbase, with direct nonempty binary keys.
#[cfg(test)]
use crate::MemBackend;
use crate::{KvBackend, KvError, WriteOp};
use std::path::Path;
#[derive(Clone)]
enum Scan {
    All,
    Prefix(Vec<u8>),
    From(Vec<u8>, usize),
    Back(Vec<u8>, usize),
}

use serde::{Deserialize, Serialize};
use tidesdb::{ColumnFamilyConfig, Config, LogLevel, SyncMode, TidesDB};

/// Per-store TidesDB resources. There is one engine per chainbase store.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TidesDbOptions {
    pub write_buffer_size_mb: usize,
    pub block_cache_mb: usize,
    pub max_open_sstables: usize,
    pub flush_threads: i32,
    /// Zero disables background compaction and its VLog GC ticker for short trials.
    pub compaction_threads: i32,
    pub compression: TidesCompression,
    pub bloom_fpr: f64,
    pub wal_sync: TidesWalSync,
    pub wal_sync_interval_ms: u64,
}
impl Default for TidesDbOptions {
    fn default() -> Self {
        Self {
            write_buffer_size_mb: 64,
            block_cache_mb: 8,
            max_open_sstables: 8192,
            flush_threads: 1,
            compaction_threads: 1,
            compression: TidesCompression::None,
            bloom_fpr: 0.01,
            wal_sync: TidesWalSync::Buffered,
            wal_sync_interval_ms: 1000,
        }
    }
}
impl TidesDbOptions {
    pub fn validate(&self) -> KvResult<()> {
        if !(1..=1024).contains(&self.write_buffer_size_mb)
            || self.block_cache_mb > 16384
            || !(32..=65536).contains(&self.max_open_sstables)
            || !(1..=64).contains(&self.flush_threads)
            || !(0..=64).contains(&self.compaction_threads)
            || !self.bloom_fpr.is_finite()
            || !(0.0..1.0).contains(&self.bloom_fpr)
            || self.bloom_fpr == 0.0
            || !(1..=3600000).contains(&self.wal_sync_interval_ms)
        {
            return Err(KvError::Backend(
                "invalid storage.tidesdb resource/bloom/WAL setting".into(),
            ));
        }
        if !self.compression.native().is_available() {
            return Err(KvError::Backend(
                "selected TidesDB compression codec is unavailable in this binary".into(),
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TidesCompression {
    #[default]
    None,
    Snappy,
    Lz4,
    Zstd,
}
impl TidesCompression {
    fn native(self) -> tidesdb::CompressionAlgorithm {
        match self {
            Self::None => tidesdb::CompressionAlgorithm::None,
            Self::Snappy => tidesdb::CompressionAlgorithm::Snappy,
            Self::Lz4 => tidesdb::CompressionAlgorithm::Lz4,
            Self::Zstd => tidesdb::CompressionAlgorithm::Zstd,
        }
    }
}
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TidesWalSync {
    #[default]
    Buffered,
    Full,
    Interval,
}
impl TidesWalSync {
    fn native(self) -> SyncMode {
        match self {
            Self::Buffered => SyncMode::None,
            Self::Full => SyncMode::Full,
            Self::Interval => SyncMode::Interval,
        }
    }
}
// TidesDB rejects zero-length native keys. Keep that one logical key in an
// isolated CF; all nonempty state keys and values remain byte-for-byte direct.
pub(crate) const EMPTY_KEY_CF: &str = "_empty_key";
pub(crate) const EMPTY_KEY_SLOT: &[u8] = b"value";
type KvResult<T> = std::result::Result<T, KvError>;
type Rows = Vec<(Vec<u8>, Vec<u8>)>;
fn error(e: tidesdb::Error) -> KvError {
    KvError::Backend(format!("TidesDB: {e}"))
}

pub struct TidesDbBackend {
    db: TidesDB,
}
impl TidesDbBackend {
    pub fn open(path: &Path) -> KvResult<Self> {
        Self::open_with_options(path, &TidesDbOptions::default())
    }
    pub fn open_with_options(path: &Path, options: &TidesDbOptions) -> KvResult<Self> {
        options.validate()?;
        if path.join("CURRENT").exists() {
            return Err(KvError::Backend(format!(
                "{} is RocksDB; use a separate converted TidesDB data directory",
                path.display()
            )));
        }
        if tidesdb::raise_open_file_limit(65536) < 65536 {
            return Err(KvError::Backend(
                "TidesDB needs RLIMIT_NOFILE >= 65536".into(),
            ));
        }
        std::fs::create_dir_all(path).map_err(|e| KvError::Backend(e.to_string()))?;
        let config = Config::new(path)
            .num_flush_threads(options.flush_threads)
            .num_compaction_threads(options.compaction_threads)
            .memtable_write_buffer_size(options.write_buffer_size_mb * 1024 * 1024)
            .block_cache_size(options.block_cache_mb * 1024 * 1024)
            .max_open_sstables(options.max_open_sstables)
            .memtable_sync_mode(options.wal_sync.native())
            .memtable_sync_interval_us(options.wal_sync_interval_ms * 1000)
            .memtable_idle_flush_seconds(0)
            .log_level(LogLevel::Error)
            .log_to_file(true);
        let db = TidesDB::open(config).map_err(error)?;
        let existing = db.list_column_families().map_err(error)?;
        for name in ["state", EMPTY_KEY_CF] {
            let cf_config = ColumnFamilyConfig::default()
                .compression_algorithm(options.compression.native())
                .bloom_fpr(options.bloom_fpr);
            if !existing.iter().any(|n| n == name) {
                db.create_column_family(name, cf_config).map_err(error)?;
            } else {
                // Apply the production CF profile on reopen too. In particular,
                // bulk-import databases must not retain disabled compaction.
                // Existing SSTs retain their own compression encoding.
                db.get_column_family(name)
                    .map_err(error)?
                    .update_runtime_config(&cf_config, true)
                    .map_err(error)?;
            }
        }
        Ok(Self { db })
    }
    fn scan(&self, query: Scan) -> KvResult<Rows> {
        if matches!(query, Scan::From(_, 0) | Scan::Back(_, 0))
            || matches!(&query, Scan::Back(key, _) if key.is_empty())
        {
            return Ok(vec![]);
        }
        let cf = self.db.get_column_family("state").map_err(error)?;
        let txn = self
            .db
            .begin_transaction_with_isolation(tidesdb::IsolationLevel::Snapshot)
            .map_err(error)?;
        let include_empty = match &query {
            Scan::All | Scan::Back(..) => true,
            Scan::Prefix(k) | Scan::From(k, _) => k.is_empty(),
        };
        let empty = if include_empty {
            let cf = self.db.get_column_family(EMPTY_KEY_CF).map_err(error)?;
            match txn.get(&cf, EMPTY_KEY_SLOT) {
                Ok(v) => Some(v),
                Err(e) if e.is_not_found() => None,
                Err(e) => return Err(error(e)),
            }
        } else {
            None
        };
        let mut rows = Vec::new();
        if !matches!(query, Scan::Back(..)) {
            if let Some(v) = empty.as_ref() {
                rows.push((vec![], v.clone()));
                if matches!(&query, Scan::From(_, n) if rows.len() >= *n) {
                    return Ok(rows);
                }
            }
        }
        // Native bounds prune SST selection; prefix checks below are still
        // required because these bounds are not an iterator output fence.
        let upper = match &query {
            Scan::Prefix(prefix) if !prefix.is_empty() => {
                let mut end = prefix.clone();
                let cut = end.iter().rposition(|b| *b != 255);
                cut.map(|i| {
                    end[i] += 1;
                    end.truncate(i + 1);
                    end
                })
            }
            _ => None,
        };
        let mut iter = match (&query, &upper) {
            (Scan::Prefix(prefix), Some(end)) => txn.new_range_iterator(&cf, prefix, end),
            _ => txn.new_iterator(&cf),
        }
        .map_err(error)?;
        match &query {
            Scan::All => iter.seek_to_first(),
            Scan::Prefix(key) | Scan::From(key, _) if key.is_empty() => iter.seek_to_first(),
            Scan::Prefix(key) | Scan::From(key, _) => iter.seek(key),
            Scan::Back(key, _) => iter.seek_for_prev(key),
        }
        .map_err(error)?;
        if let Scan::Back(before, _) = &query {
            if iter.is_valid() && iter.key().map_err(error)?.as_slice() == before.as_slice() {
                iter.prev().map_err(error)?;
            }
        }
        while iter.is_valid() {
            let (key, value) = iter.key_value().map_err(error)?;
            if let Scan::Prefix(prefix) = &query {
                if !key.starts_with(prefix) {
                    break;
                }
            }
            rows.push((key, value));
            if match &query {
                Scan::From(_, n) | Scan::Back(_, n) => rows.len() >= *n,
                _ => false,
            } {
                break;
            }
            if matches!(query, Scan::Back(..)) {
                iter.prev()
            } else {
                iter.next()
            }
            .map_err(error)?;
        }
        if let Scan::Back(_, n) = &query {
            if rows.len() < *n {
                if let Some(v) = empty {
                    rows.push((vec![], v));
                }
            }
        }
        Ok(rows)
    }
}
impl KvBackend for TidesDbBackend {
    fn get(&self, key: &[u8]) -> KvResult<Option<Vec<u8>>> {
        let cf = self
            .db
            .get_column_family(if key.is_empty() {
                EMPTY_KEY_CF
            } else {
                "state"
            })
            .map_err(error)?;
        let txn = self.db.begin_transaction().map_err(error)?;
        match txn.get_notrack(&cf, if key.is_empty() { EMPTY_KEY_SLOT } else { key }) {
            Ok(value) => Ok(Some(value)),
            Err(e) if e.is_not_found() => Ok(None),
            Err(e) => Err(error(e)),
        }
    }
    fn multi_get(&self, keys: &[&[u8]]) -> KvResult<Vec<Option<Vec<u8>>>> {
        if keys.is_empty() {
            return Ok(vec![]);
        }
        let cf = self.db.get_column_family("state").map_err(error)?;
        let empty = if keys.iter().any(|k| k.is_empty()) {
            Some(self.db.get_column_family(EMPTY_KEY_CF).map_err(error)?)
        } else {
            None
        };
        let txn = self
            .db
            .begin_transaction_with_isolation(tidesdb::IsolationLevel::Snapshot)
            .map_err(error)?;
        keys.iter()
            .map(|key| {
                let (cf, key) = if key.is_empty() {
                    (empty.as_ref().unwrap(), EMPTY_KEY_SLOT)
                } else {
                    (&cf, *key)
                };
                match txn.get_notrack(cf, key) {
                    Ok(v) => Ok(Some(v)),
                    Err(e) if e.is_not_found() => Ok(None),
                    Err(e) => Err(error(e)),
                }
            })
            .collect()
    }

    fn put(&self, key: &[u8], value: &[u8]) -> KvResult<()> {
        self.write_batch(&[WriteOp::Put(key.to_vec(), value.to_vec())])
    }
    fn delete(&self, key: &[u8]) -> KvResult<()> {
        self.write_batch(&[WriteOp::Delete(key.to_vec())])
    }
    fn write_batch(&self, ops: &[WriteOp]) -> KvResult<()> {
        if ops.is_empty() {
            return Ok(());
        }
        let cf = self.db.get_column_family("state").map_err(error)?;
        let empty_cf = if ops.iter().any(|op| match op {
            WriteOp::Put(k, _) | WriteOp::Delete(k) => k.is_empty(),
        }) {
            Some(self.db.get_column_family(EMPTY_KEY_CF).map_err(error)?)
        } else {
            None
        };
        let mut txn = self.db.begin_transaction().map_err(error)?;
        for op in ops {
            let k = match op {
                WriteOp::Put(k, _) | WriteOp::Delete(k) => k,
            };
            let (target, key) = if k.is_empty() {
                (empty_cf.as_ref().unwrap(), EMPTY_KEY_SLOT)
            } else {
                (&cf, k.as_slice())
            };
            match op {
                WriteOp::Put(_, v) => txn.put(target, key, v),
                WriteOp::Delete(_) => txn.delete(target, key),
            }
            .map_err(error)?;
        }
        txn.commit().map_err(error)
    }
    fn write_batch_sync(&self, ops: &[WriteOp]) -> KvResult<()> {
        self.write_batch(ops)?;
        self.sync_wal()
    }
    fn checkpoint(&self) -> KvResult<()> {
        self.db.checkpoint().map_err(error)
    }

    fn sync_wal(&self) -> KvResult<()> {
        self.db.sync_wal().map_err(error)
    }
    fn scan_all(&self) -> KvResult<Rows> {
        self.scan(Scan::All)
    }
    fn scan_prefix(&self, key: &[u8]) -> KvResult<Rows> {
        self.scan(Scan::Prefix(key.to_vec()))
    }
    fn scan_from(&self, key: &[u8], n: usize) -> KvResult<Rows> {
        self.scan(Scan::From(key.to_vec(), n))
    }
    fn scan_back_from(&self, key: &[u8], n: usize) -> KvResult<Rows> {
        self.scan(Scan::Back(key.to_vec(), n))
    }
}
impl TidesDbBackend {
    pub fn checkpoint(&self) -> KvResult<()> {
        self.db.checkpoint().map_err(error)
    }
    pub fn compact(&self) -> KvResult<()> {
        for name in ["state", EMPTY_KEY_CF] {
            self.db
                .get_column_family(name)
                .map_err(error)?
                .compact()
                .map_err(error)?;
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn binary_ranges_batches_and_reopen_match_memory() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested/database/tides");
        let expected = MemBackend::new();
        let db = TidesDbBackend::open(&path).unwrap();
        let ops = vec![
            WriteOp::Put(vec![], vec![]),
            WriteOp::Put(vec![0], vec![]),
            WriteOp::Put(vec![0, 0], vec![1]),
            WriteOp::Put(vec![0, 255], vec![2]),
            WriteOp::Put(vec![1], vec![3]),
            WriteOp::Put(vec![255], vec![4]),
            WriteOp::Put(vec![255, 255], vec![5]),
        ];
        expected.write_batch(&ops).unwrap();
        db.write_batch(&ops).unwrap();
        assert_eq!(db.get(&[]).unwrap(), Some(vec![]));
        assert_eq!(db.get(&[0]).unwrap(), Some(vec![]));
        assert_eq!(db.get(&[9]).unwrap(), None);
        assert_eq!(
            db.multi_get(&[&[1], &[], &[9], &[0], &[1]]).unwrap(),
            expected.multi_get(&[&[1], &[], &[9], &[0], &[1]]).unwrap()
        );
        assert!(db.multi_get(&[]).unwrap().is_empty());
        for key in [
            vec![],
            vec![0],
            vec![0, 1],
            vec![1],
            vec![255],
            vec![255, 255],
            vec![255, 255, 255],
        ] {
            assert_eq!(
                db.scan_prefix(&key).unwrap(),
                expected.scan_prefix(&key).unwrap()
            );
            for n in [0, 1, 2, 10] {
                assert_eq!(
                    db.scan_from(&key, n).unwrap(),
                    expected.scan_from(&key, n).unwrap()
                );
                assert_eq!(
                    db.scan_back_from(&key, n).unwrap(),
                    expected.scan_back_from(&key, n).unwrap()
                );
            }
        }
        let changes = vec![
            WriteOp::Delete(vec![]),
            WriteOp::Put(vec![], vec![99]),
            WriteOp::Delete(vec![0, 0]),
            WriteOp::Put(vec![1], vec![42]),
            WriteOp::Delete(vec![123]),
        ];
        expected.write_batch(&changes).unwrap();
        db.write_batch(&changes).unwrap();
        assert_eq!(db.scan_all().unwrap(), expected.scan_all().unwrap());
        db.checkpoint().unwrap();
        drop(db);
        let reopened = TidesDbBackend::open(&path).unwrap();
        assert_eq!(reopened.get(&[]).unwrap(), Some(vec![99]));
        assert_eq!(reopened.scan_all().unwrap(), expected.scan_all().unwrap());
        reopened.delete(&[]).unwrap();
        assert_eq!(reopened.get(&[]).unwrap(), None);
        expected.delete(&[]).unwrap();
        assert_eq!(reopened.scan_all().unwrap(), expected.scan_all().unwrap());
    }
}
