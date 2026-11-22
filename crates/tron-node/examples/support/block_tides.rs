//! Experimental adapter for the isolated block benchmark, never a node runtime backend.
use super::*;
use memory::Scan;
use tidesdb::{ColumnFamilyConfig, Config, LogLevel, SyncMode, TidesDB};
use tron_chainbase::{KvError, WriteOp};

// TidesDB rejects zero-length native keys. Keep that one logical key in an
// isolated CF; all nonempty state keys and values remain byte-for-byte direct.
pub(super) const EMPTY_KEY_CF: &str = "_empty_key";
pub(super) const EMPTY_KEY_SLOT: &[u8] = b"value";
type KvResult<T> = std::result::Result<T, KvError>;
type Rows = Vec<(Vec<u8>, Vec<u8>)>;
fn error(e: tidesdb::Error) -> KvError {
    KvError::Backend(format!("TidesDB: {e}"))
}

pub(super) struct TidesBackend {
    db: TidesDB,
}
impl TidesBackend {
    pub(super) fn open(path: &Path, create: bool) -> KvResult<Self> {
        if create {
            std::fs::create_dir_all(path).map_err(|e| {
                KvError::Backend(format!("create TidesDB directory {}: {e}", path.display()))
            })?;
        }
        let config = Config::new(path)
            .num_flush_threads(1)
            .num_compaction_threads(1)
            .memtable_write_buffer_size(4 * 1024 * 1024)
            .block_cache_size(8 * 1024 * 1024)
            .max_open_sstables(64)
            .memtable_sync_mode(SyncMode::None)
            .memtable_idle_flush_seconds(0)
            .log_level(LogLevel::Error)
            .log_to_file(true);
        let db = TidesDB::open(config).map_err(error)?;
        if create {
            db.create_column_family("state", ColumnFamilyConfig::default())
                .map_err(error)?;
        }
        db.get_column_family("state").map_err(error)?;
        if !db
            .list_column_families()
            .map_err(error)?
            .iter()
            .any(|n| n == EMPTY_KEY_CF)
        {
            db.create_column_family(EMPTY_KEY_CF, ColumnFamilyConfig::default())
                .map_err(error)?;
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
        let txn = self.db.begin_transaction().map_err(error)?;
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
        let mut iter = txn.new_iterator(&cf).map_err(error)?;
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
impl KvBackend for TidesBackend {
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
        match txn.get(&cf, if key.is_empty() { EMPTY_KEY_SLOT } else { key }) {
            Ok(value) => Ok(Some(value)),
            Err(e) if e.is_not_found() => Ok(None),
            Err(e) => Err(error(e)),
        }
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
impl rocks::BenchBackend for TidesBackend {
    fn flush_and_sync(&self) -> KvResult<()> {
        self.db.checkpoint().map_err(error)
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
        let db = TidesBackend::open(&path, true).unwrap();
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
        rocks::BenchBackend::flush_and_sync(&db).unwrap();
        drop(db);
        let reopened = TidesBackend::open(&path, false).unwrap();
        assert_eq!(reopened.get(&[]).unwrap(), Some(vec![99]));
        assert_eq!(reopened.scan_all().unwrap(), expected.scan_all().unwrap());
        reopened.delete(&[]).unwrap();
        assert_eq!(reopened.get(&[]).unwrap(), None);
        expected.delete(&[]).unwrap();
        assert_eq!(reopened.scan_all().unwrap(), expected.scan_all().unwrap());
    }
}
