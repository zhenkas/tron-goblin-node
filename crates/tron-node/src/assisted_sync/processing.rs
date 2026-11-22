//! Node-local lifetime accounting. Never replicated or included in a donor snapshot.
use serde::{Deserialize, Serialize};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use tron_chainbase::KvBackend;
type Result<T> = std::result::Result<T, String>;
const KEY: &[u8] = b"assisted-sync/processing-statistics/v1";
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub schema: u32,
    pub since_height: i64,
    pub since_ms: u64,
    pub local_blocks: u64,
    pub imported_blocks: u64,
    pub mode_switches: u64,
    pub assisted: bool,
    #[serde(default)]
    pub source_peer: Option<String>,
    #[serde(default)]
    pub source_switches: u64,
    pub last_height: i64,
    pub updated_ms: u64,
    import_base: u64,
    last_local_height: i64,
    last_local_id: Vec<u8>,
}
pub struct ProcessingStats {
    backend: Arc<dyn KvBackend>,
    state: Mutex<Snapshot>,
    errors: AtomicU64,
}
fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}
impl ProcessingStats {
    pub fn open(
        backend: Arc<dyn KvBackend>,
        height: i64,
        imported: u64,
        assisted: bool,
    ) -> Result<Arc<Self>> {
        let bytes = backend.get(KEY).map_err(|e| e.to_string())?;
        let state = match bytes {
            Some(bytes) => serde_json::from_slice::<Snapshot>(&bytes).map_err(|e| e.to_string())?,
            None => Snapshot {
                schema: 1,
                since_height: height,
                since_ms: now(),
                local_blocks: 0,
                imported_blocks: 0,
                mode_switches: 0,
                assisted,
                source_peer: None,
                source_switches: 0,
                last_height: height,
                updated_ms: now(),
                import_base: imported,
                last_local_height: height,
                last_local_id: Vec::new(),
            },
        };
        if state.schema != 1 || state.import_base > imported {
            return Err("invalid persisted processing statistics".into());
        }
        let stats = Arc::new(Self {
            backend,
            state: Mutex::new(state),
            errors: AtomicU64::new(0),
        });
        stats.update(|s| {
            s.imported_blocks = imported - s.import_base;
            if !assisted {
                s.source_peer = None;
            }
            if s.assisted != assisted {
                s.mode_switches = s.mode_switches.saturating_add(1);
                s.assisted = assisted;
            }
        })?;
        Ok(stats)
    }
    fn update(&self, change: impl FnOnce(&mut Snapshot)) -> Result<()> {
        let result = (|| {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| "processing statistics poisoned")?;
            let mut next = guard.clone();
            change(&mut next);
            next.updated_ms = now();
            let bytes = serde_json::to_vec(&next).map_err(|e| e.to_string())?;
            self.backend.put(KEY, &bytes).map_err(|e| e.to_string())?;
            self.backend.sync_wal().map_err(|e| e.to_string())?;
            *guard = next;
            Ok(())
        })();
        if result.is_err() {
            self.errors.fetch_add(1, Ordering::Relaxed);
        }
        result
    }
    pub fn local(&self, height: i64, id: &[u8]) -> Result<()> {
        self.update(|s| {
            // Recovery of the last committed block and duplicate callbacks are idempotent.
            // Reversible heights below the local watermark do not inflate lifetime totals.
            if height > s.last_local_height
                || (height == s.last_local_height
                    && !s.last_local_id.is_empty()
                    && s.last_local_id != id)
            {
                s.local_blocks = s.local_blocks.saturating_add(1);
                s.last_local_height = height;
                s.last_local_id = id.to_vec();
            }
            s.last_height = s.last_height.max(height);
        })
    }
    pub fn imported(&self, total: u64, height: i64) -> Result<()> {
        self.update(|s| {
            s.imported_blocks = s.imported_blocks.max(total.saturating_sub(s.import_base));
            s.last_height = s.last_height.max(height);
        })
    }
    pub fn mode(&self, assisted: bool) -> Result<()> {
        self.update(|s| {
            if !assisted {
                s.source_peer = None;
            }
            if s.assisted != assisted {
                s.mode_switches = s.mode_switches.saturating_add(1);
                s.assisted = assisted;
            }
        })
    }
    pub fn source(&self, peer: Option<&str>) -> Result<()> {
        let target = peer.map(str::to_string);
        if self.snapshot().source_peer == target {
            return Ok(());
        }
        self.update(|s| {
            if target.is_some() {
                s.source_switches = s.source_switches.saturating_add(1);
            }
            s.source_peer = target;
        })
    }
    pub fn snapshot(&self) -> Snapshot {
        self.state.lock().expect("statistics mutex").clone()
    }
    pub fn prometheus(&self, assisted: bool) -> String {
        use std::fmt::Write;
        let s = self.snapshot();
        let mut out = String::new();
        for (name, value, kind) in [
            ("local_blocks_total", s.local_blocks, "counter"),
            ("imported_blocks_total", s.imported_blocks, "counter"),
            ("mode_switches_total", s.mode_switches, "counter"),
            ("source_switches_total", s.source_switches, "counter"),
            ("assisted", u64::from(assisted), "gauge"),
            ("since_height", s.since_height.max(0) as u64, "gauge"),
            ("since_timestamp_ms", s.since_ms, "gauge"),
            ("updated_timestamp_ms", s.updated_ms, "gauge"),
            (
                "persistence_errors_total",
                self.errors.load(Ordering::Relaxed),
                "counter",
            ),
        ] {
            let _ = writeln!(
                out,
                "# TYPE tron_node_processing_{name} {kind}\ntron_node_processing_{name} {value}"
            );
        }
        if let Some(peer) = s.source_peer {
            let _ = writeln!(out, "tron_node_processing_source_info{{peer=\"{peer}\"}} 1");
        }
        out
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use tron_chainbase::MemBackend;
    #[test]
    fn local_and_import_counts_survive_restart_without_redelivery_inflation() {
        let db: Arc<dyn KvBackend> = Arc::new(MemBackend::new());
        let a = ProcessingStats::open(db.clone(), 100, 70, true).unwrap();
        a.imported(73, 103).unwrap();
        a.imported(73, 103).unwrap();
        a.mode(false).unwrap();
        a.local(104, &[4; 32]).unwrap();
        a.local(104, &[4; 32]).unwrap();
        a.local(105, &[5; 32]).unwrap();
        a.local(103, &[3; 32]).unwrap();
        assert_eq!(a.snapshot().local_blocks, 2);
        assert_eq!(a.snapshot().imported_blocks, 3);
        drop(a);
        let b = ProcessingStats::open(db, 105, 74, false).unwrap();
        b.local(105, &[5; 32]).unwrap();
        assert_eq!(b.snapshot().local_blocks, 2);
        assert_eq!(b.snapshot().imported_blocks, 4); // cursor committed before a crash in stats persistence
        assert_eq!(b.snapshot().mode_switches, 1);
        b.mode(true).unwrap();
        assert_eq!(b.snapshot().mode_switches, 2);
        assert!(b
            .prometheus(true)
            .contains("tron_node_processing_assisted 1"));
    }
    #[test]
    fn statistics_are_node_local_and_corruption_is_explicit() {
        let a: Arc<dyn KvBackend> = Arc::new(MemBackend::new());
        a.put(KEY, b"corrupt").unwrap();
        assert!(ProcessingStats::open(a, 0, 0, false).is_err());
        let b: Arc<dyn KvBackend> = Arc::new(MemBackend::new());
        let s = ProcessingStats::open(b, 200, 999, false).unwrap();
        assert_eq!(s.snapshot().imported_blocks, 0);
        assert_eq!(s.snapshot().since_height, 200);
    }
}
