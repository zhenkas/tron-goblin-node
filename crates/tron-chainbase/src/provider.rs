//! Open-time provider selection. Consumers retain Arc<dyn KvBackend>, so the
//! RocksDB hot path gets no additional engine-dispatch branch.
use crate::{KvBackend, KvError, RocksDbBackend, TidesDbBackend, TidesDbOptions};
use serde::{Deserialize, Serialize};
use std::{path::Path, sync::Arc};
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum KvEngine {
    #[default]
    Rocksdb,
    Tidesdb,
}
#[derive(Debug, Clone)]
pub struct KvProvider {
    pub engine: KvEngine,
    pub rocks_tuning: Option<(usize, i32)>,
    pub tidesdb: TidesDbOptions,
}
impl Default for KvProvider {
    fn default() -> Self {
        Self {
            engine: KvEngine::Rocksdb,
            rocks_tuning: None,
            tidesdb: TidesDbOptions::default(),
        }
    }
}
impl KvProvider {
    pub fn validate(&self) -> Result<(), KvError> {
        if self.engine == KvEngine::Tidesdb {
            self.tidesdb.validate()?;
        }
        Ok(())
    }
    /// Reject an engine mismatch before either engine creates metadata.
    pub fn check_path(&self, path: &Path) -> Result<(), KvError> {
        let mismatch = match self.engine {
            KvEngine::Rocksdb => path.join("MANIFEST").exists(),
            KvEngine::Tidesdb => path.join("CURRENT").exists(),
        };
        if mismatch {
            return Err(KvError::Backend(format!(
            "storage backend {:?} does not match existing database {}; use a matching/converted data directory",
            self.engine,path.display())));
        }
        Ok(())
    }
    pub fn open(&self, path: &Path, name: &str) -> Result<Arc<dyn KvBackend>, KvError> {
        self.validate()?;
        self.check_path(path)?;
        match self.engine {
            KvEngine::Rocksdb => {
                let backend = match crate::comparator_for_store(name) {
                    Some((cmp, f)) => {
                        RocksDbBackend::open_with_comparator(path, self.rocks_tuning, cmp, f)
                    }
                    None => match self.rocks_tuning {
                        Some((buffer, files)) => RocksDbBackend::open_tuned(path, buffer, files),
                        None => RocksDbBackend::open(path),
                    },
                }
                .map_err(|e| KvError::Backend(format!("open RocksDB {}: {e}", path.display())))?;
                Ok(Arc::new(backend))
            }
            KvEngine::Tidesdb => {
                let backend = Arc::new(TidesDbBackend::open_with_options(path, &self.tidesdb)?)
                    as Arc<dyn KvBackend>;
                if name == crate::MARKET_PAIR_PRICE_TO_ORDER_DB_NAME {
                    let marker = path.join(".kv-key-order");
                    match std::fs::read_to_string(&marker) {
                        Ok(v) if v == crate::market_price_backend::KEY_ORDER_FORMAT => {}
                        Ok(_) => {
                            return Err(KvError::Backend(
                                "incompatible market-price key order".into(),
                            ))
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                            if !backend.scan_from(&[], 1)?.is_empty() {
                                return Err(KvError::Backend(
                                    "nonempty TidesDB market-price store has no order-codec marker"
                                        .into(),
                                ));
                            }
                            std::fs::write(&marker, crate::market_price_backend::KEY_ORDER_FORMAT)
                                .map_err(|e| KvError::Backend(e.to_string()))?;
                        }
                        Err(e) => return Err(KvError::Backend(e.to_string())),
                    }
                    Ok(Arc::new(
                        crate::market_price_backend::MarketPriceBackend::new(backend),
                    ))
                } else {
                    Ok(backend)
                }
            }
        }
    }
}
