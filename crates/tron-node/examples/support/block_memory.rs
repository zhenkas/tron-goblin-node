//! Read witnesses: recording over a read-only source, then sealed RAM with no fallback.
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, RwLock,
    },
};
use tron_chainbase::{KvBackend, KvError};
type Rows = Vec<(Vec<u8>, Vec<u8>)>;
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Scan {
    All,
    From(Vec<u8>, usize),
    Prefix(Vec<u8>),
    Back(Vec<u8>, usize),
}
#[derive(Debug, Clone, Default)]
pub struct Reads {
    pub values: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    pub scans: BTreeMap<Scan, Rows>,
}
#[derive(Serialize, Deserialize)]
pub struct Dump {
    values: Vec<(String, Option<String>)>,
    scans: Vec<(Scan, Vec<(String, String)>)>,
}
impl Reads {
    pub fn dump(&self) -> Dump {
        Dump {
            values: self
                .values
                .iter()
                .map(|(k, v)| (hex::encode(k), v.as_ref().map(hex::encode)))
                .collect(),
            scans: self
                .scans
                .iter()
                .map(|(q, rows)| {
                    (
                        q.clone(),
                        rows.iter()
                            .map(|(k, v)| (hex::encode(k), hex::encode(v)))
                            .collect(),
                    )
                })
                .collect(),
        }
    }
    pub fn restore(d: Dump) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            values: d
                .values
                .into_iter()
                .map(|(k, v)| Ok((hex::decode(k)?, v.map(hex::decode).transpose()?)))
                .collect::<Result<_, hex::FromHexError>>()?,
            scans: d
                .scans
                .into_iter()
                .map(|(q, rs)| {
                    Ok((
                        q,
                        rs.into_iter()
                            .map(|(k, v)| Ok((hex::decode(k)?, hex::decode(v)?)))
                            .collect::<Result<_, hex::FromHexError>>()?,
                    ))
                })
                .collect::<Result<_, hex::FromHexError>>()?,
        })
    }
    pub fn bytes(&self) -> usize {
        self.values
            .iter()
            .map(|(k, v)| k.len() + v.as_ref().map_or(0, Vec::len))
            .sum::<usize>()
            + self
                .scans
                .values()
                .flatten()
                .map(|(k, v)| k.len() + v.len())
                .sum::<usize>()
    }
}
pub struct WitnessBackend {
    name: String,
    base: Option<Arc<dyn KvBackend>>,
    reads: Mutex<Reads>,
    pub misses: AtomicU64,
}
impl WitnessBackend {
    pub fn record(name: &str, base: Arc<dyn KvBackend>) -> Self {
        Self {
            name: name.into(),
            base: Some(base),
            reads: Mutex::new(Reads::default()),
            misses: AtomicU64::new(0),
        }
    }
    pub fn sealed(name: &str, reads: Reads) -> Self {
        Self {
            name: name.into(),
            base: None,
            reads: Mutex::new(reads),
            misses: AtomicU64::new(0),
        }
    }
    pub fn snapshot(&self) -> Reads {
        self.reads.lock().unwrap().clone()
    }
    fn missing<T>(&self, what: &str) -> Result<T, KvError> {
        self.misses.fetch_add(1, Ordering::Relaxed);
        Err(KvError::Backend(format!(
            "sealed RAM witness miss in {}: {}",
            self.name, what
        )))
    }
    fn scan(&self, q: Scan) -> Result<Rows, KvError> {
        if let Some(v) = self.reads.lock().unwrap().scans.get(&q) {
            return Ok(v.clone());
        }
        let Some(base) = self.base.as_ref() else {
            return self.missing(&format!("{q:?}"));
        };
        let rows = match &q {
            Scan::All => base.scan_all()?,
            Scan::From(k, n) => base.scan_from(k, *n)?,
            Scan::Prefix(k) => base.scan_prefix(k)?,
            Scan::Back(k, n) => base.scan_back_from(k, *n)?,
        };
        let mut reads = self.reads.lock().unwrap();
        for (k, v) in &rows {
            reads.values.insert(k.clone(), Some(v.clone()));
        }
        reads.scans.insert(q, rows.clone());
        Ok(rows)
    }
}
impl KvBackend for WitnessBackend {
    fn get(&self, k: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        if let Some(v) = self.reads.lock().unwrap().values.get(k) {
            return Ok(v.clone());
        }
        let Some(base) = self.base.as_ref() else {
            return self.missing(&format!("get {}", hex::encode(k)));
        };
        let v = base.get(k)?;
        self.reads
            .lock()
            .unwrap()
            .values
            .insert(k.to_vec(), v.clone());
        Ok(v)
    }
    fn put(&self, _: &[u8], _: &[u8]) -> Result<(), KvError> {
        Err(KvError::Backend(
            "witness is read-only; use a session".into(),
        ))
    }
    fn delete(&self, _: &[u8]) -> Result<(), KvError> {
        Err(KvError::Backend(
            "witness is read-only; use a session".into(),
        ))
    }
    fn scan_all(&self) -> Result<Rows, KvError> {
        self.scan(Scan::All)
    }
    fn scan_from(&self, k: &[u8], n: usize) -> Result<Rows, KvError> {
        self.scan(Scan::From(k.to_vec(), n))
    }
    fn scan_prefix(&self, k: &[u8]) -> Result<Rows, KvError> {
        self.scan(Scan::Prefix(k.to_vec()))
    }
    fn scan_back_from(&self, k: &[u8], n: usize) -> Result<Rows, KvError> {
        self.scan(Scan::Back(k.to_vec(), n))
    }
}
pub struct SwitchBackend(pub RwLock<Arc<dyn KvBackend>>);
impl KvBackend for SwitchBackend {
    fn get(&self, k: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        self.0.read().unwrap().get(k)
    }
    fn put(&self, _: &[u8], _: &[u8]) -> Result<(), KvError> {
        Err(KvError::Backend("read-only account-asset witness".into()))
    }
    fn delete(&self, _: &[u8]) -> Result<(), KvError> {
        Err(KvError::Backend("read-only account-asset witness".into()))
    }
    fn scan_all(&self) -> Result<Rows, KvError> {
        self.0.read().unwrap().scan_all()
    }
    fn scan_from(&self, k: &[u8], n: usize) -> Result<Rows, KvError> {
        self.0.read().unwrap().scan_from(k, n)
    }
    fn scan_prefix(&self, k: &[u8]) -> Result<Rows, KvError> {
        self.0.read().unwrap().scan_prefix(k)
    }
    fn scan_back_from(&self, k: &[u8], n: usize) -> Result<Rows, KvError> {
        self.0.read().unwrap().scan_back_from(k, n)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use tron_chainbase::{MemBackend, SessionBackend};
    #[test]
    fn sealed_witness_distinguishes_absence_from_unobserved_keys() {
        let b = Arc::new(MemBackend::new());
        b.put(b"p1", b"old").unwrap();
        b.put(b"p2", b"two").unwrap();
        let r = WitnessBackend::record("test", b);
        assert_eq!(r.get(b"absent").unwrap(), None);
        let expected = r.scan_prefix(b"p").unwrap();
        let bytes = serde_json::to_vec(&r.snapshot().dump()).unwrap();
        let sealed = Arc::new(WitnessBackend::sealed(
            "test",
            Reads::restore(serde_json::from_slice(&bytes).unwrap()).unwrap(),
        ));
        assert_eq!(sealed.get(b"absent").unwrap(), None);
        assert_eq!(sealed.get(b"p1").unwrap(), Some(b"old".to_vec()));
        assert_eq!(sealed.scan_prefix(b"p").unwrap(), expected);
        assert!(sealed.get(b"never-observed").is_err());
        assert!(sealed.scan_all().is_err());
        assert!(sealed.put(b"p1", b"bad").is_err());
        let session = SessionBackend::new(sealed.clone());
        session.put(b"p1", b"new").unwrap();
        assert_eq!(session.get(b"p1").unwrap(), Some(b"new".to_vec()));
        assert_eq!(
            SessionBackend::new(sealed).get(b"p1").unwrap(),
            Some(b"old".to_vec())
        );
    }
}

/// Add full historical scans by paging the existing historical ordered view.
/// ArchiveAtBackend deliberately rejects scan_all; the block postlude needs it.
pub struct HistoricalBackend(pub Arc<dyn KvBackend>);
impl KvBackend for HistoricalBackend {
    fn get(&self, k: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        self.0.get(k)
    }
    fn put(&self, _: &[u8], _: &[u8]) -> Result<(), KvError> {
        Err(KvError::Backend("historical source is read-only".into()))
    }
    fn delete(&self, _: &[u8]) -> Result<(), KvError> {
        Err(KvError::Backend("historical source is read-only".into()))
    }
    fn scan_all(&self) -> Result<Rows, KvError> {
        let mut rows = vec![];
        let mut cursor = vec![];
        let mut bytes = 0usize;
        loop {
            let page = self.0.scan_from(&cursor, 256)?;
            if page.is_empty() {
                return Ok(rows);
            }
            let next = [page.last().unwrap().0.as_slice(), &[0]].concat();
            for (k, v) in &page {
                bytes += k.len() + v.len();
            }
            rows.extend(page);
            if rows.len() > 250000 || bytes > 512 * 1024 * 1024 {
                return Err(KvError::Backend(
                    "historical full-scan exceeds benchmark bound".into(),
                ));
            }
            cursor = next;
        }
    }
    fn scan_from(&self, k: &[u8], n: usize) -> Result<Rows, KvError> {
        self.0.scan_from(k, n)
    }
    fn scan_prefix(&self, k: &[u8]) -> Result<Rows, KvError> {
        self.0.scan_prefix(k)
    }
    fn scan_back_from(&self, k: &[u8], n: usize) -> Result<Rows, KvError> {
        self.0.scan_back_from(k, n)
    }
}
#[cfg(test)]
mod historical_tests {
    use super::*;
    use tron_chainbase::MemBackend;
    #[test]
    fn paged_full_scan_preserves_prefix_keys_and_page_boundaries() {
        let b = Arc::new(MemBackend::new());
        for i in 0u32..600 {
            b.put(&i.to_be_bytes(), &i.to_le_bytes()).unwrap();
        }
        b.put(&[0, 0, 0, 2, 0], b"extended").unwrap();
        assert_eq!(
            HistoricalBackend(b.clone()).scan_all().unwrap(),
            b.scan_all().unwrap()
        );
    }
}
