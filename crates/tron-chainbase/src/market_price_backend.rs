//! Byte-order codec for the one chainbase store with a price comparator.
//! Positive i64 ratios fit an exact order embedding in unsigned Q64.128:
//! distinct fractions differ by > 2^-126, so truncation to 128 fraction bits
//! never collapses prices. Equal ratios produce the SAME physical key.
use crate::{KvBackend, KvError, WriteOp};
use std::sync::Arc;
type Result<T> = std::result::Result<T, KvError>;
type Rows = Vec<(Vec<u8>, Vec<u8>)>;
const PAIR: usize = 38;
pub(crate) const KEY_ORDER_FORMAT: &str = "market-price-q64.128-v1
";

pub(crate) fn price_key(key: &[u8]) -> Result<Vec<u8>> {
    if key.len() < PAIR {
        return Ok(key.to_vec());
    }
    fn qty(key: &[u8], offset: usize) -> i64 {
        let mut b = [0u8; 8];
        if key.len() > offset {
            let n = (key.len() - offset).min(8);
            b[..n].copy_from_slice(&key[offset..offset + n]);
        }
        i64::from_be_bytes(b)
    }
    let sell = qty(key, 38);
    let buy = qty(key, 46);
    let mut encoded = key[..PAIR].to_vec();
    if sell == 0 || buy == 0 {
        encoded.push(0);
        return Ok(encoded);
    }
    if sell < 0 || buy < 0 {
        return Err(KvError::Backend(
            "market price quantities must be nonnegative i64".into(),
        ));
    }
    encoded.push(1);
    let (sell, buy) = (sell as u128, buy as u128);
    let integer = (buy / sell) as u64;
    let dividend = (buy % sell) << 64;
    let high = (dividend / sell) as u64;
    let dividend = (dividend % sell) << 64;
    let low = (dividend / sell) as u64;
    encoded.extend_from_slice(&integer.to_be_bytes());
    encoded.extend_from_slice(&high.to_be_bytes());
    encoded.extend_from_slice(&low.to_be_bytes());
    Ok(encoded)
}
fn pack(key: &[u8], value: &[u8]) -> Result<Vec<u8>> {
    let n = u32::try_from(key.len()).map_err(|_| KvError::Backend("market key too long".into()))?;
    let mut out = Vec::with_capacity(4 + key.len() + value.len());
    out.extend_from_slice(&n.to_be_bytes());
    out.extend_from_slice(key);
    out.extend_from_slice(value);
    Ok(out)
}
fn unpack(value: Vec<u8>) -> Result<(Vec<u8>, Vec<u8>)> {
    let invalid = || KvError::Backend("invalid market-price key envelope".into());
    if value.len() < 4 {
        return Err(invalid());
    }
    let n = u32::from_be_bytes(value[..4].try_into().unwrap()) as usize;
    if n > value.len() - 4 {
        return Err(invalid());
    }
    Ok((value[4..4 + n].to_vec(), value[4 + n..].to_vec()))
}
pub(crate) struct MarketPriceBackend {
    inner: Arc<dyn KvBackend>,
}
impl MarketPriceBackend {
    pub(crate) fn new(inner: Arc<dyn KvBackend>) -> Self {
        Self { inner }
    }
    fn decode(rows: Rows) -> Result<Rows> {
        rows.into_iter()
            .map(|(physical, value)| {
                let row = unpack(value)?;
                if price_key(&row.0)? != physical {
                    return Err(KvError::Backend(
                        "market price key envelope disagrees with index".into(),
                    ));
                }
                Ok(row)
            })
            .collect()
    }
}
impl KvBackend for MarketPriceBackend {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner
            .get(&price_key(key)?)?
            .map(|v| unpack(v).map(|r| r.1))
            .transpose()
    }
    fn multi_get(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>> {
        let encoded: Vec<Vec<u8>> = keys.iter().map(|k| price_key(k)).collect::<Result<_>>()?;
        let borrowed: Vec<&[u8]> = encoded.iter().map(|k| k.as_slice()).collect();
        self.inner
            .multi_get(&borrowed)?
            .into_iter()
            .map(|v| v.map(|v| unpack(v).map(|r| r.1)).transpose())
            .collect()
    }
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write_batch(&[WriteOp::Put(key.to_vec(), value.to_vec())])
    }
    fn delete(&self, key: &[u8]) -> Result<()> {
        self.write_batch(&[WriteOp::Delete(key.to_vec())])
    }
    fn write_batch(&self, ops: &[WriteOp]) -> Result<()> {
        let encoded = ops
            .iter()
            .map(|op| match op {
                WriteOp::Put(k, v) => Ok(WriteOp::Put(price_key(k)?, pack(k, v)?)),
                WriteOp::Delete(k) => Ok(WriteOp::Delete(price_key(k)?)),
            })
            .collect::<Result<Vec<_>>>()?;
        self.inner.write_batch(&encoded)
    }
    fn write_batch_sync(&self, ops: &[WriteOp]) -> Result<()> {
        let encoded = ops
            .iter()
            .map(|op| match op {
                WriteOp::Put(k, v) => Ok(WriteOp::Put(price_key(k)?, pack(k, v)?)),
                WriteOp::Delete(k) => Ok(WriteOp::Delete(price_key(k)?)),
            })
            .collect::<Result<Vec<_>>>()?;
        self.inner.write_batch_sync(&encoded)
    }
    fn sync_wal(&self) -> Result<()> {
        self.inner.sync_wal()
    }
    fn checkpoint(&self) -> Result<()> {
        self.inner.checkpoint()
    }
    fn scan_all(&self) -> Result<Rows> {
        Self::decode(self.inner.scan_all()?)
    }
    fn scan_from(&self, start: &[u8], limit: usize) -> Result<Rows> {
        Self::decode(self.inner.scan_from(&price_key(start)?, limit)?)
    }
    fn scan_back_from(&self, before: &[u8], limit: usize) -> Result<Rows> {
        Self::decode(self.inner.scan_back_from(&price_key(before)?, limit)?)
    }
    fn scan_prefix(&self, prefix: &[u8]) -> Result<Rows> {
        if prefix.len() <= PAIR {
            return Self::decode(self.inner.scan_prefix(prefix)?);
        }
        // Match the custom RocksDB comparator's seek-then-prefix-fence behavior.
        let mut start = price_key(prefix)?;
        let mut out = vec![];
        loop {
            let rows = self.inner.scan_from(&start, 128)?;
            if rows.is_empty() {
                break;
            }
            let last = rows.last().unwrap().0.clone();
            let count = rows.len();
            for row in Self::decode(rows)? {
                if !row.0.starts_with(prefix) {
                    return Ok(out);
                }
                out.push(row);
            }
            if count < 128 {
                break;
            }
            start = last;
            start.push(0);
        }
        Ok(out)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn key(pair: u8, sell: i64, buy: i64) -> Vec<u8> {
        let mut k = vec![pair; 38];
        k.extend_from_slice(&sell.to_be_bytes());
        k.extend_from_slice(&buy.to_be_bytes());
        k
    }
    #[test]
    fn exact_price_embedding_matches_comparator_and_equivalence() {
        let mut keys = vec![
            key(1, 0, 0),
            key(1, 2, 0),
            key(1, 1, 2),
            key(1, 2, 4),
            key(1, i64::MAX, i64::MAX - 1),
            key(1, i64::MAX - 1, i64::MAX - 2),
            key(1, 1, i64::MAX),
            key(1, i64::MAX, 1),
            key(2, 1, 1),
            vec![1; 38],
            vec![1; 7],
        ];
        let mut seed = 938012837u64;
        for _ in 0..400 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let sell = ((seed >> 1) as i64).max(1);
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let buy = ((seed >> 1) as i64).max(1);
            keys.push(key(1, sell, buy));
        }
        for a in &keys {
            for b in &keys {
                assert_eq!(
                    crate::market_order_price_comparator(a, b),
                    price_key(a).unwrap().cmp(&price_key(b).unwrap())
                );
            }
        }
    }
    #[test]
    fn equivalent_prices_overwrite_delete_and_preserve_original_scan_keys() {
        let db = MarketPriceBackend::new(Arc::new(crate::MemBackend::new()));
        let a = key(1, 1, 2);
        let b = key(1, 2, 4);
        let low = key(1, 2, 1);
        db.put(&a, b"old").unwrap();
        db.put(&b, b"new").unwrap();
        db.put(&low, b"low").unwrap();
        assert_eq!(db.get(&a).unwrap(), Some(b"new".to_vec()));
        assert_eq!(
            db.scan_prefix(&[1; 38]).unwrap(),
            vec![(low.clone(), b"low".to_vec()), (b.clone(), b"new".to_vec())]
        );
        assert_eq!(db.scan_from(&a, 1).unwrap()[0].0, b);
        assert_eq!(db.scan_back_from(&a, 1).unwrap()[0].0, low);
        db.delete(&a).unwrap();
        assert_eq!(db.get(&b).unwrap(), None);
    }
}
