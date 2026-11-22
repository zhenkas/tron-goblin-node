use crate::model::{projected_key_table_bytes, KeyStorage};
use serde::Serialize;
use serde_json::{json, Value};
use std::{collections::BTreeMap, fs, path::Path};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
#[derive(Clone, Serialize, serde::Deserialize)]
pub struct Store {
    pub name: String,
    pub estimated_keys: u64,
    #[serde(default)]
    pub rocksdb_estimated_keys: Option<u64>,
    #[serde(default)]
    pub count_source: String,
    pub target: u64,
    pub sequence: u64,
    pub sampled: u64,
    pub raw_key_bytes: u64,
    pub raw_value_bytes: u64,
    #[serde(default)]
    pub inline_count: u64,
    #[serde(default)]
    pub inline_bytes: u64,
    pub max_value_bytes: usize,
    pub large_count: u64,
    pub large_bytes: u64,
    pub classes: BTreeMap<usize, u64>,
    pub value_lengths: BTreeMap<usize, u64>,
    pub key_lengths: BTreeMap<usize, u64>,
}
impl Store {
    pub fn observe(&mut self, k: &[u8], v: &[u8], cutoff: usize) {
        self.observe_with_step(k, v, cutoff, 0);
    }
    pub fn observe_with_step(&mut self, k: &[u8], v: &[u8], cutoff: usize, step: usize) {
        self.sampled += 1;
        self.raw_key_bytes += k.len() as u64;
        self.raw_value_bytes += v.len() as u64;
        self.max_value_bytes = self.max_value_bytes.max(v.len());
        *self.value_lengths.entry(v.len()).or_default() += 1;
        *self.key_lengths.entry(k.len()).or_default() += 1;
        if v.len() <= 12 {
            self.inline_count += 1;
            self.inline_bytes += v.len() as u64;
        } else if v.len() > cutoff {
            self.large_count += 1;
            self.large_bytes += v.len() as u64;
        } else if !v.is_empty() {
            let size = if step == 0 {
                v.len().max(16).next_power_of_two()
            } else {
                v.len().div_ceil(step) * step
            };
            *self.classes.entry(size).or_default() += 1;
        }
    }
}
pub fn target(estimated: u64, bps: u64) -> u64 {
    estimated.saturating_mul(bps).div_ceil(10_000)
}
pub fn process_memory() -> Value {
    let mut out = BTreeMap::new();
    for file in ["/proc/self/status", "/proc/self/smaps_rollup"] {
        if let Ok(text) = fs::read_to_string(file) {
            for line in text.lines() {
                let mut p = line.split_whitespace();
                if let (Some(key), Some(value)) = (p.next(), p.next()) {
                    if [
                        "VmRSS:",
                        "VmLck:",
                        "Locked:",
                        "VmHWM:",
                        "VmSize:",
                        "VmSwap:",
                        "Rss:",
                        "Anonymous:",
                        "Private_Dirty:",
                        "Swap:",
                        "Pss:",
                    ]
                    .contains(&key)
                    {
                        if let Ok(kb) = value.parse::<u64>() {
                            out.insert(key.trim_end_matches(':').to_owned(), kb * 1024);
                        }
                    }
                }
            }
        }
    }
    json!(out)
}
pub fn save(path: &Path, value: &impl Serialize) -> Result<()> {
    let temp = path.with_extension("json.new");
    fs::write(&temp, serde_json::to_vec_pretty(value)?)?;
    fs::rename(temp, path)?;
    Ok(())
}
pub fn projection(stores: &[Store], cells: u32, shards: u64) -> Value {
    projection_with_keys(stores, cells, shards, KeyStorage::Arena)
}
pub fn projection_with_keys(
    stores: &[Store],
    cells: u32,
    shards: u64,
    storage: KeyStorage,
) -> Value {
    let mut keys = 0u64;
    let (mut raw_keys, mut raw_values, mut large) = (0f64, 0f64, 0f64);
    let (mut inline_count, mut inline_bytes) = (0f64, 0f64);
    let mut classes: BTreeMap<usize, f64> = BTreeMap::new();
    let mut key_sizes: BTreeMap<usize, f64> = BTreeMap::new();
    for s in stores {
        if s.sampled == 0 {
            continue;
        }
        keys += s.estimated_keys;
        let weight = s.estimated_keys as f64 / s.sampled as f64;
        raw_keys += s.raw_key_bytes as f64 * weight;
        raw_values += s.raw_value_bytes as f64 * weight;
        large += s.large_bytes as f64 * weight;
        inline_count += s.inline_count as f64 * weight;
        inline_bytes += s.inline_bytes as f64 * weight;
        for (size, count) in &s.classes {
            *classes.entry(*size).or_default() += *count as f64 * weight;
        }
        for (size, count) in &s.key_lengths {
            *key_sizes.entry(*size).or_default() += *count as f64 * weight;
        }
    }
    let key_bytes = if storage == KeyStorage::Inline {
        key_sizes
            .iter()
            .filter(|(size, _)| **size > 64)
            .map(|(size, count)| *size as f64 * count)
            .sum::<f64>()
            .ceil() as u64
    } else {
        raw_keys.ceil() as u64
    };
    let used_pooled = classes
        .iter()
        .map(|(size, count)| *size as f64 * count)
        .sum::<f64>()
        .ceil() as u64;
    let reserved_shared = classes
        .iter()
        .map(|(size, count)| (*count / cells as f64).ceil() as u64 * cells as u64 * *size as u64)
        .sum::<u64>();
    let reserved_independent = classes
        .iter()
        .map(|(size, count)| {
            (*count / (cells as u64 * shards) as f64).ceil() as u64
                * cells as u64
                * shards
                * *size as u64
        })
        .sum::<u64>();
    let table_by_size = key_sizes
        .iter()
        .map(|(len, count)| projected_key_table_bytes(count.ceil() as u64, *len, storage))
        .sum::<u64>();
    let table_shards = key_sizes
        .iter()
        .map(|(len, count)| {
            projected_key_table_bytes((count / shards as f64).ceil() as u64, *len, storage) * shards
        })
        .sum::<u64>();
    let key_reserved = key_bytes.div_ceil(16 * 1024 * 1024) * (16 * 1024 * 1024);
    json!({
        "key_storage":storage,"estimated_live_pairs":keys,"raw_key_bytes":raw_keys.round() as u64,"raw_value_bytes":raw_values.round() as u64,
        "key_arena_used_bytes":key_bytes,"key_arena_reserved_bytes":key_reserved,"per_key_length_header_bytes":0,
        "maps_per_key_size":key_sizes.len(),"key_size_map_table_bytes":table_by_size,
        "projected_shards":shards,"sharded_key_size_map_table_bytes":table_shards,
        "inline_value_count":inline_count.round() as u64,"inline_value_bytes":inline_bytes.round() as u64,
        "value_slot_bytes_in_use":used_pooled,"standalone_large_value_bytes":large.round() as u64,
        "shared_value_pools_reserved_bytes":reserved_shared,"independent_per_shard_value_pools_reserved_bytes":reserved_independent,
        "key_size_maps_occupied_model_bytes":table_by_size+key_bytes+used_pooled+large.round() as u64,
        "sharded_key_size_maps_occupied_model_bytes":table_shards+key_bytes+used_pooled+large.round() as u64,
        "key_size_maps_reserved_model_bytes":table_by_size+key_reserved+reserved_shared+large.round() as u64,
        "weighted_value_classes":classes,"weighted_key_size_counts":key_sizes,
        "limits":"prefix-biased sample; supplied older verified counts and RocksDB estimates remain approximate for current live data; excludes allocator rounding, production overlays and runtime fragmentation. Untouched capacity is virtual reserve."
    })
}
pub const MAGIC: &[u8; 8] = b"TKVMAP01";

#[derive(serde::Serialize, serde::Deserialize)]
pub struct FileHeader {
    pub schema: u32,
    pub basis_points: u64,
    pub stores: Vec<Store>,
}
pub fn write_header(out: &mut impl std::io::Write, bps: u64, stores: &[Store]) -> Result<()> {
    let value =
        serde_json::to_vec(&serde_json::json!({"schema":1,"basis_points":bps,"stores":stores}))?;
    out.write_all(MAGIC)?;
    out.write_all(&(value.len() as u64).to_le_bytes())?;
    out.write_all(&value)?;
    Ok(())
}
pub fn write_pair(out: &mut impl std::io::Write, id: u16, key: &[u8], value: &[u8]) -> Result<()> {
    let kl = u32::try_from(key.len())?;
    let vl = u32::try_from(value.len())?;
    out.write_all(&id.to_le_bytes())?;
    out.write_all(&kl.to_le_bytes())?;
    out.write_all(&vl.to_le_bytes())?;
    out.write_all(key)?;
    out.write_all(value)?;
    Ok(())
}
pub fn write_footer(out: &mut impl std::io::Write, count: u64, sum: u64) -> Result<()> {
    out.write_all(&u16::MAX.to_le_bytes())?;
    out.write_all(&count.to_le_bytes())?;
    out.write_all(&sum.to_le_bytes())?;
    Ok(())
}
pub struct Frame<'a> {
    pub store: u16,
    pub key: &'a [u8],
    pub value: &'a [u8],
}
pub struct Input<R: std::io::Read> {
    reader: R,
    pub header: FileHeader,
    key: Vec<u8>,
    value: Vec<u8>,
    pub count: u64,
    pub checksum: u64,
    finished: bool,
}
impl<R: std::io::Read> Input<R> {
    pub fn new(mut reader: R) -> Result<Self> {
        let mut magic = [0u8; 8];
        reader.read_exact(&mut magic)?;
        if &magic != MAGIC {
            return Err("sample magic mismatch".into());
        }
        let mut bytes = [0u8; 8];
        reader.read_exact(&mut bytes)?;
        let len = u64::from_le_bytes(bytes);
        if len > 64 * 1024 * 1024 {
            return Err("sample header too large".into());
        }
        let mut json = vec![0u8; len as usize];
        reader.read_exact(&mut json)?;
        let header: FileHeader = serde_json::from_slice(&json)?;
        if header.schema != 1 || header.stores.len() >= u16::MAX as usize {
            return Err("invalid sample header".into());
        }
        Ok(Self {
            reader,
            header,
            key: Vec::new(),
            value: Vec::new(),
            count: 0,
            checksum: 0,
            finished: false,
        })
    }
    pub fn next_record(&mut self) -> Result<Option<Frame<'_>>> {
        if self.finished {
            return Ok(None);
        }
        let mut id = [0u8; 2];
        self.reader.read_exact(&mut id)?;
        let store = u16::from_le_bytes(id);
        if store == u16::MAX {
            let mut count = [0u8; 8];
            let mut sum = [0u8; 8];
            self.reader.read_exact(&mut count)?;
            self.reader.read_exact(&mut sum)?;
            if u64::from_le_bytes(count) != self.count || u64::from_le_bytes(sum) != self.checksum {
                return Err("sample footer mismatch".into());
            }
            let mut extra = [0u8; 1];
            if self.reader.read(&mut extra)? != 0 {
                return Err("unexpected data after sample footer".into());
            }
            self.finished = true;
            return Ok(None);
        }
        if store as usize >= self.header.stores.len() {
            return Err("sample namespace out of range".into());
        }
        let mut kl = [0u8; 4];
        let mut vl = [0u8; 4];
        self.reader.read_exact(&mut kl)?;
        self.reader.read_exact(&mut vl)?;
        let (kl, vl) = (
            u32::from_le_bytes(kl) as usize,
            u32::from_le_bytes(vl) as usize,
        );
        if kl > 16 * 1024 * 1024 || vl > 1024 * 1024 * 1024 {
            return Err("invalid sample record lengths".into());
        }
        self.key.resize(kl, 0);
        self.value.resize(vl, 0);
        self.reader.read_exact(&mut self.key)?;
        self.reader.read_exact(&mut self.value)?;
        self.count += 1;
        self.checksum = self
            .key
            .iter()
            .chain(&self.value)
            .fold(self.checksum, |a, b| a.wrapping_add(*b as u64));
        Ok(Some(Frame {
            store,
            key: &self.key,
            value: &self.value,
        }))
    }
}
#[cfg(test)]
mod stream_tests {
    use super::*;
    fn bytes() -> Vec<u8> {
        let mut data = Vec::new();
        // One empty metadata-only namespace is sufficient for framing tests.
        let header = serde_json::json!({"schema":1,"basis_points":100,"stores":[{
            "name":"fixture","estimated_keys":2,"target":2,"sequence":0,"sampled":0,
            "raw_key_bytes":0,"raw_value_bytes":0,"max_value_bytes":0,"large_count":0,"large_bytes":0,
            "classes":{},"value_lengths":{},"key_lengths":{}}]});
        let h = serde_json::to_vec(&header).unwrap();
        data.extend_from_slice(MAGIC);
        data.extend_from_slice(&(h.len() as u64).to_le_bytes());
        data.extend_from_slice(&h);
        write_pair(&mut data, 0, b"k", b"value").unwrap();
        write_pair(&mut data, 0, b"", b"").unwrap();
        write_footer(&mut data, 2, b"kvalue".iter().map(|x| *x as u64).sum()).unwrap();
        data
    }
    #[test]
    fn serialized_pairs_round_trip_with_empty_bytes() {
        let mut input = Input::new(std::io::Cursor::new(bytes())).unwrap();
        let r = input.next_record().unwrap().unwrap();
        assert_eq!(
            (r.store, r.key, r.value),
            (0, b"k".as_slice(), b"value".as_slice())
        );
        let r = input.next_record().unwrap().unwrap();
        assert_eq!((r.key, r.value), (b"".as_slice(), b"".as_slice()));
        assert!(input.next_record().unwrap().is_none());
        assert_eq!(input.count, 2);
    }
    #[test]
    fn truncated_and_corrupt_streams_are_rejected() {
        for corrupt in [false, true] {
            let mut data = bytes();
            if corrupt {
                let n = data.len();
                data[n - 1] ^= 1;
            } else {
                data.truncate(data.len() - 1);
            }
            let mut input = Input::new(std::io::Cursor::new(data)).unwrap();
            assert!(input.next_record().unwrap().is_some());
            assert!(input.next_record().unwrap().is_some());
            assert!(input.next_record().is_err());
        }
    }
}
