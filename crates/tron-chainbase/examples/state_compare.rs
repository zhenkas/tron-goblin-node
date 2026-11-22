//! Order-independent logical digests of a stopped node's state stores, for comparing two
//! databases of different lineage (for example a fresh snapshot against a long-running node).
//!
//! Account rows are compared without their TRC-10 balance maps; the balances are compared as
//! a separate logical set that merges inline maps with the account-asset store, so the
//! physical layout (inline or split) does not matter. With `--journal DIR --after H` the
//! assisted-sync journal is used to read every store as it was right after block H.
//!
//!   state_compare digest --db DIR --out FILE [--journal DIR --after H] [--stores a,b] [--bits 12]
//!   state_compare bucket --db DIR --pick CLASS:N[,CLASS:N] --out FILE [--full MAXLEN] [--journal DIR --after H]
use prost::Message;
use rocksdb::{Options, DB};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs,
    io::Write,
    path::{Path, PathBuf},
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, PartialEq, Message)]
struct Delta {
    #[prost(uint32, tag = "1")]
    store: u32,
    #[prost(bytes = "vec", tag = "2")]
    key: Vec<u8>,
    #[prost(bytes = "vec", optional, tag = "3")]
    before: Option<Vec<u8>>,
}
#[derive(Clone, PartialEq, Message)]
struct Bundle {
    #[prost(int64, tag = "4")]
    height: i64,
    #[prost(message, repeated, tag = "8")]
    deltas: Vec<Delta>,
}

/// Stores that hold blocks, caches or node-local bookkeeping rather than chain state.
const SKIP: &[&str] = &[
    "block", "block-index", "block-undo", "block_KDB", "trans", "trans-cache", "transactionRetStore",
    "transactionHistoryStore", "recent-block", "recent-transaction", "peers", "mempool",
    "tree-block-index", "pbft-sign-data", "section-bloom", "balance-trace", "account-trace",
    "common", "common-database", "account-asset",
];

type Overrides = HashMap<String, HashMap<Vec<u8>, Option<Vec<u8>>>>;

fn open(db: &Path, name: &str) -> Result<DB> {
    let mut opts = Options::default();
    opts.create_if_missing(false);
    opts.set_max_open_files(256);
    if let Some((label, compare)) = tron_chainbase::comparator_for_store(name) {
        opts.set_comparator(label, Box::new(compare));
    }
    Ok(DB::open_for_read_only(&opts, db.join(name), false)?)
}

/// The value each journaled key had right after block `after`: the `before` of its first later delta.
fn overrides(journal: &Path, after: i64) -> Result<Overrides> {
    let mut files: Vec<(i64, PathBuf)> = Vec::new();
    for item in fs::read_dir(journal)? {
        let path = item?.path();
        let height = path
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(|n| n.split('.').next())
            .and_then(|n| n.parse::<i64>().ok());
        if let Some(height) = height.filter(|h| *h > after) {
            files.push((height, path));
        }
    }
    files.sort();
    let first = files.first().map(|f| f.0).ok_or("journal has no blocks after the target height")?;
    if first != after + 1 {
        return Err(format!("journal starts at {first}, need {}", after + 1).into());
    }
    let mut out = Overrides::new();
    let mut expect = first;
    for (height, path) in &files {
        if *height != expect {
            return Err(format!("journal gap at {expect}").into());
        }
        expect += 1;
        let bundle = Bundle::decode(fs::read(path)?.as_slice())?;
        if bundle.height != *height {
            return Err("journal file height differs from its name".into());
        }
        for delta in bundle.deltas {
            let id = u8::try_from(delta.store).ok().and_then(tron_chainbase::UndoStoreId::from_u8)
                .ok_or("unknown journaled store")?;
            out.entry(id.db_name().to_string()).or_default().entry(delta.key).or_insert(delta.before);
        }
    }
    eprintln!("journal: {} blocks {}..{} reverted", files.len(), first, expect - 1);
    Ok(out)
}

/// Visit every row of `name` as of the target height.
fn scan(db: &Path, name: &str, overrides: &Overrides, mut visit: impl FnMut(&[u8], &[u8]) -> Result<()>) -> Result<()> {
    let store = open(db, name)?;
    let changed = overrides.get(name);
    let mut seen: HashSet<&[u8]> = HashSet::new();
    for item in store.iterator(rocksdb::IteratorMode::Start) {
        let (key, value) = item?;
        match changed.and_then(|c| c.get_key_value(key.as_ref())) {
            Some((k, before)) => {
                seen.insert(k.as_slice());
                if let Some(before) = before {
                    visit(&key, before)?;
                }
            }
            None => visit(&key, &value)?,
        }
    }
    if let Some(changed) = changed {
        // Rows deleted since the target height still existed then.
        let mut gone: Vec<_> = changed.iter().filter(|(k, _)| !seen.contains(k.as_slice())).collect();
        gone.sort();
        for (key, before) in gone {
            if let Some(before) = before {
                visit(key, before)?;
            }
        }
    }
    Ok(())
}

fn varint(bytes: &[u8], at: &mut usize) -> Result<u64> {
    let (mut value, mut shift) = (0u64, 0);
    loop {
        let byte = *bytes.get(*at).ok_or("truncated varint")?;
        *at += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte < 0x80 {
            return Ok(value);
        }
        shift += 7;
        if shift > 63 {
            return Err("varint overflow".into());
        }
    }
}
/// Top-level protobuf fields as (number, wire type, whole field range, payload range).
fn fields(bytes: &[u8]) -> Result<Vec<(u64, u8, std::ops::Range<usize>, std::ops::Range<usize>)>> {
    let (mut at, mut out) = (0, Vec::new());
    while at < bytes.len() {
        let start = at;
        let tag = varint(bytes, &mut at)?;
        let wire = (tag & 7) as u8;
        let payload = match wire {
            0 => { let s = at; varint(bytes, &mut at)?; s..at }
            1 => { at += 8; at - 8..at }
            5 => { at += 4; at - 4..at }
            2 => {
                let len = varint(bytes, &mut at)? as usize;
                at = at.checked_add(len).ok_or("length overflow")?;
                at - len..at
            }
            _ => return Err("unsupported wire type".into()),
        };
        if at > bytes.len() {
            return Err("truncated field".into());
        }
        out.push((tag >> 3, wire, start..at, payload));
    }
    Ok(out)
}
fn map_entry(bytes: &[u8]) -> Result<(Vec<u8>, i64)> {
    let (mut key, mut value) = (Vec::new(), 0i64);
    for (number, _, _, payload) in fields(bytes)? {
        match number {
            1 => key = bytes[payload].to_vec(),
            2 => { let mut at = payload.start; value = varint(bytes, &mut at)? as i64; }
            _ => {}
        }
    }
    Ok((key, value))
}

struct Sink {
    bits: u32,
    only: Option<HashSet<(String, usize)>>,
    full: usize,
    digests: BTreeMap<String, Vec<(u64, u128)>>,
    detail: Vec<String>,
}
impl Sink {
    fn add(&mut self, class: &str, key: &[u8], value: &[u8]) {
        let bucket = {
            let h = Sha256::digest(key);
            (u32::from_be_bytes([h[0], h[1], h[2], h[3]]) >> (32 - self.bits)) as usize
        };
        if let Some(picks) = &self.only {
            if picks.contains(&(class.to_string(), bucket)) {
                let shown = if value.len() <= self.full { hex::encode(value) } else { hex::encode(&Sha256::digest(value)[..8]) };
                self.detail.push(format!("{class} {} {} {shown}", hex::encode(key), value.len()));
            }
            return;
        }
        let mut hasher = Sha256::new();
        hasher.update((key.len() as u64).to_be_bytes());
        hasher.update(key);
        hasher.update(value);
        let h = hasher.finalize();
        let slots = self.digests.entry(class.to_string()).or_insert_with(|| vec![(0, 0); 1 << self.bits]);
        slots[bucket].0 += 1;
        slots[bucket].1 = slots[bucket].1.wrapping_add(u128::from_be_bytes(h[..16].try_into().unwrap()));
    }
}

fn accounts(db: &Path, overrides: &Overrides, sink: &mut Sink) -> Result<()> {
    let assets = open(db, "account-asset")?;
    // Balance rows as of the target height: rows changed since then take their journaled
    // `before` (or vanish if they did not exist yet), rows deleted since then come back.
    let no_changes = HashMap::new();
    let changed = overrides.get("account-asset").unwrap_or(&no_changes);
    let mut restored: Vec<(Vec<u8>, Vec<u8>)> =
        changed.iter().filter_map(|(k, before)| before.clone().map(|b| (k.clone(), b))).collect();
    restored.sort();
    let mut restored = restored.into_iter().peekable();
    let mut stored = assets
        .iterator(rocksdb::IteratorMode::Start)
        .filter_map(|item| item.ok())
        .filter(|(k, _)| !changed.contains_key(k.as_ref()))
        .map(|(k, v)| (k.to_vec(), v.to_vec()))
        .peekable();
    let mut rows = std::iter::from_fn(move || match (stored.peek(), restored.peek()) {
        (Some(a), Some(b)) if b.0 < a.0 => restored.next(),
        (Some(_), _) => stored.next(),
        (None, _) => restored.next(),
    })
    .peekable();
    let (mut orphans, mut unoptimized_rows, mut zero) = (0u64, 0u64, 0u64);
    let mut pending: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    scan(db, "account", overrides, |key, value| {
        // Store rows of this owner; both stores sort bytewise and account keys have one length.
        pending.clear();
        while let Some((row_key, row_value)) = rows.peek() {
            let owner = &row_key[..row_key.len().min(key.len())];
            if owner < key {
                orphans += 1;
            } else if owner == key && row_key.len() > key.len() {
                pending.push((row_key[key.len()..].to_vec(), row_value.to_vec()));
            } else if owner > key {
                break;
            }
            rows.next();
        }
        let parsed = fields(value)?;
        let mut stripped = Vec::with_capacity(value.len());
        let mut optimized = false;
        let mut v2: BTreeMap<Vec<u8>, i64> = BTreeMap::new();
        let mut v1: BTreeMap<Vec<u8>, i64> = BTreeMap::new();
        for (number, wire, whole, payload) in parsed {
            match (number, wire) {
                (56, 2) => { let (k, v) = map_entry(&value[payload])?; v2.insert(k, v); }
                (6, 2) => { let (k, v) = map_entry(&value[payload])?; v1.insert(k, v); }
                _ => {
                    if number == 60 {
                        let mut at = payload.start;
                        optimized = varint(value, &mut at)? != 0;
                    }
                    stripped.extend_from_slice(&value[whole]);
                }
            }
        }
        sink.add("account", key, &stripped);
        if optimized {
            for (token, raw) in &pending {
                let balance = match raw.len() {
                    8 => i64::from_be_bytes(raw[..].try_into().unwrap()),
                    _ => return Err(format!("asset row value of {} bytes", raw.len()).into()),
                };
                v2.entry(token.clone()).or_insert(balance);
            }
        } else {
            unoptimized_rows += pending.len() as u64;
        }
        for (class, map) in [("asset-v2", &v2), ("asset-v1", &v1)] {
            for (token, balance) in map {
                if *balance == 0 {
                    zero += 1;
                    continue;
                }
                let mut item = key.to_vec();
                item.push(0);
                item.extend_from_slice(token);
                sink.add(class, &item, &balance.to_be_bytes());
            }
        }
        Ok(())
    })?;
    orphans += rows.count() as u64;
    eprintln!("account-asset: rows without an account {orphans}, rows of unoptimized accounts {unoptimized_rows}, zero balances skipped {zero}");
    Ok(())
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let flag = |name: &str| args.iter().position(|a| a == name).and_then(|i| args.get(i + 1)).cloned();
    let mode = args.get(1).map(String::as_str).unwrap_or("");
    let db = PathBuf::from(flag("--db").ok_or("--db DIR required")?);
    let out = flag("--out").ok_or("--out FILE required")?;
    let bits: u32 = flag("--bits").map(|b| b.parse()).transpose()?.unwrap_or(12);
    if !(1..=20).contains(&bits) {
        return Err("--bits must be 1..=20".into());
    }
    let overrides = match (flag("--journal"), flag("--after")) {
        (Some(dir), Some(after)) => overrides(Path::new(&dir), after.parse()?)?,
        (None, None) => Overrides::new(),
        _ => return Err("--journal and --after go together".into()),
    };
    if mode == "rows" {
        // Raw rows of one store under a key prefix, as of the target height.
        let name = flag("--store").ok_or("--store required")?;
        let prefix = hex::decode(flag("--prefix").ok_or("--prefix HEX required")?)?;
        let store = open(&db, &name)?;
        let changed = overrides.get(&name);
        let mut rows: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        for item in store.iterator(rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward)) {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            rows.insert(key.to_vec(), value.to_vec());
        }
        for (key, before) in changed.into_iter().flatten().filter(|(k, _)| k.starts_with(&prefix)) {
            match before {
                Some(value) => rows.insert(key.clone(), value.clone()),
                None => rows.remove(key),
            };
        }
        let mut file = std::io::BufWriter::new(fs::File::create(&out)?);
        for (key, value) in rows {
            writeln!(file, "{name} {} {} {}", hex::encode(&key), value.len(), hex::encode(&value))?;
        }
        return Ok(file.flush()?);
    }
    let only = match mode {
        "digest" => None,
        "bucket" => {
            let mut picks = HashSet::new();
            for pick in flag("--pick").ok_or("--pick class:bucket[,class:bucket] required")?.split(',') {
                let (class, bucket) = pick.rsplit_once(':').ok_or("--pick wants class:bucket")?;
                picks.insert((class.to_string(), bucket.parse()?));
            }
            Some(picks)
        }
        _ => return Err("usage: state_compare digest|bucket --db DIR --out FILE ...".into()),
    };
    let mut stores: Vec<String> = match flag("--stores") {
        Some(list) => list.split(',').map(str::to_string).collect(),
        None => fs::read_dir(&db)?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .filter_map(|e| e.file_name().into_string().ok())
            .filter(|n| !SKIP.contains(&n.as_str()))
            .collect(),
    };
    stores.sort();
    if let Some(picks) = &only {
        let wanted: HashSet<&str> = picks.iter()
            .map(|(class, _)| if class.starts_with("asset-v") { "account" } else { class.as_str() })
            .collect();
        stores.retain(|s| wanted.contains(s.as_str()));
    }
    let full: usize = flag("--full").map(|b| b.parse()).transpose()?.unwrap_or(0);
    let mut sink = Sink { bits, only, full, digests: BTreeMap::new(), detail: Vec::new() };
    for name in &stores {
        let started = std::time::Instant::now();
        if name == "account" {
            accounts(&db, &overrides, &mut sink)?;
        } else {
            scan(&db, name, &overrides, |key, value| { sink.add(name, key, value); Ok(()) })?;
        }
        eprintln!("{name}: {:.0}s", started.elapsed().as_secs_f64());
    }
    let mut file = std::io::BufWriter::new(fs::File::create(&out)?);
    if sink.only.is_some() {
        sink.detail.sort();
        for line in &sink.detail {
            writeln!(file, "{line}")?;
        }
    } else {
        for (class, slots) in &sink.digests {
            let total: u64 = slots.iter().map(|s| s.0).sum();
            let all = slots.iter().fold(0u128, |a, s| a.wrapping_add(s.1));
            println!("{class} rows={total} digest={all:032x}");
            for (index, (count, digest)) in slots.iter().enumerate() {
                writeln!(file, "{class} {index} {count} {digest:032x}")?;
            }
        }
    }
    file.flush()?;
    Ok(())
}
