//! Experimental memory model, deliberately separate from the production KV provider.
use hashbrown::HashTable;
use serde::Serialize;
use std::{collections::BTreeMap, mem::size_of, ptr};

#[path = "inline.rs"]
mod inline;
use inline::InlineTable;

#[derive(Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyStorage {
    Arena,
    Inline,
}

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
const NONE: u32 = u32::MAX;
#[derive(Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ValueAllocator {
    Pooled,
    System,
}

// Packed to four-byte alignment so the union remains 12 bytes. Fields are
// copied by value; no references to its potentially unaligned fields are taken.
#[derive(Clone, Copy)]
#[repr(C, packed(4))]
struct ExternalRef {
    pointer: *mut u8,
    index: i32,
}
#[derive(Clone, Copy)]
#[repr(C)]
union Payload {
    inline: [u8; 12],
    external: ExternalRef,
}
#[derive(Clone, Copy)]
#[repr(C)]
pub struct ValueRef {
    payload: Payload,
    // Also the discriminator: len <= 12 is inline; otherwise external.
    len: u32,
}
impl ValueRef {
    fn external(self) -> ExternalRef {
        debug_assert!(self.len > 12);
        // SAFETY: only external allocations construct references with len > 12.
        unsafe { self.payload.external }
    }
}

struct Pool {
    id: usize,
    class: usize,
    stride: usize,
    capacity: u32,
    next_unused: u32,
    free_head: u32,
    live: u32,
    previous: Option<usize>,
    next: Option<usize>,
    full: bool,
    bytes: Box<[u8]>,
}
impl Pool {
    fn allocate(&mut self, value: &[u8]) -> u32 {
        let slot = if self.free_head != NONE {
            let slot = self.free_head;
            let start = slot as usize * self.stride;
            self.free_head = u32::from_le_bytes(self.bytes[start..start + 4].try_into().unwrap());
            slot
        } else {
            assert!(self.next_unused < self.capacity);
            let slot = self.next_unused;
            self.next_unused += 1;
            slot
        };
        let start = slot as usize * self.stride;
        self.bytes[start..start + value.len()].copy_from_slice(value);
        self.live += 1;
        slot
    }
    fn release(&mut self, slot: u32) {
        assert!(slot < self.next_unused && self.live > 0);
        let start = slot as usize * self.stride;
        self.bytes[start..start + 4].copy_from_slice(&self.free_head.to_le_bytes());
        self.free_head = slot;
        self.live -= 1;
    }
}
#[derive(Default)]
struct Class {
    available: Option<usize>,
    full: Option<usize>,
}

pub struct Values {
    #[allow(clippy::vec_box)] // Stable pool addresses are part of ValueRef.
    blocks: Vec<Option<Box<Pool>>>, // Live boxes remain stable; empty IDs are reused.
    free_blocks: Vec<usize>,
    classes: Vec<Class>,
    cells: u32,
    cutoff: usize,
    pool_step: usize,
    allocator: ValueAllocator,
    standalone_count: u64,
    standalone_bytes: u64,
    raw_bytes: u64,
    inline_count: u64,
    inline_bytes: u64,
    large_bytes: u64,
    large_count: u64,
}
impl Values {
    fn new(cells: u32, cutoff: usize) -> Self {
        assert!(cells > 0 && cells <= i32::MAX as u32);
        assert!(cutoff >= 16 && cutoff.is_power_of_two() && cutoff <= 32768);
        Self {
            blocks: Vec::new(),
            free_blocks: Vec::new(),
            classes: (0..=(cutoff.trailing_zeros() - 4))
                .map(|_| Class::default())
                .collect(),
            cells,
            cutoff,
            pool_step: 0,
            allocator: ValueAllocator::Pooled,
            standalone_count: 0,
            standalone_bytes: 0,
            raw_bytes: 0,
            inline_count: 0,
            inline_bytes: 0,
            large_bytes: 0,
            large_count: 0,
        }
    }
    fn set_pool_step(&mut self, step: usize) {
        assert!(
            self.blocks.is_empty(),
            "choose pool classes before allocating"
        );
        assert!(step == 0 || (step >= 16 && step.is_power_of_two() && self.cutoff % step == 0));
        self.pool_step = step;
        let count = if step == 0 {
            (self.cutoff.trailing_zeros() - 3) as usize
        } else {
            self.cutoff / step
        };
        self.classes = (0..count).map(|_| Class::default()).collect();
    }
    fn unlink(&mut self, id: usize) {
        let p = self.blocks[id].as_deref().unwrap();
        let (class, full, previous, next) = (p.class, p.full, p.previous, p.next);
        if let Some(previous) = previous {
            self.blocks[previous].as_deref_mut().unwrap().next = next;
        } else if full {
            self.classes[class].full = next;
        } else {
            self.classes[class].available = next;
        }
        if let Some(next) = next {
            self.blocks[next].as_deref_mut().unwrap().previous = previous;
        }
        self.blocks[id].as_deref_mut().unwrap().previous = None;
        self.blocks[id].as_deref_mut().unwrap().next = None;
    }
    fn link(&mut self, id: usize, full: bool) {
        let class = self.blocks[id].as_deref_mut().unwrap().class;
        let head = if full {
            self.classes[class].full
        } else {
            self.classes[class].available
        };
        self.blocks[id].as_deref_mut().unwrap().previous = None;
        self.blocks[id].as_deref_mut().unwrap().next = head;
        self.blocks[id].as_deref_mut().unwrap().full = full;
        if let Some(head) = head {
            self.blocks[head].as_deref_mut().unwrap().previous = Some(id);
        }
        if full {
            self.classes[class].full = Some(id);
        } else {
            self.classes[class].available = Some(id);
        }
    }
    fn allocate(&mut self, value: &[u8]) -> Result<ValueRef> {
        let len = u32::try_from(value.len()).map_err(|_| "value exceeds u32 length")?;
        if value.len() <= 12 {
            let mut bytes = [0u8; 12];
            bytes[..value.len()].copy_from_slice(value);
            self.raw_bytes += value.len() as u64;
            self.inline_count += 1;
            self.inline_bytes += value.len() as u64;
            return Ok(ValueRef {
                payload: Payload { inline: bytes },
                len,
            });
        }
        if self.allocator == ValueAllocator::System || value.len() > self.cutoff {
            let boxed = value.to_vec().into_boxed_slice();
            let pointer = Box::into_raw(boxed) as *mut u8;
            self.standalone_count += 1;
            self.standalone_bytes += value.len() as u64;
            if value.len() > self.cutoff {
                self.large_bytes += value.len() as u64;
                self.large_count += 1;
            }
            self.raw_bytes += value.len() as u64;
            return Ok(ValueRef {
                payload: Payload {
                    external: ExternalRef { pointer, index: -1 },
                },
                len,
            });
        }
        let (stride, class) = if self.pool_step == 0 {
            let stride = value.len().max(16).next_power_of_two();
            (stride, (stride.trailing_zeros() - 4) as usize)
        } else {
            let stride = value.len().div_ceil(self.pool_step) * self.pool_step;
            (stride, stride / self.pool_step - 1)
        };
        let id = if let Some(id) = self.classes[class].available {
            id
        } else {
            let id = if let Some(id) = self.free_blocks.pop() {
                id
            } else {
                // Release only pushes an existing ID; reserve here so it never allocates.
                self.free_blocks.reserve(self.blocks.len() + 1);
                self.blocks.push(None);
                self.blocks.len() - 1
            };
            let bytes = stride
                .checked_mul(self.cells as usize)
                .ok_or("pool capacity overflow")?;
            self.blocks[id] = Some(Box::new(Pool {
                id,
                class,
                stride,
                capacity: self.cells,
                next_unused: 0,
                free_head: NONE,
                live: 0,
                previous: None,
                next: None,
                full: false,
                bytes: vec![0; bytes].into_boxed_slice(),
            }));
            self.link(id, false);
            id
        };
        let pool = self.blocks[id].as_deref_mut().unwrap();
        let slot = pool.allocate(value);
        let pointer = pool as *mut Pool as *mut u8;
        if pool.live == pool.capacity {
            self.unlink(id);
            self.link(id, true);
        }
        self.raw_bytes += value.len() as u64;
        Ok(ValueRef {
            payload: Payload {
                external: ExternalRef {
                    pointer,
                    index: slot as i32,
                },
            },
            len,
        })
    }
    fn get<'a>(&'a self, value: &'a ValueRef) -> &'a [u8] {
        if value.len <= 12 {
            // SAFETY: inline references initialize all 12 union bytes. The slice
            // borrows the owning table entry, never a temporary copied ValueRef.
            return unsafe { &value.payload.inline[..value.len as usize] };
        }
        let external = value.external();
        if external.index == -1 {
            // SAFETY: pointer and length belong to this live standalone entry.
            return unsafe { std::slice::from_raw_parts(external.pointer, value.len as usize) };
        }
        // SAFETY: live pool boxes stay stable; entries are removed before release.
        let pool = unsafe { &*(external.pointer as *const Pool) };
        let offset = external.index as usize * pool.stride;
        &pool.bytes[offset..offset + value.len as usize]
    }
    fn release(&mut self, value: ValueRef) {
        self.raw_bytes -= value.len as u64;
        if value.len <= 12 {
            self.inline_count -= 1;
            self.inline_bytes -= value.len as u64;
            return;
        }
        let external = value.external();
        if external.index == -1 {
            self.standalone_count -= 1;
            self.standalone_bytes -= value.len as u64;
            if value.len as usize > self.cutoff {
                self.large_bytes -= value.len as u64;
                self.large_count -= 1;
            }
            // SAFETY: exactly the pointer/length created by Box::into_raw in allocate.
            // A reference is released once, after its unique owning entry is replaced/removed.
            unsafe {
                drop(Box::from_raw(ptr::slice_from_raw_parts_mut(
                    external.pointer,
                    value.len as usize,
                )));
            }
            return;
        }
        let pool = unsafe { &mut *(external.pointer as *mut Pool) };
        let id = pool.id;
        let was_full = pool.live == pool.capacity;
        pool.release(external.index as u32);
        if pool.live == 0 {
            // No table entry points here now. Unlink before dropping the pool box
            // and its whole payload; later allocations may reuse this ID in O(1).
            self.unlink(id);
            drop(self.blocks[id].take());
            self.free_blocks.push(id);
        } else if was_full {
            self.unlink(id);
            self.link(id, false);
        }
    }
    fn stats(&self) -> ValueStats {
        let mut classes = Vec::new();
        for class in 0..self.classes.len() {
            let blocks: Vec<_> = self
                .blocks
                .iter()
                .flatten()
                .filter(|b| b.class == class)
                .collect();
            if blocks.is_empty() {
                continue;
            }
            classes.push(ClassStats {
                slot_bytes: if self.pool_step == 0 {
                    16usize << class
                } else {
                    (class + 1) * self.pool_step
                },
                pools: blocks.len(),
                live_cells: blocks.iter().map(|b| b.live as u64).sum(),
                reserved_bytes: blocks.iter().map(|b| b.bytes.len() as u64).sum(),
                touched_page_span_bytes: blocks
                    .iter()
                    .map(|b| page_up(b.next_unused as u64 * b.stride as u64))
                    .sum(),
            });
        }
        ValueStats {
            allocator: self.allocator,
            pool_step_bytes: self.pool_step,
            standalone_count: self.standalone_count,
            standalone_requested_bytes: self.standalone_bytes,
            standalone_usable_bytes: None,
            raw_bytes: self.raw_bytes,
            inline_count: self.inline_count,
            inline_bytes: self.inline_bytes,
            large_bytes: self.large_bytes,
            large_count: self.large_count,
            pool_metadata_bytes: (self.blocks.iter().flatten().count() * size_of::<Pool>()
                + self.blocks.capacity() * size_of::<Option<Box<Pool>>>()
                + self.free_blocks.capacity() * size_of::<usize>()
                + self.classes.capacity() * size_of::<Class>())
                as u64,
            classes,
        }
    }
}
#[derive(Clone, Copy)]
#[repr(C)]
struct KeyRef {
    chunk: u16,
    store: u16,
    offset: u32,
}
struct Keys {
    chunks: Vec<Vec<u8>>,
    chunk_bytes: usize,
    used_bytes: u64,
}
impl Keys {
    fn put(&mut self, store: u16, key: &[u8]) -> Result<KeyRef> {
        let needed = key.len();
        if self
            .chunks
            .last()
            .map_or(true, |c| c.capacity() - c.len() < needed)
        {
            self.chunks
                .push(Vec::with_capacity(self.chunk_bytes.max(needed)));
        }
        let chunk =
            u16::try_from(self.chunks.len() - 1).map_err(|_| "key arena exceeds 65536 chunks")?;
        let bytes = self.chunks.last_mut().unwrap();
        let offset = u32::try_from(bytes.len()).map_err(|_| "key offset exceeds u32")?;
        bytes.extend_from_slice(key);
        self.used_bytes += needed as u64;
        Ok(KeyRef {
            chunk,
            store,
            offset,
        })
    }
    fn get(&self, key: KeyRef, len: usize) -> (u16, &[u8]) {
        let start = key.offset as usize;
        (
            key.store,
            &self.chunks[key.chunk as usize][start..start + len],
        )
    }
}
#[derive(Clone, Copy)]
#[repr(C)]
struct Entry {
    hash: u64,
    key: KeyRef,
    value: ValueRef,
}

pub(crate) fn key_hash(hashed_stores: &[bool], store: u16, key: &[u8]) -> u64 {
    let namespace = (store as u64).wrapping_mul(0x9e3779b97f4a7c15);
    if hashed_stores.get(store as usize).copied().unwrap_or(false) && key.len() >= 8 {
        // Existing cryptographic digest: reuse its random tail, including for
        // block IDs whose first eight bytes contain a structured block height.
        return u64::from_le_bytes(key[key.len() - 8..].try_into().unwrap()) ^ namespace;
    }
    // Small, non-cryptographic word mixer for addresses, property names and
    // numeric/index keys. Real bytes and namespace are always checked on collision.
    let mut h = namespace ^ (key.len() as u64).wrapping_mul(0x517cc1b727220a95);
    let mut chunks = key.chunks_exact(8);
    for word in &mut chunks {
        h = (h.rotate_left(27) ^ u64::from_le_bytes(word.try_into().unwrap()))
            .wrapping_mul(0x9e3779b185ebca87);
    }
    let mut tail = [0u8; 8];
    tail[..chunks.remainder().len()].copy_from_slice(chunks.remainder());
    h ^= u64::from_le_bytes(tail);
    h ^= h >> 30;
    h = h.wrapping_mul(0xbf58476d1ce4e5b9);
    h ^= h >> 27;
    h = h.wrapping_mul(0x94d049bb133111eb);
    h ^ (h >> 31)
}

pub struct Map {
    // Each table has a fixed key width. The width is stored once, here,
    // never in an entry or in the raw key arena.
    tables: BTreeMap<usize, HashTable<Entry>>,
    inline_tables: BTreeMap<usize, InlineTable>,
    key_storage: KeyStorage,
    keys: Keys,
    values: Values,
    hashed_stores: Vec<bool>,
}
impl Map {
    pub fn new(cells: u32, cutoff: usize, key_chunk: usize) -> Self {
        Self {
            tables: BTreeMap::new(),
            inline_tables: BTreeMap::new(),
            key_storage: KeyStorage::Arena,
            keys: Keys {
                chunks: Vec::new(),
                chunk_bytes: key_chunk,
                used_bytes: 0,
            },
            values: Values::new(cells, cutoff),
            hashed_stores: Vec::new(),
        }
    }
    pub fn set_key_storage(&mut self, storage: KeyStorage) {
        assert_eq!(self.len(), 0, "choose key storage before loading entries");
        self.key_storage = storage;
    }
    pub fn set_value_allocator(&mut self, allocator: ValueAllocator) {
        assert_eq!(self.len(), 0, "choose the allocator before loading entries");
        self.values.allocator = allocator;
    }
    pub fn set_pool_step(&mut self, step: usize) {
        assert_eq!(self.len(), 0, "choose pool classes before loading entries");
        self.values.set_pool_step(step);
    }
    pub fn mark_prehashed_store(&mut self, store: u16) {
        self.hashed_stores
            .resize(self.hashed_stores.len().max(store as usize + 1), false);
        self.hashed_stores[store as usize] = true;
    }
    fn hash_key(&self, store: u16, key: &[u8]) -> u64 {
        key_hash(&self.hashed_stores, store, key)
    }
    pub fn put(&mut self, store: u16, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_hashed(self.hash_key(store, key), store, key, value)
    }
    fn put_hashed(&mut self, hash: u64, store: u16, key: &[u8], value: &[u8]) -> Result<()> {
        if self.key_storage == KeyStorage::Inline && key.len() <= 64 {
            let table = self
                .inline_tables
                .entry(key.len())
                .or_insert_with(|| InlineTable::new(key.len()));
            if let Some(old) = table.get_mut(hash, store, key) {
                let replacement = self.values.allocate(value)?;
                self.values.release(std::mem::replace(old, replacement));
            } else {
                let value = self.values.allocate(value)?;
                table.insert(hash, store, key, value, &self.hashed_stores);
            }
            return Ok(());
        }
        let key_len = key.len();
        let table = self.tables.entry(key_len).or_default();
        if let Some(entry) = table.find_mut(hash, |e| self.keys.get(e.key, key_len) == (store, key))
        {
            let replacement = self.values.allocate(value)?;
            let old = std::mem::replace(&mut entry.value, replacement);
            self.values.release(old);
        } else {
            let key = self.keys.put(store, key)?;
            let value = self.values.allocate(value)?;
            table.insert_unique(hash, Entry { hash, key, value }, |e| e.hash);
        }
        Ok(())
    }
    pub fn get(&self, store: u16, key: &[u8]) -> Option<&[u8]> {
        if self.key_storage == KeyStorage::Inline && key.len() <= 64 {
            return self
                .inline_tables
                .get(&key.len())?
                .get(self.hash_key(store, key), store, key)
                .map(|v| self.values.get(v));
        }
        self.tables
            .get(&key.len())?
            .find(self.hash_key(store, key), |e| {
                self.keys.get(e.key, key.len()) == (store, key)
            })
            .map(|e| self.values.get(&e.value))
    }
    #[allow(dead_code)]
    pub fn remove(&mut self, store: u16, key: &[u8]) -> bool {
        let hash = self.hash_key(store, key);
        if self.key_storage == KeyStorage::Inline && key.len() <= 64 {
            if let Some(value) = self
                .inline_tables
                .get_mut(&key.len())
                .and_then(|t| t.remove(hash, store, key))
            {
                self.values.release(value);
                return true;
            }
            return false;
        }
        let Some(table) = self.tables.get_mut(&key.len()) else {
            return false;
        };
        match table.find_entry(hash, |e| self.keys.get(e.key, key.len()) == (store, key)) {
            Ok(e) => {
                let (entry, _) = e.remove();
                self.values.release(entry.value);
                true
            }
            Err(_) => false,
        }
    }
    pub fn len(&self) -> usize {
        self.tables.values().map(HashTable::len).sum::<usize>()
            + self
                .inline_tables
                .values()
                .map(|t| t.stats().entries)
                .sum::<usize>()
    }
    fn visit(&self, f: &mut impl FnMut(u16, &[u8], &ValueRef)) {
        for (len, table) in &self.tables {
            for e in table {
                let (store, key) = self.keys.get(e.key, *len);
                f(store, key, &e.value);
            }
        }
        for table in self.inline_tables.values() {
            table.visit(f);
        }
    }
    pub fn validate(&self) -> u64 {
        let mut sum = 0u64;
        self.visit(&mut |_, key, value| {
            sum = key
                .iter()
                .chain(self.values.get(value))
                .fold(sum, |a, b| a.wrapping_add(*b as u64));
        });
        sum
    }
    pub fn stats(&self) -> MapStats {
        let mut values = self.values.stats();
        #[cfg(all(target_os = "linux", target_env = "gnu"))]
        {
            // These example binaries use Rust's default System allocator.
            // Report usable payload capacity separately: it excludes some
            // allocator chunk headers and arena fragmentation, unlike RSS.
            let mut usable = 0u64;
            self.visit(&mut |_, _, value| {
                if value.len > 12 {
                    let external = value.external();
                    if external.index == -1 {
                        // SAFETY: live Box<[u8]> from the default System allocator.
                        usable +=
                            unsafe { libc::malloc_usable_size(external.pointer.cast()) } as u64;
                    }
                }
            });
            values.standalone_usable_bytes = Some(usable);
        }
        let mut key_size_maps: Vec<_> = self
            .tables
            .iter()
            .map(|(len, t)| KeySizeStats {
                key_bytes: *len,
                entries: t.len(),
                capacity: t.capacity(),
                allocation_bytes: t.allocation_size(),
                entry_struct_bytes: size_of::<Entry>(),
                storage: KeyStorage::Arena,
            })
            .chain(self.inline_tables.values().map(InlineTable::stats))
            .collect();
        key_size_maps.sort_by_key(|s| s.key_bytes);
        MapStats {
            entries: self.len(),
            key_storage: self.key_storage,
            entry_struct_bytes: if self.key_storage == KeyStorage::Arena {
                Some(size_of::<Entry>())
            } else {
                None
            },
            value_ref_bytes: size_of::<ValueRef>(),
            hash_capacity: key_size_maps.iter().map(|s| s.capacity).sum(),
            hash_allocation_bytes: key_size_maps.iter().map(|s| s.allocation_bytes).sum(),
            stored_full_hash_bytes: self.tables.values().map(|t| t.len() as u64 * 8).sum(),
            inline_key_bytes: self
                .inline_tables
                .values()
                .map(|t| {
                    let s = t.stats();
                    (s.entries * s.key_bytes) as u64
                })
                .sum(),
            keys_used_bytes: self.keys.used_bytes,
            keys_reserved_bytes: self.keys.chunks.iter().map(|c| c.capacity() as u64).sum(),
            key_chunk_count: self.keys.chunks.len(),
            key_metadata_bytes: (self.keys.chunks.capacity() * size_of::<Vec<u8>>()) as u64,
            key_size_maps,
            values,
        }
    }
}

impl Drop for Map {
    fn drop(&mut self) {
        for table in self.tables.values() {
            for entry in table.iter() {
                self.values.release(entry.value);
            }
        }
        for table in self.inline_tables.values() {
            let values = &mut self.values;
            table.visit(&mut |_, _, value| values.release(*value));
        }
    }
}
#[derive(Serialize)]
pub struct MapStats {
    pub entries: usize,
    pub entry_struct_bytes: Option<usize>,
    pub key_storage: KeyStorage,
    pub stored_full_hash_bytes: u64,
    pub inline_key_bytes: u64,
    pub value_ref_bytes: usize,
    pub hash_capacity: usize,
    pub hash_allocation_bytes: usize,
    pub keys_used_bytes: u64,
    pub keys_reserved_bytes: u64,
    pub key_chunk_count: usize,
    pub key_metadata_bytes: u64,
    pub key_size_maps: Vec<KeySizeStats>,
    pub values: ValueStats,
}
#[derive(Serialize)]
pub struct KeySizeStats {
    pub entry_struct_bytes: usize,
    pub storage: KeyStorage,
    pub key_bytes: usize,
    pub entries: usize,
    pub capacity: usize,
    pub allocation_bytes: usize,
}
#[derive(Serialize)]
pub struct ValueStats {
    pub allocator: ValueAllocator,
    pub pool_step_bytes: usize,
    pub standalone_count: u64,
    pub standalone_requested_bytes: u64,
    pub standalone_usable_bytes: Option<u64>,
    pub raw_bytes: u64,
    pub inline_count: u64,
    pub inline_bytes: u64,
    pub large_bytes: u64,
    pub large_count: u64,
    pub pool_metadata_bytes: u64,
    pub classes: Vec<ClassStats>,
}
#[derive(Serialize)]
pub struct ClassStats {
    pub slot_bytes: usize,
    pub pools: usize,
    pub live_cells: u64,
    pub reserved_bytes: u64,
    pub touched_page_span_bytes: u64,
}
pub fn page_up(bytes: u64) -> u64 {
    bytes.div_ceil(4096) * 4096
}

/// hashbrown 0.15.5, x86_64/SSE2 control groups, Entry has 32 bytes.
/// Validated against the public allocation_size() in tests; avoids allocating
/// the projected full-network hash table merely to measure its layout.
#[cfg(test)]
pub fn projected_hash_bytes(entries: u64) -> u64 {
    projected_hash_bytes_for_size(entries, size_of::<Entry>())
}
pub fn projected_key_table_bytes(entries: u64, key_len: usize, storage: KeyStorage) -> u64 {
    let size = if storage == KeyStorage::Inline {
        InlineTable::entry_size(key_len).unwrap_or(size_of::<Entry>())
    } else {
        size_of::<Entry>()
    };
    projected_hash_bytes_for_size(entries, size)
}
fn projected_hash_bytes_for_size(entries: u64, entry_size: usize) -> u64 {
    if entries == 0 {
        return 0;
    }
    let buckets = if entries < 4 {
        4
    } else if entries < 8 {
        8
    } else {
        (entries * 8 / 7).next_power_of_two()
    };
    buckets * (entry_size as u64 + 1) + 16
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn value_boundaries_and_namespace_identity() {
        let mut m = Map::new(4, 4096, 128);
        for n in [
            0usize, 1, 7, 8, 9, 11, 12, 13, 15, 16, 17, 31, 32, 33, 255, 4095, 4096, 4097,
        ] {
            let value = vec![n as u8; n];
            m.put(1, &n.to_le_bytes(), &value).unwrap();
            assert_eq!(m.get(1, &n.to_le_bytes()), Some(value.as_slice()));
        }
        m.put(2, b"same", b"a").unwrap();
        m.put(3, b"same", b"b").unwrap();
        assert_eq!(m.get(2, b"same"), Some(&b"a"[..]));
        assert_eq!(m.get(3, b"same"), Some(&b"b"[..]));
        assert_eq!(size_of::<ValueRef>(), 16);
        assert_eq!(size_of::<Entry>(), 32);
    }
    #[test]
    fn linear_pool_classes_round_by_sixteen_and_stop_at_256() {
        let mut m = Map::new(2, 256, 128);
        m.set_pool_step(16);
        let cases = [
            (12usize, 0usize),
            (13, 16),
            (16, 16),
            (17, 32),
            (32, 32),
            (33, 48),
            (47, 48),
            (48, 48),
            (49, 64),
            (192, 192),
            (193, 208),
            (240, 240),
            (241, 256),
            (255, 256),
            (256, 256),
            (257, 0),
            (4096, 0),
        ];
        for (n, slot) in cases {
            let key = (n as u64).to_le_bytes();
            let value = vec![n as u8; n];
            m.put(0, &key, &value).unwrap();
            assert_eq!(m.get(0, &key), Some(value.as_slice()));
            if n > 12 {
                let entry = m.tables[&8]
                    .find(m.hash_key(0, &key), |e| {
                        m.keys.get(e.key, 8) == (0, key.as_slice())
                    })
                    .unwrap();
                let external = entry.value.external();
                if n > 256 {
                    let index = external.index;
                    assert_eq!(index, -1);
                } else {
                    // SAFETY: this is the live pool reference issued by allocate.
                    let pool = unsafe { &*(external.pointer as *const Pool) };
                    assert_eq!(pool.stride, slot);
                }
            }
        }
        let stats = m.stats();
        assert_eq!(stats.values.pool_step_bytes, 16);
        assert_eq!(stats.values.standalone_count, 2);
        assert!(stats
            .values
            .classes
            .iter()
            .all(|c| c.slot_bytes % 16 == 0 && c.slot_bytes <= 256));
        assert!(stats.values.classes.iter().any(|c| c.slot_bytes == 48));
        for (n, _) in cases {
            assert!(m.remove(0, &(n as u64).to_le_bytes()));
        }
        assert!(m.values.blocks.iter().all(Option::is_none));
        assert_eq!(m.values.standalone_count, 0);
        assert_eq!(m.values.raw_bytes, 0);
        assert_eq!(size_of::<ValueRef>(), 16);
    }
    #[test]
    fn system_allocator_keeps_inline_and_allocates_every_larger_value() {
        let mut m = Map::new(2, 4096, 128);
        m.set_value_allocator(ValueAllocator::System);
        let lengths = [0usize, 8, 12, 13, 16, 32, 186, 373, 4096, 4097];
        let mut external_bytes = 0u64;
        for n in lengths {
            let key = (n as u64).to_le_bytes();
            let value = vec![0xab; n];
            m.put(0, &key, &value).unwrap();
            assert_eq!(m.get(0, &key), Some(value.as_slice()));
            if n > 12 {
                external_bytes += n as u64;
                let entry = m.tables[&8]
                    .find(m.hash_key(0, &key), |e| {
                        m.keys.get(e.key, 8) == (0, key.as_slice())
                    })
                    .unwrap();
                let external = entry.value.external();
                let index = external.index;
                assert_eq!(index, -1);
            }
        }
        assert!(m.values.blocks.is_empty());
        let stats = m.stats();
        assert_eq!(stats.values.inline_count, 3);
        assert_eq!(stats.values.standalone_count, 7);
        assert_eq!(stats.values.standalone_requested_bytes, external_bytes);
        assert!(stats.values.classes.is_empty());
        if let Some(usable) = stats.values.standalone_usable_bytes {
            assert!(usable >= external_bytes);
        }
        // Replacements cross the inline/ordinary allocation boundary both ways.
        m.put(0, &13u64.to_le_bytes(), &[9; 12]).unwrap();
        m.put(0, &8u64.to_le_bytes(), &[7; 20]).unwrap();
        for n in lengths {
            assert!(m.remove(0, &(n as u64).to_le_bytes()));
        }
        assert_eq!(m.values.standalone_count, 0);
        assert_eq!(m.values.standalone_bytes, 0);
        assert_eq!(m.values.inline_count, 0);
        assert_eq!(m.values.large_count, 0);
        assert_eq!(m.values.raw_bytes, 0);
        assert_eq!(size_of::<ValueRef>(), 16);
    }
    #[test]
    fn inline_twelve_bytes_need_no_pool_and_keep_arbitrary_bits() {
        let mut m = Map::new(2, 32, 128);
        for n in 0..=12usize {
            let bytes = vec![0xff; n];
            m.put(0, &[n as u8], &bytes).unwrap();
            assert_eq!(m.get(0, &[n as u8]), Some(bytes.as_slice()));
        }
        assert_eq!(size_of::<ExternalRef>(), 12);
        assert_eq!(size_of::<ValueRef>(), 16);
        assert_eq!(m.values.inline_count, 13);
        assert_eq!(m.values.inline_bytes, 78);
        assert!(m.values.blocks.is_empty());
        m.put(0, &[12], &[7; 13]).unwrap();
        assert_eq!(m.values.inline_count, 12);
        assert_eq!(m.values.blocks.iter().flatten().count(), 1);
        m.put(0, &[12], &[0xff; 12]).unwrap();
        assert!(m.values.blocks.iter().all(Option::is_none));
        assert_eq!(m.get(0, &[12]), Some([0xff; 12].as_slice()));
        for n in 0..=12u8 {
            assert!(m.remove(0, &[n]));
        }
        assert_eq!(m.values.raw_bytes, 0);
        assert_eq!(m.values.inline_count, 0);
        assert_eq!(m.values.inline_bytes, 0);
    }
    #[test]
    fn full_available_lists_and_intrusive_free_slot_reuse() {
        let mut m = Map::new(2, 32, 128);
        m.put(0, b"a", &[1; 16]).unwrap();
        m.put(0, b"b", &[2; 16]).unwrap();
        assert!(m.values.classes[0].available.is_none());
        assert_eq!(m.values.classes[0].full, Some(0));
        let old = m.tables[&1]
            .find(m.hash_key(0, b"a"), |e| m.keys.get(e.key, 1).1 == b"a")
            .unwrap()
            .value
            .external();
        m.put(0, b"c", &[3; 16]).unwrap();
        assert!(m.remove(0, b"a"));
        assert_eq!(m.values.classes[0].available, Some(0));
        m.put(0, b"d", &[4; 16]).unwrap();
        let new = m.tables[&1]
            .find(m.hash_key(0, b"d"), |e| m.keys.get(e.key, 1).1 == b"d")
            .unwrap()
            .value
            .external();
        assert_eq!((old.pointer, old.index), (new.pointer, new.index));
        assert_eq!(m.values.blocks.len(), 2);
        assert_eq!(m.get(0, b"b"), Some([2; 16].as_slice()));
        assert_eq!(m.get(0, b"c"), Some([3; 16].as_slice()));
    }
    #[test]
    fn empty_pools_are_freed_and_ids_reused_without_moving_live_pools() {
        let mut m = Map::new(2, 32, 128);
        for key in [b"a", b"b", b"c", b"d", b"e"] {
            m.put(0, key, &[1; 16]).unwrap();
        }
        m.put(0, b"other-class", &[2; 17]).unwrap();
        let ids = m.values.blocks.len();
        let free_capacity = m.values.free_blocks.capacity();
        // Empty a full pool, then the partial head and a pool between live neighbors.
        for key in [b"a", b"b", b"e", b"c", b"d"] {
            assert!(m.remove(0, key));
        }
        assert!(m.values.classes[0].full.is_none());
        assert!(m.values.classes[0].available.is_none());
        assert_eq!(m.values.blocks.iter().flatten().count(), 1);
        assert_eq!(m.get(0, b"other-class"), Some([2; 17].as_slice()));
        assert!(m.values.stats().classes.iter().all(|c| c.slot_bytes != 16));
        for key in [b"f", b"g", b"h", b"i", b"j"] {
            m.put(0, key, &[3; 16]).unwrap();
            assert_eq!(m.get(0, key), Some([3; 16].as_slice()));
        }
        assert_eq!(m.values.blocks.len(), ids);
        assert_eq!(m.values.free_blocks.capacity(), free_capacity);
        for key in [b"f", b"g", b"h", b"i", b"j"] {
            assert!(m.remove(0, key));
        }
        assert!(m.remove(0, b"other-class"));
        assert!(m.values.blocks.iter().all(Option::is_none));
        assert!(m.values.stats().classes.is_empty());
        assert_eq!(m.values.free_blocks.len(), ids);
        assert_eq!(m.values.raw_bytes, 0);
    }
    #[test]
    fn replacement_across_classes_and_standalone_allocations() {
        let mut m = Map::new(2, 32, 128);
        for value in [
            vec![1; 8],
            vec![2; 13],
            vec![3; 17],
            vec![4; 100],
            vec![],
            vec![5; 16],
        ] {
            m.put(0, b"x", &value).unwrap();
            assert_eq!(m.get(0, b"x"), Some(value.as_slice()));
            assert_eq!(m.len(), 1);
            assert_eq!(m.values.raw_bytes, value.len() as u64);
        }
        assert!(m.remove(0, b"x"));
        assert!(!m.remove(0, b"x"));
        assert_eq!(m.values.raw_bytes, 0);
        assert_eq!(m.values.large_count, 0);
    }
    #[test]
    fn hash_collisions_compare_real_keys() {
        let mut m = Map::new(2, 16, 128);
        m.put_hashed(42, 0, b"a", b"first").unwrap();
        m.put_hashed(42, 0, b"b", b"second").unwrap();
        assert_eq!(m.len(), 2);
        for (key, value) in [(b"a".as_slice(), b"first".as_slice()), (b"b", b"second")] {
            let e = m.tables[&1]
                .find(42, |e| m.keys.get(e.key, 1) == (0, key))
                .unwrap();
            assert_eq!(m.values.get(&e.value), value);
        }
    }
    #[test]
    fn projected_table_layout_matches_actual_allocations() {
        for n in [0, 1, 2, 3, 4, 7, 8, 14, 15, 28, 29, 100, 1000, 10000] {
            let table = HashTable::<Entry>::with_capacity(n);
            assert_eq!(
                table.allocation_size() as u64,
                projected_hash_bytes(n as u64),
                "{n}"
            );
        }
    }
    #[test]
    fn key_lengths_have_separate_maps_and_no_per_key_length_headers() {
        let mut m = Map::new(2, 32, 128);
        let mut expected = 0;
        for len in [0usize, 1, 2, 21, 28, 32, 48, 300] {
            let key = vec![7; len];
            m.put(1, &key, b"first").unwrap();
            m.put(2, &key, b"second").unwrap();
            assert_eq!(m.get(1, &key), Some(b"first".as_slice()));
            assert_eq!(m.get(2, &key), Some(b"second".as_slice()));
            expected += 2 * len;
        }
        assert_eq!(m.tables.len(), 8);
        assert_eq!(m.len(), 16);
        assert_eq!(m.keys.used_bytes, expected as u64);
        assert_eq!(size_of::<KeyRef>(), 8);
        assert_eq!(size_of::<Entry>(), 32);
    }
    #[test]
    fn prehashed_tail_collisions_keep_distinct_full_keys() {
        let mut m = Map::new(2, 16, 128);
        m.mark_prehashed_store(0);
        let mut a = [0u8; 32];
        let mut b = [0u8; 32];
        a[0] = 1;
        b[0] = 2;
        m.put(0, &a, b"first").unwrap();
        m.put(0, &b, b"second").unwrap();
        assert_eq!(m.get(0, &a), Some(&b"first"[..]));
        assert_eq!(m.get(0, &b), Some(&b"second"[..]));
        assert_eq!(m.len(), 2);
    }
}

#[cfg(test)]
mod inline_tests {
    use super::*;
    #[test]
    fn inline_widths_namespace_and_value_lifetimes() {
        let mut m = Map::new(3, 256, 128);
        m.set_key_storage(KeyStorage::Inline);
        m.set_pool_step(16);
        for len in [0usize, 1, 8, 21, 28, 32, 48, 61, 64, 65, 300] {
            let key = vec![0xff; len];
            for store in 0..2 {
                for n in [0usize, 12, 13, 17, 33, 256, 257, 900] {
                    let value = vec![n as u8; n];
                    m.put(store, &key, &value).unwrap();
                    assert_eq!(m.get(store, &key), Some(value.as_slice()));
                }
                m.put(store, &key, &[9; 12]).unwrap();
            }
        }
        let stats = m.stats();
        assert_eq!(m.len(), 22);
        assert_eq!(
            stats.inline_key_bytes,
            2 * (1 + 8 + 21 + 28 + 32 + 48 + 61 + 64)
        );
        assert_eq!(stats.keys_used_bytes, 2 * (65 + 300));
        assert_eq!(stats.stored_full_hash_bytes, 4 * 8); // Only oversized-key fallback.
        for len in [0usize, 1, 8, 21, 28, 32, 48, 61, 64, 65, 300] {
            for store in 0..2 {
                assert!(m.remove(store, &vec![0xff; len]));
            }
        }
        assert_eq!(m.len(), 0);
        assert_eq!(m.values.raw_bytes, 0);
        assert_eq!(m.values.standalone_count, 0);
        assert!(m.values.blocks.iter().all(Option::is_none));
    }
    #[test]
    fn inline_collisions_survive_growth_and_deletion() {
        let mut m = Map::new(4, 256, 128);
        m.set_key_storage(KeyStorage::Inline);
        m.mark_prehashed_store(0);
        let key = |n: u64| {
            let mut k = [0u8; 32];
            k[..8].copy_from_slice(&n.to_le_bytes());
            // Shared tail is deliberate: distinct full keys have identical lookup hashes.
            k
        };
        for n in 0..200u64 {
            m.put(0, &key(n), &n.to_le_bytes()).unwrap();
        }
        for n in (0..200u64).step_by(2) {
            assert!(m.remove(0, &key(n)));
        }
        for n in 200..400u64 {
            m.put(0, &key(n), &n.to_le_bytes()).unwrap();
        }
        for n in 0..400u64 {
            if n < 200 && n % 2 == 0 {
                assert!(m.get(0, &key(n)).is_none());
            } else {
                assert_eq!(m.get(0, &key(n)), Some(n.to_le_bytes().as_slice()));
            }
        }
        assert_eq!(m.stats().stored_full_hash_bytes, 0);
        assert_eq!(m.stats().keys_reserved_bytes, 0);
        let expected: u64 = (0..400u64)
            .filter(|n| *n >= 200 || n % 2 == 1)
            .map(|n| {
                key(n)
                    .iter()
                    .chain(n.to_le_bytes().iter())
                    .map(|b| *b as u64)
                    .sum::<u64>()
            })
            .sum();
        assert_eq!(m.validate(), expected);
    }
    #[test]
    fn inline_actual_entry_widths_and_projected_allocations_match() {
        assert_eq!(InlineTable::entry_size(21), Some(40));
        assert_eq!(InlineTable::entry_size(28), Some(48));
        assert_eq!(InlineTable::entry_size(32), Some(52));
        for len in 0..=64 {
            let mut m = Map::new(4, 256, 128);
            m.set_key_storage(KeyStorage::Inline);
            let key = vec![0; len];
            m.put(0, &key, b"x").unwrap();
            assert_eq!(
                m.stats().hash_allocation_bytes as u64,
                projected_key_table_bytes(1, len, KeyStorage::Inline)
            );
        }
        for count in [2u16, 3, 4, 7, 8, 14, 15, 28, 29, 100, 1000] {
            let mut m = Map::new(4, 256, 128);
            m.set_key_storage(KeyStorage::Inline);
            for store in 0..count {
                m.put(store, &[0; 21], b"x").unwrap();
            }
            assert_eq!(
                m.stats().hash_allocation_bytes as u64,
                projected_key_table_bytes(count as u64, 21, KeyStorage::Inline)
            );
        }
    }
}
