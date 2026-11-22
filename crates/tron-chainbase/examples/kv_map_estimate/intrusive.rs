//! Standalone single-threaded intrusive KV mirror experiment.
//! Buckets own only pointers. Node bytes are [next:ptr][value_len:u32][key][value].
//! Unaligned header access lets each size class use its exact byte stride.
#[path = "buckets.rs"]
mod buckets;
use crate::model::key_hash;
use buckets::BucketArray;
use hashbrown::HashMap;
use serde::Serialize;
use std::{
    collections::BTreeMap,
    hash::{BuildHasherDefault, Hasher},
    mem::size_of,
    ptr,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
type Node = *mut u8;
pub const HEADER: usize = size_of::<Node>() + size_of::<u32>();
#[derive(Default)]
struct SizeHasher(u64);
impl Hasher for SizeHasher {
    fn write(&mut self, bytes: &[u8]) {
        let mut h = 0u64;
        for b in bytes {
            h = h.wrapping_mul(257).wrapping_add(*b as u64);
        }
        self.write_u64(h);
    }
    fn write_usize(&mut self, n: usize) {
        self.write_u64(n as u64);
    }
    fn write_u64(&mut self, n: u64) {
        let x = n.wrapping_mul(0x9e3779b97f4a7c15);
        self.0 = x ^ (x >> 32);
    }
    fn finish(&self) -> u64 {
        self.0
    }
}
// Each helper's caller must hold an exclusive map borrow for mutation. Nodes
// belong to live pool blocks or live standalone Boxes; bytes are initialized
// before linking. Keys and values are borrowed only through &Map.
unsafe fn next(node: Node) -> Node {
    // SAFETY: caller guarantees at least HEADER live bytes; alignment is arbitrary.
    unsafe { node.cast::<Node>().read_unaligned() }
}
unsafe fn set_next(node: Node, link: Node) {
    // SAFETY: caller owns the live/free cell exclusively.
    unsafe {
        node.cast::<Node>().write_unaligned(link);
    }
}
unsafe fn value_len(node: Node) -> usize {
    // SAFETY: initialized u32 immediately follows the pointer, possibly unaligned.
    unsafe { node.add(size_of::<Node>()).cast::<u32>().read_unaligned() as usize }
}
unsafe fn set_len(node: Node, len: u32) {
    // SAFETY: initialized live allocation has at least HEADER writable bytes.
    unsafe {
        node.add(size_of::<Node>())
            .cast::<u32>()
            .write_unaligned(len);
    }
}
unsafe fn key_bytes<'a>(node: Node, key_len: usize) -> &'a [u8] {
    // SAFETY: caller bounds lifetime by map borrow and supplies this table's width.
    unsafe { std::slice::from_raw_parts(node.add(HEADER), key_len) }
}
unsafe fn value_bytes<'a>(node: Node, key_len: usize) -> &'a [u8] {
    // SAFETY: header length was checked on insertion; allocation covers payload.
    unsafe { std::slice::from_raw_parts(node.add(HEADER + key_len), value_len(node)) }
}
pub fn value_capacity(len: usize) -> usize {
    if len <= 16 {
        len
    } else {
        len.div_ceil(16) * 16
    }
}
pub fn slot_size(key_len: usize, value_len: usize) -> usize {
    HEADER + key_len + value_capacity(value_len)
}
#[derive(Default)]
struct Class {
    head: Node,
    // Box payload addresses remain stable when the registry or Vec reallocates.
    blocks: Vec<Box<[u8]>>,
    cells: u64,
    live: u64,
    reuses: u64,
    released: u64,
}
impl Class {
    fn add_block(&mut self, stride: usize, target_bytes: usize) {
        let cells = (target_bytes / stride).max(1);
        let mut block = vec![0u8; cells * stride].into_boxed_slice();
        for i in (0..cells).rev() {
            let node = unsafe { block.as_mut_ptr().add(i * stride) };
            // SAFETY: each new cell is disjoint, at least HEADER bytes, and owned.
            unsafe {
                set_next(node, self.head);
            }
            self.head = node;
        }
        self.cells += cells as u64;
        self.blocks.push(block);
    }
    fn allocate(&mut self, stride: usize, target_bytes: usize) -> Node {
        if self.head.is_null() {
            self.add_block(stride, target_bytes);
        }
        let node = self.head;
        // SAFETY: head is an initialized free cell in a retained block.
        self.head = unsafe { next(node) };
        self.live += 1;
        if self.released > self.reuses {
            self.reuses += 1;
        }
        node
    }
    unsafe fn release(&mut self, node: Node) {
        // SAFETY: caller has unlinked this unique live cell of the same size class.
        unsafe {
            set_next(node, self.head);
        }
        self.head = node;
        self.live -= 1;
        self.released += 1;
    }
}
struct Pools {
    classes: HashMap<usize, Class, BuildHasherDefault<SizeHasher>>,
    cutoff: usize,
    block_bytes: usize,
    standalone_count: u64,
    standalone_bytes: u64,
    standalone_usable: u64,
    standalone_allocations: u64,
    pooled_payload: u64,
    pooled_padding: u64,
}
impl Pools {
    fn new(cutoff: usize, block_bytes: usize) -> Self {
        Self {
            classes: HashMap::default(),
            cutoff,
            block_bytes,
            standalone_count: 0,
            standalone_bytes: 0,
            standalone_usable: 0,
            standalone_allocations: 0,
            pooled_payload: 0,
            pooled_padding: 0,
        }
    }
    fn allocate(&mut self, key: &[u8], value: &[u8]) -> Result<Node> {
        let len = u32::try_from(value.len()).map_err(|_| "value exceeds u32")?;
        let raw = HEADER
            .checked_add(key.len())
            .and_then(|n| n.checked_add(value.len()))
            .ok_or("node length overflow")?;
        let node = if value.len() > self.cutoff {
            let boxed = vec![0u8; raw].into_boxed_slice();
            self.standalone_count += 1;
            self.standalone_bytes += raw as u64;
            self.standalone_allocations += 1;
            let node = Box::into_raw(boxed) as Node;
            #[cfg(all(target_os = "linux", target_env = "gnu"))]
            // SAFETY: exact live Box allocation, queried before linking.
            unsafe {
                self.standalone_usable += libc::malloc_usable_size(node.cast()) as u64;
            }
            node
        } else {
            let stride = slot_size(key.len(), value.len());
            self.pooled_payload += raw as u64;
            self.pooled_padding += (stride - raw) as u64;
            self.classes
                .entry(stride)
                .or_default()
                .allocate(stride, self.block_bytes)
        };
        // SAFETY: allocation covers header, original key and actual value bytes.
        unsafe {
            set_next(node, ptr::null_mut());
            set_len(node, len);
            ptr::copy_nonoverlapping(key.as_ptr(), node.add(HEADER), key.len());
            ptr::copy_nonoverlapping(value.as_ptr(), node.add(HEADER + key.len()), value.len());
        }
        Ok(node)
    }
    unsafe fn release(&mut self, node: Node, key_len: usize) {
        // SAFETY: caller unlinked a unique node whose namespace/width are known.
        let len = unsafe { value_len(node) };
        let raw = HEADER + key_len + len;
        if len > self.cutoff {
            self.standalone_count -= 1;
            self.standalone_bytes -= raw as u64;
            #[cfg(all(target_os = "linux", target_env = "gnu"))]
            // SAFETY: unique live standalone allocation, queried before free.
            unsafe {
                self.standalone_usable -= libc::malloc_usable_size(node.cast()) as u64;
            }
            // SAFETY: allocation was an exact Box<[u8]> with this total length.
            unsafe {
                drop(Box::from_raw(ptr::slice_from_raw_parts_mut(node, raw)));
            }
        } else {
            let stride = slot_size(key_len, len);
            self.pooled_payload -= raw as u64;
            self.pooled_padding -= (stride - raw) as u64;
            // SAFETY: geometry uniquely identifies this cell's owning size class.
            unsafe {
                self.classes.get_mut(&stride).unwrap().release(node);
            }
        }
    }
}

struct Table {
    buckets: BucketArray,
    len: usize,
}
impl Table {
    fn new() -> Self {
        Self {
            buckets: BucketArray::empty(),
            len: 0,
        }
    }
    fn grow(&mut self, store: u16, key_len: usize, hashed: &[bool], locked: bool) -> Result<()> {
        let count = (self.buckets.len() * 2).max(4);
        let mut buckets = BucketArray::new(count, locked)?;
        for head in &self.buckets {
            let mut node = *head;
            while !node.is_null() {
                // SAFETY: each old chain contains live disjoint nodes of this width.
                unsafe {
                    let old_next = next(node);
                    let slot =
                        key_hash(hashed, store, key_bytes(node, key_len)) as usize & (count - 1);
                    set_next(node, buckets[slot]);
                    buckets[slot] = node;
                    node = old_next;
                }
            }
        }
        self.buckets = buckets; // Node addresses stay unchanged; old array unlocks on drop.
        Ok(())
    }
    fn find(&self, hash: u64, key: &[u8]) -> (Node, Node) {
        if self.buckets.is_empty() {
            return (ptr::null_mut(), ptr::null_mut());
        }
        let mut node = self.buckets[hash as usize & (self.buckets.len() - 1)];
        let mut previous = ptr::null_mut();
        while !node.is_null() {
            // SAFETY: linked nodes have the table's fixed width and initialized headers.
            unsafe {
                if key_bytes(node, key.len()) == key {
                    return (node, previous);
                }
                previous = node;
                node = next(node);
            }
        }
        (ptr::null_mut(), previous)
    }
    unsafe fn replace_link(&mut self, hash: u64, previous: Node, node: Node) {
        if previous.is_null() {
            let slot = hash as usize & (self.buckets.len() - 1);
            self.buckets[slot] = node;
        } else {
            // SAFETY: caller supplies a live predecessor in this same bucket chain.
            unsafe {
                set_next(previous, node);
            }
        }
    }
}
pub struct Map {
    // Namespace and key width are table metadata, absent from each node.
    tables: BTreeMap<(u16, usize), Table>,
    pools: Pools,
    lock_buckets: bool,
    hashed_stores: Vec<bool>,
    len: usize,
}
impl Map {
    pub fn new(cutoff: usize, block_bytes: usize) -> Self {
        assert!(block_bytes >= HEADER && cutoff <= u32::MAX as usize);
        Self {
            tables: BTreeMap::new(),
            pools: Pools::new(cutoff, block_bytes),
            lock_buckets: false,
            hashed_stores: Vec::new(),
            len: 0,
        }
    }
    pub fn set_lock_buckets(&mut self, locked: bool) {
        assert!(
            self.tables.is_empty(),
            "choose bucket locking before loading"
        );
        self.lock_buckets = locked;
    }
    pub fn mark_prehashed_store(&mut self, store: u16) {
        assert_eq!(self.len, 0, "hash policy is fixed before loading");
        self.hashed_stores
            .resize(self.hashed_stores.len().max(store as usize + 1), false);
        self.hashed_stores[store as usize] = true;
    }
    pub fn put(&mut self, store: u16, key: &[u8], value: &[u8]) -> Result<()> {
        let hash = key_hash(&self.hashed_stores, store, key);
        let table = self
            .tables
            .entry((store, key.len()))
            .or_insert_with(Table::new);
        let (old, previous) = table.find(hash, key);
        if old.is_null() && table.len == table.buckets.len() {
            table.grow(store, key.len(), &self.hashed_stores, self.lock_buckets)?;
        }
        let replacement = self.pools.allocate(key, value)?;
        // SAFETY: replacement is exclusive and unlinked. Existing nodes stay live
        // through link replacement. No user references survive this &mut Map borrow.
        unsafe {
            if !old.is_null() {
                set_next(replacement, next(old));
                table.replace_link(hash, previous, replacement);
                self.pools.release(old, key.len());
            } else {
                let slot = hash as usize & (table.buckets.len() - 1);
                set_next(replacement, table.buckets[slot]);
                table.buckets[slot] = replacement;
                table.len += 1;
                self.len += 1;
            }
        }
        Ok(())
    }
    pub fn frozen(&self) -> Frozen<'_> {
        Frozen { map: self }
    }
    pub fn get(&self, store: u16, key: &[u8]) -> Option<&[u8]> {
        let hash = key_hash(&self.hashed_stores, store, key);
        let (node, _) = self.tables.get(&(store, key.len()))?.find(hash, key);
        if node.is_null() {
            None
        } else {
            // SAFETY: map borrow keeps this live node and its storage immutable.
            Some(unsafe { value_bytes(node, key.len()) })
        }
    }
    #[allow(dead_code)]
    pub fn remove(&mut self, store: u16, key: &[u8]) -> bool {
        let hash = key_hash(&self.hashed_stores, store, key);
        let Some(table) = self.tables.get_mut(&(store, key.len())) else {
            return false;
        };
        let (node, previous) = table.find(hash, key);
        if node.is_null() {
            return false;
        }
        // SAFETY: node is linked exactly once, and this mutable borrow excludes readers.
        unsafe {
            table.replace_link(hash, previous, next(node));
            self.pools.release(node, key.len());
        }
        table.len -= 1;
        self.len -= 1;
        true
    }
    pub fn len(&self) -> usize {
        self.len
    }
    #[allow(dead_code)]
    fn visit(&self, f: &mut impl FnMut(u16, usize, Node)) {
        for ((store, width), table) in &self.tables {
            for head in &table.buckets {
                let mut node = *head;
                while !node.is_null() {
                    f(*store, *width, node);
                    // SAFETY: traversal only visits live initialized nodes.
                    node = unsafe { next(node) };
                }
            }
        }
    }
    #[allow(dead_code)]
    pub fn validate(&self) -> u64 {
        let mut sum = 0u64;
        self.visit(&mut |_, width, node| {
            // SAFETY: visitor supplies live nodes with the table's exact key width.
            unsafe {
                sum = key_bytes(node, width)
                    .iter()
                    .chain(value_bytes(node, width))
                    .fold(sum, |a, b| a.wrapping_add(*b as u64));
            }
        });
        sum
    }
    pub fn stats(&self) -> Stats {
        let mut classes: Vec<_> = self
            .pools
            .classes
            .iter()
            .map(|(stride, c)| ClassStats {
                slot_bytes: *stride,
                blocks: c.blocks.len(),
                cells: c.cells,
                live_cells: c.live,
                free_cells: c.cells - c.live,
                reuse_count: c.reuses,
                reserved_bytes: c.blocks.iter().map(|b| b.len() as u64).sum(),
            })
            .collect();
        classes.sort_by_key(|c| c.slot_bytes);
        let tables: Vec<_> = self
            .tables
            .iter()
            .map(|((store, width), t)| TableStats {
                store: *store,
                key_bytes: *width,
                entries: t.len,
                buckets: t.buckets.len(),
                bucket_allocation_bytes: t.buckets.allocation_bytes(),
                bucket_payload_bytes: t.buckets.len() * size_of::<Node>(),
                locked_bucket_bytes: t.buckets.locked_bytes(),
                empty_buckets: t.buckets.iter().filter(|n| n.is_null()).count(),
            })
            .collect();
        Stats {
            entries: self.len,
            header_bytes: HEADER,
            pointer_bytes: size_of::<Node>(),
            table_count: tables.len(),
            bucket_allocation_bytes: tables
                .iter()
                .map(|t| t.bucket_allocation_bytes as u64)
                .sum(),
            locked_bucket_bytes: tables.iter().map(|t| t.locked_bucket_bytes as u64).sum(),
            bucket_payload_bytes: tables.iter().map(|t| t.bucket_payload_bytes as u64).sum(),
            empty_bucket_bytes: tables
                .iter()
                .map(|t| (t.empty_buckets * size_of::<Node>()) as u64)
                .sum(),
            pool_reserved_bytes: classes.iter().map(|c| c.reserved_bytes).sum(),
            pooled_node_bytes_in_use: self.pools.pooled_payload + self.pools.pooled_padding,
            pooled_payload_bytes: self.pools.pooled_payload,
            pooled_padding_bytes: self.pools.pooled_padding,
            free_pool_cell_bytes: classes
                .iter()
                .map(|c| c.free_cells * c.slot_bytes as u64)
                .sum(),
            standalone_count: self.pools.standalone_count,
            standalone_requested_bytes: self.pools.standalone_bytes,
            standalone_usable_bytes: self.pools.standalone_usable,
            standalone_allocation_calls: self.pools.standalone_allocations,
            node_pool_block_allocation_calls: classes.iter().map(|c| c.blocks as u64).sum(),
            classes,
            tables,
        }
    }
}
/// Read-only view: its borrow prevents mutation/destruction throughout scoped reads.
/// No Sync/Send implementation is granted to the mutable experimental Map itself.
pub struct Frozen<'a> {
    map: &'a Map,
}
// SAFETY: Map::get only reads initialized immutable bytes and stable bucket/pool
// allocations. All mutations (including free/reuse/rehash) require &mut Map,
// excluded for the entire Frozen borrow. Scoped readers finish before it ends.
unsafe impl Sync for Frozen<'_> {}
impl Frozen<'_> {
    pub fn get(&self, store: u16, key: &[u8]) -> Option<&[u8]> {
        self.map.get(store, key)
    }
}
impl Drop for Map {
    fn drop(&mut self) {
        // Pooled blocks drop once with Pools. Only standalone nodes need per-node free.
        for ((_, width), table) in &self.tables {
            for head in &table.buckets {
                let mut node = *head;
                while !node.is_null() {
                    // SAFETY: capture next before a standalone allocation is freed.
                    unsafe {
                        let following = next(node);
                        let len = value_len(node);
                        if len > self.pools.cutoff {
                            drop(Box::from_raw(ptr::slice_from_raw_parts_mut(
                                node,
                                HEADER + width + len,
                            )));
                        }
                        node = following;
                    }
                }
            }
        }
    }
}
#[derive(Serialize)]
pub struct Stats {
    pub entries: usize,
    pub header_bytes: usize,
    pub pointer_bytes: usize,
    pub table_count: usize,
    pub bucket_allocation_bytes: u64,
    pub locked_bucket_bytes: u64,
    pub bucket_payload_bytes: u64,
    pub empty_bucket_bytes: u64,
    pub pool_reserved_bytes: u64,
    pub pooled_node_bytes_in_use: u64,
    pub pooled_payload_bytes: u64,
    pub pooled_padding_bytes: u64,
    pub free_pool_cell_bytes: u64,
    pub standalone_count: u64,
    pub standalone_requested_bytes: u64,
    pub standalone_usable_bytes: u64,
    pub standalone_allocation_calls: u64,
    pub node_pool_block_allocation_calls: u64,
    pub classes: Vec<ClassStats>,
    pub tables: Vec<TableStats>,
}
#[derive(Serialize)]
pub struct ClassStats {
    pub slot_bytes: usize,
    pub blocks: usize,
    pub cells: u64,
    pub live_cells: u64,
    pub free_cells: u64,
    pub reserved_bytes: u64,
    pub reuse_count: u64,
}
#[derive(Serialize)]
pub struct TableStats {
    pub store: u16,
    pub key_bytes: usize,
    pub entries: usize,
    pub buckets: usize,
    pub bucket_allocation_bytes: usize,
    pub locked_bucket_bytes: usize,
    pub bucket_payload_bytes: usize,
    pub empty_buckets: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_small_values_and_sixteen_byte_steps() {
        assert_eq!(HEADER, 12);
        for key_len in [0usize, 1, 21, 28, 32, 300] {
            for len in 0..=16 {
                assert_eq!(slot_size(key_len, len), HEADER + key_len + len);
            }
            for len in 17..=256 {
                assert_eq!(
                    slot_size(key_len, len),
                    HEADER + key_len + len.div_ceil(16) * 16
                );
            }
        }
    }
    #[test]
    fn one_allocation_contains_key_value_header_and_supports_unaligned_cells() {
        let mut map = Map::new(256, 200);
        for n in 0..100u64 {
            let key = [n.to_le_bytes().as_slice(), &[7; 13]].concat(); // 21-byte key.
            let value = vec![n as u8; 8]; // 41-byte, deliberately unaligned stride.
            map.put(0, &key, &value).unwrap();
            let (node, _) = map.tables[&(0, 21)].find(key_hash(&[], 0, &key), &key);
            unsafe {
                assert_eq!(key_bytes(node, 21).as_ptr(), node.add(12));
                assert_eq!(value_bytes(node, 21).as_ptr(), node.add(33));
            }
            assert_eq!(map.get(0, &key), Some(value.as_slice()));
        }
        assert_eq!(map.pools.classes.len(), 1);
        assert_eq!(map.stats().standalone_count, 0);
    }
    #[test]
    fn global_free_cells_cross_namespaces_and_key_widths_and_blocks_stay_allocated() {
        let mut map = Map::new(256, 164); // Four 41-byte cells per block.
        map.put(1, &[1; 21], &[2; 8]).unwrap();
        let old = map.tables[&(1, 21)]
            .find(key_hash(&[], 1, &[1; 21]), &[1; 21])
            .0;
        assert!(map.remove(1, &[1; 21]));
        let reserved = map.stats().pool_reserved_bytes;
        assert_eq!(map.stats().free_pool_cell_bytes, reserved);
        map.put(2, &[3; 20], &[4; 9]).unwrap(); // Same 41-byte class, another table.
        let new = map.tables[&(2, 20)]
            .find(key_hash(&[], 2, &[3; 20]), &[3; 20])
            .0;
        assert_eq!(old, new);
        assert_eq!(map.stats().pool_reserved_bytes, reserved);
        assert_eq!(map.stats().classes[0].reuse_count, 1);
        assert!(map.remove(2, &[3; 20]));
        assert_eq!(map.stats().pool_reserved_bytes, reserved);
        assert_eq!(map.pools.classes[&41].blocks.len(), 1);
    }
    #[test]
    fn colliding_chains_rehash_without_moving_nodes_and_erase_head_middle_tail() {
        let mut map = Map::new(256, 1024);
        map.mark_prehashed_store(0);
        let key = |n: u64| {
            let mut k = [0u8; 32];
            k[..8].copy_from_slice(&n.to_le_bytes());
            k
        };
        map.put(0, &key(0), b"first").unwrap();
        let first = map.tables[&(0, 32)].find(0, &key(0)).0;
        for n in 1..400u64 {
            map.put(0, &key(n), &n.to_le_bytes()).unwrap();
        }
        assert_eq!(map.tables[&(0, 32)].find(0, &key(0)).0, first);
        assert!(map.remove(0, &key(399)));
        assert!(map.remove(0, &key(0)));
        assert!(map.remove(0, &key(200)));
        for n in 1..399u64 {
            if n == 200 {
                assert!(map.get(0, &key(n)).is_none());
            } else {
                assert_eq!(map.get(0, &key(n)), Some(n.to_le_bytes().as_slice()));
            }
        }
        for n in 0..400u64 {
            map.remove(0, &key(n));
        }
        assert_eq!(map.len(), 0);
        assert_eq!(map.stats().pooled_node_bytes_in_use, 0);
        assert_eq!(
            map.stats().free_pool_cell_bytes,
            map.stats().pool_reserved_bytes
        );
    }
    #[test]
    fn replacements_and_normal_allocations_match_an_owned_reference_map() {
        let mut map = Map::new(256, 1024);
        let mut expected = BTreeMap::new();
        let mut seed = 42u64;
        for iteration in 0..5000 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let store = (seed >> 56) as u16 % 4;
            let key = (seed >> 16 & 127).to_le_bytes().to_vec();
            if seed % 5 == 0 {
                assert_eq!(
                    map.remove(store, &key),
                    expected.remove(&(store, key)).is_some()
                );
            } else {
                let lengths = [0usize, 1, 12, 16, 17, 33, 128, 256, 257, 4096, 640_613];
                let len = lengths[(seed as usize >> 8) % lengths.len()];
                let value = vec![(seed >> 32) as u8; len];
                map.put(store, &key, &value).unwrap();
                expected.insert((store, key), value);
            }
            if iteration % 100 == 0 {
                assert_eq!(map.len(), expected.len());
                for ((store, key), value) in &expected {
                    assert_eq!(map.get(*store, key), Some(value.as_slice()));
                }
            }
        }
        let checksum: u64 = expected
            .iter()
            .map(|((_, k), v)| k.iter().chain(v).map(|x| *x as u64).sum::<u64>())
            .sum();
        assert_eq!(map.validate(), checksum);
        #[cfg(all(target_os = "linux", target_env = "gnu"))]
        {
            let mut usable = 0u64;
            map.visit(&mut |_, width, node| unsafe {
                if value_len(node) > 256 {
                    usable += libc::malloc_usable_size(node.cast()) as u64;
                }
                let _ = width;
            });
            assert_eq!(map.stats().standalone_usable_bytes, usable);
        }
        let standalone = expected.values().filter(|v| v.len() > 256).count() as u64;
        assert_eq!(map.stats().standalone_count, standalone);
        for ((store, key), _) in expected {
            assert!(map.remove(store, &key));
        }
        let stats = map.stats();
        assert_eq!(stats.standalone_count, 0);
        assert_eq!(stats.standalone_requested_bytes, 0);
        assert_eq!(stats.standalone_usable_bytes, 0);
        assert_eq!(stats.pooled_payload_bytes, 0);
        assert_eq!(stats.pooled_padding_bytes, 0);
        assert_eq!(stats.free_pool_cell_bytes, stats.pool_reserved_bytes);
    }
}

#[cfg(test)]
mod locked_tests {
    use super::*;
    fn kernel_locked_bytes() -> u64 {
        std::fs::read_to_string("/proc/self/status")
            .unwrap()
            .lines()
            .find_map(|l| l.strip_prefix("VmLck:"))
            .unwrap()
            .split_whitespace()
            .next()
            .unwrap()
            .parse::<u64>()
            .unwrap()
            * 1024
    }
    #[test]
    fn isolated_bucket_pages_stay_locked_through_growth_and_unlock_on_drop() {
        let before = kernel_locked_bytes();
        let mut map = Map::new(256, 65536);
        map.set_lock_buckets(true);
        let key = |n: u64| {
            let mut k = [0u8; 21];
            k[..8].copy_from_slice(&n.to_le_bytes());
            k
        };
        for n in 0..1000u64 {
            map.put(0, &key(n), &[n as u8; 8]).unwrap();
        }
        for n in 0..1000u64 {
            assert_eq!(map.get(0, &key(n)), Some([n as u8; 8].as_slice()));
        }
        let stats = map.stats();
        assert!(stats.locked_bucket_bytes >= 8192);
        assert_eq!(kernel_locked_bytes() - before, stats.locked_bucket_bytes);
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
        let table = &map.tables[&(0, 21)];
        assert_eq!(table.buckets.as_ptr() as usize % page, 0);
        assert_eq!(
            table.buckets.locked_bytes(),
            table.buckets.allocation_bytes()
        );
        // Node pool payloads cannot share these dedicated virtual mappings.
        let start = table.buckets.as_ptr() as usize;
        let end = start + table.buckets.allocation_bytes();
        for class in map.pools.classes.values() {
            for block in &class.blocks {
                let a = block.as_ptr() as usize;
                let b = a + block.len();
                assert!(b <= start || a >= end);
            }
        }
        for n in 0..1000u64 {
            assert!(map.remove(0, &key(n)));
        }
        assert_eq!(map.len(), 0);
        assert_eq!(kernel_locked_bytes() - before, stats.locked_bucket_bytes);
        assert_eq!(
            map.stats().free_pool_cell_bytes,
            map.stats().pool_reserved_bytes
        );
        drop(map);
        assert_eq!(kernel_locked_bytes(), before);
    }
}

#[cfg(test)]
mod frozen_tests {
    use super::*;
    #[test]
    fn concurrent_frozen_reads_verify_original_bytes() {
        let mut map = Map::new(256, 65536);
        for n in 0..10000u64 {
            map.put(
                (n % 3) as u16,
                &n.to_le_bytes(),
                &vec![n as u8; if n % 2 == 0 { 32 } else { 300 }],
            )
            .unwrap();
        }
        let frozen = map.frozen();
        std::thread::scope(|scope| {
            for t in 0..10u64 {
                let frozen = &frozen;
                scope.spawn(move || {
                    for n in t * 1000..(t + 1) * 1000 {
                        assert_eq!(
                            frozen.get((n % 3) as u16, &n.to_le_bytes()).unwrap(),
                            vec![n as u8; if n % 2 == 0 { 32 } else { 300 }]
                        );
                    }
                });
            }
        });
    }
}
