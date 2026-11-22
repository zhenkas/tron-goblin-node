//! Fixed-width keys live beside ValueRef in hashbrown's bucket array.
//! The complete lookup hash is recomputed for rehash, never stored in an entry.
use super::{key_hash, KeySizeStats, KeyStorage, ValueRef};
use hashbrown::HashTable;
use std::mem::size_of;

#[repr(C)]
struct InlineEntry<const N: usize> {
    key: [u8; N],
    store: u16,
    value: ValueRef,
}
pub(super) struct Fixed<const N: usize>(HashTable<InlineEntry<N>>);
impl<const N: usize> Fixed<N> {
    fn new() -> Self {
        Self(HashTable::new())
    }
    fn get(&self, hash: u64, store: u16, key: &[u8]) -> Option<&ValueRef> {
        self.0
            .find(hash, |e| e.store == store && e.key == key)
            .map(|e| &e.value)
    }
    fn get_mut(&mut self, hash: u64, store: u16, key: &[u8]) -> Option<&mut ValueRef> {
        self.0
            .find_mut(hash, |e| e.store == store && e.key == key)
            .map(|e| &mut e.value)
    }
    fn insert(&mut self, hash: u64, store: u16, key: &[u8], value: ValueRef, hashed: &[bool]) {
        self.0.insert_unique(
            hash,
            InlineEntry {
                key: key.try_into().expect("table width equals key length"),
                store,
                value,
            },
            |e| key_hash(hashed, e.store, &e.key),
        );
    }
    fn remove(&mut self, hash: u64, store: u16, key: &[u8]) -> Option<ValueRef> {
        self.0
            .find_entry(hash, |e| e.store == store && e.key == key)
            .ok()
            .map(|e| e.remove().0.value)
    }
    fn visit(&self, f: &mut impl FnMut(u16, &[u8], &ValueRef)) {
        for e in &self.0 {
            f(e.store, &e.key, &e.value);
        }
    }
    fn stats(&self) -> KeySizeStats {
        KeySizeStats {
            key_bytes: N,
            entries: self.0.len(),
            capacity: self.0.capacity(),
            allocation_bytes: self.0.allocation_size(),
            entry_struct_bytes: size_of::<InlineEntry<N>>(),
            storage: KeyStorage::Inline,
        }
    }
}
macro_rules! inline_tables {
    ($(($variant:ident, $n:literal)),* $(,)?) => {
        pub(super) enum InlineTable { $($variant(Fixed<$n>)),* }
        impl InlineTable {
            pub fn new(len: usize) -> Self {
                match len { $($n => Self::$variant(Fixed::new())),*, _ => unreachable!("inline width") }
            }
            pub fn entry_size(len: usize) -> Option<usize> {
                match len { $($n => Some(size_of::<InlineEntry<$n>>())),*, _ => None }
            }
            pub fn get(&self, hash: u64, store: u16, key: &[u8]) -> Option<&ValueRef> {
                match self { $(Self::$variant(t) => t.get(hash, store, key)),* }
            }
            pub fn get_mut(&mut self, hash: u64, store: u16, key: &[u8]) -> Option<&mut ValueRef> {
                match self { $(Self::$variant(t) => t.get_mut(hash, store, key)),* }
            }
            pub fn insert(&mut self, hash: u64, store: u16, key: &[u8], value: ValueRef, hashed: &[bool]) {
                match self { $(Self::$variant(t) => t.insert(hash, store, key, value, hashed)),* }
            }
            pub fn remove(&mut self, hash: u64, store: u16, key: &[u8]) -> Option<ValueRef> {
                match self { $(Self::$variant(t) => t.remove(hash, store, key)),* }
            }
            pub fn visit(&self, f: &mut impl FnMut(u16, &[u8], &ValueRef)) {
                match self { $(Self::$variant(t) => t.visit(f)),* }
            }
            pub fn stats(&self) -> KeySizeStats {
                match self { $(Self::$variant(t) => t.stats()),* }
            }
        }
    };
}
inline_tables!(
    (N0, 0),
    (N1, 1),
    (N2, 2),
    (N3, 3),
    (N4, 4),
    (N5, 5),
    (N6, 6),
    (N7, 7),
    (N8, 8),
    (N9, 9),
    (N10, 10),
    (N11, 11),
    (N12, 12),
    (N13, 13),
    (N14, 14),
    (N15, 15),
    (N16, 16),
    (N17, 17),
    (N18, 18),
    (N19, 19),
    (N20, 20),
    (N21, 21),
    (N22, 22),
    (N23, 23),
    (N24, 24),
    (N25, 25),
    (N26, 26),
    (N27, 27),
    (N28, 28),
    (N29, 29),
    (N30, 30),
    (N31, 31),
    (N32, 32),
    (N33, 33),
    (N34, 34),
    (N35, 35),
    (N36, 36),
    (N37, 37),
    (N38, 38),
    (N39, 39),
    (N40, 40),
    (N41, 41),
    (N42, 42),
    (N43, 43),
    (N44, 44),
    (N45, 45),
    (N46, 46),
    (N47, 47),
    (N48, 48),
    (N49, 49),
    (N50, 50),
    (N51, 51),
    (N52, 52),
    (N53, 53),
    (N54, 54),
    (N55, 55),
    (N56, 56),
    (N57, 57),
    (N58, 58),
    (N59, 59),
    (N60, 60),
    (N61, 61),
    (N62, 62),
    (N63, 63),
    (N64, 64)
);
