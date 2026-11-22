//! Page-isolated pointer arrays, optionally pinned in physical RAM.
//! Pools use their ordinary allocator and never share these locked mappings.
use super::{Node, Result};
use std::{
    io,
    mem::size_of,
    ops::{Deref, DerefMut},
    ptr, slice,
};
pub(super) enum BucketArray {
    Heap(Vec<Node>),
    Locked {
        data: *mut Node,
        count: usize,
        bytes: usize,
    },
}
impl BucketArray {
    pub fn empty() -> Self {
        Self::Heap(Vec::new())
    }
    pub fn new(count: usize, locked: bool) -> Result<Self> {
        if !locked {
            return Ok(Self::Heap(vec![ptr::null_mut(); count]));
        }
        assert!(count > 0);
        // SAFETY: sysconf has no memory preconditions.
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if page <= 0 {
            return Err("cannot determine page size for locked buckets".into());
        }
        let payload = count
            .checked_mul(size_of::<Node>())
            .ok_or("bucket length overflow")?;
        let bytes = payload
            .checked_add(page as usize - 1)
            .ok_or("bucket mapping overflow")?
            / page as usize
            * page as usize;
        // SAFETY: private anonymous mapping, valid size, no file descriptor.
        let data = unsafe {
            libc::mmap(
                ptr::null_mut(),
                bytes,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if data == libc::MAP_FAILED {
            return Err(format!("mmap bucket array: {}", io::Error::last_os_error()).into());
        }
        // SAFETY: mmap returned an exclusive, page-aligned range of this size.
        // Linux initializes anonymous pages to zero, valid null pointer slots.
        if unsafe { libc::mlock(data, bytes) } != 0 {
            let error = io::Error::last_os_error();
            // SAFETY: no pointer to the mapping has escaped.
            unsafe {
                libc::munmap(data, bytes);
            }
            return Err(format!("mlock bucket array ({bytes} bytes): {error}; increase this process's RLIMIT_MEMLOCK").into());
        }
        Ok(Self::Locked {
            data: data.cast(),
            count,
            bytes,
        })
    }
    pub fn allocation_bytes(&self) -> usize {
        match self {
            Self::Heap(v) => v.capacity() * size_of::<Node>(),
            Self::Locked { bytes, .. } => *bytes,
        }
    }
    pub fn locked_bytes(&self) -> usize {
        match self {
            Self::Heap(_) => 0,
            Self::Locked { bytes, .. } => *bytes,
        }
    }
}
impl Deref for BucketArray {
    type Target = [Node];
    fn deref(&self) -> &[Node] {
        match self {
            Self::Heap(v) => v,
            // SAFETY: mapping holds count initialized slots and outlives this borrow.
            Self::Locked { data, count, .. } => unsafe { slice::from_raw_parts(*data, *count) },
        }
    }
}
impl DerefMut for BucketArray {
    fn deref_mut(&mut self) -> &mut [Node] {
        match self {
            Self::Heap(v) => v,
            // SAFETY: mutable borrow provides exclusive access to live mapped slots.
            Self::Locked { data, count, .. } => unsafe { slice::from_raw_parts_mut(*data, *count) },
        }
    }
}
impl<'a> IntoIterator for &'a BucketArray {
    type Item = &'a Node;
    type IntoIter = slice::Iter<'a, Node>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
impl Drop for BucketArray {
    fn drop(&mut self) {
        if let Self::Locked { data, bytes, .. } = self {
            // SAFETY: this owns the exact mapping; all slice borrows have ended.
            // Unmapping also removes its locks, even if munlock unexpectedly fails.
            unsafe {
                libc::munlock((*data).cast(), *bytes);
                libc::munmap((*data).cast(), *bytes);
            }
        }
    }
}
