//! Positioned 64-byte file reads, sharing one descriptor across fixed reader partitions.
#[allow(dead_code)]
#[path = "kv_map_estimate/intrusive.rs"]
mod intrusive;
#[allow(dead_code)]
#[path = "kv_map_estimate/model.rs"]
mod model;
#[path = "kv_map_estimate/parallel.rs"]
mod parallel;
#[allow(dead_code)]
#[path = "kv_map_estimate/progress.rs"]
mod progress;
#[allow(dead_code)]
#[path = "kv_map_estimate/sample.rs"]
mod sample;
use serde_json::json;
use std::{
    fs::{self, File},
    io,
    os::unix::{
        fs::{FileExt, MetadataExt},
        io::AsRawFd,
    },
    path::PathBuf,
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
const BYTES: usize = 64;
fn offsets(len: u64, reads: usize) -> Result<Vec<u64>> {
    if len < BYTES as u64 {
        return Err("file shorter than read size".into());
    }
    let mut seed = 0x66696c655f363442u64;
    Ok((0..reads)
        .map(|_| {
            seed = seed.wrapping_add(0x9e3779b97f4a7c15);
            let mut z = seed;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
            (z ^ (z >> 31)) % (len - BYTES as u64 + 1)
        })
        .collect())
}
fn residency(file: &File) -> Result<serde_json::Value> {
    let len = usize::try_from(file.metadata()?.len())?;
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
    if len == 0 || page == 0 {
        return Err("invalid file/page size".into());
    }
    let mut bits = vec![0u8; len.div_ceil(page)];
    // SAFETY: read-only descriptor; PROT_NONE reserves address space without
    // faulting file data in. mincore writes one byte per covered page to bits.
    let address = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_NONE,
            libc::MAP_PRIVATE,
            file.as_raw_fd(),
            0,
        )
    };
    if address == libc::MAP_FAILED {
        return Err(io::Error::last_os_error().into());
    }
    let status = unsafe { libc::mincore(address, len, bits.as_mut_ptr()) };
    let error = io::Error::last_os_error();
    unsafe {
        libc::munmap(address, len);
    }
    if status != 0 {
        return Err(error.into());
    }
    let resident = bits.iter().filter(|b| **b & 1 != 0).count();
    Ok(
        json!({"page_bytes":page,"pages":bits.len(),"resident_pages":resident,"resident_fraction":resident as f64/bits.len() as f64}),
    )
}
fn advise(file: &File, advice: i32) -> Result<()> {
    let error = unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, advice) };
    if error != 0 {
        return Err(io::Error::from_raw_os_error(error).into());
    }
    Ok(())
}
fn read_one(file: &File, offset: &u64) -> Result<parallel::Outcome> {
    let mut bytes = [0u8; BYTES];
    file.read_exact_at(&mut bytes, *offset)?;
    Ok(parallel::Outcome {
        bytes: BYTES as u64,
        exact: 1,
        checksum: progress::digest(&bytes),
        ..parallel::Outcome::default()
    })
}
fn run() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let (mut input, mut work) = (PathBuf::new(), PathBuf::new());
    let (mut threads, mut reads) = (1usize, 100000usize);
    let mut evict = false;
    while let Some(arg) = args.next() {
        if arg == "--help" {
            println!("kv_file_read --input SAMPLE --work-dir NEW_DIR [--threads 1] [--reads 100000] [--evict-file-cache]");
            return Ok(());
        }
        if arg == "--evict-file-cache" {
            evict = true;
            continue;
        }
        let v = args.next().ok_or("missing argument")?;
        match arg.as_str() {
            "--input" => input = v.into(),
            "--work-dir" => work = v.into(),
            "--threads" => threads = v.parse()?,
            "--reads" => reads = v.parse()?,
            _ => return Err("unknown argument".into()),
        }
    }
    if input.as_os_str().is_empty()
        || work.as_os_str().is_empty()
        || reads == 0
        || threads == 0
        || threads > 64
        || reads % threads != 0
    {
        return Err("invalid arguments".into());
    }
    let input = input.canonicalize()?;
    let uid = unsafe { libc::geteuid() };
    if uid == 0 || input.metadata()?.uid() != uid {
        return Err("run as the non-root file owner".into());
    }
    fs::create_dir(&work)?;
    let file = File::open(&input)?;
    let len = file.metadata()?.len();
    let offsets = offsets(len, reads)?;
    let fingerprint = offsets.iter().fold(0u64, |h, n| h.rotate_left(7) ^ n);
    advise(&file, libc::POSIX_FADV_RANDOM)?;
    let before_eviction = residency(&file)?;
    if evict {
        advise(&file, libc::POSIX_FADV_DONTNEED)?;
    }
    let before = residency(&file)?;
    let memory_before = sample::process_memory();
    let summary = parallel::run("file_random_read", &offsets, threads, |offset| {
        read_one(&file, offset)
    })?;
    let after = residency(&file)?;
    let report = json!({"schema":1,"phase":"complete","input":input,"file_bytes":len,"read_bytes_per_request":BYTES,
        "reads":reads,"threads":threads,"offset_order_fingerprint":fingerprint,"rate":summary.rate,"parallel":summary,
        "memory_before_reads":memory_before,"memory_after_reads":sample::process_memory(),
        "cache_before_eviction":before_eviction,"cache_before_reads":before,"cache_after_reads":after,
        "cache_eviction_requested":evict,"cache_policy":"FADV_RANDOM; optional DONTNEED only for this private benchmark file, residency checked by non-faulting mincore",
        "measurement":"one shared descriptor, positioned read_exact_at/pread, exactly 64 bytes requested per offset, arbitrary byte offsets including possible page crossings, identical deterministic offset sequence for each thread count; worker timer excludes preparation and stops at last worker completion; no global cache drop",
        "offset_seed":"0x66696c655f363442"});
    sample::save(&work.join("report.json"), &report)?;
    println!("{report}");
    Ok(())
}
fn main() {
    if let Err(e) = run() {
        eprintln!("file read benchmark failed: {e}");
        std::process::exit(1);
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    #[test]
    fn shared_descriptor_parallel_pread_matches_serial_without_moving_cursor() {
        let mut file = tempfile::tempfile().unwrap();
        let bytes: Vec<u8> = (0..65536usize).map(|n| (n % 251) as u8).collect();
        file.write_all(&bytes).unwrap();
        let positions = offsets(bytes.len() as u64, 1000).unwrap();
        assert_eq!(positions, offsets(bytes.len() as u64, 1000).unwrap());
        for offset in &positions {
            let mut value = [0u8; BYTES];
            file.read_exact_at(&mut value, *offset).unwrap();
            assert_eq!(&value, &bytes[*offset as usize..*offset as usize + BYTES]);
        }
        let serial =
            parallel::run("serial_fixture", &positions, 1, |n| read_one(&file, n)).unwrap();
        let concurrent =
            parallel::run("parallel_fixture", &positions, 10, |n| read_one(&file, n)).unwrap();
        assert_eq!(serial.outcome.checksum, concurrent.outcome.checksum);
        assert_eq!(concurrent.outcome.bytes, 64000);
        assert_eq!(file.metadata().unwrap().len(), bytes.len() as u64);
        assert!(offsets(63, 1).is_err());
        assert_eq!(offsets(64, 10).unwrap(), vec![0; 10]);
        assert!(read_one(&file, &(bytes.len() as u64 - 63)).is_err());
    }
}
