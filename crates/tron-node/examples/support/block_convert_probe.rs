//! Bounded migration feasibility probe. Source is always read-only; output is partial.
use super::rocks::BenchBackend;
use super::*;
use ::rocksdb::{IteratorMode, Options, ReadOptions, DB};
use tron_chainbase::WriteOp;

fn bytes(path: &Path) -> Result<u64> {
    let mut n = 0;
    for e in std::fs::read_dir(path)? {
        let e = e?;
        let m = e.metadata()?;
        n += if m.is_dir() {
            bytes(&e.path())?
        } else {
            m.len()
        };
    }
    Ok(n)
}
fn fold(digest: &mut Vec<u8>, key: &[u8], value: &[u8]) {
    let mut data = Vec::with_capacity(48 + key.len() + value.len());
    data.extend_from_slice(digest);
    data.extend_from_slice(&(key.len() as u64).to_le_bytes());
    data.extend_from_slice(key);
    data.extend_from_slice(&(value.len() as u64).to_le_bytes());
    data.extend_from_slice(value);
    *digest = tron_crypto::hash::sha256(&data).to_vec();
}
fn raw_property(text: &str, label: &str) -> Option<u64> {
    let part = text
        .split(';')
        .find(|p| p.trim_start().starts_with(label))?
        .trim();
    part.strip_prefix(label)?
        .trim_start_matches(|c: char| c == '=' || c == ':' || c.is_whitespace())
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}
fn free_bytes(path: &Path) -> Result<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let path = CString::new(path.as_os_str().as_bytes())?;
    let mut st = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    require(
        unsafe { libc::statvfs(path.as_ptr(), st.as_mut_ptr()) } == 0,
        "statvfs failed",
    )?;
    let st = unsafe { st.assume_init() };
    Ok(st.f_bavail as u64 * st.f_frsize as u64)
}
pub(super) fn run(source: &Path, out: &Path, mib: usize) -> Result<()> {
    require(
        (1..=64).contains(&mib),
        "probe budget must be 1..64 MiB per selected store",
    )?;
    full::guard(source)?;
    require(
        !out.exists(),
        "preserve prior output; choose new probe directory",
    )?;
    std::fs::create_dir_all(out)?;
    let free_before = free_bytes(out)?;
    require(
        free_before > 64 * 1024 * 1024 * 1024,
        "keep at least 64 GiB free",
    )?;
    let mut inventory = vec![];
    let mut samples = vec![];
    let cap = mib * 1024 * 1024;
    for (id, name) in STORES {
        let path = source.join("database").join(name);
        let mut options = Options::default();
        options.set_max_open_files(64);
        options.set_paranoid_checks(true);
        let db = DB::open_for_read_only(&options, &path, false)?;
        let properties = db
            .property_value("rocksdb.aggregated-table-properties")?
            .ok_or("missing SST properties")?;
        let raw_key = raw_property(&properties, "raw key size");
        let raw_value = raw_property(&properties, "raw value size");
        require(
            raw_key.is_some() && raw_value.is_some(),
            "cannot parse raw SST sizes",
        )?;
        inventory.push(json!({"store_id":id,"store":name,"physical_directory_bytes":bytes(&path)?,
            "raw_sst_key_bytes":raw_key,"raw_sst_value_bytes":raw_value,"table_properties":properties}));
        if !matches!(
            *name,
            "account" | "account-asset" | "storage-row" | "delegation" | "code"
        ) {
            continue;
        }
        let target_path = out.join("sample-database").join(name);
        let target = tides::TidesBackend::open(&target_path, true)?;
        let mut opts = ReadOptions::default();
        opts.fill_cache(false);
        opts.set_verify_checksums(true);
        opts.set_readahead_size(1024 * 1024);
        let mut ops = Vec::new();
        let mut batch_bytes = 0;
        let mut logical = 0;
        let mut count = 0u64;
        let mut digest = vec![0u8; 32];
        let started = Instant::now();
        let mut reached_end = true;
        for item in db.iterator_opt(IteratorMode::Start, opts) {
            let (k, v) = item?;
            if logical > 0 && logical + k.len() + v.len() > cap {
                reached_end = false;
                break;
            }
            fold(&mut digest, &k, &v);
            logical += k.len() + v.len();
            count += 1;
            batch_bytes += k.len() + v.len();
            ops.push(WriteOp::Put(k.to_vec(), v.to_vec()));
            if batch_bytes >= 512 * 1024 {
                require(
                    free_bytes(out)? > 64 * 1024 * 1024 * 1024,
                    "free-space reserve reached",
                )?;
                target.write_batch(&ops)?;
                ops.clear();
                batch_bytes = 0;
            }
        }
        target.write_batch(&ops)?;
        let import_ms = started.elapsed().as_secs_f64() * 1000.;
        let flush_started = Instant::now();
        target.flush_and_sync()?;
        let flush_ms = flush_started.elapsed().as_secs_f64() * 1000.;
        drop(target);
        let check_started = Instant::now();
        let reopened = tides::TidesBackend::open(&target_path, false)?;
        let mut got = vec![0u8; 32];
        let mut n = 0u64;
        let mut start = vec![];
        loop {
            let rows = reopened.scan_from(&start, 4096)?;
            if rows.is_empty() {
                break;
            }
            for (k, v) in &rows {
                fold(&mut got, k, v);
                n += 1;
            }
            start = rows.last().unwrap().0.clone();
            start.push(0);
        }
        require(
            n == count && got == digest,
            "reopened sample content differs from source",
        )?;
        drop(reopened);
        samples.push(json!({"store_id":id,"store":name,"rows":count,"logical_key_value_bytes":logical,
            "import_ms":import_ms,"flush_ms":flush_ms,"verify_ms":check_started.elapsed().as_secs_f64()*1000.,
            "target_directory_bytes":bytes(&target_path)?,"source_reached_end":reached_end,
            "prefix_sample":true,"reopened_digest_verified":true,"sha256_chain":hex::encode(digest)}));
    }
    let raw_sst_bytes = inventory
        .iter()
        .map(|x| {
            x["raw_sst_key_bytes"].as_u64().unwrap() + x["raw_sst_value_bytes"].as_u64().unwrap()
        })
        .sum::<u64>();
    let report = json!({"schema":1,"source":source,"source_open_mode":"read-only","full_conversion":false,
        "partial_sample_not_for_block_execution":true,"sample_budget_mib_per_store":mib,
        "raw_sst_key_value_bytes_including_obsolete_versions":raw_sst_bytes,"inventory":inventory,"samples":samples,
        "free_bytes_before":free_before,"free_bytes_after":free_bytes(out)?,
        "limitations":"prefix samples from five largest stores; raw SST totals include old versions and internal keys; import includes hashing and buffered writes; flush and verification separate; not a full migration ETA"});
    std::fs::write(out.join("report.json"), serde_json::to_vec_pretty(&report)?)?;
    println!("{}", report);
    Ok(())
}
