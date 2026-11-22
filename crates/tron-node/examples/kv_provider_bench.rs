//! Warm-cache adapter microbenchmark; not a cold mainnet replay.
//! cargo run --release -p tron-node --example kv_provider_bench -- NEW_OUTPUT_DIR
use std::{hint::black_box, path::Path, sync::Arc, time::Instant};
use tron_chainbase::{KvBackend, KvEngine, KvProvider, RocksDbBackend, WriteOp};
const ROWS: usize = 32_768;
const QUERIES: usize = 65_536;
fn key(i: usize) -> Vec<u8> {
    let mut k = vec![0; 32];
    for j in 0..4 {
        let n = (i as u64)
            .wrapping_mul(0x9e3779b97f4a7c15)
            .rotate_left(j * 13)
            ^ (j as u64);
        k[j as usize * 8..j as usize * 8 + 8].copy_from_slice(&n.to_be_bytes());
    }
    k
}
fn value(i: usize) -> Vec<u8> {
    let mut v = vec![(i % 251) as u8; 128];
    v[..8].copy_from_slice(&(i as u64).to_le_bytes());
    v
}
fn median(v: &[f64]) -> f64 {
    let mut a = v.to_vec();
    a.sort_by(f64::total_cmp);
    a[a.len() / 2]
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::args()
        .nth(1)
        .ok_or("new output directory required")?;
    let root = Path::new(&root);
    if root.exists() {
        return Err("output already exists".into());
    }
    std::fs::create_dir_all(root)?;
    let rocks = KvProvider {
        rocks_tuning: Some((4, 128)),
        ..Default::default()
    };
    let mut tides = rocks.clone();
    tides.engine = KvEngine::Tidesdb;
    tides.tidesdb.write_buffer_size_mb = 4;
    tides.tidesdb.block_cache_mb = 8;
    tides.tidesdb.max_open_sstables = 128;
    let engines: Vec<(&str, Arc<dyn KvBackend>)> = vec![
        (
            "rocks_direct",
            Arc::new(RocksDbBackend::open_tuned(
                root.join("rocks-direct"),
                4,
                128,
            )?),
        ),
        (
            "rocks_provider",
            rocks.open(&root.join("rocks-provider"), "state")?,
        ),
        (
            "tides_provider",
            tides.open(&root.join("tides-provider"), "state")?,
        ),
    ];
    let ops: Vec<_> = (0..ROWS).map(|i| WriteOp::Put(key(i), value(i))).collect();
    let queries: Vec<_> = (0..QUERIES)
        .map(|n| key((n.wrapping_mul(17_357) + 71) % ROWS))
        .collect();
    for (_, db) in &engines {
        for chunk in ops.chunks(512) {
            db.write_batch(chunk)?;
        }
        db.checkpoint()?;
        // Explicit warmup: this benchmark measures adapter overhead, not cold I/O.
        for k in &queries {
            black_box(db.get(k)?.ok_or("missing seeded key")?);
        }
    }
    let mut results = Vec::new();
    for (name, db) in &engines {
        let mut reads = Vec::new();
        let mut batches = Vec::new();
        let mut digest = 0u64;
        for _ in 0..7 {
            let t = Instant::now();
            for (n, k) in queries.iter().enumerate() {
                let v = db.get(k)?.ok_or("missing key")?;
                let actual = u64::from_le_bytes(v[..8].try_into()?);
                let expected = ((n * 17_357 + 71) % ROWS) as u64;
                assert_eq!(actual, expected);
                digest = digest.wrapping_add(black_box(actual));
            }
            reads.push(t.elapsed().as_nanos() as f64 / QUERIES as f64);
            let t = Instant::now();
            for (chunk_no, chunk) in queries.chunks(256).enumerate() {
                let keys: Vec<&[u8]> = chunk.iter().map(Vec::as_slice).collect();
                let values = db.multi_get(&keys)?;
                assert_eq!(values.len(), keys.len());
                for (offset, v) in values.into_iter().enumerate() {
                    let v = v.ok_or("missing batch key")?;
                    let actual = u64::from_le_bytes(v[..8].try_into()?);
                    assert_eq!(
                        actual,
                        (((chunk_no * 256 + offset) * 17_357 + 71) % ROWS) as u64
                    );
                    digest = digest.wrapping_add(black_box(actual));
                }
            }
            batches.push(t.elapsed().as_nanos() as f64 / QUERIES as f64);
        }
        let updates: Vec<_> = (0..8192).map(|i| WriteOp::Put(key(i), value(i))).collect();
        let t = Instant::now();
        for chunk in updates.chunks(256) {
            db.write_batch(chunk)?;
        }
        let write_ns = t.elapsed().as_nanos() as f64 / updates.len() as f64;
        let t = Instant::now();
        db.sync_wal()?;
        let wal_us = t.elapsed().as_micros();
        results.push(serde_json::json!({
            "engine":name,"point_ns_median":median(&reads),"point_ns_samples":reads,
            "batch256_ns_per_key_median":median(&batches),"batch256_ns_per_key_samples":batches,
            "buffered_batch256_write_ns_per_key":write_ns,"explicit_wal_sync_us":wal_us,
            "digest":digest
        }));
    }
    let result = serde_json::json!({
        "schema":1,"kind":"warm-cache-adapter-microbenchmark","rows":ROWS,
        "value_bytes":128,"key_bytes":32,"queries_per_round":QUERIES,"rounds":7,
        "write_sync":"buffered; explicit WAL sync measured separately",
        "limitations":"Not a first-pass/cold-cache or full-node benchmark; setup, SST flush and warmup excluded.",
        "results":results
    });
    let text = serde_json::to_string_pretty(&result)?;
    std::fs::write(root.join("result.json"), &text)?;
    println!("{text}");
    Ok(())
}
