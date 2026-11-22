//! Bounded benchmark queries and flushed, phase-specific percentage progress.
use crate::{intrusive::Map, Result};
use serde::Serialize;
use serde_json::json;
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    time::Instant,
};

pub fn footer_count(path: &Path) -> Result<u64> {
    let mut f = File::open(path)?;
    f.seek(SeekFrom::End(-18))?;
    let mut bytes = [0u8; 18];
    f.read_exact(&mut bytes)?;
    if u16::from_le_bytes(bytes[..2].try_into()?) != u16::MAX {
        return Err("sample has no final count footer".into());
    }
    Ok(u64::from_le_bytes(bytes[2..10].try_into()?))
}
pub struct Progress {
    phase: &'static str,
    total: u64,
    started: Instant,
    last: Instant,
    last_done: u64,
    next_percent: u64,
}
#[derive(Serialize)]
pub struct Rate {
    pub operations: u64,
    pub elapsed_seconds: f64,
    pub operations_per_second: f64,
}
impl Progress {
    pub fn new(phase: &'static str, total: u64) -> Result<Self> {
        let now = Instant::now();
        let p = Self {
            phase,
            total,
            started: now,
            last: now,
            last_done: 0,
            next_percent: 1,
        };
        p.emit(0, 0, 0.0, 0.0, 0.0)?;
        Ok(p)
    }
    fn emit(
        &self,
        percent: u64,
        done: u64,
        seconds: f64,
        interval_rate: f64,
        average_rate: f64,
    ) -> Result<()> {
        let mut out = std::io::stdout().lock();
        writeln!(
            out,
            "{}",
            json!({"phase":self.phase,"percent":percent,"completed":done,
            "total":self.total,"elapsed_seconds":seconds,"interval_ops_per_second":interval_rate,
            "average_ops_per_second":average_rate})
        )?;
        out.flush()?;
        Ok(())
    }
    pub fn update(&mut self, done: u64) -> Result<()> {
        if done > self.total {
            return Err("progress exceeds footer count".into());
        }
        let percent = if self.total == 0 {
            100
        } else {
            ((done as u128 * 100) / self.total as u128) as u64
        };
        if percent < self.next_percent {
            return Ok(());
        }
        let now = Instant::now();
        let seconds = now.duration_since(self.started).as_secs_f64();
        let interval = now
            .duration_since(self.last)
            .as_secs_f64()
            .max(f64::EPSILON);
        let interval_rate = (done - self.last_done) as f64 / interval;
        // Small fixtures may cross several percentages with one operation.
        while self.next_percent <= percent {
            self.emit(
                self.next_percent,
                done,
                seconds,
                interval_rate,
                done as f64 / seconds.max(f64::EPSILON),
            )?;
            self.next_percent += 1;
        }
        self.last = now;
        self.last_done = done;
        Ok(())
    }
    pub fn finish(&mut self, done: u64) -> Result<Rate> {
        if done != self.total {
            return Err("phase count differs from footer count".into());
        }
        self.update(done)?;
        let elapsed_seconds = self.started.elapsed().as_secs_f64();
        Ok(Rate {
            operations: done,
            elapsed_seconds,
            operations_per_second: done as f64 / elapsed_seconds.max(f64::EPSILON),
        })
    }
}
fn random(seed: &mut u64) -> u64 {
    // SplitMix64: reproducible experiment selection; no security/hash-table claim.
    *seed = seed.wrapping_add(0x9e3779b97f4a7c15);
    let mut z = *seed;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^ (z >> 31)
}
pub(crate) fn digest(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325u64, |h, b| {
        (h ^ *b as u64).wrapping_mul(0x100000001b3)
    })
}
pub(crate) struct Query {
    pub(crate) store: u16,
    pub(crate) key: Vec<u8>,
    pub(crate) value_len: usize,
    pub(crate) value_digest: u64,
}
pub struct Queries {
    queries: Vec<Query>,
    total: u64,
    count: u64,
    next_index: u64,
    seed: u64,
}
impl Queries {
    pub fn new(total: u64, requested: u64) -> Self {
        let count = requested.min(total);
        let mut s = Self {
            queries: Vec::new(),
            total,
            count,
            next_index: u64::MAX,
            seed: 0x7265616473776170,
        };
        s.select_next();
        s
    }
    fn select_next(&mut self) {
        let i = self.queries.len() as u64;
        self.next_index = if i >= self.count {
            u64::MAX
        } else {
            let start = ((i as u128 * self.total as u128) / self.count as u128) as u64;
            let end = (((i + 1) as u128 * self.total as u128) / self.count as u128) as u64;
            start + random(&mut self.seed) % (end - start)
        };
    }
    pub fn observe(&mut self, index: u64, store: u16, key: &[u8], value: &[u8]) {
        if index != self.next_index {
            return;
        }
        self.queries.push(Query {
            store,
            key: key.to_vec(),
            value_len: value.len(),
            value_digest: digest(value),
        });
        self.select_next();
    }
    pub fn into_shuffled(mut self) -> Result<Vec<Query>> {
        if self.queries.len() as u64 != self.count {
            return Err("incomplete random query sample".into());
        }
        for i in (1..self.queries.len()).rev() {
            let j = (random(&mut self.seed) % (i as u64 + 1)) as usize;
            self.queries.swap(i, j);
        }
        Ok(self.queries)
    }
    pub fn run_parallel(self, map: &Map, threads: usize) -> Result<serde_json::Value> {
        let queries = self.into_shuffled()?;
        let frozen = map.frozen();
        let summary = crate::parallel::run("random_read", &queries, threads, |q| {
            let value = frozen
                .get(q.store, &q.key)
                .ok_or("random lookup missing key")?;
            let checksum = digest(value);
            if value.len() != q.value_len || checksum != q.value_digest {
                return Err("random lookup value mismatch".into());
            }
            Ok(crate::parallel::Outcome {
                bytes: value.len() as u64,
                exact: 1,
                checksum,
                ..crate::parallel::Outcome::default()
            })
        })?;
        Ok(
            json!({"rate":summary.rate,"threads":threads,"value_bytes_consumed":summary.outcome.bytes,
            "target_reads_per_second":3000,"target_met":summary.rate.operations_per_second>=3000.0,
            "parallel":summary,"selection":"same deterministic stratified keys/shuffle; ten fixed equal slices",
            "measurement":"immutable frozen map; simultaneous read-only threads; full values consumed and verified; timer stops at last worker completion; parent report records whether a full verification sweep preceded this phase"}),
        )
    }
    pub fn run(self, map: &Map) -> Result<serde_json::Value> {
        let count = self.count;
        let queries = self.into_shuffled()?;
        let mut progress = Progress::new("random_read", count)?;
        let mut bytes = 0u64;
        for (i, q) in queries.iter().enumerate() {
            let value = map
                .get(q.store, &q.key)
                .ok_or("random lookup missing key")?;
            if value.len() != q.value_len || digest(value) != q.value_digest {
                return Err("random lookup value mismatch".into());
            }
            bytes += value.len() as u64;
            progress.update(i as u64 + 1)?;
        }
        let rate = progress.finish(count)?;
        Ok(
            json!({"rate":rate,"value_bytes_consumed":bytes,"target_reads_per_second":3000,
            "target_met":rate.operations_per_second >= 3000.0,
            "selection":"one deterministic random key per equal record-count stratum; shuffled once, without replacement",
            "measurement":"single-threaded get plus reading/digesting the entire value and progress output; no file I/O in timed phase; parent report records whether full verification preceded reads; not guaranteed cold; not production block throughput"}),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn selection_spans_input_and_reads_values_including_empty_and_large() {
        let mut map = Map::new(256, 1024);
        let mut q = Queries::new(1000, 100);
        for n in 0..1000u64 {
            let v = vec![
                n as u8;
                if n % 3 == 0 {
                    0
                } else if n % 3 == 1 {
                    257
                } else {
                    32
                }
            ];
            map.put((n % 2) as u16, &n.to_le_bytes(), &v).unwrap();
            q.observe(n, (n % 2) as u16, &n.to_le_bytes(), &v);
        }
        assert_eq!(q.queries.len(), 100);
        for (i, x) in q.queries.iter().enumerate() {
            let n = u64::from_le_bytes(x.key.as_slice().try_into().unwrap());
            assert!((i as u64 * 10..i as u64 * 10 + 10).contains(&n));
        }
        assert_eq!(q.run(&map).unwrap()["rate"]["operations"], 100);
    }
    #[test]
    fn random_read_rejects_changed_values_and_incomplete_sampling() {
        let mut map = Map::new(256, 1024);
        map.put(0, b"k", b"right").unwrap();
        let mut q = Queries::new(1, 10);
        q.observe(0, 0, b"k", b"wrong");
        assert!(q.run(&map).is_err());
        assert!(Queries::new(10, 1).run(&map).is_err());
        assert_eq!(
            Queries::new(0, 1).run(&map).unwrap()["rate"]["operations"],
            0
        );
    }
    #[test]
    fn progress_requires_exact_count_and_emits_all_percentages() {
        let mut p = Progress::new("fixture", 2).unwrap();
        p.update(1).unwrap();
        assert_eq!(p.next_percent, 51);
        assert!(p.finish(1).is_err());
        p.finish(2).unwrap();
        assert_eq!(p.next_percent, 101);
        assert!(p.update(3).is_err());
    }
}
