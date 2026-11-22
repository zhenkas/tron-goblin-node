//! Balanced fixed partitions, synchronized start, no shared lock on the read path.
use crate::{
    progress::{Progress, Rate},
    Result,
};
use serde::Serialize;
use std::{
    collections::BTreeMap,
    sync::{mpsc, Barrier, OnceLock},
    thread,
    time::Instant,
};
#[derive(Default, Clone, Copy, Serialize)]
pub struct Outcome {
    pub bytes: u64,
    pub exact: u64,
    pub missing: u64,
    pub changed: u64,
    pub checksum: u64,
}
impl Outcome {
    fn add(&mut self, other: Self) {
        self.bytes += other.bytes;
        self.exact += other.exact;
        self.missing += other.missing;
        self.changed += other.changed;
        self.checksum = self.checksum.wrapping_add(other.checksum);
    }
}
#[derive(Serialize)]
pub struct Worker {
    pub first_index: usize,
    pub end_index: usize,
    pub completed: usize,
    pub elapsed_seconds: f64,
}
#[derive(Serialize)]
pub struct Summary {
    pub rate: Rate,
    pub threads: usize,
    pub workers: Vec<Worker>,
    pub outcome: Outcome,
    pub process_io_delta: BTreeMap<String, u64>,
}
pub fn proc_io() -> BTreeMap<String, u64> {
    std::fs::read_to_string("/proc/self/io")
        .unwrap_or_default()
        .lines()
        .filter_map(|line| {
            let (k, v) = line.split_once(':')?;
            if !["rchar", "read_bytes", "syscr"].contains(&k) {
                return None;
            }
            Some((k.to_string(), v.trim().parse().ok()?))
        })
        .collect()
}
pub fn run<T: Sync>(
    phase: &'static str,
    requests: &[T],
    threads: usize,
    read: impl Fn(&T) -> Result<Outcome> + Sync,
) -> Result<Summary> {
    if threads == 0 || threads > 64 || requests.is_empty() || requests.len() % threads != 0 {
        return Err("requests must split into equal nonempty partitions (1..64 threads)".into());
    }
    let batch = (requests.len() / 100).max(1);
    let width = requests.len() / threads;
    let barrier = Barrier::new(threads + 1);
    let clock = OnceLock::<Instant>::new();
    thread::scope(|scope| -> Result<Summary> {
        let (ready_tx, ready_rx) = mpsc::channel();
        let (tx, rx) = mpsc::channel();
        let mut handles = Vec::new();
        for (id, part) in requests.chunks_exact(width).enumerate() {
            let (send, ready) = (tx.clone(), ready_tx.clone());
            let (barrier, clock, read) = (&barrier, &clock, &read);
            handles.push(scope.spawn(move || -> Result<(Worker, Outcome, Instant)> {
                ready.send(())?;
                barrier.wait();
                let start = *clock.get().unwrap();
                let mut outcome = Outcome::default();
                let mut pending = 0;
                for request in part {
                    outcome.add(read(request)?);
                    pending += 1;
                    if pending == batch {
                        send.send(pending)?;
                        pending = 0;
                    }
                }
                if pending > 0 {
                    send.send(pending)?;
                }
                let end = Instant::now();
                Ok((
                    Worker {
                        first_index: id * width,
                        end_index: (id + 1) * width,
                        completed: part.len(),
                        elapsed_seconds: end.duration_since(start).as_secs_f64(),
                    },
                    outcome,
                    end,
                ))
            }));
        }
        drop(tx);
        drop(ready_tx);
        for _ in 0..threads {
            ready_rx.recv()?;
        }
        let before = proc_io();
        let mut progress = Progress::new(phase, requests.len() as u64)?;
        let started = Instant::now();
        clock.set(started).unwrap();
        barrier.wait();
        let mut completed = 0u64;
        let mut progress_error = None;
        for count in rx {
            completed += count as u64;
            if progress_error.is_none() {
                if let Err(e) = progress.update(completed) {
                    progress_error = Some(e);
                }
            }
        }
        let mut workers = Vec::new();
        let mut outcome = Outcome::default();
        let mut end = started;
        let mut worker_error = None;
        for handle in handles {
            match handle.join() {
                Ok(Ok((worker, data, finished))) => {
                    workers.push(worker);
                    outcome.add(data);
                    end = end.max(finished);
                }
                Ok(Err(e)) => {
                    if worker_error.is_none() {
                        worker_error = Some(e);
                    }
                }
                Err(_) => {
                    if worker_error.is_none() {
                        worker_error = Some("parallel reader panicked".into());
                    }
                }
            }
        }
        if let Some(e) = worker_error.or(progress_error) {
            return Err(e);
        }
        progress.finish(completed)?;
        let elapsed_seconds = end.duration_since(started).as_secs_f64();
        let after = proc_io();
        let process_io_delta = after
            .into_iter()
            .map(|(k, v)| {
                let delta = v.saturating_sub(*before.get(&k).unwrap_or(&0));
                (k, delta)
            })
            .collect();
        Ok(Summary {
            rate: Rate {
                operations: completed,
                elapsed_seconds,
                operations_per_second: completed as f64 / elapsed_seconds.max(f64::EPSILON),
            },
            threads,
            workers,
            outcome,
            process_io_delta,
        })
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[test]
    fn ten_equal_partitions_visit_every_request_once() {
        let rows: Vec<_> = (0..100000usize).collect();
        let seen: Vec<_> = (0..rows.len()).map(|_| AtomicUsize::new(0)).collect();
        let r = run("fixture_parallel", &rows, 10, |n| {
            seen[*n].fetch_add(1, Ordering::Relaxed);
            Ok(Outcome {
                bytes: 64,
                exact: 1,
                checksum: *n as u64,
                ..Outcome::default()
            })
        })
        .unwrap();
        assert_eq!(r.rate.operations, 100000);
        assert_eq!(r.outcome.bytes, 6400000);
        assert!(seen.iter().all(|n| n.load(Ordering::Relaxed) == 1));
        assert!(r
            .workers
            .iter()
            .enumerate()
            .all(|(i, w)| w.first_index == i * 10000
                && w.end_index == (i + 1) * 10000
                && w.completed == 10000));
        assert_eq!(r.outcome.checksum, (0..100000u64).sum::<u64>());
    }
    #[test]
    fn unequal_partitions_and_errors_are_rejected() {
        assert!(run("bad", &[1, 2, 3], 2, |_| Ok(Outcome::default())).is_err());
        assert!(run::<u8>("empty", &[], 1, |_| Ok(Outcome::default())).is_err());
        assert!(run("error", &[1u8; 10], 10, |_| Err("read failed".into())).is_err());
    }
}
