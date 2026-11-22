//! Linux-only probes around the timed benchmark; no probes run inside the timer.
use super::*;
use std::os::raw::{c_int, c_long};
#[repr(C)]
struct Timespec {
    seconds: c_long,
    nanoseconds: c_long,
}
extern "C" {
    fn clock_gettime(clock: c_int, value: *mut Timespec) -> c_int;
}
pub(super) fn process_cpu_ns() -> Result<u64> {
    let mut t = Timespec {
        seconds: 0,
        nanoseconds: 0,
    };
    // CLOCK_PROCESS_CPUTIME_ID = 2 on Linux (verified in system time.h).
    let code = unsafe { clock_gettime(2, &mut t) };
    require(code == 0, "process CPU clock unavailable")?;
    Ok((t.seconds as u64) * 1_000_000_000 + t.nanoseconds as u64)
}
fn numbers(path: &Path) -> BTreeMap<String, u64> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let key = fields.next()?.trim_end_matches(':').to_owned();
            let value = fields.next()?.parse().ok()?;
            Some((key, value))
        })
        .collect()
}
fn pressure(path: &Path) -> BTreeMap<String, u64> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let scope = fields.next()?.to_owned();
            let total = fields
                .find_map(|x| x.strip_prefix("total="))?
                .parse()
                .ok()?;
            Some((scope, total))
        })
        .collect()
}
fn cgroup_dir() -> Option<PathBuf> {
    let cg = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    let relative = cg.lines().find_map(|x| x.strip_prefix("0::"))?;
    let mounts = std::fs::read_to_string("/proc/self/mountinfo").ok()?;
    for line in mounts.lines() {
        let (left, right) = line.split_once(" - ")?;
        if right.split_whitespace().next() != Some("cgroup2") {
            continue;
        }
        let fields = left.split_whitespace().collect::<Vec<_>>();
        let root = *fields.get(3)?;
        let mount = *fields.get(4)?;
        if let Some(suffix) = relative.strip_prefix(root) {
            let path = Path::new(mount).join(suffix.trim_start_matches('/'));
            if path.join("cpu.stat").is_file() {
                return Some(path);
            }
        }
    }
    None
}
#[derive(Default)]
pub(super) struct Probe {
    io: BTreeMap<String, u64>,
    status: BTreeMap<String, u64>,
    scheduler: BTreeMap<String, u64>,
    cgroup_cpu: BTreeMap<String, u64>,
    cgroup_memory: BTreeMap<String, u64>,
    host_cpu: BTreeMap<String, u64>,
    host_io: BTreeMap<String, u64>,
    cgroup_path: Option<PathBuf>,
}
pub(super) fn sample() -> Probe {
    let mut scheduler = BTreeMap::new();
    if let Ok(tasks) = std::fs::read_dir("/proc/self/task") {
        for task in tasks.flatten() {
            if let Ok(s) = std::fs::read_to_string(task.path().join("schedstat")) {
                let values = s
                    .split_whitespace()
                    .filter_map(|x| x.parse::<u64>().ok())
                    .collect::<Vec<_>>();
                if values.len() >= 3 {
                    for (i, key) in ["on_cpu_ns", "runqueue_wait_ns", "timeslices"]
                        .iter()
                        .enumerate()
                    {
                        *scheduler.entry((*key).to_owned()).or_default() += values[i];
                    }
                }
            }
        }
    }
    let cg = cgroup_dir();
    Probe {
        io: numbers(Path::new("/proc/self/io")),
        status: numbers(Path::new("/proc/self/status")),
        scheduler,
        cgroup_cpu: cg
            .as_ref()
            .map(|p| numbers(&p.join("cpu.stat")))
            .unwrap_or_default(),
        cgroup_memory: cg
            .as_ref()
            .map(|p| numbers(&p.join("memory.events")))
            .unwrap_or_default(),
        host_cpu: pressure(Path::new("/proc/pressure/cpu")),
        host_io: pressure(Path::new("/proc/pressure/io")),
        cgroup_path: cg,
    }
}
fn delta(a: &BTreeMap<String, u64>, b: &BTreeMap<String, u64>) -> Value {
    json!(b
        .iter()
        .map(|(k, v)| (k.clone(), v.saturating_sub(*a.get(k).unwrap_or(&0))))
        .collect::<BTreeMap<_, _>>())
}
pub(super) fn report(before: &Probe, after: &Probe, cpu_ns: u64, wall_ms: f64) -> Value {
    json!({"process_cpu_ms":cpu_ns as f64/1e6,"wall_ms":wall_ms,
        "io_delta":delta(&before.io,&after.io),"threads_schedstat_delta":delta(&before.scheduler,&after.scheduler),
        "context_switch_delta":{
            "voluntary":after.status.get("voluntary_ctxt_switches").unwrap_or(&0).saturating_sub(*before.status.get("voluntary_ctxt_switches").unwrap_or(&0)),
            "involuntary":after.status.get("nonvoluntary_ctxt_switches").unwrap_or(&0).saturating_sub(*before.status.get("nonvoluntary_ctxt_switches").unwrap_or(&0))},
        "rss_before_kib":before.status.get("VmRSS"),"rss_after_kib":after.status.get("VmRSS"),
        "cgroup_cpu_available":after.cgroup_path.is_some(),"cgroup_path":after.cgroup_path,
        "cgroup_cpu_delta":delta(&before.cgroup_cpu,&after.cgroup_cpu),
        "cgroup_memory_events_delta":delta(&before.cgroup_memory,&after.cgroup_memory),
        "host_cpu_pressure_total_delta_us":delta(&before.host_cpu,&after.host_cpu),
        "host_io_pressure_total_delta_us":delta(&before.host_io,&after.host_io),
        "scope":"block execution plus final writeback; process CPU covers all threads",
        "limits":"schedstat may be disabled; host pressure includes unrelated processes; probe snapshots are outside wall timer"})
}
