//! Opt-in measurements for offline benchmarks; absent from normal builds.
use std::{
    cell::RefCell,
    sync::atomic::{AtomicBool, Ordering},
    time::Instant,
};
static ENABLED: AtomicBool = AtomicBool::new(false);
thread_local! { static SAMPLES: RefCell<Vec<VmSample>> = const { RefCell::new(Vec::new()) }; }
#[derive(Debug, Clone)]
pub struct VmSample {
    pub tx_id: [u8; 32],
    pub nanoseconds: u64,
}
pub fn set_enabled(value: bool) {
    ENABLED.store(value, Ordering::Relaxed);
}
pub fn take_samples() -> Vec<VmSample> {
    SAMPLES.with(|v| std::mem::take(&mut *v.borrow_mut()))
}
pub(crate) fn start(tx_id: [u8; 32]) -> Option<(Instant, [u8; 32])> {
    ENABLED
        .load(Ordering::Relaxed)
        .then(|| (Instant::now(), tx_id))
}
pub(crate) fn finish(start: Option<(Instant, [u8; 32])>) {
    if let Some((t, tx_id)) = start {
        let nanoseconds = t.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        SAMPLES.with(|v| v.borrow_mut().push(VmSample { tx_id, nanoseconds }));
    }
}
