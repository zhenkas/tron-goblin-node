//! Domain-level critical events. The connector owns transport and durable retry.
use std::{
    fmt::Display,
    sync::{
        atomic::{AtomicBool, Ordering},
        OnceLock,
    },
    time::Duration,
};
use critical_notify::{CriticalEvent, EventSink, Reporter};

static REPORTER: OnceLock<Reporter> = OnceLock::new();
static REPORTED: AtomicBool = AtomicBool::new(false);
pub(crate) fn reporter() -> &'static Reporter {
    REPORTER.get_or_init(Reporter::from_env)
}
pub(crate) fn report(event: CriticalEvent, fatal: bool) {
    REPORTED.store(true, Ordering::Release);
    emit(reporter(), event, fatal);
}
pub(crate) fn emit(sink: &impl EventSink, event: CriticalEvent, fatal: bool) {
    let _ = sink.critical(event);
    if fatal {
        let _ = sink.flush(Duration::from_secs(2));
    }
}
/// A terminal CLI/startup fallback; runtime failures already carry a domain event.
pub fn terminal_failure(error: impl Display) {
    if !REPORTED.swap(true, Ordering::AcqRel) {
        emit(
            reporter(),
            CriticalEvent::new("node_terminal_failure", "TRON node command failed")
                .fingerprint(error),
            true,
        );
    } else {
        let _ = reporter().flush(Duration::from_secs(2));
    }
}
pub(crate) fn required_subsystem_failure(code: &'static str, error: impl Display) {
    report(
        CriticalEvent::new(code, "TRON node required subsystem failed").fingerprint(error),
        false,
    );
}
