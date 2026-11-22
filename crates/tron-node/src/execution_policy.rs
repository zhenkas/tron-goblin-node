//! Optional fail-closed policy for the daemon's block execution boundary.
//! Strict result verification runs before the block session commits. A failed
//! execution then terminates the process, including independent pruning tasks.
pub(crate) fn verify_contract_ret() -> bool {
    stop_requested() || std::env::var_os("TRON_VERIFY_CONTRACT_RET").is_some()
}

fn stop_requested() -> bool {
    std::env::var("TRON_STOP_ON_EXECUTION_ERROR").as_deref() == Ok("1")
}

pub(crate) fn enforce_outcome(block: i64, outcome: &crate::sync::AcceptOutcome) {
    if report_execution_failure(
        crate::critical_notifications::reporter(),
        stop_requested(),
        block,
        outcome,
    ) {
        if let crate::sync::AcceptOutcome::RejectedExecution(reason) = outcome {
            eprintln!(
                "FATAL block execution rejected at {block}: {reason}; stopping node (exit 78)"
            );
        }
        std::process::exit(78);
    }
}

fn report_execution_failure(
    sink: &impl critical_notify::EventSink,
    stop: bool,
    block: i64,
    outcome: &crate::sync::AcceptOutcome,
) -> bool {
    let crate::sync::AcceptOutcome::RejectedExecution(reason) = outcome else {
        return false;
    };
    if !stop {
        return false;
    }
    crate::critical_notifications::emit(
        sink,
        critical_notify::CriticalEvent::new(
            "node_execution_mismatch",
            "TRON block execution rejected; node stopping",
        )
        .fingerprint(reason)
        .context("block", block),
        true,
    );
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct Sink {
        events: std::cell::RefCell<Vec<critical_notify::CriticalEvent>>,
        flushes: std::cell::Cell<usize>,
    }
    impl critical_notify::EventSink for Sink {
        fn critical(
            &self,
            event: critical_notify::CriticalEvent,
        ) -> std::io::Result<critical_notify::ReportResult> {
            self.events.borrow_mut().push(event);
            Ok(critical_notify::ReportResult::Disabled)
        }
        fn flush(&self, timeout: std::time::Duration) -> bool {
            assert!(timeout <= std::time::Duration::from_secs(2));
            self.flushes.set(self.flushes.get() + 1);
            true
        }
    }
    #[test]
    fn only_opted_in_execution_rejection_emits_a_fatal_event() {
        let sink = Sink::default();
        let execution =
            crate::sync::AcceptOutcome::RejectedExecution("sensitive diagnostic".into());
        let validation =
            crate::sync::AcceptOutcome::RejectedValidation("invalid peer signature".into());
        assert!(!report_execution_failure(&sink, false, 42, &execution));
        assert!(!report_execution_failure(&sink, true, 42, &validation));
        assert!(sink.events.borrow().is_empty());
        assert_eq!(sink.flushes.get(), 0);
        assert!(report_execution_failure(&sink, true, 42, &execution));
        let events = sink.events.borrow();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].code(), "node_execution_mismatch");
        assert_eq!(events[0].numeric_context().get("block"), Some(&42));
        assert!(!format!("{:?}", events[0]).contains("sensitive diagnostic"));
        assert!(events[0].error_fingerprint().is_some());
        assert_eq!(sink.flushes.get(), 1);
    }

    #[test]
    fn fatal_child() {
        if let Ok(expected) = std::env::var("TRON_POLICY_TEST_CHILD") {
            assert_eq!(verify_contract_ret(), expected == "true");
            let outcome = if std::env::var_os("TRON_POLICY_VALIDATION_ONLY").is_some() {
                crate::sync::AcceptOutcome::RejectedValidation("invalid peer signature".into())
            } else {
                crate::sync::AcceptOutcome::RejectedExecution("canonical execution mismatch".into())
            };
            enforce_outcome(42, &outcome);
        }
    }

    #[test]
    fn opt_in_enables_strict_verification_and_terminates_the_process() {
        let exe = std::env::current_exe().unwrap();
        for (stop, verify, validation_only, expected_verify, exit) in [
            (None, None, false, false, 0),
            (Some("0"), None, false, false, 0),
            (Some("1"), None, false, true, 78),
            (Some("1"), None, true, true, 0),
            (None, Some("1"), false, true, 0),
        ] {
            let mut child = std::process::Command::new(&exe);
            child
                .args([
                    "--exact",
                    "execution_policy::tests::fatal_child",
                    "--nocapture",
                ])
                .env_remove("TRON_STOP_ON_EXECUTION_ERROR")
                .env_remove("TRON_VERIFY_CONTRACT_RET")
                .env_remove("TRON_POLICY_VALIDATION_ONLY")
                .env("TRON_POLICY_TEST_CHILD", expected_verify.to_string());
            if let Some(v) = stop {
                child.env("TRON_STOP_ON_EXECUTION_ERROR", v);
            }
            if let Some(v) = verify {
                child.env("TRON_VERIFY_CONTRACT_RET", v);
            }
            if validation_only {
                child.env("TRON_POLICY_VALIDATION_ONLY", "1");
            }
            let result = child.output().unwrap();
            assert_eq!(result.status.code(), Some(exit), "{:?}", result);
            if exit == 78 {
                assert!(String::from_utf8_lossy(&result.stderr)
                    .contains("FATAL block execution rejected"));
            }
        }
    }
}
