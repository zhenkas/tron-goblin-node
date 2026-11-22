# Critical notification adapter

This small Rust crate lets the node report conditions that need an operator. Domain code
supplies a static code and summary, an optional SHA-256 fingerprint of its local
error, and numeric context. It has no business, node, signing or transaction API.

```rust
use std::time::Duration;
use critical_notify::{CriticalEvent, Reporter};
let reporter = Reporter::from_env();
let _ = reporter.critical(
    CriticalEvent::new("storage_failed", "Persistent storage failed")
        .fingerprint("local diagnostic, never sent")
        .context("block", 42),
);
let _ = reporter.flush(Duration::from_secs(2)); // terminal boundary only
```

An `EventSink` trait allows domain tests to capture events without starting a
bridge. Normal calls enqueue durably and return without waiting for network
delivery. Domains decide severity: transient reconnects, request validation,
ordinary chain reverts and unavailable read sources are recoverable; storage or
integrity failure, required-subsystem exit and unknown signing outcomes require
attention. Notification failure never authorizes retrying business operations.

Delivery is a plain webhook, so any local service can act as the receiver.
`CRITICAL_NOTIFY_URL` selects it (plain HTTP on a loopback IP or literal
localhost; remote URLs, credentials in URLs and extra paths are rejected), or
`CRITICAL_NOTIFY_SOCKET` selects an absolute Unix socket. `CRITICAL_NOTIFY_TOKEN`
is sent as a bearer token. The adapter sends

```
POST /v1/notify
Authorization: Bearer <token>
Content-Type: application/json

{"request_id": "notify-<32 hex>", "severity": "critical", "text": "<JSON: component, code, summary, fingerprint, context>"}
```

and treats a 2xx JSON response containing a non-empty `event_id` as the
acknowledgement. The receiver must treat `request_id` as an idempotency key.

The installation also supplies `CRITICAL_NOTIFY_COMPONENT` and the absolute, private
`CRITICAL_NOTIFY_STATE_DIR`. Use a separate state directory per process, outside Git.
Without the endpoint and token variables the adapter is disabled; malformed enabled
configuration emits one fixed warning without printing any value.

A process-global reporter avoids multiple background senders. It writes a
private atomic JSON envelope and fsyncs the file and directory before delivery.
At most 256 pending envelopes are admitted. Same-content events within the run
return the original request identity, including after successful delivery.
Startup loads pending envelopes once; the worker sleeps on a condition variable
while idle. Only pending failures retry, with bounded exponential delay. Retries
keep identical request ID, severity and text, including missing or timed-out
responses. Acknowledgement requires a successful response containing event_id;
the receiver's event is the durable delivery record. Accepted pending envelopes are
removed only after acknowledgement. Unexpected/corrupt state disables startup
with the fixed warning and preserves files.

The token lives only in process memory and the Authorization header.
Raw error strings are streamed directly into a hash and never enter envelopes
or event text. Static summaries must not contain secrets. Numeric context is
bounded and names are restricted. Notification enqueue errors are returned to
the caller; terminal flush is bounded and callers preserve their original
business failure even when delivery is unavailable. Retained pending envelopes
are recovered after restart; a fatal exit cannot guarantee immediate delivery
while its receiver is unavailable.

`Reporter::completion(code, summary)` emits an info event solely when a requested
long script explicitly calls it. There is no automatic periodic completion or
healthy-service event.

Run only the adapter's focused integration checks:

```sh
cargo test --locked -p critical-notify
```

The tests use local mock HTTP/Unix receivers. They cover durable enqueue,
deduplication, restart recovery, unknown-response exact retry, authentication
failure, bounded flush, token/error redaction and loopback restrictions. They
do not start a blockchain, submit a transaction or exercise user scenarios.
