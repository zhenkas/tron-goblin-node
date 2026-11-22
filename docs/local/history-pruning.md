# ACK-limited history pruning and height replay

The node owns a loopback ACK endpoint and independent history-maintenance
task. Block execution, state commits, checkpointing and Firehose fsync cadence
remain unchanged. There is no indexer backpressure on block apply.

## Configuration

The feature is opt-in and requires both index and Firehose to be enabled:

~~~toml
[index]
enable = true

[index.firehose]
enable = true

[index.firehose.history]
enable = true
retain_blocks = 150000
undo_retain_blocks = 10000
batch_blocks = 128
interval_seconds = 3600
ack_port = 28092
~~~

retain_blocks is a minimum, not a maximum. Its lower bound is 65,536,
the block-reference ring required by TRON transaction validation.
batch_blocks is 1..4096 and bounds raw block bodies per maintenance batch;
each height-keyed store also receives at most that many deletes per write batch.
A block can contain many transactions, so transaction-reference deletes and
memory consumption also depend on block size. interval_seconds is
1..31,536,000. History maintenance first runs after min(interval_seconds, 30)
seconds at startup, then at the configured interval. ACK never starts a pass.

The managed mode disables the independent Firehose retain_mb eviction policy,
including startup gap recovery. Without a fresh ACK, disk use may grow beyond
the minimum reserve. Monitor free disk; an ACK is permission to discard history,
not an automatic guarantee of bounded disk use.

## ACK API

The node binds 127.0.0.1:ack_port, without a public bind option.

~~~http
POST /history/ack
Content-Type: application/json

{"height":87547127,"block_id":"<64 hexadecimal characters, including height prefix>"}
~~~

Only ACK a whole block after committing all its derived data in the indexer's
database. Use a finalized canonical checkpoint, with its exact 32-byte block ID.
For multiple independent indexers, an aggregator must ACK their lowest safe
checkpoint; this endpoint represents one monotonically advancing consumer boundary.

The handler checks syntax and monotonicity, then updates RAM only.
It does not read node databases, fsync, wake the pruning task or delete anything.

~~~json
{"accepted":true,"changed":true,"persistent":false,"prune_started":false}
~~~

The identical ACK is idempotent (changed:false). Regression and same-height
hash conflicts return 409; invalid IDs and height prefixes return 400.
accepted means recorded in memory, not yet validated against the chain.
On its timer the pruner requires the ACK to be solidified and its ID to match
the canonical block index. Failure holds pruning.

GET /history returns the RAM ACK, published prune_before boundary, configuration,
and counts of removed bodies, Firehose segments and undo records. It also reports
undo_retain_blocks, the current undo_prune_before target, and undo_interval_seconds.
prune_before is the logical
retention boundary, not a claim that events exist at every height above it.

ACKs do not survive a node restart. Resend the indexer's durable checkpoint.
A previously published pruning boundary does survive and cleanup below it can
continue without another ACK: that history was already authorized for removal.
The preserved ACK anchor is checked against the canonical chain at startup.
Malformed metadata, a mismatched database, or violation of the consensus reserve
fails closed.

## Retention semantics

For a valid ACK A, current applied head H and minimum reserve K:

~~~text
new_floor = max(old_floor, max(1, min(A, H - K + 1)))
delete heights < new_floor
~~~

The ACK block itself remains available as an anchor. If ACKs stop, pruning stops
advancing even as the node continues syncing. Genesis and the latest K blocks
are retained. The pruner does not modify account, contract or current-state data.

Cleanup removes old raw bodies (including stored forks), canonical height rows,
per-block transaction results, transaction-height references and
transaction-by-ID results. A reference or receipt reassigned to a newer inclusion
is preserved. Reference deletion is made durable before deleting its source body;
an interrupted batch can be rerun. This maintenance durability is separate from
the unchanged apply/Firehose fsync policy.

Undo has an independent undo_retain_blocks setting (default 10,000; positive
and fitting signed 64-bit). Its cleanup does not depend on an indexer ACK or
advance the block/event replay boundary. For applied head H, solid height S
and undo reserve U, it deletes heights below max(1, min(H - U + 1, S + 1)).
Thus all unfinalized undo is protected even when finalization stalls. The target
is a retained window after maintenance, with temporary extra rows between passes;
it is not a hard instantaneous cap.

Undo maintenance starts at startup and repeats every min(interval_seconds, 30)
seconds. Each write deletes at most batch_blocks undo rows and yields between
batches. Undo and ordinary history use independent maintenance futures, so a
large undo backlog cannot postpone the history timer. Both run outside block
apply, with no extra apply fsync or backpressure.
A failed batch is retried idempotently. Committed crash recovery uses the node's
existing forward checkpoint manifests; deleting finalized undo does not remove
those manifests or change their fsync policy. Consumer replay uses retained
Firehose events and receipts, not these old state before-images.

The evaluation now uses 150,000 blocks of ordinary history and 10,000 of undo,
with 128-row batches and an hourly ordinary-history schedule. Ordinary history
still requires a valid consumer ACK. Current-state databases are not pruned by history maintenance.

The logical boundary is persisted before physical deletion. gRPC rejects data
below it immediately; raw wallet/history APIs lose old rows incrementally.
Closed Firehose segments are removed only when every event is older than the
boundary. Segment inspection runs outside the apply writer lock; unlink takes
the lock briefly, one file at a time. The active segment is never removed.
Segment granularity can retain additional older events.

RocksDB deletes create tombstones. Normal compaction reclaims their disk space
later; no full online compaction or immediate disk-space reduction is promised.

## Replay without restarting the node

The additive protobuf field is optional int64 from_height = 2 in TailRequest.
Open a new gRPC Firehose.Tail request:

~~~json
{"from_height":"87531735"}
~~~

The height is inclusive. After replaying the retained journal, the same request
follows new entries. To resume after an indexed finalized ACK, use ACK + 1.
A reconnect with a different height changes only that subscriber's cursor.
Existing subscribers and node sync continue normally.

Do not also set a nonzero from_seq. Native sequence resume remains available:

~~~json
{"from_seq":"12345"}
~~~

Store the last processed Entry.seq and resume at seq + 1 for fast ordinary
reconnections. A cold height lookup scans retained journal files in bounded
chunks; its cost is linear in retained journal size. It does not add an index
write to the apply path.

Replay preserves UNWIND followed by replacement APPLY entries. Height replay
rejects skipped heights, missing transaction information, invalid frames or an
unwind before its starting anchor. Resume from an earlier finalized checkpoint
when an unwind crosses that anchor. Requests outside retained history fail
explicitly; managed mode does not silently advance an old sequence cursor.

The first complete Firehose APPLY in this evaluation is height 87,531,735.
Earlier raw block availability does not mean VM receipts or complete Firehose
events exist. The source node may still have native crash-recovery gaps or skip
oversized Firehose frames. This patch detects incomplete height replay; it does
not change those native delivery guarantees or reconstruct missing VM execution.

## Validation

From the repository root:

~~~sh
cargo test --release --locked -p tron-index --lib -- --test-threads=2
cargo test --release --locked -p tron-grpc --test firehose_tail -- --test-threads=2
cargo test --release --locked -p tron-node --lib history_pruning -- --test-threads=2
cargo test --release --locked -p tron-node --lib firehose::tests -- --test-threads=2
cargo test --release --locked -p tron-node --lib config::tests -- --test-threads=2
cargo test --release --locked -p tron-node --test sync -- --test-threads=2
cargo test --release --locked -p tron-chainbase block_undo -- --test-threads=2
cargo test --release --locked -p tron-chainbase checkpoint -- --test-threads=2
cargo build --release --locked -p tron-node --bin tron-node
~~~

Before replacing the service binary, preserve the prior executable and runtime
configuration outside Git. Ensure the new executable is readable and executable
by the service user (0755), even when cargo ran under root with umask 077.
Recreate a collected transient unit if needed, preserving its 6 GiB soft /
8 GiB hard memory limits and other existing sandbox/resource settings.

Tests cover independent undo limits, stalled finality, real RocksDB reopen,
undo write failures, startup cleanup without ACK, backward-compatible config,
and a slow-undo regression proving that history still runs on its timer,
bounded deletion, reserve/ACK guards, same-height fork bodies,
newer transaction inclusion, injected maintenance-write failures, metadata
validation, restart without a RAM ACK, and a real REST socket whose ACK waits
for the independent timer. gRPC tests cover simultaneous sequence and height
subscribers, replay/live transition, UNWIND and explicit source-gap failure.

Production history must not be pruned using invented test ACKs. The node-only
deployment initially started with ACK:null; the connected indexer now sends
real solid checkpoints. Destructive pruning checks use isolated fixtures. Runtime configuration, database files, original binary, receipts,
logs and deployment evidence remain outside Git.

Live verification after deployment read the same eight entries from height
87,531,735 and native seq 1, then replayed height 87,547,291 through 87,547,305
and followed beyond the pre-request head. Canonical block IDs matched the wallet
API; 17 peers and zero consensus divergences were observed. An invalid REST ACK
returned 400 and left ACK:null with zero deleted bodies/segments. All 147 relevant
tests passed (81 index, 3 gRPC, 9 history pruning, 10 Firehose, 44 configuration).

The independent undo update passed 129 Rust checks: 17 maintenance tests,
44 configuration tests, 29 sync/reorg tests, 10 Firehose tests, 6 undo-store
tests and 23 checkpoint tests. The monitoring suite passed 23 tests, including
older node responses without undo fields. The connected indexer service recovered
through startup replay after the node restart; all 1,192 saved balances and
totalSupply matched contract calls again.
