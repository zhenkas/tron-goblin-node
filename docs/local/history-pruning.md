# Fixed-window history pruning and height replay

The node keeps a fixed window of block history and deletes everything older,
so the data directory grows with the chain state rather than with the number
of blocks. Maintenance runs in its own tasks. Block execution, state commits,
checkpointing and Firehose fsync cadence are unchanged, and no consumer can
slow down or hold block apply.

## Configuration

The feature is opt-in and requires both index and Firehose to be enabled:

~~~toml
[index]
enable = true

[index.firehose]
enable = true

[index.firehose.history]
enable = true
retain_blocks = 100000
undo_retain_blocks = 10000
batch_blocks = 1000
status_port = 28092
~~~

| Key | Default | Meaning |
| --- | --- | --- |
| `retain_blocks` | 100,000 | Window of applied blocks kept for peers and event-log consumers. At least 65,536, the block-reference ring transaction validation needs. |
| `undo_retain_blocks` | 10,000 | Undo records kept for reorgs. |
| `batch_blocks` | 1,000 | Cleanup portion, 1..4096, shared by every history store. |
| `status_port` | 28092 | Loopback port of the read-only status endpoint. `ack_port` is accepted as an old name. |

The trusted-cluster write journal has its own window, `[assist] retain_blocks`
(10,000 by default; 0 records nothing), and uses the same `batch_blocks` portion.
See [assisted sync](../assisted-sync.md).

`interval_seconds` from earlier versions is ignored. While this mode is
enabled the Firehose `retain_mb` byte budget is not applied.

## Window and portions

For applied head H, solidified height S, window K and portion B:

~~~text
target = min(H - K + 1, S)
if target - floor >= B:  floor += floor((target - floor) / B) * B
delete heights < floor
~~~

- Nothing is removed until a whole portion is due, and the boundary then moves
  by whole portions. In steady state exactly B blocks go at a time, and between
  K and K + B blocks are stored.
- The window counts applied blocks only. Blocks that are received and waiting
  to be applied are outside it, so a node that is far behind is not limited by
  K while it catches up.
- The solidified block is never removed, even when finality stalls; it is also
  the persisted chain anchor.
- On the first start over an old database the boundary jumps to the target in
  one step (still a multiple of B) and the backlog is deleted in write batches
  of at most B rows.

Maintenance looks at the chain every 30 seconds; a portion of 1,000 blocks
takes about 50 minutes to accumulate, so this only bounds how late a due
portion starts.

Cleanup removes old raw bodies (including stored forks), canonical height rows,
per-block transaction results, transaction-height references and
transaction-by-ID results. A reference or receipt reassigned to a newer inclusion
is preserved. Reference deletion is made durable before deleting its source body;
an interrupted batch can be rerun. The pruner does not modify account, contract
or current-state data.

Undo records follow their own window: with undo reserve U, heights below
max(1, min(H - U + 1, S + 1)) are due, so all unfinalized undo is protected
even when finalization stalls. They are removed by the same rule — only when a
whole portion is due, one portion at a time. Committed crash recovery uses the
node's forward checkpoint manifests, not these records.

The logical boundary is persisted before physical deletion and survives a
restart together with its anchor (the solidified block at the time). At startup
the anchor is checked against the canonical chain; malformed metadata, a
mismatched database or a boundary inside the consensus reserve fails closed.
Cleanup below an already published boundary resumes after a restart.

gRPC rejects data below the boundary immediately; raw wallet/history APIs lose
old rows incrementally. Closed Firehose segments are removed only when every
event is older than the boundary. Segment inspection runs outside the apply
writer lock; unlink takes the lock briefly, one file at a time. The active
segment is never removed, and segment granularity can retain some older events.

RocksDB deletes create tombstones. Normal compaction reclaims their disk space
later; no full online compaction or immediate disk-space reduction is promised.

## No acknowledgements

The window does not depend on any consumer. There is no acknowledgement
endpoint, and nothing a consumer does or fails to do moves or holds the
boundary. A consumer keeps its own durable checkpoint and names the block it
wants to continue from:

- an event-log consumer opens `Firehose.Tail` with `from_height` (below);
- a cluster partner sends the block it stands on.

When that block is below the window the node answers explicitly — gRPC
`OUT_OF_RANGE` for the event log, `HISTORY_GAP` for a cluster partner — and the
consumer recovers on its own: a cluster partner takes a new state snapshot and
continues with live blocks; an indexer treats it as a critical error and has to
be caught up from an external source.

Size the window for the longest outage a consumer must survive without that
recovery: 100,000 blocks is about 83 hours at a three-second cadence.

## Status

The node binds 127.0.0.1:status_port, without a public bind option.

~~~http
GET /history
~~~

~~~json
{"prune_before":88140001,"retain_blocks":100000,"batch_blocks":1000,
 "undo_retain_blocks":10000,"undo_prune_before":88230001,"undo_removed":4000,
 "blocks_removed":3000,"segments_removed":2,"check_interval_seconds":30}
~~~

`prune_before` is the logical retention boundary, not a claim that events exist
at every height above it. Counters are since process start.

## Replay without restarting the node

The additive protobuf field is optional int64 from_height = 2 in TailRequest.
Open a new gRPC Firehose.Tail request:

~~~json
{"from_height":"87531735"}
~~~

The height is inclusive. After replaying the retained journal, the same request
follows new entries. To resume after an indexed finalized checkpoint C, use
C + 1. A reconnect with a different height changes only that subscriber's
cursor. Existing subscribers and node sync continue normally.

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
explicitly; the node does not silently advance an old cursor.

Earlier raw block availability does not mean VM receipts or complete Firehose
events exist. The source node may still have native crash-recovery gaps or skip
oversized Firehose frames. Height replay detects an incomplete range; it does
not reconstruct missing VM execution.

## Validation

From the node directory:

~~~sh
cargo test --release --locked -p tron-index --lib
cargo test --release --locked -p tron-grpc --test firehose_tail
cargo test --release --locked -p tron-node --lib history_pruning
cargo test --release --locked -p tron-node --lib assisted_sync
cargo test --release --locked -p tron-node --lib config
~~~

Tests cover whole-portion movement of the boundary, the consensus reserve,
stalled finality, a partial portion left alone, bounded deletion, same-height
fork bodies, newer transaction inclusion, injected maintenance-write failures,
metadata validation, restart with a published boundary, the read-only status
socket (an acknowledgement request is a 404), independent undo portions, real
RocksDB reopen, and the cluster journal cut in portions without a byte budget.
gRPC tests cover simultaneous sequence and height subscribers, the replay/live
transition, UNWIND and explicit source-gap failure.

Destructive pruning checks use isolated fixtures. Runtime configuration,
database files, receipts, logs and deployment evidence remain outside Git.
