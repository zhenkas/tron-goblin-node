# Stop the node when block execution cannot be reproduced

The local policy patch is independent of the parameter 102 compatibility fix.
It is integrated directly into crates/tron-node/src/execution_policy.rs and the
central block-acceptance path in this source tree.
It adds an opt-in daemon policy:

~~~ini
Environment=TRON_STOP_ON_EXECUTION_ERROR=1
Restart=no
~~~

The flag enables the existing strict success/failure comparison against the
transaction results carried by the block. A disagreement rejects the block
session before state, undo or a checkpoint manifest is committed.

A rejected execution at the central block-acceptance boundary terminates the
node with exit code 78. That boundary also covers direct applies and orphan
reprocessing, so it cannot continue processing another queued block after the
failure. Process termination stops the independent history pruner too.
A normal transaction failure that agrees with the block remains a valid
result; invalid peer-data validation rejections remain separate from execution
failures.

The node service must not automatically restart after this failure. Keep its
data and diagnostics, investigate the rejected block, fix the implementation,
then resume or restore/replay the last correct state. The indexer's committed
SQLite checkpoint remains the external source of truth.

## Validation

- A real native transaction whose canonical result claims success but whose
  execution fails is rejected with ContractRetMismatch. Account, property and
  witness bytes remain unchanged; no undo record or recoverable checkpoint
  manifest is created.
- Child-process tests verify exit 78 for rejected execution with the policy
  enabled, unchanged default behavior, compatibility with the earlier
  verification flag, and ordinary rejection of invalid peer data.
- Build the normal daemon, then verify its service environment contains the
  flag and Restart=no.

The runtime uses a drop-in on its existing service, preserving its original
environment, memory limits and storage settings. Commit frequency, fsync
behavior and event backpressure are unchanged.
