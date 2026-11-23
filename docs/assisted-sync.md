# Assisted catch-up

An authenticated donor executes blocks normally and journals their complete
logical writes before consensus commit. A receiver may apply those writes
without executing transactions. This is trusted execution replication: mTLS
authenticates the configured donor; the protocol does not independently prove
its execution or finality.

The feature is disabled by default. It currently requires a non-witness node,
BlockSession storage (`snapshot_reorg = false`), the local index and Firehose.
Enabling it forces complete write/internal-transaction capture and disables
pipelined commit overlap so imported and locally executed blocks share one
process-wide writer lock.

## Bootstrap and configuration

An empty receiver now bootstraps over the same authenticated Link, before it opens
ledger stores, normal P2P, wallet RPC or Firehose. It needs its own mTLS identity,
an authorized donor and a trusted genesis ID; no pre-copied state is required.
The built-in mainnet genesis is the default. For a private chain, set
`assist.bootstrap_genesis` to its 32-byte block ID in hex. An already populated
data directory follows ordinary replication and is never silently overwritten.

The donor permits one active snapshot owned by one authorized receiver. Its
durable request ID resumes that snapshot after either process restarts. An
explicit new request from the same receiver replaces it at the donor's current
committed head; obsolete request IDs cannot replace the newer generation again.
Another receiver cannot take over the active snapshot.

The donor takes the common block-apply lock, persists a retention pin for B+1,
and checkpoints all ledger databases through their already-open engine handles.
The process remains running. Only local checkpoint creation pauses block writers;
hashing and network transfer happen after releasing that lock. Same-filesystem
RocksDB SST hardlinks avoid an initial full data copy. Snapshot files are
immutable; auxiliary peer and mempool state is not transferred. A snapshot at
head B is offered for installation only once B is canonical solid. Reorged
unfinalized snapshots are explicitly replaced rather than installed.

The receiver downloads bounded 4-MiB chunks, verifies their SHA256 and complete
file checksums, and syncs file data before advancing its durable file/offset
cursor. A manifest pins the chain/profile, B, block ID, write-chain digest,
file paths, sizes and hashes. The complete set is verified before a durable
installation intent and atomic database-directory activation. Startup recovers
an interrupted activation before opening any public API, verifies the installed
database's actual genesis/head against the manifest, and persists the base
replication cursor before sending the snapshot-installation ACK.

The installation ACK is distinct from ordinary per-block ACKs and downstream
indexer ACKs. The donor keeps all required post-B changes even if the receiver
disconnects before its first ACK. The durable pin advances only after persisted
block ACKs; normal retention reserves still apply. Journal exhaustion is an
explicit failure, never permission to omit necessary changes.

Bootstrap files live under `<data_dir>/bootstrap/`; donor snapshot metadata and
the single checkpoint directory live under `<data_dir>/assisted-sync/snapshot*`.
Receiver states are requesting → downloading → installing → installed →
complete; the final state means the donor confirmed installation. These
files must survive restarts. Never clear an unknown session as generic cleanup.

To explicitly abandon an unfinished download, stop that receiver and change
`assist.bootstrap_reset_token` to a new unique token before restarting it.
The token is applied once and is durable across subsequent restarts. To reset
an already installed receiver, use a fresh empty data directory with the same
authorized identity; its new bootstrap request replaces the donor snapshot.
This preserves the old ledger for deliberate operator reconciliation instead
of deleting it implicitly.

A temporary receiver can explicitly release its own snapshot after it stops:
`cargo run --release -p tron-node --example assist-release -- RECEIVER_CONFIG SNAPSHOT_ID`.
The configured outbound donor endpoint is required for this one-use control
client. Release is authenticated and idempotent: it removes the temporary
checkpoint and its retention pin while preserving donor ACK/journal metadata.
A released snapshot cannot be resurrected by a delayed installation ACK.

Bootstrap is enabled by default for empty assisted receivers and can be
disabled with `assist.bootstrap = false`. It currently requires the RocksDB
provider. `assist.max_snapshot_bytes` bounds the advertised complete data size
(default 1 TiB); provision free disk for the receiver plus donor SST retention.
A new snapshot does not contain a separate indexer's projection/history;
restore that indexer's checkpoint independently with the matching event cursor.

A block ID alone does not guarantee identical state; every subsequent imported
write still checks its preimage. The initial donor write journal starts after
its enable-time head. A populated receiver below its retained floor requires a
new bootstrap; it never skips an unavailable history gap.

Keep certificates, private keys, peer configuration, journals and snapshots
outside Git. Use a dedicated CA and unique server DNS identity per deployment.
Only the leaf fingerprints explicitly listed under `peers` may open an inbound
Link RPC, even if other clients have CA-signed certificates. Outbound TLS
verifies the per-peer server CA and DNS name. The fingerprint is the SHA256 of
the leaf certificate in DER form (not the PEM file).

## Legacy donor/receiver mode

The fixed-role examples below apply when trusted_cluster is false.

Example donor configuration (paths and public identities are placeholders):

```toml
[assist]
enable = true
node_id = "donor"
listen = "127.0.0.1:51054"
certificate_file = "/private/assist/donor.pem"
private_key_file = "/private/assist/donor.key"
client_ca_file = "/private/assist/clients-ca.pem"
retain_blocks = 10000
max_journal_bytes = 34359738368

[[assist.peers]]
id = "replica"
certificate_sha256 = "REPLACE_WITH_64_HEX_DIGITS"
allow_donate = true
required_for_retention = true
# Omit these three fields for inbound-only. The donor may initiate instead:
# endpoint = "https://replica.example:51054"
# tls_domain = "replica.example"
# server_ca_file = "/private/assist/replica-server-ca.pem"
```

Receiver configuration:

```toml
[assist]
enable = true
node_id = "replica"
listen = "127.0.0.1:51054"
certificate_file = "/private/assist/replica.pem"
private_key_file = "/private/assist/replica.key"
client_ca_file = "/private/assist/clients-ca.pem"
upstream_peer = "donor"
start_assisted = true
automatic = true
enter_after_blocks = 3
enter_after_checks = 3
cooldown_seconds = 30

[[assist.peers]]
id = "donor"
certificate_sha256 = "REPLACE_WITH_64_HEX_DIGITS"
allow_receive = true
endpoint = "https://donor.example:51054"
tls_domain = "donor.example"
server_ca_file = "/private/assist/donor-server-ca.pem"
```

Either side may dial; exactly one side requests the donor role after TLS
authentication. Roles do not depend on who opened the connection. Mutual donor
cycles and unauthorized direction changes are rejected. No public unauthenticated
transport is provided. The separate endpoint leaves wallet/Firehose clients
compatible.

`start_assisted` seeds a new receiver cursor only. Restarts preserve its durable
mode and ACK position. In automatic mode, repeated lag above the configured
threshold enters assisted mode after cooldown. Receiving the complete donor
solid barrier hands the single writer back to normal block validation for the
next block. A donor outage leaves an assisted receiver waiting; it does not
silently switch to an unverified or incomplete state.

## Block and event durability

Each protobuf bundle contains chain/profile identity, height and parent ID,
the ordered store changes (key and new value), original raw block, transaction
receipts, complete Firehose facts (including VM logs, internal transactions and
native TRX balance changes), one hash of the replaced values and a chained
SHA256 digest.

The values a block replaces are not transmitted. `before_digest` is one XXH64
over them, taken in the order of the changes: the number of changes, then for
each one its store, its length-prefixed key and either an "absent" flag or a
"present" flag with the length-prefixed value. The receiver reads the same keys
from its own state, computes the same hash and refuses the block with
`STATE_PREIMAGE_MISMATCH` when it differs. The values it has just read become
its undo record, so undo data is not transmitted either. A refused block also
raises the critical event `node_cluster_state_mismatch` (for the first such
block, then at most once in ten minutes): the node keeps validating on its own,
but two members with different state need attention. A node's own journal
records keep the replaced values next to the changes (`preimages`): they restore
the undo record after a crash and are stripped again on export. The block digest
is taken over the transmitted form and is the same with or without them.

Journal records are packed on disk according to `compression`: `none`, `low`
(fast, the default) or `high` (strongest, slower). Records of any level stay
readable after the setting changes.

The donor exports only canonical solid records. Candidates from uncommitted
or abandoned branches are never exported. A failed pre-commit journal write
prevents the corresponding state commit. Bounded journal exhaustion is an
explicit error, not permission to omit writes.

The receiver fsyncs its prepared redo record, writes and syncs every touched
store plus block/receipt/undo data, appends and syncs its own Firehose entry,
then atomically persists its receiver cursor. Only then does it ACK. An
unfinished import is replayed at startup before network/RPC exposure. Repeated
delivery cannot append a duplicate APPLY. The donor's event sequence is never
reused; the receiver owns a continuous sequence through imported and normal
execution.

An indexer consuming this receiver uses the ordinary local Firehose and its
own history ACK. That ACK is independent of the receiver's upstream replication
ACK. Indexer lag cannot hold the donor's journal; the receiver's local history
policy retains its own downstream data.

The receiver first probes a common canonical anchor. It may durably unwind a
reversible suffix and emits local UNWIND before replacement APPLY entries.
Disagreement at/below local solid finality stops synchronization. Missing undo,
wrong preimages, incompatible schema, corrupt payloads and unavailable history
are explicit failures requiring reconciliation, never bypasses.

## Retention and operation

Journal files live under `<data_dir>/assisted-sync/`: `donor.json`, immutable
`blocks/*.pb`, independent `acks/<peer>.pb`, `receiver.json`, and transient
prepared import/rewind records. They are runtime data, not repository artifacts.

Required recipients hold the retention floor at their slowest persisted ACK.
A reserve of `retain_blocks` remains even after fast ACKs. Removing a required
recipient is an operator decision. Never remove journals or cursors to repair
an unknown outcome; reconcile pending records and canonical state first.

For a same-host bootstrap pilot, use an empty receiver data directory with
separate ports and its own authorized identity. The donor remains live during
checkpoint creation and transfer. Verify checksum inventory, actual genesis/head,
post-snapshot event continuity, balances, upstream ACK, restart recovery and
autonomous handoff. Existing manual offline snapshots remain supported; when
using them, hardlink only immutable SSTs and copy mutable metadata/event logs.


## Re-entry after autonomous execution

An independently executed receipt may contain cache-dependent no-op captures,
and its earlier journal checksum chain can differ without different state.
When re-entering assisted mode, Fetch carries an explicit logical anchor proof:
the canonical block, effective before/after changes, receipts and events,
excluding no-op changes and the previous checksum. The donor validates that
proof before exporting. The receiver checks every incoming preimage before
durably binding its parent to the donor chain. Original local receipts remain
preserved when an equivalent donor receipt replaces their active journal path.
Local-execution ACKs use the same checked proof; no equality by block ID alone
is used to release required history.

## Persistent processing statistics

Assisted nodes persist one node-local JSON value in the common database under
assisted-sync/processing-statistics/v1 after successful local commits, imports
and mode changes. It is excluded from snapshots and replication. Counts start
at feature activation, with separate local_blocks, imported_blocks and
mode_switches, plus current mode and activation height/time. Restarts preserve
counts; duplicate import delivery and last-block recovery do not inflate them.
These are processing counters, not current canonical chain length: reorg
re-execution may count replacement work at a previously observed height.

Prometheus exposes tron_node_processing_local_blocks_total,
tron_node_processing_imported_blocks_total, tron_node_processing_mode_switches_total,
tron_node_processing_assisted, activation/update timestamps and persistence
errors. Mode is 1 for assisted imports and 0 for autonomous execution.
The four-service owner monitor checks each node and its own indexer, persists
both lifetime and interval counts, and detects new application errors as well
as lifecycle, lag, ACK, state/balance and statistics failures. Healthy scheduled
checks stay quiet; use the existing four-hour task rather than another timer.


## Trusted cluster mode

Set trusted_cluster = true on every member to remove fixed donor/receiver roles.
Each mTLS-authenticated member can supply and receive finalized block bundles over
the same connection, regardless of who dialed. Two or more configured members may
be connected. Cluster mode must match on both ends; it cannot interoperate with
legacy fixed-role sessions.

~~~toml
[assist]
enable = true
trusted_cluster = true
node_id = "tron-a"
listen = "127.0.0.1:51054"
certificate_file = "/private/cluster/a.pem"
private_key_file = "/private/cluster/a.key"
client_ca_file = "/private/cluster/clients-ca.pem"
automatic = true
enter_after_blocks = 3
enter_after_checks = 3
cooldown_seconds = 30
retain_blocks = 10000
compression = "low"

[[assist.peers]]
id = "tron-b"
certificate_sha256 = "REPLACE_WITH_64_HEX_DIGITS"
endpoint = "https://b.example:51054"
tls_domain = "b.example"
server_ca_file = "/private/cluster/servers-ca.pem"
~~~

Configure B with its own identity and A's certificate pin. Add more peer entries
for a third member. An endpoint may be omitted for an inbound-only member.
Do not set upstream_peer. The legacy allow_donate, allow_receive and
required_for_retention fields are unnecessary and have no role in cluster mode.
Keep peer IDs, the private CA, certificate pins and network/genesis/profile
identity consistent; an ordinary public P2P peer cannot use this endpoint.

The runtime selects a fresh eligible peer by its solid height, with source
stickiness to avoid unnecessary switching. One source lease owns the shared
state writer. Old in-flight responses cannot write after an election change.
Only solid canonical bundles are supplied, with the existing anchor, checksum,
preimage and durable redo checks. Reaching the source's solid barrier returns
the node to independent validation. Losing that source permits another trusted
member to take over; with no eligible member, ordinary P2P validation can continue.
This is trusted execution, not a new consensus or quorum protocol.

There are no inter-node replication ACK messages or per-peer retention cursors.
Old legacy ACK files are preserved but do not pin the journal. The recent window
is counted in solidified blocks only: `retain_blocks`, 10,000 by default, about
eight hours at a three-second cadence. `max_journal_bytes` has no effect in cluster
mode. The window is cut in whole portions of the node's shared
`[index.firehose.history] batch_blocks` (1,000 by default): nothing is removed
until a full portion is due, so up to `retain_blocks + batch_blocks` solidified
records are stored. Records above the solid bound — received or produced but
not finalized yet — are outside the window; they are never refused or discarded,
so a member that is far behind can temporarily hold more. A disconnected member
or an unfinished snapshot cannot retain history outside the window. A member
names the block it continues from; a request below the window is answered with
an explicit history gap, which requires ordinary block execution or a new
snapshot. Blocks are never skipped.

`retain_blocks = 0` makes a member a receiver only. It records nothing for its
partners, so its own block execution pays nothing for the cluster, and it can
only take blocks: it announces no history and answers a request with a history
gap. Having no records of its own, it names the block it continues from without
a digest; every block it takes is still proved against its state by the hash of
replaced values, and consecutive imports are chained through its cursor. Use it
for a slower member next to one that keeps the window.

A journal that cannot continue from the node's current head — written by another
record format, or interrupted while the member recorded nothing — is dropped at
start and recording begins again from that head. The window is a cache of recent
changes, not a ledger.

An empty node can bootstrap from any configured member and resume its durable
snapshot download after restart. A source change restarts an incomplete download
with that source's snapshot, without mixing manifests. There is at most one active
checkpoint per supplier; an idle five-minute lease releases its temporary files.
Snapshot installed/release control responses are lifecycle messages, not journal
retention ACKs. Already installed verified state can start without the supplier.
A populated database is never overwritten automatically. During a long snapshot
transfer its starting point can leave the bounded journal window; increase the
window for the expected transfer duration or explicitly request a fresh snapshot.

Each member continues to publish its own complete Firehose events and accepts
its own indexer's independent ACK. Those local indexer ACKs are unchanged.
Persistent per-node counters include source_switches and current source_peer.
Metrics expose tron_node_processing_source_switches_total,
tron_node_processing_source_info{peer="..."},
tron_node_cluster_known_peers and tron_node_cluster_peer_connections.
The four-hour owner check records source changes and alerts when no configured
trusted peer is connected, including while ordinary execution is healthy.
