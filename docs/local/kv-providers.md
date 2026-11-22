# KV providers: RocksDB and TidesDB

The runtime selects a provider once while opening each store. Execution, RPC,
snapshot overlays, the history index, archive, state commitment and pruning
metadata keep using the existing `Arc<dyn KvBackend>` interface. No backend
selection branch is added to individual RocksDB reads or writes.

Existing configuration files select RocksDB. Use a separate, consistently
converted data directory when selecting TidesDB:

```toml
data_dir = "/path/to/complete-tidesdb-node-data"

[storage]
backend = "tidesdb"                  # "rocksdb" remains the default

[storage.tidesdb]
write_buffer_size_mb = 64
block_cache_mb = 8
max_open_sstables = 8192
flush_threads = 1
compaction_threads = 1             # 0: isolated trial without background compaction
compression = "none"                # none, snappy, lz4, zstd if compiled in
bloom_fpr = 0.01
wal_sync = "buffered"               # buffered, full, interval
wal_sync_interval_ms = 1000
```

These TidesDB resources are **per store**, including cache and worker counts.
The process opens dozens of engines; account for their aggregate memory,
threads and file descriptors. TidesDB requires an open-file limit of at least
65,536; startup raises the soft limit where permitted and otherwise refuses
the open. The vendored native package fixes xxHash symbol isolation alongside RocksDB/lz4-sys.
See [native provenance](../../vendor/README.md). This implementation pins tidesdb-rs commit
`0b4eca75a3f96799a8cd0c63fd12e62662ffbaa4` (native TidesDB 10.2.1).

The existing top-level `storage.write_buffer_size_mb`, `max_open_files` and
`block_cache_mb` settings continue to tune RocksDB. They do not override the
separate TidesDB section. Invalid TidesDB settings fail before opening stores;
unknown backend names and unknown TidesDB option names are rejected.

CF compression, Bloom filters and the normal production compaction profile
are applied on creation and reopen. Changing compression affects new SSTs;
existing SSTs retain their own readable encoding. An execution-only bulk import
does not implicitly become a complete node database.

## Batch and durability contract

`multi_get(&[&[u8]])` preserves request order, duplicates, empty keys, empty values
and missing values. Backend errors propagate as errors. RocksDB uses its native
MultiGet. This pinned TidesDB API has no native MultiGet call: its implementation
shares one snapshot transaction across all reads. Sessions and snapshots resolve
their own updates/tombstones first, then forward unresolved keys as one parent
batch. Session undo capture also reads parent values in one batch.

`write_batch` applies all puts/deletes as one native transaction (TidesDB) or
WriteBatch (RocksDB), including the logical empty key. Writes remain visible
together. `write_batch_sync` and `sync_wal` provide an explicit durability barrier.
With `wal_sync = "buffered"`, ordinary commits do not request a WAL fsync.
`"full"` syncs ordinary commits; `"interval"` uses the configured interval.
Explicit durability barriers still sync in all three modes. Buffered mode
therefore does **not** mean that the entire node will never call fsync.

`checkpoint` flushes already committed engine state; it does not commit tentative
session/snapshot layers. Overlay commit/revoke semantics and the existing
cross-store checkpoint protocol remain unchanged.

Ordinary nonempty TidesDB keys/values remain byte-for-byte identical to chainbase.
The one logical empty key is stored in CF `_empty_key`, slot `value`; regular
keys live in CF `state`. This matches the benchmark converter's empty-key layout.
Prefix/range/backward scans preserve chainbase ordering and limits.

The market price store requires a special ordering codec because TidesDB does
not expose custom comparators. Its ratio ordering preserves equivalent-price
identity for protocol-valid positive quantities and the zero-price head entry.
A nonempty price store without its `.kv-key-order` marker is rejected. Ordinary
stores use direct keys. Market is currently disabled on the observed mainnet;
this adapter preserves the store compatibility surface.

## Data-directory and tool scope

Selecting an engine is not a migration command. Opening a directory belonging to
the other engine is rejected. The store opener also rejects persisted execution
state missing the block or block-index databases before creating absent stores
or stamping the chainbase schema. A complete migration needs the block/history
and any enabled index/archive/commitment/pruning datasets in the selected engine,
in addition to the execution stores. The 27-store execution benchmark conversion
alone is insufficient for starting the daemon.

Daemon startup, configured offline replay, dump-blocks and dump-state use
`[storage].backend`. Java snapshot import/export, raw RocksDB diagnostics and
legacy admin commands still work with the RocksDB format. They are not TidesDB
migration tools. Read/write provider changes require the daemon to be stopped.

## Validation and performance

`tron-chainbase/tests/kv_providers.rs` compares both providers for ordered batch
reads, missing/empty keys, atomic batches, native seeks, market ratio identity,
reopen and WAL recovery after a child exits without destructors.
`tron-node/tests/kv_storage.rs` exercises the full store bundle, checkpoint replay,
snapshot revoke and incomplete-dataset rejection. The daemon RPC smoke tests
run against both providers. Configuration tests preserve legacy defaults and
reject invalid TidesDB options.

For a reproducible adapter microbenchmark:

```sh
cargo test --release -p tron-chainbase -- --test-threads=1
cargo test --release -p tron-node --test kv_storage --test smoke -- --test-threads=1
cargo run --release -p tron-node --example kv_provider_bench -- /new/private/output-dir
```

The benchmark compares direct RocksDB, the RocksDB provider, and the TidesDB
provider with the same deterministic 32-byte keys and 128-byte values. It checks
all returned values, flushes setup writes and explicitly warms the data before
seven measured rounds. Results include raw point/batch timings and a separate
WAL-sync measurement. This isolates adapter overhead; it is **not** the
first-pass, cold-cache, large-mainnet-database test and cannot predict its speed.
Keep generated databases and raw measurements outside Git.

Observed on a development host in the initial adapter run (seven-round medians):

| Path | Point read | Batch read, per key (256 keys) |
| --- | ---: | ---: |
| Direct RocksDB | 0.794 us | 0.738 us |
| RocksDB through provider | 0.783 us | 0.714 us |
| TidesDB through provider | 1.837 us | 1.729 us |

All paths returned the same checked values/digest. The small direct/provider
difference does not establish a speedup; no RocksDB adapter slowdown was observed
in this run. TidesDB was slower for these warm reads. Cold large-database and
whole-block performance remain separate measurements.


## Starting an isolated node from a verified execution conversion

The state conversion includes account assets and the TAPOS block index, but not
block bodies. With the target stopped, the anchor helper fetches the exact stored
head through a read-only gRPC peer, verifies its block ID against both converted
properties and block-index, and only then persists that head block. It verifies the mainnet genesis and retains its block and index identity, which
TVM uses to derive CHAINID for signature domains. It also retains
65,536 recent block-index entries for validating transaction references and removes older
locator entries that the serving peer may already have pruned. No balance,
contract-storage or property head is invented or advanced.

Run with a fresh private report path:
    cargo run --release -p tron-node --example tidesdb_anchor -- DATA_DIR GRPC_URL REPORT_JSON

When no source daemon is running, replace GRPC_URL with
rocks:/path/to/sealed-rocks-data. The helper opens only the frozen block and
block-index stores in RocksDB read-only mode; it verifies the same head/genesis
IDs and does not copy or modify the large source database. Never point this mode
at a live primary.

Then use distinct listener ports, backend=tidesdb, index.enable=false, archive,
commitment and firehose disabled. One explicit P2P peer can be a local RocksDB
node. The normal node still keeps the blocks it subsequently applies, for its
validation/reorg machinery; this is not an archival-history import.

For a short ingestion experiment on bulk-import SSTs, compaction_threads=0
disables the background compaction pool, scheduler and VLog GC. The default
remains 1. Writes, checkpoints and reopen still work; indefinitely leaving it off
will accumulate unreclaimed files. Configured CF reopens otherwise retain the
production profile documented above. These controls do not disable WAL durability
barriers.


## First-pass TidesDB read settings

Build the isolated diagnostic with:

```sh
cargo build --release -p tron-node --features block-tides-bench --example tides_read_probe
target/release/examples/tides_read_probe prepare /stopped/tides-data/database /private/trace.json 2048
target/release/examples/tides_read_probe run /stopped/tides-data/database /private/trace.json /private/report.json 64 8192
```

Preparation chooses distinct persisted keys from account, storage-row and
account-asset, adds repeated properties, and records a deterministic synthetic
access trace. It does not replay a contract or a block. Run each profile in a
fresh process with the same trace, fixed request order and no warmup pass.
Compare the returned-value digest and trace digest across profiles.

The data directory must belong to a stopped experimental node. Run the probe
with the store owner UID and primary GID; it rejects mismatches before native
open, because newly created engine files must remain writable by the daemon.
Keep the private report directory writable by that same user. The native API
has no read-only open: the probe performs no application writes, but native
open/recovery/close may write engine files. It retains existing CF settings and
disables background compaction/GC for the experiment. Reports separate native
open, point-read and close I/O. Never run it against the live primary.

The final arguments are cache MiB and maximum open SSTables **per store**.
Test the memory budget as an aggregate across engines. A smaller open-file
budget can require repeated table/metadata opens; it is a separate variable
from the data cache. Compression, Bloom filters and SST layout changes affect
persisted/new tables and cannot be evaluated by pretending an existing dataset
was rebuilt.

For a first pass from nonresident files, close the prior probe, flush only the
stopped test files outside the measured interval, evict only its relevant
.klog/.vlog files with POSIX_FADV_DONTNEED and verify residency with mincore
before each fresh run. Do not drop machine-wide caches while other services run.
Document any remaining residency; a new process alone does not make files cold.
Opening the native engines can itself populate caches before point reads.

The report includes requested bytes (rchar), storage-accounted reads
(read_bytes), returned payload bytes, cache hits/misses and entry bytes, native
write counters, elapsed time and peak RSS. These describe different layers.
Requested reads may be served by the OS cache. If task IO accounting is absent,
the probe uses its own unified cgroup IO counters and leaves requested bytes
unavailable. Run it in a dedicated scope with no other active workload; never
substitute whole-machine counters. For a live syncing node, report
both MB/s and MB/applied block: a faster node can read more bytes per second
while reading fewer bytes per block. Consecutive live windows contain different
blocks and do not establish a matched-workload speedup.
