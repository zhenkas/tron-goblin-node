# KV map footprint estimator

Standalone tron-chainbase examples estimate an in-memory KV mirror.
The exporter samples approximately the first 1% of key/value pairs in every
physical RocksDB store. After it exits, independent file-only loaders compare
flat key-size tables and intrusive pooled nodes. These examples do not install
a node backend.

## Build and run

From the repository root:

~~~sh
cargo test --release -p tron-chainbase --example kv_map_estimate --example kv_map_load
cargo build --release -p tron-chainbase --example kv_map_estimate --example kv_map_load
~~~

First export **as the non-root database owner**, to new writable locations
outside the primary data directory. The primary may remain running:

~~~sh
target/release/examples/kv_map_estimate \
  --source /path/to/tron-mainnet/data \
  --work-dir /path/to/benchmark/export-one-percent \
  --export-file /path/to/benchmark/sample.kv \
  --basis-points 100 \
  --large-cutoff 4096
~~~

Wait for that process to finish, then run the independent loader:

~~~sh
target/release/examples/kv_map_load \
  --input /path/to/benchmark/sample.kv \
  --work-dir /path/to/benchmark/file-load-one-percent \
  --cells-per-pool 1000000 \
  --large-cutoff 4096 \
  --project-shards 100
~~~

The exporter refuses root, mismatched database owners, source/output overlap,
and reuse of an existing work directory or export file. It opens stores as
RocksDB secondaries, placing logs and metadata outside the primary. The option
max_open_files=-1 keeps SST handles valid across primary compactions.
It never constructs the measured map. Its sample.kv contains actual KV data;
keep it with private runtime data, outside Git.

The loader neither imports RocksDB nor opens any database. It streams sample.kv
through bounded reusable buffers. Before creating the map it records a memory
baseline; after closing the input buffers and verifying all retained bytes it
records memory again. report.json includes **after-minus-before** RSS, PSS,
anonymous memory, virtual size and swap, plus both original measurements.
This avoids attributing retained native database allocations to the map.
Small reporting histograms are included in the measured process delta.

## Fixed-step pools with a 256-byte ceiling

To keep inline values through 12 bytes, use pool classes 16, 32, 48 ... 256,
and allocate every larger value individually:

~~~sh
kv_map_load --input sample.kv --work-dir step16-run \
  --value-allocator pooled --pool-step 16 --large-cutoff 256
~~~

Each pool still reserves one million cells by default. References, shared
value pools, available/full lists, free-cell reuse and empty-pool reclamation
are unchanged. The step rounds upward, so 33..48 bytes use the 48-byte class;
256 bytes remain pooled, while 257 bytes use ordinary allocation.

--pool-step 0 retains power-of-two classes. Positive steps must be powers of
two, at least 16, and divide the cutoff. The cutoff remains a power of two.
Reports include pool_step_bytes (0 means power-of-two classes), and per-store
histograms and projections use the selected class geometry.

## Inline-key table comparison

Add --key-storage inline to store each original fixed-width key directly beside
the 16-byte ValueRef in hashbrown's flat bucket array. The 64-bit lookup hash
is used for probing and recomputed from original bytes during rehash; it is
not retained in these entries. Full key bytes and the u16 store namespace
still resolve collisions. No key-length field is stored in an entry.

~~~sh
kv_map_load --input sample.kv --work-dir inline-key-run \
  --key-storage inline --value-allocator pooled --pool-step 16 --large-cutoff 256
~~~

There is a specialized table for every observed key width from 0 through 64
bytes. Wider keys explicitly fall back to the original arena representation.
The report identifies each table's storage and actual entry size, the number
of inline key bytes, fallback arena bytes and retained full-hash bytes.
For example, 21-, 28- and 32-byte keys produce 40-, 48- and 52-byte entries on
the measured target, including namespace, ValueRef and alignment padding.
Values through 12 bytes reside in those same entries; larger payloads remain
in shared pools or independent allocations. Original keys are never replaced
by truncated hashes.

--key-storage arena retains the original 32-byte entry with a cached lookup
hash and KeyRef, for a controlled baseline. Both variants use hashbrown 0.15.5,
separate tables by key width and identical value allocation/verification.
Inline keys eliminate the external key arena for supported widths, but also
occupy unused bucket capacity. Consequently locality does not guarantee a
smaller total footprint. Reports measure the result instead of assuming it.
Pointers into buckets must not escape a mutation: table growth moves entries.
Neither mode adds production concurrency or changes running node services.

The flat-table design stores key/value records directly in bucket storage:
see [hashbrown's explicit-hash table API](https://github.com/rust-lang/hashbrown/blob/v0.15.5/src/table.rs)
and the comparable [Abseil flat-container layout](https://abseil.io/docs/cpp/guides/container#flat_hash_map-and-flat_hash_set).

## Ordinary allocation comparison

The loader also accepts --value-allocator system. Values of length 0..12 remain
inline. Every larger value receives an independent Box<[u8]> from Rust's default
System allocator (glibc malloc/free on the measured Linux GNU host), with no
value pools. Pointer/index/length references remain 16 bytes.

Run pooled and system variants in separate processes against the same retained
sample, using different new work directories:

~~~sh
kv_map_load --input sample.kv --work-dir pooled-run --value-allocator pooled
kv_map_load --input sample.kv --work-dir system-run --value-allocator system
~~~

Both modes use the same key-size tables, namespace handling, input buffering,
verification and before/after RSS measurement. Reports identify the mode and
count standalone allocations and requested bytes. On Linux GNU they also sum
malloc_usable_size for retained standalone allocations. Usable capacity excludes
some allocator headers and arena fragmentation; total RSS delta remains the
footprint measurement. The pool-specific full-layout projection is null in
system mode, because it does not model ordinary allocation overhead.

## Intrusive combined-node comparison

The separate kv_node_load example uses bucket chains and pooled nodes.
It does not change the node's production KV provider.

~~~sh
cargo test --release -p tron-chainbase --example kv_node_load
cargo build --release -p tron-chainbase --example kv_node_load
target/release/examples/kv_node_load --input sample.kv --work-dir node-run \
  --large-cutoff 256 --pool-block-bytes 65536
~~~

Each table belongs to a (store namespace, key width) pair. This metadata is not
repeated in nodes. A bucket is one native pointer. Nodes contain exactly:

~~~text
[next pointer: 8 bytes][actual value length: u32][original key][actual value]
~~~

There is no retained hash, separate key allocation, ValueRef, pool pointer,
cell index, key-length field or allocation-kind flag in a node. Headers use
unaligned accesses so no per-cell alignment padding is necessary. On the
measured 64-bit host the header is 12 bytes; actual key and value bytes follow.
Pointers remain stable while bucket arrays grow. Growth recomputes hashes,
relinks chains and releases only the old bucket array.

For pooled values, payload capacities 0..16 are exact; larger capacities round
upward in steps of 16 bytes. A cell's stride is 12 + key width + value capacity.
Free lists are global by total stride, shared across namespaces and key widths:
for example, a 21-byte key with an 8-byte value and a 20-byte key with a 9-byte
value can reuse the same 41-byte cell. A free cell stores its successor in its
existing next-pointer field. Allocating/releasing a cell requires expected O(1)
size-class lookup and constant-time list operations. Appending a new block
initializes its free chain once.

Blocks target 64 KiB by default, holding floor(target / stride) cells, with at
least one cell. Their addresses do not change. Empty blocks are retained until
map/process destruction, including after every live node has been removed.
This is a deliberate high-water allocation contract; churn can leave more
retained memory than a fresh-load estimate shows.

The fixed --large-cutoff applies to actual value length, preserving the prior
256-byte comparison boundary by default. Larger values receive one ordinary
allocation for the entire header+key+value node, never a separate value buffer.
Allocation kind is derivable from value_size and the fixed cutoff; negative
lengths and flags are unnecessary. Ordinary allocations are freed when their
nodes are replaced/deleted. Pool blocks follow the retention contract above.

The loader streams the retained sample, verifies its count/footer checksum,
then opens a bounded second stream to compare every final lookup byte-for-byte
after all bucket growth. It measures RSS before map creation and after both
streams close, before destroying the map. Reports separate pointer arrays,
live headers/payload, pool class padding, unused cells, block allocations and
ordinary node allocations. The full-size projection is an approximate initial
live layout: it excludes future retained freed cells, allocator overhead and
pool tails, and is not a random-access performance result.

Unit tests cover unaligned strides, hash collisions, stable pointers through
rehash, deletion from chains, reuse across namespaces/key widths, retained
empty blocks and replacement across pool/ordinary allocation boundaries.
This experiment remains single-threaded; the planned production locks below
are still future work.

### Keep bucket pointers resident while pools can swap

The optional --lock-buckets flag places each bucket array in a dedicated,
page-aligned anonymous mmap and applies mlock to that mapping. Nodes and
object-pool blocks keep using ordinary pageable memory. Dedicated mappings
prevent bucket locks from pinning unrelated allocator pages.

~~~sh
# The launcher must grant enough RLIMIT_MEMLOCK before dropping privileges.
target/release/examples/kv_node_load --input sample.kv --work-dir locked-run \
  --large-cutoff 256 --pool-block-bytes 65536 --lock-buckets
~~~

Linux mlock keeps successfully locked pages resident until munlock/unmap;
see the [Linux memory-locking API](https://man7.org/linux/man-pages/man2/mlock.2.html).
The default unprivileged memlock limit may be too small for the arrays. Raise
only the benchmark process's limit, including room for old and new arrays
during resize. The experiment fails clearly on mmap/mlock errors instead of
silently continuing with swappable buckets. Growth locks the new pointer
mapping before relinking and unmaps the old one afterward.

Reports identify bucket payload, mapped allocation and locked bytes, and include
kernel VmLck/Locked measurements. In locked mode the loader verifies VmLck equals
the sum of dedicated bucket mappings. Locked bytes still count toward the
cgroup RAM limit; they reduce RAM available for pools. Full-network projections
are estimates, and all bucket pointers must fit in the intended RAM budget.
The option is disabled by default and does not alter production services.

### Progress and read throughput under swapping

The intrusive loader flushes one JSON output line at each completed percent
for load, verify_read and random_read. Each line has completed/total operations,
elapsed seconds, interval operations/s and average operations/s. The exact
denominator comes from the retained file footer and is checked again while
decoding; it is not a RocksDB cardinality estimate.

--random-reads 100000 (default) selects one key per equal record-count stratum
with reproducible random offsets, then shuffles those queries once. The timed
phase only calls get and consumes/checks the entire returned value, with no
file reads inside it. Selection storage is bounded by the requested count and
freed before the final memory measurement. It follows full exact verification,
so it is not guaranteed cold. Its rate includes value digest checking and
progress output, remains single-threaded, and is not a block-processing rate.
The report shows whether it reaches the experimental target of 3000 reads/s.

The load and verify_read rates include streaming file decoding and accounting
or byte comparison respectively. They are reported separately. Every final KV
pair is still compared byte-for-byte; the redundant random-order checksum walk
was removed. Ordinary allocation usable bytes are maintained on allocation/free,
so footprint reporting no longer walks all nodes just to recover this counter.

For a bounded swap experiment add --exit-after-report. After successful exact
verification, random reads, footprint capture and report writing, it leaves map
reclamation to process teardown. This omits an otherwise expensive final random
node walk unrelated to lookup throughput. Normal map destruction and its unit
tests remain unchanged. This option affects only the standalone benchmark.

### Direct RocksDB point-read baseline

kv_rocks_read reads the same sample file and reuses the exact stratified
selection and deterministic shuffle from kv_node_load. It queries only those
100000 keys in one thread, without building a KV map. It opens the queried
stores read-only, as their non-root owner, with logs outside the data tree.
Keep the node stopped while running this tool.

~~~sh
cargo test --release -p tron-chainbase --example kv_node_load --example kv_rocks_read
cargo build --release -p tron-chainbase --example kv_rocks_read
target/release/examples/kv_rocks_read --source /path/to/data --input sample.kv \
  --work-dir new-rocks-read-run --block-cache-mb 4096 \
  --write-buffer-mb 16 --max-open-files 128 --random-reads 100000
~~~

Use the node's current resource limits and storage settings when requested;
a result under 16/20 GiB RAM and a shared 4 GiB cache is not a memory-matched
comparison with the 500 MiB / 1 GiB swapped-map experiment. The tool uses the
node's shared HyperClock block-cache profile, bloom and index/filter pinning,
and the same owned-value DB.get point-read API. Background write/compaction
pools are irrelevant to this read-only benchmark.

File decoding, key selection and opening the DBs are reported separately
from the timed reads. Each timed lookup consumes the entire returned value
and updates progress every percent. Block cache starts in a fresh process;
the OS page cache remains intact, so this is not a guaranteed cold-disk test.
The report separately counts exact values, missing keys and changed values:
a node restarted after the original export may have changed source versions.
Missing reads must not silently be presented as successful reads. Read errors
fail the run. Source data is never written by this tool.

### Ten concurrent readers and raw 64-byte file reads

Both kv_node_load and kv_rocks_read accept --threads 10. The exact same
shuffled 100000 queries are split into ten contiguous slices of 10000,
without work stealing. All workers wait at a common barrier; preparation
and thread creation are outside the final timer, which stops at the last
worker's completion. Reports show every partition's range/count and elapsed
time, aggregate throughput and process I/O deltas. Progress remains per 1%.
The callback does not take a shared map lock or increment a global counter
on every lookup; workers send progress only in batches.

The intrusive map exposes a narrowly scoped Frozen read view. Its immutable
borrow excludes mutation, free/reuse, rehash and destruction for the entire
parallel phase. Only that view is Sync, with its safety argument at the
implementation; the mutable Map does not gain Send/Sync. This is a read-only
benchmark and does not implement the future production reader/writer locks.
RocksDB readers share the opened read-only DB handles and the node cache.

kv_file_read independently reads 100000 deterministic random byte offsets
from the retained sample file, requesting exactly 64 bytes per operation.
Both thread counts regenerate identical offsets and report an order fingerprint
and consumed-byte checksum. A single shared descriptor uses positioned pread
(read_exact_at), without changing or locking a shared file cursor.

~~~sh
target/release/examples/kv_file_read --input sample.kv --work-dir file1 \
  --threads 1 --reads 100000 --evict-file-cache
target/release/examples/kv_file_read --input sample.kv --work-dir file10 \
  --threads 10 --reads 100000 --evict-file-cache
~~~

FADV_RANDOM suppresses sequential read hints. The optional eviction flag applies
DONTNEED only to this private benchmark file, never a global cache drop or any
primary database. mincore on a non-faulting mapping records actual resident-page
fractions before/after the advisory eviction and after reads. Check those values
rather than assuming every page was evicted. Unaligned 64-byte ranges can cross
OS page boundaries; storage I/O granularity is not necessarily 64 bytes.
The report separates requested payload bytes from process read_bytes counters.
File preparation, advice and residency measurement are outside the read timer.
Run benchmark variants sequentially so their disk I/O does not compete.

## Sample format and consistency

The framed binary file contains a versioned metadata header, namespace ID,
key/value lengths and the original bytes for each pair. A footer verifies
record count and an additive byte checksum. Truncation, invalid lengths,
out-of-range namespaces and trailing bytes are rejected. Export uses a new
partial file, flushes and syncs it, then renames it. A SHA-256 of the finished
file can additionally identify the exact retained sample.

Discovery includes state and auxiliary physical RocksDB stores under the
supplied root. Each store's quota is ceil(estimated_count * basis_points / 10000).
The estimate comes from rocksdb.estimate-num-keys; small nonempty stores are
sampled at least once. This property samples SST statistics and can be badly
biased by uneven files, overwrites or tombstones. For repeatable quotas,
--counts-file can provide a JSON object mapping physical store names such as
database/account to reference counts from a previous full verified scan.
Reports retain both the supplied count and the raw RocksDB estimate with
their count source. An earlier exact count is still approximate for today's
running database; it does not establish exact current cardinality.
Extrapolation weights each store by estimated count /
actual sample count, so one row from a tiny store is not multiplied into 100.

Store views are opened sequentially while the primary runs. The export is
**not a cross-store-consistent recovery checkpoint**. Counts are estimates,
and lexicographic prefixes can bias value-size distributions. The exporter
and loader retain per-store counts and length histograms for interpretation.

## Layout

* In the default arena mode, each observed key length has its own hashbrown HashTable, keyed by
  **(store namespace, full key)**. Length belongs to the table metadata and is
  absent from individual entries and key bytes. A 32-byte entry holds a 64-bit
  lookup hash, an 8-byte key-arena reference and a 16-byte value reference.
  The key reference contains a u16 chunk ID, u16 namespace and u32 offset.
  Raw key bytes occupy shared 16 MiB chunks with **no per-key header**.
  The arena supports up to 65536 chunks (about 1 TiB at the default chunk size).
  Value pools are shared across all key-size tables.
* A value reference is **16 bytes on the measured 64-bit target**: a 12-byte
  union plus u32 actual length. Values of length 0..12 live directly in that
  union, requiring neither a pool nor a standalone allocation. Length is also
  the discriminator; the inline bytes are never interpreted as a pointer.
  For larger values the union holds a pointer and signed 32-bit cell index.
  Index -1 denotes a standalone allocation; nonnegative indices refer to cells
  in a stable boxed pool. A packed external layout avoids alignment padding
  inside the union; pointer/index fields are copied by value.
* Values of length 13 and above use power-of-two size classes 16, 32, 64 ...
  through the configured cutoff, inclusive: for example, 33..64 bytes use
  64-byte cells. Each pool reserves one million cells by default. Values above
  the cutoff use normal allocations. Reports distinguish inline payload,
  actual pooled bytes, class padding, unused capacity and populated page span.
* Each size class has intrusive doubly linked available/full pool lists.
  A freed cell stores the next free cell's integer index in its former payload.
  Allocation, release and list transitions take O(1) bookkeeping without scans
  or separate free-list nodes. A live-cell counter reaching zero unlinks and
  drops the whole empty pool, including its payload buffer. Pool IDs are
  recycled through a reserved free-ID list; live pool addresses stay stable.
* Known block/transaction digest keys reuse 64 existing digest bits, with full
  original-key and namespace comparison on collision. Numeric/property keys,
  addresses and composite storage keys use a small noncryptographic word mixer.
  A storage-row key can have a raw slot suffix; treating all 32-byte keys as
  random hashes would produce poor distribution.

Keys are variable length across physical stores: the measured account keys are
21 bytes, account-asset keys 28 bytes, block/transaction keys 32 bytes, and
storage-row keys include 32- and 48-byte forms. Full bytes are retained.

The key arena measures initial loading and does not reclaim deleted key bytes.
This experiment is not a complete long-running production KV implementation.

## Interpretation

On Linux, untouched zero-initialized pool capacity may remain uncommitted:
reserved virtual space is not automatically resident RAM or swap.

Full-size projections use one table per key length and optionally divide
each key-length class into evenly sized shards. Hash-table capacities round
to powers of two, so a small count error near a boundary may change the table
estimate sharply. Shared value pools and independent per-shard pools are
distinguished; a million cells of every value size in every shard can waste
substantial virtual space.

Occupied-byte projections exclude allocator rounding, node overlays, journals
and long-running fragmentation. They are approximate initial-layout estimates,
not random-read performance measurements. The report is measured before
destroying the map; unit tests separately verify pool release and slot/ID reuse.


## Planned concurrent mirror (future work)

User direction: one hundred fixed shards **per key length**, selected
deterministically from well-distributed key hash bits (reuse digest bits when
the key is already hashed; mix structured keys). Value pools remain shared,
with as many pools as needed for each size class.

Each map shard gets its own read/write lock: shared for reads, exclusive for
inserts, updates and deletes. Each pool also gets its own read/write lock,
allowing simultaneous readers even if one faults a value page back from swap.
The current estimator stays single-threaded; this lock design is recorded for
the later production mirror and is not implemented by these examples.

Lifetime and lock ordering must be explicit. A reader holds the map's shared
guard while locating the ValueRef and acquiring the pool's shared guard;
only then may it release the map guard and read/copy the pooled bytes.
Inline values are copied while holding the map guard. Standalone values
also need their allocation kept alive until the read finishes.
Writers follow the same map-then-pool order and take exclusive guards before
reusing a cell or dropping an empty pool. An empty pool is reclaimed only
after its last live reference and all in-flight readers are gone.
Available/full lists and the per-size allocation registry need coordinated
mutation, without placing a global lock on ordinary reads.

The optional `kv_node_load --skip-full-verification` mode loads all records and validates the full input footer/count/checksum, then goes directly to the requested sampled random reads. It avoids a separate lookup sweep over every record. The sampled reads still consume the entire value and check its length and digest. Reports explicitly set `full_verification_performed=false`, `all_pairs_verified=false`, `final_exact_lookup_count=0`, and a null full-sweep rate; they report the sampled verified count separately. This changes cache preparation compared with the default full sweep, so report it alongside timings and do not call the two modes identical cold/warm tests. The default retains the full byte-for-byte lookup sweep.
