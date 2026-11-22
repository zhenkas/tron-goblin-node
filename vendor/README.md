# Pinned native dependencies

tidesdb/ vendors the published source subset of tidesdb-rs commit
0b4eca75a3f96799a8cd0c63fd12e62662ffbaa4 and its pinned native TidesDB 10.2.1.
Upstream: https://github.com/tidesdb/tidesdb-rs
Both upstream license files and package attribution are retained.

Local change: CMake sets XXH_NAMESPACE=tron_tidesdb_ for the native target.
This renames definitions and references together, keeping the bundled xxHash
private beside RocksDB/lz4-sys. Hash algorithms, checksums, data format and
execution behavior are unchanged. No duplicate-symbol linker suppression is used.
The root Cargo patch applies to both the production provider and benchmark feature.


Additional opt-in local change: num_compaction_threads = 0 disables the native
background compaction pool and ticker (including background VLog GC); negative
values retain native default resolution. Existing nonzero/default behavior is
unchanged. Wake/drain paths already tolerate absent scheduler/queue, as exercised
by the bulk-import benchmark. This is for short isolated trials, not an indefinite
space-reclamation policy. Native source directories are tracked by build.rs.
Two upstream CMake trailing spaces were removed.
