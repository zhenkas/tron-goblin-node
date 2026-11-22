# Persistent private mainnet-like sandbox

This profile is a separate ledger: one serial producer with 27 private SR
identities and two ordinary followers. Followers use the existing symmetric
mTLS trusted cluster, each with its own contract indexer and local ACK. The
witness/assist exclusion remains enforced.

Blocks use the ordinary 3000 ms wall-clock schedule and genuine witness signatures.
DPoS solidity uses the ordinary 70% latest-witness-block calculation. With all
27 witnesses producing, solid is approximately 18 blocks behind head after
warmup. Missed witnesses or maintenance can change that lag. All 27 keys are on
one machine: this reproduces protocol behavior, not independent fault domains.

## Build and prepare

Use the maintained Rust toolchain and Node24:

~~~sh
cargo build --release --locked -p tron-node --example private-node --features private-network
node scripts/create-private-profile.mjs --root /absolute/new/private-runtime --capture /absolute/mainnet-parameters.json --binary /absolute/private-node --port-base 19100
~~~

For a fresh testnet with a distinct network ID and complete wallet history
capture enabled before ledger initialization:

~~~sh
node scripts/create-private-profile.mjs --root /absolute/new/private-runtime --capture /absolute/mainnet-parameters.json --binary /absolute/private-node --port-base 19100 --private-network-id 2026111602 --capture-history true
~~~

The private-network-id flag sets both chain_id and p2p_network_id; it accepts only
canonical decimal IDs in 1000000000..2147483647. Without the flag, the existing
2026111501 default is retained. The capture-history flag accepts true or false
(default false); true enables native, TRC20 and contract-log indexing in all three
node configs. TRC721/internal capture remains disabled. Set this before init: changing
capture sets on an existing ledger can rebuild its derived index.

Public identity outputs are root/profile.json and root/identity.json; the role
configs are root/producer.toml, root/follower-a.toml and root/follower-b.toml.
accounts.json and witness-keys.json remain private 0600 files and are never printed.
The generator does not initialize a ledger or start services. Its pure option tests
run without generating keys or runtime state:

~~~sh
node --test scripts/private-profile-options.test.mjs
~~~

The capture JSON contains schema, source, observed_at, getchainparameters,
getnowblock, getblockbynum for genesis, and getaccountresource. The generator maps
supported official parameter names to canonical keys and preserves large SUN
integers exactly. Unknown active parameters or unsupported block versions fail.

Generated files are profile.json, identity.json, three node configs, separate
0600 witness-keys.json and 0600 accounts.json. Existing profile, account or ledger
paths are refused. Keep runtime files and keys outside Git. An edited immutable
profile defines a different ledger and requires a new data directory.

The manifest commits fees, flags, real resource backing, native timing,
allocations, 27 witness identities and capture provenance into genesis identity.
The producer key file must match all 27 witnesses exactly.

## Persistent TronLink read compatibility

Every newly generated private runtime includes wallet-gateway.json. Use it as
the gateway service configuration, or copy these fields when retaining an existing
gateway origin/port policy. It enables the narrow TronLink Multicall read adapter
and points it at follower-a/private-profile.json; the upstream port is follower A.
Initialize follower A before starting the gateway so this native marker exists.

The marker provides the expected genesis and immutable profile hash. The adapter
checks both against upstream block0 on every intercepted request. Fresh ledgers
regenerate the marker at the same path, so the installed service does not retain
an old genesis literal. Generic/public gateways remain disabled unless configured.

The adapter composes actual token balanceOf reads for the fixed wallet helper
address. It does not deploy code, change consensus, sign or broadcast transactions,
or solve TronLink's independent custom-network local-watchlist key mismatch.
See [wallet gateway](WALLET-GATEWAY.md) for the method boundary and error behavior.

## Initialize and launch

~~~sh
private-node init /absolute/new/private-runtime/producer.toml
private-node init /absolute/new/private-runtime/follower-a.toml
private-node start /absolute/new/private-runtime/producer.toml
private-node start /absolute/new/private-runtime/follower-a.toml
~~~

Configure the ordinary symmetric cluster on follower-a and follower-b with
separate IDs, mTLS identities, certificate pins, and exact assist.bootstrap_genesis
from identity.json. Enable native archive capture for watched pool contracts before
deployment. Empty follower-b may start directly and download the resumable live
checkpoint from follower-a. Existing fixed retention and local indexer ACK apply.

The transported private-profile marker and persisted profile hash survive
checkpoint bootstrap. Incompatible or public data is rejected before normal
startup. Init never overwrites an existing ledger.

Port groups are 19100, 19120 and 19140. Within each group HTTP is +0, JSON-RPC +1,
gRPC +2, metrics +3, P2P +4, ACK +5. Binds and peers are loopback; discovery is
disabled; both P2P handshake layers use a distinct private network ID. Remote
wallet access needs a separately configured authenticated TLS gateway.

## Economic and timing profile

Mainnet prices, TVM/governance flags, maintenance cadence and global resource
limits come from the capture. Global Energy/Bandwidth weights have actual
whole-TRX stake backing in a funded genesis sponsor account, applied through the
ordinary native Stake2 actuator. Later stakes update those same real weights.
A large funded treasury supplies ordinary signed transfers. No credit/mint RPC
is provided.

This is a point-in-time mainnet-like profile, not mainnet state. Contract dynamic
Energy factors depend on private workload and do not automatically reproduce a
mainnet contract's accumulated usage. Resource recovery retains ordinary protocol
time. Block and transaction timestamps are not shifted.

Only the dedicated private binary recognizes persisted native unfreeze delay
300 seconds. Official unfreezeDelayDays retains the captured value (for example
14); profile.json identifies the private override explicitly. Wallet-native
transactions and TVM calls use the same effective delay. Public builds ignore
the override and retain protocol days.

Native delay does not shorten Solidity mint/unstake cadence. A separately named
sandbox contract must use actual chain-time deadlines when these application
delays are shortened.

## Verification

The node regression applies real signed blocks from all 27 identities and checks
the actual solid pointer at every block. It then removes nine signers and proves
that the remaining 18 cannot advance a 70% quorum. Other checks reject unbacked
weights, public identity values, unknown ledger mutation keys and altered timing.
The devnet-fast harness remains a separate temporary test tool.

The private producer and ordinary peer drivers share one Khaos fork tree and
one apply lock. Echoes of already executed canonical block IDs are deduplicated
before fork choice; different hashes retain all normal fork/solidity checks.
The private config tip_poll_interval_ms defaults to 1000, bounded 250..5000.
It wakes idle block/transaction relays and tip refresh; ordinary public polling
intervals remain unchanged. The generator accepts --tip-poll-ms for this setting.
