# Retaining peers with newer blocks

A connection advertising a block height above our current applied head is retained
for five minutes without useful block delivery. Evidence comes from the application's
Hello, live block inventory, or correctly sized block IDs in a sync inventory.
This is availability evidence; normal block and chain validation still decides
whether received data may be applied.

The grace period suppresses local no-progress and slow-fetch reconnects, resilience
rotation, the short inbound-silence deadline, and premature dead-end rotation.
It leaves the existing fetch window and in-flight request history intact: resetting
them on an open connection could repeat a request forbidden by the peer protocol.
Other peers can continue fetching and taking leadership normally.

A newly received, requested block body above the applied head renews the five minutes.
Duplicate or old blocks, pings and inventory announcements do not renew it. In the
single-applier path a canonical accepted block renews it. Advancing our head through
a different peer does not renew this connection's timer. Once we reach the peer's
advertised height, the ahead-peer protection no longer applies. At the exact
five-minute idle boundary the existing disconnect policies are eligible again.

Shutdown, explicit block caps, transport EOF/errors and protocol/chain failures
remain effective throughout the grace period.

Regression tests in `crates/tron-node/src/sync_peer_retention.rs` exercise the exact
deadline, renewal by new bodies, duplicate/announcement non-renewal, implicit Hello
acceptance, catching up, and progress delivered by a different peer. Run:

~~~sh
cargo test --release --locked -p tron-node --lib --test sync -- --test-threads=2
~~~
