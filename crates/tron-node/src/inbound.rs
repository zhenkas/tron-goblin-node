//! Inbound P2P server — lets other peers (java-tron deployments and our own
//! kind) **sync FROM us**.
//!
//! The rest of the node is an outbound-only client: it dials peers and pulls
//! blocks (`crate::sync::SyncDriver`). That makes us a leech — no peer can reach
//! us, so none can sync from us, and well-behaved networks deprioritise nodes
//! they can't connect to. This module closes that gap by binding the mainnet
//! P2P port, accepting inbound connections, completing the java-tron-compatible
//! handshake as the *responder*, and serving the sync protocol:
//!
//!   * `SyncBlockChain`  → `BlockChainInventory` (our ids past the shared block)
//!   * `FetchInvData`    → `Block` / `Trx` frames for the requested ids
//!   * `Libp2pKeepAlivePing` → `Libp2pKeepAlivePong` (so the peer keeps us alive)
//!
//! The serving logic itself is shared verbatim with the outbound dispatch loop
//! (`crate::sync::serve_sync_block_chain_ids` / `serve_tx_fetch_inv_data`), so
//! inbound and outbound answer identically. The handshake primitives
//! (`PeerConnection::libp2p_handshake` / `handshake`) are symmetric — both sides
//! send their Hello unconditionally — so the same functions the outbound dialer
//! uses against real java-tron work here for the responder direction.
//!
//! This task only ever SERVES an inbound peer; it never pulls blocks from it
//! (outbound dialers cover our own syncing). So it's deliberately simple and
//! fully isolated from the throughput-sensitive `run_against_peer` loop.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use prost::Message as _;
use tokio::net::TcpListener;
use tracing::{debug, info, warn};

use tron_chainbase::{BlockIndexStore, DynamicPropertiesStore, KvBackend};
use tron_mempool::TxMempool;
use tron_net::{
    Frame, HelloInputs, InboundByteBudget, Libp2pHelloInputs, MessageType, PeerConnection,
    MAINNET_P2P_VERSION,
};
use tron_proto::Endpoint;
use tron_types::BlockId;

use crate::sync::{
    chain_inventory_ends_below_announced_head, random_node_id, serve_sync_block_chain_ids,
    serve_tx_fetch_inv_data,
};

/// Mainnet libp2p network id (java-tron `Args.getNodeP2pVersion()` default).
const NETWORK_ID_MAINNET: i32 = 11_111;
/// libp2p protocol version we advertise (matches the outbound dialer).
const LIBP2P_VERSION: i32 = 2;
/// Send a keepalive ping at most this often, and treat the read loop's wakeup
/// cadence as this interval.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
/// Drop an inbound peer that has sent us nothing for this long.
const KEEPALIVE_INBOUND_DEADLINE: Duration = Duration::from_secs(120);

/// Shared, read-only serving context handed to every inbound peer task. Holds
/// Arc-cloned stores so accept-loop tasks serve concurrently without the
/// single-owner `SyncDriver`.
pub struct InboundServer {
    block_index: Option<Arc<dyn KvBackend>>,
    blocks: Arc<dyn KvBackend>,
    dyn_props: Arc<dyn KvBackend>,
    mempool: Option<Arc<TxMempool>>,
    genesis: BlockId,
    advertise_port: i32,
    /// IPv4 address named in our Hello endpoints (`p2p.advertise_host`).
    /// Never the listener's bind address: that is `0.0.0.0` by default, and
    /// java-tron's `NetUtil.validNode` refuses it.
    advertise_host: String,
    network_id: i32,
    metrics: Option<Arc<tron_rpc::Metrics>>,
    max_inbound: usize,
    inbound: AtomicUsize,
    /// Shared process-wide inbound-bytes budget (N-3). When set, every
    /// accepted peer's inbound frames draw from this pool along with the
    /// outbound dialers, capping total buffered bytes across all peers.
    inbound_budget: Option<InboundByteBudget>,
}

impl InboundServer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        block_index: Option<Arc<dyn KvBackend>>,
        blocks: Arc<dyn KvBackend>,
        dyn_props: Arc<dyn KvBackend>,
        mempool: Option<Arc<TxMempool>>,
        genesis: BlockId,
        advertise_port: i32,
        metrics: Option<Arc<tron_rpc::Metrics>>,
        max_inbound: usize,
    ) -> Self {
        Self {
            block_index,
            blocks,
            dyn_props,
            mempool,
            genesis,
            advertise_port,
            advertise_host: tron_net::DEFAULT_ADVERTISE_HOST.to_string(),
            network_id: MAINNET_P2P_VERSION,
            metrics,
            max_inbound: max_inbound.max(1),
            inbound: AtomicUsize::new(0),
            inbound_budget: None,
        }
    }

    #[cfg(feature = "private-network")]
    pub(crate) fn with_private_network(mut self, network_id: i32) -> Self {
        self.network_id = network_id;
        self
    }

    /// Attach the shared process-wide inbound-bytes budget (N-3). Use the
    /// SAME [`InboundByteBudget`] as the outbound dialers so the cap spans
    /// every connection.
    pub fn with_inbound_budget(mut self, budget: InboundByteBudget) -> Self {
        self.inbound_budget = Some(budget);
        self
    }

    /// Set the IPv4 address named in our Hello endpoints
    /// (`p2p.advertise_host`). An address java-tron's `NetUtil.validIpV4`
    /// would refuse (`0.0.0.0`, a hostname) is ignored and the default kept.
    pub fn with_advertise_host(mut self, host: &str) -> Self {
        if tron_net::is_valid_advertise_ipv4(host) {
            self.advertise_host = host.to_string();
        }
        self
    }

    fn head_number(&self) -> i64 {
        DynamicPropertiesStore::new(self.dyn_props.clone())
            .latest_block_header_number()
            .unwrap_or(0)
    }

    fn head_id(&self) -> BlockId {
        DynamicPropertiesStore::new(self.dyn_props.clone())
            .latest_block_header_hash()
            .ok()
            .flatten()
            .map(BlockId::from_raw)
            .unwrap_or(self.genesis)
    }

    /// Our latest solidified block id for the Hello `solid` field (falls back to
    /// head when there's no solid pointer / index entry — same as the dialer).
    fn solid_id(&self) -> BlockId {
        let head = self.head_id();
        let Some(num) = DynamicPropertiesStore::new(self.dyn_props.clone())
            .latest_solidified_block_num()
        else {
            return head;
        };
        self.block_index
            .as_ref()
            .and_then(|bi| BlockIndexStore::new(bi.clone()).get(num).ok())
            .unwrap_or(head)
    }

    /// Lowest block number we hold (Hello `lowest_block_num`); 0 if unknown.
    fn lowest_block_num(&self) -> i64 {
        self.block_index
            .as_ref()
            .and_then(|bi| BlockIndexStore::new(bi.clone()).lowest().ok().flatten())
            .unwrap_or(0)
    }

    fn note_served(&self) {
        if let Some(m) = &self.metrics {
            m.inc_p2p_inbound_served();
        }
    }

    fn set_peer_gauge(&self) {
        if let Some(m) = &self.metrics {
            m.set_p2p_inbound_peers(self.inbound.load(Ordering::Relaxed) as i64);
        }
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Bind `listen_addr` and accept inbound peers until `shutdown` fires. Each
/// accepted connection is handshaked + served on its own task; a bind failure
/// is logged and the listener exits (the node keeps running as outbound-only).
pub async fn run_inbound_listener(
    server: Arc<InboundServer>,
    listen_addr: SocketAddr,
    shutdown: tokio::sync::broadcast::Receiver<()>,
) {
    let listener = match TcpListener::bind(listen_addr).await {
        Ok(l) => l,
        Err(e) => {
            warn!(%listen_addr, error = %e,
                "p2p inbound: bind failed — peers will NOT be able to sync from us");
            return;
        }
    };
    info!(%listen_addr, max_inbound = server.max_inbound,
        "📡 p2p inbound listener up — peers can now sync from us");
    accept_loop(server, listener, shutdown).await;
}

/// The accept loop over an already-bound listener — factored out so tests can
/// bind an ephemeral port and drive a real TCP connection through it.
pub(crate) async fn accept_loop(
    server: Arc<InboundServer>,
    listener: TcpListener,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) {
    loop {
        let (stream, peer_addr) = tokio::select! {
            _ = shutdown.recv() => {
                info!("p2p inbound listener shutting down");
                return;
            }
            accepted = listener.accept() => match accepted {
                Ok(x) => x,
                Err(e) => {
                    debug!(error = %e, "p2p inbound accept error");
                    continue;
                }
            },
        };

        // Cap concurrent inbound connections (matches `max_peers`). Over the cap
        // we just drop the freshly-accepted socket — the peer retries elsewhere.
        if server.inbound.load(Ordering::Relaxed) >= server.max_inbound {
            debug!(%peer_addr, max = server.max_inbound, "p2p inbound at cap; refusing");
            drop(stream);
            continue;
        }

        let _ = stream.set_nodelay(true);
        let server = server.clone();
        let mut conn_shutdown = shutdown.resubscribe();
        tokio::spawn(async move {
            server.inbound.fetch_add(1, Ordering::Relaxed);
            server.set_peer_gauge();
            // Race the serve loop against shutdown. This task holds keepalive
            // timers and is not in the runtime's drained handle set, so without
            // a shutdown branch it would run into teardown and re-arm a timer
            // against the closing runtime. Observing the broadcast lets it exit
            // inside the drain window instead.
            let peer_addr_str = peer_addr.to_string();
            tokio::select! {
                _ = conn_shutdown.recv() => {}
                r = serve_inbound_peer(&server, stream, &peer_addr_str) => {
                    if let Err(e) = r {
                        debug!(%peer_addr, error = %e, "inbound peer ended");
                    }
                }
            }
            server.inbound.fetch_sub(1, Ordering::Relaxed);
            server.set_peer_gauge();
        });
    }
}

/// Handshake (as responder) + serve one inbound peer until it disconnects.
/// Generic over the stream so it can be driven over an in-memory duplex in
/// tests as well as a real `TcpStream`.
pub(crate) async fn serve_inbound_peer<S>(
    server: &InboundServer,
    stream: S,
    peer_addr: &str,
) -> Result<(), String>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut conn = PeerConnection::new(stream);
    if let Some(budget) = &server.inbound_budget {
        conn = conn.with_inbound_budget(budget.clone());
    }
    let node_id = random_node_id();
    let ts = now_ms();

    // STEP 1 — libp2p connection handshake. Both sides send their Hello
    // unconditionally, so the dialer's send-then-receive helper also drives the
    // responder correctly (our Hello crosses theirs on the wire).
    //
    // The endpoint names our advertise address. The bind address would be
    // `0.0.0.0`, which fails java-tron's `PATTERN_IPv4`: the peer's libp2p
    // `Message.parse` then rejects our Hello (`HelloMessage.valid` →
    // `NetUtil.validNode`, BAD_MESSAGE) and closes the connection. The SAME
    // endpoint goes into the application Hello below — from 4.8.3 java
    // validates that one too and disconnects with `UNEXPECTED_IDENTITY` if
    // its node id differs from this one.
    let from = Endpoint {
        address: server.advertise_host.as_bytes().to_vec(),
        address_ipv6: Vec::new(),
        port: server.advertise_port,
        node_id: node_id.clone(),
    };
    conn.libp2p_handshake(Libp2pHelloInputs {
        from: from.clone(),
        network_id: server.network_id,
        version: LIBP2P_VERSION,
        timestamp_ms: ts,
    })
    .await
    .map_err(|e| format!("libp2p_handshake: {e}"))?;

    // STEP 2 — application Hello. Advertise our TRUE genesis / solid / head /
    // lowest so a strict peer (java-tron `Channel` validation) accepts us.
    let head = server.head_id();
    // The head this connection announces; a terminal `BlockChainInventory`
    // must not end below it (see the SyncBlockChain arm).
    let hello_head_num = head.num() as i64;
    conn.handshake(HelloInputs {
        from,
        version: server.network_id,
        timestamp_ms: ts,
        genesis: server.genesis,
        solid: server.solid_id(),
        head,
        node_type: 0,
        lowest_block_num: server.lowest_block_num(),
        code_version: tron_types::CODE_VERSION_BYTES,
    })
    .await
    .map_err(|e| format!("handshake: {e}"))?;

    let peer_head = conn
        .peer_hello()
        .and_then(|h| h.head_block_id.as_ref().map(|b| b.number))
        .unwrap_or(-1);
    info!(%peer_addr, our_head = server.head_number(), peer_head,
        "inbound peer handshake ok — serving sync");

    // Serving loop. Keepalive is handled OUTSIDE the read (no &mut conn borrow
    // overlap): each iteration sends a ping if due and enforces the silence
    // deadline, then reads the next frame with a bounded timeout so the loop
    // always wakes to tick keepalive even when the peer is quiet.
    let mut last_inbound = Instant::now();
    let mut last_ping = Instant::now();
    // FETCH_INV_DATA is limited to 3 requests/s by ordinary peers. Queue
    // bursts of advertisements and pace requests instead of losing a request
    // to the peer's silent rate-limit drop.
    const TX_FETCH_INTERVAL: Duration = Duration::from_millis(400);
    const TX_FETCH_QUEUE_CAP: usize = 1000;
    let mut tx_fetch_queue = std::collections::VecDeque::<[u8; 32]>::new();
    let mut tx_fetch_queued = std::collections::HashSet::<[u8; 32]>::new();
    let mut last_tx_fetch: Option<Instant> = None;
    #[cfg(feature="devnet-fast")]
    let mut announced_head=server.head_number();
    loop {
        #[cfg(feature="devnet-fast")]
        {
            let head=server.head_number();
            if head!=announced_head {
                if let Some(index)=server.block_index.as_ref() {
                    if let Ok(id)=BlockIndexStore::new(index.clone()).get(head) {
                        conn.send_frame(Frame {ty:MessageType::Inventory,
                            payload:Bytes::from(tron_proto::Inventory {
                                r#type:tron_proto::inventory::InventoryType::Block as i32,
                                ids:vec![id.as_bytes().to_vec()],
                            }.encode_to_vec())}).await.map_err(|e|format!("private head announcement: {e}"))?;
                        announced_head=head;
                    }
                }
            }
        }

        if !tx_fetch_queue.is_empty()
            && last_tx_fetch.map_or(true, |t| t.elapsed() >= TX_FETCH_INTERVAL)
        {
            let mut ids = Vec::new();
            while ids.len() < 100 {
                let Some(id) = tx_fetch_queue.pop_front() else { break; };
                tx_fetch_queued.remove(&id);
                if server.mempool.as_ref().map_or(false, |mp| mp.get(&id).is_some()) {
                    continue;
                }
                ids.push(id.to_vec());
            }
            if !ids.is_empty() {
                let count = ids.len();
                conn.send_frame(Frame {
                    ty: MessageType::FetchInvData,
                    payload: Bytes::from(tron_proto::Inventory {
                        r#type: tron_proto::inventory::InventoryType::Trx as i32,
                        ids,
                    }.encode_to_vec()),
                }).await.map_err(|e| format!("request inbound tx bodies: {e}"))?;
                last_tx_fetch = Some(Instant::now());
                debug!(count, "requested inbound transaction bodies");
            }
        }

        if last_inbound.elapsed() > KEEPALIVE_INBOUND_DEADLINE {
            debug!(%peer_addr, "inbound peer silent past keepalive deadline; dropping");
            return Ok(());
        }
        if last_ping.elapsed() >= KEEPALIVE_INTERVAL {
            let ping = tron_proto::libp2p::KeepAliveMessage {
                timestamp: tron_net::keepalive::keepalive_timestamp_ms(),
            };
            conn.send_frame(Frame {
                ty: MessageType::Libp2pKeepAlivePing,
                payload: Bytes::from(ping.encode_to_vec()),
            })
            .await
            .map_err(|e| format!("send keepalive ping: {e}"))?;
            last_ping = Instant::now();
        }

        let mut read_timeout = if cfg!(feature="devnet-fast") {
            Duration::from_millis(100)
        } else {
            KEEPALIVE_INTERVAL
        };
        if !tx_fetch_queue.is_empty() {
            let remaining = last_tx_fetch.map_or(Duration::ZERO,
                |t| TX_FETCH_INTERVAL.saturating_sub(t.elapsed()));
            read_timeout = read_timeout.min(remaining);
        }
        let frame = match tokio::time::timeout(read_timeout, conn.next_frame()).await {
            Ok(Ok(Some(f))) => f,
            Ok(Ok(None)) => {
                debug!(%peer_addr, "inbound peer closed connection");
                return Ok(());
            }
            Ok(Err(e)) => return Err(format!("read: {e}")),
            Err(_) => continue, // read timeout → loop to tick keepalive
        };
        last_inbound = Instant::now();

        match frame.ty {
            MessageType::Libp2pKeepAlivePing => {
                let pong = tron_proto::libp2p::KeepAliveMessage {
                    timestamp: tron_net::keepalive::keepalive_timestamp_ms(),
                };
                conn.send_frame(Frame {
                    ty: MessageType::Libp2pKeepAlivePong,
                    payload: Bytes::from(pong.encode_to_vec()),
                })
                .await
                .map_err(|e| format!("send keepalive pong: {e}"))?;
            }
            MessageType::Libp2pKeepAlivePong | MessageType::P2pPong => {}
            MessageType::SyncBlockChain => {
                // The peer wants to catch up FROM us: it sent its chain locator
                // and expects our `BlockChainInventory` of ids past the shared
                // block. Identical logic to the outbound dispatch loop.
                let inv = match tron_proto::BlockInventory::decode(frame.payload) {
                    Ok(i) => i,
                    Err(e) => {
                        // No reply is possible, and the peer waits for one:
                        // java-tron's `PeerStatusCheck` disconnects with
                        // TIME_OUT when a SyncBlockChain goes unanswered
                        // (10 s from 4.8.3). java itself raises BAD_MESSAGE
                        // and disconnects here; never drop the request
                        // silently.
                        debug!(%peer_addr, error = %e,
                            "undecodable inbound SyncBlockChain; disconnecting");
                        return Ok(());
                    }
                };
                let (ids, remain) = match &server.block_index {
                    Some(bi) => serve_sync_block_chain_ids(
                        &BlockIndexStore::new(bi.clone()),
                        server.head_number(),
                        &inv.ids,
                    ),
                    None => (Vec::new(), 0),
                };
                // Empty ids means the peer's locator shares no block with our
                // main chain. java-tron's `SyncBlockChainMsgHandler` never replies
                // with an empty `BlockChainInventory` in this case — `getLostBlockIds`
                // throws (`SYNC_FAILED`) and the peer is disconnected with
                // `INCOMPATIBLE_CHAIN`. A java peer that DID receive an empty
                // inventory would reject it in `ChainInventoryMsgHandler.check()`
                // ("blockIds is empty") with `BAD_MESSAGE` and drop us. Mirror java:
                // close the connection instead of emitting an inventory it refuses.
                if ids.is_empty() {
                    debug!(%peer_addr, "inbound SyncBlockChain shares no common block; disconnecting");
                    return Ok(());
                }
                // "That is all I have" below the head we announced on this
                // connection is refused by java-tron 4.8.3
                // (`ChainInventoryMsgHandler.check`, SYNC_FAIL). It can only
                // arise when we cannot serve up to that head — an index gap,
                // or a reorg that lowered it — so close and let the peer
                // reconnect to a fresh Hello rather than send it.
                #[cfg(not(feature = "devnet-fast"))]
                let announced_head_num = hello_head_num;
                // A head announced after the Hello counts too, less the one
                // block java allows an announcement to run ahead.
                #[cfg(feature = "devnet-fast")]
                let announced_head_num = hello_head_num.max(announced_head - 1);
                if chain_inventory_ends_below_announced_head(&ids, remain, announced_head_num) {
                    debug!(%peer_addr, announced_head_num,
                        last = ids.last().map(|l| l.num()),
                        "cannot serve sync up to the announced head; disconnecting");
                    return Ok(());
                }
                let reply = tron_net::sync::chain_inventory_from_ids(&ids, remain);
                tron_net::sync::send_chain_inventory(&mut conn, &reply)
                    .await
                    .map_err(|e| format!("send_chain_inventory: {e}"))?;
                server.note_served();
                debug!(%peer_addr, served = ids.len(), remain, "served SyncBlockChain");
            }
            MessageType::Inventory => {
                // An inbound peer can submit transactions even if it only
                // downloads blocks from us. A producer may have no other
                // outbound connection to that peer.
                if let (Some(mp), Ok(inv)) = (
                    server.mempool.as_ref(),
                    tron_proto::Inventory::decode(frame.payload.as_ref()),
                ) {
                    if inv.r#type == tron_proto::inventory::InventoryType::Trx as i32 {
                        for raw in inv.ids {
                            let Ok(id) = <[u8; 32]>::try_from(raw.as_slice()) else { continue; };
                            if tx_fetch_queue.len() >= TX_FETCH_QUEUE_CAP { break; }
                            if mp.get(&id).is_none() && tx_fetch_queued.insert(id) {
                                tx_fetch_queue.push_back(id);
                            }
                        }
                    }
                }
            }
            MessageType::Trx => {
                if let Some(mp)=server.mempool.as_ref() {
                    let outcome=mp.submit(&frame.payload);
                    debug!(?outcome, "inbound transaction admission");
                }
            }
            MessageType::Trxs => {
                if let (Some(mp),Ok(batch))=(server.mempool.as_ref(),
                    tron_proto::Transactions::decode(frame.payload.as_ref())) {
                    for tx in batch.transactions.into_iter().take(100) {
                        let outcome=mp.submit(&tx.encode_to_vec());
                        debug!(?outcome, "inbound transaction admission");
                    }
                }
            }
            MessageType::FetchInvData => {
                // The peer asks for the actual block (or tx) bodies. Served from
                // the BlockStore / mempool, shared with the outbound path.
                serve_tx_fetch_inv_data(
                    &mut conn,
                    frame.payload,
                    server.mempool.as_deref(),
                    Some(&server.blocks),
                    peer_addr,
                )
                .await?;
                server.note_served();
                debug!(%peer_addr, "served FetchInvData");
            }
            MessageType::P2pDisconnect | MessageType::Libp2pDisconnect => {
                debug!(%peer_addr, "inbound peer disconnected");
                return Ok(());
            }
            // Block announcements from a peer we serve are
            // not needed for the sync-from-us role — our outbound dialers handle
            // pulling new data. Ignore them (don't drop the peer).
            other => {
                debug!(%peer_addr, ty = ?other, "inbound: ignoring non-serving frame");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tron_chainbase::{BlockStore, MemBackend};
    use tron_net::{HelloInputs, Libp2pHelloInputs};
    use tron_proto::block_header::Raw as BlockHeaderRaw;
    use tron_proto::{Block, BlockHeader};
    use tron_types::{genesis_block_id, mainnet_inputs};

    fn block_id(num: i64) -> BlockId {
        let mut raw = [0u8; 32];
        raw[..8].copy_from_slice(&(num as u64).to_be_bytes());
        // Vary the hash tail so distinct numbers get distinct ids.
        raw[8] = (num & 0xff) as u8;
        raw[9] = ((num >> 8) & 0xff) as u8;
        raw[31] = 0xab;
        BlockId::from_raw(raw)
    }

    fn mk_block(num: i64) -> Block {
        Block {
            block_header: Some(BlockHeader {
                raw_data: Some(BlockHeaderRaw {
                    number: num,
                    ..Default::default()
                }),
                witness_signature: Vec::new(),
            }),
            transactions: Vec::new(),
        }
    }

    const HEAD: i64 = 10;

    /// Build an `InboundServer` backed by an in-memory `1..=HEAD` chain.
    fn test_server() -> Arc<InboundServer> {
        test_server_holding(1..=HEAD)
    }

    /// As [`test_server`], but holding only the blocks in `held` while the
    /// head pointer still says `HEAD`.
    fn test_server_holding(held: impl IntoIterator<Item = i64>) -> Arc<InboundServer> {
        let bi_be: Arc<dyn KvBackend> = Arc::new(MemBackend::new());
        let blocks_be: Arc<dyn KvBackend> = Arc::new(MemBackend::new());
        let dp_be: Arc<dyn KvBackend> = Arc::new(MemBackend::new());
        let bi = BlockIndexStore::new(bi_be.clone());
        let bs = BlockStore::new(blocks_be.clone());
        let dp = DynamicPropertiesStore::new(dp_be.clone());
        for num in held {
            let id = block_id(num);
            bi.put(&id).unwrap();
            bs.put(&id, &mk_block(num)).unwrap();
        }
        dp.save_latest_block_header_number(HEAD);
        dp.save_latest_block_header_hash(block_id(HEAD).as_bytes());
        dp.save_latest_solidified_block_num(HEAD - 3);
        Arc::new(InboundServer::new(
            Some(bi_be),
            blocks_be,
            dp_be,
            None,
            genesis_block_id(&mainnet_inputs()),
            18_888,
            None,
            30,
        ))
    }

    /// Drive the full java-tron-compatible client protocol over `conn` and
    /// assert we sync blocks `3..=HEAD` from the server.
    async fn drive_client<S>(conn: &mut PeerConnection<S>)
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let genesis = genesis_block_id(&mainnet_inputs());
        let ts = now_ms();
        let from = Endpoint {
            address: b"127.0.0.1".to_vec(),
            address_ipv6: Vec::new(),
            port: 18_888,
            node_id: random_node_id(),
        };
        conn.libp2p_handshake(Libp2pHelloInputs {
            from: from.clone(),
            network_id: NETWORK_ID_MAINNET,
            version: LIBP2P_VERSION,
            timestamp_ms: ts,
        })
        .await
        .expect("client libp2p handshake");
        let outcome = conn
            .handshake(HelloInputs {
                from,
                version: MAINNET_P2P_VERSION,
                timestamp_ms: ts,
                genesis,
                solid: block_id(3),
                head: block_id(3),
                node_type: 0,
                lowest_block_num: 1,
                code_version: b"test-client",
            })
            .await
            .expect("client app handshake");
        assert_eq!(
            outcome
                .hello()
                .expect("server reciprocal Hello")
                .head_block_id
                .as_ref()
                .unwrap()
                .number,
            HEAD,
        );
        let locator = vec![block_id(3), block_id(2), block_id(1)];
        tron_net::sync::send_sync_request(conn, &locator)
            .await
            .expect("send sync request");
        let inv = tron_net::sync::recv_chain_inventory(conn)
            .await
            .expect("recv chain inventory");
        assert_eq!(inv.ids.first().unwrap().number, 3);
        assert_eq!(inv.ids.last().unwrap().number, HEAD);
        assert_eq!(inv.ids.len(), (HEAD - 3 + 1) as usize);
        let fetch_ids: Vec<Vec<u8>> = inv.ids.iter().map(|b| b.hash.clone()).collect();
        tron_net::sync::send_fetch_inv_data(conn, &fetch_ids)
            .await
            .expect("send fetch inv data");
        for expect_num in 3..=HEAD {
            let blk = tron_net::sync::recv_block(conn).await.expect("recv block");
            assert_eq!(blk.block_header.unwrap().raw_data.unwrap().number, expect_num);
        }
    }

    /// Real-TCP path: bind an ephemeral port, run the accept loop, and have a
    /// client connect over a genuine `TcpStream` and sync — exactly what a
    /// java-tron deployment dialing us will do.
    #[tokio::test]
    async fn inbound_listener_serves_a_real_tcp_peer() {
        let server = test_server();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (_sd_tx, sd_rx) = tokio::sync::broadcast::channel::<()>(1);
        let srv = server.clone();
        let accept = tokio::spawn(async move { accept_loop(srv, listener, sd_rx).await });

        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut conn = PeerConnection::new(stream);
        drive_client(&mut conn).await;
        assert_eq!(server.inbound.load(Ordering::Relaxed), 1, "one inbound peer tracked");

        drop(conn);
        drop(_sd_tx); // close shutdown channel → accept loop exits
        accept.abort();
    }

    /// End-to-end proof that a peer can sync FROM us. The client side drives the
    /// SAME `PeerConnection` handshake + `tron_net::sync` client functions the
    /// outbound dialer uses against real java-tron — so if it can handshake and
    /// pull a block range from our inbound server, a java-tron deployment can too.
    #[tokio::test]
    async fn inbound_peer_handshakes_and_syncs_blocks_from_us() {
        let server = test_server();
        // In-memory duplex stands in for the accepted TCP socket.
        let (client_io, server_io) = tokio::io::duplex(1 << 16);
        let srv = server.clone();
        let server_task =
            tokio::spawn(async move { serve_inbound_peer(&srv, server_io, "test-client").await });

        let mut conn = PeerConnection::new(client_io);
        drive_client(&mut conn).await;

        drop(conn);
        let _ = server_task.await;
    }

    /// A peer whose locator shares no block with our main chain must NOT receive
    /// an empty `BlockChainInventory` (java-tron's `ChainInventoryMsgHandler`
    /// rejects an empty inventory with `BAD_MESSAGE`). Mirror java's
    /// `SyncBlockChainMsgHandler`, which disconnects instead. We assert the
    /// connection is closed with no inventory frame on the wire.
    #[tokio::test]
    async fn inbound_no_common_block_disconnects_without_empty_inventory() {
        let server = test_server();
        let (client_io, server_io) = tokio::io::duplex(1 << 16);
        let srv = server.clone();
        let server_task =
            tokio::spawn(async move { serve_inbound_peer(&srv, server_io, "fork-peer").await });

        let genesis = genesis_block_id(&mainnet_inputs());
        let ts = now_ms();
        let from = Endpoint {
            address: b"127.0.0.1".to_vec(),
            address_ipv6: Vec::new(),
            port: 18_888,
            node_id: random_node_id(),
        };
        let mut conn = PeerConnection::new(client_io);
        conn.libp2p_handshake(Libp2pHelloInputs {
            from: from.clone(),
            network_id: NETWORK_ID_MAINNET,
            version: LIBP2P_VERSION,
            timestamp_ms: ts,
        })
        .await
        .expect("client libp2p handshake");
        conn.handshake(HelloInputs {
            from,
            version: MAINNET_P2P_VERSION,
            timestamp_ms: ts,
            genesis,
            solid: genesis,
            head: genesis,
            node_type: 0,
            lowest_block_num: 0,
            code_version: b"test-client",
        })
        .await
        .expect("client app handshake");

        // Locator entries that exist at NO number on the server's 1..=HEAD chain
        // (numbers above HEAD), so there's no common ancestor.
        let locator = vec![block_id(HEAD + 5), block_id(HEAD + 4)];
        tron_net::sync::send_sync_request(&mut conn, &locator)
            .await
            .expect("send sync request");

        // The server must close WITHOUT sending a (BlockChain)Inventory frame.
        // A clean EOF surfaces as PeerClosed; never a decoded inventory.
        match tron_net::sync::recv_chain_inventory(&mut conn).await {
            Err(tron_net::SyncError::PeerClosed) => {}
            Err(tron_net::SyncError::Frame(_)) => {}
            other => panic!("expected peer close, got {other:?}"),
        }

        drop(conn);
        let _ = server_task.await;
    }

    /// Run the client side of both handshakes and return the two endpoints
    /// the server named itself with: libp2p Hello first, application Hello
    /// second.
    async fn client_handshake<S>(conn: &mut PeerConnection<S>) -> (Endpoint, Endpoint)
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let genesis = genesis_block_id(&mainnet_inputs());
        let ts = now_ms();
        let from = Endpoint {
            address: b"127.0.0.1".to_vec(),
            address_ipv6: Vec::new(),
            port: 18_888,
            node_id: random_node_id(),
        };
        let libp2p_hello = conn
            .libp2p_handshake(Libp2pHelloInputs {
                from: from.clone(),
                network_id: NETWORK_ID_MAINNET,
                version: LIBP2P_VERSION,
                timestamp_ms: ts,
            })
            .await
            .expect("client libp2p handshake");
        let outcome = conn
            .handshake(HelloInputs {
                from,
                version: MAINNET_P2P_VERSION,
                timestamp_ms: ts,
                genesis,
                solid: block_id(3),
                head: block_id(3),
                node_type: 0,
                lowest_block_num: 1,
                code_version: b"test-client",
            })
            .await
            .expect("client app handshake");
        let app_hello = outcome.hello().expect("server reciprocal Hello").clone();
        assert_eq!(app_hello.head_block_id.as_ref().unwrap().number, HEAD);
        (
            libp2p_hello.from.expect("libp2p Hello names an endpoint"),
            app_hello.from.expect("app Hello names an endpoint"),
        )
    }

    /// What java-tron's `NetUtil.validNode` demands of an endpoint: 64-byte
    /// node id, port `1..=65535`, IPv4 text matching `PATTERN_IPv4`.
    fn assert_java_accepts_endpoint(ep: &Endpoint) {
        assert_eq!(ep.node_id.len(), 64, "node id length");
        assert!((1..=65_535).contains(&ep.port), "port {}", ep.port);
        let host = std::str::from_utf8(&ep.address).expect("ascii address");
        assert!(tron_net::is_valid_advertise_ipv4(host), "address {host:?}");
        assert!(ep.address_ipv6.is_empty());
    }

    /// The responder binds `0.0.0.0` but must never name it: a java peer's
    /// libp2p `Message.parse` rejects such a Hello (BAD_MESSAGE) and from
    /// 4.8.3 the application Hello is held to the same rule, plus the two
    /// must carry one node id (`UNEXPECTED_IDENTITY` otherwise).
    #[tokio::test]
    async fn inbound_hellos_name_a_valid_endpoint_with_one_node_id() {
        let server = test_server();
        let (client_io, server_io) = tokio::io::duplex(1 << 16);
        let srv = server.clone();
        let server_task =
            tokio::spawn(async move { serve_inbound_peer(&srv, server_io, "hello-peer").await });

        let mut conn = PeerConnection::new(client_io);
        let (libp2p_from, app_from) = client_handshake(&mut conn).await;
        assert_java_accepts_endpoint(&libp2p_from);
        assert_java_accepts_endpoint(&app_from);
        assert_eq!(libp2p_from.address, b"127.0.0.1".to_vec(), "default advertise address");
        assert_eq!(libp2p_from.port, 18_888);
        assert_eq!(libp2p_from, app_from, "one endpoint, one node id, in both Hellos");

        drop(conn);
        let _ = server_task.await;
    }

    #[tokio::test]
    async fn inbound_hellos_name_the_configured_advertise_host() {
        let server = Arc::try_unwrap(test_server())
            .ok()
            .unwrap()
            .with_advertise_host("203.0.113.7")
            // The bind address is not an advertise address; it is ignored.
            .with_advertise_host("0.0.0.0");
        let (client_io, server_io) = tokio::io::duplex(1 << 16);
        let server_task = tokio::spawn(async move {
            serve_inbound_peer(&server, server_io, "configured-host-peer").await
        });

        let mut conn = PeerConnection::new(client_io);
        let (libp2p_from, app_from) = client_handshake(&mut conn).await;
        assert_java_accepts_endpoint(&libp2p_from);
        assert_eq!(libp2p_from.address, b"203.0.113.7".to_vec());
        assert_eq!(libp2p_from, app_from);

        drop(conn);
        let _ = server_task.await;
    }

    /// Expect the server to close the connection without ever putting a
    /// `BlockChainInventory` on the wire.
    async fn assert_closed_without_inventory<S>(conn: &mut PeerConnection<S>)
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let reply = tokio::time::timeout(
            Duration::from_secs(5),
            tron_net::sync::recv_chain_inventory(conn),
        )
        .await
        .expect("server must close promptly, not leave the request unanswered");
        match reply {
            Err(tron_net::SyncError::PeerClosed) => {}
            Err(tron_net::SyncError::Frame(_)) => {}
            other => panic!("expected peer close, got {other:?}"),
        }
    }

    /// Our Hello announced `HEAD`, but the index stops at 5 (6..HEAD-1 are
    /// missing). The walk from the peer's block 3 ends at 5 with nothing more
    /// to offer — "remain 0" three blocks in, five short of the announced
    /// head. java-tron 4.8.3 answers that with SYNC_FAIL
    /// (`ChainInventoryMsgHandler.check`: "lastNum 5 (remainNum=0) is below
    /// hello head 10"); we close instead of sending it.
    #[tokio::test]
    async fn inbound_terminal_reply_below_announced_head_disconnects() {
        let server = test_server_holding((1..=5).chain([HEAD]));
        let (client_io, server_io) = tokio::io::duplex(1 << 16);
        let srv = server.clone();
        let server_task =
            tokio::spawn(async move { serve_inbound_peer(&srv, server_io, "gap-peer").await });

        let mut conn = PeerConnection::new(client_io);
        client_handshake(&mut conn).await;
        let locator = vec![block_id(1), block_id(2), block_id(3)];
        tron_net::sync::send_sync_request(&mut conn, &locator)
            .await
            .expect("send sync request");
        assert_closed_without_inventory(&mut conn).await;

        drop(conn);
        let _ = server_task.await;
    }

    /// A SyncBlockChain we cannot decode gets no reply, and the peer waits
    /// for one until its own timeout (java `PeerStatusCheck`, TIME_OUT). It
    /// is answered with a disconnect, never dropped silently.
    #[tokio::test]
    async fn inbound_undecodable_sync_request_disconnects() {
        let server = test_server();
        let (client_io, server_io) = tokio::io::duplex(1 << 16);
        let srv = server.clone();
        let server_task =
            tokio::spawn(async move { serve_inbound_peer(&srv, server_io, "garbage-peer").await });

        let mut conn = PeerConnection::new(client_io);
        client_handshake(&mut conn).await;
        // Field 1, length-delimited, declaring more bytes than follow.
        conn.send_frame(Frame {
            ty: MessageType::SyncBlockChain,
            payload: Bytes::from_static(&[0x0a, 0x7f, 0x01]),
        })
        .await
        .expect("send malformed sync request");
        assert_closed_without_inventory(&mut conn).await;

        drop(conn);
        let _ = server_task.await;
    }
    #[cfg(feature="devnet-fast")]
    #[tokio::test]
    async fn private_peer_receives_new_head_after_initial_sync() {
        let server=test_server();
        let (client_io,server_io)=tokio::io::duplex(1<<16);
        let srv=server.clone();
        let task=tokio::spawn(async move {serve_inbound_peer(&srv,server_io,"live-private").await});
        let mut conn=PeerConnection::new(client_io);
        drive_client(&mut conn).await;
        let next=block_id(HEAD+1);
        BlockStore::new(server.blocks.clone()).put(&next,&mk_block(HEAD+1)).unwrap();
        BlockIndexStore::new(server.block_index.as_ref().unwrap().clone()).put(&next).unwrap();
        DynamicPropertiesStore::new(server.dyn_props.clone()).save_latest_block_header_number(HEAD+1);
        let frame=tokio::time::timeout(Duration::from_secs(2),conn.next_frame()).await.unwrap().unwrap().unwrap();
        assert_eq!(frame.ty,MessageType::Inventory);
        let inv=tron_proto::Inventory::decode(frame.payload).unwrap();
        assert_eq!(inv.r#type,tron_proto::inventory::InventoryType::Block as i32);
        assert_eq!(inv.ids,vec![next.as_bytes().to_vec()]);
        drop(conn);task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn inbound_transaction_fetches_batch_bursts_under_peer_rate_limit() {
        let mut server = Arc::try_unwrap(test_server()).ok().unwrap();
        server.mempool = Some(TxMempool::new_arc(Default::default()));
        let (client_io, server_io) = tokio::io::duplex(1 << 16);
        let task = tokio::spawn(async move {
            serve_inbound_peer(&server, server_io, "tx-burst-peer").await
        });
        let mut conn = PeerConnection::new(client_io);
        drive_client(&mut conn).await;
        let limiter = crate::p2p_rate_limiter::P2pRateLimiter::new();
        limiter.register(MessageType::FetchInvData.as_byte(), 3.0);
        let advertise = |ids| Frame {
            ty: MessageType::Inventory,
            payload: Bytes::from(tron_proto::Inventory {
                r#type: tron_proto::inventory::InventoryType::Trx as i32, ids,
            }.encode_to_vec()),
        };
        conn.send_frame(advertise(vec![vec![1; 32]])).await.unwrap();
        let first = tokio::time::timeout(Duration::from_secs(2), conn.next_frame())
            .await.unwrap().unwrap().unwrap();
        assert_eq!(first.ty, MessageType::FetchInvData);
        assert!(limiter.try_acquire(first.ty.as_byte()));
        assert_eq!(tron_proto::Inventory::decode(first.payload).unwrap().ids, vec![vec![1;32]]);
        // Distinct advertisements arrive together after the first permit was
        // consumed. The next request must wait, batch, and deduplicate them.
        conn.send_frame(advertise(vec![vec![2;32]])).await.unwrap();
        conn.send_frame(advertise(vec![vec![2;32], vec![3;32]])).await.unwrap();
        let second = tokio::time::timeout(Duration::from_secs(2), conn.next_frame())
            .await.unwrap().unwrap().unwrap();
        assert_eq!(second.ty, MessageType::FetchInvData);
        assert!(limiter.try_acquire(second.ty.as_byte()), "peer must accept the second fetch");
        assert_eq!(tron_proto::Inventory::decode(second.payload).unwrap().ids,
            vec![vec![2;32], vec![3;32]]);
        drop(conn);
        task.await.unwrap().unwrap();
    }

}
