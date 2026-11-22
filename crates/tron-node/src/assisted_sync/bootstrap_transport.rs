//! Bootstrap endpoint exists before opening any ledger handle or public API.
use super::{
    bootstrap::{self, Download},
    transport::{send, work, In, Out, Tx},
    AssistConfig, AssistPeer,
};
use futures::StreamExt;
use std::{
    fs,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    transport::{Certificate, ClientTlsConfig, Endpoint, Identity, Server, ServerTlsConfig},
    Request, Response, Status,
};
use tron_grpc::assist_proto::{
    self as w,
    assisted_sync_client::AssistedSyncClient,
    assisted_sync_server::{AssistedSync, AssistedSyncServer},
    frame::Message as M,
};
struct Empty {
    config: AssistConfig,
    data: PathBuf,
    network: Vec<u8>,
    download: Mutex<Download>,
    gate: Arc<tokio::sync::Mutex<()>>,
    ready: tokio::sync::watch::Sender<bool>,
}
impl Empty {
    async fn hello(&self, tx: &Tx) -> Result<(), String> {
        send(
            tx,
            M::Hello(w::Hello {
                node_id: self.config.node_id.clone(),
                network: self.network.clone(),
                profile: bootstrap::profile(),
                wants_donor: true,
                trusted_cluster: self.config.trusted_cluster,
            }),
        )
        .await
    }
}
#[derive(Clone)]
struct Service(Arc<Empty>, crate::ShutdownSignal);
#[tonic::async_trait]
impl AssistedSync for Service {
    type LinkStream = Out;
    async fn link(
        &self,
        request: Request<tonic::Streaming<w::Frame>>,
    ) -> Result<Response<Out>, Status> {
        let certs = request
            .peer_certs()
            .ok_or_else(|| Status::unauthenticated("mTLS required"))?;
        let cert = certs
            .first()
            .ok_or_else(|| Status::unauthenticated("certificate required"))?;
        use sha2::Digest;
        let fingerprint = hex::encode(sha2::Sha256::digest(cert.as_ref()));
        let peer = self
            .0
            .config
            .peers
            .iter()
            .find(|p| {
                p.certificate_sha256.eq_ignore_ascii_case(&fingerprint)
                    && (self.0.config.trusted_cluster
                        || Some(&p.id) == self.0.config.upstream_peer.as_ref())
            })
            .cloned()
            .ok_or_else(|| Status::permission_denied("bootstrap donor not allowlisted"))?;
        let (tx, rx) = mpsc::channel(4);
        let rt = self.0.clone();
        let stop = self.1.clone();
        tokio::spawn(async move {
            let result = async {
                rt.hello(&tx).await?;
                link(rt, peer, Box::pin(request.into_inner()), tx, stop).await
            }
            .await;
            if let Err(error) = result {
                tracing::warn!(%error,"bootstrap incoming link closed");
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}
async fn link(
    rt: Arc<Empty>,
    peer: AssistPeer,
    mut input: In,
    tx: Tx,
    stop: crate::ShutdownSignal,
) -> Result<(), String> {
    let _gate = rt
        .gate
        .clone()
        .try_lock_owned()
        .map_err(|_| "bootstrap link already active")?;
    let first = tokio::time::timeout(Duration::from_secs(10), input.next())
        .await
        .map_err(|_| "bootstrap Hello timeout")?
        .ok_or("bootstrap disconnected")?
        .map_err(|e| e.to_string())?;
    let Some(M::Hello(hello)) = first.message else {
        return Err("bootstrap Hello must be first".into());
    };
    if hello.node_id != peer.id
        || hello.network != rt.network
        || hello.profile != bootstrap::profile()
        || hello.wants_donor
        || (!rt.config.trusted_cluster && !peer.allow_receive)
        || hello.trusted_cluster != rt.config.trusted_cluster
    {
        return Err("bootstrap donor role, chain or profile mismatch".into());
    }
    if rt.config.trusted_cluster {
        let r = rt.clone();
        let source = peer.id.clone();
        let complete = work(move || {
            let mut d = r.download.lock().map_err(|_| "download poisoned")?;
            if ["installing", "installed"].contains(&d.phase.as_str()) {
                d.install(&r.data)?;
                d.phase = "complete".into();
                d.save(&r.data)?;
                return Ok(true);
            }
            let path = r.data.join("bootstrap/source-peer.json");
            let old = if path.exists() {
                Some(
                    serde_json::from_slice::<String>(&fs::read(&path).map_err(|e| e.to_string())?)
                        .map_err(|e| e.to_string())?,
                )
            } else {
                None
            };
            if old.as_ref().is_some_and(|id| id != &source) {
                d.restart(&r.data, true)?;
            }
            super::journal::atomic_write(
                &path,
                &serde_json::to_vec(&source).map_err(|e| e.to_string())?,
            )?;
            Ok(false)
        })
        .await?;
        if complete {
            rt.ready.send_replace(true);
            return Ok(());
        }
    }
    let open = {
        let d = rt.download.lock().map_err(|_| "download poisoned")?;
        w::SnapshotOpen {
            request_id: d.request_id.clone(),
            replace_snapshot_id: d.replace_snapshot_id.clone(),
        }
    };
    send(&tx, M::SnapshotOpen(open.clone())).await?;
    let mut awaiting_solid = false;
    let mut ticker = tokio::time::interval(Duration::from_secs(2));
    let mut shutdown = stop.subscribe();
    loop {
        let item = tokio::select! {
            _=shutdown.recv()=>return Ok(()),
            _=ticker.tick(),if awaiting_solid=>{send(&tx,M::SnapshotOpen(open.clone())).await?;continue},
            x=input.next()=>x.ok_or("bootstrap donor disconnected")?.map_err(|e|e.to_string())?,
        };
        match item.message.ok_or("empty bootstrap frame")? {
            M::Status(_) => {}
            M::SnapshotState(state) => {
                let r = rt.clone();
                let ready = work(move || {
                    r.download
                        .lock()
                        .map_err(|_| "download poisoned")?
                        .accept_manifest(&r.data, &state, &r.network, r.config.max_snapshot_bytes)
                })
                .await?;
                awaiting_solid = !ready;
                if ready {
                    request_next(rt.clone(), &tx).await?;
                }
            }
            M::SnapshotChunk(chunk) => {
                let r = rt.clone();
                work(move || {
                    let mut d = r.download.lock().map_err(|_| "download poisoned")?;
                    let before = d.downloaded;
                    d.accept_chunk(&r.data, &chunk)?;
                    let total = d.manifest.as_ref().unwrap().bytes();
                    if total > 0
                        && (before.saturating_mul(100) / total
                            != d.downloaded.saturating_mul(100) / total)
                    {
                        tracing::info!(
                            bytes = d.downloaded,
                            total,
                            percent = d.downloaded.saturating_mul(100) / total,
                            "bootstrap download progress"
                        );
                    }
                    Ok(())
                })
                .await?;
                request_next(rt.clone(), &tx).await?;
            }
            M::SnapshotAccepted(accepted) => {
                let r = rt.clone();
                work(move || {
                    let mut d = r.download.lock().map_err(|_| "download poisoned")?;
                    if d.phase != "installed" || accepted.snapshot_id != d.request_id {
                        return Err("unexpected snapshot acceptance".into());
                    }
                    d.phase = "complete".into();
                    d.save(&r.data)
                })
                .await?;
                rt.ready.send_replace(true);
                return Ok(());
            }
            M::Failure(e) => {
                if ["SNAPSHOT_REORG", "HISTORY_GAP", "SNAPSHOT_GONE"].contains(&e.code.as_str()) {
                    let r = rt.clone();
                    let gone = e.code == "SNAPSHOT_GONE";
                    work(move || {
                        r.download
                            .lock()
                            .map_err(|_| "download poisoned")?
                            .restart(&r.data, gone)
                    })
                    .await?;
                }
                return Err(format!("{}: {}", e.code, e.detail));
            }
            _ => return Err("unexpected message during bootstrap".into()),
        }
    }
}
async fn request_next(rt: Arc<Empty>, tx: &Tx) -> Result<(), String> {
    let request = rt
        .download
        .lock()
        .map_err(|_| "download poisoned")?
        .next_read();
    if let Some(request) = request {
        send(tx, M::SnapshotRead(request)).await
    } else {
        let r = rt.clone();
        let ack = work(move || {
            r.download
                .lock()
                .map_err(|_| "download poisoned")?
                .install(&r.data)
        })
        .await?;
        if rt.config.trusted_cluster {
            let r = rt.clone();
            work(move || {
                let mut d = r.download.lock().map_err(|_| "download poisoned")?;
                d.phase = "complete".into();
                d.save(&r.data)
            })
            .await?;
            rt.ready.send_replace(true);
            let _ = send(tx, M::SnapshotInstalled(ack)).await;
            Ok(())
        } else {
            send(tx, M::SnapshotInstalled(ack)).await
        }
    }
}
pub async fn receive(
    config: &crate::NodeConfig,
    shutdown: crate::ShutdownSignal,
) -> Result<(), String> {
    let cfg = &config.assist;
    if !cfg.enable || !cfg.bootstrap || (!cfg.trusted_cluster && cfg.upstream_peer.is_none()) {
        return Ok(());
    }
    if shutdown.is_shutdown() {
        return Err("bootstrap cancelled before startup".into());
    }
    let data = &config.data_dir;
    let cursor = data.join("bootstrap/receiver.json");
    if !cursor.exists() && (data.join("database").exists() || data.join("db").exists()) {
        return Ok(());
    }
    fs::create_dir_all(data.join("bootstrap")).map_err(|e| e.to_string())?;
    let process_lock = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(data.join("bootstrap/process.lock"))
        .map_err(|e| e.to_string())?;
    use std::os::fd::AsRawFd;
    if unsafe { libc::flock(process_lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        return Err("another bootstrap process owns this data directory".into());
    }
    if config.storage.provider().map_err(|e| e.to_string())?.engine
        != tron_chainbase::KvEngine::Rocksdb
    {
        return Err("snapshot bootstrap currently requires RocksDB".into());
    }
    let mut download = Download::load_or_create(data, &cfg.bootstrap_reset_token)?;
    if cfg.trusted_cluster && ["installing", "installed"].contains(&download.phase.as_str()) {
        download.install(data)?;
        download.phase = "complete".into();
        download.save(data)?;
    }
    if download.phase == "complete" {
        if !data.join("database/properties/CURRENT").exists() {
            return Err(
                "completed bootstrap ledger is missing; use a fresh empty data directory".into(),
            );
        }
        return Ok(());
    }
    let network = match &cfg.bootstrap_genesis {
        Some(id) => hex::decode(id).map_err(|_| "invalid bootstrap genesis hex")?,
        None => tron_types::genesis_block_id(&tron_types::mainnet_inputs())
            .as_bytes()
            .to_vec(),
    };
    if network.len() != 32 {
        return Err("bootstrap genesis must be a 32-byte block ID".into());
    }
    let (ready, mut ready_rx) = tokio::sync::watch::channel(false);
    let rt = Arc::new(Empty {
        config: cfg.clone(),
        data: data.clone(),
        network,
        download: Mutex::new(download),
        gate: Arc::new(tokio::sync::Mutex::new(())),
        ready,
    });
    let stop = crate::ShutdownSignal::new();
    let mut handles = Vec::new();
    let identity = Identity::from_pem(
        fs::read(&cfg.certificate_file).map_err(|e| e.to_string())?,
        fs::read(&cfg.private_key_file).map_err(|e| e.to_string())?,
    );
    if let Some(address) = cfg.listen {
        let listener = tokio::net::TcpListener::bind(address)
            .await
            .map_err(|e| e.to_string())?;
        let incoming = futures::stream::unfold(listener, |l| async {
            Some((l.accept().await.map(|(s, _)| s), l))
        });
        let service = AssistedSyncServer::new(Service(rt.clone(), stop.clone()))
            .max_decoding_message_size(cfg.max_block_bytes + 1024)
            .max_encoding_message_size(cfg.max_block_bytes + 1024);
        let mut server = Server::builder()
            .tls_config(
                ServerTlsConfig::new()
                    .identity(identity.clone())
                    .client_ca_root(Certificate::from_pem(
                        fs::read(&cfg.client_ca_file).map_err(|e| e.to_string())?,
                    )),
            )
            .map_err(|e| e.to_string())?;
        let s = stop.clone();
        let failed_signal = s.clone();
        handles.push(tokio::spawn(async move {
            let mut rx = s.subscribe();
            if let Err(error) = server
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, async move {
                    if !s.is_shutdown() {
                        let _ = rx.recv().await;
                    }
                })
                .await
            {
                tracing::error!(%error,"bootstrap listener failed");
                failed_signal.shutdown();
            }
        }));
    }
    for peer in cfg
        .peers
        .iter()
        .filter(|p| {
            (cfg.trusted_cluster || Some(&p.id) == cfg.upstream_peer.as_ref())
                && p.endpoint.is_some()
        })
        .cloned()
    {
        let tls = ClientTlsConfig::new()
            .identity(identity.clone())
            .ca_certificate(Certificate::from_pem(
                fs::read(&peer.server_ca_file).map_err(|e| e.to_string())?,
            ))
            .domain_name(peer.tls_domain.clone());
        let endpoint = Endpoint::from_shared(peer.endpoint.clone().unwrap())
            .map_err(|e| e.to_string())?
            .tls_config(tls)
            .map_err(|e| e.to_string())?
            .connect_timeout(Duration::from_secs(10));
        let r = rt.clone();
        let s = stop.clone();
        handles.push(tokio::spawn(async move{
            let mut rx=s.subscribe();
            while !s.is_shutdown(){
                let attempt=async{
                    let channel=endpoint.connect().await.map_err(|e|e.to_string())?;
                    let (tx,rx)=mpsc::channel::<Result<w::Frame,Status>>(4);
                    let outbound=ReceiverStream::new(rx).filter_map(|x|async{x.ok()});
                    r.hello(&tx).await?;
                    let incoming=AssistedSyncClient::new(channel).max_decoding_message_size(r.config.max_block_bytes+1024).max_encoding_message_size(r.config.max_block_bytes+1024)
                        .link(outbound).await.map_err(|e|e.to_string())?.into_inner();
                    link(r.clone(),peer.clone(),Box::pin(incoming),tx,s.clone()).await
                };
                tokio::select!{result=attempt=>if let Err(error)=result{tracing::warn!(%error,"bootstrap dial closed");},_=rx.recv()=>break}
                tokio::select!{_=tokio::time::sleep(Duration::from_secs(2))=>{},_=rx.recv()=>break}
            }
        }));
    }
    if handles.is_empty() {
        return Err("bootstrap requires a listener or donor endpoint".into());
    }
    tracing::info!(data_dir=?data,"empty receiver bootstrapping before ledger/RPC startup");
    let mut cancel = shutdown.subscribe();
    let mut failed = stop.subscribe();
    let result = tokio::select! {
        _=ready_rx.changed()=>if *ready_rx.borrow(){Ok(())}else{Err("bootstrap completion channel closed".into())},
        _=cancel.recv()=>Err("bootstrap interrupted; durable progress retained".into()),
        _=failed.recv()=>Err("bootstrap transport stopped".into()),
    };
    stop.shutdown();
    for handle in handles {
        let _ = handle.await;
    }
    result
}
