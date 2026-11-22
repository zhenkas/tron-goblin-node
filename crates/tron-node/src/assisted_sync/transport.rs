use super::{AssistPeer, AssistRuntime};
use futures::{Stream, StreamExt};
use sha2::{Digest, Sha256};
use std::{fs, pin::Pin, sync::Arc, time::Duration};
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
pub(super) type Out = Pin<Box<dyn Stream<Item = Result<w::Frame, Status>> + Send>>;
pub(super) type In = Pin<Box<dyn Stream<Item = Result<w::Frame, Status>> + Send>>;
pub(super) type Tx = mpsc::Sender<Result<w::Frame, Status>>;
pub(super) fn frame(message: M) -> w::Frame {
    w::Frame {
        message: Some(message),
    }
}
pub(super) async fn send(tx: &Tx, message: M) -> Result<(), String> {
    tokio::time::timeout(Duration::from_secs(30), tx.send(Ok(frame(message))))
        .await
        .map_err(|_| "peer output timeout")?
        .map_err(|_| "peer disconnected".into())
}
pub(super) async fn work<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, String> + Send + 'static,
) -> Result<T, String> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| e.to_string())?
}
#[derive(Clone)]
struct Service {
    runtime: Arc<AssistRuntime>,
    shutdown: crate::ShutdownSignal,
}
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
        let certificate = certs
            .first()
            .ok_or_else(|| Status::unauthenticated("client certificate missing"))?;
        let fingerprint = hex::encode(Sha256::digest(certificate.as_ref()));
        let peer = self
            .runtime
            .config
            .peers
            .iter()
            .find(|p| p.certificate_sha256.eq_ignore_ascii_case(&fingerprint))
            .cloned()
            .ok_or_else(|| Status::permission_denied("certificate is not allowlisted"))?;
        let (tx, rx) = mpsc::channel(4);
        let runtime = self.runtime.clone();
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            if let Err(error) =
                link(runtime, peer, Box::pin(request.into_inner()), tx, shutdown).await
            {
                tracing::warn!(%error,"assisted inbound link closed");
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

/// Bind before returning so invalid TLS/listen configuration cannot silently
/// leave a supposedly enabled donor unavailable.
pub async fn start(
    runtime: Arc<AssistRuntime>,
    shutdown: crate::ShutdownSignal,
) -> Result<Vec<tokio::task::JoinHandle<()>>, String> {
    let identity = Identity::from_pem(
        fs::read(&runtime.config.certificate_file).map_err(|e| e.to_string())?,
        fs::read(&runtime.config.private_key_file).map_err(|e| e.to_string())?,
    );
    let ca = fs::read(&runtime.config.client_ca_file).map_err(|e| e.to_string())?;
    let mut handles = Vec::new();
    if runtime.config.trusted_cluster {
        let r = runtime.clone();
        let stop = shutdown.clone();
        handles.push(tokio::spawn(async move{
            let mut done=stop.subscribe();
            loop{
                tokio::select!{_=done.recv()=>break,_=tokio::time::sleep(Duration::from_secs(60))=>{}}
                let rt=r.clone();
                if let Err(error)=work(move||rt.expire_cluster_snapshot()).await{tracing::error!(%error,"cluster snapshot lease cleanup failed");}
            }
        }));
    }

    if let Some(address) = runtime.config.listen {
        let listener = tokio::net::TcpListener::bind(address)
            .await
            .map_err(|e| e.to_string())?;
        let incoming = futures::stream::unfold(listener, |listener| async {
            Some((listener.accept().await.map(|(socket, _)| socket), listener))
        });
        let service = AssistedSyncServer::new(Service {
            runtime: runtime.clone(),
            shutdown: shutdown.clone(),
        })
        .max_decoding_message_size(runtime.config.max_block_bytes + 1024)
        .max_encoding_message_size(runtime.config.max_block_bytes + 1024);
        let mut server = Server::builder()
            .tls_config(
                ServerTlsConfig::new()
                    .identity(identity.clone())
                    .client_ca_root(Certificate::from_pem(ca)),
            )
            .map_err(|e| e.to_string())?;
        let stop = shutdown.clone();
        handles.push(tokio::spawn(async move {
            let mut receiver = stop.subscribe();
            let trigger = stop.clone();
            if let Err(error) = server
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, async move {
                    if !stop.is_shutdown() {
                        let _ = receiver.recv().await;
                    }
                })
                .await
            {
                tracing::error!(%error,"assisted TLS listener failed");
                trigger.shutdown();
            }
        }));
    }
    for peer in runtime
        .config
        .peers
        .iter()
        .filter(|p| p.endpoint.is_some())
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
        let rt = runtime.clone();
        let stop = shutdown.clone();
        handles.push(tokio::spawn(async move{
            let mut receiver=stop.subscribe();
            while !stop.is_shutdown(){
                let result=async{
                    let channel=endpoint.connect().await.map_err(|e|e.to_string())?;
                    let (tx,rx)=mpsc::channel::<Result<w::Frame,Status>>(4);
                    let outbound=ReceiverStream::new(rx).filter_map(|x|async{x.ok()});
                    let mut client=AssistedSyncClient::new(channel)
                        .max_decoding_message_size(rt.config.max_block_bytes+1024)
                        .max_encoding_message_size(rt.config.max_block_bytes+1024);
                    // Hello is queued before awaiting response headers; either
                    // connection direction follows exactly the same protocol.
                    send_hello(&rt,&peer,&tx).await?;
                    let incoming=client.link(outbound).await.map_err(|e|e.to_string())?.into_inner();
                    link_inner(rt.clone(),peer.clone(),Box::pin(incoming),tx,stop.clone()).await
                };
                tokio::select!{
                    result=result=>if let Err(error)=result {tracing::warn!(peer=%peer.id,%error,"assisted dial link closed");},
                    _=receiver.recv()=>break,
                }
                tokio::select!{_=tokio::time::sleep(Duration::from_secs(5))=>{},_=receiver.recv()=>break}
            }
        }));
    }
    Ok(handles)
}
async fn send_hello(rt: &AssistRuntime, peer: &AssistPeer, tx: &Tx) -> Result<(), String> {
    send(
        tx,
        M::Hello(w::Hello {
            node_id: rt.config.node_id.clone(),
            network: rt.network.clone(),
            profile: rt.profile.clone(),
            wants_donor: !rt.config.trusted_cluster
                && rt.config.upstream_peer.as_ref() == Some(&peer.id),
            trusted_cluster: rt.config.trusted_cluster,
        }),
    )
    .await
}
async fn link(
    rt: Arc<AssistRuntime>,
    peer: AssistPeer,
    input: In,
    tx: Tx,
    stop: crate::ShutdownSignal,
) -> Result<(), String> {
    send_hello(&rt, &peer, &tx).await?;
    link_inner(rt, peer, input, tx, stop).await
}
async fn link_inner(
    rt: Arc<AssistRuntime>,
    peer: AssistPeer,
    mut input: In,
    tx: Tx,
    stop: crate::ShutdownSignal,
) -> Result<(), String> {
    let first = tokio::time::timeout(Duration::from_secs(10), input.next())
        .await
        .map_err(|_| "Hello timeout")?
        .ok_or("peer disconnected before Hello")?
        .map_err(|e| e.to_string())?;
    let Some(M::Hello(hello)) = first.message else {
        return Err("Hello must be first".into());
    };
    let receive = rt.config.upstream_peer.as_ref() == Some(&peer.id);
    if hello.node_id != peer.id || hello.network != rt.network || hello.profile != rt.profile {
        return Err("peer identity, genesis or state profile mismatch".into());
    }
    if rt.config.trusted_cluster || hello.trusted_cluster {
        if rt.config.trusted_cluster != hello.trusted_cluster {
            return Err("trusted cluster mode mismatch".into());
        }
        let connection = super::cluster::connection_id();
        let result =
            super::cluster_transport::link(rt.clone(), peer.clone(), connection, input, tx, stop)
                .await;
        let r = rt.clone();
        let id = peer.id.clone();
        work(move || r.cluster_disconnect(&id, connection)).await?;
        return result;
    }
    if hello.wants_donor == receive
        || (hello.wants_donor && !peer.allow_donate)
        || (receive && !peer.allow_receive)
    {
        return Err("peer role is not authorized or would form a donor cycle".into());
    }
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut receiver = stop.subscribe();
    let mut remote: Option<w::Status> = None;
    let mut pending = false;
    let mut semantic_anchor: Option<Vec<u8>> = None;
    let mut last_ack: Option<w::Ack> = None;
    let mut last_input = tokio::time::Instant::now();
    loop {
        if stop.is_shutdown() {
            return Ok(());
        }
        tokio::select! {
            _=receiver.recv()=>return Ok(()),
            _=interval.tick()=>{
                if last_input.elapsed()>Duration::from_secs(120){return Err("peer idle timeout".into())}
                let r=rt.clone();send(&tx,M::Status(work(move||r.status()).await?)).await?;
            },
            incoming=input.next()=>{
                last_input=tokio::time::Instant::now();
                let message=incoming.ok_or("peer stream ended")?.map_err(|e|e.to_string())?.message.ok_or("empty frame")?;
                match message{
                    M::Hello(_)=>return Err("duplicate Hello".into()),
                    M::Status(status)=>{
                        if receive{
                            let r=rt.clone();let s=status.clone();
                            let assisted=work(move||r.observe_status(&s)).await?;
                            if !assisted {pending=false;}
                            if assisted&&!pending {
                                let r=rt.clone();let height=work(move||Ok(r.head()?.0)).await?.min(status.solid);
                                send(&tx,M::Probe(w::Probe{height})).await?;pending=true;
                            }else if !assisted{
                                let r=rt.clone();let solid=status.solid;
                                if let Some(ack)=work(move||r.local_ack(solid)).await?{
                                    if last_ack.as_ref()!=Some(&ack){send(&tx,M::Ack(ack.clone())).await?;last_ack=Some(ack);}
                                }
                            }
                        }
                        remote=Some(status);
                    },
                    M::Probe(probe)=>{
                        if !peer.allow_donate||receive{return Err("unauthorized anchor probe".into())}
                        let r=rt.clone();let height=probe.height;
                        let anchor=work(move||{
                            if height<0||height>r.status()?.solid{return Err("anchor outside solid history".into())}
                            Ok(w::Anchor{height,block_id:r.canonical_id(height)?})
                        }).await?;
                        send(&tx,M::Anchor(anchor)).await?;
                    },
                    M::Anchor(anchor)=>{
                        if !receive||!pending{return Err("unexpected anchor".into())}
                        let r=rt.clone();let height=anchor.height;let id=anchor.block_id;
                        let matched=work(move||r.align_anchor(height,&id)).await?;
                        if matched{
                            let r=rt.clone();let fetch=work(move||r.fetch_cursor()).await?;
                            semantic_anchor=(!fetch.after_semantic_digest.is_empty()).then(||fetch.after_semantic_digest.clone());
                            send(&tx,M::Fetch(fetch)).await?;
                        }else{
                            send(&tx,M::Probe(w::Probe{height:height-1})).await?;
                        }
                    },
                    M::Fetch(fetch)=>{
                        if !peer.allow_donate||receive{return Err("unauthorized block fetch".into())}
                        let r=rt.clone();
                        if let Some(block)=work(move||r.export_next(&fetch)).await?{send(&tx,M::Block(block)).await?;}
                        else{let r=rt.clone();send(&tx,M::Status(work(move||r.status()).await?)).await?;}
                    },
                    M::Block(block)=>{
                        if !receive||!pending{return Err("unsolicited block bundle".into())}
                        if block.height>remote.as_ref().ok_or("missing solid barrier")?.solid{return Err("unfinalized bundle".into())}
                        let r=rt.clone();
                        let proof=semantic_anchor.take();
                            let ack=match work(move||r.import_with_anchor(&block,proof.as_deref())).await{
                            Ok(ack)=>ack,Err(error)=>{if rt.has_pending_import(){stop.shutdown();}return Err(error)}
                        };
                        send(&tx,M::Ack(ack.clone())).await?;last_ack=Some(ack);
                        let r=rt.clone();let s=remote.clone().unwrap();
                        if work(move||r.observe_status(&s)).await?{
                            let r=rt.clone();let fetch=work(move||r.fetch_cursor()).await?;
                            semantic_anchor=(!fetch.after_semantic_digest.is_empty()).then(||fetch.after_semantic_digest.clone());
                            send(&tx,M::Fetch(fetch)).await?;
                        }else{pending=false;}
                    },
                    M::Ack(ack)=>{
                        if !peer.allow_donate||receive{return Err("unauthorized replication ACK".into())}
                        let r=rt.clone();let id=peer.id.clone();work(move||r.accept_ack(&id,ack)).await?;
                    },
                    M::SnapshotOpen(request)=>{
                        if !peer.allow_donate||receive{return Err("unauthorized snapshot request".into())}
                        let r=rt.clone();let owner=peer.id.clone();
                        match work(move||r.snapshot_open(&owner,&request)).await {
                            Ok(state)=>send(&tx,M::SnapshotState(state)).await?,
                            Err(error)=>{
                                let code=error.split(':').next().unwrap_or("SNAPSHOT_ERROR").to_string();
                                send(&tx,M::Failure(w::Failure{code,detail:error.clone()})).await?;
                                return Err(error)
                            }
                        }
                        last_input=tokio::time::Instant::now();
                    },
                    M::SnapshotRead(request)=>{
                        if !peer.allow_donate||receive{return Err("unauthorized snapshot read".into())}
                        let r=rt.clone();let owner=peer.id.clone();
                        send(&tx,M::SnapshotChunk(work(move||r.snapshot_read(&owner,&request)).await?)).await?;
                    },
                    M::SnapshotInstalled(ack)=>{
                        if !peer.allow_donate||receive{return Err("unauthorized snapshot ACK".into())}
                        let r=rt.clone();let owner=peer.id.clone();
                        send(&tx,M::SnapshotAccepted(work(move||r.snapshot_installed(&owner,&ack)).await?)).await?;
                    },
                    M::SnapshotRelease(request)=>{
                        if !peer.allow_donate||receive{return Err("unauthorized snapshot release".into())}
                        let r=rt.clone();let owner=peer.id.clone();
                        send(&tx,M::SnapshotAccepted(work(move||r.snapshot_release(&owner,&request)).await?)).await?;
                    },
                    M::SnapshotState(_)|M::SnapshotChunk(_)|M::SnapshotAccepted(_)=>return Err("snapshot response outside bootstrap startup".into()),
                    M::Failure(failure)=>return Err(format!("{}: {}",failure.code,failure.detail)),
                }
            }
        }
    }
}
