//! Symmetric Link: every authenticated member may request and serve on the same stream.
use super::{
    cluster::SourceLease,
    transport::{send, work, In, Tx},
    AssistPeer, AssistRuntime,
};
use futures::StreamExt;
use std::{sync::Arc, time::Duration};
use tron_grpc::assist_proto::{self as w, frame::Message as M};
async fn fetch(rt: Arc<AssistRuntime>, tx: &Tx) -> Result<Option<Vec<u8>>, String> {
    let request = work(move || rt.fetch_cursor()).await?;
    let proof =
        (!request.after_semantic_digest.is_empty()).then(|| request.after_semantic_digest.clone());
    send(tx, M::Fetch(request)).await?;
    Ok(proof)
}
pub(super) async fn link(
    rt: Arc<AssistRuntime>,
    peer: AssistPeer,
    connection: u64,
    mut input: In,
    tx: Tx,
    stop: crate::ShutdownSignal,
) -> Result<(), String> {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut shutdown = stop.subscribe();
    let mut last_input = tokio::time::Instant::now();
    let mut remote: Option<w::Status> = None;
    let mut pending: Option<SourceLease> = None;
    let mut proof: Option<Vec<u8>> = None;
    loop {
        if stop.is_shutdown() {
            return Ok(());
        }
        tokio::select! {
            _=shutdown.recv()=>return Ok(()),
            _=interval.tick()=>{
                if last_input.elapsed()>Duration::from_secs(120){return Err("cluster peer idle timeout".into())}
                let r=rt.clone();send(&tx,M::Status(work(move||r.status()).await?)).await?;
            },
            item=input.next()=>{
                last_input=tokio::time::Instant::now();
                let message=item.ok_or("cluster peer disconnected")?.map_err(|e|e.to_string())?.message.ok_or("empty cluster frame")?;
                match message{
                    M::Hello(_)=>return Err("duplicate cluster Hello".into()),
                    M::Status(status)=>{
                        remote=Some(status.clone());let r=rt.clone();let id=peer.id.clone();
                        let lease=work(move||r.cluster_observe(&id,connection,status)).await?;
                        if let Some(lease)=lease.filter(|_|rt.is_assisted()){
                            if pending.as_ref()!=Some(&lease){
                                pending=Some(lease);proof=None;
                                let r=rt.clone();let height=work(move||Ok(r.head()?.0)).await?.min(remote.as_ref().unwrap().solid);
                                send(&tx,M::Probe(w::Probe{height})).await?;
                            }
                        }else{pending=None;proof=None;}
                    },
                    M::Probe(request)=>{
                        let r=rt.clone();let anchor=work(move||{
                            if request.height<0 || request.height>r.status()?.solid{return Err("cluster anchor outside solid history".into())}
                            Ok(w::Anchor{height:request.height,block_id:r.canonical_id(request.height)?})
                        }).await?;
                        send(&tx,M::Anchor(anchor)).await?;
                    },
                    M::Anchor(anchor)=>{
                        let Some(lease)=pending.clone().filter(|l|rt.cluster_current(l))else{continue};
                        let r=rt.clone();let token=lease.clone();
                        match work(move||r.align_from_cluster(anchor.height,&anchor.block_id,&token)).await{
                            Ok(true)=>proof=fetch(rt.clone(),&tx).await?,
                            Ok(false)=>{
                                if anchor.height<=0{return Err("cluster genesis conflict".into())}
                                send(&tx,M::Probe(w::Probe{height:anchor.height-1})).await?;
                            },
                            Err(e) if e=="CLUSTER_SOURCE_CHANGED"=>{pending=None;proof=None;},
                            Err(e)=>return Err(e),
                        }
                    },
                    M::Fetch(request)=>{
                        let r=rt.clone();
                        match work(move||r.export_next(&request)).await{
                            Ok(Some(block))=>send(&tx,M::Block(block)).await?,
                            Ok(None)=>{let r=rt.clone();send(&tx,M::Status(work(move||r.status()).await?)).await?;},
                            Err(error)=>{
                                let code=error.split(':').next().unwrap_or("CLUSTER_FETCH_ERROR").to_string();
                                send(&tx,M::Failure(w::Failure{code,detail:error})).await?;
                            }
                        }
                    },
                    M::Block(block)=>{
                        let Some(lease)=pending.clone().filter(|l|rt.cluster_current(l))else{continue};
                        if block.height>remote.as_ref().ok_or("missing cluster solid barrier")?.solid{return Err("unfinalized cluster bundle".into())}
                        let r=rt.clone();let token=lease.clone();let p=proof.take();
                        match work(move||r.import_from_cluster(&block,p.as_deref(),&token)).await{
                            Ok(_)=>{},
                            Err(e) if e=="CLUSTER_SOURCE_CHANGED"=>{pending=None;continue},
                            Err(e)=>{if rt.has_pending_import(){stop.shutdown();}return Err(e)},
                        }
                        // Progress is local and durable. No per-peer ACK is sent or retained.
                        let r=rt.clone();let id=peer.id.clone();let status=remote.clone().unwrap();
                        let lease=work(move||r.cluster_observe(&id,connection,status)).await?;
                        if let Some(lease)=lease.filter(|_|rt.is_assisted()){
                            pending=Some(lease);proof=fetch(rt.clone(),&tx).await?;
                        }else{pending=None;}
                    },
                    M::Ack(_)=>return Err("replication ACK is not used by trusted cluster".into()),
                    M::SnapshotOpen(request)=>{
                        let r=rt.clone();let id=peer.id.clone();
                        match work(move||r.snapshot_open(&id,&request)).await{
                            Ok(state)=>send(&tx,M::SnapshotState(state)).await?,
                            Err(error)=>{let code=error.split(':').next().unwrap_or("SNAPSHOT_ERROR").into();send(&tx,M::Failure(w::Failure{code,detail:error})).await?;}
                        }
                        last_input=tokio::time::Instant::now();
                    },
                    M::SnapshotRead(request)=>{
                        let r=rt.clone();let id=peer.id.clone();
                        send(&tx,M::SnapshotChunk(work(move||r.snapshot_read(&id,&request)).await?)).await?;
                    },
                    M::SnapshotInstalled(request)=>{
                        let r=rt.clone();let id=peer.id.clone();
                        send(&tx,M::SnapshotAccepted(work(move||r.snapshot_installed(&id,&request)).await?)).await?;
                    },
                    M::SnapshotRelease(request)=>{
                        let r=rt.clone();let id=peer.id.clone();
                        send(&tx,M::SnapshotAccepted(work(move||r.snapshot_release(&id,&request)).await?)).await?;
                    },
                    M::SnapshotState(_)|M::SnapshotChunk(_)|M::SnapshotAccepted(_)=>return Err("snapshot response outside empty bootstrap".into()),
                    M::Failure(e)=>{
                        // The partner compared this node's proof of its head with its own record.
                        if e.code=="STATE_DIGEST_MISMATCH"{super::runtime::report_state_mismatch(rt.head().map(|h|h.0).unwrap_or(-1));}
                        return Err(format!("{}: {}",e.code,e.detail))
                    },
                }
            }
        }
    }
}
