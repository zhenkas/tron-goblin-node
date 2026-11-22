//! Explicitly release a temporary bootstrap session after stopping its receiver.
use futures::StreamExt;
use std::{fs, time::Duration};
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};
use tron_grpc::assist_proto::{
    self as w, assisted_sync_client::AssistedSyncClient, frame::Message as M,
};
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 3 {
        return Err("usage: assist-release RECEIVER_CONFIG SNAPSHOT_ID".into());
    }
    let config = tron_node::NodeConfig::from_file(std::path::Path::new(&args[1]))?;
    let c = &config.assist;
    c.validate()?;
    let p = c
        .peers
        .iter()
        .find(|p| Some(&p.id) == c.upstream_peer.as_ref() && p.allow_receive)
        .ok_or("configured donor required")?;
    let tls = ClientTlsConfig::new()
        .identity(Identity::from_pem(
            fs::read(&c.certificate_file)?,
            fs::read(&c.private_key_file)?,
        ))
        .ca_certificate(Certificate::from_pem(fs::read(&p.server_ca_file)?))
        .domain_name(p.tls_domain.clone());
    let channel = Endpoint::from_shared(
        p.endpoint
            .clone()
            .ok_or("outbound donor endpoint required")?,
    )?
    .tls_config(tls)?
    .connect_timeout(Duration::from_secs(10))
    .connect()
    .await?;
    let network = match &c.bootstrap_genesis {
        Some(id) => hex::decode(id)?,
        None => tron_types::genesis_block_id(&tron_types::mainnet_inputs())
            .as_bytes()
            .to_vec(),
    };
    let (tx, rx) = tokio::sync::mpsc::channel(2);
    tx.send(w::Frame {
        message: Some(M::Hello(w::Hello {
            node_id: c.node_id.clone(),
            network: network.clone(),
            profile: tron_node::assisted_sync::bootstrap::profile(),
            wants_donor: true,
            trusted_cluster: c.trusted_cluster,
        })),
    })
    .await?;
    let mut client = AssistedSyncClient::new(channel);
    let mut stream = client
        .link(tokio_stream::wrappers::ReceiverStream::new(rx))
        .await?
        .into_inner();
    let first = tokio::time::timeout(Duration::from_secs(10), stream.next())
        .await?
        .ok_or("missing donor Hello")??;
    let Some(M::Hello(h)) = first.message else {
        return Err("donor Hello required".into());
    };
    if h.node_id != p.id
        || h.network != network
        || h.profile != tron_node::assisted_sync::bootstrap::profile()
        || h.wants_donor
    {
        return Err("donor identity mismatch".into());
    }
    tx.send(w::Frame {
        message: Some(M::SnapshotRelease(w::SnapshotRelease {
            snapshot_id: args[2].clone(),
        })),
    })
    .await?;
    tokio::time::timeout(Duration::from_secs(60), async {
        while let Some(frame) = stream.next().await {
            match frame?.message {
                Some(M::SnapshotAccepted(a)) if a.snapshot_id == args[2] => {
                    return Ok::<(), Box<dyn std::error::Error>>(())
                }
                Some(M::Failure(f)) => return Err(f.detail.into()),
                _ => {}
            }
        }
        Err("release was not acknowledged".into())
    })
    .await??;
    println!("snapshot session released");
    Ok(())
}
