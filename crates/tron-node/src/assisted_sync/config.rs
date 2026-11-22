use serde::{Deserialize, Serialize};
use std::{collections::HashSet, path::PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AssistConfig {
    pub enable: bool,
    /// Symmetric trusted peers, fixed-window history and no replication ACKs.
    pub trusted_cluster: bool,
    pub node_id: String,
    pub listen: Option<std::net::SocketAddr>,
    pub certificate_file: PathBuf,
    pub private_key_file: PathBuf,
    pub client_ca_file: PathBuf,
    pub peers: Vec<AssistPeer>,
    pub upstream_peer: Option<String>,
    pub start_assisted: bool,
    pub automatic: bool,
    pub enter_after_blocks: u64,
    pub enter_after_checks: u32,
    pub cooldown_seconds: u64,
    pub retain_blocks: u64,
    pub max_journal_bytes: u64,
    pub max_block_bytes: usize,
    /// Bootstrap an empty data directory before opening any ledger/RPC.
    pub bootstrap: bool,
    /// Trusted genesis ID for private networks; absent means built-in mainnet.
    pub bootstrap_genesis: Option<String>,
    /// Change only to explicitly replace an unfinished bootstrap session.
    pub bootstrap_reset_token: String,
    pub max_snapshot_bytes: u64,
}

impl Default for AssistConfig {
    fn default() -> Self {
        Self {
            enable: false,
            trusted_cluster: false,
            node_id: String::new(),
            listen: None,
            certificate_file: PathBuf::new(),
            private_key_file: PathBuf::new(),
            client_ca_file: PathBuf::new(),
            peers: Vec::new(),
            upstream_peer: None,
            start_assisted: false,
            automatic: true,
            enter_after_blocks: 3,
            enter_after_checks: 3,
            cooldown_seconds: 30,
            retain_blocks: 10_000,
            max_journal_bytes: 32 * 1024 * 1024 * 1024,
            max_block_bytes: 64 * 1024 * 1024,
            bootstrap: true,
            bootstrap_genesis: None,
            bootstrap_reset_token: String::new(),
            max_snapshot_bytes: 1024 * 1024 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AssistPeer {
    pub id: String,
    // Omit endpoint to accept inbound connections only.
    pub endpoint: Option<String>,
    pub tls_domain: String,
    pub server_ca_file: PathBuf,
    // SHA256 of the peer's leaf DER certificate. Required for inbound peers.
    pub certificate_sha256: String,
    pub allow_donate: bool,
    pub allow_receive: bool,
    pub required_for_retention: bool,
}

impl AssistConfig {
    pub fn validate(&self) -> Result<(), String> {
        if !self.enable {
            return Ok(());
        }
        fn valid_id(s: &str) -> bool {
            !s.is_empty()
                && s.len() <= 64
                && s.bytes()
                    .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
        }
        if !valid_id(&self.node_id) {
            return Err("assist.node_id must be a nonempty safe identifier".into());
        }
        for p in [
            &self.certificate_file,
            &self.private_key_file,
            &self.client_ca_file,
        ] {
            if p.as_os_str().is_empty() {
                return Err(
                    "assist requires mTLS certificate, private key and client CA files".into(),
                );
            }
        }
        if self.max_block_bytes < 1024 || self.max_block_bytes > 256 * 1024 * 1024 {
            return Err("assist.max_block_bytes must be 1 KiB..256 MiB".into());
        }
        if self.max_journal_bytes < self.max_block_bytes as u64
            || self.retain_blocks == 0
            || self.enter_after_blocks == 0
            || self.enter_after_checks == 0
        {
            return Err(
                "assist journal/retention/automatic limits must be positive and consistent".into(),
            );
        }
        let mut ids = HashSet::new();
        let mut certs = HashSet::new();
        for peer in &self.peers {
            if !valid_id(&peer.id) || peer.id == self.node_id || !ids.insert(&peer.id) {
                return Err(
                    "assist peer identifiers must be unique and different from node_id".into(),
                );
            }
            let cert = hex::decode(&peer.certificate_sha256)
                .map_err(|_| "assist peer certificate_sha256 must be 64 hex characters")?;
            if cert.len() != 32 || !certs.insert(cert) {
                return Err(
                    "assist peer certificates must be unique 32-byte SHA256 fingerprints".into(),
                );
            }
            if let Some(endpoint) = &peer.endpoint {
                if !endpoint.starts_with("https://")
                    || peer.tls_domain.is_empty()
                    || peer.server_ca_file.as_os_str().is_empty()
                {
                    return Err("assist outbound connections require https://, tls_domain and a per-peer server_ca_file".into());
                }
            }
            if !self.trusted_cluster && !peer.allow_donate && !peer.allow_receive {
                return Err("assist peer must grant at least one direction".into());
            }
        }
        if self.trusted_cluster {
            if self.upstream_peer.is_some() {
                return Err(
                    "trusted cluster selects sources dynamically; omit upstream_peer".into(),
                );
            }
        } else if let Some(id) = &self.upstream_peer {
            if !self.peers.iter().any(|p| &p.id == id && p.allow_receive) {
                return Err(
                    "assist.upstream_peer must name a peer permitted to donate to this node".into(),
                );
            }
        } else if self.start_assisted {
            return Err("assist.start_assisted requires upstream_peer".into());
        }
        Ok(())
    }
}
