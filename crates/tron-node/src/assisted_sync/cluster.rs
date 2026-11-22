//! Source election for trusted peers. Election and imports share the apply lock.
use super::{journal::Result, AssistRuntime};
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};
use tron_grpc::assist_proto as wire;
static CONNECTION: AtomicU64 = AtomicU64::new(1);
pub(super) fn connection_id() -> u64 {
    CONNECTION.fetch_add(1, Ordering::Relaxed)
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceLease {
    pub peer: String,
    pub connection: u64,
    pub epoch: u64,
}
struct PeerView {
    status: wire::Status,
    seen: Instant,
}
#[derive(Default)]
pub struct ClusterState {
    peers: BTreeMap<(String, u64), PeerView>,
    selected: Option<(String, u64)>,
    epoch: u64,
}
impl AssistRuntime {
    pub fn cluster_observe(
        &self,
        peer: &str,
        connection: u64,
        status: wire::Status,
    ) -> Result<Option<SourceLease>> {
        if !self.config.trusted_cluster || !self.config.peers.iter().any(|p| p.id == peer) {
            return Err("cluster peer is not configured".into());
        }
        if status.solid < 0 || status.solid > status.head || status.solid_id.len() != 32 {
            return Err("invalid cluster peer status".into());
        }
        let _writer = self.leadership.lock_apply();
        let head = self.head()?.0;
        let mut state = self.cluster.lock().map_err(|_| "cluster state poisoned")?;
        state.peers.insert(
            (peer.into(), connection),
            PeerView {
                status,
                seen: Instant::now(),
            },
        );
        state
            .peers
            .retain(|_, v| v.seen.elapsed() < Duration::from_secs(10));
        let best = state
            .peers
            .iter()
            .filter(|(_, v)| v.status.retained_from <= head.saturating_add(1))
            .max_by(|(ka, a), (kb, b)| a.status.solid.cmp(&b.status.solid).then_with(|| kb.cmp(ka)))
            .map(|(k, _)| k.clone());
        let selected = state.selected.clone().filter(|k| {
            state
                .peers
                .get(k)
                .is_some_and(|v| v.status.retained_from <= head.saturating_add(1))
        });
        let chosen = match (selected, best) {
            (Some(old), Some(best)) => {
                let current = &state.peers[&old];
                let candidate = &state.peers[&best];
                if current.seen.elapsed() > Duration::from_secs(5)
                    || candidate.status.solid
                        > current
                            .status
                            .solid
                            .saturating_add(self.config.enter_after_blocks as i64)
                {
                    Some(best)
                } else {
                    Some(old)
                }
            }
            (_, best) => best,
        };
        if state.selected != chosen {
            state.selected = chosen;
            state.epoch = state.epoch.wrapping_add(1);
        }
        let epoch = state.epoch;
        let own = (peer.to_string(), connection);
        if state.selected.is_none() {
            drop(state);
            if self.is_assisted() && !self.has_pending_import() {
                self.switch_mode(false)?;
            }
            self.processing.source(None)?;
            return Ok(None);
        }
        if state.selected.as_ref() != Some(&own) {
            return Ok(None);
        }
        let status = state.peers[&own].status.clone();
        drop(state);
        self.observe_status_locked(&status)?;
        self.processing
            .source(if self.is_assisted() { Some(peer) } else { None })?;
        Ok(Some(SourceLease {
            peer: peer.into(),
            connection,
            epoch,
        }))
    }
    pub fn cluster_current(&self, lease: &SourceLease) -> bool {
        self.cluster.lock().is_ok_and(|s| {
            s.epoch == lease.epoch
                && s.selected.as_ref() == Some(&(lease.peer.clone(), lease.connection))
        })
    }
    pub fn cluster_disconnect(&self, peer: &str, connection: u64) -> Result<()> {
        let _writer = self.leadership.lock_apply();
        let mut state = self.cluster.lock().map_err(|_| "cluster state poisoned")?;
        let key = (peer.to_string(), connection);
        state.peers.remove(&key);
        if state.selected.as_ref() == Some(&key) {
            state.selected = None;
            state.epoch = state.epoch.wrapping_add(1);
            drop(state);
            // An imported head is already durable. With no active supplier,
            // normal validation may continue through ordinary network peers.
            if self.is_assisted() && !self.has_pending_import() {
                self.switch_mode(false)?;
            }
            self.processing.source(None)?;
        }
        Ok(())
    }
    pub fn cluster_prometheus(&self) -> String {
        if !self.config.trusted_cluster {
            return String::new();
        }
        let count = self
            .cluster
            .lock()
            .map(|s| {
                s.peers
                    .values()
                    .filter(|p| p.seen.elapsed() < Duration::from_secs(10))
                    .count()
            })
            .unwrap_or(0);
        format!("# TYPE tron_node_cluster_peer_connections gauge\ntron_node_cluster_peer_connections {count}\n# TYPE tron_node_cluster_known_peers gauge\ntron_node_cluster_known_peers {}\n",self.config.peers.len())
    }
}
