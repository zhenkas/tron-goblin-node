//! Online history maintenance; deliberately absent from block execution and ACK.
use crate::{config::HistoryPruningConfig, firehose::FirehoseWriter, storage::OpenedStores};
use axum::{
    extract::{DefaultBodyLimit, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use prost::Message;
use serde::Deserialize;
use serde_json::{json, Value};
#[cfg(test)]
use tron_chainbase::RocksDbBackend;

use std::{
    path::Path,
    sync::{
        atomic::{AtomicI64, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tron_chainbase::{BlockIndexStore, BlockStore, DynamicPropertiesStore, KvBackend, WriteOp};
use tron_index::{HistoryAck, HistoryControl};

pub const CONSENSUS_MIN_BLOCKS: u64 = 65_536;
#[derive(Clone)]
pub struct HistoryStores {
    blocks: Arc<dyn KvBackend>,
    index: Arc<dyn KvBackend>,
    ret: Arc<dyn KvBackend>,
    transactions: Arc<dyn KvBackend>,
    tx_history: Arc<dyn KvBackend>,
    undo: Arc<dyn KvBackend>,
    props: Arc<dyn KvBackend>,
}
impl From<&OpenedStores> for HistoryStores {
    fn from(s: &OpenedStores) -> Self {
        Self {
            blocks: s.blocks.clone(),
            index: s.block_index.clone(),
            ret: s.transaction_ret.clone(),
            transactions: s.transactions.clone(),
            tx_history: s.tx_history.clone(),
            undo: s.block_undo.clone(),
            props: s.dyn_props.clone(),
        }
    }
}
pub struct HistoryPruner {
    pub control: Arc<HistoryControl>,
    pub config: HistoryPruningConfig,
    stores: HistoryStores,
    meta: Arc<dyn KvBackend>,
    writer: Arc<FirehoseWriter>,
    pub blocks_removed: AtomicU64,
    pub segments_removed: AtomicU64,
    pub undo_removed: AtomicU64,
    undo_prune_before: AtomicI64,
}
fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}
pub fn prune_floor(head: i64, solid: i64, ack: i64, keep: u64) -> i64 {
    let keep = keep.max(CONSENSUS_MIN_BLOCKS).min(i64::MAX as u64) as i64;
    ack.min(solid.saturating_add(1))
        .min(head.saturating_sub(keep).saturating_add(1))
        .max(1)
}
/// Undo serves reorgs, not consumer replay. Keep every unfinalized block even
/// if finalization temporarily falls behind the configured target window.
/// Crash recovery rolls committed writes forward through checkpoint manifests.
pub fn undo_floor(head: i64, solid: i64, keep: u64) -> i64 {
    head.saturating_sub(keep.min(i64::MAX as u64) as i64)
        .saturating_add(1)
        .min(solid.saturating_add(1))
        .max(1)
}
impl HistoryPruner {
    pub fn open(
        path: &Path,
        config: HistoryPruningConfig,
        stores: &OpenedStores,
        writer: Arc<FirehoseWriter>,
    ) -> Result<Self, String> {
        let provider = tron_chainbase::KvProvider {
            rocks_tuning: Some((1, 32)),
            ..Default::default()
        };
        Self::open_with_provider(path, config, stores, writer, &provider)
    }
    pub fn open_with_provider(
        path: &Path,
        config: HistoryPruningConfig,
        stores: &OpenedStores,
        writer: Arc<FirehoseWriter>,
        provider: &tron_chainbase::KvProvider,
    ) -> Result<Self, String> {
        config.validate()?;
        let meta = provider.open(path, "history-pruning").map_err(err)?;
        Self::from_parts(config, HistoryStores::from(stores), meta, writer)
    }
    fn from_parts(
        config: HistoryPruningConfig,
        stores: HistoryStores,
        meta: Arc<dyn KvBackend>,
        writer: Arc<FirehoseWriter>,
    ) -> Result<Self, String> {
        let floor = match meta.get(b"floor").map_err(err)? {
            Some(v) if v.len() == 8 => i64::from_be_bytes(v.try_into().unwrap()),
            Some(_) => return Err("invalid persisted history floor".into()),
            None => 1,
        };
        if floor < 1 {
            return Err("invalid history floor".into());
        }
        if floor > 1 {
            let raw = meta
                .get(b"anchor")
                .map_err(err)?
                .ok_or("history floor has no canonical anchor")?;
            if raw.len() != 40 {
                return Err("invalid history anchor".into());
            }
            let height = i64::from_be_bytes(raw[..8].try_into().unwrap());
            let canonical = BlockIndexStore::new(stores.index.clone())
                .get(height)
                .map_err(err)?;
            if canonical.as_bytes().as_slice() != &raw[8..] {
                return Err("history metadata belongs to a different canonical chain".into());
            }
            let head = DynamicPropertiesStore::new(stores.props.clone())
                .latest_block_header_number()
                .ok_or("missing head")?;
            if floor > head.saturating_sub(CONSENSUS_MIN_BLOCKS as i64 - 1).max(1) {
                return Err("persisted history floor violates the consensus reserve".into());
            }
        }
        let control = Arc::new(HistoryControl::new(floor));
        writer.manage_history(control.clone());
        Ok(Self {
            control,
            config,
            stores,
            meta,
            writer,
            blocks_removed: AtomicU64::new(0),
            segments_removed: AtomicU64::new(0),
            undo_removed: AtomicU64::new(0),
            undo_prune_before: AtomicI64::new(1),
        })
    }
    /// Validate off the ACK path. Publishing a floor never deletes its ACK anchor.
    pub fn begin_pass(&self) -> Result<i64, String> {
        let Some(ack) = self.control.ack() else {
            return Ok(self.control.floor());
        };
        let props = DynamicPropertiesStore::new(self.stores.props.clone());
        let head = props.latest_block_header_number().ok_or("missing head")?;
        let solid = props
            .latest_solidified_block_num()
            .ok_or("missing solidified height")?;
        if ack.height > solid {
            return Err("ACK is not solidified yet; pruning held".into());
        }
        let canonical = BlockIndexStore::new(self.stores.index.clone())
            .get(ack.height)
            .map_err(err)?;
        if canonical.as_bytes() != &ack.block_id {
            return Err("ACK hash is not canonical; pruning held".into());
        }
        let floor = prune_floor(head, solid, ack.height, self.config.retain_blocks)
            .max(self.control.floor());
        if floor > self.control.floor() {
            // Maintenance metadata only. Does not change node/apply fsync policy.
            let mut anchor = ack.height.to_be_bytes().to_vec();
            anchor.extend_from_slice(&ack.block_id);
            self.meta
                .write_batch_sync(&[
                    WriteOp::Put(b"floor".to_vec(), floor.to_be_bytes().to_vec()),
                    WriteOp::Put(b"anchor".to_vec(), anchor),
                ])
                .map_err(err)?;
            self.control.publish_floor(floor);
        }
        Ok(floor)
    }
    /// At most batch_blocks bodies in a batch. No consensus-state keys are written.
    pub fn prune_batch(&self, floor: i64) -> Result<usize, String> {
        if floor > self.control.floor() {
            return Err("unpublished history floor".into());
        }
        let mut start = [0u8; 32];
        start[..8].copy_from_slice(&1i64.to_be_bytes());
        let rows = self
            .stores
            .blocks
            .scan_from(&start, self.config.batch_blocks)
            .map_err(err)?;
        let mut bodies = Vec::new();
        let mut refs = Vec::new();
        let mut infos = Vec::new();
        let bs = BlockStore::new(self.stores.blocks.clone());
        for (key, raw) in rows {
            if key.len() != 32 {
                return Err("invalid block key during pruning".into());
            }
            let height = i64::from_be_bytes(key[..8].try_into().unwrap());
            if height >= floor {
                break;
            }
            let block = tron_proto::Block::decode(raw.as_slice()).map_err(err)?;
            let id = tron_types::BlockId::from_raw(key.as_slice().try_into().unwrap());
            for txid in bs.tx_ids_for(&id, &block) {
                // Retain a reference or receipt that now belongs to a newer inclusion.
                if self.stores.transactions.get(&txid).map_err(err)?.as_deref()
                    == Some(&height.to_be_bytes())
                {
                    refs.push(WriteOp::Delete(txid.to_vec()));
                }
                if let Some(raw) = self.stores.tx_history.get(&txid).map_err(err)? {
                    let info = tron_proto::TransactionInfo::decode(raw.as_slice()).map_err(err)?;
                    if info.block_number == height {
                        infos.push(WriteOp::Delete(txid.to_vec()));
                    }
                }
            }
            bodies.push(WriteOp::Delete(key));
        }
        // Reference deletion must survive before the only tx-id source is removed.
        // Re-running a partial maintenance batch is idempotent.
        if !refs.is_empty() {
            self.stores
                .transactions
                .write_batch_sync(&refs)
                .map_err(err)?;
        }
        if !infos.is_empty() {
            self.stores
                .tx_history
                .write_batch_sync(&infos)
                .map_err(err)?;
        }
        let mut auxiliary = 0;
        for backend in [&self.stores.ret, &self.stores.index] {
            let mut ops = Vec::new();
            for (key, _) in backend
                .scan_from(&1i64.to_be_bytes(), self.config.batch_blocks)
                .map_err(err)?
            {
                let receipt_balance =
                    Arc::ptr_eq(backend, &self.stores.ret) && key.len() == 9 && key[8] == b'B';
                if key.len() != 8 && !receipt_balance {
                    return Err("invalid height-keyed history row".into());
                }
                if i64::from_be_bytes(key[..8].try_into().unwrap()) >= floor {
                    break;
                }
                ops.push(WriteOp::Delete(key));
            }
            if !ops.is_empty() {
                backend.write_batch_sync(&ops).map_err(err)?;
                auxiliary += ops.len();
            }
        }
        if !bodies.is_empty() {
            self.stores.blocks.write_batch_sync(&bodies).map_err(err)?;
        }
        self.blocks_removed
            .fetch_add(bodies.len() as u64, Ordering::Relaxed);
        Ok(bodies.len() + auxiliary)
    }
    /// No ACK dependency and no changes to block bodies, receipts or replay floors.
    /// Peek before loading a batch: at the tip only a few new rows need deletion.
    pub fn prune_undo_batch(&self) -> Result<usize, String> {
        let props = DynamicPropertiesStore::new(self.stores.props.clone());
        let head = props.latest_block_header_number().ok_or("missing head")?;
        let solid = props
            .latest_solidified_block_num()
            .ok_or("missing solidified height")?;
        if head < 0 || solid < 0 || solid > head {
            return Err("inconsistent chain horizon; undo pruning held".into());
        }
        let floor = undo_floor(head, solid, self.config.undo_retain_blocks);
        self.undo_prune_before.store(floor, Ordering::Relaxed);
        let start = 1i64.to_be_bytes();
        let first = self.stores.undo.scan_from(&start, 1).map_err(err)?;
        let Some((key, _)) = first.first() else {
            return Ok(0);
        };
        if key.len() != 8 {
            return Err("invalid undo height key".into());
        }
        let oldest = i64::from_be_bytes(key.as_slice().try_into().unwrap());
        if oldest >= floor {
            return Ok(0);
        }
        let limit = (floor - oldest).min(self.config.batch_blocks as i64) as usize;
        let mut ops = Vec::new();
        for (key, _) in self.stores.undo.scan_from(&start, limit).map_err(err)? {
            if key.len() != 8 {
                return Err("invalid undo height key".into());
            }
            if i64::from_be_bytes(key.as_slice().try_into().unwrap()) >= floor {
                break;
            }
            ops.push(WriteOp::Delete(key));
        }
        if !ops.is_empty() {
            self.stores.undo.write_batch_sync(&ops).map_err(err)?;
            self.undo_removed
                .fetch_add(ops.len() as u64, Ordering::Relaxed);
        }
        Ok(ops.len())
    }
    pub fn prune_segment(&self, floor: i64) -> Result<bool, String> {
        let removed = self.writer.prune_history_segment(floor)?;
        self.segments_removed
            .fetch_add(removed as u64, Ordering::Relaxed);
        Ok(removed)
    }
    pub fn status(&self) -> Value {
        let ack = self
            .control
            .ack()
            .map(|a| json!({"height":a.height,"block_id":hex::encode(a.block_id)}));
        json!({"ack":ack,"ack_persistent":false,"prune_before":self.control.floor(),
            "retain_blocks":self.config.retain_blocks,"undo_retain_blocks":self.config.undo_retain_blocks,
            "undo_prune_before":self.undo_prune_before.load(Ordering::Relaxed),"undo_removed":self.undo_removed.load(Ordering::Relaxed),
            "undo_interval_seconds":self.config.interval_seconds.min(30),"batch_blocks":self.config.batch_blocks,
            "interval_seconds":self.config.interval_seconds,"blocks_removed":self.blocks_removed.load(Ordering::Relaxed),
            "segments_removed":self.segments_removed.load(Ordering::Relaxed)})
    }
}
#[derive(Deserialize)]
struct AckRequest {
    height: i64,
    block_id: String,
}
async fn acknowledge(
    State(p): State<Arc<HistoryPruner>>,
    Json(body): Json<AckRequest>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let bytes = hex::decode(&body.block_id)
        .map_err(|_| (StatusCode::BAD_REQUEST, "invalid block_id hex".into()))?;
    let block_id: [u8; 32] = bytes
        .try_into()
        .map_err(|_| (StatusCode::BAD_REQUEST, "block_id must be 32 bytes".into()))?;
    if block_id[..8] != body.height.to_be_bytes() {
        return Err((
            StatusCode::BAD_REQUEST,
            "block_id height prefix does not match height".into(),
        ));
    }
    let changed = p
        .control
        .accept(HistoryAck {
            height: body.height,
            block_id,
        })
        .map_err(|e| (StatusCode::CONFLICT, e.to_string()))?;
    Ok(Json(
        json!({"accepted":true,"changed":changed,"persistent":false,"prune_started":false}),
    ))
}
async fn status(State(p): State<Arc<HistoryPruner>>) -> Json<Value> {
    Json(p.status())
}
pub fn router(pruner: Arc<HistoryPruner>) -> Router {
    Router::new()
        .route("/history/ack", post(acknowledge))
        .route("/history", get(status))
        .layer(DefaultBodyLimit::max(4096))
        .with_state(pruner)
}
pub async fn run(pruner: Arc<HistoryPruner>, shutdown_signal: crate::ShutdownSignal) {
    // Independent futures: a large existing undo backlog cannot postpone history maintenance.
    tokio::join!(
        run_history(pruner.clone(), shutdown_signal.clone()),
        run_undo(pruner, shutdown_signal)
    );
}
async fn run_undo(pruner: Arc<HistoryPruner>, shutdown_signal: crate::ShutdownSignal) {
    let mut shutdown = shutdown_signal.subscribe();
    if shutdown_signal.is_shutdown() {
        return;
    }
    let period = Duration::from_secs(pruner.config.interval_seconds.min(30));
    let mut timer = tokio::time::interval(period);
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! { _=shutdown.recv()=>return, _=timer.tick()=>{} }
        loop {
            let p = pruner.clone();
            match tokio::task::spawn_blocking(move || p.prune_undo_batch()).await {
                Ok(Ok(0)) => break,
                Ok(Ok(_)) => {}
                other => {
                    tracing::warn!(result=?other,"undo maintenance held; retry on next tick");
                    break;
                }
            }
            tokio::select! { _=shutdown.recv()=>return, _=tokio::time::sleep(Duration::from_millis(5))=>{} }
        }
    }
}
async fn run_history(pruner: Arc<HistoryPruner>, shutdown_signal: crate::ShutdownSignal) {
    let mut shutdown = shutdown_signal.subscribe();
    if shutdown_signal.is_shutdown() {
        return;
    }
    let period = Duration::from_secs(pruner.config.interval_seconds);
    let startup_delay = period.min(Duration::from_secs(30));
    let mut timer = tokio::time::interval_at(tokio::time::Instant::now() + startup_delay, period);
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! { _=shutdown.recv()=>return, _=timer.tick()=>{} }
        let p = pruner.clone();
        let floor = match tokio::task::spawn_blocking(move || p.begin_pass()).await {
            Ok(Ok(f)) => f,
            other => {
                tracing::warn!(result=?other,"history pruning held");
                continue;
            }
        };
        loop {
            let p = pruner.clone();
            match tokio::task::spawn_blocking(move || p.prune_batch(floor)).await {
                Ok(Ok(0)) => break,
                Ok(Ok(_)) => {}
                other => {
                    tracing::error!(result=?other,"history batch failed; resume on next interval");
                    break;
                }
            }
            tokio::select! {_=shutdown.recv()=>return,_=tokio::time::sleep(Duration::from_millis(5))=>{}}
        }
        loop {
            let p = pruner.clone();
            match tokio::task::spawn_blocking(move || p.prune_segment(floor)).await {
                Ok(Ok(true)) => {}
                Ok(Ok(false)) => break,
                other => {
                    tracing::warn!(result=?other,"history segment cleanup held");
                    break;
                }
            }
            tokio::select! {_=shutdown.recv()=>return,_=tokio::task::yield_now()=>{}}
        }
        tracing::info!(
            floor,
            blocks = pruner.blocks_removed.load(Ordering::Relaxed),
            "history maintenance pass complete"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use tron_chainbase::MemBackend;
    fn mem() -> Arc<dyn KvBackend> {
        Arc::new(MemBackend::new())
    }
    fn stores() -> HistoryStores {
        HistoryStores {
            blocks: mem(),
            index: mem(),
            ret: mem(),
            transactions: mem(),
            tx_history: mem(),
            undo: mem(),
            props: mem(),
        }
    }
    fn id(height: i64) -> [u8; 32] {
        let mut id = [0u8; 32];
        id[..8].copy_from_slice(&height.to_be_bytes());
        id
    }
    fn setup(s: &HistoryStores, head: i64, solid: i64) {
        let p = DynamicPropertiesStore::new(s.props.clone());
        p.save_latest_block_header_number(head);
        p.save_latest_solidified_block_num(solid);
        s.index.put(&solid.to_be_bytes(), &id(solid)).unwrap();
    }
    fn add_block(
        s: &HistoryStores,
        height: i64,
        variant: i64,
        with_tx: bool,
    ) -> (tron_types::BlockId, Vec<[u8; 32]>) {
        let block = tron_proto::Block {
            block_header: Some(tron_proto::BlockHeader {
                raw_data: Some(tron_proto::block_header::Raw {
                    number: height,
                    timestamp: height * 3000 + variant,
                    parent_hash: vec![0; 32],
                    ..Default::default()
                }),
                ..Default::default()
            }),
            transactions: if with_tx {
                vec![tron_proto::Transaction {
                    raw_data: Some(tron_proto::transaction::Raw {
                        timestamp: height,
                        ..Default::default()
                    }),
                    ..Default::default()
                }]
            } else {
                vec![]
            },
        };
        let id = tron_types::block_id_from_block(&block).unwrap();
        let bs = BlockStore::new(s.blocks.clone());
        bs.put(&id, &block).unwrap();
        BlockIndexStore::new(s.index.clone()).put(&id).unwrap();
        let ids = bs.tx_ids_for(&id, &block);
        for txid in &ids {
            s.transactions.put(txid, &height.to_be_bytes()).unwrap();
            s.tx_history
                .put(
                    txid,
                    &tron_proto::TransactionInfo {
                        block_number: height,
                        ..Default::default()
                    }
                    .encode_to_vec(),
                )
                .unwrap();
        }
        s.ret
            .put(
                &height.to_be_bytes(),
                &tron_proto::TransactionRet::default().encode_to_vec(),
            )
            .unwrap();
        s.undo.put(&height.to_be_bytes(), b"undo").unwrap();
        (id, ids)
    }
    fn pruner(s: HistoryStores, meta: Arc<dyn KvBackend>, dir: &Path) -> HistoryPruner {
        let writer = Arc::new(
            FirehoseWriter::open(
                dir,
                u64::MAX,
                s.blocks.clone(),
                s.index.clone(),
                s.ret.clone(),
                s.props.clone(),
            )
            .unwrap(),
        );
        HistoryPruner::from_parts(
            HistoryPruningConfig {
                retain_blocks: 65536,
                batch_blocks: 2,
                ..Default::default()
            },
            s,
            meta,
            writer,
        )
        .unwrap()
    }
    #[test]
    fn native_balance_sidecars_follow_the_receipt_history_floor() {
        let s = stores();
        setup(&s, 65540, 65538);
        add_block(&s, 1, 0, false);
        add_block(&s, 5, 0, false);
        for h in [1, 5] {
            s.ret
                .put(&crate::firehose::balance_key(h), b"captured")
                .unwrap();
        }
        let dir = tempfile::tempdir().unwrap();
        let p = pruner(s.clone(), mem(), dir.path());
        p.control
            .accept(HistoryAck {
                height: 65538,
                block_id: id(65538),
            })
            .unwrap();
        let floor = p.begin_pass().unwrap();
        assert_eq!(floor, 5);
        for _ in 0..4 {
            p.prune_batch(floor).unwrap();
        }
        assert!(s.ret.get(&1i64.to_be_bytes()).unwrap().is_none());
        assert!(s
            .ret
            .get(&crate::firehose::balance_key(1))
            .unwrap()
            .is_none());
        assert!(s.ret.get(&5i64.to_be_bytes()).unwrap().is_some());
        assert!(s
            .ret
            .get(&crate::firehose::balance_key(5))
            .unwrap()
            .is_some());
    }

    #[test]
    fn floor_respects_ack_solid_and_minimum_reserve() {
        assert_eq!(prune_floor(100000, 99980, 90000, 70000), 30001);
        assert_eq!(prune_floor(100000, 99980, 20000, 70000), 20000);
        assert_eq!(prune_floor(100000, 99980, 90000, 0), 34465);
        assert_eq!(prune_floor(20, 19, 18, 65536), 1);
    }
    #[test]
    fn no_ack_does_not_prune_and_bad_or_unfinalized_ack_is_held() {
        let s = stores();
        setup(&s, 65540, 65538);
        add_block(&s, 1, 0, false);
        let dir = tempfile::tempdir().unwrap();
        let p = pruner(s.clone(), mem(), dir.path());
        assert_eq!(p.begin_pass().unwrap(), 1);
        assert_eq!(p.prune_batch(1).unwrap(), 0);
        p.control
            .accept(HistoryAck {
                height: 65538,
                block_id: [9; 32],
            })
            .unwrap();
        assert!(p.begin_pass().is_err());
        assert_eq!(p.control.floor(), 1);
        p.control
            .accept(HistoryAck {
                height: 65539,
                block_id: id(65539),
            })
            .unwrap();
        assert!(p.begin_pass().unwrap_err().contains("not solidified"));
    }
    #[test]
    fn bounded_batches_remove_all_history_keep_current_and_ack_anchor() {
        let s = stores();
        setup(&s, 65540, 65538);
        let (one, ids) = add_block(&s, 1, 0, true);
        let (_two, _) = add_block(&s, 2, 0, false);
        add_block(&s, 2, 1, false);
        add_block(&s, 3, 0, false);
        let (recent, _) = add_block(&s, 65540, 0, false);
        s.props.put(b"state-sentinel", b"unchanged").unwrap();
        let dir = tempfile::tempdir().unwrap();
        let p = pruner(s.clone(), mem(), dir.path());
        p.control
            .accept(HistoryAck {
                height: 65538,
                block_id: id(65538),
            })
            .unwrap();
        let floor = p.begin_pass().unwrap();
        assert_eq!(floor, 5);
        p.prune_batch(floor).unwrap();
        assert_eq!(p.blocks_removed.load(Ordering::Relaxed), 2);
        assert!(!s.blocks.contains(one.as_bytes()).unwrap());
        assert_eq!(s.blocks.scan_from(&[0u8; 32], 100).unwrap().len(), 3);
        assert_eq!(s.transactions.get(&ids[0]).unwrap(), None);
        assert_eq!(s.tx_history.get(&ids[0]).unwrap(), None);
        while p.prune_batch(floor).unwrap() != 0 {}
        assert_eq!(p.blocks_removed.load(Ordering::Relaxed), 4);
        assert!(s.blocks.contains(recent.as_bytes()).unwrap());
        assert!(s.index.contains(&65538i64.to_be_bytes()).unwrap());
        assert_eq!(
            s.props.get(b"state-sentinel").unwrap(),
            Some(b"unchanged".to_vec())
        );
        assert!(s.undo.contains(&1i64.to_be_bytes()).unwrap());
        for backend in [&s.ret, &s.index] {
            assert!(!backend.contains(&1i64.to_be_bytes()).unwrap());
        }
    }
    #[test]
    fn newer_transaction_inclusion_and_receipt_are_retained() {
        let s = stores();
        setup(&s, 65540, 65538);
        let (_, ids) = add_block(&s, 1, 0, true);
        s.transactions
            .put(&ids[0], &65540i64.to_be_bytes())
            .unwrap();
        s.tx_history
            .put(
                &ids[0],
                &tron_proto::TransactionInfo {
                    block_number: 65540,
                    ..Default::default()
                }
                .encode_to_vec(),
            )
            .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let p = pruner(s.clone(), mem(), dir.path());
        p.control
            .accept(HistoryAck {
                height: 65538,
                block_id: id(65538),
            })
            .unwrap();
        let f = p.begin_pass().unwrap();
        p.prune_batch(f).unwrap();
        assert_eq!(
            s.transactions.get(&ids[0]).unwrap(),
            Some(65540i64.to_be_bytes().to_vec())
        );
        assert!(s.tx_history.contains(&ids[0]).unwrap());
    }
    struct FailSync {
        inner: Arc<dyn KvBackend>,
        fail: AtomicBool,
        delay: Duration,
    }
    impl KvBackend for FailSync {
        fn get(&self, k: &[u8]) -> Result<Option<Vec<u8>>, tron_chainbase::KvError> {
            self.inner.get(k)
        }
        fn put(&self, k: &[u8], v: &[u8]) -> Result<(), tron_chainbase::KvError> {
            self.inner.put(k, v)
        }
        fn delete(&self, k: &[u8]) -> Result<(), tron_chainbase::KvError> {
            self.inner.delete(k)
        }
        fn scan_from(
            &self,
            k: &[u8],
            n: usize,
        ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, tron_chainbase::KvError> {
            self.inner.scan_from(k, n)
        }
        fn write_batch_sync(&self, ops: &[WriteOp]) -> Result<(), tron_chainbase::KvError> {
            if self.fail.swap(false, Ordering::SeqCst) {
                return Err(tron_chainbase::KvError::Backend(
                    "injected maintenance crash".into(),
                ));
            }
            if !self.delay.is_zero() {
                std::thread::sleep(self.delay);
            }
            self.inner.write_batch_sync(ops)
        }
    }
    #[test]
    fn interrupted_batch_retries_with_body_retained_until_refs_are_durable() {
        let mut s = stores();
        setup(&s, 65540, 65538);
        let wrapper = Arc::new(FailSync {
            inner: s.blocks.clone(),
            fail: AtomicBool::new(false),
            delay: Duration::ZERO,
        });
        s.blocks = wrapper.clone();
        let (old, ids) = add_block(&s, 1, 0, true);
        let dir = tempfile::tempdir().unwrap();
        let p = pruner(s.clone(), mem(), dir.path());
        p.control
            .accept(HistoryAck {
                height: 65538,
                block_id: id(65538),
            })
            .unwrap();
        let f = p.begin_pass().unwrap();
        wrapper.fail.store(true, Ordering::SeqCst);
        assert!(p.prune_batch(f).is_err());
        assert!(s.blocks.contains(old.as_bytes()).unwrap());
        assert!(!s.transactions.contains(&ids[0]).unwrap());
        p.prune_batch(f).unwrap();
        assert!(!s.blocks.contains(old.as_bytes()).unwrap());
    }
    #[tokio::test]
    async fn rest_ack_has_no_database_or_pruning_side_effect_and_restart_loses_it() {
        let s = stores();
        setup(&s, 65540, 65538);
        add_block(&s, 1, 0, false);
        let meta = mem();
        let dir = tempfile::tempdir().unwrap();
        let p = Arc::new(pruner(s.clone(), meta.clone(), dir.path()));
        let _ = acknowledge(
            State(p.clone()),
            Json(AckRequest {
                height: 65538,
                block_id: hex::encode(id(65538)),
            }),
        )
        .await
        .unwrap();
        assert_eq!(p.control.floor(), 1);
        assert_eq!(meta.get(b"floor").unwrap(), None);
        assert_eq!(p.blocks_removed.load(Ordering::Relaxed), 0);
        assert_eq!(p.undo_removed.load(Ordering::Relaxed), 0);
        assert!(s.undo.contains(&1i64.to_be_bytes()).unwrap());
        let floor = p.begin_pass().unwrap();
        assert_eq!(floor, 5);
        let restarted =
            HistoryPruner::from_parts(p.config.clone(), s, meta, p.writer.clone()).unwrap();
        assert_eq!(restarted.control.floor(), 5);
        assert_eq!(restarted.control.ack(), None);
    }

    #[test]
    fn invalid_persisted_floor_or_chain_is_rejected_before_cleanup() {
        let s = stores();
        setup(&s, 65540, 65538);
        let dir = tempfile::tempdir().unwrap();
        let p = pruner(s.clone(), mem(), dir.path());
        for (floor, anchor) in [
            (vec![1], None),
            (5i64.to_be_bytes().to_vec(), None),
            (5i64.to_be_bytes().to_vec(), Some(vec![1])),
            (
                5i64.to_be_bytes().to_vec(),
                Some([65538i64.to_be_bytes().as_slice(), &[9u8; 32]].concat()),
            ),
            (
                100i64.to_be_bytes().to_vec(),
                Some([65538i64.to_be_bytes().as_slice(), &id(65538)].concat()),
            ),
        ] {
            let meta = mem();
            meta.put(b"floor", &floor).unwrap();
            if let Some(anchor) = anchor {
                meta.put(b"anchor", &anchor).unwrap();
            }
            assert!(
                HistoryPruner::from_parts(p.config.clone(), s.clone(), meta, p.writer.clone())
                    .is_err()
            );
        }
    }
    #[test]
    fn failed_floor_persistence_never_authorizes_deletion() {
        let s = stores();
        setup(&s, 65540, 65538);
        let (old, _) = add_block(&s, 1, 0, false);
        let meta = Arc::new(FailSync {
            inner: mem(),
            fail: AtomicBool::new(true),
            delay: Duration::ZERO,
        });
        let dir = tempfile::tempdir().unwrap();
        let p = pruner(s.clone(), meta, dir.path());
        p.control
            .accept(HistoryAck {
                height: 65538,
                block_id: id(65538),
            })
            .unwrap();
        assert!(p.begin_pass().is_err());
        assert_eq!(p.control.floor(), 1);
        assert!(p.prune_batch(5).is_err());
        assert!(s.blocks.contains(old.as_bytes()).unwrap());
    }
    #[tokio::test]
    async fn socket_ack_waits_for_independent_timer_and_shutdown_is_sticky() {
        let s = stores();
        setup(&s, 65540, 65538);
        let (old, _) = add_block(&s, 1, 0, false);
        let dir = tempfile::tempdir().unwrap();
        let mut p = pruner(s.clone(), mem(), dir.path());
        p.config.interval_seconds = 1;
        let p = Arc::new(p);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router(p.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let shutdown = crate::ShutdownSignal::new();
        let maintenance = tokio::spawn(run(p.clone(), shutdown.clone()));
        tokio::task::yield_now().await;
        let body = json!({"height":65538,"block_id":hex::encode(id(65538))}).to_string();
        let response=tokio::task::spawn_blocking(move||{
            use std::io::{Read,Write};
            let mut socket=std::net::TcpStream::connect(addr).unwrap();
            socket.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
            write!(socket,"POST /history/ack HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",body.len(),body).unwrap();
            let mut result=String::new();socket.read_to_string(&mut result).unwrap();result
        }).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 200"));
        assert!(response.contains("\"prune_started\":false"));
        assert_eq!(p.control.floor(), 1);
        assert!(s.blocks.contains(old.as_bytes()).unwrap());
        tokio::time::timeout(Duration::from_secs(3), async {
            while s.blocks.contains(old.as_bytes()).unwrap() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(p.control.floor(), 5);
        shutdown.shutdown();
        tokio::time::timeout(Duration::from_secs(1), maintenance)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), run(p, shutdown))
            .await
            .unwrap();
        server.abort();
    }
    #[test]
    fn undo_window_keeps_boundary_and_all_unfinalized_blocks() {
        assert_eq!(undo_floor(100000, 99982, 10000), 90001);
        assert_eq!(undo_floor(20, 18, 10000), 1);
        assert_eq!(undo_floor(100000, 80000, 10000), 80001);
        assert_eq!(undo_floor(i64::MAX, i64::MAX - 18, 10000), i64::MAX - 9999);
    }
    #[test]
    fn undo_prunes_without_ack_in_bounded_batches_and_preserves_replay_data() {
        let s = stores();
        setup(&s, 10010, 10008);
        let (body, ids) = add_block(&s, 1, 0, true);
        for h in [2i64, 9, 10, 11, 10010] {
            s.undo.put(&h.to_be_bytes(), b"undo").unwrap();
        }
        s.props.put(b"state-sentinel", b"unchanged").unwrap();
        let dir = tempfile::tempdir().unwrap();
        let p = pruner(s.clone(), mem(), dir.path());
        assert_eq!(p.control.ack(), None);
        assert_eq!(p.prune_undo_batch().unwrap(), 2);
        assert_eq!(p.prune_undo_batch().unwrap(), 2);
        assert_eq!(p.prune_undo_batch().unwrap(), 0);
        assert_eq!(p.undo_removed.load(Ordering::Relaxed), 4);
        assert!(s.undo.contains(&11i64.to_be_bytes()).unwrap());
        assert!(s.undo.contains(&10010i64.to_be_bytes()).unwrap());
        assert!(s.blocks.contains(body.as_bytes()).unwrap());
        assert!(s.index.contains(&1i64.to_be_bytes()).unwrap());
        assert!(s.ret.contains(&1i64.to_be_bytes()).unwrap());
        assert!(s.transactions.contains(&ids[0]).unwrap());
        assert!(s.tx_history.contains(&ids[0]).unwrap());
        assert_eq!(
            s.props.get(b"state-sentinel").unwrap(),
            Some(b"unchanged".to_vec())
        );
        assert_eq!(p.control.floor(), 1);
        // An ACK that cannot authorize history deletion cannot block undo maintenance.
        p.control
            .accept(HistoryAck {
                height: 10009,
                block_id: id(10009),
            })
            .unwrap();
        assert!(p.begin_pass().is_err());
        assert_eq!(p.prune_undo_batch().unwrap(), 0);
    }
    #[test]
    fn stalled_finality_and_inconsistent_horizons_hold_required_undo() {
        let s = stores();
        setup(&s, 20000, 5);
        for h in [5i64, 6, 10001, 20000] {
            s.undo.put(&h.to_be_bytes(), b"undo").unwrap();
        }
        let dir = tempfile::tempdir().unwrap();
        let p = pruner(s.clone(), mem(), dir.path());
        assert_eq!(p.prune_undo_batch().unwrap(), 1);
        assert!(s.undo.contains(&6i64.to_be_bytes()).unwrap());
        setup(&s, 20000, 20001);
        assert!(p.prune_undo_batch().is_err());
        assert!(s.undo.contains(&6i64.to_be_bytes()).unwrap());
    }
    #[test]
    fn undo_failure_retries_without_advancing_counter_and_restart_recomputes_window() {
        let mut s = stores();
        setup(&s, 10010, 10008);
        let wrapper = Arc::new(FailSync {
            inner: s.undo.clone(),
            fail: AtomicBool::new(false),
            delay: Duration::ZERO,
        });
        s.undo = wrapper.clone();
        for h in [1i64, 2, 10, 11] {
            s.undo.put(&h.to_be_bytes(), b"undo").unwrap();
        }
        let dir = tempfile::tempdir().unwrap();
        let p = pruner(s.clone(), mem(), dir.path());
        wrapper.fail.store(true, Ordering::SeqCst);
        assert!(p.prune_undo_batch().is_err());
        assert_eq!(p.undo_removed.load(Ordering::Relaxed), 0);
        assert!(s.undo.contains(&1i64.to_be_bytes()).unwrap());
        assert_eq!(p.prune_undo_batch().unwrap(), 2);
        let restarted =
            HistoryPruner::from_parts(p.config.clone(), s.clone(), mem(), p.writer.clone())
                .unwrap();
        assert_eq!(restarted.prune_undo_batch().unwrap(), 1);
        assert_eq!(restarted.prune_undo_batch().unwrap(), 0);
        assert!(s.undo.contains(&11i64.to_be_bytes()).unwrap());
        assert_eq!(restarted.control.ack(), None);
    }
    #[test]
    fn undo_rocksdb_deletion_survives_close_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let undo_path = dir.path().join("undo");
        {
            let mut s = stores();
            s.undo = Arc::new(RocksDbBackend::open_tuned(&undo_path, 1, 8).unwrap());
            setup(&s, 10003, 10001);
            for h in [1i64, 2, 3, 4] {
                s.undo.put(&h.to_be_bytes(), b"undo").unwrap();
            }
            let p = pruner(s.clone(), mem(), &dir.path().join("journal"));
            assert_eq!(p.prune_undo_batch().unwrap(), 2);
            assert_eq!(p.prune_undo_batch().unwrap(), 1);
        }
        let backend = RocksDbBackend::open_tuned(&undo_path, 1, 8).unwrap();
        assert_eq!(
            backend.scan_from(&1i64.to_be_bytes(), 10).unwrap(),
            vec![(4i64.to_be_bytes().to_vec(), b"undo".to_vec())]
        );
    }
    #[tokio::test]
    async fn undo_startup_maintenance_is_independent_of_hourly_history_and_ack() {
        let s = stores();
        setup(&s, 10010, 10008);
        let (body, _) = add_block(&s, 1, 0, false);
        let dir = tempfile::tempdir().unwrap();
        let mut p = pruner(s.clone(), mem(), dir.path());
        p.config.interval_seconds = 3600;
        let p = Arc::new(p);
        let shutdown = crate::ShutdownSignal::new();
        let maintenance = tokio::spawn(run(p.clone(), shutdown.clone()));
        tokio::time::timeout(Duration::from_secs(2), async {
            while s.undo.contains(&1i64.to_be_bytes()).unwrap() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(s.blocks.contains(body.as_bytes()).unwrap());
        assert_eq!(p.control.floor(), 1);
        shutdown.shutdown();
        tokio::time::timeout(Duration::from_secs(1), maintenance)
            .await
            .unwrap()
            .unwrap();
    }
    #[test]
    fn independent_undo_config_is_backward_compatible_and_validated() {
        let old: HistoryPruningConfig = toml::from_str("retain_blocks = 150000").unwrap();
        assert_eq!(old.undo_retain_blocks, 10000);
        let parsed: HistoryPruningConfig =
            toml::from_str("retain_blocks = 150000\nundo_retain_blocks = 12345").unwrap();
        assert_eq!(parsed.undo_retain_blocks, 12345);
        parsed.validate().unwrap();
        for value in [0u64, i64::MAX as u64 + 1] {
            let mut invalid = parsed.clone();
            invalid.undo_retain_blocks = value;
            assert!(invalid.validate().is_err());
        }
    }
    #[tokio::test]
    async fn slow_undo_backlog_cannot_postpone_history_timer() {
        let mut s = stores();
        setup(&s, 65540, 65538);
        let (old, _) = add_block(&s, 1, 0, false);
        for h in 2i64..80 {
            s.undo.put(&h.to_be_bytes(), b"undo").unwrap();
        }
        s.undo = Arc::new(FailSync {
            inner: s.undo.clone(),
            fail: AtomicBool::new(false),
            delay: Duration::from_millis(100),
        });
        let dir = tempfile::tempdir().unwrap();
        let mut p = pruner(s.clone(), mem(), dir.path());
        p.config.interval_seconds = 1;
        p.control
            .accept(HistoryAck {
                height: 65538,
                block_id: id(65538),
            })
            .unwrap();
        let p = Arc::new(p);
        let shutdown = crate::ShutdownSignal::new();
        let maintenance = tokio::spawn(run(p.clone(), shutdown.clone()));
        tokio::time::timeout(Duration::from_secs(2), async {
            while s.blocks.contains(old.as_bytes()).unwrap() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        // Forty deliberately slow undo batches take >4 seconds. History must finish first.
        assert!(s.undo.contains(&79i64.to_be_bytes()).unwrap());
        assert!(p.undo_removed.load(Ordering::Relaxed) < 79);
        shutdown.shutdown();
        tokio::time::timeout(Duration::from_secs(1), maintenance)
            .await
            .unwrap()
            .unwrap();
    }
}
