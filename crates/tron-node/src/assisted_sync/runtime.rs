use super::{
    journal::{self, Journal, Result},
    AssistConfig,
};
use prost::Message;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Instant,
};
use tron_chainbase::{BlockIndexStore, DynamicPropertiesStore, KvBackend, UndoStoreId, WriteOp};
use tron_executor::{BlockCommitObserver, BlockExecutionReport, CapturedDelta};
use tron_grpc::{assist_proto as wire, firehose_proto as fh};
use tron_proto::Block;
use tron_types::block_id_from_block;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReceiverCursor {
    pub height: i64,
    pub block_id: Vec<u8>,
    pub digest: Vec<u8>,
    pub assisted: bool,
    pub imported_blocks: u64,
}

struct Policy {
    high_checks: u32,
    switched_at: Instant,
}

/// One normal/import writer boundary for the entire process. Network reads
/// never hold it. All mutation work runs on blocking workers.
pub struct AssistRuntime {
    pub config: AssistConfig,
    pub stores: crate::storage::OpenedStores,
    pub network: Vec<u8>,
    pub profile: Vec<u8>,
    pub journal: Mutex<Journal>,
    pub leadership: Arc<crate::sync::SyncLeadership>,
    pub khaos: Arc<tron_consensus::KhaosDb>,
    hook: Arc<crate::index_hook::IndexHook>,
    pub(super) root: PathBuf,
    pub(super) snapshot_lock: Mutex<()>,
    cursor: Mutex<ReceiverCursor>,
    assisted: AtomicBool,
    policy: Mutex<Policy>,
    pub generation: AtomicU64,
    pub(super) cluster: Mutex<super::cluster::ClusterState>,
    pub processing: Arc<super::processing::ProcessingStats>,
}

impl AssistRuntime {
    pub fn open(
        config: AssistConfig,
        data_dir: &std::path::Path,
        stores: crate::storage::OpenedStores,
        hook: Arc<crate::index_hook::IndexHook>,
        leadership: Arc<crate::sync::SyncLeadership>,
        khaos: Arc<tron_consensus::KhaosDb>,
    ) -> Result<Self> {
        config.validate()?;
        let network = BlockIndexStore::new(stores.block_index.clone())
            .get(0)
            .map_err(|e| format!("assist requires initialized genesis: {e}"))?
            .as_bytes()
            .to_vec();
        let profile = Sha256::digest(journal::PROFILE).to_vec();
        let (height, id) = read_head(&stores)?;
        let root = data_dir.join("assisted-sync");
        let journal = Journal::open(
            root.clone(),
            config.clone(),
            network.clone(),
            profile.clone(),
            height,
            id.clone(),
        )?;
        let cursor_path = root.join("receiver.json");
        let cursor: ReceiverCursor = if cursor_path.exists() {
            let mut cursor: ReceiverCursor =
                serde_json::from_slice(&fs::read(&cursor_path).map_err(|e| e.to_string())?)
                    .map_err(|e| format!("receiver cursor: {e}"))?;
            if journal.reset && !cursor.digest.is_empty() {
                // The old chain of record digests ended with the dropped window.
                cursor.digest.clear();
                journal::atomic_write(
                    &cursor_path,
                    &serde_json::to_vec(&cursor).map_err(|e| e.to_string())?,
                )?;
            }
            cursor
        } else {
            ReceiverCursor {
                height,
                block_id: id,
                digest: vec![0; 32],
                assisted: config.start_assisted,
                imported_blocks: 0,
            }
        };
        if cursor.block_id.len() != 32
            || (!cursor.digest.is_empty() && cursor.digest.len() != 32)
            || cursor.height < 0
        {
            return Err("invalid receiver cursor".into());
        }
        let processing = super::processing::ProcessingStats::open(
            stores.common.clone(),
            height,
            cursor.imported_blocks,
            cursor.assisted,
        )?;
        hook.attach_processing_stats(processing.clone());
        let runtime = Self {
            processing,
            config,
            stores,
            network,
            profile,
            journal: Mutex::new(journal),
            leadership,
            khaos,
            hook,
            root,
            snapshot_lock: Mutex::new(()),
            assisted: AtomicBool::new(cursor.assisted),
            cursor: Mutex::new(cursor),
            policy: Mutex::new(Policy {
                high_checks: 0,
                switched_at: Instant::now(),
            }),
            generation: AtomicU64::new(0),
            cluster: Mutex::new(super::cluster::ClusterState::default()),
        };
        runtime.recover()?;
        if runtime.config.trusted_cluster && runtime.is_assisted() {
            let _writer = runtime.leadership.lock_apply();
            runtime.switch_mode(false)?;
        }
        Ok(runtime)
    }

    pub fn is_assisted(&self) -> bool {
        self.assisted.load(Ordering::Acquire)
    }

    pub fn head(&self) -> Result<(i64, Vec<u8>)> {
        read_head(&self.stores)
    }

    pub fn canonical_id(&self, height: i64) -> Result<Vec<u8>> {
        BlockIndexStore::new(self.stores.block_index.clone())
            .get(height)
            .map(|id| id.as_bytes().to_vec())
            .map_err(|e| e.to_string())
    }

    pub fn status(&self) -> Result<wire::Status> {
        let (head, head_id) = self.head()?;
        let solid = DynamicPropertiesStore::new(self.stores.dyn_props.clone())
            .latest_solidified_block_num()
            .ok_or("missing local solid height")?
            .min(head);
        let solid_id = self.canonical_id(solid)?;
        Ok(wire::Status {
            head,
            head_id,
            solid,
            solid_id,
            retained_from: if self.config.keeps_journal() {
                self.journal
                    .lock()
                    .map_err(|_| "journal poisoned")?
                    .meta
                    .floor
            } else {
                i64::MAX // A receiver-only member has nothing to give.
            },
            mode: if self.is_assisted() {
                "assisted"
            } else {
                "autonomous"
            }
            .into(),
        })
    }

    pub fn export_next(&self, request: &wire::Fetch) -> Result<Option<wire::BlockBundle>> {
        let status = self.status()?;
        if request.after_height < 0 || request.after_height > status.solid {
            return Err("ANCHOR_AHEAD: receiver anchor is above donor solid".into());
        }
        if self.canonical_id(request.after_height)? != request.after_id {
            return Err("ANCHOR_MISMATCH: receiver and donor branches differ".into());
        }
        let next = request
            .after_height
            .checked_add(1)
            .ok_or("height overflow")?;
        if next > status.solid {
            return Ok(None);
        }
        let id = self.canonical_id(next)?;
        if !self.config.keeps_journal() {
            return Err("HISTORY_GAP: this member records no block changes".into());
        }
        let journal = self.journal.lock().map_err(|_| "journal poisoned")?;
        if next < journal.meta.floor {
            return Err("HISTORY_GAP: receiver needs a newer consistent snapshot".into());
        }
        let block = journal
            .read(next, &id)?
            .ok_or("HISTORY_GAP: finalized block has no write journal")?;
        if !request.after_semantic_digest.is_empty() {
            let anchor = journal
                .read(request.after_height, &request.after_id)?
                .ok_or("HISTORY_GAP: logical anchor receipt missing")?;
            if request.after_semantic_digest != journal::semantic_digest(&anchor)? {
                return Err("STATE_DIGEST_MISMATCH: logical anchor writes differ".into());
            }
        } else if !request.after_digest.is_empty()
            && request.after_digest != vec![0; 32]
            && request.after_digest != block.previous_digest
        {
            return Err("STATE_DIGEST_MISMATCH: receiver anchor writes differ".into());
        }
        // Prior values stay here; the partner proves them from its own state.
        Ok(Some(journal::bare(&block)))
    }

    pub fn accept_ack(&self, peer: &str, mut ack: wire::Ack) -> Result<()> {
        let status = self.status()?;
        if ack.height > status.solid || self.canonical_id(ack.height)? != ack.block_id {
            return Err("ACK must name a canonical solid block".into());
        }
        let mut journal = self.journal.lock().map_err(|_| "journal poisoned")?;
        if !ack.semantic_digest.is_empty() {
            if journal
                .acks
                .get(peer)
                .is_some_and(|old| old.height == ack.height && old.block_id == ack.block_id)
            {
                return Ok(());
            }
            let canonical = journal
                .read(ack.height, &ack.block_id)?
                .ok_or("ACK receipt unavailable")?;
            if journal::semantic_digest(&canonical)? != ack.semantic_digest {
                return Err("ACK logical writes differ".into());
            }
            ack.digest = canonical.digest;
            ack.semantic_digest.clear();
        }
        journal.acknowledge(peer, ack.clone())?;
        journal.prune(status.solid)?;
        drop(journal);
        super::bootstrap::advance_pin(self, peer, &ack)?;
        Ok(())
    }

    /// Local execution can release donor history too, but only when the
    /// canonical write-set digest is available for the same history chain.
    pub fn local_ack(&self, remote_solid: i64) -> Result<Option<wire::Ack>> {
        let height = self.head()?.0.min(remote_solid);
        let id = self.canonical_id(height)?;
        Ok(self
            .journal
            .lock()
            .map_err(|_| "journal poisoned")?
            .read(height, &id)?
            .map(|block| wire::Ack {
                height,
                block_id: id,
                semantic_digest: journal::semantic_digest(&block).unwrap_or_default(),
                digest: block.digest,
            }))
    }

    pub fn fetch_cursor(&self) -> Result<wire::Fetch> {
        let (height, id) = self.head()?;
        let cursor = self.cursor.lock().map_err(|_| "cursor poisoned")?;
        if cursor.height == height && cursor.block_id == id {
            return Ok(wire::Fetch {
                after_height: height,
                after_id: id,
                after_digest: cursor.digest.clone(),
                after_semantic_digest: Vec::new(),
            });
        }
        // The head was executed here. A member that records its own changes
        // proves them; a receiver-only member names the block alone and every
        // block it takes is checked against its state by the prior-value hash.
        let local = if self.config.keeps_journal() {
            let journal = self.journal.lock().map_err(|_| "journal poisoned")?;
            match journal.read(height, &id)? {
                Some(block) => Some(block),
                // Recording starts above this block; it has no record yet.
                None if journal.meta.anchor_height == height && journal.meta.anchor_id == id => {
                    None
                }
                None => return Err("local anchor receipt missing".into()),
            }
        } else {
            None
        };
        Ok(wire::Fetch {
            after_height: height,
            after_id: id,
            after_semantic_digest: match &local {
                Some(block) => journal::semantic_digest(block)?,
                None => Vec::new(),
            },
            after_digest: local.map(|b| b.digest).unwrap_or_default(),
        })
    }

    fn persist_cursor(&self, cursor: &ReceiverCursor) -> Result<()> {
        journal::atomic_write(
            &self.root.join("receiver.json"),
            &serde_json::to_vec(cursor).map_err(|e| e.to_string())?,
        )
    }

    pub(super) fn switch_mode(&self, assisted: bool) -> Result<()> {
        // Caller holds the same fleet apply lock as normal execution.
        let mut cursor = self.cursor.lock().map_err(|_| "cursor poisoned")?;
        cursor.assisted = assisted;
        self.persist_cursor(&cursor)?;
        self.assisted.store(assisted, Ordering::Release);
        if let Err(error) = self.processing.mode(assisted) {
            tracing::error!(%error,"processing statistics persistence failed");
        }
        self.generation.fetch_add(1, Ordering::AcqRel);
        self.policy
            .lock()
            .map_err(|_| "policy poisoned")?
            .switched_at = Instant::now();
        tracing::info!(
            assisted,
            height = self.head()?.0,
            "assisted sync mode changed"
        );
        Ok(())
    }

    pub fn observe_status(&self, status: &wire::Status) -> Result<bool> {
        let _writer = self.leadership.lock_apply();
        self.observe_status_locked(status)
    }
    pub(super) fn observe_status_locked(&self, status: &wire::Status) -> Result<bool> {
        if status.solid < 0 || status.solid > status.head || status.solid_id.len() != 32 {
            return Err("invalid donor solid status".into());
        }

        let (head, _) = self.head()?;
        let mut policy = self.policy.lock().map_err(|_| "policy poisoned")?;
        let lag = status.solid.saturating_sub(head).max(0) as u64;
        if !self.is_assisted() && self.config.automatic && lag > self.config.enter_after_blocks {
            policy.high_checks = policy.high_checks.saturating_add(1);
            let enter = policy.high_checks >= self.config.enter_after_checks
                && policy.switched_at.elapsed().as_secs() >= self.config.cooldown_seconds;
            drop(policy);
            if enter {
                self.switch_mode(true)?;
            }
        } else {
            policy.high_checks = 0;
            drop(policy);
        }
        // A fixed solid barrier is enough: new solid blocks may be produced
        // while we hand off; never require the donor to pause its own chain.
        if self.is_assisted()
            && head >= status.solid
            && self.canonical_id(status.solid)? == status.solid_id
        {
            self.switch_mode(false)?;
        }
        Ok(self.is_assisted())
    }

    pub fn recover(&self) -> Result<()> {
        let rewind = self.root.join("rewind.json");
        if rewind.exists() {
            let plan: Rewind =
                serde_json::from_slice(&fs::read(&rewind).map_err(|e| e.to_string())?)
                    .map_err(|e| e.to_string())?;
            let _writer = self.leadership.lock_apply();
            self.apply_rewind(&plan)?;
        }
        let pending = self.root.join("incoming.pb");
        if pending.exists() {
            let bytes = fs::read(&pending).map_err(|e| e.to_string())?;
            if bytes.len() > self.config.max_block_bytes {
                return Err("oversized pending import".into());
            }
            let block = journal::unpack(&bytes, self.config.max_block_bytes)?;
            let _writer = self.leadership.lock_apply();
            self.apply_prepared(&block, true)?;
        }
        // A source crash may happen after the consensus checkpoint commits
        // but before its ordinary best-effort index hook. Repair that block
        // from the already durable pre-commit journal before serving clients.
        if !self.is_assisted() {
            let (height, id) = self.head()?;
            let record = self
                .journal
                .lock()
                .map_err(|_| "journal poisoned")?
                .read(height, &id)?;
            if let Some(bundle) = record {
                let (block, mut event) = self.validate_payload(&bundle)?;
                self.write_auxiliary(&bundle, &event)?;
                event.solidified_height =
                    DynamicPropertiesStore::new(self.stores.dyn_props.clone())
                        .latest_solidified_block_num()
                        .unwrap_or(0);
                let deltas: Vec<_> = bundle
                    .deltas
                    .iter()
                    .map(|d| CapturedDelta {
                        store: UndoStoreId::from_u8(d.store as u8).unwrap(),
                        key: d.key.clone(),
                        before: d.before.clone(),
                        after: d.after.clone(),
                    })
                    .collect();
                self.hook.on_assisted_block(&block, &id, &deltas, event)?;
                let cursor = self.cursor.lock().map_err(|_| "cursor poisoned")?;
                if cursor.height != height || cursor.block_id != id {
                    self.processing.local(height, &id)?;
                }
            }
        }
        Ok(())
    }

    pub fn import(&self, block: &wire::BlockBundle) -> Result<wire::Ack> {
        self.import_with_anchor(block, None)
    }
    pub fn import_with_anchor(
        &self,
        block: &wire::BlockBundle,
        proof: Option<&[u8]>,
    ) -> Result<wire::Ack> {
        self.import_checked(block, proof, None)
    }
    pub fn import_from_cluster(
        &self,
        block: &wire::BlockBundle,
        proof: Option<&[u8]>,
        lease: &super::cluster::SourceLease,
    ) -> Result<wire::Ack> {
        self.import_checked(block, proof, Some(lease))
    }
    fn import_checked(
        &self,
        block: &wire::BlockBundle,
        proof: Option<&[u8]>,
        source: Option<&super::cluster::SourceLease>,
    ) -> Result<wire::Ack> {
        journal::verify_block(
            block,
            &self.network,
            &self.profile,
            self.config.max_block_bytes,
        )?;
        let _writer = self.leadership.lock_apply();
        if source.is_some_and(|lease| !self.cluster_current(lease)) {
            return Err("CLUSTER_SOURCE_CHANGED".into());
        }
        {
            let cursor = self.cursor.lock().map_err(|_| "cursor poisoned")?;
            if cursor.height == block.height
                && cursor.block_id == block.block_id
                && cursor.digest == block.digest
            {
                return Ok(wire::Ack {
                    height: block.height,
                    block_id: block.block_id.clone(),
                    digest: block.digest.clone(),
                    semantic_digest: Vec::new(),
                });
            }
        }
        if !self.is_assisted() {
            return Err("import is not the current writer".into());
        }
        let (head, id) = self.head()?;
        if head + 1 != block.height || id != block.parent_id {
            return Err("import does not extend the local durable head".into());
        }
        self.validate_payload(block)?;
        // The values these writes replace are read from this node's own state:
        // their hash must be the donor's, and they become the undo record.
        let block = &self.with_local_preimages(block)?;
        if !self.config.keeps_journal() {
            // No local records to chain to: consecutive imports are chained
            // through the receiver cursor instead.
            let cursor = self.cursor.lock().map_err(|_| "cursor poisoned")?;
            if cursor.height == head
                && cursor.block_id == id
                && cursor.digest.len() == 32
                && cursor.digest != [0; 32]
                && cursor.digest != block.previous_digest
            {
                return Err("imported block does not continue the received chain".into());
            }
        } else if let Some(proof) = proof {
            let journal = self.journal.lock().map_err(|_| "journal poisoned")?;
            let local = journal
                .read(head, &id)?
                .ok_or("local anchor receipt missing")?;
            if proof != journal::semantic_digest(&local)? {
                return Err("local semantic anchor changed before import".into());
            }
            journal.bind_parent(head, &id, &block.previous_digest)?;
        }
        journal::atomic_write(
            &self.root.join("incoming.pb"),
            &journal::pack(block, self.config.compression)?,
        )?;
        self.apply_prepared(block, false)?;
        Ok(wire::Ack {
            height: block.height,
            block_id: block.block_id.clone(),
            digest: block.digest.clone(),
            semantic_digest: Vec::new(),
        })
    }

    fn with_local_preimages(&self, bundle: &wire::BlockBundle) -> Result<wire::BlockBundle> {
        let mut local = bundle.clone();
        for delta in &mut local.deltas {
            let store = u8::try_from(delta.store)
                .ok()
                .and_then(UndoStoreId::from_u8)
                .ok_or("unknown store")?;
            delta.before = state_backend(&self.stores, store)
                .get(&delta.key)
                .map_err(|e| e.to_string())?;
        }
        if journal::before_digest_of(&local) != local.before_digest {
            report_state_mismatch(local.height);
            return Err("STATE_PREIMAGE_MISMATCH: snapshot/writes differ from donor".into());
        }
        local.preimages = true;
        Ok(local)
    }

    fn validate_payload(&self, bundle: &wire::BlockBundle) -> Result<(Block, fh::BlockApplied)> {
        let block = Block::decode(bundle.raw_block.as_slice()).map_err(|e| e.to_string())?;
        let id = block_id_from_block(&block).map_err(|e| format!("{e:?}"))?;
        let raw = block
            .block_header
            .as_ref()
            .and_then(|h| h.raw_data.as_ref())
            .ok_or("missing block header")?;
        if id.as_bytes().as_slice() != bundle.block_id || raw.parent_hash != bundle.parent_id {
            return Err("raw block and write bundle identities differ".into());
        }
        let event = fh::BlockApplied::decode(bundle.firehose_apply.as_slice())
            .map_err(|e| e.to_string())?;
        let ret = tron_proto::TransactionRet::decode(bundle.transaction_ret.as_slice())
            .map_err(|e| e.to_string())?;
        if event.height != bundle.height
            || event.block_id != bundle.block_id
            || event.parent_id != bundle.parent_id
            || event.txinfo_missing
            || !event.trx_balances_complete
            || event.txs.len() != block.transactions.len()
            || ret.transactioninfo.len() != block.transactions.len()
        {
            return Err(
                "replication bundle has incomplete or mismatched block events/receipts".into(),
            );
        }
        Ok((block, event))
    }

    fn write_auxiliary(&self, bundle: &wire::BlockBundle, event: &fh::BlockApplied) -> Result<()> {
        self.stores
            .blocks
            .put(&bundle.block_id, &bundle.raw_block)
            .map_err(|e| e.to_string())?;
        self.stores
            .block_index
            .put(&bundle.height.to_be_bytes(), &bundle.block_id)
            .map_err(|e| e.to_string())?;
        self.stores
            .transaction_ret
            .put(&bundle.height.to_be_bytes(), &bundle.transaction_ret)
            .map_err(|e| e.to_string())?;
        let balance = fh::BlockApplied {
            block_id: bundle.block_id.clone(),
            trx_balance_changes: event.trx_balance_changes.clone(),
            trx_balances_complete: true,
            ..Default::default()
        };
        self.stores
            .transaction_ret
            .put(
                &crate::firehose::balance_key(bundle.height),
                &balance.encode_to_vec(),
            )
            .map_err(|e| e.to_string())?;
        let refs: Vec<_> = event
            .txs
            .iter()
            .map(|tx| WriteOp::Put(tx.txid.clone(), bundle.height.to_be_bytes().to_vec()))
            .collect();
        self.stores
            .transactions
            .write_batch(&refs)
            .map_err(|e| e.to_string())?;
        self.stores
            .block_undo
            .put(&bundle.height.to_be_bytes(), &journal::undo_record(bundle)?)
            .map_err(|e| e.to_string())?;

        for backend in [
            &self.stores.blocks,
            &self.stores.block_index,
            &self.stores.transaction_ret,
            &self.stores.transactions,
            &self.stores.block_undo,
        ] {
            backend.sync_wal().map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    fn apply_prepared(&self, bundle: &wire::BlockBundle, recovery: bool) -> Result<()> {
        journal::verify_block(
            bundle,
            &self.network,
            &self.profile,
            self.config.max_block_bytes,
        )?;
        let (block, mut event) = self.validate_payload(bundle)?;
        {
            let cursor = self.cursor.lock().map_err(|_| "cursor poisoned")?;
            if cursor.height > bundle.height {
                return Err("pending import is older than receiver cursor".into());
            }
            if cursor.height == bundle.height
                && cursor.digest == bundle.digest
                && cursor.block_id == bundle.block_id
            {
                journal::remove_durable(&self.root.join("incoming.pb"))?;
                return Ok(());
            }
        }
        let (h, id) = self.head()?;
        let parent = h + 1 == bundle.height && id == bundle.parent_id;
        let partial = recovery && h == bundle.height && id == bundle.block_id;
        if !parent && !partial {
            return Err("pending import head is neither parent nor partial committed block".into());
        }
        if !bundle.preimages {
            return Err("pending import carries no prior values".into());
        }
        if self.config.keeps_journal() {
            let mut journal = self.journal.lock().map_err(|_| "journal poisoned")?;
            journal.cluster_reserve(bundle.height - 1)?;
            journal.stage(bundle, true)?;
        }
        let mut batches: BTreeMap<u32, Vec<WriteOp>> = BTreeMap::new();
        let mut deltas = Vec::with_capacity(bundle.deltas.len());
        for delta in &bundle.deltas {
            let id = UndoStoreId::from_u8(delta.store as u8).ok_or("unknown store")?;
            batches
                .entry(delta.store)
                .or_default()
                .push(match &delta.after {
                    Some(v) => WriteOp::Put(delta.key.clone(), v.clone()),
                    None => WriteOp::Delete(delta.key.clone()),
                });
            deltas.push(CapturedDelta {
                store: id,
                key: delta.key.clone(),
                before: delta.before.clone(),
                after: delta.after.clone(),
            });
        }
        for (id, ops) in &batches {
            state_backend(
                &self.stores,
                UndoStoreId::from_u8(*id as u8).ok_or("unknown store")?,
            )
            .write_batch(ops)
            .map_err(|e| e.to_string())?;
        }
        // Auxiliary payloads are part of the SAME redo record, rather than
        // a best-effort hook after acknowledging the consensus state.
        self.write_auxiliary(bundle, &event)?;
        DynamicPropertiesStore::new(self.stores.dyn_props.clone())
            .save_latest_solidified_block_num(bundle.height);
        for id in batches.keys() {
            state_backend(
                &self.stores,
                UndoStoreId::from_u8(*id as u8).ok_or("unknown store")?,
            )
            .sync_wal()
            .map_err(|e| e.to_string())?;
        }
        for backend in [
            &self.stores.blocks,
            &self.stores.block_index,
            &self.stores.transaction_ret,
            &self.stores.transactions,
            &self.stores.block_undo,
            &self.stores.dyn_props,
        ] {
            backend.sync_wal().map_err(|e| e.to_string())?;
        }
        // Authenticated solid imports are final. The ordinary Firehose
        // payload carries that current fact for existing indexer consumers.
        event.solidified_height = bundle.height;
        self.hook
            .on_assisted_block(&block, &bundle.block_id, &deltas, event)?;
        self.khaos
            .start(block)
            .map_err(|e| format!("imported fork-tree seed: {e:?}"))?;
        let mut cursor = self.cursor.lock().map_err(|_| "cursor poisoned")?;
        cursor.height = bundle.height;
        cursor.block_id = bundle.block_id.clone();
        cursor.digest = bundle.digest.clone();
        cursor.assisted = true;
        cursor.imported_blocks += 1;
        self.persist_cursor(&cursor)?;
        if let Err(error) = self
            .processing
            .imported(cursor.imported_blocks, cursor.height)
        {
            tracing::error!(%error,"processing statistics persistence failed");
        }
        self.assisted.store(true, Ordering::Release);
        self.generation.fetch_add(1, Ordering::AcqRel);
        journal::remove_durable(&self.root.join("incoming.pb"))?;
        tracing::debug!(
            height = bundle.height,
            "assisted block and events committed"
        );
        Ok(())
    }
}

/// A partner's block replaces values this node does not have: the two states
/// differ. The block is refused and the node keeps validating on its own, but
/// the difference itself needs attention. Raised for the first such block and
/// then at most once in ten minutes while the condition lasts.
pub(super) fn report_state_mismatch(height: i64) {
    static LAST: Mutex<Option<Instant>> = Mutex::new(None);
    let Ok(mut last) = LAST.lock() else { return };
    if last.is_some_and(|at| at.elapsed().as_secs() < 600) {
        return;
    }
    *last = Some(Instant::now());
    tracing::error!(
        height,
        "cluster block refused: local state differs from the partner's"
    );
    crate::critical_notifications::report(
        critical_notify::CriticalEvent::new(
            "node_cluster_state_mismatch",
            "Cluster partner's block does not match this node's state",
        )
        .context("block", height),
        false,
    );
}

pub fn read_head(stores: &crate::storage::OpenedStores) -> Result<(i64, Vec<u8>)> {
    let dp = DynamicPropertiesStore::new(stores.dyn_props.clone());
    let height = dp
        .latest_block_header_number()
        .ok_or("missing durable head number")?;
    let id = dp
        .latest_block_header_hash()
        .map_err(|e| e.to_string())?
        .ok_or("missing durable head hash")?;
    Ok((height, id.to_vec()))
}

pub fn state_backend(s: &crate::storage::OpenedStores, id: UndoStoreId) -> Arc<dyn KvBackend> {
    use UndoStoreId::*;
    match id {
        Accounts => s.accounts.clone(),
        Witnesses => s.witnesses.clone(),
        Votes => s.votes.clone(),
        Delegation => s.delegation.clone(),
        DelegatedResources => s.delegated_resources.clone(),
        DynProps => s.dyn_props.clone(),
        Proposals => s.proposals.clone(),
        NameIndex => s.name_index.clone(),
        IdIndex => s.id_index.clone(),
        AssetV1 => s.asset_v1.clone(),
        AssetV2 => s.asset_v2.clone(),
        Contracts => s.contracts.clone(),
        Abi => s.abi.clone(),
        ExchangeV1 => s.exchange_v1.clone(),
        ExchangeV2 => s.exchange_v2.clone(),
        MarketOrders => s.market_orders.clone(),
        MarketAccount => s.market_account.clone(),
        AccountAsset => s.account_asset.clone(),
        Nullifiers => s.nullifiers.clone(),
        MerkleTrees => s.merkle_trees.clone(),
        Code => s.code.clone(),
        StorageRow => s.storage_row.clone(),
        ContractState => s.contract_state.clone(),
        BlockIndex => s.block_index.clone(),
        WitnessSchedule => s.witness_schedule.clone(),
        DelegatedResourceAccountIndex => s.delegated_resource_account_index.clone(),
    }
}

impl BlockCommitObserver for AssistRuntime {
    fn prepare(
        &self,
        block: &Block,
        report: &BlockExecutionReport,
        undo: &tron_chainbase::BlockUndoRecord,
    ) -> Result<()> {
        if !self.config.keeps_journal() {
            return Ok(()); // Receiver only: nothing is recorded for partners.
        }
        let deltas = report
            .state_deltas
            .as_ref()
            .ok_or("replication requires complete state deltas")?;
        // The undo record is rebuilt from these prior values on a partner and
        // after a crash here, so both captures must describe the same writes.
        if undo.entries.len() != deltas.len()
            || undo
                .entries
                .iter()
                .zip(deltas)
                .any(|(u, d)| u.store != d.store || u.key != d.key || u.before != d.before)
        {
            return Err("captured state deltas and the undo record differ".into());
        }
        let id = report.block_id.as_bytes().to_vec();
        let raw = block
            .block_header
            .as_ref()
            .and_then(|h| h.raw_data.as_ref())
            .ok_or("missing block header")?;
        let raw_block = self
            .stores
            .blocks
            .get(&id)
            .map_err(|e| e.to_string())?
            .unwrap_or_else(|| block.encode_to_vec());
        let ret = crate::index_hook::build_transaction_ret(block, &report.block_id, report);
        let mut event = crate::firehose::build_apply(raw.number, block, Some(&ret), 0);
        let balances = crate::firehose::capture_balances(&report.block_id, Some(deltas))
            .map_err(|e| e.to_string())?;
        event.trx_balance_changes = balances.trx_balance_changes;
        event.trx_balances_complete = balances.trx_balances_complete;
        if event.txinfo_missing
            || !event.trx_balances_complete
            || report.tx_results.len() != block.transactions.len()
        {
            return Err("refusing incomplete replication event capture".into());
        }
        let mut journal = self.journal.lock().map_err(|_| "journal poisoned")?;
        if raw.number % 128 == 0 {
            let solid = DynamicPropertiesStore::new(self.stores.dyn_props.clone())
                .latest_solidified_block_num()
                .unwrap_or(0);
            journal.prune(solid)?;
        }
        let solid = DynamicPropertiesStore::new(self.stores.dyn_props.clone())
            .latest_solidified_block_num()
            .unwrap_or(0);
        let Some(previous_digest) = journal.source_parent(raw.number, &raw.parent_hash, solid)?
        else {
            return Ok(()); // Re-execution of the pre-activation reversible suffix.
        };
        let rotation = report.maintenance.as_ref().map(|m| wire::Rotation {
            before: m
                .prev_active
                .iter()
                .map(|a| a.as_bytes().to_vec())
                .collect(),
            current: m.new_active.iter().map(|a| a.as_bytes().to_vec()).collect(),
            before_maintenance_time_ms: m.before_maintenance_time_ms,
        });
        let mut bundle = wire::BlockBundle {
            format: journal::FORMAT,
            network: self.network.clone(),
            profile: self.profile.clone(),
            height: raw.number,
            block_id: id,
            parent_id: raw.parent_hash.clone(),
            previous_digest,
            deltas: deltas
                .iter()
                .map(|d| wire::Delta {
                    store: d.store as u32,
                    key: d.key.clone(),
                    before: d.before.clone(),
                    after: d.after.clone(),
                })
                .collect(),
            raw_block,
            transaction_ret: ret.encode_to_vec(),
            firehose_apply: event.encode_to_vec(),
            undo: Vec::new(),
            rotation,
            digest: Vec::new(),
            before_digest: Vec::new(),
            preimages: true,
        };
        // The executor hands the changes over in their final order; the hash
        // and every partner follow that order as it is.
        bundle.before_digest = journal::before_digest_of(&bundle);
        bundle.digest = journal::block_digest(&bundle);
        journal.cluster_reserve(solid)?;
        journal.stage(&bundle, false)
    }
}

#[derive(Serialize, Deserialize)]
struct Rewind {
    height: i64,
    block_id: Vec<u8>,
    // Undo bytes are copied into the redo manifest before any undo-store
    // deletion. Replaying ALL entries newest-first is idempotent.
    records: Vec<(i64, Vec<u8>)>,
}
impl AssistRuntime {
    pub fn has_pending_import(&self) -> bool {
        self.root.join("incoming.pb").exists() || self.root.join("rewind.json").exists()
    }

    pub fn align_anchor(&self, height: i64, id: &[u8]) -> Result<bool> {
        self.align_checked(height, id, None)
    }
    pub fn align_from_cluster(
        &self,
        height: i64,
        id: &[u8],
        lease: &super::cluster::SourceLease,
    ) -> Result<bool> {
        self.align_checked(height, id, Some(lease))
    }
    fn align_checked(
        &self,
        height: i64,
        id: &[u8],
        source: Option<&super::cluster::SourceLease>,
    ) -> Result<bool> {
        let _writer = self.leadership.lock_apply();
        if source.is_some_and(|lease| !self.cluster_current(lease)) {
            return Err("CLUSTER_SOURCE_CHANGED".into());
        }
        if !self.is_assisted() {
            return Err("receiver no longer owns the writer".into());
        }
        let (head, _) = self.head()?;
        let solid = DynamicPropertiesStore::new(self.stores.dyn_props.clone())
            .latest_solidified_block_num()
            .unwrap_or(0);
        if height < solid || height > head {
            return Err("SOLID_CONFLICT: anchor lies outside reversible suffix".into());
        }
        if self.canonical_id(height)? != id {
            if height == solid {
                return Err(
                    "SOLID_CONFLICT: authenticated donor disagrees with local finality".into(),
                );
            }
            return Ok(false);
        }
        if head == height {
            return Ok(true);
        }
        let mut records = Vec::new();
        for h in ((height + 1)..=head).rev() {
            let bytes = self
                .stores
                .block_undo
                .get(&h.to_be_bytes())
                .map_err(|e| e.to_string())?
                .ok_or("missing reversible undo")?;
            tron_chainbase::BlockUndoRecord::decode(&bytes).map_err(|e| e.to_string())?;
            records.push((h, bytes));
        }
        let plan = Rewind {
            height,
            block_id: id.to_vec(),
            records,
        };
        journal::atomic_write(
            &self.root.join("rewind.json"),
            &serde_json::to_vec(&plan).map_err(|e| e.to_string())?,
        )?;
        self.apply_rewind(&plan)?;
        Ok(true)
    }

    fn apply_rewind(&self, plan: &Rewind) -> Result<()> {
        let (current, _) = self.head()?;
        let start = plan
            .records
            .first()
            .map(|r| r.0)
            .ok_or("empty rewind manifest")?;
        if plan.block_id.len() != 32
            || current > start
            || plan.height < 0
            || start - plan.height > 10_000
            || plan.records.len() as i64 != start - plan.height
            || plan
                .records
                .iter()
                .enumerate()
                .any(|(i, r)| r.0 != start - i as i64)
        {
            return Err("invalid rewind manifest/head bounds".into());
        }
        let mut touched = BTreeMap::new();
        for (height, bytes) in &plan.records {
            let undo = tron_chainbase::BlockUndoRecord::decode(bytes).map_err(|e| e.to_string())?;
            let mut batches: BTreeMap<u8, Vec<WriteOp>> = BTreeMap::new();
            for e in undo.entries {
                touched.insert(e.store as u8, ());
                batches
                    .entry(e.store as u8)
                    .or_default()
                    .push(match e.before {
                        Some(value) => WriteOp::Put(e.key, value),
                        None => WriteOp::Delete(e.key),
                    });
            }
            for (store, ops) in batches {
                state_backend(
                    &self.stores,
                    UndoStoreId::from_u8(store).ok_or("invalid undo store")?,
                )
                .write_batch(&ops)
                .map_err(|e| e.to_string())?;
            }
            if let Some(raw) = self
                .stores
                .transaction_ret
                .get(&height.to_be_bytes())
                .map_err(|e| e.to_string())?
            {
                let ret = tron_proto::TransactionRet::decode(raw.as_slice())
                    .map_err(|e| e.to_string())?;
                for tx in ret.transactioninfo {
                    if self
                        .stores
                        .transactions
                        .get(&tx.id)
                        .map_err(|e| e.to_string())?
                        == Some(height.to_be_bytes().to_vec())
                    {
                        self.stores
                            .transactions
                            .delete(&tx.id)
                            .map_err(|e| e.to_string())?;
                    }
                }
            }
            self.stores
                .block_index
                .delete(&height.to_be_bytes())
                .map_err(|e| e.to_string())?;
            // Receipts are retained until the whole rewind commits: they
            // contain the tx references needed if redo repeats after a crash.
        }
        for store in touched.keys() {
            state_backend(
                &self.stores,
                UndoStoreId::from_u8(*store).ok_or("invalid store")?,
            )
            .sync_wal()
            .map_err(|e| e.to_string())?;
        }
        self.stores
            .block_index
            .sync_wal()
            .map_err(|e| e.to_string())?;
        self.stores
            .transactions
            .sync_wal()
            .map_err(|e| e.to_string())?;
        let (height, id) = self.head()?;
        if height != plan.height || id != plan.block_id {
            return Err("rewind did not restore its anchored head".into());
        }
        self.hook.on_assisted_rewind(height)?;
        let raw = self
            .stores
            .blocks
            .get(&id)
            .map_err(|e| e.to_string())?
            .ok_or("anchor block missing")?;
        self.khaos
            .start(Block::decode(raw.as_slice()).map_err(|e| e.to_string())?)
            .map_err(|e| format!("{e:?}"))?;
        let mut cursor = self.cursor.lock().map_err(|_| "cursor poisoned")?;
        cursor.height = height;
        cursor.block_id = id;
        cursor.digest = Vec::new();
        cursor.assisted = true;
        self.persist_cursor(&cursor)?;
        self.generation.fetch_add(1, Ordering::AcqRel);
        journal::remove_durable(&self.root.join("rewind.json"))?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
