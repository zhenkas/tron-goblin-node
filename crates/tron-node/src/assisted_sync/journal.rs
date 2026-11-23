//! Durable candidates and independent replication acknowledgements.
//! A candidate is fsynced BEFORE consensus commit, but is exportable only
//! after the caller proves its block ID canonical and solid.
use super::{AssistConfig, Compression};
use prost::Message;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    hash::Hasher,
    io::{Read, Write},
    path::{Path, PathBuf},
};
use tron_grpc::assist_proto::{Ack, BlockBundle, Delta};

pub type Result<T> = std::result::Result<T, String>;
/// Format 2: prior values are not transmitted. A block carries one hash of
/// them and the undo record is derived from the receiver's own state.
pub const FORMAT: u32 = 2;
/// Version this tag whenever canonical write/event semantics change.
pub const PROFILE: &[u8] = b"tron-assist-v2/deltas-before-hash-ret-native-events";

/// The transmitted form: the same block without the local prior values.
pub fn bare(block: &BlockBundle) -> BlockBundle {
    BlockBundle {
        format: block.format,
        network: block.network.clone(),
        profile: block.profile.clone(),
        height: block.height,
        block_id: block.block_id.clone(),
        parent_id: block.parent_id.clone(),
        previous_digest: block.previous_digest.clone(),
        deltas: block
            .deltas
            .iter()
            .map(|d| Delta {
                store: d.store,
                key: d.key.clone(),
                before: None,
                after: d.after.clone(),
            })
            .collect(),
        raw_block: block.raw_block.clone(),
        transaction_ret: block.transaction_ret.clone(),
        firehose_apply: block.firehose_apply.clone(),
        undo: block.undo.clone(),
        rotation: block.rotation.clone(),
        digest: block.digest.clone(),
        before_digest: block.before_digest.clone(),
        preimages: false,
    }
}

/// Identity of the transmitted form; prior values are covered through
/// `before_digest`, so a record hashes the same with or without them.
pub fn block_digest(block: &BlockBundle) -> Vec<u8> {
    let mut payload = bare(block);
    payload.digest.clear();
    Sha256::digest(payload.encode_to_vec()).to_vec()
}

/// One reproducible hash of everything the block's writes replace. Entries
/// are taken in delta order (sorted by store and key); each contributes its
/// store, its length-prefixed key and either an "absent" flag or a "present"
/// flag with the length-prefixed value. The entry count comes first.
pub fn before_digest<'a>(
    entries: impl ExactSizeIterator<Item = (u32, &'a [u8], Option<&'a [u8]>)>,
) -> Vec<u8> {
    let mut hash = twox_hash::XxHash64::with_seed(0);
    hash.write(&(entries.len() as u64).to_be_bytes());
    for (store, key, before) in entries {
        hash.write(&store.to_be_bytes());
        hash.write(&(key.len() as u64).to_be_bytes());
        hash.write(key);
        match before {
            None => hash.write(&[0]),
            Some(value) => {
                hash.write(&[1]);
                hash.write(&(value.len() as u64).to_be_bytes());
                hash.write(value);
            }
        }
    }
    hash.finish().to_be_bytes().to_vec()
}

pub fn before_digest_of(block: &BlockBundle) -> Vec<u8> {
    before_digest(
        block
            .deltas
            .iter()
            .map(|d| (d.store, d.key.as_slice(), d.before.as_deref())),
    )
}

/// The undo record of a block that carries its prior values.
pub fn undo_record(block: &BlockBundle) -> Result<Vec<u8>> {
    if !block.preimages {
        return Err("replication block carries no prior values".into());
    }
    let mut record = tron_chainbase::BlockUndoRecord::new();
    for d in &block.deltas {
        record.push(tron_chainbase::UndoEntry {
            store: u8::try_from(d.store)
                .ok()
                .and_then(tron_chainbase::UndoStoreId::from_u8)
                .ok_or("unknown replicated store")?,
            key: d.key.clone(),
            before: d.before.clone(),
        });
    }
    Ok(record.encode())
}

/// Proof of the same logical block changes, independent of a prior local hash chain.
/// Cache-dependent no-op captures do not change state.
pub fn semantic_digest(block: &BlockBundle) -> Result<Vec<u8>> {
    if !block.preimages {
        return Err("logical digest requires the block's prior values".into());
    }
    let mut payload = block.clone();
    payload.digest.clear();
    payload.previous_digest.clear();
    payload.before_digest.clear();
    payload.deltas.retain(|d| d.before != d.after);
    Ok(Sha256::digest(payload.encode_to_vec()).to_vec())
}

const PACKED: &[u8; 4] = b"TAJ2";

/// File form of a record: marker, codec byte, then the encoded block.
pub fn pack(block: &BlockBundle, compression: Compression) -> Result<Vec<u8>> {
    let raw = block.encode_to_vec();
    let mut out = Vec::with_capacity(raw.len() / 3 + 16);
    out.extend_from_slice(PACKED);
    let level = match compression {
        Compression::None => {
            out.push(0);
            out.extend_from_slice(&raw);
            return Ok(out);
        }
        Compression::Low => flate2::Compression::new(1),
        Compression::High => flate2::Compression::best(),
    };
    out.push(1);
    let mut encoder = flate2::write::ZlibEncoder::new(out, level);
    encoder
        .write_all(&raw)
        .and_then(|_| encoder.finish())
        .map_err(|e| format!("journal pack: {e}"))
}

pub fn unpack(bytes: &[u8], max: usize) -> Result<BlockBundle> {
    let body = bytes
        .strip_prefix(PACKED.as_slice())
        .ok_or("journal record of another format")?;
    let (codec, body) = body.split_first().ok_or("truncated journal record")?;
    let raw;
    let encoded = match codec {
        0 => body,
        1 => {
            let mut out = Vec::new();
            flate2::read::ZlibDecoder::new(body)
                .take(max as u64 + 1)
                .read_to_end(&mut out)
                .map_err(|e| format!("journal unpack: {e}"))?;
            raw = out;
            raw.as_slice()
        }
        _ => return Err("unknown journal record codec".into()),
    };
    if encoded.len() > max {
        return Err("oversized journal record".into());
    }
    BlockBundle::decode(encoded).map_err(|e| format!("journal decode: {e}"))
}

pub fn verify_block(block: &BlockBundle, network: &[u8], profile: &[u8], max: usize) -> Result<()> {
    if block.format != FORMAT || block.network != network || block.profile != profile {
        return Err("incompatible replication network/profile/format".into());
    }
    if block.height < 1
        || block.block_id.len() != 32
        || block.parent_id.len() != 32
        || block.digest.len() != 32
        || block.previous_digest.len() != 32
        || block.before_digest.len() != 8
        || !block.undo.is_empty()
    {
        return Err("malformed replication block identity".into());
    }
    if block.preimages {
        if before_digest_of(block) != block.before_digest {
            return Err("replication block prior values differ from their hash".into());
        }
    } else if block.deltas.iter().any(|d| d.before.is_some()) {
        return Err("replication block carries unmarked prior values".into());
    }
    if u64::from_be_bytes(block.block_id[..8].try_into().unwrap()) != block.height as u64 {
        return Err("replication height differs from block ID".into());
    }
    if block.encoded_len() > max || block_digest(block) != block.digest {
        return Err("replication block exceeds limit or checksum differs".into());
    }
    let mut previous: Option<(u32, &[u8])> = None;
    for delta in &block.deltas {
        if tron_chainbase::UndoStoreId::from_u8(
            u8::try_from(delta.store).map_err(|_| "unknown replicated store")?,
        )
        .is_none()
        {
            return Err("unknown replicated store".into());
        }
        let key = (delta.store, delta.key.as_slice());
        if previous.as_ref().is_some_and(|p| *p >= key) {
            return Err("replication deltas must have unique sorted (store,key) identities".into());
        }
        previous = Some(key);
    }
    Ok(())
}

pub fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().ok_or("missing file parent")?;
    fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    let tmp = path.with_extension("tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tmp)
        .map_err(|e| format!("journal create: {e}"))?;
    file.write_all(bytes)
        .and_then(|_| file.sync_all())
        .map_err(|e| format!("journal sync: {e}"))?;
    fs::rename(&tmp, path).map_err(|e| format!("journal rename: {e}"))?;
    File::open(parent)
        .and_then(|f| f.sync_all())
        .map_err(|e| format!("journal directory sync: {e}"))
}

pub fn remove_durable(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.to_string()),
    }
    File::open(path.parent().ok_or("missing parent")?)
        .and_then(|f| f.sync_all())
        .map_err(|e| e.to_string())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Meta {
    pub format: u32,
    pub network: Vec<u8>,
    pub profile: Vec<u8>,
    pub anchor_height: i64,
    pub anchor_id: Vec<u8>,
    pub anchor_digest: Vec<u8>,
    pub floor: i64,
}

pub struct Journal {
    root: PathBuf,
    pub meta: Meta,
    pub acks: BTreeMap<String, Ack>,
    bytes: u64,
    config: AssistConfig,
    /// Floor below which files were already removed in this process; lets the
    /// per-block cluster check skip the directory walk.
    swept_floor: i64,
    /// The stored window did not continue to the current head (another
    /// format, or blocks applied while nothing was recorded) and was dropped.
    pub reset: bool,
}

impl Journal {
    pub fn open(
        root: PathBuf,
        config: AssistConfig,
        network: Vec<u8>,
        profile: Vec<u8>,
        anchor_height: i64,
        anchor_id: Vec<u8>,
    ) -> Result<Self> {
        fs::create_dir_all(root.join("blocks")).map_err(|e| e.to_string())?;
        fs::create_dir_all(root.join("acks")).map_err(|e| e.to_string())?;
        let meta_path = root.join("donor.json");
        let stored: Option<Meta> = if meta_path.exists() {
            Some(
                serde_json::from_slice(&fs::read(&meta_path).map_err(|e| e.to_string())?)
                    .map_err(|e| format!("replication metadata: {e}"))?,
            )
        } else {
            None
        };
        // A trusted cluster's journal is a window cache, not a ledger. One
        // that cannot continue from the current head is started again there.
        let reset = config.trusted_cluster
            && stored.as_ref().is_some_and(|m| {
                m.network == network
                    && (m.format != FORMAT
                        || m.profile != profile
                        || ((m.anchor_height, m.anchor_id.as_slice())
                            != (anchor_height, anchor_id.as_slice())
                            && !root
                                .join("blocks")
                                .join(format!("{anchor_height:020}.{}.pb", hex::encode(&anchor_id)))
                                .exists()))
            });
        if reset {
            // Files first: an interrupted reset is simply repeated.
            for dir in ["blocks", "anchors"] {
                let dir = root.join(dir);
                if !dir.exists() {
                    continue;
                }
                for item in fs::read_dir(&dir).map_err(|e| e.to_string())? {
                    fs::remove_file(item.map_err(|e| e.to_string())?.path())
                        .map_err(|e| e.to_string())?;
                }
            }
        }
        let meta = match stored {
            Some(meta) if !reset => meta,
            _ => {
                let meta = Meta {
                    format: FORMAT,
                    network: network.clone(),
                    profile: profile.clone(),
                    anchor_height,
                    anchor_id,
                    anchor_digest: vec![0; 32],
                    floor: anchor_height + 1,
                };
                atomic_write(
                    &meta_path,
                    &serde_json::to_vec(&meta).map_err(|e| e.to_string())?,
                )?;
                meta
            }
        };
        if meta.format != FORMAT
            || meta.network != network
            || meta.profile != profile
            || meta.anchor_id.len() != 32
            || meta.anchor_digest.len() != 32
            || meta.floor < meta.anchor_height + 1
        {
            return Err(
                "replication journal belongs to another network/profile or has invalid metadata"
                    .into(),
            );
        }
        let mut bytes = 0;
        for item in fs::read_dir(root.join("blocks")).map_err(|e| e.to_string())? {
            let p = item.map_err(|e| e.to_string())?.path();
            if p.extension().is_some_and(|x| x == "pb") {
                bytes += fs::metadata(&p).map_err(|e| e.to_string())?.len();
            }
        }
        let mut acks = BTreeMap::new();
        for peer in &config.peers {
            let p = root.join("acks").join(format!("{}.pb", peer.id));
            if p.exists() {
                let ack = Ack::decode(fs::read(&p).map_err(|e| e.to_string())?.as_slice())
                    .map_err(|e| format!("replication ACK: {e}"))?;
                if ack.height < meta.anchor_height
                    || ack.block_id.len() != 32
                    || ack.digest.len() != 32
                {
                    return Err("invalid retained replication ACK".into());
                }
                acks.insert(peer.id.clone(), ack);
            }
        }
        Ok(Self {
            root,
            meta,
            acks,
            bytes,
            config,
            swept_floor: 0,
            reset,
        })
    }

    fn block_path(&self, height: i64, id: &[u8]) -> PathBuf {
        self.root
            .join("blocks")
            .join(format!("{height:020}.{}.pb", hex::encode(id)))
    }

    /// The enable-time head can still be reversible. Re-executing that
    /// suffix must not stop consensus. Nothing at/below activation is
    /// advertised as available; its anchor may move only before finality
    /// and before any downstream ACK has accepted this history.
    pub fn source_parent(
        &mut self,
        height: i64,
        parent: &[u8],
        solid: i64,
    ) -> Result<Option<Vec<u8>>> {
        if height <= self.meta.anchor_height {
            return Ok(None);
        }
        if height == self.meta.anchor_height + 1 && parent != self.meta.anchor_id {
            if self.meta.anchor_height == 0
                || solid > self.meta.anchor_height
                || !self.acks.is_empty()
            {
                return Err("activation anchor changed after finality/replication".into());
            }
            if parent.len() != 32 {
                return Err("invalid activation parent".into());
            }
            self.meta.anchor_id = parent.to_vec();
            self.meta.anchor_digest = vec![0; 32];
            self.persist_meta()?;
        }
        self.parent_digest(height, parent).map(Some)
    }

    pub fn bind_parent(&self, height: i64, id: &[u8], digest: &[u8]) -> Result<()> {
        if height < self.meta.floor - 1 || id.len() != 32 || digest.len() != 32 {
            return Err("invalid imported parent binding".into());
        }
        let ack = Ack {
            height,
            block_id: id.to_vec(),
            digest: digest.to_vec(),
            semantic_digest: Vec::new(),
        };
        atomic_write(
            &self
                .root
                .join("anchors")
                .join(format!("{height:020}.{}.pb", hex::encode(id))),
            &ack.encode_to_vec(),
        )
    }
    pub fn parent_digest(&self, height: i64, parent: &[u8]) -> Result<Vec<u8>> {
        let bound = self.root.join("anchors").join(format!(
            "{:020}.{}.pb",
            height - 1,
            hex::encode(parent)
        ));
        if bound.exists() {
            let ack = Ack::decode(fs::read(bound).map_err(|e| e.to_string())?.as_slice())
                .map_err(|e| e.to_string())?;
            if ack.height != height - 1 || ack.block_id != parent || ack.digest.len() != 32 {
                return Err("invalid durable parent binding".into());
            }
            return Ok(ack.digest);
        }
        if height == self.meta.anchor_height + 1 && parent == self.meta.anchor_id {
            return Ok(self.meta.anchor_digest.clone());
        }
        self.read(height - 1, parent)?
            .map(|b| b.digest)
            .ok_or_else(|| "missing parent write journal".into())
    }

    pub fn read(&self, height: i64, id: &[u8]) -> Result<Option<BlockBundle>> {
        let path = self.block_path(height, id);
        match fs::read(&path) {
            Ok(bytes) => {
                if bytes.len() > self.config.max_block_bytes {
                    return Err("oversized journal record".into());
                }
                let block = unpack(&bytes, self.config.max_block_bytes)?;
                if !block.preimages {
                    return Err("journal record carries no prior values".into());
                }
                verify_block(
                    &block,
                    &self.meta.network,
                    &self.meta.profile,
                    self.config.max_block_bytes,
                )?;
                if block.height != height || block.block_id != id {
                    return Err("journal filename/identity mismatch".into());
                }
                Ok(Some(block))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.to_string()),
        }
    }

    pub fn stage(&mut self, block: &BlockBundle, imported: bool) -> Result<()> {
        if !block.preimages {
            return Err("write journal record requires the block's prior values".into());
        }
        verify_block(
            block,
            &self.meta.network,
            &self.meta.profile,
            self.config.max_block_bytes,
        )?;
        if block.height < self.meta.floor {
            return Err("write journal record is below retained floor".into());
        }
        if let Some(old) = self.read(block.height, &block.block_id)? {
            if old.digest == block.digest {
                return Ok(());
            }
            if !imported || semantic_digest(&old)? != semantic_digest(block)? {
                return Err("different write-set for the same block ID".into());
            }
            // Retain the original local receipt before adopting the authenticated donor chain.
            let old_path = self.block_path(old.height, &old.block_id);
            let prior = old_path.with_file_name(format!(
                "{:020}.{}.{}.prior.pb",
                old.height,
                hex::encode(&old.block_id),
                hex::encode(&old.digest)
            ));
            if !prior.exists() {
                fs::hard_link(&old_path, &prior).map_err(|e| e.to_string())?;
                File::open(prior.parent().unwrap())
                    .and_then(|f| f.sync_all())
                    .map_err(|e| e.to_string())?;
            }
        }
        let parent = self.parent_digest(block.height, &block.parent_id)?;
        if parent != block.previous_digest {
            // A trusted snapshot fixes state at the anchor, but may predate
            // replication capture. Bind its first authenticated parent digest.
            if imported
                && block.height == self.meta.anchor_height + 1
                && self.meta.anchor_digest == vec![0; 32]
                && block.parent_id == self.meta.anchor_id
            {
                self.meta.anchor_digest = block.previous_digest.clone();
                self.persist_meta()?;
            } else {
                return Err("write journal parent checksum differs".into());
            }
        }
        let bytes = pack(block, self.config.compression)?;
        // A trusted cluster is bounded by its block window. Blocks that are
        // received but not yet solidified are outside it and are never refused.
        if !self.config.trusted_cluster
            && self.bytes.saturating_add(bytes.len() as u64) > self.config.max_journal_bytes
        {
            return Err(
                "replication journal budget exhausted; refusing an unrecorded commit".into(),
            );
        }
        atomic_write(&self.block_path(block.height, &block.block_id), &bytes)?;
        self.bytes += bytes.len() as u64;
        Ok(())
    }

    fn persist_meta(&self) -> Result<()> {
        atomic_write(
            &self.root.join("donor.json"),
            &serde_json::to_vec(&self.meta).map_err(|e| e.to_string())?,
        )
    }

    pub fn acknowledge(&mut self, peer: &str, ack: Ack) -> Result<bool> {
        if !self
            .config
            .peers
            .iter()
            .any(|p| p.id == peer && p.allow_donate)
        {
            return Err("peer has no donor permission".into());
        }
        if let Some(old) = self.acks.get(peer) {
            if old.height > ack.height {
                return Err("replication ACK regressed".into());
            }
            if old.height == ack.height {
                return if old == &ack {
                    Ok(false)
                } else {
                    Err("conflicting replication ACK".into())
                };
            }
        }
        let block = self
            .read(ack.height, &ack.block_id)?
            .ok_or("ACK names an unavailable write record")?;
        if block.digest != ack.digest {
            return Err("ACK write-set checksum differs".into());
        }
        atomic_write(
            &self.root.join("acks").join(format!("{peer}.pb")),
            &ack.encode_to_vec(),
        )?;
        self.acks.insert(peer.into(), ack);
        Ok(true)
    }

    // The caller supplies the current canonical solid bound. Closed records
    // only; required consumers hold their own independent retention cursor.
    /// Keep the fixed block window of a trusted cluster. No peer can pin this
    /// cache, and blocks above the solid bound are never counted or refused.
    pub fn cluster_reserve(&mut self, solid: i64) -> Result<()> {
        if !self.config.trusted_cluster {
            return Ok(());
        }
        self.prune(solid).map(|_| ())
    }

    pub fn prune(&mut self, solid: i64) -> Result<usize> {
        let mut floor = (solid - self.config.retain_blocks.min(i64::MAX as u64) as i64 + 1)
            .max(self.meta.floor);
        if self.config.trusted_cluster {
            // Whole portions only: nothing moves until a full portion is due,
            // so the directory is walked once per portion, not once per block.
            let batch = self.config.prune_batch_blocks.clamp(1, i64::MAX as u64) as i64;
            floor = self.meta.floor + (floor - self.meta.floor) / batch * batch;
            if floor == self.meta.floor && self.swept_floor == floor {
                return Ok(0);
            }
        }
        if !self.config.trusted_cluster {
            for peer in self
                .config
                .peers
                .iter()
                .filter(|p| p.allow_donate && p.required_for_retention)
            {
                floor = floor.min(
                    self.acks
                        .get(&peer.id)
                        .map(|a| a.height)
                        .unwrap_or(self.meta.floor),
                );
            }
            if let Some(pin) = super::bootstrap::retention_pin(&self.root)? {
                if pin < self.meta.floor {
                    return Err("bootstrap retention pin is below the retained journal".into());
                }
                floor = floor.min(pin);
            }
        }
        if floor > self.meta.floor {
            self.meta.floor = floor;
            self.persist_meta()?; // Logical floor precedes physical removal.
        }
        let floor = self.meta.floor; // Resume physical cleanup after a crash too.
        let mut removed = 0;
        for item in fs::read_dir(self.root.join("blocks")).map_err(|e| e.to_string())? {
            let p = item.map_err(|e| e.to_string())?.path();
            let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
            let height = name.split('.').next().and_then(|s| s.parse::<i64>().ok());
            if p.extension().is_some_and(|x| x == "pb") && height.is_some_and(|h| h < floor) {
                let size = fs::metadata(&p).map_err(|e| e.to_string())?.len();
                fs::remove_file(&p).map_err(|e| e.to_string())?;
                self.bytes = self.bytes.saturating_sub(size);
                removed += 1;
            }
        }
        self.swept_floor = floor;
        Ok(removed)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
}
