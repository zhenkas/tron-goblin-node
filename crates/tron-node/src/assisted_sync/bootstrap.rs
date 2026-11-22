//! Single-owner, durable bootstrap: immutable engine checkpoints + resumable chunks.
use super::{
    journal::{self, Result},
    AssistRuntime,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Component, Path, PathBuf},
};
use tron_grpc::assist_proto as w;
/// Test-only abrupt exit: no Rust drop/normal shutdown can hide a missing barrier.
#[inline]
pub(super) fn crash_point(_name: &str) {
    #[cfg(test)]
    if std::env::var("TRON_BOOTSTRAP_CRASH_AT").ok().as_deref() == Some(_name) {
        std::process::exit(86);
    }
}

pub const CHUNK: usize = 4 * 1024 * 1024;
pub fn profile() -> Vec<u8> {
    Sha256::digest(b"tron-assist-v1/full-deltas-ret-native-events-undo").to_vec()
}
fn io(e: impl std::fmt::Display) -> String {
    e.to_string()
}
fn save<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    journal::atomic_write(path, &serde_json::to_vec(value).map_err(io)?)
}
fn read<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    serde_json::from_slice(&fs::read(path).map_err(io)?).map_err(io)
}
fn sync_dir(path: &Path) -> Result<()> {
    File::open(path).and_then(|f| f.sync_all()).map_err(io)
}
pub fn safe_path(root: &Path, relative: &str) -> Result<PathBuf> {
    if relative.is_empty()
        || relative.contains('\\')
        || Path::new(relative)
            .components()
            .any(|c| !matches!(c, Component::Normal(_)))
    {
        return Err("unsafe snapshot file path".into());
    }
    if !relative.starts_with("database/") && !["devnet.json", "private-profile.json"].contains(&relative) {
        return Err("unexpected snapshot file root".into());
    }
    Ok(root.join(relative))
}
pub fn hash_file(path: &Path) -> Result<Vec<u8>> {
    let mut file = File::open(path).map_err(io)?;
    let mut hash = Sha256::new();
    let mut buf = vec![0; CHUNK];
    loop {
        let n = file.read(&mut buf).map_err(io)?;
        if n == 0 {
            break;
        }
        hash.update(&buf[..n]);
    }
    Ok(hash.finalize().to_vec())
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Entry {
    pub path: String,
    pub size: u64,
    pub sha256: Vec<u8>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub format: u32,
    pub snapshot_id: String,
    pub network: Vec<u8>,
    pub profile: Vec<u8>,
    pub height: i64,
    pub block_id: Vec<u8>,
    pub digest: Vec<u8>,
    pub files: Vec<Entry>,
}
impl Manifest {
    pub fn hash(&self) -> Result<Vec<u8>> {
        Ok(Sha256::digest(serde_json::to_vec(self).map_err(io)?).to_vec())
    }
    pub fn bytes(&self) -> u64 {
        self.files.iter().map(|f| f.size).sum()
    }
    pub fn validate(&self, network: &[u8], max: u64) -> Result<()> {
        if self.format != 1
            || self.network != network
            || self.profile != profile()
            || self.height < 0
            || self.block_id.len() != 32
            || self.digest.len() != 32
        {
            return Err("snapshot chain, schema or anchor mismatch".into());
        }
        if !valid_id(&self.snapshot_id) || self.files.is_empty() || self.files.len() > 200_000 {
            return Err("invalid snapshot identity or file count".into());
        }
        let mut names = std::collections::BTreeSet::new();
        let mut total = 0u64;
        for f in &self.files {
            safe_path(Path::new("."), &f.path)?;
            if !names.insert(&f.path) || f.sha256.len() != 32 {
                return Err("duplicate snapshot file or invalid hash".into());
            }
            total = total.checked_add(f.size).ok_or("snapshot size overflow")?;
        }
        if total > max {
            return Err("snapshot exceeds configured disk budget".into());
        }
        for name in ["properties", "block", "block-index"] {
            if !names.contains(&format!("database/{name}/CURRENT")) {
                return Err("snapshot is missing a mandatory store".into());
            }
        }
        Ok(())
    }
}
fn epoch_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
fn cluster_lease_alive(root: &Path) -> bool {
    read::<u64>(&root.join("snapshot-lease.json"))
        .is_ok_and(|t| epoch_seconds().saturating_sub(t) < 300)
}
fn renew_cluster_lease(root: &Path, force: bool) -> Result<()> {
    let p = root.join("snapshot-lease.json");
    let old = read::<u64>(&p).unwrap_or(0);
    let now = epoch_seconds();
    if force || now.saturating_sub(old) >= 30 {
        save(&p, &now)?;
    }
    Ok(())
}
fn valid_id(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct DonorSession {
    owner: String,
    request_id: String,
    phase: String,
    retention_height: i64,
    manifest: Manifest,
    #[serde(default)]
    retired: Vec<String>,
}
pub fn retention_pin(root: &Path) -> Result<Option<i64>> {
    let p = root.join("snapshot.json");
    if p.exists() {
        let s: DonorSession = read(&p)?;
        Ok((s.phase != "released").then_some(s.retention_height))
    } else {
        Ok(None)
    }
}
pub fn advance_pin(rt: &AssistRuntime, peer: &str, ack: &w::Ack) -> Result<()> {
    let _guard = rt
        .snapshot_lock
        .lock()
        .map_err(|_| "snapshot lock poisoned")?;
    let p = rt.root.join("snapshot.json");
    if p.exists() {
        let mut s: DonorSession = read(&p)?;
        if s.owner == peer && s.phase == "installed" {
            s.retention_height = s
                .retention_height
                .max(ack.height.checked_add(1).ok_or("ACK overflow")?);
            save(&p, &s)?;
        }
    }
    Ok(())
}
fn enumerate(root: &Path, dir: &Path, out: &mut Vec<Entry>) -> Result<()> {
    let mut entries = fs::read_dir(dir)
        .map_err(io)?
        .collect::<std::io::Result<Vec<_>>>()
        .map_err(io)?;
    entries.sort_by_key(|e| e.file_name());
    for e in entries {
        let typ = e.file_type().map_err(io)?;
        let p = e.path();
        if typ.is_dir() {
            enumerate(root, &p, out)?;
        } else if typ.is_file() {
            // Engine locks and diagnostic logs have no role in the restored ledger.
            let name = e.file_name();
            let name = name.to_string_lossy();
            if name == "LOCK" || name == "LOG" || name.starts_with("LOG.old") {
                continue;
            }
            out.push(Entry {
                path: p.strip_prefix(root).map_err(io)?.to_string_lossy().into(),
                size: e.metadata().map_err(io)?.len(),
                sha256: hash_file(&p)?,
            });
        } else {
            return Err("snapshot contains a nonregular file".into());
        }
    }
    sync_dir(dir)
}
impl AssistRuntime {
    pub fn expire_cluster_snapshot(&self) -> Result<()> {
        if !self.config.trusted_cluster {
            return Ok(());
        }
        let _guard = self
            .snapshot_lock
            .lock()
            .map_err(|_| "snapshot lock poisoned")?;
        let p = self.root.join("snapshot.json");
        if p.exists() && !cluster_lease_alive(&self.root) {
            let mut state: DonorSession = read(&p)?;
            if state.phase != "released" {
                state.phase = "released".into();
                save(&p, &state)?;
                let tree = self.root.join("snapshot");
                if tree.exists() {
                    fs::remove_dir_all(tree).map_err(io)?;
                    sync_dir(&self.root)?;
                }
            }
        }
        Ok(())
    }
    fn status_without_journal_solid(&self) -> Result<i64> {
        tron_chainbase::DynamicPropertiesStore::new(self.stores.dyn_props.clone())
            .latest_solidified_block_num()
            .ok_or_else(|| "missing solid".into())
    }
    pub fn snapshot_open(&self, peer: &str, request: &w::SnapshotOpen) -> Result<w::SnapshotState> {
        if !valid_id(&request.request_id) {
            return Err("invalid bootstrap request ID".into());
        }
        let _session = self
            .snapshot_lock
            .lock()
            .map_err(|_| "snapshot lock poisoned")?;
        let metadata = self.root.join("snapshot.json");
        let mut old: Option<DonorSession> = if metadata.exists() {
            Some(read(&metadata)?)
        } else {
            None
        };
        if let Some(s) = &old {
            if s.owner != peer
                && s.phase != "released"
                && !(self.config.trusted_cluster
                    && (s.phase == "installed" || !cluster_lease_alive(&self.root)))
            {
                return Err("SNAPSHOT_BUSY: another receiver owns the only snapshot".into());
            }
            if s.phase == "released" && s.request_id == request.request_id {
                return Err("SNAPSHOT_GONE: this session was explicitly released".into());
            }
            if s.retired.contains(&request.request_id) {
                return Err("SNAPSHOT_SUPERSEDED: obsolete create request".into());
            }
            if s.request_id != request.request_id
                && !request.replace_snapshot_id.is_empty()
                && request.replace_snapshot_id != s.manifest.snapshot_id
            {
                return Err(
                    "SNAPSHOT_CONFLICT: replacement requires the active snapshot ID".into(),
                );
            }
        } else if !request.replace_snapshot_id.is_empty() {
            return Err("SNAPSHOT_GONE: explicit new bootstrap required".into());
        }
        if old.as_ref().is_none_or(|s| {
            s.request_id != request.request_id
                || !["ready", "installed"].contains(&s.phase.as_str())
        }) {
            // Persist the retention pin while holding the same writer and journal locks.
            // A partially created set is discarded in full after restart, never mixed across blocks.
            let started = std::time::Instant::now();
            let _writer = self.leadership.lock_apply();
            let (height, block_id) = self.head()?;
            let journal = self.journal.lock().map_err(|_| "journal poisoned")?;
            let digest = journal.parent_digest(height + 1, &block_id)?;
            let id = request.request_id.clone();
            let mut retired = old.as_ref().map(|s| s.retired.clone()).unwrap_or_default();
            if let Some(previous) = &old {
                if previous.request_id != request.request_id {
                    retired.push(previous.request_id.clone());
                }
            }
            let s = DonorSession {
                retired,
                owner: peer.into(),
                request_id: id.clone(),
                phase: "creating".into(),
                retention_height: height + 1,
                manifest: Manifest {
                    format: 1,
                    snapshot_id: id,
                    network: self.network.clone(),
                    profile: self.profile.clone(),
                    height,
                    block_id,
                    digest,
                    files: vec![],
                },
            };
            save(&metadata, &s)?;
            crash_point("donor-pinned");
            drop(journal);
            let root = self.root.join("snapshot");
            if root.exists() {
                fs::remove_dir_all(&root).map_err(io)?;
                sync_dir(&self.root)?;
            }
            fs::create_dir_all(&root).map_err(io)?;
            self.stores.export_checkpoints(&root.join("database"))?;
            for name in ["devnet.json", "private-profile.json"] {
                let marker = self.root.parent().ok_or("missing data dir")?.join(name);
                if marker.exists() {
                    journal::atomic_write(&root.join(name), &fs::read(marker).map_err(io)?)?;
                }
            }
            // All checkpoint handles are now independent of future donor writes.
            crash_point("donor-checkpointed");
            drop(_writer);
            tracing::info!(
                height,
                seconds = started.elapsed().as_secs_f64(),
                "bootstrap checkpoint writer pause complete"
            );
            let mut s = s;
            enumerate(&root, &root, &mut s.manifest.files)?;
            s.manifest
                .validate(&self.network, self.config.max_snapshot_bytes)?;
            s.phase = "ready".into();
            save(&metadata, &s)?;
            crash_point("donor-ready");
            old = Some(s);
        }
        let s = old.ok_or("snapshot session missing")?;
        if self.config.trusted_cluster {
            renew_cluster_lease(&self.root, true)?;
        }
        let status = self.status()?;
        if self.canonical_id(s.manifest.height)? != s.manifest.block_id {
            return Err("SNAPSHOT_REORG: explicitly replace the snapshot".into());
        }
        if self
            .journal
            .lock()
            .map_err(|_| "journal poisoned")?
            .meta
            .floor
            > s.manifest.height + 1
        {
            return Err("HISTORY_GAP: replace snapshot; its post-snapshot journal was acknowledged and pruned".into());
        }
        Ok(w::SnapshotState {
            manifest_json: serde_json::to_vec(&s.manifest).map_err(io)?,
            ready: status.solid >= s.manifest.height,
        })
    }
    pub fn snapshot_read(&self, peer: &str, request: &w::SnapshotRead) -> Result<w::SnapshotChunk> {
        let _session = self
            .snapshot_lock
            .lock()
            .map_err(|_| "snapshot lock poisoned")?;
        let s: DonorSession = read(&self.root.join("snapshot.json"))?;
        if s.owner != peer
            || s.manifest.snapshot_id != request.snapshot_id
            || !["ready", "installed"].contains(&s.phase.as_str())
        {
            return Err("snapshot session is not available to this receiver".into());
        }
        if self.status()?.solid < s.manifest.height
            || self.canonical_id(s.manifest.height)? != s.manifest.block_id
        {
            return Err("snapshot is not canonical solid".into());
        }
        if self.config.trusted_cluster {
            renew_cluster_lease(&self.root, false)?;
        }
        let entry = s
            .manifest
            .files
            .get(request.file as usize)
            .ok_or("invalid snapshot file index")?;
        if request.offset > entry.size {
            return Err("snapshot offset exceeds file size".into());
        }
        let path = safe_path(&self.root.join("snapshot"), &entry.path)?;
        let mut file = File::open(path).map_err(io)?;
        file.seek(SeekFrom::Start(request.offset)).map_err(io)?;
        let mut data = vec![0; ((entry.size - request.offset).min(CHUNK as u64)) as usize];
        file.read_exact(&mut data).map_err(io)?;
        Ok(w::SnapshotChunk {
            snapshot_id: request.snapshot_id.clone(),
            file: request.file,
            offset: request.offset,
            sha256: Sha256::digest(&data).to_vec(),
            data,
        })
    }
    pub fn snapshot_release(
        &self,
        peer: &str,
        request: &w::SnapshotRelease,
    ) -> Result<w::SnapshotAccepted> {
        let _session = self
            .snapshot_lock
            .lock()
            .map_err(|_| "snapshot lock poisoned")?;
        let path = self.root.join("snapshot.json");
        let mut s: DonorSession = read(&path)?;
        if s.owner != peer || s.manifest.snapshot_id != request.snapshot_id {
            return Err("snapshot release ownership mismatch".into());
        }
        s.phase = "released".into();
        save(&path, &s)?; // A retry or crash must never resurrect the retention lease.
        let files = self.root.join("snapshot");
        if files.exists() {
            fs::remove_dir_all(files).map_err(io)?;
            sync_dir(&self.root)?;
        }
        Ok(w::SnapshotAccepted {
            snapshot_id: request.snapshot_id.clone(),
        })
    }

    pub fn snapshot_installed(
        &self,
        peer: &str,
        ack: &w::SnapshotInstalled,
    ) -> Result<w::SnapshotAccepted> {
        let _session = self
            .snapshot_lock
            .lock()
            .map_err(|_| "snapshot lock poisoned")?;
        let path = self.root.join("snapshot.json");
        let mut s: DonorSession = read(&path)?;
        if s.owner != peer
            || !["ready", "installed"].contains(&s.phase.as_str())
            || s.manifest.snapshot_id != ack.snapshot_id
            || s.manifest.hash()? != ack.manifest_sha256
        {
            return Err("snapshot installation ACK mismatch".into());
        }
        if self.status()?.solid < s.manifest.height
            || self.canonical_id(s.manifest.height)? != s.manifest.block_id
        {
            return Err("snapshot installation ACK is not solid".into());
        }
        // This is a validated base-state ACK, distinct from ACKing a journal record.
        let mut j = self.journal.lock().map_err(|_| "journal poisoned")?;
        let base = w::Ack {
            height: s.manifest.height,
            block_id: s.manifest.block_id.clone(),
            digest: s.manifest.digest.clone(),
            semantic_digest: Vec::new(),
        };
        if !self.config.trusted_cluster
            && j.acks.get(peer).is_none_or(|old| old.height < base.height)
        {
            journal::atomic_write(
                &self.root.join("acks").join(format!("{peer}.pb")),
                &prost::Message::encode_to_vec(&base),
            )?;
            j.acks.insert(peer.into(), base);
        }
        s.phase = "installed".into();
        save(&path, &s)?;
        j.prune(self.status_without_journal_solid()?)?;
        crash_point("donor-installed-ack");
        Ok(w::SnapshotAccepted {
            snapshot_id: ack.snapshot_id.clone(),
        })
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Download {
    pub request_id: String,
    pub replace_snapshot_id: String,
    pub reset_token: String,
    pub phase: String,
    pub manifest: Option<Manifest>,
    pub file: usize,
    pub offset: u64,
    pub downloaded: u64,
}
impl Download {
    fn path(data: &Path) -> PathBuf {
        data.join("bootstrap/receiver.json")
    }
    pub fn save(&self, data: &Path) -> Result<()> {
        save(&Self::path(data), self)
    }
    pub fn load_or_create(data: &Path, token: &str) -> Result<Self> {
        let p = Self::path(data);
        let mut s: Self = if p.exists() {
            read(&p)?
        } else {
            Self {
                request_id: format!("{:032x}", rand::random::<u128>()),
                replace_snapshot_id: String::new(),
                reset_token: token.into(),
                phase: "requesting".into(),
                manifest: None,
                file: 0,
                offset: 0,
                downloaded: 0,
            }
        };
        if s.reset_token != token {
            if s.phase == "complete" || s.phase == "installed" || s.phase == "installing" {
                return Err(
                    "bootstrap reset requires a fresh empty data directory after installation"
                        .into(),
                );
            }
            s = Self {
                replace_snapshot_id: s
                    .manifest
                    .as_ref()
                    .map(|m| m.snapshot_id.clone())
                    .unwrap_or_else(|| s.request_id.clone()),
                request_id: format!("{:032x}", rand::random::<u128>()),
                reset_token: token.into(),
                phase: "requesting".into(),
                manifest: None,
                file: 0,
                offset: 0,
                downloaded: 0,
            };
        }
        if !valid_id(&s.request_id)
            || ![
                "requesting",
                "downloading",
                "installing",
                "installed",
                "complete",
            ]
            .contains(&s.phase.as_str())
        {
            return Err("invalid durable bootstrap cursor".into());
        }
        if let Some(m) = &s.manifest {
            if s.file > m.files.len()
                || (s.file == m.files.len() && s.offset != 0)
                || m.files.get(s.file).is_some_and(|f| s.offset > f.size)
            {
                return Err("durable snapshot offset is outside the manifest".into());
            }
        } else if s.file != 0 || s.offset != 0 || s.phase != "requesting" {
            return Err("bootstrap cursor has no manifest".into());
        }
        s.save(data)?;
        Ok(s)
    }
    pub fn restart(&mut self, data: &Path, donor_lost_session: bool) -> Result<()> {
        if ["installing", "installed", "complete"].contains(&self.phase.as_str()) {
            return Err("cannot discard an activated snapshot automatically".into());
        }
        let previous = self.request_id.clone();
        *self = Self {
            request_id: format!("{:032x}", rand::random::<u128>()),
            replace_snapshot_id: if donor_lost_session {
                String::new()
            } else {
                previous.clone()
            },
            reset_token: self.reset_token.clone(),
            phase: "requesting".into(),
            manifest: None,
            file: 0,
            offset: 0,
            downloaded: 0,
        };
        self.save(data)?;
        let obsolete = data.join("bootstrap").join(previous);
        if obsolete.exists() {
            fs::remove_dir_all(obsolete).map_err(io)?;
            sync_dir(&data.join("bootstrap"))?;
        }
        Ok(())
    }
    pub fn stage(&self, data: &Path) -> PathBuf {
        data.join("bootstrap").join(&self.request_id)
    }
    pub fn accept_manifest(
        &mut self,
        data: &Path,
        state: &w::SnapshotState,
        network: &[u8],
        max: u64,
    ) -> Result<bool> {
        let m: Manifest = serde_json::from_slice(&state.manifest_json).map_err(io)?;
        m.validate(network, max)?;
        if m.snapshot_id != self.request_id {
            return Err("snapshot request identity mismatch".into());
        }
        if let Some(previous) = &self.manifest {
            if previous.hash()? != m.hash()? {
                return Err("immutable snapshot manifest changed".into());
            }
        }
        self.manifest = Some(m);
        if state.ready && self.phase == "requesting" {
            self.phase = "downloading".into();
        }
        self.save(data)?;
        Ok(state.ready)
    }
    pub fn next_read(&self) -> Option<w::SnapshotRead> {
        let m = self.manifest.as_ref()?;
        (self.file < m.files.len()).then(|| w::SnapshotRead {
            snapshot_id: m.snapshot_id.clone(),
            file: self.file as u32,
            offset: self.offset,
        })
    }
    pub fn accept_chunk(&mut self, data: &Path, c: &w::SnapshotChunk) -> Result<()> {
        let m = self.manifest.as_ref().ok_or("chunk without manifest")?;
        let entry = m.files.get(self.file).ok_or("chunk after snapshot end")?;
        if c.snapshot_id != m.snapshot_id
            || c.file as usize != self.file
            || c.offset != self.offset
            || c.data.len() > CHUNK
            || c.sha256.as_slice() != &Sha256::digest(&c.data)[..]
            || c.data.len() as u64 != (entry.size - self.offset).min(CHUNK as u64)
        {
            return Err("snapshot chunk identity, length or checksum mismatch".into());
        }
        let root = self.stage(data);
        let path = safe_path(&root, &entry.path)?;
        fs::create_dir_all(path.parent().ok_or("file parent missing")?).map_err(io)?;
        let mut f = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(io)?;
        if f.metadata().map_err(io)?.len() < self.offset {
            return Err("durable download cursor is ahead of local file".into());
        }
        f.set_len(self.offset).map_err(io)?;
        f.seek(SeekFrom::Start(self.offset)).map_err(io)?;
        f.write_all(&c.data)
            .and_then(|_| f.sync_all())
            .map_err(io)?;
        sync_dir(path.parent().unwrap())?;
        crash_point("chunk-data-synced");
        self.offset += c.data.len() as u64;
        self.downloaded += c.data.len() as u64;
        if self.offset == entry.size {
            if hash_file(&path)? != entry.sha256 {
                return Err("complete snapshot file checksum mismatch".into());
            }
            self.file += 1;
            self.offset = 0;
        }
        self.save(data)?;
        crash_point("chunk-cursor-synced");
        Ok(())
    }
    pub fn install(&mut self, data: &Path) -> Result<w::SnapshotInstalled> {
        let m = self.manifest.clone().ok_or("install without manifest")?;
        if self.file != m.files.len() {
            return Err("cannot install incomplete snapshot".into());
        }
        let staging = self.stage(data);
        if self.phase != "installed" {
            if self.phase != "installing" {
                for f in &m.files {
                    if hash_file(&safe_path(&staging, &f.path)?)? != f.sha256 {
                        return Err("snapshot changed before installation".into());
                    }
                }
                // READY intent precedes atomic activation. No backend or public RPC is open yet.
                self.phase = "installing".into();
                self.save(data)?;
                crash_point("receiver-install-intent");
            }
            let destination = data.join("database");
            if staging.join("database").exists() {
                if destination.exists() || data.join("db").exists() {
                    return Err("refusing to overwrite an existing ledger".into());
                }
                fs::rename(staging.join("database"), &destination).map_err(io)?;
                sync_dir(data)?;
                sync_dir(&staging)?;
                crash_point("receiver-database-activated");
            } else if !destination.exists() {
                return Err("installation lost both staged and activated database".into());
            }
            for name in ["devnet.json", "private-profile.json"] {
                let marker = staging.join(name);
                if marker.exists() {
                    journal::atomic_write(&data.join(name), &fs::read(marker).map_err(io)?)?;
                }
            }
            {
                use tron_chainbase::KvBackend;
                let idx = tron_chainbase::RocksDbBackend::open(destination.join("block-index"))
                    .map_err(io)?;
                if idx.get(&0i64.to_be_bytes()).map_err(io)? != Some(m.network.clone())
                    || idx.get(&m.height.to_be_bytes()).map_err(io)? != Some(m.block_id.clone())
                {
                    return Err(
                        "installed database genesis or checkpoint anchor differs from manifest"
                            .into(),
                    );
                }
                let properties =
                    tron_chainbase::RocksDbBackend::open(destination.join("properties"))
                        .map_err(io)?;
                let dp =
                    tron_chainbase::DynamicPropertiesStore::new(std::sync::Arc::new(properties));
                if dp.latest_block_header_number() != Some(m.height)
                    || dp
                        .latest_block_header_hash()
                        .map_err(io)?
                        .map(|x| x.to_vec())
                        != Some(m.block_id.clone())
                {
                    return Err("installed database head differs from manifest".into());
                }
            }
            let root = data.join("assisted-sync");
            let cursor = super::runtime::ReceiverCursor {
                height: m.height,
                block_id: m.block_id.clone(),
                digest: m.digest.clone(),
                assisted: true,
                imported_blocks: 0,
            };
            save(&root.join("receiver.json"), &cursor)?;
            let meta = journal::Meta {
                format: journal::FORMAT,
                network: m.network.clone(),
                profile: m.profile.clone(),
                anchor_height: m.height,
                anchor_id: m.block_id.clone(),
                anchor_digest: m.digest.clone(),
                floor: m.height + 1,
            };
            save(&root.join("donor.json"), &meta)?;
            crash_point("receiver-base-cursor-synced");
            self.phase = "installed".into();
            self.save(data)?;
            crash_point("receiver-installed");
        }
        Ok(w::SnapshotInstalled {
            snapshot_id: m.snapshot_id.clone(),
            manifest_sha256: m.hash()?,
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn paths_cannot_escape_ledger_staging() {
        for s in [
            "../secret",
            "/tmp/x",
            "database/../x",
            "database/x/../../x",
            "database\\\\x",
            "assisted-sync/donor.json",
        ] {
            assert!(safe_path(Path::new("/tmp/stage"), s).is_err(), "{s}");
        }
        assert!(safe_path(Path::new("/tmp/stage"), "database/properties/CURRENT").is_ok());
    }
    #[test]
    fn reset_is_explicit_and_durable() {
        let t = tempfile::tempdir().unwrap();
        let a = Download::load_or_create(t.path(), "").unwrap();
        assert_eq!(
            Download::load_or_create(t.path(), "").unwrap().request_id,
            a.request_id
        );
        let b = Download::load_or_create(t.path(), "reset-1").unwrap();
        assert_ne!(a.request_id, b.request_id);
        assert_eq!(b.replace_snapshot_id, a.request_id);
        assert_eq!(
            Download::load_or_create(t.path(), "reset-1")
                .unwrap()
                .request_id,
            b.request_id
        );
    }
}
