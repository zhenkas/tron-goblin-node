//! AccountAssetStore — directory name `account-asset`.
//!
//! Per-account TRC-10 asset balances, split out of `AccountStore` for
//! storage efficiency. Each row is one `(owner_address, asset_id) →
//! balance` entry; what's logically `Map<asset_id, balance>` inside an
//! Account becomes a flat namespace of composite-keyed rows.
//!
//! Key:   `address(21) ‖ asset_id_bytes` (variable length).
//!        java-tron computes `Bytes.concat(account.getAddress().toByteArray(),
//!        k.getBytes())` where `k` is the asset-id string.
//! Value: 8-byte BE `i64` balance (sun-denominated).
//!
//! Source: `org.tron.core.store.AccountAssetStore`.

use std::sync::{Arc, OnceLock};

use tron_crypto::address::{Address, ADDRESS_LENGTH};
use tron_proto::Account;

use crate::backend::KvBackend;
use crate::stores::StoreError;

pub const DB_NAME: &str = "account-asset";

/// Process-wide account-asset backend, mirroring java-tron's static
/// `AssetUtil.accountAssetStore` (installed once at `ChainBaseManager` init).
/// Lets consensus actuators merge an optimized account's TRC10 balances
/// without threading the store through every `StateBackends`/dispatch call —
/// java takes exactly this shortcut for the same reason.
static ACCOUNT_ASSET_BACKEND: OnceLock<Arc<dyn KvBackend>> = OnceLock::new();

/// Install the global account-asset backend (java-tron's
/// `AssetUtil.setAccountAssetStore`). Set once at node startup; subsequent
/// calls are ignored. Never set in unit tests, so [`import_all_asset`] stays
/// a no-op there.
pub fn set_account_asset_backend(backend: Arc<dyn KvBackend>) {
    let _ = ACCOUNT_ASSET_BACKEND.set(backend);
}

/// java-tron's `AssetUtil.importAllAsset`: when `account` is asset-optimized
/// and the global backend is installed, merge its TRC10 balances out of the
/// account-asset store back into `asset_v2` so reads/debits see the real
/// balance. No-op for non-optimized accounts and when no backend is set.
pub fn import_all_asset(account: &mut Account) {
    if !account.asset_optimized {
        return;
    }
    if let Some(backend) = ACCOUNT_ASSET_BACKEND.get() {
        AccountAssetStore::new(backend.clone()).import_all_asset(account);
    }
}

/// Diagnostic: return the raw `(asset_id_string, balance)` rows the global
/// account-asset backend holds for `owner`, IGNORING the per-account
/// `asset_optimized` flag. Used only by env-gated SELFDESTRUCT tracing to tell
/// apart "store has no rows" from "asset_optimized=false skipped the rows".
/// Empty when no backend is installed (unit tests). Not on any consensus path.
pub fn account_asset_rows_for_trace(owner: &Address) -> Vec<(String, i64)> {
    let Some(backend) = ACCOUNT_ASSET_BACKEND.get() else {
        return Vec::new();
    };
    AccountAssetStore::new(backend.clone())
        .get_all_assets(owner)
        .map(|rows| {
            rows.into_iter()
                .map(|(id, bal)| (String::from_utf8_lossy(&id).into_owned(), bal))
                .collect()
        })
        .unwrap_or_default()
}

/// Result of [`split_account_assets`]: the account row as java-tron stores
/// it, plus the `account-asset` row writes that carry its TRC-10 balances.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountAssetSplit {
    /// Account bytes with `asset` / `asset_v2` cleared and
    /// `asset_optimized = true`.
    pub account: Vec<u8>,
    /// Row writes for the `account-asset` store, sorted by key.
    pub asset_ops: Vec<crate::backend::WriteOp>,
}

/// java-tron's `SnapshotRoot.processAccount` for one account write: the
/// on-disk form of an account under `ALLOW_ASSET_OPTIMIZATION`.
///
/// * not yet optimized -> every existing `account-asset` row of the address
///   is deleted and the flag is set (`getDeletedAssets` + `setAssetOptimized`);
/// * each inline `asset_v2` entry becomes a row write, a zero balance a row
///   delete (`AccountAssetStore.getAssets`);
/// * `asset` and `asset_v2` are cleared (`AccountCapsule.clearAsset`).
///
/// `rows` is the `account-asset` view as of just before this write; it is
/// only scanned for accounts that are not optimized yet. `existed` tells
/// whether the account row existed before the write: a brand-new account
/// has no rows to delete (rows are removed together with their account), so
/// the scan is skipped for it.
///
/// Returns `None` when the stored form equals `raw` (already optimized, no
/// inline balances) so the caller keeps the original bytes untouched.
pub fn split_account_assets(
    key: &[u8],
    raw: &[u8],
    existed: bool,
    rows: &dyn KvBackend,
) -> Result<Option<AccountAssetSplit>, StoreError> {
    use crate::backend::WriteOp;
    use prost::Message;

    let mut account = Account::decode(raw)?;
    if account.asset_optimized && account.asset.is_empty() && account.asset_v2.is_empty() {
        return Ok(None);
    }
    let mut ops: std::collections::BTreeMap<Vec<u8>, Option<Vec<u8>>> =
        std::collections::BTreeMap::new();
    if !account.asset_optimized {
        if existed {
            for (k, _) in rows.scan_prefix(key)? {
                ops.insert(k, None);
            }
        }
        account.asset_optimized = true;
    }
    // java keys the rows by `account.getAddress()`; that is the store key
    // for every well-formed account.
    let owner: &[u8] = if account.address.is_empty() { key } else { &account.address };
    for (id, balance) in &account.asset_v2 {
        let mut k = Vec::with_capacity(owner.len() + id.len());
        k.extend_from_slice(owner);
        k.extend_from_slice(id.as_bytes());
        ops.insert(k, (*balance != 0).then(|| balance.to_be_bytes().to_vec()));
    }
    account.asset.clear();
    account.asset_v2.clear();
    Ok(Some(AccountAssetSplit {
        account: account.encode_to_vec(),
        asset_ops: ops
            .into_iter()
            .map(|(k, v)| match v {
                Some(v) => WriteOp::Put(k, v),
                None => WriteOp::Delete(k),
            })
            .collect(),
    }))
}

/// Row deletes for an account that is being removed (java-tron
/// `AccountAssetStore.getDeletedAssets`).
pub fn deleted_account_asset_ops(
    key: &[u8],
    rows: &dyn KvBackend,
) -> Result<Vec<crate::backend::WriteOp>, StoreError> {
    Ok(rows
        .scan_prefix(key)?
        .into_iter()
        .map(|(k, _)| crate::backend::WriteOp::Delete(k))
        .collect())
}

pub struct AccountAssetStore {
    backend: Arc<dyn KvBackend>,
}

impl AccountAssetStore {
    pub const DB_NAME: &'static str = DB_NAME;

    pub fn new(backend: Arc<dyn KvBackend>) -> Self {
        Self { backend }
    }

    /// Build the composite key `address ‖ asset_id_bytes` (asset id is
    /// the UTF-8 bytes of the decimal-string token id, just like
    /// [`super::AssetIssueV2Store`]).
    pub fn key_for(address: &Address, asset_id: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(address.as_bytes().len() + asset_id.len());
        out.extend_from_slice(address.as_bytes());
        out.extend_from_slice(asset_id);
        out
    }

    pub fn put(&self, address: &Address, asset_id: &[u8], balance: i64) -> Result<(), StoreError> {
        let key = Self::key_for(address, asset_id);
        self.backend.put(&key, &balance.to_be_bytes())?;
        Ok(())
    }

    pub fn get(&self, address: &Address, asset_id: &[u8]) -> Result<Option<i64>, StoreError> {
        let key = Self::key_for(address, asset_id);
        let Some(bytes) = self.backend.get(&key)? else {
            return Ok(None);
        };
        if bytes.len() != 8 {
            return Err(StoreError::InvalidValueLength {
                got: bytes.len(),
                expected: 8,
            });
        }
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&bytes);
        Ok(Some(i64::from_be_bytes(buf)))
    }

    /// Return every `(asset_id, balance)` row for the given owner.
    /// Mirrors java-tron's
    /// `AccountAssetStore.getAllAssets(byte[] owner_address)` (which
    /// uses a prefix scan over the address bytes). Used by
    /// `Wallet.getAccount` to populate the `asset_v2` map and by the
    /// account-deletion path to enumerate every row to remove.
    ///
    /// Skips rows whose value isn't exactly 8 bytes (corrupted entries
    /// log + continue, matching java-tron).
    pub fn get_all_assets(&self, owner: &Address) -> Result<Vec<(Vec<u8>, i64)>, StoreError> {
        Ok(self
            .backend
            .scan_prefix(owner.as_bytes())?
            .into_iter()
            .filter_map(|(k, v)| {
                if v.len() != 8 {
                    // C-8: asset balances are i64 (8 bytes); any other
                    // length is corruption. Log + continue (java-tron
                    // parity) — as the doc-comment above already promises.
                    tracing::error!(
                        store = "account-asset",
                        key = %hex::encode(&k),
                        value_len = v.len(),
                        "skipping account-asset row whose value isn't an 8-byte balance"
                    );
                    return None;
                }
                // Strip the 21-byte address prefix to leave the
                // asset_id portion of the composite key.
                let asset_id = k.get(owner.as_bytes().len()..)?.to_vec();
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&v);
                Some((asset_id, i64::from_be_bytes(buf)))
            })
            .collect())
    }

    /// java-tron's `AssetUtil.importAllAsset`: merge this account's TRC10
    /// balances out of the store back into `asset_v2` — store rows first,
    /// then inline entries override (so an in-flight modification kept inline
    /// wins). Gated on `asset_optimized`; otherwise the balances are already
    /// inline and there's nothing to merge.
    pub fn import_all_asset(&self, account: &mut Account) {
        if !account.asset_optimized || account.address.len() != ADDRESS_LENGTH {
            return;
        }
        let mut addr = [0u8; ADDRESS_LENGTH];
        addr.copy_from_slice(&account.address);
        let owner = Address::from_raw(addr);
        let Ok(rows) = self.get_all_assets(&owner) else {
            return;
        };
        let mut merged: std::collections::BTreeMap<String, i64> = rows
            .into_iter()
            .map(|(id, bal)| (String::from_utf8_lossy(&id).into_owned(), bal))
            .collect();
        for (k, v) in &account.asset_v2 {
            merged.insert(k.clone(), *v);
        }
        account.asset_v2 = merged;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::MemBackend;
    use std::sync::Arc;

    fn addr(byte: u8) -> Address {
        let mut a = [0u8; 21];
        a[0] = 0x41;
        a[1..].fill(byte);
        Address::from_raw(a)
    }

    fn optimized(v2: &[(&str, i64)], flag: bool) -> Account {
        Account {
            address: addr(0xaa).as_bytes().to_vec(),
            balance: 7,
            asset_optimized: flag,
            asset: v2.iter().map(|(k, v)| (format!("name{k}"), *v)).collect(),
            asset_v2: v2.iter().map(|(k, v)| (k.to_string(), *v)).collect(),
            ..Default::default()
        }
    }

    #[test]
    fn split_moves_balances_to_rows_and_clears_both_maps() {
        use crate::backend::WriteOp;
        use prost::Message;
        let rows: Arc<dyn KvBackend> = Arc::new(MemBackend::new());
        let store = AccountAssetStore::new(rows.clone());
        let alice = addr(0xaa);
        // Stale rows of a not-yet-optimized account are dropped first.
        store.put(&alice, b"1000009", 55).unwrap();
        let raw = optimized(&[("1000001", 100), ("1000002", 0)], false).encode_to_vec();
        let split = split_account_assets(alice.as_bytes(), &raw, true, rows.as_ref())
            .unwrap()
            .expect("needs splitting");
        let stored = Account::decode(split.account.as_slice()).unwrap();
        assert!(stored.asset_optimized);
        assert!(stored.asset.is_empty() && stored.asset_v2.is_empty());
        assert_eq!(stored.balance, 7);
        assert_eq!(
            split.asset_ops,
            vec![
                WriteOp::Put(AccountAssetStore::key_for(&alice, b"1000001"), 100i64.to_be_bytes().to_vec()),
                WriteOp::Delete(AccountAssetStore::key_for(&alice, b"1000002")),
                WriteOp::Delete(AccountAssetStore::key_for(&alice, b"1000009")),
            ]
        );
    }

    #[test]
    fn split_of_optimized_account_touches_only_inline_rows() {
        use crate::backend::WriteOp;
        use prost::Message;
        let rows: Arc<dyn KvBackend> = Arc::new(MemBackend::new());
        let store = AccountAssetStore::new(rows.clone());
        let alice = addr(0xaa);
        store.put(&alice, b"1000009", 55).unwrap();
        let raw = optimized(&[("1000001", 3)], true).encode_to_vec();
        let split = split_account_assets(alice.as_bytes(), &raw, true, rows.as_ref())
            .unwrap()
            .unwrap();
        assert_eq!(
            split.asset_ops,
            vec![WriteOp::Put(AccountAssetStore::key_for(&alice, b"1000001"), 3i64.to_be_bytes().to_vec())]
        );
        // Already in stored form: nothing to do, bytes stay as they are.
        assert!(split_account_assets(alice.as_bytes(), &split.account, true, rows.as_ref())
            .unwrap()
            .is_none());
    }

    #[test]
    fn new_account_is_flagged_without_scanning_rows() {
        use prost::Message;
        let rows: Arc<dyn KvBackend> = Arc::new(MemBackend::new());
        let alice = addr(0xaa);
        let raw = optimized(&[], false).encode_to_vec();
        let split = split_account_assets(alice.as_bytes(), &raw, false, rows.as_ref())
            .unwrap()
            .unwrap();
        assert!(split.asset_ops.is_empty());
        assert!(Account::decode(split.account.as_slice()).unwrap().asset_optimized);
    }

    #[test]
    fn get_all_assets_returns_only_target_owner() {
        let backend: Arc<dyn KvBackend> = Arc::new(MemBackend::new());
        let store = AccountAssetStore::new(backend);
        let alice = addr(0xaa);
        let bob = addr(0xbb);
        store.put(&alice, b"1000001", 100).unwrap();
        store.put(&alice, b"1000002", 200).unwrap();
        store.put(&bob, b"1000003", 300).unwrap();
        let alice_assets = store.get_all_assets(&alice).unwrap();
        assert_eq!(alice_assets.len(), 2);
        let mut found = alice_assets
            .iter()
            .map(|(id, bal)| (String::from_utf8_lossy(id).to_string(), *bal))
            .collect::<Vec<_>>();
        found.sort();
        assert_eq!(
            found,
            vec![("1000001".to_string(), 100), ("1000002".to_string(), 200)]
        );
    }

    #[test]
    fn get_all_assets_empty_for_unknown_owner() {
        let backend: Arc<dyn KvBackend> = Arc::new(MemBackend::new());
        let store = AccountAssetStore::new(backend);
        assert!(store.get_all_assets(&addr(0xff)).unwrap().is_empty());
    }
}
