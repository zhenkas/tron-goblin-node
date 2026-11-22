//! Block commit stores accounts the way java-tron does under
//! `ALLOW_ASSET_OPTIMIZATION` (`SnapshotRoot.processAccount`): the account
//! row carries `asset_optimized` and no inline TRC-10 maps, the balances
//! live as rows in `account-asset`.
//!
//! Pinned here, over real TransferAsset blocks:
//!   * the stored form after a block, and that the next block still reads
//!     the right balances from it;
//!   * rollback restores account rows AND balance rows byte-for-byte;
//!   * the pipelined applier ends in the same bytes and the same undo logs
//!     as the classic path, including a block that executes while the
//!     previous block's balance rows are still being committed.
//!
//! `import_all_asset` reads a process-wide backend (java's static
//! `AssetUtil.accountAssetStore`), so every test in this binary shares one
//! `account-asset` backend and runs under a lock.

use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};

use hex_literal::hex;
use prost::Message;
use prost_types::Any;
use tron_chainbase::{
    AccountAssetStore, AccountStore, BlockUndoStore, CheckPointV2, DynamicPropertiesStore,
    KvBackend, MemBackend, UndoEntry, UndoStoreId,
};
use tron_crypto::address::Address;
use tron_executor::{
    execute_block_with_undo_checkpoint_and_config, rollback_block, ApplyPipeline, ExecConfig,
    StateBackends,
};
use tron_proto::block_header::Raw as BlockHeaderRaw;
use tron_proto::transaction::contract::ContractType;
use tron_proto::transaction::{Contract as TxContract, Raw as TxRaw};
use tron_proto::{Account, AccountType, Block, BlockHeader, Transaction, TransferAssetContract};
use tron_types::{calc_tx_trie_root, sign_block};

const ALICE: [u8; 21] = hex!("412e988a386a799f506693793c6a5af6b54dfaabfb");
const ALICE_PRIV: [u8; 32] =
    hex!("1234567890123456789012345678901234567890123456789012345678901234");
const BOB: [u8; 21] = hex!("41a614f803b6fd780986a42c78ec9c7f77e6ded13c");
const ISSUER: [u8; 21] = hex!("4193df56b9d51e84e90c4d61c4ce80a6fb3e57f5dd");
const TOKEN: &str = "1000001";
const STALE_TOKEN: &str = "1000009";
/// A balance ALICE holds and never moves.
const IDLE_TOKEN: &str = "1000002";

fn mem() -> Arc<dyn KvBackend> {
    Arc::new(MemBackend::new())
}

/// The one `account-asset` backend of this test binary, emptied for each
/// test (callers hold [`lock`]).
fn shared_rows() -> Arc<dyn KvBackend> {
    static ROWS: OnceLock<Arc<dyn KvBackend>> = OnceLock::new();
    let rows = ROWS.get_or_init(mem).clone();
    tron_chainbase::set_account_asset_backend(rows.clone());
    for (k, _) in rows.scan_all().unwrap() {
        rows.delete(&k).unwrap();
    }
    rows
}

fn lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

/// ALICE holds 100 TOKEN inline and is NOT optimized yet (an account last
/// written before the optimization), with a stale balance row; BOB exists.
fn seeded_state() -> StateBackends {
    let rows = shared_rows();
    let state = StateBackends {
        accounts: mem(),
        witnesses: mem(),
        votes: mem(),
        delegation: mem(),
        delegated_resources: mem(),
        delegated_resource_account_index: None,
        dyn_props: mem(),
        proposals: mem(),
        name_index: mem(),
        id_index: mem(),
        asset_v1: mem(),
        asset_v2: mem(),
        contracts: mem(),
        abi: mem(),
        exchange_v1: mem(),
        exchange_v2: mem(),
        market_orders: mem(),
        market_account: mem(),
        nullifiers: mem(),
        merkle_trees: None,
        code: None,
        storage_row: None,
        contract_state: None,
        block_index: None,
        witness_schedule: None,
        reward_vi: None,
        account_asset: Some(rows.clone()),
    };
    let dp = DynamicPropertiesStore::new(state.dyn_props.clone());
    dp.save_latest_block_header_timestamp(1_700_000_000_000);
    dp.save_allow_same_token_name(1);
    dp.put_long(b"ALLOW_ASSET_OPTIMIZATION", 1);
    let accounts = AccountStore::new(state.accounts.clone());
    accounts
        .put(
            &Address::from_raw(ALICE),
            &Account {
                address: ALICE.to_vec(),
                balance: 10_000_000,
                r#type: AccountType::Normal as i32,
                asset: [("OLDNAME".to_string(), 100)].into_iter().collect(),
                asset_v2: [(TOKEN.to_string(), 100), (IDLE_TOKEN.to_string(), 7)]
                    .into_iter()
                    .collect(),
                ..Default::default()
            },
        )
        .unwrap();
    accounts
        .put(
            &Address::from_raw(BOB),
            &Account {
                address: BOB.to_vec(),
                balance: 1,
                r#type: AccountType::Normal as i32,
                ..Default::default()
            },
        )
        .unwrap();
    AccountAssetStore::new(rows)
        .put(&Address::from_raw(ALICE), STALE_TOKEN.as_bytes(), 55)
        .unwrap();
    tron_chainbase::AssetIssueV2Store::new(state.asset_v2.clone())
        .put(
            TOKEN.parse().unwrap(),
            &tron_proto::AssetIssueContract {
                id: TOKEN.to_string(),
                owner_address: ISSUER.to_vec(),
                name: b"TKN".to_vec(),
                ..Default::default()
            },
        )
        .unwrap();
    state
}

fn transfer_asset_tx(amount: i64, nonce: i64) -> Transaction {
    let tc = TransferAssetContract {
        asset_name: TOKEN.as_bytes().to_vec(),
        owner_address: ALICE.to_vec(),
        to_address: BOB.to_vec(),
        amount,
    };
    let mut tx = Transaction {
        raw_data: Some(TxRaw {
            ref_block_bytes: vec![0, 1],
            ref_block_num: 0,
            ref_block_hash: vec![0u8; 8],
            expiration: 1_700_000_000_000 + 86_400_000,
            auths: Vec::new(),
            data: Vec::new(),
            contract: vec![TxContract {
                r#type: ContractType::TransferAssetContract as i32,
                parameter: Some(Any {
                    type_url: "type.googleapis.com/protocol.TransferAssetContract".into(),
                    value: tc.encode_to_vec(),
                }),
                provider: Vec::new(),
                contract_name: Vec::new(),
                permission_id: 0,
            }],
            scripts: Vec::new(),
            timestamp: 1_700_000_000_000 + nonce,
            fee_limit: 0,
        }),
        signature: Vec::new(),
        ret: Vec::new(),
        unparsed_field10: None,
    };
    tron_types::sign_transaction(&mut tx, &ALICE_PRIV).expect("sign");
    tx
}

fn block(number: i64, transactions: Vec<Transaction>) -> Block {
    let tx_root = calc_tx_trie_root(&transactions).map(|h| h.to_vec()).unwrap_or_default();
    let mut block = Block {
        transactions,
        block_header: Some(BlockHeader {
            raw_data: Some(BlockHeaderRaw {
                timestamp: 1_700_000_000_000 + number * 3000,
                tx_trie_root: tx_root,
                parent_hash: vec![0u8; 32],
                number,
                witness_id: 0,
                witness_address: ALICE.to_vec(),
                version: 28,
                account_state_root: Vec::new(),
            }),
            witness_signature: Vec::new(),
        }),
    };
    sign_block(&mut block, &ALICE_PRIV).expect("sign");
    block
}

/// Three blocks: 30, then 20, then the remaining 50 (ALICE's balance
/// reaches zero, which java stores as "no row").
fn chain() -> Vec<Block> {
    vec![
        block(1, vec![transfer_asset_tx(30, 1)]),
        block(2, vec![transfer_asset_tx(20, 2)]),
        block(3, vec![transfer_asset_tx(50, 3)]),
    ]
}

fn tmp_checkpoint_root(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "tron-asset-split-{tag}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

type Rows = Vec<(Vec<u8>, Vec<u8>)>;

fn sorted(b: &Arc<dyn KvBackend>) -> Rows {
    let mut v = b.scan_all().unwrap();
    v.sort();
    v
}

/// (account rows, balance rows, dynamic properties) — the stores these
/// blocks touch.
fn snapshot(state: &StateBackends) -> (Rows, Rows, Rows) {
    (
        sorted(&state.accounts),
        sorted(state.account_asset.as_ref().unwrap()),
        sorted(&state.dyn_props),
    )
}

fn account(state: &StateBackends, a: [u8; 21]) -> Account {
    AccountStore::new(state.accounts.clone())
        .get(&Address::from_raw(a))
        .unwrap()
        .unwrap()
}

fn row(state: &StateBackends, a: [u8; 21], token: &str) -> Option<i64> {
    AccountAssetStore::new(state.account_asset.clone().unwrap())
        .get(&Address::from_raw(a), token.as_bytes())
        .unwrap()
}

fn undo_logs(backend: &Arc<dyn KvBackend>) -> Vec<(Vec<u8>, Vec<UndoEntry>)> {
    let store = BlockUndoStore::new(backend.clone());
    let mut out = Vec::new();
    for (k, _) in sorted(backend) {
        let num = i64::from_be_bytes(k.as_slice().try_into().unwrap());
        let mut entries = store.get(num).unwrap().unwrap().entries;
        entries.sort_by(|a, b| (a.store as u8, &a.key).cmp(&(b.store as u8, &b.key)));
        out.push((k, entries));
    }
    out
}

fn assert_stored_form(state: &StateBackends, who: [u8; 21]) {
    let a = account(state, who);
    assert!(a.asset_optimized, "written account must carry asset_optimized");
    assert!(a.asset.is_empty(), "name-keyed asset map is cleared on write");
    assert!(a.asset_v2.is_empty(), "TRC-10 balances live in account-asset rows");
}

#[test]
fn commit_stores_accounts_like_java_and_rollback_restores_rows() {
    let _g = lock();
    let state = seeded_state();
    let initial = snapshot(&state);
    let undo = BlockUndoStore::new(mem());
    let cp = CheckPointV2::new(&tmp_checkpoint_root("classic"));
    let config = ExecConfig { capture_state_deltas: true, ..ExecConfig::unsigned() };
    let blocks = chain();

    let report = execute_block_with_undo_checkpoint_and_config(
        &state, &blocks[0], None, &undo, &cp, &config, None,
    )
    .unwrap();
    assert_eq!(report.failures(), 0, "{:?}", report.tx_results);
    assert_stored_form(&state, ALICE);
    assert_stored_form(&state, BOB);
    assert_eq!(row(&state, ALICE, TOKEN), Some(70));
    assert_eq!(row(&state, BOB, TOKEN), Some(30));
    // First optimized write of an account drops whatever rows it had.
    assert_eq!(row(&state, ALICE, STALE_TOKEN), None);
    // The balance rows travel with the block: replicas and the archive get
    // them as ordinary deltas of their own store.
    let deltas = report.state_deltas.expect("deltas captured");
    let asset_deltas: Vec<_> =
        deltas.iter().filter(|d| d.store == UndoStoreId::AccountAsset).collect();
    assert_eq!(asset_deltas.len(), 4, "ALICE token, ALICE idle token, ALICE stale row, BOB token");
    let after_1 = snapshot(&state);

    // The next block reads ALICE's balance back out of the rows.
    let report = execute_block_with_undo_checkpoint_and_config(
        &state, &blocks[1], None, &undo, &cp, &config, None,
    )
    .unwrap();
    assert_eq!(report.failures(), 0, "{:?}", report.tx_results);
    assert_eq!(row(&state, ALICE, TOKEN), Some(50));
    assert_eq!(row(&state, BOB, TOKEN), Some(50));
    // ALICE's account is written back with both balances, but only the one
    // that moved is written to its row again.
    let deltas = report.state_deltas.expect("deltas captured");
    let asset_deltas: Vec<_> =
        deltas.iter().filter(|d| d.store == UndoStoreId::AccountAsset).collect();
    assert_eq!(asset_deltas.len(), 2, "ALICE token and BOB token; the idle balance is untouched");
    assert_eq!(row(&state, ALICE, IDLE_TOKEN), Some(7));
    let after_2 = snapshot(&state);

    let report = execute_block_with_undo_checkpoint_and_config(
        &state, &blocks[2], None, &undo, &cp, &config, None,
    )
    .unwrap();
    assert_eq!(report.failures(), 0, "{:?}", report.tx_results);
    assert_eq!(row(&state, ALICE, TOKEN), None, "a zero balance is no row");
    assert_eq!(row(&state, BOB, TOKEN), Some(100));
    assert_stored_form(&state, ALICE);

    rollback_block(&state, 3, &undo).unwrap();
    assert_eq!(snapshot(&state), after_2);
    rollback_block(&state, 2, &undo).unwrap();
    assert_eq!(snapshot(&state), after_1);
    rollback_block(&state, 1, &undo).unwrap();
    assert_eq!(snapshot(&state), initial, "inline balances and the stale row are back");
}

#[test]
fn pipelined_apply_matches_classic_bytes_and_undo_logs() {
    let _g = lock();
    let config = ExecConfig::unsigned();
    let blocks = chain();

    // Classic reference.
    let state = seeded_state();
    let classic_undo_be = mem();
    let undo = BlockUndoStore::new(classic_undo_be.clone());
    let cp = CheckPointV2::new(&tmp_checkpoint_root("ref"));
    for b in &blocks {
        let r = execute_block_with_undo_checkpoint_and_config(
            &state, b, None, &undo, &cp, &config, None,
        )
        .unwrap();
        assert_eq!(r.failures(), 0, "{:?}", r.tx_results);
    }
    let classic = snapshot(&state);
    let classic_undo = undo_logs(&classic_undo_be);

    // Pipelined, from the same starting state. Block N+1 executes while
    // block N's rows may not have reached the base stores yet.
    let state = seeded_state();
    let pipelined_undo_be = mem();
    let mut pipeline = ApplyPipeline::new(
        &state,
        BlockUndoStore::new(pipelined_undo_be.clone()),
        CheckPointV2::new(&tmp_checkpoint_root("pipe")),
    );
    let mut left = 100;
    for (b, moved) in blocks.iter().zip([30, 20, 50]) {
        let r = pipeline.apply(b, None, &config, None).unwrap();
        assert_eq!(r.failures(), 0, "{:?}", r.tx_results);
        // Until the commit is joined, readers of the pipeline view get the
        // account as executed: its inline balance does not depend on
        // whether the balance rows have reached the base store yet.
        left -= moved;
        let pending = AccountStore::new(pipeline.view().accounts.clone())
            .get(&Address::from_raw(ALICE))
            .unwrap()
            .unwrap();
        assert_eq!(pending.asset_v2.get(TOKEN), Some(&left));
    }
    pipeline.flush().unwrap();
    assert_stored_form(&state, ALICE);
    assert_eq!(snapshot(&state), classic);
    assert_eq!(undo_logs(&pipelined_undo_be), classic_undo);

    // And the pipelined undo logs really unwind to the seeded state.
    let fresh = {
        drop(pipeline);
        let undo = BlockUndoStore::new(pipelined_undo_be);
        for n in [3, 2, 1] {
            rollback_block(&state, n, &undo).unwrap();
        }
        snapshot(&state)
    };
    let seeded = snapshot(&seeded_state());
    assert_eq!(fresh, seeded);
}

#[test]
fn accounts_stay_inline_while_the_optimization_is_off() {
    let _g = lock();
    let state = seeded_state();
    DynamicPropertiesStore::new(state.dyn_props.clone()).put_long(b"ALLOW_ASSET_OPTIMIZATION", 0);
    let undo = BlockUndoStore::new(mem());
    let cp = CheckPointV2::new(&tmp_checkpoint_root("off"));
    let r = execute_block_with_undo_checkpoint_and_config(
        &state, &chain()[0], None, &undo, &cp, &ExecConfig::unsigned(), None,
    )
    .unwrap();
    assert_eq!(r.failures(), 0, "{:?}", r.tx_results);
    let alice = account(&state, ALICE);
    assert!(!alice.asset_optimized);
    assert_eq!(alice.asset_v2.get(TOKEN), Some(&70));
    assert_eq!(row(&state, ALICE, TOKEN), None);
    assert_eq!(row(&state, ALICE, STALE_TOKEN), Some(55));
}
