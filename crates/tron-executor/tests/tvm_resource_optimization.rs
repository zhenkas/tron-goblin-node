//! `ALLOW_TVM_RESOURCE_OPTIMIZATION` (proposal #104, java-tron 4.8.3) in
//! `TransactionTrace.pay`: what happens to the budget-time energy reservation
//! of a VM transaction that SUCCEEDS having used no energy at all.
//!
//! java reserves the caller's (and a distinct origin's) frozen energy while it
//! computes the budget (`VMActuator.getAccountEnergyLimitWithFixRatio` /
//! `getTotalEnergyLimitWithFixRatio`, Stake 2.0 only) and commits it with the
//! VM's repository. `TransactionTrace.pay` then takes the reservation back
//! (`resetAccountUsage`) on the in-memory capsules and relies on
//! `ReceiptCapsule.payEnergyBill` to store them — but `payEnergyBill` returns
//! before storing anything when `energyUsageTotal <= 0`. So before #104 the
//! reservation of a zero-energy success stays committed; #104 adds the missing
//! `accountStore.put` for the origin and the caller.
//!
//! `VMActuator.call` only computes a budget `if (isNotEmpty(code))`, so a
//! trigger of a contract with no code reserves nothing: before #104 its
//! accounts are not touched at all, with #104 they are stored after a
//! `resetAccountUsage` fed with the receipt's all-zero capture.

use std::sync::Arc;

use prost::Message;
use prost_types::Any;
use tron_chainbase::{
    AccountStore, CodeStore, ContractStore, DynamicPropertiesStore, KvBackend, MemBackend,
};
use tron_crypto::address::Address;
use tron_executor::energy::{get_pre_tx_energy, vm_energy_budget_trigger};
use tron_executor::{execute_block_with_config, ExecConfig, StateBackends, TxOutcome};
use tron_proto::account::{AccountResource, FreezeV2};
use tron_proto::{
    transaction::{contract::ContractType, Contract as TxContract, Raw as TxRaw},
    Account, Block, BlockHeader, SmartContract, Transaction, TriggerSmartContract,
};

const FEE_LIMIT: i64 = 100_000_000;
const STALE_USAGE: i64 = 5_000_000;
/// Head-block timestamp (the block before the one applied).
const HEAD_TS: i64 = 1_700_000_000_000;

fn mem() -> Arc<dyn KvBackend> {
    Arc::new(MemBackend::new())
}

fn addr_with_byte(byte: u8) -> [u8; 21] {
    let mut a = [0u8; 21];
    a[0] = 0x41;
    a[1..].fill(byte);
    a
}

/// Same derivation as `vm_integration.rs`: the caller must own the key that
/// signs the transaction.
fn caller_keypair(seed: u8) -> ([u8; 32], [u8; 21]) {
    use tron_crypto::signature::RecoverableSignature;
    let mut priv_key = [0u8; 32];
    priv_key[0] = 0x10;
    priv_key[31] = seed;
    let dummy_hash = [0x42u8; 32];
    let sig = RecoverableSignature::sign_prehash(&priv_key, &dummy_hash).expect("sign");
    let pub_key = sig.recover_uncompressed_pubkey(&dummy_hash).expect("recover");
    let h = tron_crypto::hash::keccak256(&pub_key[1..]);
    let mut addr = [0u8; 21];
    addr[0] = 0x41;
    addr[1..].copy_from_slice(&h[12..]);
    (priv_key, addr)
}

fn build_state() -> StateBackends {
    StateBackends {
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
        code: Some(mem()),
        storage_row: Some(mem()),
        contract_state: Some(mem()),
        block_index: Some(mem()),
        witness_schedule: Some(mem()),
        reward_vi: None,
        account_asset: None,
    }
}

/// Stake 2.0 era with a non-zero global energy limit, so a staked account has
/// frozen energy to reserve. Applied identically to the executed state and to
/// the scratch store the expected values are derived from.
fn seed_dyn_props(dp: &DynamicPropertiesStore, resource_optimization: bool) {
    dp.save_total_energy_limit(100_000_000_000);
    dp.save_total_energy_current_limit(100_000_000_000);
    dp.save_total_energy_weight(2_000);
    dp.save_unfreeze_delay_days(14);
    dp.put_long(b"ALLOW_BLACKHOLE_OPTIMIZATION", 1);
    // Past ENERGY_LIMIT_HARD_FORK: the fix-ratio budget.
    dp.save_latest_block_header_number(60_000_000);
    dp.save_latest_block_header_timestamp(HEAD_TS);
    if resource_optimization {
        dp.put_long(b"ALLOW_TVM_RESOURCE_OPTIMIZATION", 1);
    }
}

/// 1,000 TRX staked for energy and a stale, non-zero usage stamped 1,000 slots
/// before the head, so the budget's decay and its reservation are both visible.
fn staked_account(addr: [u8; 21], now_slot: i64) -> Account {
    Account {
        address: addr.to_vec(),
        balance: 10_000_000_000,
        frozen_v2: vec![FreezeV2 { r#type: 1, amount: 1_000_000_000 }],
        account_resource: Some(AccountResource {
            energy_usage: STALE_USAGE,
            latest_consume_time_for_energy: now_slot - 1_000,
            ..Default::default()
        }),
        ..Default::default()
    }
}

struct Fixture {
    state: StateBackends,
    caller_priv: [u8; 32],
    caller: [u8; 21],
    origin: [u8; 21],
    contract: [u8; 21],
    now_slot: i64,
}

/// Caller and a distinct contract origin, both staked; the contract charges
/// the caller 50% so the origin reserves a share too. `bytecode` empty means a
/// contract row with no code.
fn fixture(resource_optimization: bool, bytecode: &[u8]) -> Fixture {
    let state = build_state();
    let dp = DynamicPropertiesStore::new(state.dyn_props.clone());
    seed_dyn_props(&dp, resource_optimization);
    let now_slot = dp.head_slot();

    let (caller_priv, caller) = caller_keypair(0xb4);
    let origin = addr_with_byte(0x0b);
    let contract = addr_with_byte(0xc4);
    let accounts = AccountStore::new(state.accounts.clone());
    accounts
        .put(&Address::from_raw(caller), &staked_account(caller, now_slot))
        .unwrap();
    accounts
        .put(&Address::from_raw(origin), &staked_account(origin, now_slot))
        .unwrap();
    accounts
        .put(
            &Address::from_raw(contract),
            &Account {
                address: contract.to_vec(),
                r#type: tron_proto::AccountType::Contract as i32,
                ..Default::default()
            },
        )
        .unwrap();
    if !bytecode.is_empty() {
        // java layout: runtime code keyed by the contract address.
        CodeStore::new(state.code.as_ref().unwrap().clone())
            .put(&contract, bytecode)
            .unwrap();
    }
    ContractStore::new(state.contracts.clone())
        .put(
            &Address::from_raw(contract),
            &SmartContract {
                origin_address: origin.to_vec(),
                contract_address: contract.to_vec(),
                consume_user_resource_percent: 50,
                origin_energy_limit: 10_000_000,
                ..Default::default()
            },
        )
        .unwrap();
    Fixture { state, caller_priv, caller, origin, contract, now_slot }
}

/// What the budget writes for this fixture: `(decayed, reserved)` usage of the
/// caller and of the origin — java's `receipt.get*EnergyUsage()` (post-decay)
/// and `get*EnergyMergedUsage()` (post-reservation).
fn expected_budget(resource_optimization: bool) -> ((i64, i64), (i64, i64)) {
    let dp = DynamicPropertiesStore::new(mem());
    seed_dyn_props(&dp, resource_optimization);
    let now_slot = dp.head_slot();
    let accounts = AccountStore::new(mem());
    let caller = Address::from_raw(addr_with_byte(0x01));
    let origin = Address::from_raw(addr_with_byte(0x02));
    let caller_acct = staked_account(addr_with_byte(0x01), now_slot);
    let origin_acct = staked_account(addr_with_byte(0x02), now_slot);
    accounts.put(&caller, &caller_acct).unwrap();
    accounts.put(&origin, &origin_acct).unwrap();
    let _ = vm_energy_budget_trigger(
        &accounts,
        &dp,
        &caller,
        &caller_acct,
        Some((&origin, &origin_acct)),
        50,
        10_000_000,
        FEE_LIMIT,
        0,
        now_slot,
    );
    let c = get_pre_tx_energy(&caller).expect("caller capture");
    let o = get_pre_tx_energy(&origin).expect("origin capture");
    ((c.usage, c.merged_usage), (o.usage, o.merged_usage))
}

/// Apply one block holding a single signed `TriggerSmartContract` under the
/// consensus budget rules (`require_fee_limit`), and return the caller's and
/// the origin's energy resource rows afterwards.
fn run(f: &Fixture) -> (AccountResource, AccountResource) {
    let trigger = TriggerSmartContract {
        owner_address: f.caller.to_vec(),
        contract_address: f.contract.to_vec(),
        ..Default::default()
    };
    let mut tx = Transaction {
        raw_data: Some(TxRaw {
            contract: vec![TxContract {
                r#type: ContractType::TriggerSmartContract as i32,
                parameter: Some(Any {
                    type_url: "type.googleapis.com/protocol.TriggerSmartContract".into(),
                    value: trigger.encode_to_vec(),
                }),
                ..Default::default()
            }],
            timestamp: HEAD_TS,
            fee_limit: FEE_LIMIT,
            ..Default::default()
        }),
        signature: Vec::new(),
        ret: Vec::new(),
        unparsed_field10: None,
    };
    tron_types::sign_transaction(&mut tx, &f.caller_priv).expect("sign tx");
    let txs = vec![tx];
    let block = Block {
        block_header: Some(BlockHeader {
            raw_data: Some(tron_proto::block_header::Raw {
                number: 60_000_001,
                parent_hash: vec![0u8; 32],
                timestamp: HEAD_TS + 3_000,
                tx_trie_root: tron_types::calc_tx_trie_root(&txs)
                    .map(|h| h.to_vec())
                    .unwrap_or_default(),
                ..Default::default()
            }),
            ..Default::default()
        }),
        transactions: txs,
    };
    let config = ExecConfig { require_fee_limit: true, ..ExecConfig::unsigned() };
    let report =
        execute_block_with_config(&f.state, &block, None, &config).expect("execute_block");
    let result = &report.tx_results[0];
    assert!(
        matches!(result.outcome, TxOutcome::Success),
        "trigger must succeed, got {:?}",
        result.outcome
    );
    assert_eq!(
        result.receipt.energy_usage_total, 0,
        "the fixture is a zero-energy success"
    );
    let accounts = AccountStore::new(f.state.accounts.clone());
    let res = |a: [u8; 21]| {
        accounts
            .get(&Address::from_raw(a))
            .unwrap()
            .unwrap()
            .account_resource
            .unwrap_or_default()
    };
    (res(f.caller), res(f.origin))
}

/// Contract code is a single `STOP`: the budget reserves, the VM succeeds on
/// zero energy. Before #104 java never stores the reset, so the reservation
/// stays on both accounts.
#[test]
fn zero_energy_success_keeps_the_reservation_before_proposal_104() {
    let f = fixture(false, &[0x00]);
    let ((caller_decayed, caller_reserved), (origin_decayed, origin_reserved)) =
        expected_budget(false);
    assert!(caller_decayed < STALE_USAGE && caller_reserved > caller_decayed);
    assert!(origin_decayed < STALE_USAGE && origin_reserved > origin_decayed);

    let (caller, origin) = run(&f);
    assert_eq!(caller.energy_usage, caller_reserved, "caller keeps the reserved usage");
    assert_eq!(origin.energy_usage, origin_reserved, "origin keeps the reserved usage");
    assert_eq!(caller.latest_consume_time_for_energy, f.now_slot);
    assert_eq!(origin.latest_consume_time_for_energy, f.now_slot);
}

/// Same transaction with #104 active: `TransactionTrace.pay` stores the reset
/// accounts, so both end on the post-decay usage.
#[test]
fn zero_energy_success_stores_the_reset_with_proposal_104() {
    let f = fixture(true, &[0x00]);
    let ((caller_decayed, caller_reserved), (origin_decayed, origin_reserved)) =
        expected_budget(true);
    assert!(caller_reserved > caller_decayed && origin_reserved > origin_decayed);

    let (caller, origin) = run(&f);
    assert_eq!(caller.energy_usage, caller_decayed, "caller reservation is given back");
    assert_eq!(origin.energy_usage, origin_decayed, "origin reservation is given back");
    assert_eq!(caller.latest_consume_time_for_energy, f.now_slot);
    assert_eq!(origin.latest_consume_time_for_energy, f.now_slot);
}

/// A contract with no code: `VMActuator.call` computes no budget, so nothing
/// is reserved, and before #104 nothing is stored either — the energy fields
/// of both accounts are exactly as they were.
#[test]
fn codeless_trigger_leaves_energy_rows_untouched_before_proposal_104() {
    let f = fixture(false, &[]);
    let (caller, origin) = run(&f);
    for (who, res) in [("caller", caller), ("origin", origin)] {
        assert_eq!(res.energy_usage, STALE_USAGE, "{who}: usage is not decayed");
        assert_eq!(
            res.latest_consume_time_for_energy,
            f.now_slot - 1_000,
            "{who}: consume time is not stamped"
        );
        assert_eq!(res.energy_window_size, 0, "{who}: window is not rewritten");
    }
}

/// With #104 the same codeless trigger stores both accounts after a
/// `resetAccountUsage` whose capture is all zero: usage and consume time are
/// unchanged, but the window is rewritten from its implicit default (0) to the
/// explicit `getWindowSize()` (28,800 slots) because the usage is non-zero.
#[test]
fn codeless_trigger_stores_the_zero_capture_reset_with_proposal_104() {
    let f = fixture(true, &[]);
    let (caller, origin) = run(&f);
    for (who, res) in [("caller", caller), ("origin", origin)] {
        assert_eq!(res.energy_usage, STALE_USAGE, "{who}: usage is not decayed");
        assert_eq!(
            res.latest_consume_time_for_energy,
            f.now_slot - 1_000,
            "{who}: consume time is not stamped"
        );
        assert_eq!(res.energy_window_size, 28_800, "{who}: window made explicit");
        assert!(!res.energy_window_optimized, "{who}: v1 window (no cancel-all-unfreeze)");
    }
}
