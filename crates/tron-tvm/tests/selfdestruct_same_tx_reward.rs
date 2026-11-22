//! SELFDESTRUCT of a contract created in the same transaction still settles
//! its (empty) reward position.
//!
//! java's `Program.suicide` calls `VoteRewardUtil.withdrawReward` against the
//! in-flight `Repository`, which already holds the new contract account. With
//! no votes that leaves `begin_cycle = current + 1` in the delegation store,
//! and the row outlives the contract. The committed account store has no row
//! for such a contract, so we used to skip the settle: at mainnet height
//! 88,067,120 about 26,000 of these rows (one-shot deposit forwarders, e.g.
//! tx eb2bb031… in block 87,705,667) were missing from our state.
use std::sync::Arc;

use tron_chainbase::{
    AbiStore, AccountStore, CodeStore, ContractStateStore, ContractStore, DelegatedResourceStore,
    DelegationStore, DynamicPropertiesStore, KvBackend, MemBackend, StorageRowStore, VotesStore,
    WitnessStore,
};
use tron_crypto::address::Address;
use tron_proto::{Account, TriggerSmartContract};
use tron_tvm::database::code_hash;
use tron_tvm::execute::{execute_trigger, VmBlockEnv, VmOutcome, VmStores};

const NOW_MS: i64 = 1_700_000_000_000;
const CYCLE: i64 = 10_244;

fn mem() -> Arc<dyn KvBackend> {
    Arc::new(MemBackend::new())
}

fn stores(allow_vote: bool) -> VmStores {
    let dp = Arc::new(DynamicPropertiesStore::new(mem()));
    for k in ["ALLOW_TVM_CONSTANTINOPLE", "ALLOW_TVM_SOLIDITY_059", "ALLOW_TVM_ISTANBUL", "ALLOW_TVM_FREEZE"] {
        dp.put_long(k.as_bytes(), 1);
    }
    dp.put_long(b"ALLOW_TVM_VOTE", i64::from(allow_vote));
    dp.put_long(b"CURRENT_CYCLE_NUMBER", CYCLE);
    dp.save_latest_block_header_timestamp(NOW_MS);
    VmStores {
        accounts: Arc::new(AccountStore::new(mem())),
        code: Arc::new(CodeStore::new(mem())),
        storage: Arc::new(StorageRowStore::new(mem())),
        witnesses: Arc::new(WitnessStore::new(mem())),
        contract_state: Arc::new(ContractStateStore::new(mem())),
        dynamic_properties: dp,
        delegated_resources: Arc::new(DelegatedResourceStore::new(mem())),
        delegated_resource_account_index: None,
        delegation: Arc::new(DelegationStore::new(mem())),
        block_index: None,
        contracts: Some(Arc::new(ContractStore::new(mem()))),
        votes: Some(Arc::new(VotesStore::new(mem()))),
        reward_vi: None,
        abi: Some(Arc::new(AbiStore::new(mem()))),
    }
}

fn tron_addr(byte: u8) -> [u8; 21] {
    let mut a = [0u8; 21];
    a[0] = 0x41;
    a[1..].fill(byte);
    a
}

/// Runs a factory that CREATEs a child whose constructor is `CALLER
/// SELFDESTRUCT`, records the child address in slot 0, and returns it.
fn create_and_destroy(stores: &VmStores) -> Address {
    let (caller, factory) = (tron_addr(0x11), tron_addr(0x22));
    stores
        .accounts
        .put(&Address::from_raw(caller), &Account { address: caller.to_vec(), balance: 1_000_000_000, ..Default::default() })
        .unwrap();
    // PUSH2 0x33ff PUSH1 0 MSTORE | PUSH1 2 PUSH1 30 PUSH1 0 CREATE | PUSH1 0 SSTORE STOP
    let code = hex::decode("6133ff6000526002601e6000f060005500").unwrap();
    let hash = code_hash(&code);
    stores.code.put(hash.as_slice(), &code).unwrap();
    stores.code.put(&factory, &code).unwrap();
    stores
        .accounts
        .put(
            &Address::from_raw(factory),
            &Account {
                address: factory.to_vec(),
                code: code.clone(),
                code_hash: hash.as_slice().to_vec(),
                r#type: tron_proto::AccountType::Contract as i32,
                ..Default::default()
            },
        )
        .unwrap();
    let outcome = execute_trigger(
        stores,
        VmBlockEnv { block_number: 1, block_timestamp_ms: NOW_MS, ..Default::default() },
        &TriggerSmartContract {
            owner_address: caller.to_vec(),
            contract_address: factory.to_vec(),
            ..Default::default()
        },
        1_000_000,
    );
    assert!(matches!(outcome, VmOutcome::Success { .. }), "factory call failed: {outcome:?}");
    let slot = StorageRowStore::compose_key(&Address::from_raw(factory), &[0u8; 32]);
    let word = stores.storage.get(&slot).unwrap().expect("factory recorded no child");
    assert!(word[12..].iter().any(|b| *b != 0), "CREATE returned zero");
    let mut child = [0x41u8; 21];
    child[1..].copy_from_slice(&word[12..]);
    let child = Address::from_raw(child);
    assert!(stores.accounts.get(&child).unwrap().is_none(), "child survived its selfdestruct");
    child
}

#[test]
fn destroying_a_same_tx_contract_advances_its_begin_cycle() {
    let stores = stores(true);
    let child = create_and_destroy(&stores);
    let row = stores.delegation.get_raw(&DelegationStore::begin_cycle_key(&child)).unwrap();
    assert_eq!(row, Some((CYCLE + 1).to_be_bytes().to_vec()));
}

#[test]
fn no_row_is_written_before_tvm_vote() {
    let stores = stores(false);
    let child = create_and_destroy(&stores);
    assert_eq!(stores.delegation.get_raw(&DelegationStore::begin_cycle_key(&child)).unwrap(), None);
}
