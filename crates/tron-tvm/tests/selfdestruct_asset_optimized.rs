//! SELFDESTRUCT's TRC-10 sweep between asset-optimized accounts, whose
//! balances live in the account-asset store instead of the account row.
//!
//! Mainnet block 87,672,512 (tx 24842c13…): contract `X` self-destructed to
//! its owner while both were asset-optimized. java-tron ended with the owner
//! holding 444,444 of token 1005074 and the contract holding none; we credited
//! the owner from zero (222,222) and left the contract's store rows readable.
use std::sync::Arc;

use tron_chainbase::{
    import_all_asset, set_account_asset_backend, AccountAssetStore, AccountStore, CodeStore,
    ContractStateStore, DelegatedResourceStore, DelegationStore, DynamicPropertiesStore, KvBackend,
    MemBackend, StorageRowStore, VotesStore, WitnessStore,
};
use tron_crypto::address::Address;
use tron_proto::{Account, TriggerSmartContract};
use tron_tvm::database::code_hash;
use tron_tvm::execute::{execute_trigger, VmBlockEnv, VmOutcome, VmStores};

const NOW_MS: i64 = 1_700_000_000_000;
const TOKEN: &str = "1005074";

fn mem() -> Arc<dyn KvBackend> {
    Arc::new(MemBackend::new())
}

fn stores() -> VmStores {
    let dp = Arc::new(DynamicPropertiesStore::new(mem()));
    for k in [
        "ALLOW_TVM_TRANSFER_TRC10",
        "ALLOW_TVM_CONSTANTINOPLE",
        "ALLOW_TVM_SOLIDITY_059",
        "ALLOW_TVM_ISTANBUL",
        "ALLOW_TVM_FREEZE",
        "ALLOW_TVM_VOTE",
        "ALLOW_TVM_LONDON",
        "ALLOW_TVM_SHANGHAI",
        "ALLOW_TVM_CANCUN",
        "ALLOW_ENERGY_ADJUSTMENT",
        "ALLOW_TVM_SELFDESTRUCT_RESTRICTION",
        "ALLOW_NEW_RESOURCE_MODEL",
        "ALLOW_DELEGATE_RESOURCE",
    ] {
        dp.put_long(k.as_bytes(), 1);
    }
    dp.put_long(b"UNFREEZE_DELAY_DAYS", 14);
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
        contracts: None,
        votes: Some(Arc::new(VotesStore::new(mem()))),
        reward_vi: None,
        abi: None,
    }
}

fn tron_addr(byte: u8) -> [u8; 21] {
    let mut a = [0u8; 21];
    a[0] = 0x41;
    a[1..].fill(byte);
    a
}

fn effective_balance(stores: &VmStores, addr: [u8; 21]) -> i64 {
    let mut account = stores.accounts.get(&Address::from_raw(addr)).unwrap().unwrap();
    import_all_asset(&mut account);
    account.asset_v2.get(TOKEN).copied().unwrap_or(0)
}

#[test]
fn sweep_adds_to_the_inheritors_store_balance_and_leaves_no_ghost_on_the_owner() {
    let stores = stores();
    // The process-wide store both optimized accounts keep their balances in.
    let asset_backend = mem();
    set_account_asset_backend(asset_backend.clone());
    let assets = AccountAssetStore::new(asset_backend);

    let contract = tron_addr(0xd5);
    let owner = tron_addr(0xac);
    let caller = tron_addr(0x01);
    assets.put(&Address::from_raw(contract), TOKEN.as_bytes(), 222_222).unwrap();
    assets.put(&Address::from_raw(owner), TOKEN.as_bytes(), 222_222).unwrap();

    // PUSH20 owner ; SELFDESTRUCT
    let mut bytecode = vec![0x73];
    bytecode.extend_from_slice(&owner[1..]);
    bytecode.push(0xff);
    let hash = code_hash(&bytecode);
    stores.code.put(hash.as_slice(), &bytecode).unwrap();
    stores
        .accounts
        .put(
            &Address::from_raw(contract),
            &Account {
                address: contract.to_vec(),
                r#type: tron_proto::AccountType::Contract as i32,
                code: bytecode,
                code_hash: hash.as_slice().to_vec(),
                asset_optimized: true,
                ..Default::default()
            },
        )
        .unwrap();
    stores
        .accounts
        .put(
            &Address::from_raw(owner),
            &Account { address: owner.to_vec(), balance: 1, asset_optimized: true, ..Default::default() },
        )
        .unwrap();
    stores
        .accounts
        .put(
            &Address::from_raw(caller),
            &Account { address: caller.to_vec(), balance: 1_000_000_000, ..Default::default() },
        )
        .unwrap();
    assert_eq!(effective_balance(&stores, owner), 222_222);
    assert_eq!(effective_balance(&stores, contract), 222_222);

    let out = execute_trigger(
        &stores,
        VmBlockEnv { block_number: 1, block_timestamp_ms: NOW_MS, ..Default::default() },
        &TriggerSmartContract {
            owner_address: caller.to_vec(),
            contract_address: contract.to_vec(),
            call_value: 0,
            data: vec![],
            call_token_value: 0,
            token_id: 0,
        },
        1_000_000,
    );
    assert!(matches!(out, VmOutcome::Success { .. }), "expected Success, got {out:?}");

    assert_eq!(
        effective_balance(&stores, owner),
        444_444,
        "the swept tokens add to the balance the inheritor already holds in the store"
    );
    if stores.accounts.get(&Address::from_raw(contract)).unwrap().is_some() {
        assert_eq!(
            effective_balance(&stores, contract),
            0,
            "a surviving owner must not read its swept store rows as a balance"
        );
    }
}
