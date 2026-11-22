//! Nested CREATE whose constructor REVERTs with data starting with 0xEF,
//! under `ALLOW_TVM_LONDON` — java `Program.createContractImpl`:
//!
//! ```java
//! byte[] code = createResult.getHReturn();
//! if (code.length != 0 && VMConfig.allowTvmLondon() && code[0] == (byte) 0xEF) {
//!   if (!createResult.isRevert() || !VMConfig.allowTvmResourceOptimization()) {
//!     createResult.setException(Program.Exception.invalidCodeException());
//!   }
//! }
//! ```
//!
//! Before `ALLOW_TVM_RESOURCE_OPTIMIZATION` (#104) the check ignores that the
//! "code" is really revert data: the constructor's result gets an exception,
//! so the creating frame takes the early `return` — no `refundEnergyAfterVM`
//! (the whole budget forwarded to the constructor is forfeited) and no
//! `returnDataBuffer`. With #104 a reverting constructor is an ordinary
//! revert: unused energy comes back and the revert data is visible to the
//! creator.

use std::sync::Arc;

use tron_chainbase::{
    AccountStore, CodeStore, ContractStateStore, ContractStore, DelegatedResourceStore,
    DelegationStore, DynamicPropertiesStore, KvBackend, MemBackend, StorageRowStore, VotesStore,
    WitnessStore,
};
use tron_crypto::address::Address;
use tron_proto::transaction::result::ContractResult;
use tron_proto::{Account, SmartContract, TriggerSmartContract};
use tron_tvm::database::code_hash;
use tron_tvm::execute::{execute_trigger, VmBlockEnv, VmOutcome, VmStores};

const FACTORY: [u8; 21] = [0x41, 0xfa, 0xc7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
const CALLER: [u8; 21] = [0x41, 0xca, 0x11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2];
const LIMIT: u64 = 1_000_000;

fn mem() -> Arc<dyn KvBackend> {
    Arc::new(MemBackend::new())
}

/// Constructor: `MSTORE8(0, first_byte); REVERT(0, 1)`.
fn reverting_init_code(first_byte: u8) -> [u8; 10] {
    [0x60, first_byte, 0x60, 0x00, 0x53, 0x60, 0x01, 0x60, 0x00, 0xfd]
}

/// Factory: CREATE the constructor above, then return two words — the CREATE
/// result and RETURNDATASIZE as the creator sees them.
fn factory_code(first_byte: u8) -> Vec<u8> {
    let mut code = vec![0x69]; // PUSH10 <init code>
    code.extend_from_slice(&reverting_init_code(first_byte));
    code.extend_from_slice(&[
        0x60, 0x00, 0x52, // MSTORE(0, init)  -> init code at memory[22..32]
        0x60, 0x0a, 0x60, 0x16, 0x60, 0x00, 0xf0, // CREATE(value 0, offset 22, size 10)
        0x60, 0x00, 0x52, // MSTORE(0, create result)
        0x3d, 0x60, 0x20, 0x52, // MSTORE(32, RETURNDATASIZE)
        0x60, 0x40, 0x60, 0x00, 0xf3, // RETURN(0, 64)
    ]);
    code
}

fn stores(london: bool, resource_optimization: bool) -> VmStores {
    let dp = Arc::new(DynamicPropertiesStore::new(mem()));
    for k in [
        "ALLOW_TVM_TRANSFER_TRC10",
        "ALLOW_TVM_CONSTANTINOPLE",
        "ALLOW_TVM_SOLIDITY_059",
        "ALLOW_TVM_ISTANBUL",
        "ALLOW_ENERGY_ADJUSTMENT",
        "ALLOW_HIGHER_LIMIT_FOR_MAX_CPU_TIME_OF_ONE_TX",
        "ALLOW_MULTI_SIGN",
    ] {
        dp.put_long(k.as_bytes(), 1);
    }
    if london {
        dp.put_long(b"ALLOW_TVM_LONDON", 1);
    }
    if resource_optimization {
        dp.put_long(b"ALLOW_TVM_RESOURCE_OPTIMIZATION", 1);
    }
    dp.put_long(b"ENERGY_FEE", 100);
    dp.save_latest_block_header_timestamp(1_785_253_899_000);
    let contracts = Arc::new(ContractStore::new(mem()));
    contracts
        .put(
            &Address::from_raw(FACTORY),
            &SmartContract {
                contract_address: FACTORY.to_vec(),
                origin_address: CALLER.to_vec(),
                consume_user_resource_percent: 100,
                ..Default::default()
            },
        )
        .unwrap();
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
        contracts: Some(contracts),
        votes: Some(Arc::new(VotesStore::new(mem()))),
        reward_vi: None,
        abi: None,
    }
}

fn run(london: bool, resource_optimization: bool, first_byte: u8) -> VmOutcome {
    let stores = stores(london, resource_optimization);
    let code = factory_code(first_byte);
    let hash = code_hash(&code);
    stores.code.put(hash.as_slice(), &code).unwrap();
    stores
        .accounts
        .put(
            &Address::from_raw(FACTORY),
            &Account {
                address: FACTORY.to_vec(),
                code: code.clone(),
                code_hash: hash.as_slice().to_vec(),
                r#type: tron_proto::AccountType::Contract as i32,
                ..Default::default()
            },
        )
        .unwrap();
    stores
        .accounts
        .put(
            &Address::from_raw(CALLER),
            &Account { address: CALLER.to_vec(), balance: 1_000_000_000, ..Default::default() },
        )
        .unwrap();
    execute_trigger(
        &stores,
        VmBlockEnv { block_number: 90_000_000, block_timestamp_ms: 1_785_253_902_000, beneficiary: [0u8; 20] },
        &TriggerSmartContract {
            owner_address: CALLER.to_vec(),
            contract_address: FACTORY.to_vec(),
            ..Default::default()
        },
        LIMIT,
    )
}

/// The creator saw an ordinary constructor revert: CREATE pushed 0, the one
/// byte of revert data is in the return buffer, and only the energy actually
/// used was charged.
fn assert_plain_revert_seen(out: VmOutcome) -> u64 {
    match out {
        VmOutcome::Success { return_data, energy_used, .. } => {
            assert_eq!(return_data.len(), 64);
            assert!(return_data[..32].iter().all(|b| *b == 0), "CREATE pushed 0");
            assert_eq!(return_data[63], 1, "RETURNDATASIZE == 1");
            assert!(return_data[32..63].iter().all(|b| *b == 0));
            assert!(
                energy_used < LIMIT / 10,
                "unused constructor energy must be refunded, used {energy_used}"
            );
            energy_used
        }
        other => panic!("expected the factory to finish, got {other:?}"),
    }
}

/// Before #104: the 0xEF revert data is treated as invalid code. The factory
/// forwarded its whole remaining budget to the constructor and gets none of
/// it back, so its next opcode runs out of energy — the transaction ends
/// OUT_OF_ENERGY with the full limit charged.
#[test]
fn london_without_proposal_104_turns_the_ef_revert_into_invalid_code() {
    match run(true, false, 0xEF) {
        VmOutcome::Halt { result, energy_used, .. } => {
            assert_eq!(result, ContractResult::OutOfEnergy);
            assert_eq!(energy_used, LIMIT, "the forwarded budget is forfeited");
        }
        other => panic!("expected the creator to run out of energy, got {other:?}"),
    }
}

/// With #104 the same constructor revert stays a revert.
#[test]
fn london_with_proposal_104_keeps_the_ef_revert_a_revert() {
    let with_ef = assert_plain_revert_seen(run(true, true, 0xEF));
    // ...and costs exactly what any other one-byte revert costs.
    let without_ef = assert_plain_revert_seen(run(true, true, 0xEE));
    assert_eq!(with_ef, without_ef);
}

/// The rule is about the first byte only: other revert data is a plain revert
/// in both flag states.
#[test]
fn non_ef_revert_data_is_a_plain_revert_in_both_flag_states() {
    let off = assert_plain_revert_seen(run(true, false, 0xEE));
    let on = assert_plain_revert_seen(run(true, true, 0xEE));
    assert_eq!(off, on);
}

/// ...and about `ALLOW_TVM_LONDON`: before London there is no 0xEF check at
/// all, so #104 has nothing to change.
#[test]
fn before_london_the_ef_revert_is_a_plain_revert_in_both_flag_states() {
    let off = assert_plain_revert_seen(run(false, false, 0xEF));
    let on = assert_plain_revert_seen(run(false, true, 0xEF));
    assert_eq!(off, on);
}
