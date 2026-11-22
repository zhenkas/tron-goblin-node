//! Native voting must execute in disposable RPC state, including through a proxy.
use serde_json::json;
use std::sync::Arc;
use tron_chainbase::{
    AccountStore, CodeStore, DelegationStore, DynamicPropertiesStore, KvBackend, MemBackend,
    VotesStore, WitnessStore,
};
use tron_crypto::address::Address;
use tron_proto::{account::Frozen, Account, TriggerSmartContract, Vote, Votes, Witness};
use tron_rpc::{methods, EthCallBackends, RpcState};
use tron_tvm::{
    database::code_hash,
    execute::{VmBlockEnv, VmOutcome},
};

fn mem() -> Arc<dyn KvBackend> {
    Arc::new(MemBackend::new())
}
fn addr(byte: u8) -> [u8; 21] {
    let mut a = [byte; 21];
    a[0] = 0x41;
    a
}
fn word(n: u64) -> [u8; 32] {
    let mut w = [0; 32];
    w[24..].copy_from_slice(&n.to_be_bytes());
    w
}
fn push(code: &mut Vec<u8>, w: [u8; 32]) {
    code.push(0x7f);
    code.extend(w);
}
fn arguments(sr: [u8; 21], count: u64) -> Vec<u8> {
    let mut address = [0; 32];
    address[12..].copy_from_slice(&sr[1..]);
    [word(64), word(128), word(1), address, word(1), word(count)].concat()
}

// Copy canonical ABI arguments to memory, VOTEWITNESS, store its flag, and
// require success just like solc's native vote builtin. A post-vote revert
// exercises unwinding after account/reward/vote writes have actually occurred.
fn voter_code(revert_after_vote: bool) -> Vec<u8> {
    let mut c = vec![0x60, 192, 0x60, 4, 0x60, 0, 0x37];
    for n in [64, 1, 128, 1] {
        push(&mut c, word(n));
    }
    c.extend([0xd8, 0x80, 0x60, 0, 0x55, 0x80, 0x60, 0, 0x52]);
    c.push(0x61);
    let destination = c.len();
    c.extend([0, 0, 0x57]);
    c.extend([0x60, 0, 0x60, 0, 0xfd]);
    let success = c.len() as u16;
    c[destination..destination + 2].copy_from_slice(&success.to_be_bytes());
    c.push(0x5b);
    if revert_after_vote {
        c.extend([0x60, 0, 0x60, 0, 0xfd]);
    } else {
        c.extend([0x60, 32, 0x60, 0, 0xf3]);
    }
    c
}

// Same caller/storage/return/revert semantics as the pool's forwarding tail.
fn proxy_code(implementation: [u8; 21]) -> Vec<u8> {
    let mut c = vec![
        0x36, 0x60, 0, 0x60, 0, 0x37, 0x60, 0, 0x60, 0, 0x36, 0x60, 0, 0x73,
    ];
    c.extend_from_slice(&implementation[1..]);
    c.extend([0x5a, 0xf4, 0x3d, 0x60, 0, 0x60, 0, 0x3e]);
    c.push(0x61);
    let destination = c.len();
    c.extend([0, 0, 0x57]);
    c.extend([0x3d, 0x60, 0, 0xfd]);
    let success = c.len() as u16;
    c[destination..destination + 2].copy_from_slice(&success.to_be_bytes());
    c.extend([0x5b, 0x3d, 0x60, 0, 0xf3]);
    c
}

fn install(b: &EthCallBackends, address: [u8; 21], code: Vec<u8>, power: i64, prior: Vec<Vote>) {
    let hash = code_hash(&code);
    CodeStore::new(b.code.clone())
        .put(hash.as_slice(), &code)
        .unwrap();
    AccountStore::new(b.accounts.clone())
        .put(
            &Address::from_raw(address),
            &Account {
                address: address.to_vec(),
                balance: 1_000_000_000,
                code,
                code_hash: hash.as_slice().to_vec(),
                frozen: vec![Frozen {
                    frozen_balance: power * 1_000_000,
                    expire_time: 0,
                }],
                votes: prior,
                ..Default::default()
            },
        )
        .unwrap();
}
struct Fixture {
    state: RpcState,
    b: EthCallBackends,
    caller: [u8; 21],
    pool: [u8; 21],
    sr: [u8; 21],
}
fn fixture(revert_after_vote: bool, power: i64) -> Fixture {
    let b = EthCallBackends {
        accounts: mem(),
        code: mem(),
        storage: mem(),
        witnesses: mem(),
        votes: mem(),
        contract_state: mem(),
        dyn_props: mem(),
        delegated_resources: mem(),
        delegation: mem(),
        contracts: mem(),
        block_index: Some(mem()),
    };
    let (caller, pool, implementation, sr) = (addr(0xa1), addr(0xc1), addr(0xc2), addr(0x11));
    let prior = vec![Vote {
        vote_address: sr.to_vec(),
        vote_count: 7,
    }];
    install(&b, caller, vec![], 0, vec![]);
    install(&b, implementation, voter_code(revert_after_vote), 0, vec![]);
    install(&b, pool, proxy_code(implementation), power, prior.clone());
    VotesStore::new(b.votes.clone())
        .put(
            &Address::from_raw(pool),
            &Votes {
                address: pool.to_vec(),
                old_votes: prior.clone(),
                new_votes: prior,
            },
        )
        .unwrap();
    WitnessStore::new(b.witnesses.clone())
        .put(
            &Address::from_raw(sr),
            &Witness {
                address: sr.to_vec(),
                ..Default::default()
            },
        )
        .unwrap();
    let dp = DynamicPropertiesStore::new(b.dyn_props.clone());
    dp.put_long(b"ALLOW_TVM_VOTE", 1);
    dp.put_long(b"UNFREEZE_DELAY_DAYS", 14);
    dp.put_long(b"ALLOW_CHANGE_DELEGATION", 1);
    dp.save_current_cycle_number(2);
    dp.save_latest_block_header_timestamp(1_700_000_000_000);
    let delegation = DelegationStore::new(b.delegation.clone());
    delegation.set_begin_cycle(&Address::from_raw(pool), 1);
    delegation.set_end_cycle(&Address::from_raw(pool), 1);
    delegation.set_witness_vote(1, &Address::from_raw(sr), 7);
    delegation.add_reward(1, &Address::from_raw(sr), 14_000_000);
    let state = RpcState::new(
        b.accounts.clone(),
        mem(),
        mem(),
        mem(),
        b.dyn_props.clone(),
        11_111,
    )
    .with_support_constant(true)
    .with_eth_call_backends(b.clone());
    Fixture {
        state,
        b,
        caller,
        pool,
        sr,
    }
}
type Snapshot = Vec<Vec<(Vec<u8>, Vec<u8>)>>;
fn snapshot(b: &EthCallBackends) -> Snapshot {
    [
        &b.accounts,
        &b.code,
        &b.storage,
        &b.witnesses,
        &b.votes,
        &b.contract_state,
        &b.dyn_props,
        &b.delegated_resources,
        &b.delegation,
        &b.contracts,
        b.block_index.as_ref().unwrap(),
    ]
    .iter()
    .map(|be| be.scan_all().unwrap())
    .collect()
}
fn rpc(f: &Fixture, sr: [u8; 21], count: u64) -> serde_json::Value {
    methods::trigger_constant_contract(
        &json!([{
            "owner_address": format!("0x{}", hex::encode(f.caller)),
            "contract_address": format!("0x{}", hex::encode(f.pool)),
            "function_selector": "voteForSrs(address[],uint256[])",
            "parameter": hex::encode(arguments(sr, count)),
        }]),
        &f.state,
    )
    .unwrap()
}
#[test]
fn proxy_constant_vote_succeeds_and_discards_all_state_changes() {
    let f = fixture(false, 200_000);
    let before = snapshot(&f.b);
    let v = rpc(&f, f.sr, 200_000);
    assert_eq!(v["result"]["result"], true, "{v}");
    assert_eq!(v["constant_result"][0], hex::encode(word(1)));
    assert_eq!(
        snapshot(&f.b),
        before,
        "RPC success must never commit an overlay"
    );
    // Inspect the same disposable VM layer to prove successful voting did real
    // work in accounts, votes, rewards and storage, rather than faking success.
    let vm = methods::build_call_vm_stores(&f.b);
    let trigger = TriggerSmartContract {
        owner_address: f.caller.to_vec(),
        contract_address: f.pool.to_vec(),
        data: [hex::decode("87382cbc").unwrap(), arguments(f.sr, 200_000)].concat(),
        ..Default::default()
    };
    let (outcome, _) = methods::dispatch_constant_trigger(
        &f.state,
        &vm,
        VmBlockEnv {
            block_number: 1,
            block_timestamp_ms: 1_700_000_000_000,
            ..Default::default()
        },
        &trigger,
        2_000_000,
    );
    assert!(matches!(outcome, VmOutcome::Success { .. }), "{outcome:?}");
    let account = vm
        .accounts
        .get(&Address::from_raw(f.pool))
        .unwrap()
        .unwrap();
    assert_eq!(account.votes[0].vote_count, 200_000);
    assert_eq!(
        account.allowance, 14_000_000,
        "reward settle must remain simulated"
    );
    assert_eq!(
        vm.votes
            .as_ref()
            .unwrap()
            .get(&Address::from_raw(f.pool))
            .unwrap()
            .unwrap()
            .new_votes[0]
            .vote_count,
        200_000
    );
    assert_eq!(vm.delegation.get_begin_cycle(&Address::from_raw(f.pool)), 2);
    assert_eq!(
        snapshot(&f.b),
        before,
        "even a live session must not write the base"
    );
    drop(vm);
    assert_eq!(snapshot(&f.b), before);
}
#[test]
fn proxy_constant_vote_rejects_unregistered_witness_without_writes() {
    let f = fixture(false, 200_000);
    let before = snapshot(&f.b);
    let v = rpc(&f, addr(0xee), 200_000);
    assert_eq!(v["result"]["result"], false, "{v}");
    assert_eq!(v["result"]["code"], "CONTRACT_EXE_ERROR");
    assert_eq!(snapshot(&f.b), before);
}
#[test]
fn proxy_constant_vote_rejects_insufficient_power_without_writes() {
    let f = fixture(false, 199_999);
    let before = snapshot(&f.b);
    let v = rpc(&f, f.sr, 200_000);
    assert_eq!(v["result"]["result"], false, "{v}");
    assert_eq!(snapshot(&f.b), before);
}
#[test]
fn proxy_constant_vote_followed_by_revert_discards_all_state_changes() {
    let f = fixture(true, 200_000);
    let before = snapshot(&f.b);
    let v = rpc(&f, f.sr, 200_000);
    assert_eq!(v["result"]["result"], false, "{v}");
    assert_eq!(v["result"]["message"], "REVERT opcode executed");
    assert_eq!(
        snapshot(&f.b),
        before,
        "post-vote revert must not escape any store"
    );
}
