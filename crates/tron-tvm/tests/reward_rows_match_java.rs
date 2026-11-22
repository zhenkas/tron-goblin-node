//! The delegation rows the reward code leaves behind, byte for byte as
//! java-tron's `MortgageService` writes them.
//!
//! * `payReward` calls `delegationStore.addReward(cycle, witness, value)`
//!   unconditionally: a witness whose voters get nothing this block (the
//!   witness keeps 100 %, or its standby share rounds to 0) still gets the
//!   cycle's reward row, holding 0.
//! * `withdrawReward` snapshots the WHOLE account as read on entry
//!   (`setAccountVote(endCycle, address, accountCapsule)`), not only its
//!   votes, and before the reward is added to its allowance.

use std::sync::Arc;

use tron_chainbase::{AccountStore, DelegationStore, DynamicPropertiesStore, KvBackend, MemBackend};
use tron_crypto::address::Address;
use tron_proto::{Account, Vote};
use tron_tvm::reward::{pay_block_reward, pay_standby_witness, withdraw_reward};

fn mem() -> Arc<dyn KvBackend> {
    Arc::new(MemBackend::new())
}

fn addr(byte: u8) -> Address {
    let mut a = [0u8; 21];
    a[0] = 0x41;
    a[1..].fill(byte);
    Address::from_raw(a)
}

struct Env {
    accounts: AccountStore,
    delegation: DelegationStore,
    dyn_props: DynamicPropertiesStore,
}

const CYCLE: i64 = 7;

fn env() -> Env {
    let env = Env {
        accounts: AccountStore::new(mem()),
        delegation: DelegationStore::new(mem()),
        dyn_props: DynamicPropertiesStore::new(mem()),
    };
    env.dyn_props.save_current_cycle_number(CYCLE);
    env
}

fn reward_row(env: &Env, witness: &Address) -> Option<Vec<u8>> {
    env.delegation.get_raw(&DelegationStore::reward_key(CYCLE, witness)).unwrap()
}

#[test]
fn a_witness_keeping_the_whole_reward_still_gets_a_zero_reward_row() {
    let env = env();
    let witness = addr(0x11);
    env.accounts
        .put(&witness, &Account { address: witness.as_bytes().to_vec(), ..Default::default() })
        .unwrap();
    env.delegation.set_brokerage(CYCLE, &witness, 100);

    pay_block_reward(&env.accounts, &env.delegation, &env.dyn_props, &witness, 16_000_000).unwrap();

    assert_eq!(reward_row(&env, &witness), Some(0i64.to_be_bytes().to_vec()));
    assert_eq!(env.accounts.get(&witness).unwrap().unwrap().allowance, 16_000_000);
}

#[test]
fn a_zero_block_reward_leaves_a_zero_row_and_no_account_write() {
    let env = env();
    let witness = addr(0x12);
    pay_block_reward(&env.accounts, &env.delegation, &env.dyn_props, &witness, 0).unwrap();
    assert_eq!(reward_row(&env, &witness), Some(0i64.to_be_bytes().to_vec()));
    // `adjustAllowance` returns early for amount <= 0: no account appears.
    assert!(env.accounts.get(&witness).unwrap().is_none());
}

#[test]
fn a_standby_share_that_rounds_to_zero_still_leaves_its_row() {
    let env = env();
    env.dyn_props.save_witness_127_pay_per_block(10);
    let (big, tiny) = (addr(0x21), addr(0x22));
    env.delegation.set_brokerage(CYCLE, &big, 0);
    env.delegation.set_brokerage(CYCLE, &tiny, 0);
    // tiny's share: 1 * (10 / 1_000_001) truncates to 0.
    pay_standby_witness(
        &env.accounts,
        &env.delegation,
        &env.dyn_props,
        &[(big, 1_000_000), (tiny, 1)],
    )
    .unwrap();
    assert_eq!(reward_row(&env, &big), Some(9i64.to_be_bytes().to_vec()));
    assert_eq!(reward_row(&env, &tiny), Some(0i64.to_be_bytes().to_vec()));
}

#[test]
fn withdraw_snapshots_the_whole_account_as_read_on_entry() {
    let env = env();
    let (voter, witness) = (addr(0x31), addr(0x32));
    // A cycle the voter is owed for: 1000 sun for 10 votes out of 10.
    env.delegation.set_begin_cycle(&voter, CYCLE - 1);
    env.delegation.set_end_cycle(&voter, CYCLE - 1);
    env.delegation.add_reward(CYCLE - 1, &witness, 1000);
    env.delegation.set_witness_vote(CYCLE - 1, &witness, 10);
    let before = Account {
        address: voter.as_bytes().to_vec(),
        balance: 123,
        allowance: 5,
        create_time: 1_600_000_000_000,
        net_usage: 77,
        votes: vec![Vote { vote_address: witness.as_bytes().to_vec(), vote_count: 10 }],
        ..Default::default()
    };
    env.accounts.put(&voter, &before).unwrap();

    let paid = withdraw_reward(&voter, &env.accounts, &env.delegation, &env.dyn_props, None).unwrap();
    assert!(paid > 0, "the test must actually pay a reward");
    assert_eq!(env.accounts.get(&voter).unwrap().unwrap().allowance, 5 + paid);

    let snapshot = env
        .delegation
        .get_account_vote(CYCLE, &voter)
        .unwrap()
        .expect("account-vote snapshot");
    assert_eq!(snapshot, before, "whole account, allowance as on entry");
}
