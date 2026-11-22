
//! Explicit, loopback-only private-chain bootstrap. Not in normal node builds.
use std::path::Path;
use serde::Deserialize;
use tron_chainbase::{BlockIndexStore, BlockStore, DynamicPropertiesStore};
use crate::{NodeConfig, OpenedStores};

#[derive(Deserialize)]
struct Asset { address: String, name: String, balance: i64 }
#[derive(Deserialize)]
struct Genesis { timestamp: i64, assets: Vec<Asset>, witness: String }

pub fn validate(config: &NodeConfig) -> Result<(), Box<dyn std::error::Error>> {
    if config.rpc.chain_id != 20261030
        || !config.data_dir.components().any(|c| c.as_os_str().to_string_lossy().starts_with("rust-devnet-tests-"))
        || config.http.host != "127.0.0.1" || config.rpc.host != "127.0.0.1"
        || config.grpc.host != "127.0.0.1" || config.metrics.host != "127.0.0.1"
        || config.p2p.listen_host != "127.0.0.1" || config.p2p.discover_enable
        || !config.p2p.discover_tree_urls.is_empty()
        || config.p2p.fast_forward_nodes.iter().any(|p| !p.starts_with("127.0.0.1:"))
        || config.p2p.peers.is_empty()
        || config.p2p.peers.iter().any(|p| !p.starts_with("127.0.0.1:"))
    { return Err("devnet-fast requires its private chain ID, fresh test runtime and loopback-only endpoints/peers".into()); }
    Ok(())
}
fn address(s: &str) -> Result<[u8;21], Box<dyn std::error::Error>> {
    Ok(hex::decode(s)?.try_into().map_err(|_| "invalid genesis address length")?)
}
pub fn initialize(config: &NodeConfig, genesis_file: &Path) -> Result<(), Box<dyn std::error::Error>> {
    validate(config)?;
    if config.data_dir.join("db").exists() { return Err("refusing to overwrite an existing devnet ledger".into()); }
    let g: Genesis = serde_json::from_slice(&std::fs::read(genesis_file)?)?;
    let assets: Vec<_> = g.assets.iter().map(|a| Ok(tron_types::GenesisAsset {
        address: address(&a.address)?, balance:a.balance,
        name: Box::leak(a.name.clone().into_boxed_str()),
    })).collect::<Result<_, Box<dyn std::error::Error>>>()?;
    let witness=address(&g.witness)?;
    let inputs=tron_types::GenesisInputs {
        timestamp:g.timestamp, parent_hash:[0x50;32], assets:&assets, witness_address:&witness,
    };
    let block=tron_types::build_genesis_block(&inputs);
    let id=tron_types::genesis_block_id(&inputs);
    tron_chainbase::set_block_cache_bytes(64*1024*1024);
    let stores=OpenedStores::open_configured(&config.data_dir, &config.storage)?;
    BlockStore::new(stores.blocks.clone()).put(&id,&block)?;
    BlockIndexStore::new(stores.block_index.clone()).put(&id)?;
    let dp=DynamicPropertiesStore::new(stores.dyn_props.clone());
    dp.save_latest_block_header_number(0);
    dp.save_latest_block_header_timestamp(g.timestamp);
    dp.save_genesis_block_timestamp(g.timestamp);
    dp.save_latest_block_header_hash(id.as_bytes());
    dp.put_long(b"LATEST_SOLIDIFIED_BLOCK_NUM",0);
    tron_executor::apply_genesis_allocations(&stores.to_state_backends(),&assets,&[
        tron_types::GenesisWitness {address:witness,vote_count:100_000_000,url:"http://localhost"}
    ])?;
    let committee = config.resolve_committee()?;
    crate::runtime::seed_committee_initial_values(&stores, &committee);
    // These upgrades are active from our synthetic genesis, rather than
    // switched on by a later governance proposal.
    if committee.allow_tvm_vote == 1 || committee.allow_new_reward == 1 {
        dp.put_long(b"NEW_REWARD_ALGORITHM_EFFECTIVE_CYCLE", 0);
    }
    dp.put_long(b"MAINTENANCE_TIME_INTERVAL",60_000);
    dp.put_long(b"NEXT_MAINTENANCE_TIME",g.timestamp+60_000);
    dp.put_long(b"TRANSACTION_FEE",1_000);
    for backend in [&stores.accounts, &stores.witnesses, &stores.witness_schedule, &stores.name_index, &stores.dyn_props, &stores.blocks, &stores.block_index] { backend.sync_wal()?; }
    std::fs::write(config.data_dir.join("devnet.json"),serde_json::to_vec(&serde_json::json!({
        "schema":1,"chain_id":20261030,"genesis":hex::encode(id.as_bytes()),"unfreeze_day_ms":tron_tvm::unfreeze_day_ms()
    }))?)?;
    println!("private genesis initialized: {}",hex::encode(id.as_bytes()));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fast_build_rejects_public_network_defaults_and_external_peers() {
        let mut c=NodeConfig::default();
        assert!(validate(&c).is_err());
        c.data_dir="/tmp/rust-devnet-tests-guard/observer".into();
        c.rpc.chain_id=20261030;
        c.p2p.listen_host="127.0.0.1".into();
        c.p2p.discover_enable=false;c.p2p.discover_tree_urls.clear();
        c.p2p.peers=vec!["127.0.0.1:18890".into()];
        assert!(validate(&c).is_ok());
        c.p2p.peers=vec!["1.2.3.4:18888".into()];assert!(validate(&c).is_err());
    }
    #[test]
    fn private_genesis_enables_reward_cycles_from_zero() {
        let root = std::env::temp_dir().join(format!("rust-devnet-tests-reward-init-{}-{}",
            std::process::id(), std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
        std::fs::create_dir_all(&root).unwrap();
        let mut c = NodeConfig::default();
        c.data_dir = root.join("chain");
        c.rpc.chain_id = 20261030;
        c.p2p.listen_host = "127.0.0.1".into();
        c.p2p.discover_enable = false;
        c.p2p.discover_tree_urls.clear();
        c.p2p.peers = vec!["127.0.0.1:18890".into()];
        c.committee.changed_delegation = 1;
        c.committee.allow_tvm_vote = 1;
        c.committee.allow_new_reward = 1;
        let address = format!("41{}", "11".repeat(20));
        let genesis = root.join("genesis.json");
        std::fs::write(&genesis, serde_json::to_vec(&serde_json::json!({
            "timestamp": 1_700_000_000_000_i64,
            "witness": address,
            "assets": [{"address": address, "name": "sr", "balance": 1_000_000_000_i64}]
        })).unwrap()).unwrap();
        initialize(&c, &genesis).unwrap();
        // The account-asset backend is process-global; reopening the whole
        // bundle in this process would contend with its retained handle.
        let properties = tron_chainbase::RocksDbBackend::open_tuned(
            c.data_dir.join("database/properties"), 2, 32).unwrap();
        let dp = DynamicPropertiesStore::new(std::sync::Arc::new(properties));
        assert!(dp.allow_change_delegation(), "canonical CHANGE_DELEGATION key activates cycles");
        assert_eq!(dp.get_long(b"NEW_REWARD_ALGORITHM_EFFECTIVE_CYCLE"), Some(0));
        drop(dp);
        std::fs::remove_dir_all(root).unwrap();
    }

}
