//! Persistent isolated sandbox, deliberately unavailable in ordinary public builds.
use crate::{NodeConfig, OpenedStores};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};
use tron_chainbase::{AccountStore, BlockIndexStore, BlockStore, DynamicPropertiesStore};
use tron_crypto::address::Address;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Asset {
    pub address: String,
    pub name: String,
    pub balance: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Witness {
    pub address: String,
    pub vote_count: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Stake {
    pub address: String,
    pub energy_sun: i64,
    pub bandwidth_sun: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    pub schema: u32,
    pub name: String,
    pub chain_id: u64,
    pub p2p_network_id: i32,
    pub timestamp_ms: i64,
    pub native_unfreeze_delay_secs: u64,
    pub source: serde_json::Value,
    pub witnesses: Vec<Witness>,
    pub assets: Vec<Asset>,
    pub stakes: Vec<Stake>,
    /// Exact canonical DynamicPropertiesStore keys, mapped from captured mainnet.
    pub parameters: BTreeMap<String, i64>,
}
fn address(s: &str) -> Result<[u8; 21]> {
    let raw: [u8; 21] = hex::decode(s)?
        .try_into()
        .map_err(|_| "private address must be 21 bytes")?;
    if raw[0] != 0x41 {
        return Err("private account address prefix must be 41".into());
    }
    Ok(raw)
}
impl Profile {
    pub fn load(path: &Path) -> Result<Self> {
        let profile: Self = serde_json::from_slice(&std::fs::read(path)?)?;
        profile.validate()?;
        Ok(profile)
    }
    pub fn validate(&self) -> Result<()> {
        if self.schema != 1
            || self.name.is_empty()
            || self.name.len() > 64
            || !self
                .name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            || self.chain_id < 1_000_000_000
            || self.chain_id > i32::MAX as u64
            || self.p2p_network_id != self.chain_id as i32
            || [1, 11_111, 201_910_292].contains(&self.p2p_network_id)
            || self.timestamp_ms <= 0
            || self.timestamp_ms % 3_000 != 0
            || !(60..=86_400).contains(&self.native_unfreeze_delay_secs)
        {
            return Err("invalid isolated private identity or native delay".into());
        }
        if self.witnesses.len() != 27 {
            return Err("private profile requires exactly 27 logical witnesses".into());
        }
        if self
            .source
            .get("version")
            .and_then(serde_json::Value::as_i64)
            != Some(tron_chainbase::BLOCK_VERSION as i64)
        {
            return Err("captured private protocol version must match this binary".into());
        }
        let mut witnesses = BTreeSet::new();
        for witness in &self.witnesses {
            if !witnesses.insert(address(&witness.address)?) || witness.vote_count <= 0 {
                return Err(
                    "private witnesses must be distinct and have positive genesis votes".into(),
                );
            }
        }
        let mut assets = BTreeMap::new();
        let mut names = BTreeSet::new();
        let mut total = 0_i64;
        for asset in &self.assets {
            if asset.balance < 0
                || asset.name.is_empty()
                || !names.insert(&asset.name)
                || assets
                    .insert(address(&asset.address)?, asset.balance)
                    .is_some()
            {
                return Err("invalid or duplicate private genesis allocation".into());
            }
            total = total
                .checked_add(asset.balance)
                .ok_or("private genesis total exceeds signed SUN range")?;
        }
        if witnesses.iter().any(|a| !assets.contains_key(a)) {
            return Err("every logical witness requires an explicit genesis allocation".into());
        }
        for key in self.parameters.keys() {
            if !parameter_keys().contains(key.as_str()) {
                return Err(format!("unsupported captured chain parameter: {key}").into());
            }
        }
        for key in [
            "ENERGY_FEE",
            "TRANSACTION_FEE",
            "MAINTENANCE_TIME_INTERVAL",
            "TOTAL_ENERGY_LIMIT",
            "TOTAL_ENERGY_CURRENT_LIMIT",
            "TOTAL_NET_LIMIT",
            "UNFREEZE_DELAY_DAYS",
        ] {
            if self.parameters.get(key).copied().unwrap_or(0) <= 0 {
                return Err(format!("required mainnet parameter must be positive: {key}").into());
            }
        }
        // Current local producer supports DPoS finality. PBFT has an independent
        // one-key runtime and must not masquerade as a 27-key PBFT quorum.
        if self.parameters.get("ALLOW_PBFT").copied().unwrap_or(0) != 0 {
            return Err(
                "private 27-key producer currently requires mainnet DPoS profile with ALLOW_PBFT=0"
                    .into(),
            );
        }
        let mut energy = 0_i64;
        let mut net = 0_i64;
        let mut staked = BTreeSet::new();
        for stake in &self.stakes {
            let addr = address(&stake.address)?;
            let sum = stake
                .energy_sun
                .checked_add(stake.bandwidth_sun)
                .ok_or("private stake overflow")?;
            if stake.energy_sun < 0
                || stake.bandwidth_sun < 0
                || sum <= 0
                || stake.energy_sun % 1_000_000 != 0
                || stake.bandwidth_sun % 1_000_000 != 0
                || !staked.insert(addr)
                || assets.get(&addr).copied().unwrap_or(-1) < sum
            {
                return Err(
                    "background resource stakes must be fully genesis-funded whole TRX".into(),
                );
            }
            energy = energy
                .checked_add(stake.energy_sun / 1_000_000)
                .ok_or("energy weight overflow")?;
            net = net
                .checked_add(stake.bandwidth_sun / 1_000_000)
                .ok_or("net weight overflow")?;
        }
        if energy <= 0
            || net <= 0
            || self.parameters.get("TOTAL_ENERGY_WEIGHT") != Some(&energy)
            || self.parameters.get("TOTAL_NET_WEIGHT") != Some(&net)
        {
            return Err(
                "mainnet resource weights must equal actual funded genesis stake backing".into(),
            );
        }
        Ok(())
    }
    pub fn hash(&self) -> Result<[u8; 32]> {
        Ok(Sha256::digest(serde_json::to_vec(self)?).into())
    }
    pub fn genesis(&self) -> Result<(tron_proto::Block, tron_types::BlockId)> {
        let assets = self
            .assets
            .iter()
            .map(|a| {
                Ok(tron_types::GenesisAsset {
                    address: address(&a.address)?,
                    name: Box::leak(a.name.clone().into_boxed_str()),
                    balance: a.balance,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let witness = address(&self.witnesses[0].address)?;
        // The parent field commits the entire immutable profile (parameters,
        // identities, native timing and resource backing) into chain identity.
        let inputs = tron_types::GenesisInputs {
            timestamp: self.timestamp_ms,
            parent_hash: self.hash()?,
            assets: &assets,
            witness_address: &witness,
        };
        Ok((
            tron_types::build_genesis_block(&inputs),
            tron_types::genesis_block_id(&inputs),
        ))
    }
}
pub fn profile(config: &NodeConfig) -> Result<Profile> {
    Profile::load(
        &config
            .private_network
            .as_ref()
            .ok_or("private profile missing")?
            .manifest,
    )
}
pub fn validate_config(config: &NodeConfig) -> Result<Profile> {
    if cfg!(feature = "devnet-fast") {
        return Err("private-network and devnet-fast are mutually exclusive".into());
    }
    let p = profile(config)?;
    if config.rpc.chain_id != p.chain_id
        || !config.data_dir.is_absolute()
        || config.http.host != "127.0.0.1"
        || config.rpc.host != "127.0.0.1"
        || config.grpc.host != "127.0.0.1"
        || config.metrics.host != "127.0.0.1"
        || config.p2p.listen_host != "127.0.0.1"
        || config.p2p.discover_enable
        || !config.p2p.discover_tree_urls.is_empty()
        || config.p2p.node_discovery_persist
        || config.p2p.peers.is_empty()
        || config.p2p.tip_test.is_some()
        || config.p2p.follow_tip
        || config.p2p.explore
        || config.p2p.mempool
        || config.storage.snapshot_reorg
        || !config.index.enable
        || !config.index.firehose.enable
        || config
            .p2p
            .peers
            .iter()
            .chain(config.p2p.fast_forward_nodes.iter())
            .any(|s| {
                !s.parse::<std::net::SocketAddr>()
                    .is_ok_and(|a| a.ip().is_loopback() && a.port() != 0)
            })
    {
        return Err("private profile requires isolated loopback endpoints, explicit peers, complete indexing and real ledger execution".into());
    }
    if config.data_dir.join("database").exists() || config.data_dir.join("db").exists() {
        let expected_genesis = hex::encode(p.genesis()?.1.as_bytes());
        let expected_hash = hex::encode(p.hash()?);
        let marker = config.data_dir.join("private-profile.json");
        if marker.is_file() {
            let m: serde_json::Value = serde_json::from_slice(&std::fs::read(marker)?)?;
            if m["genesis"].as_str() != Some(&expected_genesis)
                || m["profile_hash"].as_str() != Some(&expected_hash)
            {
                return Err("existing private data marker differs from profile".into());
            }
        } else {
            // A crash can occur after atomic database activation but before
            // the transported marker is copied. Only that durable, pinned
            // bootstrap intent may proceed to recovery; public ledgers fail
            // before opening or replaying any database.
            let intent_path = config.data_dir.join("bootstrap/receiver.json");
            let intent: crate::assisted_sync::bootstrap::Download = serde_json::from_slice(
                &std::fs::read(intent_path)
                    .map_err(|_| "existing ledger has no private identity marker")?,
            )?;
            let manifest = intent
                .manifest
                .as_ref()
                .ok_or("bootstrap manifest missing")?;
            manifest
                .validate(p.genesis()?.1.as_bytes(), config.assist.max_snapshot_bytes)
                .map_err(|e| format!("private bootstrap intent: {e}"))?;
            if !config.assist.enable
                || intent.phase != "installing"
                || !manifest
                    .files
                    .iter()
                    .any(|f| f.path == "private-profile.json")
            {
                return Err("existing ledger has no completed private identity".into());
            }
        }
    }
    let private = config.private_network.as_ref().unwrap();
    if !(250..=5000).contains(&private.tip_poll_interval_ms) {
        return Err("private tip_poll_interval_ms must be250..5000".into());
    }
    if config.witness.is_some() != private.witness_keys.is_some() {
        return Err("private producer requires its separate witness key file; followers must have no witness keys".into());
    }
    if config.assist.enable
        && config.assist.bootstrap_genesis.as_deref()
            != Some(hex::encode(p.genesis()?.1.as_bytes()).as_str())
    {
        return Err("private assist requires the exact pinned profile genesis".into());
    }
    Ok(p)
}
pub fn verify_ledger(config: &NodeConfig, stores: &OpenedStores) -> Result<()> {
    let p = profile(config)?;
    let dp = DynamicPropertiesStore::new(stores.dyn_props.clone());
    let expected = p.genesis()?.1;
    if BlockIndexStore::new(stores.block_index.clone()).get(0)? != expected
        || dp.get_bytes(b"PRIVATE_PROFILE_HASH").as_deref() != Some(p.hash()?.as_slice())
        || dp.get_long(b"PRIVATE_PROTOCOL_VERSION") != Some(tron_chainbase::BLOCK_VERSION as i64)
        || dp.get_long(b"PRIVATE_UNFREEZE_DELAY_MS")
            != Some((p.native_unfreeze_delay_secs * 1000) as i64)
    {
        return Err("private ledger identity/timing does not match immutable profile; refuse public or incompatible data".into());
    }
    Ok(())
}

fn parameter_keys() -> BTreeSet<&'static str> {
    [
        r"MAINTENANCE_TIME_INTERVAL",
        r"ACCOUNT_UPGRADE_COST",
        r"CREATE_ACCOUNT_FEE",
        r"TRANSACTION_FEE",
        r"ASSET_ISSUE_FEE",
        r"WITNESS_PAY_PER_BLOCK",
        r"WITNESS_STANDBY_ALLOWANCE",
        r"CREATE_NEW_ACCOUNT_FEE_IN_SYSTEM_CONTRACT",
        r"CREATE_NEW_ACCOUNT_BANDWIDTH_RATE",
        r"ALLOW_CREATION_OF_CONTRACTS",
        r"REMOVE_THE_POWER_OF_THE_GR",
        r"ENERGY_FEE",
        r"EXCHANGE_CREATE_FEE",
        r"MAX_CPU_TIME_OF_ONE_TX",
        r"ALLOW_UPDATE_ACCOUNT_NAME",
        r" ALLOW_SAME_TOKEN_NAME",
        r"ALLOW_DELEGATE_RESOURCE",
        r"TOTAL_ENERGY_LIMIT",
        r"ALLOW_TVM_TRANSFER_TRC10",
        r"TOTAL_ENERGY_CURRENT_LIMIT",
        r"ALLOW_MULTI_SIGN",
        r"ALLOW_ADAPTIVE_ENERGY",
        r"TOTAL_ENERGY_TARGET_LIMIT",
        r"TOTAL_ENERGY_AVERAGE_USAGE",
        r"UPDATE_ACCOUNT_PERMISSION_FEE",
        r"MULTI_SIGN_FEE",
        r"ALLOW_ACCOUNT_STATE_ROOT",
        r"ALLOW_PROTO_FILTER_NUM",
        r"ALLOW_TVM_CONSTANTINOPLE",
        r"ALLOW_TVM_SOLIDITY_059",
        r"ALLOW_TVM_ISTANBUL",
        r"ALLOW_SHIELDED_TRC20_TRANSACTION",
        r"FORBID_TRANSFER_TO_CONTRACT",
        r"ADAPTIVE_RESOURCE_LIMIT_TARGET_RATIO",
        r"ADAPTIVE_RESOURCE_LIMIT_MULTIPLIER",
        r"CHANGE_DELEGATION",
        r"WITNESS_127_PAY_PER_BLOCK",
        r"ALLOW_MARKET_TRANSACTION",
        r"MARKET_SELL_FEE",
        r"MARKET_CANCEL_FEE",
        r"ALLOW_PBFT",
        r"ALLOW_TRANSACTION_FEE_POOL",
        r"MAX_FEE_LIMIT",
        r"ALLOW_BLACKHOLE_OPTIMIZATION",
        r"ALLOW_NEW_RESOURCE_MODEL",
        r"ALLOW_TVM_FREEZE",
        r"ALLOW_TVM_VOTE",
        r"ALLOW_TVM_LONDON",
        r"ALLOW_TVM_COMPATIBLE_EVM",
        r"ALLOW_ACCOUNT_ASSET_OPTIMIZATION",
        r"FREE_NET_LIMIT",
        r"TOTAL_NET_LIMIT",
        r"ALLOW_HIGHER_LIMIT_FOR_MAX_CPU_TIME_OF_ONE_TX",
        r"ALLOW_ASSET_OPTIMIZATION",
        r"ALLOW_NEW_REWARD",
        r"MEMO_FEE",
        r"ALLOW_DELEGATE_OPTIMIZATION",
        r"UNFREEZE_DELAY_DAYS",
        r"ALLOW_OPTIMIZED_RETURN_VALUE_OF_CHAIN_ID",
        r"ALLOW_DYNAMIC_ENERGY",
        r"DYNAMIC_ENERGY_THRESHOLD",
        r"DYNAMIC_ENERGY_INCREASE_FACTOR",
        r"DYNAMIC_ENERGY_MAX_FACTOR",
        r"ALLOW_TVM_SHANGHAI",
        r"ALLOW_CANCEL_ALL_UNFREEZE_V2",
        r"MAX_DELEGATE_LOCK_PERIOD",
        r"ALLOW_OLD_REWARD_OPT",
        r"ALLOW_ENERGY_ADJUSTMENT",
        r"MAX_CREATE_ACCOUNT_TX_SIZE",
        r"ALLOW_STRICT_MATH",
        r"CONSENSUS_LOGIC_OPTIMIZATION",
        r"ALLOW_TVM_CANCUN",
        r"ALLOW_TVM_BLOB",
        r"ALLOW_TVM_SELFDESTRUCT_RESTRICTION",
        r"PROPOSAL_EXPIRE_TIME",
        r"ALLOW_TVM_OSAKA",
        r"ALLOW_TVM_PRAGUE",
        r"ALLOW_HARDEN_RESOURCE_CALCULATION",
        r"ALLOW_HARDEN_EXCHANGE_CALCULATION",
        r"ALLOW_OPTIMIZE_TVM_STORAGE",
        r"ALLOW_OPTIMIZE_TVM_STORAGE_KEY",
        r"TOTAL_ENERGY_WEIGHT",
        r"TOTAL_NET_WEIGHT",
    ]
    .into_iter()
    .collect()
}
/// Fresh genesis only. Existing ledgers are never replaced, repaired or reset.
pub fn initialize(config: &NodeConfig) -> Result<()> {
    let p = validate_config(config)?;
    if config.data_dir.join("database").exists()
        || config.data_dir.join("db").exists()
        || config.data_dir.join("private-profile.json").exists()
    {
        return Err("refusing to overwrite an existing private ledger".into());
    }
    if config.witness.is_some() {
        let _ = witness_identities(config)?;
    }
    let stores = OpenedStores::open_configured(&config.data_dir, &config.storage)?;
    let dp = DynamicPropertiesStore::new(stores.dyn_props.clone());
    let (block, id) = p.genesis()?;
    let assets = p
        .assets
        .iter()
        .map(|a| {
            Ok(tron_types::GenesisAsset {
                address: address(&a.address)?,
                name: Box::leak(a.name.clone().into_boxed_str()),
                balance: a.balance,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let witnesses = p
        .witnesses
        .iter()
        .map(|w| {
            Ok(tron_types::GenesisWitness {
                address: address(&w.address)?,
                vote_count: w.vote_count,
                url: "http://private.invalid",
            })
        })
        .collect::<Result<Vec<_>>>()?;
    tron_executor::apply_genesis_allocations(&stores.to_state_backends(), &assets, &witnesses)?;
    crate::runtime::seed_committee_initial_values(
        &stores,
        &crate::config::CommitteeConfig::default(),
    );
    for (key, value) in &p.parameters {
        if key != "TOTAL_ENERGY_WEIGHT" && key != "TOTAL_NET_WEIGHT" {
            dp.put_long(key.as_bytes(), *value);
        }
    }
    // Resource ratios are backed by actual account stake through the ordinary
    // native actuator, rather than an invented denominator detached from state.
    dp.save_total_energy_weight(0);
    dp.save_total_net_weight(0);
    let accounts = AccountStore::new(stores.accounts.clone());
    for stake in &p.stakes {
        for (resource, amount) in [(0, stake.bandwidth_sun), (1, stake.energy_sun)] {
            if amount == 0 {
                continue;
            }
            let contract = tron_proto::FreezeBalanceV2Contract {
                owner_address: address(&stake.address)?.to_vec(),
                frozen_balance: amount,
                resource,
            };
            tron_actuator::freeze_v2::validate_freeze_balance_v2(&accounts, &dp, &contract)?;
            tron_actuator::freeze_v2::execute_freeze_balance_v2(&accounts, &dp, &contract)?;
        }
    }
    if dp.total_energy_weight() != p.parameters["TOTAL_ENERGY_WEIGHT"]
        || dp.total_net_weight() != p.parameters["TOTAL_NET_WEIGHT"]
    {
        return Err("genesis native stake weights do not match captured profile".into());
    }
    BlockStore::new(stores.blocks.clone()).put(&id, &block)?;
    BlockIndexStore::new(stores.block_index.clone()).put(&id)?;
    dp.save_latest_block_header_number(0);
    dp.save_latest_block_header_timestamp(p.timestamp_ms);
    dp.save_genesis_block_timestamp(p.timestamp_ms);
    dp.save_latest_block_header_hash(id.as_bytes());
    dp.put_long(b"LATEST_SOLIDIFIED_BLOCK_NUM", 0);
    dp.put_long(
        b"NEXT_MAINTENANCE_TIME",
        p.timestamp_ms
            .checked_add(p.parameters["MAINTENANCE_TIME_INTERVAL"])
            .ok_or("maintenance overflow")?,
    );
    dp.put_long(
        b"PRIVATE_UNFREEZE_DELAY_MS",
        (p.native_unfreeze_delay_secs * 1000) as i64,
    );
    dp.put_long(
        b"PRIVATE_PROTOCOL_VERSION",
        tron_chainbase::BLOCK_VERSION as i64,
    );
    dp.put_bytes(b"PRIVATE_PROFILE_HASH", &p.hash()?);
    // This private chain starts at the captured contemporary protocol, without
    // inventing block height, timestamp, rewards or finalized witness history.
    let active = tron_chainbase::WitnessScheduleStore::new(stores.witness_schedule.clone())
        .load_active()?
        .ok_or("genesis witness schedule absent")?;
    let versions = active
        .iter()
        .copied()
        .map(|a| (a, tron_chainbase::BLOCK_VERSION))
        .collect();
    tron_chainbase::backfill_fork_stats(&dp, &active, &versions);
    if dp.get_long(b"ALLOW_NEW_REWARD").unwrap_or(0) == 1
        || dp.get_long(b"ALLOW_TVM_VOTE").unwrap_or(0) == 1
    {
        dp.put_long(b"NEW_REWARD_ALGORITHM_EFFECTIVE_CYCLE", 0);
    }
    for backend in [
        &stores.accounts,
        &stores.witnesses,
        &stores.witness_schedule,
        &stores.name_index,
        &stores.dyn_props,
        &stores.blocks,
        &stores.block_index,
    ] {
        backend.sync_wal()?;
    }
    std::fs::write(
        config.data_dir.join("private-profile.json"),
        serde_json::to_vec(&serde_json::json!({
            "schema": 1, "genesis": hex::encode(id.as_bytes()), "profile_hash": hex::encode(p.hash()?),
            "native_unfreeze_delay_secs": p.native_unfreeze_delay_secs
        }))?,
    )?;
    println!(
        "{}",
        serde_json::json!({"genesis": hex::encode(id.as_bytes()),"profile_hash":hex::encode(p.hash()?)})
    );
    Ok(())
}
/// Raw keys are read only by the dedicated producer, from a separate 0600 file.
pub fn witness_identities(config: &NodeConfig) -> Result<Vec<crate::sr_runtime::SrIdentity>> {
    let p = profile(config)?;
    let path = config
        .private_network
        .as_ref()
        .and_then(|c| c.witness_keys.as_ref())
        .ok_or("private witness key file missing")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if std::fs::metadata(path)?.permissions().mode() & 0o077 != 0 {
            return Err("private witness key file must not be accessible to group/others".into());
        }
    }
    let keys: Vec<String> = serde_json::from_slice(&std::fs::read(path)?)?;
    let mut identities = Vec::new();
    let mut addresses = BTreeSet::new();
    for key in keys {
        let bytes: [u8; 32] = hex::decode(&key)?
            .try_into()
            .map_err(|_| "private witness key must be 32 bytes")?;
        let addr = tron_wallet::address_from_private(&bytes)?;
        if !addresses.insert(addr) {
            return Err("duplicate private witness key".into());
        }
        identities.push(crate::sr_runtime::SrIdentity {
            witness_address: addr,
            witness_priv_key: bytes,
        });
    }
    let expected = p
        .witnesses
        .iter()
        .map(|w| address(&w.address).map(Address::from_raw))
        .collect::<Result<BTreeSet<_>>>()?;
    if addresses != expected {
        return Err("private witness keys must exactly match all 27 profile witnesses".into());
    }
    Ok(identities)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tron_chainbase::{BlockUndoStore, KvBackend, MemBackend};

    fn identities() -> Vec<crate::SrIdentity> {
        (1..=27)
            .map(|i| {
                let mut key = [0u8; 32];
                key[31] = i;
                crate::SrIdentity {
                    witness_address: tron_wallet::address_from_private(&key).unwrap(),
                    witness_priv_key: key,
                }
            })
            .collect()
    }
    fn fixture() -> Profile {
        let ids = identities();
        let witnesses = ids
            .iter()
            .enumerate()
            .map(|(i, k)| Witness {
                address: hex::encode(k.witness_address.as_bytes()),
                vote_count: 100_000 - i as i64,
            })
            .collect();
        let assets = ids
            .iter()
            .enumerate()
            .map(|(i, k)| Asset {
                address: hex::encode(k.witness_address.as_bytes()),
                name: format!("sr{i}"),
                balance: 10_000_000,
            })
            .collect();
        Profile {
            schema: 1,
            name: "private-test".into(),
            chain_id: 2_026_111_501,
            p2p_network_id: 2_026_111_501,
            timestamp_ms: 1_800_000_000_000,
            native_unfreeze_delay_secs: 300,
            source: serde_json::json!({"network":"mainnet","version":39}),
            witnesses,
            assets,
            stakes: vec![Stake {
                address: hex::encode(ids[0].witness_address.as_bytes()),
                energy_sun: 2_000_000,
                bandwidth_sun: 1_000_000,
            }],
            parameters: [
                ("ENERGY_FEE", 100),
                ("TRANSACTION_FEE", 1000),
                ("TOTAL_ENERGY_LIMIT", 180_000_000_000),
                ("TOTAL_ENERGY_CURRENT_LIMIT", 180_000_000_000),
                ("TOTAL_ENERGY_WEIGHT", 2),
                ("TOTAL_NET_WEIGHT", 1),
                ("TOTAL_NET_LIMIT", 43_200_000_000),
                ("UNFREEZE_DELAY_DAYS", 14),
                ("MAINTENANCE_TIME_INTERVAL", 21_600_000),
            ]
            .into_iter()
            .map(|(k, v)| (k.into(), v))
            .collect(),
        }
    }
    fn mem() -> Arc<dyn KvBackend> {
        Arc::new(MemBackend::new())
    }
    fn memory_state() -> tron_executor::StateBackends {
        tron_executor::StateBackends {
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
    #[test]
    fn immutable_identity_covers_native_timing_parameters_and_resource_backing() {
        let p = fixture();
        p.validate().unwrap();
        let mut current_parameters = p.clone();
        current_parameters.parameters.insert("ALLOW_TVM_BLOB".into(), 0);
        current_parameters.parameters.insert(" ALLOW_SAME_TOKEN_NAME".into(), 1);
        current_parameters.validate().unwrap();
        assert_ne!(p.hash().unwrap(), current_parameters.hash().unwrap());
        let original = p.genesis().unwrap().1;
        let mut changed = p.clone();
        changed.native_unfreeze_delay_secs = 301;
        assert_ne!(original, changed.genesis().unwrap().1);
        let mut changed = p.clone();
        changed.parameters.insert("ENERGY_FEE".into(), 101);
        assert_ne!(original, changed.genesis().unwrap().1);
        let mut changed = p.clone();
        changed.parameters.insert("TOTAL_ENERGY_WEIGHT".into(), 3);
        assert!(changed.validate().is_err());
        let mut changed = p.clone();
        changed.witnesses.pop();
        assert!(changed.validate().is_err());
        let mut changed = p.clone();
        changed.chain_id = 11_111;
        assert!(changed.validate().is_err());
        let mut changed = p.clone();
        changed.native_unfreeze_delay_secs = 0;
        assert!(changed.validate().is_err());
        let mut changed = p.clone();
        changed
            .parameters
            .insert("LATEST_SOLIDIFIED_BLOCK_NUM".into(), 100);
        assert!(changed.validate().is_err());
    }
    #[test]
    fn private_startup_rejects_public_or_mismatched_existing_ledger_before_open() {
        let root = std::env::temp_dir().join(format!(
            "private-profile-guard-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let p = fixture();
        let manifest = root.join("profile.json");
        std::fs::write(&manifest, serde_json::to_vec(&p).unwrap()).unwrap();
        let mut config = NodeConfig::default();
        config.private_network = Some(crate::config::PrivateNetworkConfig {
            manifest,
            witness_keys: None,
            tip_poll_interval_ms: 1000,
        });
        config.rpc.chain_id = p.chain_id;
        config.data_dir = root.join("follower");
        config.p2p.listen_host = "127.0.0.1".into();
        config.p2p.discover_enable = false;
        config.p2p.discover_tree_urls.clear();
        config.p2p.node_discovery_persist = false;
        config.p2p.peers = vec!["127.0.0.1:19104".into()];
        config.storage.snapshot_reorg = false;
        config.index.enable = true;
        config.index.firehose.enable = true;
        validate_config(&config).unwrap();
        config.private_network.as_mut().unwrap().tip_poll_interval_ms = 0;
        assert!(validate_config(&config).is_err());
        config.private_network.as_mut().unwrap().tip_poll_interval_ms = 1000;
        config.p2p.peers = vec!["8.8.8.8:18888".into()];
        assert!(validate_config(&config).is_err());
        config.p2p.peers = vec!["127.0.0.1:19104".into()];
        std::fs::create_dir_all(config.data_dir.join("database")).unwrap();
        assert!(
            validate_config(&config)
                .unwrap_err()
                .to_string()
                .contains("no private identity")
        );
        std::fs::write(
            config.data_dir.join("private-profile.json"),
            r#"{"genesis":"bad","profile_hash":"bad"}"#,
        )
        .unwrap();
        assert!(
            validate_config(&config)
                .unwrap_err()
                .to_string()
                .contains("differs")
        );
        std::fs::write(config.data_dir.join("private-profile.json"),serde_json::to_vec(&serde_json::json!({
            "genesis":hex::encode(p.genesis().unwrap().1.as_bytes()),"profile_hash":hex::encode(p.hash().unwrap())
        })).unwrap()).unwrap();
        validate_config(&config).unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn native_private_override_is_real_delay_without_changing_chain_day_getter() {
        let dp = DynamicPropertiesStore::new(mem());
        dp.put_long(b"UNFREEZE_DELAY_DAYS", 14);
        assert_eq!(tron_tvm::unfreeze_delay_ms(&dp), 14 * 86_400_000);
        assert!(!tron_chainbase::energy_limit_hard_fork_active(&dp));
        dp.put_long(
            b"PRIVATE_PROTOCOL_VERSION",
            tron_chainbase::BLOCK_VERSION as i64,
        );
        assert!(tron_chainbase::energy_limit_hard_fork_active(&dp));
        assert_eq!(dp.latest_block_header_number(), None);
        dp.put_long(b"PRIVATE_UNFREEZE_DELAY_MS", 300_000);
        assert_eq!(tron_tvm::unfreeze_delay_ms(&dp), 300_000);
        assert_eq!(dp.get_long(b"UNFREEZE_DELAY_DAYS"), Some(14));
    }
    #[test]
    fn twenty_seven_real_signers_produce_dpos_lag_and_missing_quorum_stalls() {
        let p = fixture();
        let state = memory_state();
        let blocks = mem();
        let dp = DynamicPropertiesStore::new(state.dyn_props.clone());
        let (genesis, id) = p.genesis().unwrap();
        let assets = p
            .assets
            .iter()
            .map(|a| tron_types::GenesisAsset {
                address: address(&a.address).unwrap(),
                name: Box::leak(a.name.clone().into_boxed_str()),
                balance: a.balance,
            })
            .collect::<Vec<_>>();
        let witnesses = p
            .witnesses
            .iter()
            .map(|w| tron_types::GenesisWitness {
                address: address(&w.address).unwrap(),
                vote_count: w.vote_count,
                url: "http://private.invalid",
            })
            .collect::<Vec<_>>();
        tron_executor::apply_genesis_allocations(&state, &assets, &witnesses).unwrap();
        BlockStore::new(blocks.clone()).put(&id, &genesis).unwrap();
        BlockIndexStore::new(state.block_index.clone().unwrap())
            .put(&id)
            .unwrap();
        dp.save_latest_block_header_number(0);
        dp.save_latest_block_header_hash(id.as_bytes());
        dp.save_latest_block_header_timestamp(p.timestamp_ms);
        dp.save_genesis_block_timestamp(p.timestamp_ms);
        dp.put_long(b"LATEST_SOLIDIFIED_BLOCK_NUM", 0);
        dp.put_long(b"MAINTENANCE_TIME_INTERVAL", 21_600_000);
        dp.put_long(b"NEXT_MAINTENANCE_TIME", p.timestamp_ms + 21_600_000);
        let shared_khaos = Arc::new(tron_consensus::KhaosDb::new());
        let leadership = Arc::new(crate::sync::SyncLeadership::new());
        let config = crate::SyncConfig {
            peers: Vec::new(), max_blocks: None,
            tail_interval: std::time::Duration::from_secs(3),
            initial_backoff: std::time::Duration::from_millis(1),
            blocks_backend: blocks.clone(), progress_log_interval:0,advertise_port:19104,
            tip_test:false,p2p_rate_limits:Default::default(),
            fetch_block_timeout:std::time::Duration::from_millis(200),
            fetch_inflight_per_peer:16,peer_is_fast_forward:false,follow_tip:false,
        };
        let mut driver = crate::SyncDriver::new(state.clone(),config)
            .with_shared_khaos(shared_khaos.clone()).with_leadership(leadership.clone());
        assert_eq!(driver.tip_repoll_interval(),std::time::Duration::from_secs(12));
        driver = driver.with_private_network(p.p2p_network_id,std::time::Duration::from_secs(1));
        assert_eq!(driver.tip_repoll_interval(),std::time::Duration::from_secs(1));
        let make = |keys: Vec<crate::SrIdentity>, tree: Arc<tron_consensus::KhaosDb>| {
            let first = crate::SrIdentity {
                witness_address: keys[0].witness_address,
                witness_priv_key: keys[0].witness_priv_key,
            };
            crate::SrRuntime::new(
                state.clone(),
                blocks.clone(),
                state.witness_schedule.clone().unwrap(),
                tree,
                BlockUndoStore::new(mem()),
                Arc::new(tron_mempool::TxMempool::new(Default::default())),
                first,
                100,
            )
            .with_private_identities(keys)
            .with_private_apply_lock(leadership.clone())
        };
        let runtime = make(identities(),shared_khaos.clone());
        let active =
            tron_chainbase::WitnessScheduleStore::new(state.witness_schedule.clone().unwrap())
                .load_active()
                .unwrap()
                .unwrap();
        for slot in 1..=54_i64 {
            let notice = runtime
                .try_produce_at(i64::MIN, p.timestamp_ms + slot * 3000)
                .unwrap()
                .unwrap();
            assert_eq!(notice.block_num, slot);
            let block = BlockStore::new(blocks.clone())
                .get(&notice.block_id)
                .unwrap();
            assert!(matches!(
                driver.accept_block_synced(&block,None),
                crate::AcceptOutcome::AlreadyKnown(id) if id == notice.block_id
            ), "a peer echo must not re-execute a locally produced canonical block");
            assert_eq!(shared_khaos.head().unwrap().id,notice.block_id);
            assert_eq!(driver.resume_head(),Some(notice.block_id));
            assert_eq!(
                block
                    .block_header
                    .unwrap()
                    .raw_data
                    .unwrap()
                    .witness_address,
                tron_consensus::slot::scheduled_witness(slot, &active).as_bytes()
            );
            assert_eq!(dp.latest_solidified_block_num(), Some((slot - 18).max(0)));
        }
        // Reproduce the live stale-stream case: old canonical16/17 arrive
        // after local production has reached54 with solid36.
        for number in [16,17] {
            let id = BlockIndexStore::new(state.block_index.clone().unwrap()).get(number).unwrap();
            let echo = BlockStore::new(blocks.clone()).get(&id).unwrap();
            assert!(matches!(driver.accept_block_synced(&echo,None),crate::AcceptOutcome::AlreadyKnown(got) if got==id));
        }
        // A different header hash is not classified as a canonical duplicate.
        let tip = dp.latest_block_header_hash().unwrap().unwrap();
        let mut competing = BlockStore::new(blocks.clone()).get(&tron_types::BlockId::from_raw(tip)).unwrap();
        let raw = competing.block_header.as_mut().unwrap().raw_data.as_mut().unwrap();
        raw.version -= 1;
        let signer = identities().into_iter().find(|k| k.witness_address.as_bytes()==raw.witness_address.as_slice()).unwrap();
        tron_types::sign_block(&mut competing,&signer.witness_priv_key).unwrap();
        assert!(!matches!(driver.accept_block_synced(&competing,None),crate::AcceptOutcome::AlreadyKnown(_)));

        // Missing disk history halfway through startup must not publish a
        // partial shared tree or move durable head. A restored read retries.
        let missing_id = BlockIndexStore::new(state.block_index.clone().unwrap()).get(30).unwrap();
        let missing_block = BlockStore::new(blocks.clone()).get(&missing_id).unwrap();
        blocks.delete(missing_id.as_bytes()).unwrap();
        let recovery_tree = Arc::new(tron_consensus::KhaosDb::new());
        let recovery = make(identities(),recovery_tree.clone());
        assert!(recovery.try_produce_at(i64::MIN,p.timestamp_ms+55*3000).is_err());
        assert!(recovery_tree.head().is_none(),"failed startup must not seed a partial shared tree");
        assert_eq!(dp.latest_block_header_number(),Some(54));
        BlockStore::new(blocks.clone()).put(&missing_id,&missing_block).unwrap();
        assert_eq!(recovery.try_produce_at(i64::MIN,p.timestamp_ms+55*3000).unwrap().unwrap().block_num,55);

        let mut keys = identities();
        keys.truncate(18);
        let restarted_tree = Arc::new(tron_consensus::KhaosDb::new());
        let runtime = make(keys,restarted_tree.clone());
        for slot in 55..=162_i64 {
            runtime
                .try_produce_at(i64::MIN, p.timestamp_ms + slot * 3000)
                .unwrap();
        }
        assert!(dp.latest_block_header_number().unwrap() > 100);
        assert_eq!(restarted_tree.head().unwrap().id,driver.resume_head().unwrap(),
            "restarted producer must seed and advance the executed canonical ancestry");
        assert!(
            dp.latest_solidified_block_num().unwrap() <= 54,
            "18 available identities cannot advance a real 70% finality quorum"
        );
    }
}
