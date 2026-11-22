//! Confirmed HTTP wallet compatibility: latest may advance beyond solidity.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use prost::Message;
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;
use tron_chainbase::{
    BlockIndexStore, BlockStore, BlockUndoStore, DynamicPropertiesStore, KvBackend, MemBackend,
    TransactionRetStore, TransactionStore, UndoStoreId,
};
use tron_crypto::address::Address;
use tron_index::{ArchiveWriter, DeltaRef};
use tron_proto::block_header::Raw as HeaderRaw;
use tron_proto::{Account, Block, BlockHeader, Transaction, TransactionInfo, TransactionRet};
use tron_rpc::{ArchiveApiState, RpcState};

fn mem() -> Arc<dyn KvBackend> {
    Arc::new(MemBackend::new())
}
fn address(byte: u8) -> [u8; 21] {
    let mut a = [byte; 21];
    a[0] = 0x41;
    a
}
const CHAIN_ID: u64 = 0x12345678;

fn fixture() -> (RpcState, [String; 2]) {
    let accounts = mem();
    let blocks = mem();
    let index = mem();
    let txs = mem();
    let dp = mem();
    let ret = mem();
    let delegated = mem();
    let assets = mem();
    let alice = address(0xaa);
    let v100 = Account {
        address: alice.to_vec(),
        balance: 100_000_000,
        net_usage: 10,
        ..Default::default()
    }
    .encode_to_vec();
    let v200 = Account {
        address: alice.to_vec(),
        balance: 200_000_000,
        net_usage: 20,
        ..Default::default()
    }
    .encode_to_vec();
    accounts.put(&alice, &v200).unwrap();
    let dps = DynamicPropertiesStore::new(dp.clone());
    dps.save_latest_block_header_number(12);
    dps.save_latest_solidified_block_num(11);
    let from = Address::from_raw(alice);
    let to = Address::from_raw(address(0xbb));
    let key = tron_chainbase::DelegatedResourceStore::v2_unlocked_key(&from, &to);
    let prior = tron_proto::DelegatedResource {
        from: alice.to_vec(),
        to: to.as_bytes().to_vec(),
        frozen_balance_for_energy: 100,
        ..Default::default()
    }
    .encode_to_vec();
    let current = tron_proto::DelegatedResource {
        from: alice.to_vec(),
        to: to.as_bytes().to_vec(),
        frozen_balance_for_energy: 200,
        ..Default::default()
    }
    .encode_to_vec();
    delegated.put(&key, &current).unwrap();
    let backends = vec![
        (UndoStoreId::Accounts, accounts.clone()),
        (UndoStoreId::DynProps, dp.clone()),
        (UndoStoreId::DelegatedResources, delegated.clone()),
        (UndoStoreId::AssetV2, assets.clone()),
    ];
    let writer = ArchiveWriter::new(mem(), Some(BlockUndoStore::new(mem())), backends.clone());
    writer.check_or_init().unwrap();
    writer.on_block_applied(11, Some(&[])).unwrap();
    writer
        .on_block_applied(
            12,
            Some(&[
                DeltaRef {
                    store: UndoStoreId::Accounts,
                    key: &alice,
                    before: Some(&v100),
                    after: Some(&v200),
                },
                DeltaRef {
                    store: UndoStoreId::DelegatedResources,
                    key: &key,
                    before: Some(&prior),
                    after: Some(&current),
                },
            ]),
        )
        .unwrap();
    let mut ids = Vec::new();
    for h in [11i64, 12] {
        let transaction = Transaction {
            raw_data: Some(tron_proto::transaction::Raw {
                timestamp: 1_700_000_000_000 + h,
                ..Default::default()
            }),
            ..Default::default()
        };
        let txid = tron_types::tx_id(&transaction).unwrap();
        let block = Block {
            block_header: Some(BlockHeader {
                raw_data: Some(HeaderRaw {
                    number: h,
                    timestamp: 1_700_000_000_000 + h * 3_000,
                    parent_hash: vec![0; 32],
                    witness_address: alice.to_vec(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            transactions: vec![transaction],
        };
        let id = tron_types::block_id_from_block(&block).unwrap();
        BlockStore::new(blocks.clone()).put(&id, &block).unwrap();
        BlockIndexStore::new(index.clone()).put(&id).unwrap();
        TransactionStore::new(txs.clone())
            .put_block_ref(&txid, h)
            .unwrap();
        TransactionRetStore::new(ret.clone())
            .put(
                h,
                &TransactionRet {
                    transactioninfo: vec![TransactionInfo {
                        id: txid.to_vec(),
                        block_number: h,
                        block_time_stamp: 1_700_000_000_000 + h * 3_000,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            )
            .unwrap();
        if h == 12 {
            dps.save_latest_block_header_hash(id.as_bytes());
        }
        ids.push(hex::encode(txid));
    }
    let state = RpcState::new(accounts, blocks, index, txs, dp, CHAIN_ID)
        .with_governance_stores(mem(), mem(), delegated, mem(), assets, mem())
        .with_transaction_ret(ret)
        .with_archive(ArchiveApiState::new(writer.reader(), backends));
    (state, [ids.remove(0), ids.remove(0)])
}

async fn call(app: axum::Router, path: &str, body: Value) -> Value {
    let res = app
        .oneshot(
            Request::post(path)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap()
}
async fn get(app: axum::Router, path: &str) -> Value {
    let res = app
        .oneshot(Request::get(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap()
}

#[tokio::test]
async fn confirmed_block_routes_never_return_unconfirmed_head_or_height() {
    let (state, _) = fixture();
    let app = tron_rpc::http_rest::router(state);
    let latest = get(app.clone(), "/wallet/getnowblock").await;
    let solid = get(app.clone(), "/walletsolidity/getnowblock").await;
    assert_eq!(latest["block_header"]["raw_data"]["number"], 12);
    assert_eq!(solid["block_header"]["raw_data"]["number"], 11);
    assert_eq!(
        solid,
        call(app.clone(), "/walletsolidity/getnowblock", json!({})).await
    );
    assert_eq!(
        call(
            app.clone(),
            "/walletsolidity/getblockbynum",
            json!({"num":12})
        )
        .await,
        json!({})
    );
    assert_eq!(
        call(
            app.clone(),
            "/walletsolidity/getblockbynum",
            json!({"num":11})
        )
        .await,
        solid
    );
    assert_ne!(latest["blockID"], solid["blockID"]);
    assert!(solid["blockID"]
        .as_str()
        .unwrap()
        .chars()
        .all(|c| c.is_ascii_hexdigit()));
}

#[tokio::test]
async fn confirmed_account_and_resources_read_archived_solid_state() {
    let (state, _) = fixture();
    let app = tron_rpc::http_rest::router(state);
    let alice = Address::from_raw(address(0xaa));
    for visible in [false, true] {
        let who = if visible {
            tron_crypto::base58check::encode_address(&alice)
        } else {
            hex::encode(alice.as_bytes())
        };
        let body = json!({"address":who,"visible":visible});
        let latest = call(app.clone(), "/wallet/getaccount", body.clone()).await;
        let solid = call(app.clone(), "/walletsolidity/getaccount", body.clone()).await;
        assert_eq!(latest["balance"], 200_000_000);
        assert_eq!(solid["balance"], 100_000_000);
        assert_eq!(solid["address"], who);
        let latest = call(app.clone(), "/wallet/getaccountresource", body.clone()).await;
        let solid = call(app.clone(), "/walletsolidity/getaccountresource", body).await;
        assert_eq!(latest["NetUsed"], 20);
        assert_eq!(solid["NetUsed"], 10);
    }
}

#[tokio::test]
async fn confirmed_transactions_and_receipts_work_without_tx_id_history_index() {
    let (state, ids) = fixture();
    assert!(state.tx_history.is_none());
    let app = tron_rpc::http_rest::router(state);
    for (id, height) in ids.iter().zip([11, 12]) {
        let request = json!({"value":id});
        let latest = call(
            app.clone(),
            "/wallet/gettransactioninfobyid",
            request.clone(),
        )
        .await;
        assert_eq!(latest["blockNumber"], height);
        let solid = call(
            app.clone(),
            "/walletsolidity/gettransactioninfobyid",
            request.clone(),
        )
        .await;
        let transaction = call(
            app.clone(),
            "/walletsolidity/gettransactionbyid",
            request.clone(),
        )
        .await;
        if height == 11 {
            assert_eq!(solid, latest);
            assert_eq!(transaction["txID"], id.as_str());
        } else {
            assert_eq!(solid, json!({}));
            assert_eq!(transaction, json!({}));
            assert_eq!(
                call(app.clone(), "/wallet/gettransactionbyid", request).await["txID"],
                id.as_str()
            );
        }
    }
}

#[tokio::test]
async fn missing_archive_or_missing_coverage_is_explicit_not_latest_balance() {
    let (state, _) = fixture();
    for archive in [None, state.archive.clone()] {
        let mut changed = state.clone();
        changed.archive = archive;
        if changed.archive.is_some() {
            changed.dyn_props.save_latest_solidified_block_num(3);
        }
        let v = call(
            tron_rpc::http_rest::router(changed),
            "/walletsolidity/getaccount",
            json!({"address":hex::encode(address(0xaa))}),
        )
        .await;
        assert!(
            v["Error"]
                .as_str()
                .unwrap()
                .contains("solidified state unavailable"),
            "{v}"
        );
        assert!(v.get("balance").is_none());
    }
}

#[tokio::test]
async fn unset_solid_pointer_does_not_fall_back_to_latest() {
    let (state, _) = fixture();
    // A new dyn-props store has no solid pointer. Block 12 still exists.
    let empty = RpcState::new(mem(), mem(), mem(), mem(), mem(), CHAIN_ID);
    assert!(
        tron_rpc::methods::get_now_block_solidity(&json!([]), &empty)
            .unwrap()
            .is_null()
    );
    state.dyn_props.save_latest_solidified_block_num(0);
    let v = get(
        tron_rpc::http_rest::router(state),
        "/walletsolidity/getnowblock",
    )
    .await;
    assert_eq!(v, json!({}));
}

#[tokio::test]
async fn wallet_read_build_and_chain_identity_request_shapes_are_coherent() {
    let (state, _) = fixture();
    let app = tron_rpc::http_rest::router(state.clone());
    let owner = tron_crypto::base58check::encode_address(&Address::from_raw(address(0xaa)));
    let receiver = tron_crypto::base58check::encode_address(&Address::from_raw(address(0xbb)));
    let response = call(
        app.clone(),
        "/wallet/createtransaction",
        json!({
            "owner_address":owner,"to_address":receiver,"amount":1,"visible":true
        }),
    )
    .await;
    assert!(response.get("Error").is_none(), "{response}");
    assert_eq!(response["raw_data"]["ref_block_bytes"], "000c");
    assert_eq!(
        response["raw_data"]["contract"][0]["parameter"]["value"]["owner_address"],
        owner
    );
    assert_eq!(
        response["raw_data"]["contract"][0]["parameter"]["value"]["to_address"],
        receiver
    );
    assert!(response["raw_data_hex"]
        .as_str()
        .unwrap()
        .chars()
        .all(|c| c.is_ascii_hexdigit()));
    assert!(response.get("signature").is_none());
    let rpc = tron_rpc::server::router(state);
    for (method, expected) in [
        ("eth_chainId", json!("0x12345678")),
        ("net_version", json!(CHAIN_ID.to_string())),
    ] {
        let v = call(
            rpc.clone(),
            "/",
            json!({"jsonrpc":"2.0","id":1,"method":method,"params":[]}),
        )
        .await;
        assert_eq!(v["result"], expected);
    }
    let _ = call(app, "/wallet/getchainparameters", json!({})).await;
}

#[tokio::test]
async fn confirmed_delegation_uses_archived_target_and_visible_http_addresses() {
    let (state, _) = fixture();
    let app = tron_rpc::http_rest::router(state);
    let from = tron_crypto::base58check::encode_address(&Address::from_raw(address(0xaa)));
    let to = tron_crypto::base58check::encode_address(&Address::from_raw(address(0xbb)));
    let body = json!({"fromAddress":from,"toAddress":to,"visible":true});
    let latest = call(app.clone(), "/wallet/getdelegatedresourcev2", body.clone()).await;
    let solid = call(app, "/walletsolidity/getdelegatedresourcev2", body).await;
    assert_eq!(
        latest["delegatedResource"][0]["frozenBalanceForEnergy"],
        200
    );
    assert_eq!(solid["delegatedResource"][0]["frozenBalanceForEnergy"], 100);
    assert_eq!(solid["delegatedResource"][0]["from"], from);
    assert_eq!(solid["delegatedResource"][0]["to"], to);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confirmed_account_waits_for_apply_and_archive_capture_before_live_fallback() {
    use std::sync::{mpsc, Mutex};
    use std::time::Duration;
    let accounts = mem();
    let dp = mem();
    let props = DynamicPropertiesStore::new(dp.clone());
    props.save_latest_block_header_number(12);
    props.save_latest_solidified_block_num(11);
    let backends = vec![
        (UndoStoreId::Accounts, accounts.clone()),
        (UndoStoreId::DynProps, dp.clone()),
    ];
    let writer = ArchiveWriter::new(mem(), Some(BlockUndoStore::new(mem())), backends.clone());
    writer.check_or_init().unwrap();
    writer.on_block_applied(11, Some(&[])).unwrap();
    writer.on_block_applied(12, Some(&[])).unwrap();
    let reader = writer.reader();
    let gate = Arc::new(Mutex::new(()));
    let (staged_tx, staged_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (attempt_tx, attempt_rx) = mpsc::channel();
    let lock = gate.clone();
    let live = accounts.clone();
    let pending = address(0xcc);
    // This first-ever account write has no archive row until block 13 capture.
    // A lock-free archive fallback would wrongly return its unconfirmed balance.
    let applying = std::thread::spawn(move || {
        let _apply = lock.lock().unwrap();
        let bytes = Account {
            address: pending.to_vec(),
            balance: 999,
            ..Default::default()
        }
        .encode_to_vec();
        live.put(&pending, &bytes).unwrap();
        staged_tx.send(()).unwrap();
        release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        writer
            .on_block_applied(
                13,
                Some(&[DeltaRef {
                    store: UndoStoreId::Accounts,
                    key: &pending,
                    before: None,
                    after: Some(&bytes),
                }]),
            )
            .unwrap();
    });
    staged_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let state = RpcState::new(accounts, mem(), mem(), mem(), dp, CHAIN_ID)
        .with_archive(ArchiveApiState::new(reader, backends))
        .with_committed_read_gate(Arc::new(move |read| {
            attempt_tx.send(()).unwrap();
            let _committed = gate.lock().unwrap();
            read()
        }));
    let app = tron_rpc::http_rest::router(state);
    let request = tokio::spawn(call(
        app,
        "/walletsolidity/getaccount",
        json!({"address":hex::encode(pending)}),
    ));
    tokio::task::spawn_blocking(move || attempt_rx.recv_timeout(Duration::from_secs(5)))
        .await
        .unwrap()
        .unwrap();
    let blocked = !request.is_finished();
    release_tx.send(()).unwrap();
    let value = tokio::time::timeout(Duration::from_secs(5), request)
        .await
        .unwrap()
        .unwrap();
    applying.join().unwrap();
    assert!(
        blocked,
        "confirmed RPC must wait for the canonical apply critical section"
    );
    assert!(
        value.is_null() || value == json!({}),
        "new account did not exist at solid 11: {value}"
    );
}

#[tokio::test]
async fn partial_archive_store_set_cannot_silently_fall_back_to_live_dyn_props() {
    let accounts = mem();
    let dp = mem();
    let writer = ArchiveWriter::new(
        mem(),
        Some(BlockUndoStore::new(mem())),
        vec![(UndoStoreId::Accounts, accounts.clone())],
    );
    writer.check_or_init().unwrap();
    writer.on_block_applied(1, Some(&[])).unwrap();
    DynamicPropertiesStore::new(dp.clone()).save_latest_solidified_block_num(1);
    let state = RpcState::new(accounts.clone(), mem(), mem(), mem(), dp, CHAIN_ID).with_archive(
        ArchiveApiState::new(writer.reader(), vec![(UndoStoreId::Accounts, accounts)]),
    );
    let value = call(
        tron_rpc::http_rest::router(state),
        "/walletsolidity/getaccount",
        json!({"address":hex::encode(address(0xaa))}),
    )
    .await;
    assert!(
        value["Error"].as_str().unwrap().contains("DynProps"),
        "{value}"
    );
}

#[tokio::test]
async fn confirmed_accounts_refuse_archive_capture_lag_after_later_commit() {
    let (state, _) = fixture();
    state.dyn_props.save_latest_block_header_number(13);
    let value = call(
        tron_rpc::http_rest::router(state),
        "/walletsolidity/getaccount",
        json!({"address":hex::encode(address(0xaa))}),
    )
    .await;
    assert!(
        value["Error"]
            .as_str()
            .unwrap()
            .contains("capture is behind"),
        "{value}"
    );
    assert!(value.get("balance").is_none());
}

#[tokio::test]
async fn missing_wallet_http_records_use_empty_java_objects_not_json_rpc_null() {
    let (state, _) = fixture();
    let http = tron_rpc::http_rest::router(state.clone());
    for prefix in ["wallet", "walletsolidity"] {
        let account = call(
            http.clone(),
            &format!("/{prefix}/getaccount"),
            json!({"address": hex::encode(address(0xcc))}),
        )
        .await;
        assert_eq!(
            account,
            json!({}),
            "{prefix}: absent account is an empty protobuf object"
        );
        for method in ["gettransactionbyid", "gettransactioninfobyid"] {
            let value = call(
                http.clone(),
                &format!("/{prefix}/{method}"),
                json!({"value": "ff".repeat(32)}),
            )
            .await;
            assert_eq!(value, json!({}), "{prefix}/{method}");
        }
    }
    let rpc = tron_rpc::server::router(state);
    let value = call(
        rpc,
        "/",
        json!({"jsonrpc":"2.0","id":1,
        "method":"getAccount","params":[format!("0x{}", "cc".repeat(20))]}),
    )
    .await;
    assert!(
        value["result"].is_null(),
        "JSON-RPC retains its normal missing-record null"
    );
}
