use super::*;
use crate::{
    firehose::FirehoseWriter,
    index_hook::IndexHook,
    storage::{OpenedStores, SnapshotStack},
};
use std::path::Path;
use tron_chainbase::{BlockUndoRecord, MemBackend, UndoEntry};
use tron_grpc::assist_proto::Delta;

fn stores() -> OpenedStores {
    fn mem() -> Arc<dyn KvBackend> {
        Arc::new(MemBackend::new())
    }
    OpenedStores {
        accounts: mem(),
        witnesses: mem(),
        votes: mem(),
        delegation: mem(),
        delegated_resources: mem(),
        dyn_props: mem(),
        proposals: mem(),
        name_index: mem(),
        id_index: mem(),
        asset_v1: mem(),
        asset_v2: mem(),
        account_asset: mem(),
        contracts: mem(),
        abi: mem(),
        exchange_v1: mem(),
        exchange_v2: mem(),
        market_orders: mem(),
        nullifiers: mem(),
        merkle_trees: mem(),
        code: mem(),
        storage_row: mem(),
        contract_state: mem(),
        block_index: mem(),
        blocks: mem(),
        transactions: mem(),
        tx_history: mem(),
        transaction_ret: mem(),
        delegated_resource_account_index: mem(),
        market_account: mem(),
        market_pair_to_price: mem(),
        market_pair_price_to_order: mem(),
        balance_trace: mem(),
        witness_schedule: mem(),
        reward_vi: mem(),
        block_undo: mem(),
        pbft_sign_data: mem(),
        common_database: mem(),
        common: mem(),
        mempool: mem(),
        snapshots: SnapshotStack::empty(),
    }
}
fn block(height: i64, parent: &[u8]) -> Block {
    Block {
        block_header: Some(tron_proto::BlockHeader {
            raw_data: Some(tron_proto::block_header::Raw {
                number: height,
                parent_hash: parent.to_vec(),
                timestamp: 1_700_000_000_000 + height * 3000,
                ..Default::default()
            }),
            ..Default::default()
        }),
        transactions: vec![],
    }
}
fn config() -> AssistConfig {
    AssistConfig {
        enable: true,
        node_id: "receiver".into(),
        certificate_file: "cert".into(),
        private_key_file: "key".into(),
        client_ca_file: "ca".into(),
        upstream_peer: Some("donor".into()),
        start_assisted: true,
        cooldown_seconds: 0,
        peers: vec![super::super::AssistPeer {
            id: "donor".into(),
            certificate_sha256: "11".repeat(32),
            allow_receive: true,
            ..Default::default()
        }],
        ..Default::default()
    }
}
fn rig(path: &Path, cfg: AssistConfig) -> Arc<AssistRuntime> {
    let s = stores();
    let genesis = block(0, &[0; 32]);
    let id = block_id_from_block(&genesis).unwrap();
    BlockIndexStore::new(s.block_index.clone())
        .put(&id)
        .unwrap();
    s.blocks
        .put(id.as_bytes(), &genesis.encode_to_vec())
        .unwrap();
    let dp = DynamicPropertiesStore::new(s.dyn_props.clone());
    dp.save_latest_block_header_number(0);
    dp.save_latest_block_header_hash(id.as_bytes());
    dp.save_latest_solidified_block_num(0);
    let fire = Arc::new(
        FirehoseWriter::open(
            path.join("firehose"),
            1 << 24,
            s.blocks.clone(),
            s.block_index.clone(),
            s.transaction_ret.clone(),
            s.dyn_props.clone(),
        )
        .unwrap(),
    );
    let hook = Arc::new(IndexHook::new(s.transaction_ret.clone()).with_firehose(fire));
    Arc::new(
        AssistRuntime::open(
            cfg,
            path,
            s,
            hook,
            Arc::new(crate::sync::SyncLeadership::new()),
            Arc::new(tron_consensus::KhaosDb::new()),
        )
        .unwrap(),
    )
}
fn bundle(rt: &AssistRuntime) -> wire::BlockBundle {
    let (head, parent) = rt.head().unwrap();
    let height = head + 1;
    let b = block(height, &parent);
    let id = block_id_from_block(&b).unwrap().as_bytes().to_vec();
    let mut deltas = vec![
        Delta {
            store: UndoStoreId::DynProps as u32,
            key: b"latest_block_header_number".to_vec(),
            before: Some(head.to_be_bytes().to_vec()),
            after: Some(height.to_be_bytes().to_vec()),
        },
        Delta {
            store: UndoStoreId::DynProps as u32,
            key: b"latest_block_header_hash".to_vec(),
            before: Some(parent.clone()),
            after: Some(id.clone()),
        },
        Delta {
            store: UndoStoreId::StorageRow as u32,
            key: b"sample".to_vec(),
            before: rt.stores.storage_row.get(b"sample").unwrap(),
            after: Some(height.to_be_bytes().to_vec()),
        },
    ];
    deltas.sort_by(|a, b| (a.store, &a.key).cmp(&(b.store, &b.key)));
    let undo = BlockUndoRecord {
        entries: deltas
            .iter()
            .map(|d| UndoEntry {
                store: UndoStoreId::from_u8(d.store as u8).unwrap(),
                key: d.key.clone(),
                before: d.before.clone(),
            })
            .collect(),
    };
    let previous_digest = rt
        .journal
        .lock()
        .unwrap()
        .parent_digest(height, &parent)
        .unwrap();
    let mut out = wire::BlockBundle {
        format: journal::FORMAT,
        network: rt.network.clone(),
        profile: rt.profile.clone(),
        height,
        block_id: id.clone(),
        parent_id: parent.clone(),
        previous_digest,
        deltas,
        raw_block: b.encode_to_vec(),
        transaction_ret: tron_proto::TransactionRet::default().encode_to_vec(),
        firehose_apply: fh::BlockApplied {
            height,
            block_id: id,
            parent_id: parent,
            trx_balances_complete: true,
            ..Default::default()
        }
        .encode_to_vec(),
        undo: undo.encode(),
        ..Default::default()
    };
    out.digest = journal::block_digest(&out);
    out
}
#[test]
fn import_redo_duplicate_ack_and_independent_event_sequence() {
    let temp = tempfile::tempdir().unwrap();
    let r = rig(temp.path(), config());
    let first = bundle(&r);
    let ack = r.import(&first).unwrap();
    assert_eq!(ack.height, 1);
    assert_eq!(r.head().unwrap().0, 1);
    assert_eq!(
        r.stores.storage_row.get(b"sample").unwrap(),
        Some(1i64.to_be_bytes().to_vec())
    );
    assert_eq!(r.import(&first).unwrap(), ack);
    assert_eq!(r.cursor.lock().unwrap().imported_blocks, 1);
    let second = bundle(&r);
    journal::atomic_write(&r.root.join("incoming.pb"), &second.encode_to_vec()).unwrap();
    // Simulate death after the head store committed but BEFORE other stores.
    let dp = DynamicPropertiesStore::new(r.stores.dyn_props.clone());
    dp.save_latest_block_header_number(2);
    dp.save_latest_block_header_hash(&second.block_id.clone().try_into().unwrap());
    r.recover().unwrap();
    assert_eq!(
        r.stores.storage_row.get(b"sample").unwrap(),
        Some(2i64.to_be_bytes().to_vec())
    );
    assert_eq!(r.cursor.lock().unwrap().imported_blocks, 2);
    assert!(!r.has_pending_import());
    assert_eq!(r.import(&second).unwrap().height, 2);
    let corrupt = wire::BlockBundle {
        raw_block: vec![1, 2, 3],
        ..second
    };
    assert!(r.import(&corrupt).is_err());
}
#[test]
fn import_failure_never_advances_cursor_and_recovery_finishes_event() {
    let temp = tempfile::tempdir().unwrap();
    let r = rig(temp.path(), config());
    let mut bad = bundle(&r);
    let mut event = fh::BlockApplied::decode(bad.firehose_apply.as_slice()).unwrap();
    event.trx_balances_complete = false;
    bad.firehose_apply = event.encode_to_vec();
    bad.digest = journal::block_digest(&bad);
    assert!(r.import(&bad).is_err());
    assert_eq!(r.head().unwrap().0, 0);
    assert_eq!(r.cursor.lock().unwrap().height, 0);
    assert!(!r.has_pending_import());
    let good = bundle(&r);
    journal::atomic_write(&r.root.join("incoming.pb"), &good.encode_to_vec()).unwrap();
    r.recover().unwrap();
    assert_eq!(r.cursor.lock().unwrap().height, 1);
    assert_eq!(r.local_ack(1).unwrap().unwrap().digest, good.digest);
}
#[test]
fn automatic_entry_and_handoff_use_solid_barrier() {
    let temp = tempfile::tempdir().unwrap();
    let r = rig(temp.path(), config());
    r.switch_mode(false).unwrap();
    let status = wire::Status {
        head: 10,
        solid: 6,
        solid_id: vec![1; 32],
        ..Default::default()
    };
    assert!(!r.observe_status(&status).unwrap());
    assert!(!r.observe_status(&status).unwrap());
    assert!(r.observe_status(&status).unwrap());
    let b = bundle(&r);
    r.import(&b).unwrap();
    let caught = wire::Status {
        head: 7,
        solid: 1,
        solid_id: b.block_id,
        ..Default::default()
    };
    assert!(!r.observe_status(&caught).unwrap());
}
#[test]
fn reversible_suffix_rewinds_events_and_finality_conflict_is_refused() {
    let temp = tempfile::tempdir().unwrap();
    let r = rig(temp.path(), config());
    let first = bundle(&r);
    r.import(&first).unwrap();
    let second = bundle(&r);
    r.import(&second).unwrap();
    // Model an autonomously executed (not finalized) second block.
    DynamicPropertiesStore::new(r.stores.dyn_props.clone()).save_latest_solidified_block_num(1);
    assert!(!r.align_anchor(2, &[9; 32]).unwrap());
    assert!(r.align_anchor(1, &first.block_id).unwrap());
    assert_eq!(r.head().unwrap().0, 1);
    assert_eq!(
        r.stores.storage_row.get(b"sample").unwrap(),
        Some(1i64.to_be_bytes().to_vec())
    );
    assert!(r
        .align_anchor(1, &[9; 32])
        .unwrap_err()
        .contains("SOLID_CONFLICT"));
    // The replacement block has the same state changes and must reuse no
    // donor event sequence; this node's log emits UNWIND then APPLY.
    r.import(&second).unwrap();
    assert_eq!(r.head().unwrap().0, 2);
}
#[test]
fn journal_ack_retention_restart_and_corruption() {
    let temp = tempfile::tempdir().unwrap();
    let r = rig(temp.path(), config());
    let mut blocks = Vec::new();
    for _ in 0..4 {
        let b = bundle(&r);
        r.import(&b).unwrap();
        blocks.push(b);
    }
    let root = r.journal.lock().unwrap().root().to_path_buf();
    let mut cfg = config();
    cfg.retain_blocks = 1;
    cfg.peers = vec![super::super::AssistPeer {
        id: "slow".into(),
        allow_donate: true,
        required_for_retention: true,
        ..Default::default()
    }];
    let mut journal = Journal::open(
        root.clone(),
        cfg.clone(),
        r.network.clone(),
        r.profile.clone(),
        0,
        r.network.clone(),
    )
    .unwrap();
    assert_eq!(journal.prune(4).unwrap(), 0);
    let ack = wire::Ack {
        height: 3,
        block_id: blocks[2].block_id.clone(),
        digest: blocks[2].digest.clone(),
        semantic_digest: Vec::new(),
    };
    assert!(journal.acknowledge("slow", ack.clone()).unwrap());
    assert!(!journal.acknowledge("slow", ack.clone()).unwrap());
    assert!(journal.acknowledge("stranger", ack.clone()).is_err());
    assert_eq!(journal.prune(4).unwrap(), 2);
    drop(journal);
    let mut journal = Journal::open(
        root,
        cfg,
        r.network.clone(),
        r.profile.clone(),
        0,
        r.network.clone(),
    )
    .unwrap();
    assert_eq!(journal.acks["slow"], ack);
    assert_eq!(journal.meta.floor, 3);
    assert_eq!(journal.prune(4).unwrap(), 0);
    let mut wrong = ack;
    wrong.digest = vec![0; 32];
    assert!(journal.acknowledge("slow", wrong).is_err());
}

fn certificate_set(
    root: &Path,
) -> (
    std::path::PathBuf,
    Vec<(std::path::PathBuf, std::path::PathBuf, String)>,
) {
    use std::process::{Command, Stdio};
    fn openssl(root: &Path, args: &[&str]) {
        assert!(Command::new("/usr/bin/openssl")
            .current_dir(root)
            .args(args)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap()
            .success());
    }
    openssl(
        root,
        &[
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            "ca.key",
            "-out",
            "ca.pem",
            "-days",
            "1",
            "-subj",
            "/CN=Assist-test-CA",
            "-addext",
            "basicConstraints=critical,CA:TRUE",
        ],
    );
    fs::write(root.join("ext"),"subjectAltName=DNS:localhost\nbasicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth,clientAuth\n").unwrap();
    let mut identities = vec![];
    for name in ["donor", "receiver", "outsider"] {
        let key = format!("{name}.key");
        let csr = format!("{name}.csr");
        let cert = format!("{name}.pem");
        openssl(
            root,
            &[
                "req",
                "-new",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-keyout",
                &key,
                "-out",
                &csr,
                "-subj",
                &format!("/CN={name}"),
            ],
        );
        openssl(
            root,
            &[
                "x509",
                "-req",
                "-in",
                &csr,
                "-CA",
                "ca.pem",
                "-CAkey",
                "ca.key",
                "-CAcreateserial",
                "-out",
                &cert,
                "-days",
                "1",
                "-extfile",
                "ext",
            ],
        );
        let der = Command::new("/usr/bin/openssl")
            .current_dir(root)
            .args(["x509", "-in", &cert, "-outform", "DER"])
            .output()
            .unwrap();
        assert!(der.status.success());
        identities.push((
            root.join(cert),
            root.join(key),
            hex::encode(Sha256::digest(der.stdout)),
        ));
    }
    (root.join("ca.pem"), identities)
}
fn port() -> std::net::SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}
async fn encrypted_pair(donor_dials: bool) {
    let temp = tempfile::tempdir().unwrap();
    let (ca, ids) = certificate_set(temp.path());
    let donor_address = port();
    let receiver_address = port();
    let peer = |id: &str, n: usize, donate: bool| super::super::AssistPeer {
        id: id.into(),
        certificate_sha256: ids[n].2.clone(),
        allow_donate: donate,
        allow_receive: !donate,
        tls_domain: "localhost".into(),
        server_ca_file: ca.clone(),
        ..Default::default()
    };
    let mut donor_cfg = AssistConfig {
        enable: true,
        node_id: "donor".into(),
        listen: Some(donor_address),
        certificate_file: ids[0].0.clone(),
        private_key_file: ids[0].1.clone(),
        client_ca_file: ca.clone(),
        peers: vec![peer("receiver", 1, true)],
        cooldown_seconds: 0,
        ..Default::default()
    };
    let mut receiver_cfg = AssistConfig {
        enable: true,
        node_id: "receiver".into(),
        listen: Some(receiver_address),
        certificate_file: ids[1].0.clone(),
        private_key_file: ids[1].1.clone(),
        client_ca_file: ca.clone(),
        peers: vec![peer("donor", 0, false)],
        upstream_peer: Some("donor".into()),
        start_assisted: true,
        cooldown_seconds: 0,
        ..Default::default()
    };
    if donor_dials {
        donor_cfg.peers[0].endpoint = Some(format!("https://{receiver_address}"));
    } else {
        receiver_cfg.peers[0].endpoint = Some(format!("https://{donor_address}"));
    }
    let donor = rig(&temp.path().join("d"), donor_cfg);
    donor.switch_mode(true).unwrap();
    for _ in 0..3 {
        let b = bundle(&donor);
        donor.import(&b).unwrap();
    }
    donor.switch_mode(false).unwrap();
    let receiver = rig(&temp.path().join("r"), receiver_cfg);
    let stop = crate::ShutdownSignal::new();
    let mut tasks = super::super::start(donor.clone(), stop.clone())
        .await
        .unwrap();
    tasks.extend(
        super::super::start(receiver.clone(), stop.clone())
            .await
            .unwrap(),
    );
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            if receiver.head().unwrap().0 == 3
                && !receiver.is_assisted()
                && donor
                    .journal
                    .lock()
                    .unwrap()
                    .acks
                    .get("receiver")
                    .is_some_and(|a| a.height == 3)
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("authenticated pair must catch up and hand off in either dial direction");
    assert_eq!(
        receiver.stores.storage_row.get(b"sample").unwrap(),
        donor.stores.storage_row.get(b"sample").unwrap()
    );
    let events = tron_index::FirehoseLogReader::new(temp.path().join("r/firehose"))
        .read_from(1, 100)
        .unwrap();
    assert_eq!(events.len(), 3);
    for (i, (seq, bytes)) in events.iter().enumerate() {
        let entry = fh::Entry::decode(bytes.as_slice()).unwrap();
        assert_eq!(*seq, i as u64 + 1);
        let Some(fh::entry::Event::Apply(a)) = entry.event else {
            panic!("expected APPLY")
        };
        assert_eq!(a.height, i as i64 + 1);
        assert_eq!(a.solidified_height, a.height);
    }
    // A later lag must re-enter over the SAME stream after a completed
    // handoff. This also exercises resetting the outstanding-fetch state.
    donor.switch_mode(true).unwrap();
    for _ in 0..6 {
        let next = bundle(&donor);
        donor.import(&next).unwrap();
    }
    donor.switch_mode(false).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            if receiver.head().unwrap().0 == 9
                && !receiver.is_assisted()
                && donor
                    .journal
                    .lock()
                    .unwrap()
                    .acks
                    .get("receiver")
                    .is_some_and(|a| a.height == 9)
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("automatic lag re-entry must resume fetching after handoff");
    assert_eq!(receiver.cursor.lock().unwrap().imported_blocks, 9);

    // A CA-signed but unlisted client still cannot open the replication RPC.
    use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};
    let channel = Endpoint::from_shared(format!("https://{donor_address}"))
        .unwrap()
        .tls_config(
            ClientTlsConfig::new()
                .domain_name("localhost")
                .ca_certificate(Certificate::from_pem(fs::read(&ca).unwrap()))
                .identity(Identity::from_pem(
                    fs::read(&ids[2].0).unwrap(),
                    fs::read(&ids[2].1).unwrap(),
                )),
        )
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = wire::assisted_sync_client::AssistedSyncClient::new(channel);
    let error = client
        .link(tokio_stream::iter(vec![wire::Frame {
            message: Some(wire::frame::Message::Hello(wire::Hello {
                node_id: "receiver".into(),
                network: donor.network.clone(),
                profile: donor.profile.clone(),
                wants_donor: true,
                trusted_cluster: false,
            })),
        }]))
        .await
        .err()
        .expect("unknown client must be denied despite valid CA");
    assert_eq!(error.code(), tonic::Code::PermissionDenied);
    stop.shutdown();
    for task in tasks {
        tokio::time::timeout(std::time::Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap();
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mtls_receiver_dials_donor_and_unknown_certificate_is_denied() {
    encrypted_pair(false).await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mtls_donor_dials_receiver_and_unknown_certificate_is_denied() {
    encrypted_pair(true).await
}

#[test]
fn precommit_observer_failure_aborts_state_and_capture_matches_normal_commit() {
    use std::sync::atomic::AtomicBool;
    use tron_executor::{BlockCommitObserver, BlockExecutionReport, ExecConfig};
    struct Reject(AtomicBool);
    impl BlockCommitObserver for Reject {
        fn prepare(&self, _: &Block, _: &BlockExecutionReport, _: &BlockUndoRecord) -> Result<()> {
            self.0.store(true, Ordering::Relaxed);
            Err("injected journal disk failure".into())
        }
    }
    let temp = tempfile::tempdir().unwrap();
    let r = rig(&temp.path().join("observed"), config());
    let (_, parent) = r.head().unwrap();
    let b = block(1, &parent);
    let state = r.stores.to_state_backends();
    let undo = tron_chainbase::BlockUndoStore::new(r.stores.block_undo.clone());
    let checkpoint = tron_chainbase::CheckPointV2::new(&temp.path().join("cp"));
    let cfg = ExecConfig {
        capture_state_deltas: true,
        verify_tx_trie: false,
        ..ExecConfig::unsigned()
    };
    let reject = Reject(AtomicBool::new(false));
    let result = tron_executor::execute_block_with_observer(
        &state,
        &b,
        None,
        &undo,
        &checkpoint,
        &cfg,
        None,
        &reject,
    );
    assert!(result.is_err());
    assert!(
        reject.0.load(Ordering::Relaxed),
        "must reach durable journal boundary"
    );
    assert_eq!(r.head().unwrap().0, 0);
    assert!(undo.get(1).unwrap().is_none());
    let report = tron_executor::execute_block_with_observer(
        &state,
        &b,
        None,
        &undo,
        &checkpoint,
        &cfg,
        None,
        r.as_ref(),
    )
    .unwrap();
    assert_eq!(r.head().unwrap().0, 1);
    let normal = rig(&temp.path().join("normal"), config());
    let normal_state = normal.stores.to_state_backends();
    let normal_undo = tron_chainbase::BlockUndoStore::new(normal.stores.block_undo.clone());
    tron_executor::execute_block_with_undo_checkpoint_and_config(
        &normal_state,
        &b,
        None,
        &normal_undo,
        &checkpoint,
        &cfg,
        None,
    )
    .unwrap();
    for delta in report.state_deltas.as_ref().unwrap() {
        let observed = state_backend(&r.stores, delta.store)
            .get(&delta.key)
            .unwrap();
        assert_eq!(
            observed,
            state_backend(&normal.stores, delta.store)
                .get(&delta.key)
                .unwrap()
        );
        assert_eq!(observed, delta.after);
    }
    assert!(r
        .journal
        .lock()
        .unwrap()
        .read(1, report.block_id.as_bytes())
        .unwrap()
        .is_some());
}

#[test]
fn source_activation_anchor_can_move_only_before_finality_and_ack() {
    let temp = tempfile::tempdir().unwrap();
    let r = rig(temp.path(), config());
    let root = temp.path().join("activation");
    let mut old = vec![7; 32];
    old[..8].copy_from_slice(&10u64.to_be_bytes());
    let mut new = vec![8; 32];
    new[..8].copy_from_slice(&10u64.to_be_bytes());
    let mut j = Journal::open(
        root,
        config(),
        r.network.clone(),
        r.profile.clone(),
        10,
        old,
    )
    .unwrap();
    assert_eq!(j.source_parent(10, &[0; 32], 9).unwrap(), None);
    assert_eq!(j.source_parent(11, &new, 9).unwrap(), Some(vec![0; 32]));
    assert_eq!(j.meta.anchor_id, new);
    let mut conflicting = new.clone();
    conflicting[10] = 99;
    assert!(j.source_parent(11, &conflicting, 11).is_err());
    j.acks.insert("accepted".into(), wire::Ack::default());
    assert!(j.source_parent(11, &conflicting, 9).is_err());
}

fn disk_rig(path: &Path, cfg: AssistConfig) -> Arc<AssistRuntime> {
    let s = OpenedStores::open_tuned(path, 2, 32).unwrap();
    if DynamicPropertiesStore::new(s.dyn_props.clone())
        .latest_block_header_number()
        .is_none()
    {
        let genesis = block(0, &[0; 32]);
        let id = block_id_from_block(&genesis).unwrap();
        BlockIndexStore::new(s.block_index.clone())
            .put(&id)
            .unwrap();
        s.blocks
            .put(id.as_bytes(), &genesis.encode_to_vec())
            .unwrap();
        let dp = DynamicPropertiesStore::new(s.dyn_props.clone());
        dp.save_latest_block_header_number(0);
        dp.save_latest_block_header_hash(id.as_bytes());
        dp.save_latest_solidified_block_num(0);
    }
    let fire = Arc::new(
        FirehoseWriter::open(
            path.join("firehose"),
            1 << 24,
            s.blocks.clone(),
            s.block_index.clone(),
            s.transaction_ret.clone(),
            s.dyn_props.clone(),
        )
        .unwrap(),
    );
    let hook = Arc::new(IndexHook::new(s.transaction_ret.clone()).with_firehose(fire));
    Arc::new(
        AssistRuntime::open(
            cfg,
            path,
            s,
            hook,
            Arc::new(crate::sync::SyncLeadership::new()),
            Arc::new(tron_consensus::KhaosDb::new()),
        )
        .unwrap(),
    )
}
#[test]
#[serial_test::serial]
fn bootstrap_checkpoint_resume_restart_install_and_post_snapshot_events() {
    use super::super::bootstrap::{Download, Manifest};
    let t = tempfile::tempdir().unwrap();
    let donor_path = t.path().join("donor");
    let receiver_path = t.path().join("receiver");
    let mut cfg = config();
    cfg.node_id = "donor".into();
    cfg.upstream_peer = None;
    cfg.start_assisted = false;
    cfg.peers[0].id = "receiver".into();
    cfg.peers[0].allow_donate = true;
    cfg.peers[0].allow_receive = false;
    let d = disk_rig(&donor_path, cfg.clone());
    let mut payload = vec![0u8; 9 * 1024 * 1024];
    use rand::RngCore;
    rand::thread_rng().fill_bytes(&mut payload);
    d.stores
        .storage_row
        .put(b"large-immutable", &payload)
        .unwrap();
    let mut download = Download::load_or_create(&receiver_path, "").unwrap();
    let open = wire::SnapshotOpen {
        request_id: download.request_id.clone(),
        replace_snapshot_id: String::new(),
    };
    let offered = d.snapshot_open("receiver", &open).unwrap();
    let m: Manifest = serde_json::from_slice(&offered.manifest_json).unwrap();
    assert!(offered.ready);
    assert_eq!(m.height, 0);
    assert!(d
        .snapshot_open("stranger", &open)
        .unwrap_err()
        .contains("SNAPSHOT_BUSY"));
    download
        .accept_manifest(&receiver_path, &offered, &d.network, cfg.max_snapshot_bytes)
        .unwrap();
    // Persist actual data first, then cursor. A restart must neither recreate the snapshot nor resend completed files.
    loop {
        let req = download.next_read().unwrap();
        let c = d.snapshot_read("receiver", &req).unwrap();
        download.accept_chunk(&receiver_path, &c).unwrap();
        if download.offset > 0 {
            break;
        }
    }
    assert!(
        download.offset > 0,
        "exercise a restart inside a multi-chunk file"
    );
    let saved_bytes = download.downloaded;
    drop(download);
    let mut download = Download::load_or_create(&receiver_path, "").unwrap();
    assert_eq!(download.downloaded, saved_bytes);
    d.switch_mode(true).unwrap();
    for _ in 0..5 {
        let b = bundle(&d);
        d.import(&b).unwrap();
    }
    d.switch_mode(false).unwrap();
    assert_eq!(
        d.journal.lock().unwrap().prune(10_000).unwrap(),
        0,
        "disconnected bootstrap pins B+1"
    );
    drop(d);
    let d = disk_rig(&donor_path, cfg);
    assert_eq!(
        d.snapshot_open("receiver", &open).unwrap().manifest_json,
        offered.manifest_json
    );
    // Corrupt wire bytes are rejected without advancing the durable cursor.
    let req = download.next_read().unwrap();
    let mut corrupt = d.snapshot_read("receiver", &req).unwrap();
    if corrupt.data.is_empty() {
        corrupt.sha256[0] ^= 1;
    } else {
        corrupt.data[0] ^= 1;
    }
    assert!(download.accept_chunk(&receiver_path, &corrupt).is_err());
    assert_eq!(download.downloaded, saved_bytes);
    while let Some(req) = download.next_read() {
        let c = d.snapshot_read("receiver", &req).unwrap();
        download.accept_chunk(&receiver_path, &c).unwrap();
    }
    // Emulate a crash after atomic DB activation but before receiver metadata/ACK.
    download.phase = "installing".into();
    download.save(&receiver_path).unwrap();
    fs::rename(
        download.stage(&receiver_path).join("database"),
        receiver_path.join("database"),
    )
    .unwrap();
    drop(download);
    let mut download = Download::load_or_create(&receiver_path, "").unwrap();
    let ack = download.install(&receiver_path).unwrap();
    assert_eq!(
        d.snapshot_installed("receiver", &ack).unwrap().snapshot_id,
        open.request_id
    );
    assert_eq!(
        d.snapshot_installed("receiver", &ack).unwrap().snapshot_id,
        open.request_id,
        "lost ACK response is idempotent"
    );
    let r = disk_rig(&receiver_path, config());
    assert_eq!(r.head().unwrap().0, 0);
    assert_eq!(
        r.stores.storage_row.get(b"large-immutable").unwrap(),
        Some(payload)
    );
    for h in 1..=5 {
        let b = d.export_next(&r.fetch_cursor().unwrap()).unwrap().unwrap();
        assert_eq!(b.height, h);
        let ack = r.import(&b).unwrap();
        d.accept_ack("receiver", ack).unwrap();
    }
    assert!(!r.observe_status(&d.status().unwrap()).unwrap());
    let events = tron_index::FirehoseLogReader::new(receiver_path.join("firehose"))
        .read_from(1, 100)
        .unwrap();
    assert_eq!(events.len(), 5);
    assert_eq!(
        r.stores.storage_row.get(b"sample").unwrap(),
        d.stores.storage_row.get(b"sample").unwrap()
    );
    // Explicit replacement captures the CURRENT head; replay of a retired create cannot replace it again.
    let newer = wire::SnapshotOpen {
        request_id: "reset-current".into(),
        replace_snapshot_id: open.request_id.clone(),
    };
    let state = d.snapshot_open("receiver", &newer).unwrap();
    let new_m: Manifest = serde_json::from_slice(&state.manifest_json).unwrap();
    assert_eq!(new_m.height, 5);
    assert!(d
        .snapshot_open("receiver", &open)
        .unwrap_err()
        .contains("SUPERSEDED"));
    let release = wire::SnapshotRelease {
        snapshot_id: newer.request_id.clone(),
    };
    assert!(d.snapshot_release("stranger", &release).is_err());
    d.snapshot_release("receiver", &release).unwrap();
    d.snapshot_release("receiver", &release).unwrap();
    assert_eq!(
        super::super::bootstrap::retention_pin(&donor_path.join("assisted-sync")).unwrap(),
        None
    );
    assert_eq!(
        fs::read_dir(donor_path.join("assisted-sync"))
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name() == "snapshot")
            .count(),
        0
    );
}
async fn empty_bootstrap_network(donor_dials: bool) {
    empty_bootstrap_network_mode(donor_dials, false).await
}
async fn empty_bootstrap_network_mode(donor_dials: bool, cluster: bool) {
    let t = tempfile::tempdir().unwrap();
    let (ca, ids) = certificate_set(t.path());
    let donor_address = port();
    let receiver_address = port();
    let mut dcfg = AssistConfig {
        enable: true,
        node_id: "donor".into(),
        listen: Some(donor_address),
        certificate_file: ids[0].0.clone(),
        private_key_file: ids[0].1.clone(),
        client_ca_file: ca.clone(),
        cooldown_seconds: 0,
        peers: vec![super::super::AssistPeer {
            id: "receiver".into(),
            certificate_sha256: ids[1].2.clone(),
            allow_donate: true,
            tls_domain: "localhost".into(),
            server_ca_file: ca.clone(),
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut rcfg = AssistConfig {
        enable: true,
        node_id: "receiver".into(),
        listen: Some(receiver_address),
        certificate_file: ids[1].0.clone(),
        private_key_file: ids[1].1.clone(),
        client_ca_file: ca.clone(),
        cooldown_seconds: 0,
        start_assisted: true,
        upstream_peer: Some("donor".into()),
        peers: vec![super::super::AssistPeer {
            id: "donor".into(),
            certificate_sha256: ids[0].2.clone(),
            allow_receive: true,
            tls_domain: "localhost".into(),
            server_ca_file: ca.clone(),
            ..Default::default()
        }],
        ..Default::default()
    };
    if cluster {
        dcfg.trusted_cluster = true;
        rcfg.trusted_cluster = true;
        rcfg.upstream_peer = None;
        rcfg.start_assisted = false;
        dcfg.enter_after_checks = 1;
        rcfg.enter_after_checks = 1;
        dcfg.enter_after_blocks = 1;
        rcfg.enter_after_blocks = 1;
        dcfg.peers[0].allow_donate = false;
        rcfg.peers[0].allow_receive = false;
    }
    if donor_dials {
        dcfg.peers[0].endpoint = Some(format!("https://{receiver_address}"));
    } else {
        rcfg.peers[0].endpoint = Some(format!("https://{donor_address}"));
    }
    let d = disk_rig(&t.path().join("d"), dcfg);
    let stop = crate::ShutdownSignal::new();
    let mut tasks = super::super::start(d.clone(), stop.clone()).await.unwrap();
    let mut node = crate::NodeConfig::default();
    node.data_dir = t.path().join("empty");
    node.assist = rcfg.clone();
    node.assist.bootstrap_genesis = Some(hex::encode(&d.network));
    assert!(
        !node.data_dir.exists(),
        "receiver starts with no state at all"
    );
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        super::super::receive_bootstrap(&node, stop.clone()),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(node.data_dir.join("database/properties/CURRENT").exists());
    let r = disk_rig(&node.data_dir, rcfg);
    if !cluster {
        tasks.extend(super::super::start(r.clone(), stop.clone()).await.unwrap());
    }
    // Fast private-chain writes continue after snapshot handoff.
    d.switch_mode(true).unwrap();
    for _ in 0..12 {
        let b = bundle(&d);
        d.import(&b).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    d.switch_mode(false).unwrap();
    if cluster {
        tasks.extend(super::super::start(r.clone(), stop.clone()).await.unwrap());
    }
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            if r.head().unwrap().0 == 12 && !r.is_assisted() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("empty bootstrap must switch to ordinary catch-up and autonomous leadership");
    assert_eq!(
        r.stores.storage_row.get(b"sample").unwrap(),
        d.stores.storage_row.get(b"sample").unwrap()
    );
    stop.shutdown();
    for task in tasks {
        tokio::time::timeout(std::time::Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap();
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial]
async fn bootstrap_empty_receiver_dials_and_then_imports_events() {
    empty_bootstrap_network(false).await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial]
async fn bootstrap_empty_receiver_accepts_donor_dial_and_then_imports_events() {
    empty_bootstrap_network(true).await
}

fn disk_donor_config() -> AssistConfig {
    let mut c = config();
    c.node_id = "donor".into();
    c.upstream_peer = None;
    c.start_assisted = false;
    c.peers[0].id = "receiver".into();
    c.peers[0].allow_donate = true;
    c.peers[0].allow_receive = false;
    c
}
#[test]
#[ignore = "subprocess entry point for abrupt bootstrap crash recovery"]
fn bootstrap_crash_child() {
    use super::super::bootstrap::Download;
    let source = std::path::PathBuf::from(std::env::var("TRON_BOOTSTRAP_TEST_SOURCE").unwrap());
    let target = std::path::PathBuf::from(std::env::var("TRON_BOOTSTRAP_TEST_TARGET").unwrap());
    let d = disk_rig(&source, disk_donor_config());
    let mut download = Download::load_or_create(&target, "").unwrap();
    if download.phase != "complete" {
        let req = wire::SnapshotOpen {
            request_id: download.request_id.clone(),
            replace_snapshot_id: download.replace_snapshot_id.clone(),
        };
        let state = d.snapshot_open("receiver", &req).unwrap();
        assert!(state.ready);
        download
            .accept_manifest(&target, &state, &d.network, d.config.max_snapshot_bytes)
            .unwrap();
        while let Some(req) = download.next_read() {
            let chunk = d.snapshot_read("receiver", &req).unwrap();
            download.accept_chunk(&target, &chunk).unwrap();
        }
        let ack = download.install(&target).unwrap();
        d.snapshot_installed("receiver", &ack).unwrap();
        download.phase = "complete".into();
        download.save(&target).unwrap();
    }
    let r = disk_rig(&target, config());
    while r.head().unwrap().0 < d.head().unwrap().0 {
        let b = d.export_next(&r.fetch_cursor().unwrap()).unwrap().unwrap();
        let ack = r.import(&b).unwrap();
        d.accept_ack("receiver", ack).unwrap();
    }
    assert_eq!(r.head().unwrap(), d.head().unwrap());
    assert_eq!(
        r.stores.storage_row.get(b"sample").unwrap(),
        d.stores.storage_row.get(b"sample").unwrap()
    );
}
#[test]
#[serial_test::serial]
fn bootstrap_abrupt_process_exit_recovers_every_durable_boundary() {
    use std::process::{Command, Stdio};
    let t = tempfile::tempdir().unwrap();
    let source = t.path().join("source");
    let d = disk_rig(&source, disk_donor_config());
    d.stores
        .storage_row
        .put(b"seed", &vec![17; 128 * 1024])
        .unwrap();
    drop(d);
    let exe = std::env::current_exe().unwrap();
    for point in [
        "donor-pinned",
        "donor-checkpointed",
        "donor-ready",
        "chunk-data-synced",
        "chunk-cursor-synced",
        "receiver-install-intent",
        "receiver-database-activated",
        "receiver-base-cursor-synced",
        "receiver-installed",
        "donor-installed-ack",
    ] {
        let target = t.path().join(point);
        let command = || {
            let mut c = Command::new(&exe);
            c.args([
                "--exact",
                "assisted_sync::runtime::tests::bootstrap_crash_child",
                "--ignored",
                "--nocapture",
            ])
            .env("TRON_BOOTSTRAP_TEST_SOURCE", &source)
            .env("TRON_BOOTSTRAP_TEST_TARGET", &target)
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
            c
        };
        let crashed = command()
            .env("TRON_BOOTSTRAP_CRASH_AT", point)
            .output()
            .unwrap();
        assert_eq!(
            crashed.status.code(),
            Some(86),
            "crash boundary {point}: {}",
            String::from_utf8_lossy(&crashed.stderr)
        );
        // The healthy donor continues to grow between failure and receiver resume.
        let d = disk_rig(&source, disk_donor_config());
        d.switch_mode(true).unwrap();
        let b = bundle(&d);
        d.import(&b).unwrap();
        d.switch_mode(false).unwrap();
        drop(d);
        let resumed = command()
            .env_remove("TRON_BOOTSTRAP_CRASH_AT")
            .output()
            .unwrap();
        assert!(
            resumed.status.success(),
            "restart at {point}: {}",
            String::from_utf8_lossy(&resumed.stderr)
        );
    }
}

#[test]
fn independent_noop_captures_reenter_with_checked_logical_anchor() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    let mut cfg = config();
    cfg.peers[0].allow_donate = true;
    let donor = rig(a.path(), cfg.clone());
    let receiver = rig(b.path(), cfg);
    let first = bundle(&donor);
    donor.import(&first).unwrap();
    receiver.import(&first).unwrap();
    let local = bundle(&donor);
    let mut source = local.clone();
    source.deltas.push(Delta {
        store: UndoStoreId::Code as u32,
        key: b"cached-read".to_vec(),
        before: None,
        after: None,
    });
    source
        .deltas
        .sort_by(|a, b| (a.store, &a.key).cmp(&(b.store, &b.key)));
    let mut undo = BlockUndoRecord::decode(&source.undo).unwrap();
    undo.entries.push(UndoEntry {
        store: UndoStoreId::Code,
        key: b"cached-read".to_vec(),
        before: None,
    });
    source.undo = undo.encode();
    source.digest = journal::block_digest(&source);
    assert_ne!(source.digest, local.digest);
    assert_eq!(
        journal::semantic_digest(&source).unwrap(),
        journal::semantic_digest(&local).unwrap()
    );
    donor.import(&source).unwrap();
    receiver.import(&local).unwrap();
    // Model a locally committed block: state/event receipt exists, upstream cursor remains at the previous import.
    {
        let mut c = receiver.cursor.lock().unwrap();
        c.height = first.height;
        c.block_id = first.block_id.clone();
        c.digest = first.digest.clone();
        receiver.persist_cursor(&c).unwrap();
    }
    receiver.switch_mode(false).unwrap();
    let ack = receiver.local_ack(2).unwrap().unwrap();
    donor.accept_ack("donor", ack).unwrap();
    assert_eq!(
        donor.journal.lock().unwrap().acks["donor"].digest,
        source.digest
    );
    let third = bundle(&donor);
    donor.import(&third).unwrap();
    receiver.switch_mode(true).unwrap();
    let fetch = receiver.fetch_cursor().unwrap();
    assert!(!fetch.after_semantic_digest.is_empty());
    let mut wrong = fetch.clone();
    wrong.after_semantic_digest[0] ^= 1;
    assert!(donor
        .export_next(&wrong)
        .unwrap_err()
        .contains("logical anchor writes differ"));
    let exported = donor.export_next(&fetch).unwrap().unwrap();
    // Preserve an independently produced candidate for the same future canonical block.
    let mut candidate = exported.clone();
    candidate.previous_digest = local.digest.clone();
    candidate.digest = journal::block_digest(&candidate);
    receiver
        .journal
        .lock()
        .unwrap()
        .stage(&candidate, false)
        .unwrap();
    receiver
        .import_with_anchor(&exported, Some(&fetch.after_semantic_digest))
        .unwrap();
    assert_eq!(
        receiver.stores.storage_row.get(b"sample").unwrap(),
        donor.stores.storage_row.get(b"sample").unwrap()
    );
    assert_eq!(
        receiver.fetch_cursor().unwrap().after_digest,
        exported.digest
    );
    assert!(std::fs::read_dir(b.path().join("assisted-sync/blocks"))
        .unwrap()
        .any(|f| f
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with(".prior.pb")));
    let reopened = Journal::open(
        b.path().join("assisted-sync"),
        receiver.config.clone(),
        receiver.network.clone(),
        receiver.profile.clone(),
        3,
        exported.block_id.clone(),
    )
    .unwrap();
    assert_eq!(
        reopened.parent_digest(3, &source.block_id).unwrap(),
        source.digest
    );
    assert_eq!(
        reopened.parent_digest(4, &exported.block_id).unwrap(),
        exported.digest
    );
}

fn cluster_config() -> AssistConfig {
    let mut c = config();
    c.trusted_cluster = true;
    c.upstream_peer = None;
    c.start_assisted = false;
    c.enter_after_blocks = 1;
    c.enter_after_checks = 1;
    for p in &mut c.peers {
        p.allow_donate = false;
        p.allow_receive = false;
        p.required_for_retention = true;
    }
    c
}
#[test]
fn trusted_cluster_selects_peer_rejects_stale_writer_and_falls_back() {
    let t = tempfile::tempdir().unwrap();
    let mut cfg = cluster_config();
    let mut other = cfg.peers[0].clone();
    other.id = "third".into();
    other.certificate_sha256 = "22".repeat(32);
    cfg.peers.push(other);
    let r = rig(t.path(), cfg);
    let status = |height| wire::Status {
        head: height,
        solid: height,
        head_id: vec![7; 32],
        solid_id: vec![7; 32],
        retained_from: 1,
        mode: "autonomous".into(),
    };
    assert!(r.cluster_observe("untrusted", 99, status(20)).is_err());
    let a = r.cluster_observe("donor", 1, status(5)).unwrap().unwrap();
    assert!(r.is_assisted());
    let b = r.cluster_observe("third", 2, status(10)).unwrap().unwrap();
    assert!(!r.cluster_current(&a));
    assert!(r.cluster_current(&b));
    let block = bundle(&r);
    assert_eq!(
        r.import_from_cluster(&block, None, &a).unwrap_err(),
        "CLUSTER_SOURCE_CHANGED"
    );
    assert_eq!(r.head().unwrap().0, 0);
    assert!(!r.has_pending_import());
    r.cluster_disconnect("third", 2).unwrap();
    assert!(
        !r.is_assisted(),
        "a lost supplier must not block independent validation"
    );
    assert!(!r.cluster_current(&b));
}
#[test]
fn trusted_cluster_prunes_without_ack_or_snapshot_pin_and_rotates_bytes() {
    let t = tempfile::tempdir().unwrap();
    let mut cfg = cluster_config();
    cfg.retain_blocks = 3;
    let r = rig(&t.path().join("count"), cfg);
    r.switch_mode(true).unwrap();
    for _ in 0..9 {
        let b = bundle(&r);
        r.import(&b).unwrap();
    }
    let mut j = r.journal.lock().unwrap();
    j.acks.insert(
        "disconnected".into(),
        wire::Ack {
            height: 1,
            ..Default::default()
        },
    );
    // This stale/corrupt pin must not even be read in fixed-window cluster mode.
    fs::write(
        t.path().join("count/assisted-sync/snapshot.json"),
        b"expired snapshot metadata",
    )
    .unwrap();
    j.prune(9).unwrap();
    assert_eq!(j.meta.floor, 7);
    assert!(j
        .read(
            6,
            &block_id_from_block(&block(6, &[0; 32]))
                .unwrap()
                .as_bytes()
                .to_vec()
        )
        .unwrap()
        .is_none());
    drop(j);
    let mut cfg = cluster_config();
    cfg.max_block_bytes = 1024;
    cfg.max_journal_bytes = 4096;
    cfg.retain_blocks = 1000;
    let r = rig(&t.path().join("bytes"), cfg.clone());
    r.switch_mode(true).unwrap();
    for _ in 0..40 {
        let b = bundle(&r);
        r.import(&b).unwrap();
    }
    let bytes: u64 = fs::read_dir(t.path().join("bytes/assisted-sync/blocks"))
        .unwrap()
        .map(|p| p.unwrap().metadata().unwrap().len())
        .sum();
    assert!(bytes <= cfg.max_journal_bytes);
    assert!(r.journal.lock().unwrap().meta.floor > 1);
    assert!(r.journal.lock().unwrap().acks.is_empty());
    let gone = wire::Fetch {
        after_height: 0,
        after_id: r.network.clone(),
        ..Default::default()
    };
    assert!(r.export_next(&gone).unwrap_err().contains("HISTORY_GAP"));
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn trusted_cluster_three_members_reverse_flow_on_same_topology() {
    let t = tempfile::tempdir().unwrap();
    let (ca, ids) = certificate_set(t.path());
    let addresses = [port(), port(), port()];
    let names = ["node-a", "node-b", "node-c"];
    let mut nodes = Vec::new();
    for i in 0..3 {
        let mut c = cluster_config();
        c.node_id = names[i].into();
        c.listen = Some(addresses[i]);
        c.certificate_file = ids[i].0.clone();
        c.private_key_file = ids[i].1.clone();
        c.client_ca_file = ca.clone();
        c.peers = (0..3)
            .filter(|j| *j != i)
            .map(|j| super::super::AssistPeer {
                id: names[j].into(),
                certificate_sha256: ids[j].2.clone(),
                tls_domain: "localhost".into(),
                server_ca_file: ca.clone(),
                endpoint: ((i == 0 && j == 1) || (i == 1 && j == 2))
                    .then(|| format!("https://{}", addresses[j])),
                ..Default::default()
            })
            .collect();
        nodes.push(rig(&t.path().join(names[i]), c));
    }
    nodes[0].switch_mode(true).unwrap();
    for _ in 0..6 {
        let b = bundle(&nodes[0]);
        nodes[0].import(&b).unwrap();
    }
    nodes[0].switch_mode(false).unwrap();
    for target in [6, 12] {
        let stop = crate::ShutdownSignal::new();
        let mut tasks = Vec::new();
        for node in &nodes {
            tasks.extend(
                super::super::start(node.clone(), stop.clone())
                    .await
                    .unwrap(),
            );
        }
        tokio::time::timeout(std::time::Duration::from_secs(40), async {
            loop {
                if nodes
                    .iter()
                    .all(|n| n.head().unwrap().0 == target && !n.is_assisted())
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(40)).await;
            }
        })
        .await
        .expect("all trusted members must catch up in both logical directions");
        for node in &nodes {
            assert!(
                node.journal.lock().unwrap().acks.is_empty(),
                "cluster must not create replication ACKs"
            );
            assert_eq!(
                node.stores.storage_row.get(b"sample").unwrap(),
                Some(target.to_be_bytes().to_vec())
            );
        }
        stop.shutdown();
        for task in tasks {
            tokio::time::timeout(std::time::Duration::from_secs(5), task)
                .await
                .unwrap()
                .unwrap();
        }
        if target == 6 {
            nodes[2].switch_mode(true).unwrap();
            for _ in 0..6 {
                let b = bundle(&nodes[2]);
                nodes[2].import(&b).unwrap();
            }
            nodes[2].switch_mode(false).unwrap();
        }
    }
    assert!(nodes[0].processing.snapshot().imported_blocks > 0);
    assert!(nodes[2].processing.snapshot().imported_blocks > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[serial_test::serial]
async fn trusted_cluster_empty_bootstrap_requester_dials() {
    empty_bootstrap_network_mode(false, true).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[serial_test::serial]
async fn trusted_cluster_empty_bootstrap_supplier_dials() {
    empty_bootstrap_network_mode(true, true).await;
}
#[test]
#[serial_test::serial]
fn trusted_cluster_snapshot_idle_lease_releases_single_slot() {
    let t = tempfile::tempdir().unwrap();
    let d = disk_rig(t.path(), cluster_config());
    let open = wire::SnapshotOpen {
        request_id: "cluster-first".into(),
        replace_snapshot_id: String::new(),
    };
    d.snapshot_open("donor", &open).unwrap();
    assert!(t.path().join("assisted-sync/snapshot").exists());
    fs::write(t.path().join("assisted-sync/snapshot-lease.json"), b"0").unwrap();
    d.expire_cluster_snapshot().unwrap();
    assert!(!t.path().join("assisted-sync/snapshot").exists());
    let next = wire::SnapshotOpen {
        request_id: "cluster-next".into(),
        replace_snapshot_id: String::new(),
    };
    d.snapshot_open("third", &next).unwrap();
    assert!(d.journal.lock().unwrap().acks.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn trusted_cluster_active_supplier_disconnect_fails_over_without_ack() {
    let t = tempfile::tempdir().unwrap();
    let (ca, ids) = certificate_set(t.path());
    let addresses = [port(), port(), port()];
    let names = ["node-a", "node-b", "node-c"];
    let mut nodes = Vec::new();
    let mut stops = Vec::new();
    let mut tasks = Vec::new();
    for i in 0..3 {
        let mut c = cluster_config();
        c.node_id = names[i].into();
        c.listen = Some(addresses[i]);
        c.certificate_file = ids[i].0.clone();
        c.private_key_file = ids[i].1.clone();
        c.client_ca_file = ca.clone();
        c.peers = (0..3)
            .filter(|j| *j != i)
            .map(|j| super::super::AssistPeer {
                id: names[j].into(),
                certificate_sha256: ids[j].2.clone(),
                tls_domain: "localhost".into(),
                server_ca_file: ca.clone(),
                endpoint: (i == 2).then(|| format!("https://{}", addresses[j])),
                ..Default::default()
            })
            .collect();
        nodes.push(rig(&t.path().join(names[i]), c));
        stops.push(crate::ShutdownSignal::new());
    }
    nodes[0].switch_mode(true).unwrap();
    nodes[1].switch_mode(true).unwrap();
    for _ in 0..512 {
        let b = bundle(&nodes[0]);
        nodes[0].import(&b).unwrap();
        nodes[1].import(&b).unwrap();
    }
    nodes[0].switch_mode(false).unwrap();
    nodes[1].switch_mode(false).unwrap();
    for i in 0..3 {
        tasks.extend(
            super::super::start(nodes[i].clone(), stops[i].clone())
                .await
                .unwrap(),
        );
    }
    let selected = tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            let head = nodes[2].head().unwrap().0;
            if head > 3 {
                assert!(head < 512, "must interrupt an active transfer");
                if let Some(peer) = nodes[2].processing.snapshot().source_peer {
                    break peer;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let failed = names.iter().position(|n| *n == selected).unwrap();
    assert!(failed < 2);
    stops[failed].shutdown();
    tokio::time::timeout(std::time::Duration::from_secs(40), async {
        while nodes[2].head().unwrap().0 < 512 || nodes[2].is_assisted() {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("a second trusted member must finish the in-flight catch-up");
    assert!(nodes[2].processing.snapshot().source_switches >= 2);
    assert_eq!(
        nodes[2].stores.storage_row.get(b"sample").unwrap(),
        Some(512i64.to_be_bytes().to_vec())
    );
    for node in &nodes {
        assert!(node.journal.lock().unwrap().acks.is_empty());
    }
    for stop in &stops {
        stop.shutdown();
    }
    for task in tasks {
        tokio::time::timeout(std::time::Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap();
    }
}
