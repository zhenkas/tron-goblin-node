//! End-to-end firehose tail: a real gRPC server + the generated
//! client. Covers replay (history before the connect), live follow
//! (entries appended while the stream is open), and cursor resume.

use std::sync::Arc;
use std::time::Duration;

use prost::Message as _;
use tokio::sync::oneshot;
use tron_chainbase::{KvBackend, MemBackend};
use tron_grpc::firehose_proto::firehose_client::FirehoseClient;
use tron_grpc::firehose_proto::{self as fh};
use tron_index::FirehoseLogWriter;
use tron_rpc::RpcState;

fn mem() -> Arc<dyn KvBackend> {
    Arc::new(MemBackend::new())
}

fn tmp_dir() -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!(
        "tron-fh-tail-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn apply_entry(seq: u64, height: i64) -> fh::Entry {
    fh::Entry {
        seq,
        event: Some(fh::entry::Event::Apply(fh::BlockApplied {
            height,
            block_id: vec![height as u8; 32],
            timestamp_ms: 1_700_000_000_000 + height * 3000,
            ..Default::default()
        })),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tail_replays_history_follows_live_and_resumes_by_cursor() {
    let dir = tmp_dir();
    let mut writer = FirehoseLogWriter::open(&dir, u64::MAX).unwrap();
    for h in 1..=3i64 {
        let seq = writer.next_seq();
        writer.append(&apply_entry(seq, h).encode_to_vec()).unwrap();
    }
    writer.sync().unwrap();

    let state = RpcState::new(mem(), mem(), mem(), mem(), mem(), 1)
        .with_firehose(writer.tail_handle());

    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        let shut = async move {
            let _ = shutdown_rx.await;
        };
        tron_grpc::start_server(state, addr, shut).await
    });
    tokio::time::sleep(Duration::from_millis(150)).await;

    let mut client = FirehoseClient::connect(format!("http://{addr}")).await.expect("connect");

    // ---- replay: history present before the connect ----
    let mut stream = client
        .tail(fh::TailRequest { from_seq: 1, ..Default::default() })
        .await
        .expect("tail")
        .into_inner();
    for expect in 1..=3u64 {
        let entry = tokio::time::timeout(Duration::from_secs(5), stream.message())
            .await
            .expect("timely")
            .expect("stream ok")
            .expect("entry");
        assert_eq!(entry.seq, expect);
        match entry.event {
            Some(fh::entry::Event::Apply(a)) => assert_eq!(a.height, expect as i64),
            other => panic!("unexpected event: {other:?}"),
        }
    }

    // ---- live follow: appended while the stream is parked ----
    let seq = writer.next_seq();
    writer.append(&apply_entry(seq, 4).encode_to_vec()).unwrap();
    // Visibility follows durability: the un-fsynced append must NOT
    // reach the stream yet (a torn tail could erase it and reassign
    // its seq — consumers may only persist entries that cannot
    // vanish).
    let premature = tokio::time::timeout(Duration::from_millis(300), stream.message()).await;
    assert!(premature.is_err(), "entry visible before fsync: {premature:?}");
    writer.sync().unwrap();
    let entry = tokio::time::timeout(Duration::from_secs(5), stream.message())
        .await
        .expect("live entry arrives")
        .expect("stream ok")
        .expect("entry");
    assert_eq!(entry.seq, 4);

    // ---- resume: a second consumer from its own cursor ----
    let mut resumed = client
        .tail(fh::TailRequest { from_seq: 3, ..Default::default() })
        .await
        .expect("tail resume")
        .into_inner();
    let seqs = [
        resumed.message().await.unwrap().unwrap().seq,
        resumed.message().await.unwrap().unwrap().seq,
    ];
    assert_eq!(seqs, [3, 4], "resume replays exactly from the cursor");

    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(3), server).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn height_replay_is_live_independent_and_preserves_unwind() {
    let dir=tmp_dir();let mut writer=FirehoseLogWriter::open(&dir,u64::MAX).unwrap();
    let control=Arc::new(tron_index::HistoryControl::new(2));writer.manage_history(control.clone());
    for h in 1..=3 {let seq=writer.next_seq();writer.append(&apply_entry(seq,h).encode_to_vec()).unwrap();}
    writer.sync().unwrap();
    let state=RpcState::new(mem(),mem(),mem(),mem(),mem(),1).with_firehose(writer.tail_handle());
    let socket=std::net::TcpListener::bind("127.0.0.1:0").unwrap();let addr=socket.local_addr().unwrap();drop(socket);
    let (shutdown_tx,shutdown_rx)=oneshot::channel::<()>();
    let server=tokio::spawn(async move{tron_grpc::start_server(state,addr,async move{let _=shutdown_rx.await;}).await});
    tokio::time::sleep(Duration::from_millis(150)).await;
    let mut client=FirehoseClient::connect(format!("http://{addr}")).await.unwrap();
    let err=client.tail(fh::TailRequest{from_height:Some(1),..Default::default()}).await.unwrap_err();
    assert_eq!(err.code(),tonic::Code::OutOfRange);
    let err=client.tail(fh::TailRequest{from_height:Some(2),from_seq:2}).await.unwrap_err();
    assert_eq!(err.code(),tonic::Code::InvalidArgument);
    let mut height=client.tail(fh::TailRequest{from_height:Some(2),..Default::default()}).await.unwrap().into_inner();
    assert_eq!(height.message().await.unwrap().unwrap().seq,2);
    assert_eq!(height.message().await.unwrap().unwrap().seq,3);
    let mut ordinary=client.tail(fh::TailRequest{from_seq:3,..Default::default()}).await.unwrap().into_inner();
    assert_eq!(ordinary.message().await.unwrap().unwrap().seq,3);
    writer.append(&apply_entry(4,4).encode_to_vec()).unwrap();writer.sync().unwrap();
    assert_eq!(height.message().await.unwrap().unwrap().seq,4);
    assert_eq!(ordinary.message().await.unwrap().unwrap().seq,4);
    writer.append(&fh::Entry{seq:5,event:Some(fh::entry::Event::Unwind(fh::Unwind{to_height:2}))}.encode_to_vec()).unwrap();
    writer.append(&apply_entry(6,3).encode_to_vec()).unwrap();writer.sync().unwrap();
    assert!(matches!(height.message().await.unwrap().unwrap().event,Some(fh::entry::Event::Unwind(_))));
    assert_eq!(height.message().await.unwrap().unwrap().seq,6);
    control.publish_floor(3);
    assert_eq!(client.tail(fh::TailRequest{from_height:Some(2),..Default::default()}).await.unwrap_err().code(),tonic::Code::OutOfRange);
    drop(height);drop(ordinary);drop(client);let _=shutdown_tx.send(());let _=tokio::time::timeout(Duration::from_secs(3),server).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn height_replay_stops_on_a_skipped_block_instead_of_hiding_it() {
    let dir=tmp_dir();let mut writer=FirehoseLogWriter::open(&dir,u64::MAX).unwrap();
    writer.append(&apply_entry(1,10).encode_to_vec()).unwrap();writer.append(&apply_entry(2,12).encode_to_vec()).unwrap();writer.sync().unwrap();
    let state=RpcState::new(mem(),mem(),mem(),mem(),mem(),1).with_firehose(writer.tail_handle());
    let socket=std::net::TcpListener::bind("127.0.0.1:0").unwrap();let addr=socket.local_addr().unwrap();drop(socket);
    let (shutdown_tx,shutdown_rx)=oneshot::channel::<()>();
    let server=tokio::spawn(async move{tron_grpc::start_server(state,addr,async move{let _=shutdown_rx.await;}).await});
    tokio::time::sleep(Duration::from_millis(150)).await;
    let mut client=FirehoseClient::connect(format!("http://{addr}")).await.unwrap();
    let mut stream=client.tail(fh::TailRequest{from_height:Some(10),..Default::default()}).await.unwrap().into_inner();
    assert_eq!(stream.message().await.unwrap().unwrap().seq,1);
    assert_eq!(stream.message().await.unwrap_err().code(),tonic::Code::DataLoss);
    drop(stream);drop(client);let _=shutdown_tx.send(());let _=tokio::time::timeout(Duration::from_secs(3),server).await;
}

fn crc32(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for b in data {
        crc ^= *b as u32;
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xEDB8_8320 & (!(crc & 1)).wrapping_add(1));
        }
    }
    !crc
}

/// The shape a power loss left in the mainnet log: APPLYs up to height N, a
/// zero-filled region (which the old open accepted as 8-byte empty frames, so
/// each consumed a seq), an APPLY far above N written by the restarted node
/// (it took the zero-fill for an empty log and skipped gap repair), and then
/// the next start's UNWIND + re-APPLYs. The stream used to die with
/// `empty firehose entry`; it must instead stay seq-contiguous and
/// height-consistent all the way through the UNWIND.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tail_survives_zero_filled_region_and_orphan_apply_before_unwind() {
    use std::io::Write as _;
    let dir = tmp_dir();
    {
        let mut writer = FirehoseLogWriter::open(&dir, u64::MAX).unwrap();
        for (seq, h) in (1u64..).zip(10..=13i64) {
            writer.append(&apply_entry(seq, h).encode_to_vec()).unwrap();
        }
        writer.sync().unwrap();
    }
    let seg = std::fs::read_dir(&dir).unwrap().next().unwrap().unwrap().path();
    let mut f = std::fs::OpenOptions::new().append(true).open(&seg).unwrap();
    f.write_all(&[0u8; 3 * 8]).unwrap(); // seqs 5, 6, 7
    let orphan = apply_entry(8, 19).encode_to_vec();
    f.write_all(&(orphan.len() as u32).to_le_bytes()).unwrap();
    f.write_all(&crc32(&orphan).to_le_bytes()).unwrap();
    f.write_all(&orphan).unwrap();
    drop(f);

    let mut writer = FirehoseLogWriter::open(&dir, u64::MAX).unwrap();
    writer.manage_history(Arc::new(tron_index::HistoryControl::new(1)));
    assert_eq!(writer.next_seq(), 9, "interior padding keeps its seqs");
    writer
        .append(&fh::Entry { seq: 9, event: Some(fh::entry::Event::Unwind(fh::Unwind { to_height: 11 })) }.encode_to_vec())
        .unwrap();
    writer.append(&apply_entry(10, 12).encode_to_vec()).unwrap();
    writer.append(&apply_entry(11, 13).encode_to_vec()).unwrap();
    writer.sync().unwrap();

    let state = RpcState::new(mem(), mem(), mem(), mem(), mem(), 1).with_firehose(writer.tail_handle());
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = socket.local_addr().unwrap();
    drop(socket);
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server = tokio::spawn(async move { tron_grpc::start_server(state, addr, async move { let _ = shutdown_rx.await; }).await });
    tokio::time::sleep(Duration::from_millis(150)).await;
    let mut client = FirehoseClient::connect(format!("http://{addr}")).await.unwrap();

    fn shape(e: fh::Entry) -> (u64, char, i64) {
        match e.event {
            Some(fh::entry::Event::Apply(a)) => (e.seq, 'A', a.height),
            Some(fh::entry::Event::Unwind(u)) => (e.seq, 'U', u.to_height),
            None => (e.seq, '?', -1),
        }
    }

    // Height replay (the indexer): gap-free seqs, no height the consumer
    // cannot apply, and the orphan is accounted for by a no-op UNWIND.
    let mut stream = client.tail(fh::TailRequest { from_height: Some(10), ..Default::default() }).await.unwrap().into_inner();
    let mut got = Vec::new();
    for _ in 0..11 {
        got.push(shape(stream.message().await.unwrap().unwrap()));
    }
    assert_eq!(
        got,
        vec![
            (1, 'A', 10), (2, 'A', 11), (3, 'A', 12), (4, 'A', 13),
            (5, 'U', 13), (6, 'U', 13), (7, 'U', 13), // padding
            (8, 'U', 13),                             // orphan APPLY(19)
            (9, 'U', 11), (10, 'A', 12), (11, 'A', 13),
        ]
    );

    // A replay anchored after the damage seeks straight across the padding.
    let mut later = client.tail(fh::TailRequest { from_height: Some(12), ..Default::default() }).await.unwrap().into_inner();
    assert_eq!(shape(later.message().await.unwrap().unwrap()), (3, 'A', 12));

    // Seq cursor resuming inside the padding: still contiguous; the log's
    // entries are otherwise passed through untouched.
    let mut by_seq = client.tail(fh::TailRequest { from_seq: 6, ..Default::default() }).await.unwrap().into_inner();
    let mut got = Vec::new();
    for _ in 0..4 {
        got.push(shape(by_seq.message().await.unwrap().unwrap()));
    }
    assert_eq!(got, vec![(6, 'U', 13), (7, 'U', 13), (8, 'A', 19), (9, 'U', 11)]);

    drop(stream); drop(later); drop(by_seq); drop(client);
    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(3), server).await;
}
