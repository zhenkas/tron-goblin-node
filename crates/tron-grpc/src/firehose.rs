//! The firehose `Tail` service — a resumable server-stream over the
//! durable firehose log.
//!
//! The stream is **replay-then-follow**: everything from the
//! consumer's cursor (`from_seq`) up to the current head streams
//! immediately (read off disk in bounded chunks on a blocking thread),
//! then the task parks on the writer's watch channel and forwards each
//! newly appended entry. A slow or dead consumer only backs up its own
//! bounded channel — never the apply path, never another consumer
//! (absolute sink isolation; each tail is an independent reader with
//! its own cursor).

use prost::Message as _;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tron_index::FirehoseTailHandle;

use crate::firehose_proto::firehose_server::Firehose;
use crate::firehose_proto::{Entry, TailRequest};

/// Entries per blocking read — bounds memory per tailer while
/// replaying deep history.
const READ_CHUNK: usize = 256;
/// Per-consumer in-flight buffer; backpressure beyond this.
const CHANNEL_DEPTH: usize = 64;

/// Most entries a height replay holds back above a missing height while it
/// looks for the UNWIND that retracts them (see `Withheld`).
const MAX_WITHHELD: usize = 4096;

/// Height-replay state for entries the consumer cannot apply: APPLYs above a
/// height the log never recorded (the node appended past a crash-damaged tail
/// without repairing the gap). They are only deliverable if a later UNWIND
/// retracts them, so they are held back until the log says which it is.
struct Withheld {
    seqs: Vec<u64>,
    expected: i64,
    got: i64,
}

/// A semantically empty entry for a seq the log cannot serve as written — a
/// padding frame (zero-fill left by a power loss, which consumed a seq) or a
/// retracted orphan. "Drop everything above the height you are already at"
/// changes nothing for the consumer, while keeping the stream's seqs
/// contiguous and every entry a real event, as the wire contract promises.
fn noop_unwind(seq: u64, tip: i64) -> Entry {
    Entry {
        seq,
        event: Some(crate::firehose_proto::entry::Event::Unwind(
            crate::firehose_proto::Unwind { to_height: tip },
        )),
    }
}

fn entry_height(payload: &[u8]) -> Option<i64> {
    match Entry::decode(payload).ok()?.event? {
        crate::firehose_proto::entry::Event::Apply(a) => Some(a.height),
        crate::firehose_proto::entry::Event::Unwind(u) => Some(u.to_height),
    }
}

pub struct FirehoseService {
    handle: FirehoseTailHandle,
}

impl FirehoseService {
    pub fn new(handle: FirehoseTailHandle) -> Self {
        Self { handle }
    }
}

#[tonic::async_trait]
impl Firehose for FirehoseService {
    type TailStream = ReceiverStream<Result<Entry, Status>>;

    async fn tail(
        &self,
        request: Request<TailRequest>,
    ) -> Result<Response<Self::TailStream>, Status> {
        let request = request.into_inner();
        if request.from_height.is_some() && request.from_seq != 0 {
            return Err(Status::invalid_argument("choose from_height or from_seq, not both"));
        }
        let replay_height = request.from_height;
        let from_seq = if let Some(height) = replay_height {
            let handle = self.handle.clone();
            tokio::task::spawn_blocking(move || resolve_height(&handle, height))
                .await.map_err(|e| Status::internal(format!("height seek task: {e}")))??
        } else {
            request.from_seq.max(1)
        };
        let mut handle = self.handle.clone();
        let (tx, rx) = mpsc::channel::<Result<Entry, Status>>(CHANNEL_DEPTH);

        tokio::spawn(async move {
            let mut next = from_seq;
            let mut expected_height = replay_height;
            // Reject a cursor ahead of the durable log head. `durable + 1` is
            // allowed — a caught-up consumer legitimately waits for the next
            // entry — but anything beyond means the log was reset/recreated
            // (seqs restart lower) or the cursor is stale. Without this the
            // stream parks in `wait_past` forever: it stays open, delivers
            // nothing, and when seqs eventually catch up they name unrelated
            // blocks.
            let durable0 = handle.durable_seq();
            if from_seq > durable0 + 1 {
                let _ = tx
                    .send(Err(Status::out_of_range(format!(
                        "from_seq {from_seq} is ahead of the durable firehose head \
                         {durable0} — the log was reset or the cursor is stale; resume \
                         at {} or earlier",
                        durable0 + 1
                    ))))
                    .await;
                return;
            }
            // Offset-resumable read position: a live tail seeks to the
            // new frames instead of rescanning (and re-CRC-ing) the
            // active segment from its start on every block.
            let mut pos: Option<tron_index::ReadPos> = None;
            // Height the consumer is at after everything sent so far (seq-mode
            // streams learn it lazily; a height replay starts just below its
            // anchor).
            let mut tip: Option<i64> = replay_height.map(|h| h - 1);
            let mut withheld: Option<Withheld> = None;
            loop {
                // Visibility is bounded by the DURABLE mark, never the
                // appended head: an un-fsynced tail can be truncated by
                // a power loss and its seqs reassigned — a consumer
                // must never persist entries that can still vanish.
                let durable = handle.durable_seq();
                if next > durable {
                    if let Some(w) = &withheld {
                        // The whole durable log was read and nothing retracts
                        // the entries above the missing height: a real hole.
                        let _ = tx.send(Err(Status::data_loss(format!(
                            "incomplete height replay: expected {}, got {}; txinfo_missing=false",
                            w.expected, w.got
                        )))).await;
                        return;
                    }
                    // Caught up — park on the writer's durability
                    // wake-up. A closed channel means node shutdown.
                    if !handle.wait_past(next.saturating_sub(1)).await {
                        return;
                    }
                    continue;
                }
                let reader = handle.reader();
                let read_at = next;
                let read_pos = pos;
                let read = match tokio::task::spawn_blocking(move || {
                    reader.read_chunk(read_at, durable, READ_CHUNK, read_pos)
                })
                .await
                {
                    Ok(Ok(r)) => r,
                    Ok(Err(e)) => {
                        let _ = tx.send(Err(Status::internal(format!("firehose read: {e}")))).await;
                        return;
                    }
                    Err(join) => {
                        let _ = tx
                            .send(Err(Status::internal(format!("firehose read task: {join}"))))
                            .await;
                        return;
                    }
                };
                let (chunk, new_pos) = read;
                pos = new_pos;
                if chunk.is_empty() {
                    // `next <= durable` here (the caught-up case returned
                    // above), yet nothing was read: the durable entry at `next`
                    // is gone — pruned by retention, or a corrupt/torn frame
                    // below the durable mark truncated the scan. `wait_past`
                    // would return immediately (durable >= next), so looping
                    // here pegs a core re-CRC-ing the segment forever. Surface
                    // it once as a data-loss error and stop; the consumer must
                    // resync from the oldest retained entry.
                    let _ = tx
                        .send(Err(Status::data_loss(format!(
                            "firehose entry {next} is at or below the durable head \
                             {durable} but is not readable — it was pruned by retention \
                             or the log is corrupt/truncated below its durable mark; \
                             resync from the oldest retained entry"
                        ))))
                        .await;
                    return;
                }
                for (seq, payload) in chunk {
                    if payload.is_empty() {
                        // Padding frame: no event, but it consumed `seq`.
                        if handle.history_control().is_some() && seq != next {
                            let _ = tx.send(Err(Status::out_of_range(format!(
                                "history was pruned: expected seq {next}, got {seq}"
                            )))).await;
                            return;
                        }
                        if let Some(w) = withheld.as_mut() {
                            w.seqs.push(seq);
                        } else {
                            if tip.is_none() {
                                // Seq-mode stream that starts on padding: the
                                // consumer is at the entry before it.
                                let reader = handle.reader();
                                tip = tokio::task::spawn_blocking(move || reader.last_before(seq))
                                    .await
                                    .ok()
                                    .and_then(|r| r.ok())
                                    .flatten()
                                    .and_then(|(_, p)| entry_height(&p));
                            }
                            // No entry precedes it at all (nothing to stand
                            // on): the stream simply starts after the padding.
                            if let Some(t) = tip {
                                if tx.send(Ok(noop_unwind(seq, t))).await.is_err() {
                                    return;
                                }
                            }
                        }
                        next = seq + 1;
                        continue;
                    }
                    let entry = match Entry::decode(payload.as_slice()) {
                        Ok(e) => e,
                        Err(e) => {
                            let _ = tx
                                .send(Err(Status::internal(format!(
                                    "firehose entry {seq} undecodable: {e}"
                                ))))
                                .await;
                            return;
                        }
                    };
                    if let Some(control) = handle.history_control() {
                        let floor = control.floor();
                        let below = match &entry.event {
                            Some(crate::firehose_proto::entry::Event::Apply(a)) => a.height < floor,
                            Some(crate::firehose_proto::entry::Event::Unwind(u)) => u.to_height < floor - 1,
                            None => false,
                        };
                        if seq != next || below {
                            let _ = tx.send(Err(Status::out_of_range(format!(
                                "history was pruned: expected seq {next}, got {seq}; earliest permitted height {floor}"
                            )))).await;
                            return;
                        }
                    }
                    if let Some(expected) = expected_height {
                        match &entry.event {
                            Some(crate::firehose_proto::entry::Event::Apply(a)) => {
                                if a.height > expected && !a.txinfo_missing {
                                    // Above a height the log never recorded.
                                    // Fatal unless a later UNWIND retracts it
                                    // (then the consumer never needed it), so
                                    // hold it back instead of failing here.
                                    let w = withheld.get_or_insert(Withheld {
                                        seqs: Vec::new(),
                                        expected,
                                        got: a.height,
                                    });
                                    w.seqs.push(seq);
                                    if w.seqs.len() <= MAX_WITHHELD {
                                        next = seq + 1;
                                        continue;
                                    }
                                }
                                if let Some(w) = &withheld {
                                    let _ = tx.send(Err(Status::data_loss(format!(
                                        "incomplete height replay: expected {}, got {}; txinfo_missing=false",
                                        w.expected, w.got
                                    )))).await;
                                    return;
                                }
                                if a.height != expected || a.txinfo_missing {
                                    let _ = tx.send(Err(Status::data_loss(format!(
                                        "incomplete height replay: expected {expected}, got {}; txinfo_missing={}",
                                        a.height, a.txinfo_missing
                                    )))).await;
                                    return;
                                }
                                expected_height = Some(a.height + 1);
                            }
                            Some(crate::firehose_proto::entry::Event::Unwind(u)) => {
                                if u.to_height < replay_height.unwrap() - 1 {
                                    let _ = tx.send(Err(Status::data_loss("unwind precedes replay anchor; restart from an earlier finalized checkpoint"))).await;
                                    return;
                                }
                                if withheld.is_some() && u.to_height >= expected {
                                    // Retracts only withheld entries; the
                                    // missing height is still missing.
                                    withheld.as_mut().expect("checked").seqs.push(seq);
                                    next = seq + 1;
                                    continue;
                                }
                                if let Some(w) = withheld.take() {
                                    // This UNWIND drops the missing height and
                                    // everything held back above it. Their seqs
                                    // were consumed, so account for each one.
                                    for s in w.seqs {
                                        if tx.send(Ok(noop_unwind(s, expected - 1))).await.is_err() {
                                            return;
                                        }
                                    }
                                }
                                expected_height = Some(u.to_height + 1);
                            }
                            None => {
                                let _ = tx.send(Err(Status::data_loss("empty firehose entry"))).await;
                                return;
                            }
                        }
                    }
                    tip = match &entry.event {
                        Some(crate::firehose_proto::entry::Event::Apply(a)) => Some(a.height),
                        Some(crate::firehose_proto::entry::Event::Unwind(u)) => Some(u.to_height),
                        None => tip,
                    };
                    if tx.send(Ok(entry)).await.is_err() {
                        return; // consumer hung up
                    }
                    next = seq + 1;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

/// A read-side seek only: does not touch the apply writer or any other subscriber.
/// Bounded chunks/offset resumes keep memory bounded. Cold lookup is linear in
/// retained journal history; use the returned native seq for subsequent resumes.
fn resolve_height(handle: &FirehoseTailHandle, height: i64) -> Result<u64, Status> {
    if height < 1 { return Err(Status::invalid_argument("from_height must be positive")); }
    if let Some(c) = handle.history_control() {
        if height < c.floor() {
            return Err(Status::out_of_range(format!("from_height {height} precedes history floor {}", c.floor())));
        }
    }
    let reader = handle.reader();
    let durable = handle.durable_seq();
    let oldest = reader.oldest_seq().map_err(|e| Status::internal(e.to_string()))?.unwrap_or(1);
    // The journal can hold tens of gigabytes; decoding it from the oldest
    // entry outlasts the request deadline. Start at the newest segment that
    // opens below the wanted height, and only fall back to the full scan when
    // that shortcut does not reach it (heights are not monotonic across
    // unwinds).
    let start = segment_start_below(&reader, durable, height).unwrap_or(oldest);
    match scan_for_height(&reader, durable, start, height) {
        Err(e) if start != oldest && e.code() == tonic::Code::OutOfRange => {
            scan_for_height(&reader, durable, oldest, height)
        }
        other => other,
    }
}

/// First seq of the newest segment whose first entry is an APPLY below `height`.
fn segment_start_below(
    reader: &tron_index::firehose_log::FirehoseLogReader,
    durable: u64,
    height: i64,
) -> Option<u64> {
    let starts = reader.segment_starts().ok()?;
    for start in starts.into_iter().rev().filter(|s| *s <= durable) {
        let first = reader.read_from(start, 1).ok()?.into_iter().next()?;
        if first.0 != start || first.1.is_empty() {
            continue;
        }
        if let Some(crate::firehose_proto::entry::Event::Apply(a)) =
            Entry::decode(first.1.as_slice()).ok()?.event
        {
            if a.height < height {
                return Some(start);
            }
        }
    }
    None
}

fn scan_for_height(
    reader: &tron_index::firehose_log::FirehoseLogReader,
    durable: u64,
    start: u64,
    height: i64,
) -> Result<u64, Status> {
    let mut next = start;
    let mut pos = None;
    let mut first = None;
    let mut last = None;
    while next <= durable {
        let (chunk, new_pos) = reader.read_chunk(next, durable, 64, pos)
            .map_err(|e| Status::data_loss(format!("height seek: {e}")))?;
        if chunk.is_empty() { return Err(Status::data_loss("unreadable durable journal during height seek")); }
        pos = new_pos;
        for (seq, payload) in chunk {
            if seq != next { return Err(Status::out_of_range("journal changed/pruned during height seek; retry")); }
            if payload.is_empty() { next = seq + 1; continue; } // padding frame
            let e = Entry::decode(payload.as_slice()).map_err(|e| Status::data_loss(e.to_string()))?;
            match e.event {
                Some(crate::firehose_proto::entry::Event::Apply(a)) => {
                    first.get_or_insert(a.height);
                    last = Some(a.height);
                    if a.height == height { return Ok(seq); }
                }
                Some(crate::firehose_proto::entry::Event::Unwind(u)) => last = Some(u.to_height),
                None => return Err(Status::data_loss("empty journal entry")),
            }
            next = seq + 1;
        }
    }
    if last == Some(height - 1) { return Ok(durable + 1); }
    Err(Status::out_of_range(format!(
        "height {height} is not in retained complete journal history (first APPLY {first:?}, current log height {last:?})"
    )))
}
