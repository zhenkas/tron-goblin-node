//! Connection retention for peers advertising blocks beyond our applied head.
//! The grace period is renewed by a new requested block body, never by pings,
//! repeated bodies or inventory announcements alone.
use std::time::{Duration, Instant};

/// Five minutes (user decision 2026-11-21, was one hour): the retention also
/// holds back the wedged-sync hard reset, so it bounds how long a stalled
/// head can sit before the self-heal runs.
const AHEAD_PEER_IDLE_TIMEOUT: Duration = Duration::from_secs(5 * 60);

pub(crate) struct AheadPeerRetention {
    advertised_tip: i64,
    highest_received: i64,
    last_progress_at: Instant,
}

impl AheadPeerRetention {
    pub(crate) fn new(peer_tip: i64, local_head: i64, now: Instant) -> Self {
        Self {
            advertised_tip: peer_tip,
            highest_received: local_head,
            last_progress_at: now,
        }
    }

    pub(crate) fn note_advertised(&mut self, number: i64) {
        self.advertised_tip = self.advertised_tip.max(number);
    }

    pub(crate) fn note_requested_block(&mut self, number: i64, local_head: i64, now: Instant) {
        if number > local_head && number > self.highest_received {
            self.highest_received = number;
            self.note_advertised(number);
            self.last_progress_at = now;
        }
    }

    pub(crate) fn should_retain(&self, local_head: i64, now: Instant) -> bool {
        self.advertised_tip > local_head
            && now.saturating_duration_since(self.last_progress_at) < AHEAD_PEER_IDLE_TIMEOUT
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ahead_peer_survives_short_watchdogs_until_exactly_five_minutes() {
        let now = Instant::now();
        let peer = AheadPeerRetention::new(200, 100, now);
        for seconds in [45, 120, 299] {
            assert!(peer.should_retain(100, now + Duration::from_secs(seconds)));
        }
        assert!(!peer.should_retain(100, now + AHEAD_PEER_IDLE_TIMEOUT));
    }

    #[test]
    fn behind_caught_up_and_unknown_peers_do_not_gain_protection() {
        let now = Instant::now();
        for tip in [-1, 99, 100] {
            assert!(!AheadPeerRetention::new(tip, 100, now).should_retain(100, now));
        }
    }

    #[test]
    fn inventory_can_confirm_an_implicitly_accepted_peer_is_ahead() {
        let now = Instant::now();
        let mut peer = AheadPeerRetention::new(-1, 100, now);
        peer.note_advertised(101);
        assert!(peer.should_retain(100, now + Duration::from_secs(200)));
    }

    #[test]
    fn repeated_or_new_announcements_do_not_renew_idle_deadline() {
        let now = Instant::now();
        let mut peer = AheadPeerRetention::new(200, 100, now);
        peer.note_advertised(200);
        peer.note_advertised(201);
        assert!(!peer.should_retain(100, now + AHEAD_PEER_IDLE_TIMEOUT));
    }

    #[test]
    fn new_requested_block_renews_full_period_for_a_slow_serving_peer() {
        let now = Instant::now();
        let mut peer = AheadPeerRetention::new(200, 100, now);
        peer.note_requested_block(101, 100, now + Duration::from_secs(290));
        assert!(peer.should_retain(100, now + Duration::from_secs(301)));
        assert!(!peer.should_retain(100, now + Duration::from_secs(590)));
    }

    #[test]
    fn duplicate_and_old_blocks_do_not_renew_idle_deadline() {
        let now = Instant::now();
        let mut peer = AheadPeerRetention::new(200, 100, now);
        peer.note_requested_block(101, 100, now + Duration::from_secs(10));
        peer.note_requested_block(101, 100, now + Duration::from_secs(250));
        peer.note_requested_block(100, 100, now + Duration::from_secs(250));
        assert!(!peer.should_retain(100, now + Duration::from_secs(310)));
    }

    #[test]
    fn other_peers_advancing_head_do_not_renew_this_peers_deadline() {
        let now = Instant::now();
        let peer = AheadPeerRetention::new(200, 100, now);
        assert!(peer.should_retain(150, now + Duration::from_secs(299)));
        assert!(!peer.should_retain(150, now + AHEAD_PEER_IDLE_TIMEOUT));
        assert!(!peer.should_retain(200, now + Duration::from_secs(1)));
    }
}
