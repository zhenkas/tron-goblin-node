//! Timestamp carried by the connection-level keepalive ping and pong.
//!
//! java-tron's libp2p validates the timestamp of both messages one-sidedly
//! (`PingMessage.valid()` / `PongMessage.valid()`):
//!
//! ```text
//! timestamp > 0 && timestamp <= System.currentTimeMillis() + NETWORK_TIME_DIFF   // 1000 ms
//! ```
//!
//! A message that fails the check closes the connection with `BAD_MESSAGE`.
//! The value is not used for anything else: latency is measured by the peer
//! from its own clock (`now - pingSent`). Only a timestamp from the future is
//! rejected, so a host whose wall clock runs more than a second ahead loses
//! every java peer a few seconds after the handshake, while a clock that runs
//! behind costs nothing.
//!
//! The node therefore sends a timestamp that is deliberately a few seconds in
//! the past. It stays valid for the peer whatever the local clock does within
//! that margin.

use std::time::{SystemTime, UNIX_EPOCH};

/// How far in the past the keepalive timestamp is placed.
pub const KEEPALIVE_TIMESTAMP_BACKDATE_MS: i64 = 3_000;

/// Keepalive timestamp for a given wall-clock reading (milliseconds since the
/// epoch). Always positive: java rejects `timestamp <= 0`.
pub fn keepalive_timestamp_at(wall_clock_ms: i64) -> i64 {
    wall_clock_ms
        .saturating_sub(KEEPALIVE_TIMESTAMP_BACKDATE_MS)
        .max(1)
}

/// Keepalive timestamp for the current wall clock.
pub fn keepalive_timestamp_ms() -> i64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    keepalive_timestamp_at(now)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// java: `timestamp > 0 && timestamp <= peer_now + 1000`.
    fn java_accepts(timestamp: i64, peer_now_ms: i64) -> bool {
        timestamp > 0 && timestamp <= peer_now_ms + 1_000
    }

    #[test]
    fn timestamp_is_valid_for_a_peer_when_local_clock_runs_ahead() {
        let true_now = 1_795_000_000_000_i64;
        // Without the margin a clock 1.5 s ahead is rejected.
        assert!(!java_accepts(true_now + 1_500, true_now));
        for ahead_ms in [0, 500, 1_000, 1_500, 2_500, 3_900] {
            let ts = keepalive_timestamp_at(true_now + ahead_ms);
            assert!(java_accepts(ts, true_now), "local clock {ahead_ms} ms ahead");
        }
    }

    #[test]
    fn timestamp_is_valid_when_local_clock_runs_behind() {
        let true_now = 1_795_000_000_000_i64;
        let ts = keepalive_timestamp_at(true_now - 60_000);
        assert!(java_accepts(ts, true_now));
    }

    #[test]
    fn timestamp_is_always_positive() {
        assert_eq!(keepalive_timestamp_at(0), 1);
        assert_eq!(keepalive_timestamp_at(2_999), 1);
        assert_eq!(keepalive_timestamp_at(i64::MIN), 1);
        assert_eq!(keepalive_timestamp_at(3_001), 1);
        assert_eq!(keepalive_timestamp_at(3_002), 2);
    }

    #[test]
    fn current_timestamp_is_backdated() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let ts = keepalive_timestamp_ms();
        assert!(ts <= now - KEEPALIVE_TIMESTAMP_BACKDATE_MS + 50);
        assert!(ts > now - KEEPALIVE_TIMESTAMP_BACKDATE_MS - 5_000);
    }
}
