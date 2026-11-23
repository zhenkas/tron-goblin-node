//! The published history boundary: heights below it are being deleted.
//!
//! Written only by node maintenance, which moves it by a fixed window of
//! applied blocks. Readers (the event-log tail) refuse requests below it.
use std::sync::atomic::{AtomicI64, Ordering};

pub struct HistoryControl {
    floor: AtomicI64,
}
impl HistoryControl {
    pub fn new(floor: i64) -> Self {
        Self {
            floor: AtomicI64::new(floor.max(1)),
        }
    }
    pub fn floor(&self) -> i64 {
        self.floor.load(Ordering::Acquire)
    }
    pub fn publish_floor(&self, floor: i64) {
        self.floor.fetch_max(floor, Ordering::AcqRel);
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn floor_starts_at_one_and_never_moves_back() {
        assert_eq!(HistoryControl::new(0).floor(), 1);
        let c = HistoryControl::new(3);
        c.publish_floor(8);
        c.publish_floor(7);
        assert_eq!(c.floor(), 8);
    }
}
