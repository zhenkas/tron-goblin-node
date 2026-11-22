//! Maintenance-only cursors. ACK never performs I/O or wakes pruning/apply.
use std::sync::{
    atomic::{AtomicI64, Ordering},
    RwLock,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryAck {
    pub height: i64,
    pub block_id: [u8; 32],
}

pub struct HistoryControl {
    ack: RwLock<Option<HistoryAck>>,
    floor: AtomicI64,
}
impl HistoryControl {
    pub fn new(floor: i64) -> Self {
        Self {
            ack: RwLock::new(None),
            floor: AtomicI64::new(floor.max(1)),
        }
    }
    pub fn floor(&self) -> i64 {
        self.floor.load(Ordering::Acquire)
    }
    pub fn publish_floor(&self, floor: i64) {
        self.floor.fetch_max(floor, Ordering::AcqRel);
    }
    pub fn ack(&self) -> Option<HistoryAck> {
        self.ack.read().expect("history ACK poisoned").clone()
    }
    pub fn accept(&self, ack: HistoryAck) -> Result<bool, &'static str> {
        if ack.height < self.floor() || ack.height < 1 {
            return Err("ACK is below retained history");
        }
        let mut guard = self.ack.write().expect("history ACK poisoned");
        if let Some(old) = guard.as_ref() {
            if ack.height < old.height {
                return Err("ACK cannot move backward");
            }
            if ack.height == old.height {
                return if *old == ack {
                    Ok(false)
                } else {
                    Err("conflicting ACK at the same height")
                };
            }
        }
        *guard = Some(ack);
        Ok(true)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ack_is_monotonic_idempotent_and_does_not_change_floor() {
        let c = HistoryControl::new(3);
        let a = HistoryAck {
            height: 10,
            block_id: [1; 32],
        };
        assert_eq!(c.accept(a.clone()), Ok(true));
        assert_eq!(c.accept(a), Ok(false));
        assert_eq!(c.floor(), 3);
        assert!(c
            .accept(HistoryAck {
                height: 9,
                block_id: [1; 32]
            })
            .is_err());
        assert!(c
            .accept(HistoryAck {
                height: 10,
                block_id: [2; 32]
            })
            .is_err());
        c.publish_floor(8);
        c.publish_floor(7);
        assert_eq!(c.floor(), 8);
    }
    #[test]
    fn restart_has_no_ack() {
        let c = HistoryControl::new(8);
        assert_eq!(c.ack(), None);
        assert!(c
            .accept(HistoryAck {
                height: 7,
                block_id: [0; 32]
            })
            .is_err());
    }
}
