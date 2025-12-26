//! Hybrid Logical Clock implementation for ACCORD timestamps.
//!
//! Timestamps provide total ordering across all transactions in the cluster.
//! Each timestamp is a tuple of (physical_time, sequence, node_id).

use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

/// Hybrid Logical Clock timestamp.
///
/// Provides total ordering: (time, seq, node_id).
/// Two timestamps from the same millisecond are ordered by sequence,
/// and ties are broken by node_id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, bitcode::Encode, bitcode::Decode)]
pub struct Timestamp {
    /// Physical time in milliseconds since Unix epoch
    pub time: u64,
    /// Logical sequence for ordering within the same millisecond
    pub seq: u32,
    /// Node ID for tie-breaking between nodes
    pub node_id: u16,
}

impl Timestamp {
    /// Zero timestamp (minimum value)
    pub const ZERO: Self = Self {
        time: 0,
        seq: 0,
        node_id: 0,
    };

    /// Maximum timestamp
    pub const MAX: Self = Self {
        time: u64::MAX,
        seq: u32::MAX,
        node_id: u16::MAX,
    };

    /// Create a new timestamp
    pub fn new(time: u64, seq: u32, node_id: u16) -> Self {
        Self { time, seq, node_id }
    }

    /// Create the next timestamp after this one (same node)
    pub fn increment(&self) -> Self {
        Self {
            time: self.time,
            seq: self.seq.saturating_add(1),
            node_id: self.node_id,
        }
    }

    /// Create a timestamp that is guaranteed to be after this one
    pub fn next(&self, node_id: u16) -> Self {
        if node_id > self.node_id {
            Self {
                time: self.time,
                seq: self.seq,
                node_id,
            }
        } else {
            Self {
                time: self.time,
                seq: self.seq.saturating_add(1),
                node_id,
            }
        }
    }
}

impl PartialOrd for Timestamp {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Timestamp {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.time
            .cmp(&other.time)
            .then_with(|| self.seq.cmp(&other.seq))
            .then_with(|| self.node_id.cmp(&other.node_id))
    }
}

impl std::fmt::Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.time, self.seq, self.node_id)
    }
}

/// State for the hybrid clock
#[derive(Clone, Copy, Default)]
struct ClockState {
    time: u64,
    seq: u32,
}

/// Hybrid Logical Clock for generating monotonically increasing timestamps.
///
/// Combines physical time with logical counters to ensure:
/// 1. Timestamps are monotonically increasing on each node
/// 2. Timestamps respect causality when updated with received timestamps
/// 3. Timestamps stay close to physical time
pub struct HybridClock {
    node_id: u16,
    state: Mutex<ClockState>,
}

impl HybridClock {
    /// Create a new hybrid clock for the given node
    pub fn new(node_id: u16) -> Self {
        Self {
            node_id,
            state: Mutex::new(ClockState::default()),
        }
    }

    /// Get current physical time in milliseconds
    fn physical_time() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before Unix epoch")
            .as_millis() as u64
    }

    /// Generate a new timestamp that is guaranteed to be greater than any
    /// previously generated timestamp on this node.
    pub fn now(&self) -> Timestamp {
        let phys = Self::physical_time();
        let mut state = self.state.lock().unwrap();

        let (new_time, new_seq) = if phys > state.time {
            // Physical time advanced, reset sequence
            (phys, 0)
        } else {
            // Same or earlier physical time, increment sequence
            (state.time, state.seq.saturating_add(1))
        };

        state.time = new_time;
        state.seq = new_seq;

        Timestamp::new(new_time, new_seq, self.node_id)
    }

    /// Update the clock with a received timestamp and return a new timestamp
    /// that is guaranteed to be greater than both the received timestamp and
    /// any previously generated timestamp.
    pub fn update(&self, received: Timestamp) -> Timestamp {
        let phys = Self::physical_time();
        let mut state = self.state.lock().unwrap();

        // Take max of physical, last, and received times
        let max_time = phys.max(state.time).max(received.time);

        let new_seq = if max_time > state.time && max_time > received.time {
            // Physical time is ahead, reset sequence
            0
        } else if max_time == state.time && max_time == received.time {
            // All three equal, take max sequence + 1
            state.seq.max(received.seq).saturating_add(1)
        } else if max_time == state.time {
            // Last time is max
            state.seq.saturating_add(1)
        } else {
            // Received time is max
            received.seq.saturating_add(1)
        };

        state.time = max_time;
        state.seq = new_seq;

        Timestamp::new(max_time, new_seq, self.node_id)
    }

    /// Get the node ID for this clock
    pub fn node_id(&self) -> u16 {
        self.node_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp_ordering() {
        let t1 = Timestamp::new(100, 0, 1);
        let t2 = Timestamp::new(100, 1, 1);
        let t3 = Timestamp::new(100, 1, 2);
        let t4 = Timestamp::new(101, 0, 0);

        assert!(t1 < t2, "same time, higher seq should be greater");
        assert!(t2 < t3, "same time+seq, higher node_id should be greater");
        assert!(t3 < t4, "higher time should be greater");
    }

    #[test]
    fn test_timestamp_equality() {
        let t1 = Timestamp::new(100, 5, 3);
        let t2 = Timestamp::new(100, 5, 3);
        assert_eq!(t1, t2);
    }

    #[test]
    fn test_timestamp_increment() {
        let t1 = Timestamp::new(100, 5, 3);
        let t2 = t1.increment();

        assert_eq!(t2.time, 100);
        assert_eq!(t2.seq, 6);
        assert_eq!(t2.node_id, 3);
        assert!(t2 > t1);
    }

    #[test]
    fn test_timestamp_next() {
        let t1 = Timestamp::new(100, 5, 3);

        // Higher node_id, same time and seq
        let t2 = t1.next(5);
        assert_eq!(t2.time, 100);
        assert_eq!(t2.seq, 5);
        assert_eq!(t2.node_id, 5);
        assert!(t2 > t1);

        // Lower node_id, must increment seq
        let t3 = t1.next(1);
        assert_eq!(t3.time, 100);
        assert_eq!(t3.seq, 6);
        assert_eq!(t3.node_id, 1);
        assert!(t3 > t1);
    }

    #[test]
    fn test_clock_monotonic() {
        let clock = HybridClock::new(1);

        let mut prev = clock.now();
        for _ in 0..1000 {
            let curr = clock.now();
            assert!(curr > prev, "clock must be monotonically increasing");
            prev = curr;
        }
    }

    #[test]
    fn test_clock_update_with_past() {
        let clock = HybridClock::new(1);

        let t1 = clock.now();
        let past = Timestamp::new(0, 0, 2);
        let t2 = clock.update(past);

        assert!(t2 > t1, "update should return timestamp > last");
        assert!(t2 > past, "update should return timestamp > received");
    }

    #[test]
    fn test_clock_update_with_future() {
        let clock = HybridClock::new(1);

        let _t1 = clock.now();
        let future = Timestamp::new(u64::MAX / 2, 100, 2);
        let t2 = clock.update(future);

        assert!(t2 > future, "update should return timestamp > received");
        assert_eq!(t2.time, future.time);
        assert!(t2.seq > future.seq);
    }

    #[test]
    fn test_timestamp_display() {
        let t = Timestamp::new(1234567890, 42, 7);
        assert_eq!(t.to_string(), "1234567890.42.7");
    }

    #[test]
    fn test_timestamp_serialization() {
        let t1 = Timestamp::new(1234567890, 42, 7);
        let encoded = bitcode::encode(&t1);
        let t2: Timestamp = bitcode::decode(&encoded).unwrap();
        assert_eq!(t1, t2);
    }
}
