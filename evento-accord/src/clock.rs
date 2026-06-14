//! Cluster node identity and the Hybrid Logical Clock.
//!
//! Accord names and orders every transaction by a globally unique,
//! monotonically increasing [`Timestamp`]. Uniqueness comes from the triple
//! `(micros, logical, node)`: `node` breaks ties between stamps issued in the
//! same microsecond on different nodes, and `logical` breaks ties on the same
//! node. The [`Clock`] trait abstracts time so the simulation harness can drive
//! it deterministically; [`HybridLogicalClock`] is the production implementation.

use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Identifier for a node in the cluster.
///
/// Node ids are assigned statically via configuration (M1 membership) and are
/// totally ordered so they can break ties between otherwise-equal timestamps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(pub u64);

/// A Hybrid Logical Clock timestamp.
///
/// The field order is significant: deriving [`Ord`] compares `micros` first,
/// then `logical`, then `node`, which is exactly the ordering Accord requires.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Timestamp {
    /// Wall-clock microseconds since the Unix epoch (the physical component).
    pub micros: u64,
    /// Logical counter, incremented when multiple stamps share `micros`.
    pub logical: u32,
    /// Issuing node — the final tie-breaker, guaranteeing global uniqueness.
    pub node: NodeId,
}

/// The identity of a transaction.
///
/// Equal to the timestamp `t0` proposed by its coordinator during PreAccept; in
/// Accord a transaction is named by its original timestamp. The execution
/// timestamp `t` (which may be raised on the slow path) is tracked separately.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TxnId(pub Timestamp);

/// A recovery ballot, used to fence stale coordinators during the recovery
/// protocol (M2). Higher ballots win; a fresh transaction starts at ballot
/// equal to its `t0`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Ballot(pub Timestamp);

/// Abstract monotonic clock.
///
/// Two operations: [`now`](Clock::now) issues a fresh, strictly-increasing
/// timestamp, and [`witness`](Clock::witness) advances this node's clock past a
/// timestamp observed from a peer so that any timestamp we subsequently issue is
/// causally after it.
pub trait Clock: Send + Sync + 'static {
    /// Issues a fresh timestamp strictly greater than every timestamp this
    /// clock has previously issued or witnessed.
    fn now(&self) -> Timestamp;

    /// Advances the clock to be strictly past `observed`, merging a peer's
    /// timestamp into the local Hybrid Logical Clock state.
    fn witness(&self, observed: Timestamp);
}

/// Production [`Clock`]: a Hybrid Logical Clock combining wall-clock
/// microseconds with a logical counter, tagged with this node's [`NodeId`].
pub struct HybridLogicalClock {
    node: NodeId,
    /// `(micros, logical)` — the high-water mark of physical and logical time.
    state: Mutex<(u64, u32)>,
}

impl HybridLogicalClock {
    /// Creates a clock issuing timestamps tagged with `node`.
    pub fn new(node: NodeId) -> Self {
        Self {
            node,
            state: Mutex::new((0, 0)),
        }
    }

    /// Current wall clock in microseconds since the Unix epoch (0 if the system
    /// clock is before the epoch, which should never happen in practice).
    fn physical_now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0)
    }
}

impl Clock for HybridLogicalClock {
    fn now(&self) -> Timestamp {
        let phys = Self::physical_now();
        let mut guard = self.state.lock().expect("clock poisoned");
        let (micros, logical) = &mut *guard;

        // HLC send rule: advance physical if the wall clock moved forward,
        // otherwise bump the logical counter to keep timestamps strictly
        // increasing even within the same microsecond.
        if phys > *micros {
            *micros = phys;
            *logical = 0;
        } else {
            *logical += 1;
        }

        Timestamp {
            micros: *micros,
            logical: *logical,
            node: self.node,
        }
    }

    fn witness(&self, observed: Timestamp) {
        let phys = Self::physical_now();
        let mut guard = self.state.lock().expect("clock poisoned");
        let (micros, logical) = &mut *guard;

        // HLC receive rule: the new physical high-water mark is the max of our
        // physical, the observed physical, and the real wall clock; the logical
        // counter is reconciled so the result is strictly past both inputs.
        let high = (*micros).max(observed.micros).max(phys);
        let new_logical = if high == *micros && high == observed.micros {
            (*logical).max(observed.logical) + 1
        } else if high == *micros {
            *logical + 1
        } else if high == observed.micros {
            observed.logical + 1
        } else {
            0
        };

        *micros = high;
        *logical = new_logical;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn now_is_strictly_increasing() {
        let clock = HybridLogicalClock::new(NodeId(1));
        let mut prev = clock.now();
        for _ in 0..10_000 {
            let next = clock.now();
            assert!(next > prev, "{next:?} must be > {prev:?}");
            prev = next;
        }
    }

    #[test]
    fn witness_advances_past_a_future_peer_timestamp() {
        let clock = HybridLogicalClock::new(NodeId(1));
        // A peer stamp far in the future relative to our wall clock.
        let future = Timestamp {
            micros: HybridLogicalClock::physical_now() + 1_000_000_000,
            logical: 7,
            node: NodeId(99),
        };
        clock.witness(future);
        let next = clock.now();
        assert!(next > future, "{next:?} must be > witnessed {future:?}");
    }

    #[test]
    fn timestamps_break_ties_by_node() {
        // Same physical/logical, different node => still totally ordered.
        let a = Timestamp {
            micros: 100,
            logical: 0,
            node: NodeId(1),
        };
        let b = Timestamp {
            micros: 100,
            logical: 0,
            node: NodeId(2),
        };
        assert!(a < b);
        assert_ne!(a, b);
    }

    #[test]
    fn txn_id_orders_by_underlying_timestamp() {
        let a = TxnId(Timestamp {
            micros: 1,
            logical: 0,
            node: NodeId(5),
        });
        let b = TxnId(Timestamp {
            micros: 2,
            logical: 0,
            node: NodeId(1),
        });
        assert!(a < b, "physical time dominates the node tie-breaker");
    }
}
