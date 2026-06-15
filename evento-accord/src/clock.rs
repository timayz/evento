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

impl Timestamp {
    /// The smallest possible timestamp — the initial redundancy floor (nothing is
    /// redundant yet) and a natural lower bound.
    pub const MIN: Timestamp = Timestamp {
        micros: 0,
        logical: 0,
        node: NodeId(0),
    };
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

/// Maximum clock skew the Hybrid Logical Clock will adopt from a peer. A
/// witnessed timestamp more than this far beyond the local wall clock is capped,
/// so a faulty/far-future timestamp cannot run the clock away. Recovery and
/// compaction margins (`RECOVERY_TIMEOUT`, `COMPACTION_MARGIN` in `node.rs`) are
/// kept comfortably above this so the bounded residual skew is absorbed.
pub const MAX_SKEW_MICROS: u64 = 200_000; // 200ms

/// Wall-clock microseconds since the Unix epoch (0 if the system clock is before
/// the epoch, which should never happen in practice). The default physical-time
/// source for [`HybridLogicalClock`].
pub fn system_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

/// Production [`Clock`]: a Hybrid Logical Clock combining a physical-time source
/// (microseconds) with a logical counter, tagged with this node's [`NodeId`].
pub struct HybridLogicalClock {
    node: NodeId,
    /// `(micros, logical)` — the high-water mark of physical and logical time.
    state: Mutex<(u64, u32)>,
    /// Physical-time source in microseconds. The wall clock in production; the
    /// deterministic simulation injects a virtual source so the clock — and thus
    /// every timestamp — is bit-reproducible.
    physical: Box<dyn Fn() -> u64 + Send + Sync>,
    /// How far beyond the local wall clock a witnessed peer timestamp may push
    /// this clock (the skew bound). Defaults to [`MAX_SKEW_MICROS`]; tune per
    /// deployment with [`with_max_skew`](Self::with_max_skew).
    max_skew_micros: u64,
}

impl HybridLogicalClock {
    /// Creates a clock issuing timestamps tagged with `node`, driven by the
    /// wall clock ([`system_micros`]).
    pub fn new(node: NodeId) -> Self {
        Self::with_physical(node, system_micros)
    }

    /// Creates a clock with a custom physical-time source (microseconds, from any
    /// fixed epoch). Used by the deterministic simulation to drive the clock from
    /// virtual time; production uses [`new`](Self::new).
    pub fn with_physical(node: NodeId, physical: impl Fn() -> u64 + Send + Sync + 'static) -> Self {
        Self {
            node,
            state: Mutex::new((0, 0)),
            physical: Box::new(physical),
            max_skew_micros: MAX_SKEW_MICROS,
        }
    }

    /// Sets the clock-skew bound — how far a witnessed peer timestamp may push this
    /// clock beyond its own wall clock. Tune for the deployment's expected drift
    /// (geo links need more headroom); recovery/compaction margins should stay
    /// above it. Defaults to [`MAX_SKEW_MICROS`].
    pub fn with_max_skew(mut self, max_skew_micros: u64) -> Self {
        self.max_skew_micros = max_skew_micros;
        self
    }

    /// The current physical time in microseconds, from the injected source.
    fn physical_now(&self) -> u64 {
        (self.physical)()
    }
}

impl Clock for HybridLogicalClock {
    fn now(&self) -> Timestamp {
        let phys = self.physical_now();
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
        let phys = self.physical_now();
        let mut guard = self.state.lock().expect("clock poisoned");
        let (micros, logical) = &mut *guard;

        // Bound how far a peer can push this clock: a timestamp more than
        // MAX_SKEW beyond our own wall clock is capped. A peer within MAX_SKEW is
        // adopted in full (a lagging node catches up to the cluster); a faulty or
        // far-future timestamp cannot run the clock away — which, unbounded, would
        // pin `phys` below `micros` forever and grow the `logical: u32` counter
        // without bound until it collides.
        let observed_micros = observed.micros.min(phys.saturating_add(self.max_skew_micros));

        // HLC receive rule: the new physical high-water mark is the max of our
        // physical, the (bounded) observed physical, and the real wall clock; the
        // logical counter is reconciled so the result is strictly past both inputs.
        let high = (*micros).max(observed_micros).max(phys);
        let new_logical = if high == *micros && high == observed_micros {
            (*logical).max(observed.logical) + 1
        } else if high == *micros {
            *logical + 1
        } else if high == observed_micros {
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
    fn witness_caps_a_far_future_peer_timestamp_at_max_skew() {
        // A fixed wall clock so the bound can be asserted exactly.
        let wall = 1_000_000u64;
        let clock = HybridLogicalClock::with_physical(NodeId(1), move || wall);
        // A peer stamp far beyond our wall clock + MAX_SKEW (a faulty/malicious value).
        let future = Timestamp {
            micros: wall + 1_000_000_000,
            logical: 7,
            node: NodeId(99),
        };
        clock.witness(future);
        let next = clock.now();
        assert!(
            next.micros <= wall + MAX_SKEW_MICROS,
            "{next:?} must be capped at wall + MAX_SKEW, not the far-future peer value"
        );
    }

    #[test]
    fn witness_adopts_a_peer_within_max_skew() {
        let wall = 1_000_000u64;
        let clock = HybridLogicalClock::with_physical(NodeId(1), move || wall);
        // A peer modestly ahead (within MAX_SKEW): a lagging node catches up.
        let ahead = Timestamp {
            micros: wall + MAX_SKEW_MICROS / 2,
            logical: 3,
            node: NodeId(2),
        };
        clock.witness(ahead);
        let next = clock.now();
        assert!(next > ahead, "{next:?} must advance past the adopted peer {ahead:?}");
        assert!(
            next.micros >= ahead.micros,
            "the clock adopted the peer's within-bound physical time"
        );
    }

    #[test]
    fn with_max_skew_overrides_the_default_bound() {
        let wall = 1_000_000u64;
        let custom_skew = 50u64; // far tighter than the default
        let clock =
            HybridLogicalClock::with_physical(NodeId(1), move || wall).with_max_skew(custom_skew);
        let future = Timestamp {
            micros: wall + 1_000_000,
            logical: 0,
            node: NodeId(2),
        };
        clock.witness(future);
        assert!(
            clock.now().micros <= wall + custom_skew,
            "the configured (tighter) skew bound is enforced"
        );
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
