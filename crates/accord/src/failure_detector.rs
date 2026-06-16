//! Phi-accrual failure detector (Hayashibara et al.).
//!
//! Instead of a single fixed timeout, each peer accrues a continuous suspicion
//! level **φ** from the distribution of its heartbeat inter-arrival times: every
//! inbound message is a heartbeat, and a peer that falls silent relative to its
//! own observed rhythm has its φ rise. The recovery sweep treats a coordinator as
//! dead once `φ > threshold`, so detection **adapts to a link's real latency**
//! (a slow geo link tolerates longer gaps without false alarms) rather than
//! tripping on one hard-coded value.
//!
//! Until a peer has a few samples it is never suspected — the node's fixed
//! `recovery_timeout` fallback governs that window — so this only ever makes
//! detection *more* informed, never less safe.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use tokio::time::Instant;

use crate::clock::NodeId;

/// Most inter-arrival samples kept per peer.
const WINDOW: usize = 200;
/// Floor on the inter-arrival standard deviation (ms), so a near-constant beat
/// interval doesn't make φ hypersensitive to a single slightly-late beat.
const MIN_STD_MS: f64 = 50.0;
/// Samples required before φ is meaningful; below this a peer is never suspected
/// (the caller's fixed-timeout fallback governs that early window).
const MIN_SAMPLES: usize = 3;

struct Window {
    last: Instant,
    intervals: VecDeque<f64>,
}

/// Per-peer phi-accrual estimator. Cheap, interior-mutable, shared across a
/// node's tasks.
#[derive(Default)]
pub struct FailureDetector {
    peers: Mutex<HashMap<NodeId, Window>>,
}

impl FailureDetector {
    /// Creates an empty detector.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a heartbeat (any inbound message) from `peer` at the current time.
    pub fn heartbeat(&self, peer: NodeId) {
        let now = Instant::now();
        let mut peers = self.peers.lock().expect("failure detector poisoned");
        match peers.get_mut(&peer) {
            Some(window) => {
                let interval = now.saturating_duration_since(window.last).as_secs_f64() * 1000.0;
                window.last = now;
                if window.intervals.len() == WINDOW {
                    window.intervals.pop_front();
                }
                window.intervals.push_back(interval);
            }
            None => {
                peers.insert(
                    peer,
                    Window {
                        last: now,
                        intervals: VecDeque::new(),
                    },
                );
            }
        }
    }

    /// Whether `peer` is suspected dead: its φ exceeds `threshold`. Returns false
    /// until enough samples exist, deferring to the caller's fallback.
    pub fn suspect(&self, peer: NodeId, threshold: f64) -> bool {
        let now = Instant::now();
        let peers = self.peers.lock().expect("failure detector poisoned");
        let Some(window) = peers.get(&peer) else {
            return false;
        };
        if window.intervals.len() < MIN_SAMPLES {
            return false;
        }
        let (mean, std) = mean_std(&window.intervals);
        let elapsed = now.saturating_duration_since(window.last).as_secs_f64() * 1000.0;
        phi_value(elapsed, mean, std.max(MIN_STD_MS)) > threshold
    }
}

/// Mean and (population) standard deviation of the samples.
fn mean_std(xs: &VecDeque<f64>) -> (f64, f64) {
    let n = xs.len() as f64;
    let mean = xs.iter().sum::<f64>() / n;
    let var = xs.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n;
    (mean, var.sqrt())
}

/// φ for an `elapsed` gap given the inter-arrival mean/std — the logistic
/// approximation of `-log10(P(arrival later than now))` under a normal model
/// (the form used by Akka's detector). Larger φ ⇒ more suspicious.
fn phi_value(elapsed_ms: f64, mean_ms: f64, std_ms: f64) -> f64 {
    let y = (elapsed_ms - mean_ms) / std_ms;
    let e = (-y * (1.5976 + 0.070566 * y * y)).exp();
    if elapsed_ms > mean_ms {
        -(e / (1.0 + e)).log10()
    } else {
        -(1.0 - 1.0 / (1.0 + e)).log10()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn phi_is_low_at_the_mean_and_high_far_beyond_it() {
        // At the expected interval, suspicion is negligible.
        assert!(phi_value(100.0, 100.0, 50.0) < 1.0);
        // A few standard deviations late, suspicion is high.
        assert!(phi_value(100.0 + 6.0 * 50.0, 100.0, 50.0) > 8.0);
        // φ grows monotonically with the gap.
        assert!(phi_value(300.0, 100.0, 50.0) < phi_value(500.0, 100.0, 50.0));
    }

    #[tokio::test(start_paused = true)]
    async fn suspects_a_peer_that_stops_beating() {
        let fd = FailureDetector::new();
        let peer = NodeId(1);

        // Regular 100ms beats build a stable distribution.
        for _ in 0..10 {
            fd.heartbeat(peer);
            tokio::time::advance(Duration::from_millis(100)).await;
        }
        assert!(
            !fd.suspect(peer, 8.0),
            "a regularly-beating peer is not suspected"
        );

        // A long silence drives φ over the threshold.
        tokio::time::advance(Duration::from_millis(1000)).await;
        assert!(fd.suspect(peer, 8.0), "a long-silent peer is suspected");
    }

    #[tokio::test(start_paused = true)]
    async fn unknown_or_fresh_peers_are_never_suspected() {
        let fd = FailureDetector::new();
        // Never heard from.
        assert!(!fd.suspect(NodeId(9), 8.0));
        // Too few samples to judge.
        fd.heartbeat(NodeId(1));
        tokio::time::advance(Duration::from_millis(10_000)).await;
        assert!(!fd.suspect(NodeId(1), 8.0), "insufficient samples ⇒ defer");
    }
}
