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
//!
//! Three guards keep a live but slow peer from being suspected:
//!
//! - **One beat per burst.** A write exchanges several messages a millisecond
//!   apart; counted one by one they would teach the detector that the peer beats
//!   every millisecond, and the next ordinary gap would look like a crash.
//!   Messages closer than 50 ms extend the current beat instead.
//! - **An acceptable pause.** The caller passes how long a live peer may go quiet
//!   beyond its usual rhythm (a scheduler stall, a slow fsync, a GC pause) before
//!   φ starts to rise.
//! - **Silence is measured against the other peers.** A peer's silence runs only
//!   until the last time *another* peer was heard from. If this node stalled (its
//!   inbox was busy, or the machine was starved), every peer goes quiet at once
//!   and no one is blamed. With no other peer to compare against, the detector
//!   never suspects: recovering needs a quorum, which a lone survivor of two
//!   nodes does not have, so the fixed timeout is all it needs.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::Duration;

use tokio::time::Instant;

use crate::clock::NodeId;

/// Most inter-arrival samples kept per peer.
const WINDOW: usize = 200;
/// Floor on the inter-arrival standard deviation (ms), so a near-constant beat
/// interval doesn't make φ hypersensitive to a single slightly-late beat.
const MIN_STD_MS: f64 = 50.0;
/// Messages from one peer closer together than this (ms) belong to the same
/// beat: they extend it rather than adding an inter-arrival sample.
const MIN_INTERVAL_MS: f64 = 50.0;
/// Samples required before φ is meaningful; below this a peer is never suspected
/// (the caller's fixed-timeout fallback governs that early window).
const MIN_SAMPLES: usize = 3;

struct Window {
    /// Last message from the peer.
    last: Instant,
    /// Start of the current beat (its first message).
    beat: Instant,
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
                window.last = now;
                let interval = millis(now.saturating_duration_since(window.beat));
                if interval < MIN_INTERVAL_MS {
                    return;
                }
                window.beat = now;
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
                        beat: now,
                        intervals: VecDeque::new(),
                    },
                );
            }
        }
    }

    /// When the last message from `peer` arrived, if one ever did.
    pub fn last_heard(&self, peer: NodeId) -> Option<Instant> {
        self.peers
            .lock()
            .expect("failure detector poisoned")
            .get(&peer)
            .map(|window| window.last)
    }

    /// Whether `peer` is suspected dead: its φ exceeds `threshold` once
    /// `acceptable_pause` is allowed on top of its usual interval. Its silence is
    /// counted only up to the last message from any other peer (see the module
    /// docs). Returns false until enough samples exist, deferring to the caller's
    /// fallback.
    pub fn suspect(&self, peer: NodeId, threshold: f64, acceptable_pause: Duration) -> bool {
        let now = Instant::now();
        let peers = self.peers.lock().expect("failure detector poisoned");
        let Some(window) = peers.get(&peer) else {
            return false;
        };
        if window.intervals.len() < MIN_SAMPLES {
            return false;
        }
        let Some(heard) = peers
            .iter()
            .filter(|(&other, _)| other != peer)
            .map(|(_, other)| other.last)
            .max()
        else {
            return false;
        };
        let (mean, std) = mean_std(&window.intervals);
        let elapsed = millis(heard.min(now).saturating_duration_since(window.last));
        let mean = mean + millis(acceptable_pause);
        phi_value(elapsed, mean, std.max(MIN_STD_MS)) > threshold
    }
}

fn millis(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
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

    const PAUSE: Duration = Duration::from_millis(500);

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
        let (peer, other) = (NodeId(1), NodeId(2));

        // Regular 100ms beats build a stable distribution.
        for _ in 0..10 {
            fd.heartbeat(peer);
            fd.heartbeat(other);
            tokio::time::advance(Duration::from_millis(100)).await;
        }
        assert!(
            !fd.suspect(peer, 8.0, PAUSE),
            "a regularly-beating peer is not suspected"
        );

        // A long silence, while the other peer keeps beating, drives φ over the
        // threshold.
        for _ in 0..20 {
            tokio::time::advance(Duration::from_millis(100)).await;
            fd.heartbeat(other);
        }
        assert!(
            fd.suspect(peer, 8.0, PAUSE),
            "a long-silent peer is suspected"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn unknown_or_fresh_peers_are_never_suspected() {
        let fd = FailureDetector::new();
        // Never heard from.
        assert!(!fd.suspect(NodeId(9), 8.0, PAUSE));
        // Too few samples to judge.
        fd.heartbeat(NodeId(1));
        tokio::time::advance(Duration::from_millis(10_000)).await;
        assert!(
            !fd.suspect(NodeId(1), 8.0, PAUSE),
            "insufficient samples ⇒ defer"
        );
    }

    /// A write is a burst of messages a millisecond apart. Those gaps say nothing
    /// about how often a live peer beats, so they must not shrink the expected
    /// interval: a sweep that runs a few hundred ms late is not a crash.
    #[tokio::test(start_paused = true)]
    async fn a_burst_of_messages_does_not_make_a_late_beat_suspicious() {
        let fd = FailureDetector::new();
        let (coordinator, other) = (NodeId(1), NodeId(2));
        for _ in 0..10 {
            for _ in 0..20 {
                fd.heartbeat(coordinator);
                fd.heartbeat(other);
                tokio::time::advance(Duration::from_millis(1)).await;
            }
            tokio::time::advance(Duration::from_millis(80)).await;
        }
        // The coordinator's next beat is 400 ms late; the other peer keeps beating.
        for _ in 0..4 {
            tokio::time::advance(Duration::from_millis(100)).await;
            fd.heartbeat(other);
        }
        assert!(
            !fd.suspect(coordinator, 8.0, PAUSE),
            "a late beat after a burst is not a crash"
        );
        // A silence many beats long still is.
        for _ in 0..20 {
            tokio::time::advance(Duration::from_millis(100)).await;
            fd.heartbeat(other);
        }
        assert!(
            fd.suspect(coordinator, 8.0, PAUSE),
            "a long-silent peer is suspected"
        );
    }

    /// When no peer has been heard from, this node is the one that stalled (its
    /// inbox was busy, or the machine was starved): the silence is local, so no
    /// peer is suspected for it.
    #[tokio::test(start_paused = true)]
    async fn silence_from_every_peer_suspects_no_one() {
        let fd = FailureDetector::new();
        let (coordinator, other) = (NodeId(1), NodeId(2));
        for _ in 0..10 {
            fd.heartbeat(coordinator);
            fd.heartbeat(other);
            tokio::time::advance(Duration::from_millis(100)).await;
        }
        tokio::time::advance(Duration::from_millis(2000)).await;
        assert!(!fd.suspect(coordinator, 8.0, PAUSE), "the stall was local");
        assert!(!fd.suspect(other, 8.0, PAUSE), "the stall was local");

        // Once the other peer is heard from again, the coordinator's silence is real.
        fd.heartbeat(other);
        assert!(
            fd.suspect(coordinator, 8.0, PAUSE),
            "only the coordinator stayed silent"
        );
        assert!(!fd.suspect(other, 8.0, PAUSE));
    }
}
