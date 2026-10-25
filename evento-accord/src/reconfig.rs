//! Two-phase layout changes (#288).
//!
//! A change that gives a key another replica set is decided as a
//! [`Transition`](ConfigEntry::Transition) first: the old layout stays live —
//! for ownership, watermarks, coverage, anti-entropy, compaction and recovery
//! — while the keys it changes ("moved" keys) take no new writes, and the
//! writes in flight on them drain under the old layout. An
//! [`Active`](ConfigEntry::Active) entry installs the new layout once they
//! have, so each key has one live layout at any time.
//!
//! **Fence.** Installing a transition, a replica records a fence point
//! `cap`: above every `t0` it ever voted for, and above every `t0` it will
//! vote for unasked ([`Fence::cap_for`]). From then on it votes for no
//! transaction it never witnessed, planned before the transition ended, on a
//! moved key, with a `t0` above `cap` ([`Fence::refuses`]). Below `cap` it
//! votes as before, so a transaction that only a dead replica and one live
//! replica hold is still recovered. A fence is never lifted.
//!
//! **Drain point.** A commit needs a slow quorum of each shard it touches, so
//! a write on a moved key that can still commit has a vote from every slow
//! quorum of the moved key's shard — from its fenced members, at or below
//! their `cap`. With fence reports from a slow quorum of every changing
//! shard, [`drain_point`] is the highest of them: no write on a moved key
//! above it ever commits.
//!
//! **Drained.** A replica's watermark report is below its vote fence (it never
//! votes for a transaction it never witnessed below its report), and it
//! holds no unapplied transaction below it. Once a slow quorum of a shard
//! reports past the drain point ([`drained`]), every write on a moved key
//! that ever commits is applied there, and none other will. Every shard is
//! waited for, so a transaction that also touches an unchanged shard is
//! applied there too.
//!
//! **No drain needed.** A change after which every quorum of each changed
//! shard still holds a replica of every quorum it had ([`quorums_intersect`]:
//! adding one replica to a shard of three, or removing one) is installed at
//! once. A transaction planned before it is voted on and recovered under its
//! own layout, a coordinator that planned before it gets no new vote from a
//! replica that installed it, and the two layouts' quorums meet: a
//! transaction planned under the new layout witnesses, through a replica the
//! two share, every one the old layout committed, and a replica that joins
//! catches up through the replicas its shard kept.
//!
//! **Activation.** The [`Active`](ConfigEntry::Active) entry carries the drain
//! point and a clock floor above every moved write's execution
//! ([`Drained`]). A replica that installs it invalidates what it holds
//! unapplied on a moved key above the drain point (it never commits), and
//! votes for no transaction planned under the new layout on a moved key below
//! the floor, so the new writes' events stamp after the moved ones'.

use std::collections::HashMap;
use std::sync::Arc;

use crate::api::{ConfigEntry, FenceRecord, Layout};
use crate::clock::{NodeId, Timestamp};
use crate::message::Key;

/// Shard index → whether its replica set differs in `next`: a key changes
/// replicas between the two layouts exactly when its shard does. Both layouts
/// have as many shards (see [`validate_change`]); if not, every shard counts
/// as changed.
pub(crate) fn changed_shards(live: &Layout, next: &Layout) -> Vec<bool> {
    if live.shards().len() != next.shards().len() {
        return vec![true; live.shards().len()];
    }
    live.shards()
        .iter()
        .zip(next.shards())
        .map(|(a, b)| {
            let mut a = a.clone();
            let mut b = b.clone();
            a.sort_unstable();
            b.sort_unstable();
            a != b
        })
        .collect()
}

/// Whether, for every shard, each slow quorum of `next` holds a replica of
/// each slow quorum of `live`: a quorum of the new replica set holds at least
/// `slow_q(next) − added` of the old replicas, and two subsets of the old set
/// meet when their sizes add up past it. Then a change needs no drain (see
/// the module docs). Both layouts have as many shards (see
/// [`validate_change`]); if not, they do not intersect.
pub(crate) fn quorums_intersect(live: &Layout, next: &Layout) -> bool {
    if live.shards().len() != next.shards().len() {
        return false;
    }
    (0..live.shards().len()).all(|shard| {
        let before = live.replicas_of(shard);
        let after = next.replicas_of(shard);
        let added = after.iter().filter(|node| !before.contains(node)).count();
        next.slow_quorum_of(shard).saturating_sub(added) + live.slow_quorum_of(shard) > before.len()
    })
}

/// Checks that a change from the `live` layout to `next` can be carried out:
/// the shard count stays, every shard keeps a replica it already had, and no
/// replica moves to another shard. A shard that kept a replica is joined
/// through it, and nothing moves between shards.
pub(crate) fn validate_change(live: &[Vec<NodeId>], next: &[Vec<NodeId>]) -> anyhow::Result<()> {
    anyhow::ensure!(
        live.len() == next.len(),
        "a change cannot move keys between shards yet: {} shards would become {}",
        live.len(),
        next.len()
    );
    for (shard, (before, after)) in live.iter().zip(next).enumerate() {
        anyhow::ensure!(
            after.iter().any(|node| before.contains(node)),
            "shard {shard} would keep none of its replicas {before:?}: it could not be joined"
        );
        for node in after {
            if let Some(other) = live.iter().position(|replicas| replicas.contains(node)) {
                anyhow::ensure!(
                    other == shard,
                    "{node:?} would move from shard {other} to shard {shard}: \
                     a member cannot gain another shard's keys yet"
                );
            }
        }
    }
    Ok(())
}

/// The layout transition a node has installed and not ended yet.
pub(crate) struct Transition {
    /// The epoch the change was first decided at.
    pub since: u64,
    /// The live layout.
    pub live: Arc<Layout>,
    /// The layout the change installs.
    pub pending: Arc<Layout>,
    /// Shard index (of `live`) → whether its replica set changes.
    changed: Vec<bool>,
}

impl Transition {
    pub fn new(since: u64, live: Arc<Layout>, pending: Arc<Layout>) -> Self {
        let changed = changed_shards(&live, &pending);
        Self {
            since,
            live,
            pending,
            changed,
        }
    }

    /// Whether `key`'s replica set changes.
    pub fn moves(&self, key: &Key) -> bool {
        self.changed[self.live.shard_of(key)]
    }

    /// The replicas of a changing shard, by shard.
    fn changing_shards(&self) -> impl Iterator<Item = usize> + '_ {
        self.changed
            .iter()
            .enumerate()
            .filter(|(_, &changed)| changed)
            .map(|(shard, _)| shard)
    }
}

/// A fence a replica installed for a transition (see the module docs).
pub(crate) struct Fence {
    /// The epoch the transition was first decided at.
    pub since: u64,
    /// The epoch the transition ended at (activated, or replaced by another
    /// change): a transaction planned under it or later is not fenced.
    /// `None` while it is in progress.
    pub end: Option<u64>,
    /// The fence point.
    pub cap: Timestamp,
    /// Whether the journal holds it: only then is it reported.
    pub journaled: bool,
    /// The layout live during the transition, and which of its shards change.
    live: Arc<Layout>,
    changed: Vec<bool>,
}

impl Fence {
    pub fn new(transition: &Transition, cap: Timestamp, journaled: bool) -> Self {
        Self {
            since: transition.since,
            end: None,
            cap,
            journaled,
            live: Arc::clone(&transition.live),
            changed: transition.changed.clone(),
        }
    }

    /// The fence point of a replica installing a transition now: above every
    /// `t0` it holds or compacted (`highest`), above every `t0` it lets a
    /// coordinator date ahead of its clock (`horizon_micros`, its reading plus
    /// two skews: see `Node::future_dated`), and above everything a node that
    /// lost its journal may have voted on before (`amnesia_micros`).
    pub fn cap_for(
        highest: Timestamp,
        horizon_micros: u64,
        amnesia_micros: Option<u64>,
    ) -> Timestamp {
        let ceiling = |micros: u64| Timestamp {
            micros,
            logical: u32::MAX,
            node: NodeId(u64::MAX),
        };
        let mut cap = highest.max(ceiling(horizon_micros));
        if let Some(amnesia) = amnesia_micros {
            cap = cap.max(ceiling(amnesia));
        }
        cap
    }

    /// Whether a vote for a transaction this replica never witnessed, planned
    /// under `epoch`, at `t0`, touching `keys` (its route when it has one), is
    /// refused.
    pub fn refuses(&self, epoch: u64, t0: Timestamp, keys: &[Key]) -> bool {
        self.end.is_none_or(|end| epoch < end)
            && t0 > self.cap
            && keys.iter().any(|key| self.changed[self.live.shard_of(key)])
    }

    /// The record the journal keeps.
    pub fn record(&self) -> FenceRecord {
        FenceRecord {
            since: self.since,
            cap: self.cap,
        }
    }
}

/// An activation's clock floor: no vote for a transaction planned under it
/// or later, on a key it moved, below `floor` (see [`Drained`](crate::api::Drained)).
pub(crate) struct Floor {
    pub epoch: u64,
    pub floor: Timestamp,
    live: Arc<Layout>,
    changed: Vec<bool>,
}

impl Floor {
    /// The floor of the activation at `epoch` that ended `transition`.
    pub fn new(epoch: u64, floor: Timestamp, transition: &Transition) -> Self {
        Self {
            epoch,
            floor,
            live: Arc::clone(&transition.live),
            changed: transition.changed.clone(),
        }
    }

    /// Whether a vote for a transaction planned under `epoch`, at `t0`,
    /// touching `keys`, is refused.
    pub fn refuses(&self, epoch: u64, t0: Timestamp, keys: &[Key]) -> bool {
        epoch >= self.epoch
            && t0 < self.floor
            && keys.iter().any(|key| self.changed[self.live.shard_of(key)])
    }
}

/// The drain point of `transition` from the fence reports gathered so far:
/// `None` until a slow quorum of every changing shard reported its fence for
/// it; then the highest fence reported by a replica of a changing shard.
pub(crate) fn drain_point(
    transition: &Transition,
    fences: &HashMap<NodeId, FenceRecord>,
) -> Option<Timestamp> {
    let mut point = Timestamp::MIN;
    for shard in transition.changing_shards() {
        let caps: Vec<Timestamp> = transition
            .live
            .replicas_of(shard)
            .iter()
            .filter_map(|node| fences.get(node))
            .filter(|fence| fence.since == transition.since)
            .map(|fence| fence.cap)
            .collect();
        if caps.len() < transition.live.slow_quorum_of(shard) {
            return None;
        }
        point = caps.into_iter().fold(point, Timestamp::max);
    }
    Some(point)
}

/// Whether every shard of the `live` layout has a slow quorum whose watermark
/// reports are past `point`.
pub(crate) fn drained(
    live: &Layout,
    point: Timestamp,
    reports: &HashMap<NodeId, Timestamp>,
) -> bool {
    (0..live.shards().len()).all(|shard| {
        let past = live
            .replicas_of(shard)
            .iter()
            .filter(|node| reports.get(node).is_some_and(|&report| report > point))
            .count();
        past >= live.slow_quorum_of(shard)
    })
}

/// The entry that repeats `entry` at a later epoch, changing nothing: what a
/// recovery of an epoch nobody accepted a value for decides.
pub(crate) fn repeat(entry: &ConfigEntry) -> ConfigEntry {
    match entry {
        ConfigEntry::Active { layout, .. } => ConfigEntry::active(layout.clone()),
        transition @ ConfigEntry::Transition { .. } => transition.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(nodes: &[u64]) -> Vec<NodeId> {
        nodes.iter().copied().map(NodeId).collect()
    }

    fn at(micros: u64) -> Timestamp {
        Timestamp {
            micros,
            logical: 0,
            node: NodeId(1),
        }
    }

    fn fence(since: u64, cap: u64) -> FenceRecord {
        FenceRecord {
            since,
            cap: at(cap),
        }
    }

    /// Replacing a node changes its shard only; a shard keeps its index.
    #[test]
    fn only_the_shards_whose_replicas_change_move() {
        let live = Layout::new(0, vec![ids(&[0, 1, 2]), ids(&[3, 4, 5])]);
        let next = Layout::new(1, vec![ids(&[2, 1, 0]), ids(&[3, 4, 6])]);
        assert_eq!(changed_shards(&live, &next), vec![false, true]);
    }

    /// One replica added to a shard of three, or one removed, keeps every
    /// new quorum meeting every old one; a replacement, two additions, or a
    /// shrink from five to three do not.
    #[test]
    fn only_a_change_whose_quorums_meet_skips_the_drain() {
        let live = Layout::new(0, vec![ids(&[0, 1, 2])]);
        let meets = |next: &[u64]| quorums_intersect(&live, &Layout::new(1, vec![ids(next)]));
        assert!(meets(&[0, 1, 2]));
        assert!(meets(&[0, 1, 2, 3]), "adding one");
        assert!(meets(&[0, 1]), "removing one");
        assert!(!meets(&[0, 1, 3]), "replacing one");
        assert!(!meets(&[0, 1, 2, 3, 4]), "adding two");
        let five = Layout::new(0, vec![ids(&[0, 1, 2, 3, 4])]);
        assert!(!quorums_intersect(
            &five,
            &Layout::new(1, vec![ids(&[0, 1, 2])])
        ));
        assert!(quorums_intersect(
            &five,
            &Layout::new(1, vec![ids(&[0, 1, 2, 3])])
        ));
    }

    #[test]
    fn a_change_keeps_shards_and_a_replica_of_each() {
        let live = vec![ids(&[0, 1, 2]), ids(&[3, 4, 5])];
        assert!(validate_change(&live, &[ids(&[0, 1, 6]), ids(&[3, 4, 5])]).is_ok());
        assert!(validate_change(&live, &[ids(&[0, 1, 2, 6]), ids(&[3, 4])]).is_ok());
        // A shard more: keys would move between shards.
        assert!(validate_change(&live, &[ids(&[0, 1]), ids(&[2, 3]), ids(&[4, 5])]).is_err());
        // A shard of new nodes only: nobody to join through.
        assert!(validate_change(&live, &[ids(&[0, 1, 2]), ids(&[6, 7, 8])]).is_err());
        // A member moving to the other shard would gain its keys.
        assert!(validate_change(&live, &[ids(&[0, 1]), ids(&[2, 3, 4, 5])]).is_err());
    }

    /// The fence refuses only what it is about: never-witnessed votes for a
    /// transaction planned before the transition ended, on a moved key, above
    /// the fence point.
    #[test]
    fn a_fence_refuses_late_votes_on_moved_keys_only() {
        let live = Arc::new(Layout::new(0, vec![ids(&[0, 1, 2]), ids(&[3, 4, 5])]));
        let next = Arc::new(Layout::new(1, vec![ids(&[0, 1, 2]), ids(&[3, 4, 6])]));
        let transition = Transition::new(1, Arc::clone(&live), next);
        let keys: Vec<Key> = (0..32).map(|i| Key(format!("k{i}"))).collect();
        let moved = keys.iter().find(|k| live.shard_of(k) == 1).unwrap().clone();
        let kept = keys.iter().find(|k| live.shard_of(k) == 0).unwrap().clone();
        let mut fence = Fence::new(&transition, at(100), true);

        assert!(fence.refuses(0, at(101), std::slice::from_ref(&moved)));
        assert!(
            !fence.refuses(0, at(100), std::slice::from_ref(&moved)),
            "not above the cap"
        );
        assert!(
            !fence.refuses(0, at(101), std::slice::from_ref(&kept)),
            "the key stays"
        );
        assert!(
            fence.refuses(0, at(101), &[kept.clone(), moved.clone()]),
            "a route touching a moved key"
        );
        fence.end = Some(2);
        assert!(fence.refuses(1, at(101), std::slice::from_ref(&moved)));
        assert!(
            !fence.refuses(2, at(101), std::slice::from_ref(&moved)),
            "planned after it ended"
        );
    }

    #[test]
    fn the_fence_point_covers_the_vote_horizon() {
        let cap = Fence::cap_for(at(50), 80, None);
        assert!(cap > at(80) && cap.micros == 80, "{cap:?}");
        assert_eq!(Fence::cap_for(at(90), 80, None), at(90));
        assert_eq!(Fence::cap_for(at(50), 80, Some(120)).micros, 120);
    }

    /// The drain point waits for a slow quorum of every changing shard, and
    /// takes the highest fence among them.
    #[test]
    fn the_drain_point_needs_a_slow_quorum_of_each_changing_shard() {
        let live = Arc::new(Layout::new(0, vec![ids(&[0, 1, 2]), ids(&[3, 4, 5])]));
        let next = Arc::new(Layout::new(1, vec![ids(&[0, 1, 6]), ids(&[3, 4, 5])]));
        let transition = Transition::new(1, live, next);
        let mut fences = HashMap::new();
        fences.insert(NodeId(0), fence(1, 10));
        assert_eq!(drain_point(&transition, &fences), None);
        // Another transition's report does not count.
        fences.insert(NodeId(1), fence(0, 30));
        assert_eq!(drain_point(&transition, &fences), None);
        fences.insert(NodeId(1), fence(1, 20));
        // The unchanged second shard is not waited for.
        assert_eq!(drain_point(&transition, &fences), Some(at(20)));
        fences.insert(NodeId(2), fence(1, 40));
        assert_eq!(drain_point(&transition, &fences), Some(at(40)));
    }

    /// Drained once every shard — changed or not — has a slow quorum past the
    /// drain point.
    #[test]
    fn drained_needs_a_slow_quorum_of_every_shard_past_the_point() {
        let live = Layout::new(0, vec![ids(&[0, 1, 2]), ids(&[3, 4, 5])]);
        let mut reports = HashMap::new();
        for (node, report) in [(0, 30), (1, 30), (3, 30)] {
            reports.insert(NodeId(node), at(report));
        }
        assert!(
            !drained(&live, at(20), &reports),
            "one replica of the second shard"
        );
        reports.insert(NodeId(4), at(20));
        assert!(
            !drained(&live, at(20), &reports),
            "a report at the point is not past it"
        );
        reports.insert(NodeId(4), at(21));
        assert!(drained(&live, at(20), &reports));
    }
}
