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
//! applied there too. So is every replica that keeps a moved slot
//! ([`keepers`]): one that lagged would have to catch up through
//! anti-entropy under the new layout, where a replica that copied the slot
//! holds its writes as events, with no command to ship, and vouches for
//! them all the same.
//!
//! **No drain needed.** A change after which no slot moves to another shard,
//! no replica gains or loses a slot, and every quorum of each changed shard
//! still holds a replica of every quorum it had ([`installs_at_once`]: adding
//! a replica to a shard of three) is installed at once. A transaction
//! planned before it is voted on and recovered under its own layout, a
//! coordinator that planned before it gets no new vote from a replica that
//! installed it, and the two layouts' quorums meet: a transaction planned
//! under the new layout witnesses, through a replica the two share, every one
//! the old layout committed, and a new replica joins through one its shard
//! kept (`Node::join`). Quorums that meet prove nothing of refusals, though:
//! a dependency a node never witnessed, its layout unknown, is proven never
//! committed under each of the two (`Node::orphan_plans`, #443).
//!
//! **Copying the moved slots.** In a transition, every node that gains slots
//! ([`gainers`]) copies them before the change installs its layout: from each
//! shard that holds some, it picks a replica whose watermark report is past
//! that shard's own drain point (the highest fence a slow quorum of it
//! reported) and copies those slots' events. The keys are drained there and
//! take no new write, so the copy is final: every write on them that commits
//! is applied at that replica, and no other will. The activation waits until
//! every gainer has its slots.
//!
//! **Activation.** The [`Active`](ConfigEntry::Active) entry carries the drain
//! point and a clock floor above every moved write's execution
//! ([`Drained`]). A replica that installs it invalidates what it holds
//! unapplied on a moved key above the drain point (it never commits), and
//! votes for no transaction planned under the new layout on a moved key below
//! the floor, so the new writes' events stamp after the moved ones'.

use std::collections::HashMap;
use std::sync::Arc;

use crate::api::{slot_of, ConfigEntry, FenceRecord, Layout};
use crate::clock::{NodeId, Timestamp};
use crate::message::Key;

/// Whether `a` and `b` name the same replicas, in any order.
fn same_replicas(a: &[NodeId], b: &[NodeId]) -> bool {
    a.len() == b.len() && a.iter().all(|node| b.contains(node))
}

/// The distinct `(live shard, next shard)` pairs some slot goes between.
fn slot_moves(live: &Layout, next: &Layout) -> Vec<(usize, usize)> {
    let mut pairs: Vec<(usize, usize)> = live
        .slots()
        .iter()
        .zip(next.slots())
        .map(|(&a, &b)| (a as usize, b as usize))
        .collect();
    pairs.sort_unstable();
    pairs.dedup();
    pairs
}

/// Slot → whether its replica set differs in `next`: a key changes replicas
/// between the two layouts exactly when its slot does.
pub(crate) fn changed_slots(live: &Layout, next: &Layout) -> Vec<bool> {
    let changed: HashMap<(usize, usize), bool> = slot_moves(live, next)
        .into_iter()
        .map(|(a, b)| {
            let changed = !same_replicas(live.replicas_of(a), next.replicas_of(b));
            ((a, b), changed)
        })
        .collect();
    live.slots()
        .iter()
        .zip(next.slots())
        .map(|(&a, &b)| changed[&(a as usize, b as usize)])
        .collect()
}

/// Whether, for every slot, each slow quorum of its replicas in `next` holds
/// a replica of each slow quorum of its replicas in `live`: a quorum of the
/// new replica set holds at least `slow_q(next) − added` of the old replicas,
/// and two subsets of the old set meet when their sizes add up past it. Then
/// a change needs no drain (see the module docs).
pub(crate) fn quorums_intersect(live: &Layout, next: &Layout) -> bool {
    slot_moves(live, next).into_iter().all(|(a, b)| {
        let before = live.replicas_of(a);
        let after = next.replicas_of(b);
        let added = after.iter().filter(|node| !before.contains(node)).count();
        next.slow_quorum_of(b).saturating_sub(added) + live.slow_quorum_of(a) > before.len()
    })
}

/// Whether a change from `live` to `next` can install at once: no slot
/// moves to another shard, no replica of `live` gains a slot or loses one,
/// and the quorums meet ([`quorums_intersect`]). Every replica that gains
/// slots is then a new one joining a shard that kept a replica (see
/// `Node::join`), and every transaction planned before is finished by
/// replicas that all stay. Not so after a removal: a second one could
/// leave such a transaction without a read quorum of replicas that stay —
/// a replica that left gets none of the writes after it, on which the
/// transaction's execution may come to depend.
pub(crate) fn installs_at_once(live: &Layout, next: &Layout) -> bool {
    live.slots() == next.slots()
        && gainers(live, next).iter().all(|node| !live.contains(*node))
        && slot_moves(live, next).into_iter().all(|(a, b)| {
            live.replicas_of(a)
                .iter()
                .all(|node| next.replicas_of(b).contains(node))
        })
        && quorums_intersect(live, next)
}

/// The slots `node` replicates in `next` but not in `live`, by the shard of
/// `live` that holds them.
pub(crate) fn gained_slots(
    live: &Layout,
    next: &Layout,
    node: NodeId,
) -> HashMap<usize, Vec<usize>> {
    let mut gained: HashMap<usize, Vec<usize>> = HashMap::new();
    for (slot, (&a, &b)) in live.slots().iter().zip(next.slots()).enumerate() {
        let (a, b) = (a as usize, b as usize);
        if next.replicas_of(b).contains(&node) && !live.replicas_of(a).contains(&node) {
            gained.entry(a).or_default().push(slot);
        }
    }
    gained
}

/// Every node that replicates some slot in `next` it does not in `live`.
pub(crate) fn gainers(live: &Layout, next: &Layout) -> Vec<NodeId> {
    let mut gainers: Vec<NodeId> = slot_moves(live, next)
        .into_iter()
        .flat_map(|(a, b)| {
            next.replicas_of(b)
                .iter()
                .copied()
                .filter(|node| !live.replicas_of(a).contains(node))
                .collect::<Vec<_>>()
        })
        .collect();
    gainers.sort_unstable();
    gainers.dedup();
    gainers
}

/// Every node that replicates a slot whose replica set changes both in
/// `live` and in `next`: it keeps the slot, with the writes it holds of it,
/// and copies nothing.
pub(crate) fn keepers(live: &Layout, next: &Layout) -> Vec<NodeId> {
    let mut keepers: Vec<NodeId> = slot_moves(live, next)
        .into_iter()
        .filter(|&(a, b)| !same_replicas(live.replicas_of(a), next.replicas_of(b)))
        .flat_map(|(a, b)| {
            next.replicas_of(b)
                .iter()
                .copied()
                .filter(|node| live.replicas_of(a).contains(node))
                .collect::<Vec<_>>()
        })
        .collect();
    keepers.sort_unstable();
    keepers.dedup();
    keepers
}

/// The layout transition a node has installed and not ended yet.
pub(crate) struct Transition {
    /// The epoch the change was first decided at.
    pub since: u64,
    /// The live layout.
    pub live: Arc<Layout>,
    /// The layout the change installs.
    pub pending: Arc<Layout>,
    /// Slot → whether its replica set changes.
    changed: Vec<bool>,
    /// The nodes that gain slots, which copy them before the change installs
    /// its layout.
    pub gainers: Vec<NodeId>,
    /// The nodes that keep a moved slot, whose reports the change waits for.
    pub keepers: Vec<NodeId>,
}

impl Transition {
    pub fn new(since: u64, live: Arc<Layout>, pending: Arc<Layout>) -> Self {
        let changed = changed_slots(&live, &pending);
        let gainers = gainers(&live, &pending);
        let keepers = keepers(&live, &pending);
        Self {
            since,
            live,
            pending,
            changed,
            gainers,
            keepers,
        }
    }

    /// Whether `key`'s replica set changes.
    pub fn moves(&self, key: &Key) -> bool {
        self.changed[slot_of(key)]
    }

    /// The shards of the live layout a changing slot is in.
    fn changing_shards(&self) -> Vec<usize> {
        let mut shards: Vec<usize> = self
            .changed
            .iter()
            .zip(self.live.slots())
            .filter(|(&changed, _)| changed)
            .map(|(_, &shard)| shard as usize)
            .collect();
        shards.sort_unstable();
        shards.dedup();
        shards
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
    /// Slot → whether the transition moves it.
    changed: Vec<bool>,
}

impl Fence {
    pub fn new(transition: &Transition, cap: Timestamp, journaled: bool) -> Self {
        Self {
            since: transition.since,
            end: None,
            cap,
            journaled,
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
            && keys.iter().any(|key| self.changed[slot_of(key)])
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
    /// Slot → whether the activation moved it.
    changed: Vec<bool>,
}

impl Floor {
    /// The floor of the activation at `epoch` that ended `transition`.
    pub fn new(epoch: u64, floor: Timestamp, transition: &Transition) -> Self {
        Self {
            epoch,
            floor,
            changed: transition.changed.clone(),
        }
    }

    /// Whether a vote for a transaction planned under `epoch`, at `t0`,
    /// touching `keys`, is refused.
    pub fn refuses(&self, epoch: u64, t0: Timestamp, keys: &[Key]) -> bool {
        epoch >= self.epoch && t0 < self.floor && keys.iter().any(|key| self.changed[slot_of(key)])
    }
}

/// The drain point of `shard` of the live layout in the transition first
/// decided at `since`: `None` until a slow quorum of its replicas reported
/// their fence for it; then the highest of their fences. Every write on the
/// shard's moved keys that can still commit has a `t0` at or below it.
pub(crate) fn shard_drain_point(
    live: &Layout,
    shard: usize,
    since: u64,
    fences: &HashMap<NodeId, FenceRecord>,
) -> Option<Timestamp> {
    let caps: Vec<Timestamp> = live
        .replicas_of(shard)
        .iter()
        .filter_map(|node| fences.get(node))
        .filter(|fence| fence.since == since)
        .map(|fence| fence.cap)
        .collect();
    if caps.len() < live.slow_quorum_of(shard) {
        return None;
    }
    caps.into_iter().max()
}

/// The drain point of `transition` from the fence reports gathered so far:
/// `None` until a slow quorum of every changing shard reported its fence for
/// it; then the highest of their drain points ([`shard_drain_point`]).
pub(crate) fn drain_point(
    transition: &Transition,
    fences: &HashMap<NodeId, FenceRecord>,
) -> Option<Timestamp> {
    let mut point = Timestamp::MIN;
    for shard in transition.changing_shards() {
        let shard_point = shard_drain_point(&transition.live, shard, transition.since, fences)?;
        point = point.max(shard_point);
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
        ConfigEntry::Active { layout, .. } => ConfigEntry::Active {
            layout: layout.clone(),
            drained: None,
        },
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

    /// Replacing a node changes its shard's slots only; reordering a replica
    /// set changes nothing.
    #[test]
    fn only_the_slots_whose_replicas_change_move() {
        let live = Layout::new(0, vec![ids(&[0, 1, 2]), ids(&[3, 4, 5])]);
        let next = Layout::new(1, vec![ids(&[2, 1, 0]), ids(&[3, 4, 6])]);
        let changed = changed_slots(&live, &next);
        for (slot, &shard) in live.slots().iter().enumerate() {
            assert_eq!(changed[slot], shard == 1, "slot {slot}");
        }
    }

    /// A split moves only the slots the new shard takes, from both old
    /// shards; with as many shards, nothing moves; a merge moves only the
    /// removed shard's slots.
    #[test]
    fn a_rebalance_moves_only_the_slots_it_must() {
        let two = crate::api::ShardMap::even(vec![ids(&[0, 1, 2]), ids(&[3, 4, 5])]);
        let three = two.rebalanced(vec![ids(&[0, 1, 2]), ids(&[3, 4, 5]), ids(&[6, 7, 8])]);
        three.validate().unwrap();
        let moved: Vec<usize> = (0..crate::api::SLOTS)
            .filter(|&slot| two.slots[slot] != three.slots[slot])
            .collect();
        assert_eq!(moved.len(), crate::api::SLOTS / 3);
        assert!(
            moved.iter().all(|&slot| three.slots[slot] == 2),
            "only to the new shard"
        );
        assert!(moved.iter().any(|&slot| two.slots[slot] == 0));
        assert!(moved.iter().any(|&slot| two.slots[slot] == 1));

        let same = three.rebalanced(vec![ids(&[0, 1, 9]), ids(&[3, 4, 5]), ids(&[6, 7, 8])]);
        assert_eq!(same.slots, three.slots, "a replacement moves no slot");

        let merged = three.rebalanced(vec![ids(&[0, 1, 2]), ids(&[3, 4, 5])]);
        merged.validate().unwrap();
        for slot in 0..crate::api::SLOTS {
            if three.slots[slot] != 2 {
                assert_eq!(merged.slots[slot], three.slots[slot], "slot {slot} stays");
            }
        }
    }

    /// The nodes that gain slots, and from which shard: a new replica of a
    /// shard gains all of its slots, a split's new shard the slots it takes.
    #[test]
    fn a_change_names_what_each_node_gains() {
        let live = Layout::new(0, vec![ids(&[0, 1, 2])]);
        let next = Layout::new(1, vec![ids(&[0, 1, 2, 3])]);
        assert_eq!(gainers(&live, &next), ids(&[3]));
        assert_eq!(
            gained_slots(&live, &next, NodeId(3))[&0].len(),
            crate::api::SLOTS
        );
        assert!(gained_slots(&live, &next, NodeId(0)).is_empty());

        let split = Layout::from_map(
            1,
            live.map()
                .rebalanced(vec![ids(&[0, 1, 2]), ids(&[3, 4, 5])]),
        );
        assert_eq!(gainers(&live, &split), ids(&[3, 4, 5]));
        assert_eq!(
            gained_slots(&live, &split, NodeId(4))[&0].len(),
            crate::api::SLOTS / 2
        );
    }

    /// The replicas a change keeps on a moved slot: a replacement's other
    /// replicas, both shards' when a member moves between them, and none of a
    /// split's, whose moved slots all go to new replicas.
    #[test]
    fn a_change_names_the_replicas_that_keep_a_moved_slot() {
        let live = Layout::new(0, vec![ids(&[0, 1, 2]), ids(&[3, 4, 5, 6])]);
        let next = |shards: Vec<Vec<NodeId>>| Layout::from_map(1, live.map().rebalanced(shards));
        let replaced = next(vec![ids(&[0, 1, 7]), ids(&[3, 4, 5, 6])]);
        assert_eq!(keepers(&live, &replaced), ids(&[0, 1]));
        let moved = next(vec![ids(&[0, 1, 2, 6]), ids(&[3, 4, 5])]);
        assert_eq!(keepers(&live, &moved), ids(&[0, 1, 2, 3, 4, 5]));
        let split = next(vec![ids(&[0, 1, 2]), ids(&[3, 4, 5, 6]), ids(&[7, 8, 9])]);
        assert!(keepers(&live, &split).is_empty());
    }

    /// A change installs at once only when no slot changes shard, no member
    /// gains or loses a slot, and the quorums meet.
    #[test]
    fn only_new_replicas_of_kept_shards_join_without_a_transition() {
        let live = Layout::new(0, vec![ids(&[0, 1, 2]), ids(&[3, 4, 5])]);
        let at_once = |shards: Vec<Vec<NodeId>>| {
            installs_at_once(&live, &Layout::from_map(1, live.map().rebalanced(shards)))
        };
        assert!(
            at_once(vec![ids(&[0, 1, 2, 6]), ids(&[3, 4, 5])]),
            "a new replica"
        );
        assert!(!at_once(vec![ids(&[0, 1]), ids(&[3, 4, 5])]), "a removal");
        assert!(
            !at_once(vec![ids(&[0, 1, 6]), ids(&[3, 4, 5])]),
            "a replacement"
        );
        assert!(
            !at_once(vec![ids(&[0, 1, 2, 3]), ids(&[4, 5])]),
            "a member moving shards"
        );
        assert!(
            !at_once(vec![ids(&[0, 1, 2]), ids(&[3, 4, 5]), ids(&[6, 7, 8])]),
            "a split"
        );
    }

    /// One replica added to a shard of three, or one removed, keeps every
    /// new quorum meeting every old one; a replacement, two additions, or a
    /// shrink from five to three do not. (Only the addition skips the drain:
    /// see `installs_at_once`.)
    #[test]
    fn only_some_changes_keep_every_quorum_meeting() {
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
